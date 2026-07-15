use std::collections::BTreeMap;
use std::fmt::{self, Display, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use bytes::Bytes;
use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, Context, IgnoreRef, Props};
use kairo_remote::{
    RemoteAssociationAddress, RemoteError, TcpRemoteActorRuntime, TcpRemoteActorRuntimeBuilder,
};
use kairo_serialization::ActorRefWireData;

use crate::leave_coordinator::{ClusterLeaveCoordinator, register_cluster_coordinated_shutdown};
use crate::{
    CLUSTER_SYSTEM_MANIFESTS, Cluster, ClusterEventPublisher, ClusterGossipProcess,
    ClusterGossipProcessMsg, ClusterGossipProcessSettings, ClusterGossipState,
    ClusterGossipWireInbound, ClusterGossipWireOutbound, ClusterHeartbeatConnector,
    ClusterInitJoinResponder, ClusterInitJoinResponderMsg, ClusterInitJoinResponderPort,
    ClusterInitJoinResponderState, ClusterMembership, ClusterMembershipMsg,
    ClusterMembershipRemoteEnvelopeOutbound, ClusterMembershipWireInbound,
    ClusterMembershipWireOutbound, ClusterMembershipWireOutboundActor, ClusterRemotePeerConnector,
    ClusterSeedJoinEffect, ClusterSeedJoinProcess, ClusterSeedJoinProcessMsg,
    ClusterSeedJoinProcessSettings, ClusterSeedJoinState, ClusterSeedJoinWireInbound,
    ClusterSeedJoinWireOutbound, ClusterSeedJoinWireOutboundActor, ClusterSubscriptionEvent,
    ClusterSubscriptionInitialState, ClusterSystemInbound, DEFAULT_CLUSTER_HEARTBEAT_RECEIVER_PATH,
    DEFAULT_CLUSTER_HEARTBEAT_SENDER_PATH, HeartbeatReceiver, HeartbeatRemoteReceiverInbound,
    HeartbeatRemoteResponseInbound, HeartbeatSender, HeartbeatSenderMsg, HeartbeatSenderSettings,
    MemberEvent, UniqueAddress,
};

const READY_TIMEOUT: Duration = Duration::from_secs(2);
const MANUAL_JOIN_TIMER: &str = "cluster-manual-join";

#[derive(Debug, Clone)]
pub struct ClusterDaemonBootstrapSettings {
    node_uid: u64,
    seed_nodes: Vec<kairo_actor::Address>,
    roles: Vec<String>,
    config_digest: Option<Bytes>,
    seed_process: ClusterSeedJoinProcessSettings,
    gossip_process: ClusterGossipProcessSettings,
    heartbeat_sender: HeartbeatSenderSettings,
    shutdown_timeout: Duration,
    auto_join: bool,
}

impl ClusterDaemonBootstrapSettings {
    pub fn new(node_uid: u64) -> Self {
        Self {
            node_uid,
            seed_nodes: Vec::new(),
            roles: Vec::new(),
            config_digest: Some(Bytes::new()),
            seed_process: ClusterSeedJoinProcessSettings::default(),
            gossip_process: ClusterGossipProcessSettings::default(),
            heartbeat_sender: HeartbeatSenderSettings::default(),
            shutdown_timeout: Duration::from_secs(10),
            auto_join: true,
        }
    }
    pub fn with_seed_nodes(mut self, value: Vec<kairo_actor::Address>) -> Self {
        self.seed_nodes = value;
        self
    }
    pub fn with_roles(mut self, value: Vec<String>) -> Self {
        self.roles = value;
        self
    }
    pub fn with_config_digest(mut self, value: Option<Bytes>) -> Self {
        self.config_digest = value;
        self
    }
    pub fn with_seed_process_settings(mut self, value: ClusterSeedJoinProcessSettings) -> Self {
        self.seed_process = value;
        self
    }
    pub fn with_gossip_process_settings(mut self, value: ClusterGossipProcessSettings) -> Self {
        self.gossip_process = value;
        self
    }
    pub fn with_heartbeat_sender_settings(mut self, value: HeartbeatSenderSettings) -> Self {
        self.heartbeat_sender = value;
        self
    }
    pub fn with_shutdown_timeout(mut self, value: Duration) -> Self {
        self.shutdown_timeout = value;
        self
    }
    pub fn with_auto_join(mut self, value: bool) -> Self {
        self.auto_join = value;
        self
    }
}

#[derive(Debug)]
pub enum ClusterDaemonBootstrapError {
    Actor(ActorError),
    NotMaterialized,
    Remote(RemoteError),
}

impl Display for ClusterDaemonBootstrapError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Actor(e) => write!(f, "{e}"),
            Self::NotMaterialized => write!(f, "cluster daemon has not been materialized by bind"),
            Self::Remote(e) => write!(f, "{e}"),
        }
    }
}
impl std::error::Error for ClusterDaemonBootstrapError {}
impl From<ActorError> for ClusterDaemonBootstrapError {
    fn from(e: ActorError) -> Self {
        Self::Actor(e)
    }
}
impl From<RemoteError> for ClusterDaemonBootstrapError {
    fn from(e: RemoteError) -> Self {
        Self::Remote(e)
    }
}

#[derive(Clone)]
pub struct ClusterDaemonHandle {
    root: ActorRef<()>,
    self_node: UniqueAddress,
    cluster: Cluster,
    daemon: ActorRef<ClusterDaemonMsg>,
    membership: ActorRef<ClusterMembershipMsg>,
    gossip_process: ActorRef<ClusterGossipProcessMsg>,
    heartbeat_sender: ActorRef<HeartbeatSenderMsg>,
    seed_process: ActorRef<ClusterSeedJoinProcessMsg>,
    responder: ActorRef<ClusterInitJoinResponderMsg>,
}
impl ClusterDaemonHandle {
    pub fn root(&self) -> &ActorRef<()> {
        &self.root
    }
    pub fn self_node(&self) -> &UniqueAddress {
        &self.self_node
    }
    pub fn cluster(&self) -> &Cluster {
        &self.cluster
    }
    pub fn membership(&self) -> &ActorRef<ClusterMembershipMsg> {
        &self.membership
    }
    pub fn gossip_process(&self) -> &ActorRef<ClusterGossipProcessMsg> {
        &self.gossip_process
    }
    pub fn heartbeat_sender(&self) -> &ActorRef<HeartbeatSenderMsg> {
        &self.heartbeat_sender
    }
    pub fn seed_process(&self) -> &ActorRef<ClusterSeedJoinProcessMsg> {
        &self.seed_process
    }
    pub fn responder(&self) -> &ActorRef<ClusterInitJoinResponderMsg> {
        &self.responder
    }
}

#[derive(Clone)]
pub struct ClusterDaemonRegistration {
    settings: ClusterDaemonBootstrapSettings,
    handle: Arc<Mutex<Option<ClusterDaemonHandle>>>,
}
impl ClusterDaemonRegistration {
    pub fn handle(&self) -> Option<ClusterDaemonHandle> {
        self.handle
            .lock()
            .expect("cluster daemon handle poisoned")
            .clone()
    }
    pub fn activate(
        &self,
        runtime: &TcpRemoteActorRuntime,
    ) -> Result<ClusterDaemonHandle, ClusterDaemonBootstrapError> {
        let handle = self
            .handle()
            .ok_or(ClusterDaemonBootstrapError::NotMaterialized)?;
        handle
            .daemon
            .tell(ClusterDaemonMsg::StartPeerManagement {
                cluster: Box::new(handle.cluster.clone()),
                self_node: handle.self_node.clone(),
                peer_manager: runtime.peer_manager(),
            })
            .map_err(|error| ActorError::Message(error.reason().to_string()))?;
        for seed in &self.settings.seed_nodes {
            if seed == &handle.self_node.address {
                continue;
            }
            runtime.dial(remote_address_for(seed)?)?;
        }
        if self.settings.auto_join {
            handle
                .daemon
                .tell(ClusterDaemonMsg::StartConfiguredJoin)
                .map_err(|error| ActorError::Message(error.reason().to_string()))?;
        }
        let extension_handle = handle.clone();
        runtime
            .system()
            .register_extension(move |_| crate::ClusterExtension::new(extension_handle));
        Ok(handle)
    }
}

fn remote_address_for(
    address: &kairo_actor::Address,
) -> Result<RemoteAssociationAddress, RemoteError> {
    let host = address
        .host()
        .ok_or_else(|| RemoteError::Outbound("cluster peer has no host".to_string()))?;
    RemoteAssociationAddress::new(address.protocol(), address.system(), host, address.port())
}

pub fn register_cluster_daemon(
    builder: &mut TcpRemoteActorRuntimeBuilder,
    settings: ClusterDaemonBootstrapSettings,
) -> Result<ClusterDaemonRegistration, ClusterDaemonBootstrapError> {
    let handle = Arc::new(Mutex::new(None));
    let slot = Arc::clone(&handle);
    let factory_settings = settings.clone();
    builder.register_control_handler(&CLUSTER_SYSTEM_MANIFESTS, move |context| {
        let self_node = UniqueAddress::new(
            kairo_actor::Address::new(
                context.system().address().protocol(),
                context.system().name(),
                Some(context.settings().canonical_hostname.clone()),
                Some(context.settings().canonical_port),
            ),
            factory_settings.node_uid,
        );
        let (tx, rx) = mpsc::channel();
        let config = DaemonConfig {
            self_node: self_node.clone(),
            roles: factory_settings.roles.clone(),
            seed_nodes: factory_settings.seed_nodes.clone(),
            config_digest: factory_settings.config_digest.clone(),
            seed_process: factory_settings.seed_process.with_start_immediately(false),
            gossip_process: factory_settings.gossip_process,
            heartbeat_sender_settings: factory_settings.heartbeat_sender.clone(),
            heartbeat_sender: None,
            registry: context.registry().clone(),
            outbound: context.outbound().clone(),
            ready: tx,
        };
        let root = context
            .system()
            .spawn_system(
                "cluster",
                Props::new(move || ClusterRoot {
                    config: Some(config),
                }),
            )
            .map_err(|e| RemoteError::Inbound(e.to_string()))?;
        let ready = rx.recv_timeout(READY_TIMEOUT).map_err(|e| {
            RemoteError::Inbound(format!("cluster daemon startup timed out: {e}"))
        })??;
        let DaemonReady {
            inbound,
            publisher,
            daemon,
            membership,
            gossip_process,
            heartbeat_sender,
            leave_coordinator,
            join_wire: _,
            seed_process,
            responder,
        } = ready;
        register_cluster_coordinated_shutdown(
            context.system(),
            root.clone(),
            leave_coordinator,
            factory_settings.shutdown_timeout,
        )
        .map_err(|error| RemoteError::Inbound(error.to_string()))?;
        let daemon = ClusterDaemonHandle {
            root,
            self_node: self_node.clone(),
            cluster: Cluster::with_membership(
                publisher,
                self_node.clone(),
                membership.clone(),
                daemon.clone(),
            ),
            daemon,
            membership,
            gossip_process,
            heartbeat_sender,
            seed_process,
            responder,
        };
        *slot.lock().expect("cluster daemon handle poisoned") = Some(daemon);
        Ok(move |envelope| {
            inbound
                .receive(envelope)
                .map_err(|error| RemoteError::Inbound(error.to_string()))
        })
    })?;
    Ok(ClusterDaemonRegistration { settings, handle })
}

struct DaemonConfig {
    self_node: UniqueAddress,
    roles: Vec<String>,
    seed_nodes: Vec<kairo_actor::Address>,
    config_digest: Option<Bytes>,
    seed_process: ClusterSeedJoinProcessSettings,
    gossip_process: ClusterGossipProcessSettings,
    heartbeat_sender_settings: HeartbeatSenderSettings,
    heartbeat_sender: Option<ActorRef<HeartbeatSenderMsg>>,
    registry: Arc<kairo_serialization::Registry>,
    outbound: Arc<dyn kairo_remote::RemoteOutbound>,
    ready: mpsc::Sender<Result<DaemonReady, RemoteError>>,
}
struct DaemonReady {
    inbound: ClusterSystemInbound,
    publisher: ActorRef<crate::ClusterEventPublisherMsg>,
    daemon: ActorRef<ClusterDaemonMsg>,
    membership: ActorRef<ClusterMembershipMsg>,
    gossip_process: ActorRef<ClusterGossipProcessMsg>,
    heartbeat_sender: ActorRef<HeartbeatSenderMsg>,
    leave_coordinator: ActorRef<crate::leave_coordinator::ClusterLeaveCoordinatorMsg>,
    join_wire: ClusterSeedJoinWireOutbound,
    seed_process: ActorRef<ClusterSeedJoinProcessMsg>,
    responder: ActorRef<ClusterInitJoinResponderMsg>,
}
struct ClusterRoot {
    config: Option<DaemonConfig>,
}
struct ClusterCore {
    config: Option<DaemonConfig>,
}
struct ClusterDaemon {
    config: Option<DaemonConfig>,
    peer_connector: Option<ActorRef<crate::ClusterRemotePeerConnectorMsg>>,
    peer_manager: Option<kairo_remote::TcpRemotePeerManager>,
    join_wire: Option<ClusterSeedJoinWireOutbound>,
    seed_process: Option<ActorRef<ClusterSeedJoinProcessMsg>>,
    self_node: Option<UniqueAddress>,
    manual_join_target: Option<kairo_actor::Address>,
    manual_join_connecting: bool,
    join_requested: bool,
    joined: bool,
    join_retry_interval: Duration,
}
pub(crate) enum ClusterDaemonMsg {
    StartPeerManagement {
        cluster: Box<Cluster>,
        self_node: UniqueAddress,
        peer_manager: kairo_remote::TcpRemotePeerManager,
    },
    StartConfiguredJoin,
    JoinTo {
        address: kairo_actor::Address,
    },
    ManualJoinConnected {
        address: kairo_actor::Address,
        result: Result<(), String>,
    },
    ManualJoinRetry,
    Cluster(Box<ClusterSubscriptionEvent>),
}

impl ClusterDaemon {
    fn start_manual_join(
        &mut self,
        ctx: &mut Context<ClusterDaemonMsg>,
        address: kairo_actor::Address,
    ) -> ActorResult {
        self.join_requested = true;
        let Some(self_node) = &self.self_node else {
            return Err(ActorError::Message(
                "cluster daemon is missing self identity".to_string(),
            ));
        };
        if address == self_node.address {
            if let Some(join_wire) = &self.join_wire {
                join_wire
                    .send_effect(ClusterSeedJoinEffect::JoinSelf)
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            return Ok(());
        }
        self.manual_join_target = Some(address);
        self.connect_manual_join(ctx)
    }

    fn connect_manual_join(&mut self, ctx: &Context<ClusterDaemonMsg>) -> ActorResult {
        if self.manual_join_connecting || self.joined {
            return Ok(());
        }
        let Some(address) = self.manual_join_target.clone() else {
            return Ok(());
        };
        let Some(peer_manager) = self.peer_manager.clone() else {
            ctx.schedule_once_self(self.join_retry_interval, ClusterDaemonMsg::ManualJoinRetry);
            return Ok(());
        };
        let association =
            remote_address_for(&address).map_err(|error| ActorError::Message(error.to_string()))?;
        self.manual_join_connecting = true;
        ctx.spawn_task(move |daemon| {
            let result = peer_manager
                .connect(association)
                .map_err(|error| error.to_string());
            let _ = daemon.tell(ClusterDaemonMsg::ManualJoinConnected { address, result });
        })?;
        Ok(())
    }

    fn observe_cluster(
        &mut self,
        ctx: &mut Context<ClusterDaemonMsg>,
        event: ClusterSubscriptionEvent,
    ) {
        let Some(self_node) = &self.self_node else {
            return;
        };
        let joined = match event {
            ClusterSubscriptionEvent::CurrentState(state) => state
                .members
                .iter()
                .any(|member| member.unique_address == *self_node),
            ClusterSubscriptionEvent::Event(crate::ClusterEvent::Member(event)) => match event {
                MemberEvent::Joined(member)
                | MemberEvent::WeaklyUp(member)
                | MemberEvent::Up(member)
                | MemberEvent::Left(member)
                | MemberEvent::Exited(member)
                | MemberEvent::Downed(member) => member.unique_address == *self_node,
                MemberEvent::Removed { .. } => false,
            },
            ClusterSubscriptionEvent::Event(_) => false,
        };
        if joined {
            self.joined = true;
            self.manual_join_target = None;
            self.manual_join_connecting = false;
            ctx.cancel_timer(MANUAL_JOIN_TIMER);
        }
    }
}

impl Actor for ClusterRoot {
    type Msg = ();
    fn started(&mut self, ctx: &mut Context<()>) -> ActorResult {
        let mut c = self
            .config
            .take()
            .ok_or_else(|| ActorError::Message("missing cluster core config".to_string()))?;
        if c.heartbeat_sender_settings.monitored_by_nr_of_members == 0 {
            return Err(ActorError::Message(
                "cluster heartbeat monitored member count must be greater than zero".to_string(),
            ));
        }
        let heartbeat_sender = ctx.spawn(
            "heartbeatSender",
            Props::new({
                let node = c.self_node.clone();
                let settings = c.heartbeat_sender_settings.clone();
                move || {
                    HeartbeatSender::new(node.clone(), settings.clone())
                        .expect("validated cluster heartbeat settings")
                }
            }),
        )?;
        ctx.spawn(
            "heartbeatReceiver",
            Props::new({
                let node = c.self_node.clone();
                move || HeartbeatReceiver::new(node.clone())
            }),
        )?;
        c.heartbeat_sender = Some(heartbeat_sender);
        ctx.spawn("core", Props::new(move || ClusterCore { config: Some(c) }))?;
        Ok(())
    }
    fn receive(&mut self, _: &mut Context<()>, _: ()) -> ActorResult {
        Ok(())
    }
}
impl Actor for ClusterCore {
    type Msg = ();
    fn started(&mut self, ctx: &mut Context<()>) -> ActorResult {
        let c = self
            .config
            .take()
            .ok_or_else(|| ActorError::Message("missing cluster daemon config".to_string()))?;
        ctx.spawn(
            "daemon",
            Props::new(move || ClusterDaemon {
                config: Some(c),
                peer_connector: None,
                peer_manager: None,
                join_wire: None,
                seed_process: None,
                self_node: None,
                manual_join_target: None,
                manual_join_connecting: false,
                join_requested: false,
                joined: false,
                join_retry_interval: Duration::from_secs(1),
            }),
        )?;
        Ok(())
    }
    fn receive(&mut self, _: &mut Context<()>, _: ()) -> ActorResult {
        Ok(())
    }
}
impl Actor for ClusterDaemon {
    type Msg = ClusterDaemonMsg;
    fn started(&mut self, ctx: &mut Context<Self::Msg>) -> ActorResult {
        let c = self
            .config
            .take()
            .ok_or_else(|| ActorError::Message("missing daemon graph config".to_string()))?;
        let result = build_daemon_graph(ctx, &c).map_err(|e| RemoteError::Inbound(e.to_string()));
        if let Ok(ready) = &result {
            self.join_wire = Some(ready.join_wire.clone());
            self.seed_process = Some(ready.seed_process.clone());
            self.self_node = Some(c.self_node.clone());
            self.join_retry_interval = c.seed_process.retry_interval();
        }
        let failure = result.as_ref().err().map(ToString::to_string);
        let _ = c.ready.send(result);
        if let Some(e) = failure {
            Err(ActorError::Message(e))
        } else {
            Ok(())
        }
    }
    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            ClusterDaemonMsg::StartPeerManagement {
                cluster,
                self_node,
                peer_manager,
            } if self.peer_connector.is_none() => {
                self.peer_manager = Some(peer_manager.clone());
                let cluster = *cluster;
                let subscription =
                    ctx.message_adapter(|event| ClusterDaemonMsg::Cluster(Box::new(event)))?;
                cluster
                    .subscribe_with_initial_state(
                        subscription,
                        ClusterSubscriptionInitialState::Snapshot,
                    )
                    .map_err(|error| ActorError::Message(error.to_string()))?;
                self.peer_connector = Some(ctx.spawn(
                    "peer-connector",
                    Props::new(move || {
                        ClusterRemotePeerConnector::new(
                            cluster.clone(),
                            self_node.clone(),
                            peer_manager.clone(),
                        )
                    }),
                )?);
            }
            ClusterDaemonMsg::StartPeerManagement { .. } => {}
            ClusterDaemonMsg::StartConfiguredJoin if !self.join_requested => {
                self.join_requested = true;
                if let Some(seed_process) = &self.seed_process {
                    seed_process
                        .tell(ClusterSeedJoinProcessMsg::Start)
                        .map_err(|error| ActorError::Message(error.reason().to_string()))?;
                }
            }
            ClusterDaemonMsg::StartConfiguredJoin => {}
            ClusterDaemonMsg::JoinTo { address } if !self.join_requested => {
                self.start_manual_join(ctx, address)?;
            }
            ClusterDaemonMsg::JoinTo { .. } => {}
            ClusterDaemonMsg::ManualJoinConnected { address, result } => {
                self.manual_join_connecting = false;
                if self.manual_join_target.as_ref() != Some(&address) || self.joined {
                    return Ok(());
                }
                if result.is_ok()
                    && let Some(join_wire) = &self.join_wire
                {
                    let _ = join_wire.send_effect(ClusterSeedJoinEffect::Join {
                        target: address.clone(),
                    });
                }
                ctx.start_single_timer(
                    MANUAL_JOIN_TIMER,
                    self.join_retry_interval,
                    ClusterDaemonMsg::ManualJoinRetry,
                );
            }
            ClusterDaemonMsg::ManualJoinRetry if !self.joined => {
                self.connect_manual_join(ctx)?;
            }
            ClusterDaemonMsg::ManualJoinRetry => {}
            ClusterDaemonMsg::Cluster(event) => self.observe_cluster(ctx, *event),
        }
        Ok(())
    }
}

fn build_daemon_graph(
    ctx: &Context<ClusterDaemonMsg>,
    c: &DaemonConfig,
) -> Result<DaemonReady, ActorError> {
    let self_node = c.self_node.clone();
    let publisher = ctx.spawn(
        "publisher",
        Props::new({
            let n = self_node.clone();
            move || ClusterEventPublisher::new(n)
        }),
    )?;
    let membership = ctx.spawn(
        "membership",
        Props::new({
            let n = self_node.clone();
            let r = c.roles.clone();
            let p = publisher.clone();
            move || ClusterMembership::new(n, r, p)
        }),
    )?;
    let leave_coordinator = ctx.spawn(
        "leave-coordinator",
        Props::new({
            let cluster = Cluster::new(publisher.clone());
            let node = self_node.clone();
            let membership = membership.clone();
            let registry = c.registry.clone();
            let outbound = c.outbound.clone();
            move || {
                ClusterLeaveCoordinator::new(
                    cluster.clone(),
                    node.clone(),
                    membership.clone(),
                    registry.clone(),
                    outbound.clone(),
                )
            }
        }),
    )?;
    let heartbeat_sender = c
        .heartbeat_sender
        .clone()
        .ok_or_else(|| ActorError::Message("missing cluster heartbeat sender".to_string()))?;
    let heartbeat_sender_wire = ActorRefWireData::new(format!(
        "{}{}",
        self_node.address, DEFAULT_CLUSTER_HEARTBEAT_SENDER_PATH
    ))
    .map_err(|error| ActorError::Message(error.to_string()))?;
    let heartbeat_receiver_wire = ActorRefWireData::new(format!(
        "{}{}",
        self_node.address, DEFAULT_CLUSTER_HEARTBEAT_RECEIVER_PATH
    ))
    .map_err(|error| ActorError::Message(error.to_string()))?;
    ctx.spawn(
        "heartbeat-connector",
        Props::new({
            let cluster = Cluster::new(publisher.clone());
            let node = self_node.clone();
            let membership = membership.clone();
            let sender = heartbeat_sender.clone();
            let sender_wire = heartbeat_sender_wire.clone();
            let registry = c.registry.clone();
            let outbound = c.outbound.clone();
            move || {
                ClusterHeartbeatConnector::new(
                    cluster.clone(),
                    node.clone(),
                    membership.clone(),
                    sender.clone(),
                    sender_wire.clone(),
                    registry.clone(),
                    outbound.clone(),
                )
            }
        }),
    )?;
    let gossip_process = ctx.spawn(
        "gossip-process",
        Props::new({
            let state = ClusterGossipState::new(self_node.clone());
            let membership = membership.clone();
            let outbound =
                ClusterGossipWireOutbound::from_arc(c.registry.clone(), c.outbound.clone());
            let settings = c.gossip_process;
            move || {
                ClusterGossipProcess::new(
                    state.clone(),
                    membership.clone(),
                    outbound.clone(),
                    settings,
                )
            }
        }),
    )?;
    let wire = ClusterSeedJoinWireOutbound::new(
        self_node.clone(),
        c.roles.clone(),
        c.registry.clone(),
        c.outbound.clone(),
        membership.clone(),
        IgnoreRef::new(),
    );
    let join_wire = wire.clone();
    let effects = ctx.spawn(
        "seed-effects",
        Props::new({
            let w = wire.clone();
            move || ClusterSeedJoinWireOutboundActor::new(w)
        }),
    )?;
    let seeds = if c.seed_nodes.is_empty() {
        vec![self_node.address.clone()]
    } else {
        c.seed_nodes.clone()
    };
    let state = ClusterSeedJoinState::new(
        self_node.address.clone(),
        seeds,
        c.config_digest.clone().unwrap_or_default(),
    )
    .map_err(|e| ActorError::Message(e.to_string()))?;
    let seed_process = ctx.spawn(
        "seed-process",
        Props::new({
            let s = c.seed_process;
            move || ClusterSeedJoinProcess::new(state, effects, s)
        }),
    )?;
    let responder = ctx.spawn(
        "init-join-responder",
        Props::new({
            let state = ClusterInitJoinResponderState::new(
                self_node.address.clone(),
                c.config_digest.clone(),
            );
            move || ClusterInitJoinResponder::new(state, wire)
        }),
    )?;
    membership
        .tell(ClusterMembershipMsg::RegisterInitJoinResponder {
            responder: responder.clone(),
        })
        .map_err(|error| ActorError::Message(error.reason().to_string()))?;
    let route_system = ctx.system().clone();
    let route_registry = c.registry.clone();
    let route_remote = ClusterMembershipRemoteEnvelopeOutbound::from_arc(c.outbound.clone());
    let routes = Arc::new(Mutex::new(
        BTreeMap::<String, ActorRef<ClusterMembershipMsg>>::new(),
    ));
    let route_ids = Arc::new(AtomicU64::new(1));
    let membership_inbound = ClusterMembershipWireInbound::new(
        self_node.clone(),
        c.registry.clone(),
        membership.clone(),
    )
    .with_seed_join_process(seed_process.clone())
    .with_reply_route_factory(move |node| {
        let key = node.ordering_key();
        let mut routes = routes
            .lock()
            .map_err(|_| "cluster reply routes poisoned".to_string())?;
        if let Some(route) = routes.get(&key) {
            return Ok(route.clone());
        }
        let outbound = ClusterMembershipWireOutbound::new(
            node.clone(),
            route_registry.clone(),
            route_remote.clone(),
        );
        let id = route_ids.fetch_add(1, Ordering::Relaxed);
        let route = route_system
            .spawn_system(
                format!("cluster-membership-reply-{id}"),
                Props::new(move || ClusterMembershipWireOutboundActor::new(outbound)),
            )
            .map_err(|error| error.to_string())?;
        routes.insert(key, route.clone());
        Ok(route)
    });
    let seed_inbound = ClusterSeedJoinWireInbound::new(
        c.registry.clone(),
        seed_process.clone(),
        ClusterInitJoinResponderPort::new(responder.clone()),
    );
    let gossip_inbound = ClusterGossipWireInbound::new(c.registry.clone(), gossip_process.clone());
    let heartbeat_receiver_inbound = HeartbeatRemoteReceiverInbound::from_arc(
        self_node.clone(),
        c.registry.clone(),
        c.outbound.clone(),
    )
    .with_sender(Some(heartbeat_receiver_wire));
    let heartbeat_response_inbound = HeartbeatRemoteResponseInbound::new(
        heartbeat_sender_wire,
        c.registry.clone(),
        heartbeat_sender.clone(),
    );
    Ok(DaemonReady {
        inbound: ClusterSystemInbound::new(self_node)
            .with_membership(membership_inbound)
            .with_gossip(gossip_inbound)
            .with_heartbeat_receiver(heartbeat_receiver_inbound)
            .with_heartbeat_response(heartbeat_response_inbound)
            .with_seed_join(seed_inbound),
        publisher,
        daemon: ctx.myself().clone(),
        membership,
        gossip_process,
        heartbeat_sender,
        leave_coordinator,
        join_wire,
        seed_process,
        responder,
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use kairo_remote::{
        RemoteSettings, TcpRemoteActorRuntime, TcpRemoteReconnectSettings,
        register_remote_protocol_codecs,
    };
    use kairo_serialization::Registry;
    use kairo_testkit::{ActorSystemTestKit, TestProbe, await_assert};

    use super::*;
    use crate::{
        ClusterSeedJoinPhase, ClusterSeedJoinProcessSnapshot, DeadlineFailureDetectorSettings,
        Gossip, HeartbeatSenderSnapshot, MemberStatus, ReachabilityStatus,
        register_cluster_protocol_codecs,
    };

    fn registry() -> Arc<Registry> {
        let mut registry = Registry::new();
        register_remote_protocol_codecs(&mut registry).unwrap();
        register_cluster_protocol_codecs(&mut registry).unwrap();
        Arc::new(registry)
    }

    fn current_gossip(
        probe: &kairo_testkit::TestProbe<Gossip>,
        handle: &ClusterDaemonHandle,
    ) -> Gossip {
        handle
            .membership()
            .tell(ClusterMembershipMsg::SendCurrentGossip {
                reply_to: probe.actor_ref(),
            })
            .unwrap();
        probe.expect_msg(Duration::from_secs(1)).unwrap()
    }

    struct ComposedNode {
        kit: ActorSystemTestKit,
        runtime: TcpRemoteActorRuntime,
        handle: ClusterDaemonHandle,
        state: TestProbe<Gossip>,
        heartbeat_state: TestProbe<HeartbeatSenderSnapshot>,
    }

    impl ComposedNode {
        fn start(
            system: &str,
            node_uid: u64,
            remote_uid: u64,
            seed_nodes: Vec<kairo_actor::Address>,
            registry: Arc<Registry>,
        ) -> Self {
            Self::start_with_acceptable_pause(
                system,
                node_uid,
                remote_uid,
                seed_nodes,
                registry,
                Duration::from_millis(45),
            )
        }

        fn start_with_acceptable_pause(
            system: &str,
            node_uid: u64,
            remote_uid: u64,
            seed_nodes: Vec<kairo_actor::Address>,
            registry: Arc<Registry>,
            acceptable_pause: Duration,
        ) -> Self {
            Self::start_with_options(
                system,
                node_uid,
                remote_uid,
                seed_nodes,
                registry,
                acceptable_pause,
                true,
            )
        }

        fn start_with_options(
            system: &str,
            node_uid: u64,
            remote_uid: u64,
            seed_nodes: Vec<kairo_actor::Address>,
            registry: Arc<Registry>,
            acceptable_pause: Duration,
            auto_join: bool,
        ) -> Self {
            let kit = ActorSystemTestKit::new(system).unwrap();
            let mut builder = TcpRemoteActorRuntime::builder(
                kit.system().clone(),
                registry,
                RemoteSettings::new("127.0.0.1", 0),
                remote_uid,
            )
            .with_reconnect_settings(
                TcpRemoteReconnectSettings::new(
                    Duration::from_millis(200),
                    Duration::from_millis(400),
                )
                .unwrap(),
            );
            let registration = register_cluster_daemon(
                &mut builder,
                ClusterDaemonBootstrapSettings::new(node_uid)
                    .with_seed_nodes(seed_nodes)
                    .with_config_digest(Some(Bytes::from_static(b"cluster")))
                    .with_gossip_process_settings(
                        ClusterGossipProcessSettings::new(Duration::from_millis(15)).unwrap(),
                    )
                    .with_heartbeat_sender_settings(
                        HeartbeatSenderSettings::new(
                            5,
                            DeadlineFailureDetectorSettings::new(
                                Duration::from_millis(15),
                                acceptable_pause,
                            )
                            .unwrap(),
                        )
                        .with_heartbeat_expected_response_after(Duration::from_millis(10)),
                    )
                    .with_shutdown_timeout(Duration::from_secs(3))
                    .with_auto_join(auto_join),
            )
            .unwrap();
            let runtime = builder.bind().unwrap();
            let handle = registration.activate(&runtime).unwrap();
            let state = kit.create_probe::<Gossip>("state").unwrap();
            let heartbeat_state = kit
                .create_probe::<HeartbeatSenderSnapshot>("heartbeat-state")
                .unwrap();
            Self {
                kit,
                runtime,
                handle,
                state,
                heartbeat_state,
            }
        }

        fn gossip(&self) -> Gossip {
            current_gossip(&self.state, &self.handle)
        }

        fn heartbeat(&self) -> HeartbeatSenderSnapshot {
            self.handle
                .heartbeat_sender()
                .tell(HeartbeatSenderMsg::SendSnapshot {
                    reply_to: self.heartbeat_state.actor_ref(),
                })
                .unwrap();
            self.heartbeat_state
                .expect_msg(Duration::from_secs(1))
                .unwrap()
        }

        fn shutdown(self) {
            self.kit.system().stop(self.handle.root());
            self.runtime.shutdown().unwrap();
            self.kit.shutdown(Duration::from_secs(1)).unwrap();
        }
    }

    #[test]
    fn two_composed_runtimes_form_through_automatic_seed_contact() {
        let seed_kit = ActorSystemTestKit::new("daemon-seed").unwrap();
        let registry = registry();
        let mut seed_builder = TcpRemoteActorRuntime::builder(
            seed_kit.system().clone(),
            registry.clone(),
            RemoteSettings::new("127.0.0.1", 0),
            101,
        );
        let seed_registration = register_cluster_daemon(
            &mut seed_builder,
            ClusterDaemonBootstrapSettings::new(1)
                .with_config_digest(Some(Bytes::from_static(b"cluster")))
                .with_gossip_process_settings(
                    ClusterGossipProcessSettings::new(Duration::from_millis(20)).unwrap(),
                ),
        )
        .unwrap();
        let seed_runtime = seed_builder.bind().unwrap();
        let inactive = seed_registration.handle().unwrap();
        assert!(!seed_kit.system().has_extension::<crate::ClusterExtension>());
        let seed_process_state = seed_kit
            .create_probe::<ClusterSeedJoinProcessSnapshot>("seed-process-state")
            .unwrap();
        inactive
            .seed_process()
            .tell(ClusterSeedJoinProcessMsg::Snapshot {
                reply_to: seed_process_state.actor_ref(),
            })
            .unwrap();
        assert_eq!(
            seed_process_state
                .expect_msg(Duration::from_secs(1))
                .unwrap()
                .phase,
            ClusterSeedJoinPhase::Ready
        );
        let seed_handle = seed_registration.activate(&seed_runtime).unwrap();
        assert!(seed_kit.system().has_extension::<crate::ClusterExtension>());
        let seed_state = seed_kit.create_probe::<Gossip>("seed-state").unwrap();
        assert!(
            seed_handle
                .responder()
                .path()
                .as_str()
                .contains("/system/cluster#")
        );
        assert!(seed_handle.responder().path().as_str().contains("/core#"));
        assert!(seed_handle.responder().path().as_str().contains("/daemon#"));
        await_assert(Duration::from_secs(2), Duration::from_millis(5), || {
            let gossip = current_gossip(&seed_state, &seed_handle);
            (gossip.member(seed_handle.self_node()).map(|m| m.status) == Some(MemberStatus::Up))
                .then_some(())
                .ok_or_else(|| "first seed has not formed".to_string())
        })
        .unwrap();

        let joining_kit = ActorSystemTestKit::new("daemon-joining").unwrap();
        let mut joining_builder = TcpRemoteActorRuntime::builder(
            joining_kit.system().clone(),
            registry,
            RemoteSettings::new("127.0.0.1", 0),
            202,
        );
        let joining_registration = register_cluster_daemon(
            &mut joining_builder,
            ClusterDaemonBootstrapSettings::new(2)
                .with_seed_nodes(vec![seed_handle.self_node().address.clone()])
                .with_config_digest(Some(Bytes::from_static(b"cluster")))
                .with_gossip_process_settings(
                    ClusterGossipProcessSettings::new(Duration::from_millis(20)).unwrap(),
                ),
        )
        .unwrap();
        let joining_runtime = joining_builder.bind().unwrap();
        let joining_handle = joining_registration.activate(&joining_runtime).unwrap();
        let joining_state = joining_kit.create_probe::<Gossip>("joining-state").unwrap();

        await_assert(Duration::from_secs(3), Duration::from_millis(10), || {
            let seed_gossip = current_gossip(&seed_state, &seed_handle);
            let joining_gossip = current_gossip(&joining_state, &joining_handle);
            let both_up = [seed_handle.self_node(), joining_handle.self_node()]
                .into_iter()
                .all(|node| {
                    seed_gossip.member(node).map(|member| member.status) == Some(MemberStatus::Up)
                        && joining_gossip.member(node).map(|member| member.status)
                            == Some(MemberStatus::Up)
                });
            let both_converged = [seed_handle.self_node(), joining_handle.self_node()]
                .into_iter()
                .all(|node| {
                    seed_gossip.seen_by().contains(node) && joining_gossip.seen_by().contains(node)
                });
            if both_up && both_converged {
                Ok(())
            } else {
                Err("periodic gossip has not converged both members to Up".to_string())
            }
        })
        .unwrap();

        seed_kit.system().stop(seed_handle.root());
        joining_kit.system().stop(joining_handle.root());
        joining_runtime.shutdown().unwrap();
        seed_runtime.shutdown().unwrap();
        joining_kit.shutdown(Duration::from_secs(1)).unwrap();
        seed_kit.shutdown(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn three_composed_runtimes_form_full_mesh_and_converge_to_up() {
        let registry = registry();
        let seed = ComposedNode::start("daemon-mesh-seed", 11, 1011, vec![], registry.clone());
        await_assert(Duration::from_secs(2), Duration::from_millis(5), || {
            (seed
                .gossip()
                .member(seed.handle.self_node())
                .map(|member| member.status)
                == Some(MemberStatus::Up))
            .then_some(())
            .ok_or_else(|| "first seed has not formed".to_string())
        })
        .unwrap();
        let seed_address = seed.handle.self_node().address.clone();
        let second = ComposedNode::start(
            "daemon-mesh-second",
            22,
            2022,
            vec![seed_address.clone()],
            registry.clone(),
        );
        let third =
            ComposedNode::start("daemon-mesh-third", 33, 3033, vec![seed_address], registry);
        let members = [
            seed.handle.self_node().clone(),
            second.handle.self_node().clone(),
            third.handle.self_node().clone(),
        ];

        await_assert(Duration::from_secs(5), Duration::from_millis(10), || {
            let views = [seed.gossip(), second.gossip(), third.gossip()];
            let converged = views.iter().all(|gossip| {
                members.iter().all(|node| {
                    gossip.member(node).map(|member| member.status) == Some(MemberStatus::Up)
                        && gossip.seen_by().contains(node)
                })
            });
            let full_mesh = [
                seed.runtime.association_cache().route_count(),
                second.runtime.association_cache().route_count(),
                third.runtime.association_cache().route_count(),
            ]
            .into_iter()
            .all(|routes| routes >= 2);
            (converged && full_mesh).then_some(()).ok_or_else(|| {
                format!(
                    "three-node cluster has not converged: converged={converged}, routes=[{}, {}, {}]",
                    seed.runtime.association_cache().route_count(),
                    second.runtime.association_cache().route_count(),
                    third.runtime.association_cache().route_count(),
                )
            })
        })
        .unwrap();

        await_assert(Duration::from_secs(3), Duration::from_millis(10), || {
            let snapshots = [seed.heartbeat(), second.heartbeat(), third.heartbeat()];
            snapshots
                .iter()
                .all(|snapshot| {
                    snapshot.initialized
                        && snapshot.sequence_nr >= 8
                        && snapshot.active_receivers.len() == 2
                        && snapshot.monitored_receivers.len() == 2
                        && snapshot.unavailable_receivers.is_empty()
                })
                .then_some(())
                .ok_or_else(|| "automatic remote heartbeat responses are not healthy".to_string())
        })
        .unwrap();

        third.shutdown();
        second.shutdown();
        seed.shutdown();
    }

    #[test]
    fn composed_heartbeat_failure_marks_peer_unreachable_in_gossip() {
        let registry = registry();
        let seed = ComposedNode::start("daemon-fd-seed", 41, 4041, vec![], registry.clone());
        await_assert(Duration::from_secs(2), Duration::from_millis(5), || {
            (seed
                .gossip()
                .member(seed.handle.self_node())
                .map(|member| member.status)
                == Some(MemberStatus::Up))
            .then_some(())
            .ok_or_else(|| "failure-detector seed has not formed".to_string())
        })
        .unwrap();
        let peer = ComposedNode::start(
            "daemon-fd-peer",
            42,
            4042,
            vec![seed.handle.self_node().address.clone()],
            registry,
        );
        let peer_node = peer.handle.self_node().clone();
        await_assert(Duration::from_secs(3), Duration::from_millis(10), || {
            let heartbeat = seed.heartbeat();
            (heartbeat.monitored_receivers.contains(&peer_node)
                && heartbeat.sequence_nr >= 3
                && heartbeat.unavailable_receivers.is_empty())
            .then_some(())
            .ok_or_else(|| "peer heartbeat was not established".to_string())
        })
        .unwrap();

        peer.kit.system().stop(peer.handle.root());
        peer.runtime.shutdown().unwrap();
        peer.kit.shutdown(Duration::from_secs(1)).unwrap();

        await_assert(Duration::from_secs(3), Duration::from_millis(10), || {
            let gossip = seed.gossip();
            (gossip
                .reachability()
                .status(seed.handle.self_node(), &peer_node)
                == ReachabilityStatus::Unreachable)
                .then_some(())
                .ok_or_else(|| "heartbeat failure has not entered gossip reachability".to_string())
        })
        .unwrap();

        seed.shutdown();
    }

    #[test]
    fn composed_heartbeat_recovers_reconnected_peer_to_reachable() {
        let registry = registry();
        let seed = ComposedNode::start("daemon-recovery-seed", 51, 5051, vec![], registry.clone());
        await_assert(Duration::from_secs(2), Duration::from_millis(5), || {
            (seed
                .gossip()
                .member(seed.handle.self_node())
                .map(|member| member.status)
                == Some(MemberStatus::Up))
            .then_some(())
            .ok_or_else(|| "recovery seed has not formed".to_string())
        })
        .unwrap();
        let peer = ComposedNode::start(
            "daemon-recovery-peer",
            52,
            5052,
            vec![seed.handle.self_node().address.clone()],
            registry,
        );
        let peer_node = peer.handle.self_node().clone();
        await_assert(Duration::from_secs(3), Duration::from_millis(10), || {
            let heartbeat = seed.heartbeat();
            (heartbeat.monitored_receivers.contains(&peer_node)
                && heartbeat.sequence_nr >= 3
                && heartbeat.unavailable_receivers.is_empty())
            .then_some(())
            .ok_or_else(|| "recovery heartbeat was not established".to_string())
        })
        .unwrap();
        let peer_address = crate::ClusterAssociationPeerTarget::new(peer_node.clone())
            .unwrap()
            .association()
            .clone();
        seed.runtime
            .association_cache()
            .remove_route_and_close(&peer_address, "heartbeat recovery test route loss")
            .expect("seed route should exist")
            .unwrap();

        await_assert(Duration::from_secs(3), Duration::from_millis(10), || {
            (seed
                .gossip()
                .reachability()
                .status(seed.handle.self_node(), &peer_node)
                == ReachabilityStatus::Unreachable)
                .then_some(())
                .ok_or_else(|| "route loss was not observed as unreachable".to_string())
        })
        .unwrap();
        await_assert(Duration::from_secs(4), Duration::from_millis(10), || {
            let gossip = seed.gossip();
            let heartbeat = seed.heartbeat();
            (gossip
                .reachability()
                .status(seed.handle.self_node(), &peer_node)
                == ReachabilityStatus::Reachable
                && heartbeat.unavailable_receivers.is_empty())
            .then_some(())
            .ok_or_else(|| "reconnected peer has not recovered to reachable".to_string())
        })
        .unwrap();

        peer.shutdown();
        seed.shutdown();
    }

    #[test]
    fn composed_coordinated_shutdown_removes_leaving_peer_gracefully() {
        let registry = registry();
        let acceptable_pause = Duration::from_secs(5);
        let seed = ComposedNode::start_with_acceptable_pause(
            "daemon-leave-seed",
            61,
            6061,
            vec![],
            registry.clone(),
            acceptable_pause,
        );
        await_assert(Duration::from_secs(2), Duration::from_millis(5), || {
            (seed
                .gossip()
                .member(seed.handle.self_node())
                .map(|member| member.status)
                == Some(MemberStatus::Up))
            .then_some(())
            .ok_or_else(|| "leave seed has not formed".to_string())
        })
        .unwrap();
        let peer = ComposedNode::start_with_acceptable_pause(
            "daemon-leave-peer",
            62,
            6062,
            vec![seed.handle.self_node().address.clone()],
            registry,
            acceptable_pause,
        );
        let peer_node = peer.handle.self_node().clone();
        await_assert(Duration::from_secs(3), Duration::from_millis(10), || {
            let seed_gossip = seed.gossip();
            let peer_gossip = peer.gossip();
            [seed.handle.self_node(), &peer_node]
                .into_iter()
                .all(|node| {
                    seed_gossip.member(node).map(|member| member.status) == Some(MemberStatus::Up)
                        && peer_gossip.member(node).map(|member| member.status)
                            == Some(MemberStatus::Up)
                })
                .then_some(())
                .ok_or_else(|| "leaving pair has not converged to Up".to_string())
        })
        .unwrap();

        peer.handle.cluster().leave_self().unwrap();
        assert!(peer.handle.root().wait_for_stop(Duration::from_secs(3)));

        await_assert(Duration::from_secs(3), Duration::from_millis(10), || {
            let gossip = seed.gossip();
            (!gossip.has_member(&peer_node) && gossip.tombstones().contains_key(&peer_node))
                .then_some(())
                .ok_or_else(|| "confirmed exiting peer has not been removed".to_string())
        })
        .unwrap();

        peer.runtime.shutdown().unwrap();
        peer.kit.shutdown(Duration::from_secs(1)).unwrap();
        seed.shutdown();
    }

    #[test]
    fn actor_system_cluster_extension_manually_joins_an_existing_node() {
        let registry = registry();
        let seed = ComposedNode::start("daemon-extension-seed", 71, 7071, vec![], registry.clone());
        await_assert(Duration::from_secs(2), Duration::from_millis(5), || {
            (seed
                .gossip()
                .member(seed.handle.self_node())
                .map(|member| member.status)
                == Some(MemberStatus::Up))
            .then_some(())
            .ok_or_else(|| "extension seed has not formed".to_string())
        })
        .unwrap();
        let joining = ComposedNode::start_with_options(
            "daemon-extension-joining",
            72,
            7072,
            vec![],
            registry,
            Duration::from_millis(45),
            false,
        );
        assert!(joining.gossip().members().is_empty());
        let extension = crate::ClusterExtension::get(joining.kit.system()).unwrap();
        assert_eq!(extension.self_node(), joining.handle.self_node());

        extension
            .join(seed.handle.self_node().address.clone())
            .unwrap();
        let nodes = [
            seed.handle.self_node().clone(),
            joining.handle.self_node().clone(),
        ];
        await_assert(Duration::from_secs(3), Duration::from_millis(10), || {
            let views = [seed.gossip(), joining.gossip()];
            views
                .iter()
                .all(|gossip| {
                    nodes.iter().all(|node| {
                        gossip.member(node).map(|member| member.status) == Some(MemberStatus::Up)
                    })
                })
                .then_some(())
                .ok_or_else(|| "manual extension join has not converged".to_string())
        })
        .unwrap();

        extension
            .join(seed.handle.self_node().address.clone())
            .unwrap();
        assert_eq!(joining.gossip().members().len(), 2);

        joining.shutdown();
        seed.shutdown();
    }
}
