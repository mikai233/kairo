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

use crate::{
    CLUSTER_SYSTEM_MANIFESTS, Cluster, ClusterEventPublisher, ClusterGossipProcess,
    ClusterGossipProcessMsg, ClusterGossipProcessSettings, ClusterGossipState,
    ClusterGossipWireInbound, ClusterGossipWireOutbound, ClusterInitJoinResponder,
    ClusterInitJoinResponderMsg, ClusterInitJoinResponderPort, ClusterInitJoinResponderState,
    ClusterMembership, ClusterMembershipMsg, ClusterMembershipRemoteEnvelopeOutbound,
    ClusterMembershipWireInbound, ClusterMembershipWireOutbound,
    ClusterMembershipWireOutboundActor, ClusterRemotePeerConnector, ClusterSeedJoinProcess,
    ClusterSeedJoinProcessMsg, ClusterSeedJoinProcessSettings, ClusterSeedJoinState,
    ClusterSeedJoinWireInbound, ClusterSeedJoinWireOutbound, ClusterSeedJoinWireOutboundActor,
    ClusterSystemInbound, UniqueAddress,
};

const READY_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Clone)]
pub struct ClusterDaemonBootstrapSettings {
    node_uid: u64,
    seed_nodes: Vec<kairo_actor::Address>,
    roles: Vec<String>,
    config_digest: Option<Bytes>,
    seed_process: ClusterSeedJoinProcessSettings,
    gossip_process: ClusterGossipProcessSettings,
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
                cluster: handle.cluster.clone(),
                self_node: handle.self_node.clone(),
                peer_manager: runtime.peer_manager(),
            })
            .map_err(|error| ActorError::Message(error.reason().to_string()))?;
        for seed in &self.settings.seed_nodes {
            if seed == &handle.self_node.address {
                continue;
            }
            let host = seed
                .host()
                .ok_or_else(|| RemoteError::Outbound("cluster seed has no host".to_string()))?;
            runtime.dial(RemoteAssociationAddress::new(
                seed.protocol(),
                seed.system(),
                host,
                seed.port(),
            )?)?;
        }
        handle
            .seed_process
            .tell(ClusterSeedJoinProcessMsg::Start)
            .map_err(|error| ActorError::Message(error.reason().to_string()))?;
        Ok(handle)
    }
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
            seed_process,
            responder,
        } = ready;
        let daemon = ClusterDaemonHandle {
            root,
            self_node,
            cluster: Cluster::new(publisher),
            daemon,
            membership,
            gossip_process,
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
}
enum ClusterDaemonMsg {
    StartPeerManagement {
        cluster: Cluster,
        self_node: UniqueAddress,
        peer_manager: kairo_remote::TcpRemotePeerManager,
    },
}

impl Actor for ClusterRoot {
    type Msg = ();
    fn started(&mut self, ctx: &mut Context<()>) -> ActorResult {
        let c = self
            .config
            .take()
            .ok_or_else(|| ActorError::Message("missing cluster core config".to_string()))?;
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
    Ok(DaemonReady {
        inbound: ClusterSystemInbound::new(self_node)
            .with_membership(membership_inbound)
            .with_gossip(gossip_inbound)
            .with_seed_join(seed_inbound),
        publisher,
        daemon: ctx.myself().clone(),
        membership,
        gossip_process,
        seed_process,
        responder,
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use kairo_remote::{RemoteSettings, TcpRemoteActorRuntime, register_remote_protocol_codecs};
    use kairo_serialization::Registry;
    use kairo_testkit::{ActorSystemTestKit, TestProbe, await_assert};

    use super::*;
    use crate::{
        ClusterSeedJoinPhase, ClusterSeedJoinProcessSnapshot, Gossip, MemberStatus,
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
    }

    impl ComposedNode {
        fn start(
            system: &str,
            node_uid: u64,
            remote_uid: u64,
            seed_nodes: Vec<kairo_actor::Address>,
            registry: Arc<Registry>,
        ) -> Self {
            let kit = ActorSystemTestKit::new(system).unwrap();
            let mut builder = TcpRemoteActorRuntime::builder(
                kit.system().clone(),
                registry,
                RemoteSettings::new("127.0.0.1", 0),
                remote_uid,
            );
            let registration = register_cluster_daemon(
                &mut builder,
                ClusterDaemonBootstrapSettings::new(node_uid)
                    .with_seed_nodes(seed_nodes)
                    .with_config_digest(Some(Bytes::from_static(b"cluster")))
                    .with_gossip_process_settings(
                        ClusterGossipProcessSettings::new(Duration::from_millis(15)).unwrap(),
                    ),
            )
            .unwrap();
            let runtime = builder.bind().unwrap();
            let handle = registration.activate(&runtime).unwrap();
            let state = kit.create_probe::<Gossip>("state").unwrap();
            Self {
                kit,
                runtime,
                handle,
                state,
            }
        }

        fn gossip(&self) -> Gossip {
            current_gossip(&self.state, &self.handle)
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

        third.shutdown();
        second.shutdown();
        seed.shutdown();
    }
}
