use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use kairo::actor::{
    Actor, ActorError, ActorRef, ActorResult, ActorSystem, Address, Props, Recipient,
};
use kairo::cluster::{
    Cluster, ClusterEventPublisher, ClusterEventPublisherMsg, Gossip, Member, MemberStatus,
    UniqueAddress,
};
use kairo::cluster_tools::{
    ClusterToolsSystemInbound, ClusterToolsTcpPeerBootstrap, ClusterToolsTcpPeerBootstrapSettings,
    ClusterToolsTcpPeerConnectorMsg, ClusterToolsTcpPeerConnectorSettings,
    ClusterToolsTcpPeerConnectorSnapshot, ClusterToolsTcpPeerRuntime,
    DistributedPubSubMediatorActor, DistributedPubSubMediatorMsg, LocalPubSubMsg,
    PubSubGossipActor, PubSubGossipMsg, PubSubGossipWireInbound, PubSubRemoteDeliveryInbound,
    PubSubRemoteDeliveryOutbound, PubSubStatus, PubSubSubscribeAck, SingletonManagerActor,
    SingletonManagerMsg, SingletonManagerRemoteInbound, TopicName, TopicPublishMode,
    register_cluster_tools_protocol_codecs,
};
use kairo::remote::{RemoteAssociationCache, RemoteOutbound, RemoteSettings};
use kairo::serialization::Registry;

use crate::reply::spawn_one_shot_reply;

static SNAPSHOT_ID: AtomicU64 = AtomicU64::new(0);
pub const EXAMPLE_PUBSUB_TOPIC: &str = "example-status";

#[derive(Default)]
pub struct RecordingPubSubStatuses {
    received: Mutex<Vec<PubSubStatus>>,
    changed: Condvar,
}

impl RecordingPubSubStatuses {
    pub fn wait_for_len(&self, len: usize, timeout: Duration) -> Vec<PubSubStatus> {
        let deadline = Instant::now() + timeout;
        let mut received = self.received.lock().expect("pubsub statuses poisoned");
        while received.len() < len {
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                break;
            };
            let (next_received, wait) = self
                .changed
                .wait_timeout(received, remaining)
                .expect("pubsub statuses poisoned");
            received = next_received;
            if wait.timed_out() {
                break;
            }
        }
        received.clone()
    }
}

struct RecordingPubSubStatusActor {
    recorder: Arc<RecordingPubSubStatuses>,
}

impl RecordingPubSubStatusActor {
    fn new(recorder: Arc<RecordingPubSubStatuses>) -> Self {
        Self { recorder }
    }
}

impl Actor for RecordingPubSubStatusActor {
    type Msg = PubSubStatus;

    fn receive(
        &mut self,
        _ctx: &mut kairo::actor::Context<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorResult {
        self.recorder
            .received
            .lock()
            .expect("pubsub statuses poisoned")
            .push(msg);
        self.recorder.changed.notify_all();
        Ok(())
    }
}

pub struct ClusterToolsTcpExampleNode {
    system: ActorSystem,
    publisher: ActorRef<ClusterEventPublisherMsg>,
    bootstrap: ClusterToolsTcpPeerBootstrap<PubSubStatus>,
    registry: Arc<Registry>,
    association_cache: RemoteAssociationCache,
    status_recorder: Arc<RecordingPubSubStatuses>,
}

impl ClusterToolsTcpExampleNode {
    pub fn bind(
        system_name: &str,
        node_uid: u64,
        system_uid: u64,
        connector_name: &str,
    ) -> Result<Self, Box<dyn Error>> {
        let system = ActorSystem::builder(system_name).build()?;
        let publisher_node = UniqueAddress::new(Address::local(system_name), node_uid);
        let publisher = system.spawn(
            "cluster-events",
            Props::new(move || ClusterEventPublisher::new(publisher_node.clone())),
        )?;
        let cluster = Cluster::new(publisher.clone());
        let registry = cluster_tools_registry()?;
        let inbound_system = system.clone();
        let inbound_registry = registry.clone();
        let status_recorder = Arc::new(RecordingPubSubStatuses::default());
        let status_recorder_actor = system.spawn(
            "pubsub-status-recorder",
            Props::new({
                let status_recorder = status_recorder.clone();
                move || RecordingPubSubStatusActor::new(status_recorder.clone())
            }),
        )?;
        let mediator_slot = Arc::new(Mutex::new(None));
        let inbound_mediator_slot = mediator_slot.clone();
        let settings = ClusterToolsTcpPeerBootstrapSettings::new()
            .with_connector_name(connector_name)
            .with_connector_settings(
                ClusterToolsTcpPeerConnectorSettings::new(Duration::from_millis(100))?
                    .with_automatic_retry_ticks(false),
            )
            .with_shutdown_timeout(Duration::from_secs(1));
        let runtime = ClusterToolsTcpPeerRuntime::bind(
            system_name,
            node_uid,
            system_uid,
            RemoteSettings::new("127.0.0.1", 0),
            move |self_node| {
                inbound_for(
                    &inbound_system,
                    inbound_registry,
                    self_node,
                    inbound_mediator_slot,
                )
            },
        )?;
        let association_cache = runtime.association_cache().clone();
        subscribe_status_recorder(&system, &mediator_slot, status_recorder_actor)?;
        let bootstrap =
            ClusterToolsTcpPeerBootstrap::spawn_with_runtime(&system, cluster, runtime, settings)?;

        Ok(Self {
            system,
            publisher,
            bootstrap,
            registry,
            association_cache,
            status_recorder,
        })
    }

    pub fn self_node(&self) -> &UniqueAddress {
        self.bootstrap.self_node()
    }

    pub fn local_address(&self) -> String {
        self.bootstrap.local_address().to_string()
    }

    pub fn publish_up_members(
        &self,
        members: impl IntoIterator<Item = UniqueAddress>,
    ) -> Result<(), ActorError> {
        self.publisher
            .tell(ClusterEventPublisherMsg::PublishChanges(
                Gossip::from_members(members.into_iter().map(up_member)),
            ))
            .map_err(|error| ActorError::Message(error.reason().to_string()))
    }

    pub fn connector_snapshot(
        &self,
        timeout: Duration,
    ) -> Result<ClusterToolsTcpPeerConnectorSnapshot, Box<dyn Error>> {
        let id = SNAPSHOT_ID.fetch_add(1, Ordering::Relaxed);
        let (reply_to, replies) =
            spawn_one_shot_reply(&self.system, format!("cluster-tools-snapshot-{id}"))?;
        self.bootstrap
            .connector()
            .tell(ClusterToolsTcpPeerConnectorMsg::Snapshot { reply_to })?;
        Ok(replies.recv_timeout(timeout)?)
    }

    pub fn wait_for_route_count(
        &self,
        route_count: usize,
        timeout: Duration,
    ) -> Result<ClusterToolsTcpPeerConnectorSnapshot, Box<dyn Error>> {
        let deadline = Instant::now() + timeout;
        loop {
            let snapshot = self.connector_snapshot(timeout)?;
            if snapshot.route_count == route_count {
                return Ok(snapshot);
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "timed out waiting for {route_count} cluster-tools peer route(s)"
                )
                .into());
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    pub fn send_status_to(
        &self,
        target: &ClusterToolsTcpExampleNode,
        message: PubSubStatus,
    ) -> Result<(), Box<dyn Error>> {
        let outbound = PubSubRemoteDeliveryOutbound::<PubSubStatus>::from_arc(
            target.self_node().clone(),
            self.registry.clone(),
            Arc::new(self.association_cache.clone()) as Arc<dyn RemoteOutbound>,
        );
        outbound.tell(LocalPubSubMsg::Publish {
            topic: TopicName::new(EXAMPLE_PUBSUB_TOPIC),
            message,
            mode: TopicPublishMode::Broadcast,
            reply_to: None,
        })?;
        Ok(())
    }

    pub fn wait_for_status_count(&self, len: usize, timeout: Duration) -> Vec<PubSubStatus> {
        self.status_recorder.wait_for_len(len, timeout)
    }

    pub fn shutdown(self, timeout: Duration) -> Result<(), ActorError> {
        self.system
            .run_coordinated_shutdown("cluster-tools tcp example complete", timeout)
    }
}

pub fn bind_two_nodes()
-> Result<(ClusterToolsTcpExampleNode, ClusterToolsTcpExampleNode), Box<dyn Error>> {
    Ok((
        ClusterToolsTcpExampleNode::bind("tools-node-a", 1, 11, "tools-node-a-peers")?,
        ClusterToolsTcpExampleNode::bind("tools-node-b", 2, 22, "tools-node-b-peers")?,
    ))
}

pub fn bind_three_nodes() -> Result<
    (
        ClusterToolsTcpExampleNode,
        ClusterToolsTcpExampleNode,
        ClusterToolsTcpExampleNode,
    ),
    Box<dyn Error>,
> {
    Ok((
        ClusterToolsTcpExampleNode::bind("tools-three-node-a", 1, 11, "tools-three-node-a-peers")?,
        ClusterToolsTcpExampleNode::bind("tools-three-node-b", 2, 22, "tools-three-node-b-peers")?,
        ClusterToolsTcpExampleNode::bind("tools-three-node-c", 3, 33, "tools-three-node-c-peers")?,
    ))
}

fn cluster_tools_registry() -> Result<Arc<Registry>, Box<dyn Error>> {
    let mut registry = Registry::new();
    register_cluster_tools_protocol_codecs(&mut registry)?;
    Ok(Arc::new(registry))
}

fn inbound_for(
    system: &ActorSystem,
    registry: Arc<Registry>,
    self_node: UniqueAddress,
    mediator_slot: Arc<Mutex<Option<ActorRef<DistributedPubSubMediatorMsg<PubSubStatus>>>>>,
) -> ClusterToolsSystemInbound<PubSubStatus> {
    let gossip_node = self_node.clone();
    let gossip = system
        .spawn(
            "pubsub-gossip",
            Props::new(move || PubSubGossipActor::new(gossip_node.clone())),
        )
        .expect("cluster-tools example pubsub gossip actor should spawn");
    let mediator_node = self_node.clone();
    let mediator = system
        .spawn(
            "pubsub-mediator",
            Props::new(move || {
                DistributedPubSubMediatorActor::<PubSubStatus>::new(mediator_node.clone())
            }),
        )
        .expect("cluster-tools example pubsub mediator actor should spawn");
    *mediator_slot
        .lock()
        .expect("cluster-tools example mediator slot poisoned") = Some(mediator.clone());
    let singleton_node = self_node.clone();
    let singleton = system
        .spawn(
            "singleton-manager",
            Props::new(move || SingletonManagerActor::new(singleton_node.clone())),
        )
        .expect("cluster-tools example singleton manager actor should spawn");

    ClusterToolsSystemInbound::new(self_node.clone())
        .with_pubsub_gossip(PubSubGossipWireInbound::new(
            self_node.clone(),
            registry.clone(),
            gossip as ActorRef<PubSubGossipMsg>,
        ))
        .with_pubsub_delivery(PubSubRemoteDeliveryInbound::new(
            self_node.clone(),
            registry.clone(),
            mediator as ActorRef<DistributedPubSubMediatorMsg<PubSubStatus>>,
        ))
        .with_singleton_manager(SingletonManagerRemoteInbound::new(
            self_node,
            registry,
            singleton as ActorRef<SingletonManagerMsg>,
        ))
}

fn subscribe_status_recorder(
    system: &ActorSystem,
    mediator_slot: &Arc<Mutex<Option<ActorRef<DistributedPubSubMediatorMsg<PubSubStatus>>>>>,
    subscriber: ActorRef<PubSubStatus>,
) -> Result<(), Box<dyn Error>> {
    let mediator = mediator_slot
        .lock()
        .expect("cluster-tools example mediator slot poisoned")
        .clone()
        .ok_or("cluster-tools example mediator was not installed")?;
    let id = SNAPSHOT_ID.fetch_add(1, Ordering::Relaxed);
    let (reply_to, replies) = spawn_one_shot_reply::<PubSubSubscribeAck>(
        system,
        format!("pubsub-status-subscribe-{id}"),
    )?;
    mediator.tell(DistributedPubSubMediatorMsg::Subscribe {
        topic: TopicName::new(EXAMPLE_PUBSUB_TOPIC),
        subscriber,
        reply_to: Some(reply_to),
    })?;
    replies.recv_timeout(Duration::from_secs(1))?;
    Ok(())
}

fn up_member(node: UniqueAddress) -> Member {
    Member::new(node, vec![]).with_status(MemberStatus::Up)
}
