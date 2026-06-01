use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use kairo::actor::{ActorError, ActorRef, ActorSystem, Address, Props};
use kairo::cluster::{
    Cluster, ClusterEventPublisher, ClusterEventPublisherMsg, Gossip, Member, MemberStatus,
    UniqueAddress,
};
use kairo::cluster_tools::{
    ClusterToolsSystemInbound, ClusterToolsTcpPeerBootstrap, ClusterToolsTcpPeerBootstrapSettings,
    ClusterToolsTcpPeerConnectorMsg, ClusterToolsTcpPeerConnectorSettings,
    ClusterToolsTcpPeerConnectorSnapshot, DistributedPubSubMediatorActor,
    DistributedPubSubMediatorMsg, PubSubGossipActor, PubSubGossipMsg, PubSubGossipWireInbound,
    PubSubRemoteDeliveryInbound, PubSubStatus, SingletonManagerActor, SingletonManagerMsg,
    SingletonManagerRemoteInbound, register_cluster_tools_protocol_codecs,
};
use kairo::remote::RemoteSettings;
use kairo::serialization::Registry;

use crate::reply::spawn_one_shot_reply;

static SNAPSHOT_ID: AtomicU64 = AtomicU64::new(0);

pub struct ClusterToolsTcpExampleNode {
    system: ActorSystem,
    publisher: ActorRef<ClusterEventPublisherMsg>,
    bootstrap: ClusterToolsTcpPeerBootstrap<PubSubStatus>,
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
        let settings = ClusterToolsTcpPeerBootstrapSettings::new()
            .with_connector_name(connector_name)
            .with_connector_settings(
                ClusterToolsTcpPeerConnectorSettings::new(Duration::from_millis(100))?
                    .with_automatic_retry_ticks(false),
            )
            .with_shutdown_timeout(Duration::from_secs(1));
        let bootstrap = ClusterToolsTcpPeerBootstrap::bind_and_spawn(
            &system,
            cluster,
            node_uid,
            system_uid,
            RemoteSettings::new("127.0.0.1", 0),
            settings,
            move |self_node| inbound_for(&inbound_system, registry, self_node),
        )?;

        Ok(Self {
            system,
            publisher,
            bootstrap,
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

fn up_member(node: UniqueAddress) -> Member {
    Member::new(node, vec![]).with_status(MemberStatus::Up)
}
