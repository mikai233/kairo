use std::error::Error;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use kairo::actor::{ActorError, ActorRef, ActorSystem, Address, Props};
use kairo::cluster::{
    Cluster, ClusterEventPublisher, ClusterEventPublisherMsg, ClusterSystemInbound,
    ClusterTcpPeerBootstrap, ClusterTcpPeerBootstrapIdentity, ClusterTcpPeerBootstrapSettings,
    ClusterTcpPeerConnectorMsg, ClusterTcpPeerConnectorSettings, ClusterTcpPeerConnectorSnapshot,
    Gossip, Member, MemberStatus, UniqueAddress,
};
use kairo::remote::RemoteSettings;

use crate::reply::spawn_one_shot_reply;

static SNAPSHOT_ID: AtomicU64 = AtomicU64::new(0);

pub struct ClusterTcpExampleNode {
    system: ActorSystem,
    publisher: ActorRef<ClusterEventPublisherMsg>,
    bootstrap: ClusterTcpPeerBootstrap,
}

impl ClusterTcpExampleNode {
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
        let settings = ClusterTcpPeerBootstrapSettings::new(RemoteSettings::new("127.0.0.1", 0))
            .with_connector_name(connector_name)
            .with_connector_settings(
                ClusterTcpPeerConnectorSettings::new(Duration::from_millis(100))?
                    .with_automatic_retry_ticks(false),
            )
            .with_shutdown_timeout(Duration::from_secs(1));
        let bootstrap = ClusterTcpPeerBootstrap::bind_and_spawn(
            &system,
            cluster,
            ClusterTcpPeerBootstrapIdentity::new(node_uid, system_uid),
            settings,
            |self_node, _cache| ClusterSystemInbound::new(self_node),
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
    ) -> Result<ClusterTcpPeerConnectorSnapshot, Box<dyn Error>> {
        let id = SNAPSHOT_ID.fetch_add(1, Ordering::Relaxed);
        let (reply_to, replies) =
            spawn_one_shot_reply(&self.system, format!("cluster-snapshot-{id}"))?;
        self.bootstrap
            .connector()
            .tell(ClusterTcpPeerConnectorMsg::Snapshot { reply_to })?;
        Ok(replies.recv_timeout(timeout)?)
    }

    pub fn wait_for_route_count(
        &self,
        route_count: usize,
        timeout: Duration,
    ) -> Result<ClusterTcpPeerConnectorSnapshot, Box<dyn Error>> {
        let deadline = Instant::now() + timeout;
        loop {
            let snapshot = self.connector_snapshot(timeout)?;
            if snapshot.route_count == route_count {
                return Ok(snapshot);
            }
            if Instant::now() >= deadline {
                return Err(
                    format!("timed out waiting for {route_count} cluster peer route(s)").into(),
                );
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    pub fn shutdown(self, timeout: Duration) -> Result<(), ActorError> {
        self.system
            .run_coordinated_shutdown("cluster tcp example complete", timeout)
    }
}

pub fn bind_two_nodes() -> Result<(ClusterTcpExampleNode, ClusterTcpExampleNode), Box<dyn Error>> {
    Ok((
        ClusterTcpExampleNode::bind("cluster-node-a", 1, 11, "cluster-node-a-peers")?,
        ClusterTcpExampleNode::bind("cluster-node-b", 2, 22, "cluster-node-b-peers")?,
    ))
}

pub fn bind_three_nodes() -> Result<
    (
        ClusterTcpExampleNode,
        ClusterTcpExampleNode,
        ClusterTcpExampleNode,
    ),
    Box<dyn Error>,
> {
    Ok((
        ClusterTcpExampleNode::bind("cluster-three-node-a", 1, 11, "cluster-three-node-a-peers")?,
        ClusterTcpExampleNode::bind("cluster-three-node-b", 2, 22, "cluster-three-node-b-peers")?,
        ClusterTcpExampleNode::bind("cluster-three-node-c", 3, 33, "cluster-three-node-c-peers")?,
    ))
}

fn up_member(node: UniqueAddress) -> Member {
    Member::new(node, vec![]).with_status(MemberStatus::Up)
}
