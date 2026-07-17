use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use kairo::actor::{Actor, ActorError, ActorRef, ActorResult, ActorSystem, Address, Props};
use kairo::cluster::{
    Cluster, ClusterEventPublisher, ClusterEventPublisherMsg, ClusterMembershipMsg,
    ClusterMembershipRemoteEnvelopeOutbound, ClusterMembershipWireInbound,
    ClusterMembershipWireOutbound, ClusterSystemInbound, ClusterTcpPeerBootstrap,
    ClusterTcpPeerBootstrapSettings, ClusterTcpPeerConnectorMsg, ClusterTcpPeerConnectorSettings,
    ClusterTcpPeerConnectorSnapshot, ClusterTcpPeerRuntime, Gossip, Join, Member, MemberStatus,
    UniqueAddress, register_cluster_protocol_codecs,
};
use kairo::remote::{RemoteAssociationCache, RemoteOutbound, RemoteSettings};
use kairo::serialization::Registry;

use crate::reply::spawn_one_shot_reply;

static SNAPSHOT_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Default)]
pub struct RecordingClusterJoins {
    received: Mutex<Vec<Join>>,
    changed: Condvar,
}

impl RecordingClusterJoins {
    pub fn wait_for_len(&self, len: usize, timeout: Duration) -> Vec<Join> {
        let deadline = Instant::now() + timeout;
        let mut received = self.received.lock().expect("cluster joins poisoned");
        while received.len() < len {
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                break;
            };
            let (next_received, wait) = self
                .changed
                .wait_timeout(received, remaining)
                .expect("cluster joins poisoned");
            received = next_received;
            if wait.timed_out() {
                break;
            }
        }
        received.clone()
    }
}

struct RecordingClusterMembershipActor {
    joins: Arc<RecordingClusterJoins>,
}

impl RecordingClusterMembershipActor {
    fn new(joins: Arc<RecordingClusterJoins>) -> Self {
        Self { joins }
    }
}

impl Actor for RecordingClusterMembershipActor {
    type Msg = ClusterMembershipMsg;

    fn receive(
        &mut self,
        _ctx: &mut kairo::actor::Context<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorResult {
        if let ClusterMembershipMsg::Join { join, .. } = msg {
            self.joins
                .received
                .lock()
                .expect("cluster joins poisoned")
                .push(join);
            self.joins.changed.notify_all();
        }
        Ok(())
    }
}

pub struct ClusterTcpExampleNode {
    system: ActorSystem,
    publisher: ActorRef<ClusterEventPublisherMsg>,
    bootstrap: ClusterTcpPeerBootstrap,
    registry: Arc<Registry>,
    association_cache: RemoteAssociationCache,
    join_recorder: Arc<RecordingClusterJoins>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterTcpShutdownObservation {
    pub route_count_before_shutdown: usize,
    pub route_count_after_shutdown: usize,
    pub connector_stopped: bool,
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
        let registry = cluster_registry()?;
        let inbound_registry = registry.clone();
        let join_recorder = Arc::new(RecordingClusterJoins::default());
        let membership = system.spawn(
            "cluster-membership-recorder",
            Props::new({
                let join_recorder = join_recorder.clone();
                move || RecordingClusterMembershipActor::new(join_recorder.clone())
            }),
        )?;
        let settings = ClusterTcpPeerBootstrapSettings::new(RemoteSettings::new("127.0.0.1", 0))
            .with_connector_name(connector_name)
            .with_connector_settings(
                ClusterTcpPeerConnectorSettings::new(Duration::from_millis(100))?
                    .with_automatic_retry_ticks(false),
            )
            .with_shutdown_timeout(Duration::from_secs(1));
        let runtime = ClusterTcpPeerRuntime::bind(
            system_name,
            node_uid,
            system_uid,
            settings.remote_settings().clone(),
            move |self_node, _cache| {
                ClusterSystemInbound::new(self_node.clone()).with_membership(
                    ClusterMembershipWireInbound::new(
                        self_node,
                        inbound_registry,
                        membership as ActorRef<ClusterMembershipMsg>,
                    ),
                )
            },
        )?;
        let association_cache = runtime.association_cache().clone();
        let bootstrap =
            ClusterTcpPeerBootstrap::spawn_with_runtime(&system, cluster, runtime, settings)?;

        Ok(Self {
            system,
            publisher,
            bootstrap,
            registry,
            association_cache,
            join_recorder,
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
            let Some(remaining) = remaining_until(deadline) else {
                return Err(format!(
                    "timed out waiting for {route_count} cluster peer route(s): no snapshot observed"
                )
                .into());
            };
            let snapshot = self.connector_snapshot(remaining)?;
            if snapshot.route_count == route_count {
                return Ok(snapshot);
            }
            if !sleep_until_next_poll(deadline) {
                return Err(format!(
                    "timed out waiting for {route_count} cluster peer route(s): {snapshot:?}"
                )
                .into());
            }
        }
    }

    pub fn send_join_to(
        &self,
        target: &ClusterTcpExampleNode,
        roles: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<(), Box<dyn Error>> {
        let outbound = ClusterMembershipWireOutbound::new(
            target.self_node().clone(),
            self.registry.clone(),
            ClusterMembershipRemoteEnvelopeOutbound::from_arc(Arc::new(
                self.association_cache.clone(),
            )
                as Arc<dyn RemoteOutbound>),
        );
        outbound.send_membership(ClusterMembershipMsg::Join {
            join: Join::new(
                self.self_node().clone(),
                roles.into_iter().map(Into::into).collect(),
            ),
            reply_to: None,
        })?;
        Ok(())
    }

    pub fn wait_for_join_count(&self, len: usize, timeout: Duration) -> Vec<Join> {
        self.join_recorder.wait_for_len(len, timeout)
    }

    pub fn shutdown(self, timeout: Duration) -> Result<(), ActorError> {
        self.system
            .run_coordinated_shutdown("cluster tcp example complete", timeout)
    }

    pub fn shutdown_with_observation(
        self,
        timeout: Duration,
    ) -> Result<ClusterTcpShutdownObservation, ActorError> {
        let deadline = shutdown_deadline(timeout);
        let connector = self.bootstrap.connector().clone();
        let route_count_before_shutdown = self
            .connector_snapshot(remaining_shutdown_time(deadline)?)
            .map_err(|error| ActorError::Message(error.to_string()))?
            .route_count;
        self.system.run_coordinated_shutdown(
            "cluster tcp example complete",
            remaining_shutdown_time(deadline)?,
        )?;
        let route_count_after_shutdown = self.association_cache.route_count();
        Ok(ClusterTcpShutdownObservation {
            route_count_before_shutdown,
            route_count_after_shutdown,
            connector_stopped: connector.wait_for_stop(remaining_shutdown_time(deadline)?),
        })
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

fn cluster_registry() -> Result<Arc<Registry>, Box<dyn Error>> {
    let mut registry = Registry::new();
    register_cluster_protocol_codecs(&mut registry)?;
    Ok(Arc::new(registry))
}

fn remaining_until(deadline: Instant) -> Option<Duration> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    (!remaining.is_zero()).then_some(remaining)
}

fn sleep_until_next_poll(deadline: Instant) -> bool {
    let Some(remaining) = remaining_until(deadline) else {
        return false;
    };
    thread::sleep(Duration::from_millis(10).min(remaining));
    true
}

fn shutdown_deadline(timeout: Duration) -> Instant {
    Instant::now()
        .checked_add(timeout)
        .unwrap_or_else(|| Instant::now() + Duration::from_secs(60 * 60 * 24 * 365))
}

fn remaining_shutdown_time(deadline: Instant) -> Result<Duration, ActorError> {
    deadline
        .checked_duration_since(Instant::now())
        .ok_or(ActorError::TerminationTimeout)
}
