use std::error::Error;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use kairo::actor::{ActorError, ActorRef, ActorSystem, Address, Props};
use kairo::cluster::{
    Cluster, ClusterEventPublisher, ClusterEventPublisherMsg, Gossip, Member, MemberStatus,
    UniqueAddress,
};
use kairo::distributed_data::{
    ReplicaId, ReplicatorRead, ReplicatorRemoteAssociationCacheOutbound,
    ReplicatorRemoteEnvelopeOutbound, ReplicatorRemoteReplyError, ReplicatorRemoteReplyReceiver,
    ReplicatorRemoteRequestError, ReplicatorRemoteRequestReceiver, ReplicatorRemoteTarget,
    ReplicatorTcpPeerBootstrap, ReplicatorTcpPeerBootstrapSettings, ReplicatorTcpPeerConnectorMsg,
    ReplicatorTcpPeerConnectorSettings, ReplicatorTcpPeerConnectorSnapshot,
    ReplicatorTcpPeerRuntime, register_ddata_protocol_codecs, replicator_actor_ref_for,
};
use kairo::remote::{RemoteAssociationCache, RemoteSettings};
use kairo::serialization::Registry;
use kairo::serialization::RemoteEnvelope;

use crate::reply::spawn_one_shot_reply;

static SNAPSHOT_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Default)]
pub struct IgnoreReplicatorRequests;

impl ReplicatorRemoteRequestReceiver for IgnoreReplicatorRequests {
    fn receive_request_from(
        &self,
        _from: ReplicaId,
        _envelope: RemoteEnvelope,
    ) -> Result<(), ReplicatorRemoteRequestError> {
        Ok(())
    }
}

#[derive(Default)]
pub struct IgnoreReplicatorReplies;

impl ReplicatorRemoteReplyReceiver for IgnoreReplicatorReplies {
    fn receive_reply_from(
        &self,
        _from: ReplicaId,
        _envelope: RemoteEnvelope,
    ) -> Result<(), ReplicatorRemoteReplyError> {
        Ok(())
    }
}

#[derive(Default)]
pub struct RecordingReplicatorRequests {
    received: Mutex<Vec<(ReplicaId, RemoteEnvelope)>>,
    changed: Condvar,
}

impl RecordingReplicatorRequests {
    pub fn wait_for_len(&self, len: usize, timeout: Duration) -> Vec<(ReplicaId, RemoteEnvelope)> {
        let deadline = Instant::now() + timeout;
        let mut received = self.received.lock().expect("requests poisoned");
        while received.len() < len {
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                break;
            };
            let (next_received, wait) = self
                .changed
                .wait_timeout(received, remaining)
                .expect("requests poisoned");
            received = next_received;
            if wait.timed_out() {
                break;
            }
        }
        received.clone()
    }
}

impl ReplicatorRemoteRequestReceiver for RecordingReplicatorRequests {
    fn receive_request_from(
        &self,
        from: ReplicaId,
        envelope: RemoteEnvelope,
    ) -> Result<(), ReplicatorRemoteRequestError> {
        self.received
            .lock()
            .expect("requests poisoned")
            .push((from, envelope));
        self.changed.notify_all();
        Ok(())
    }
}

pub struct DDataTcpExampleNode {
    system: ActorSystem,
    publisher: ActorRef<ClusterEventPublisherMsg>,
    bootstrap: ReplicatorTcpPeerBootstrap,
    registry: Arc<Registry>,
    association_cache: RemoteAssociationCache,
    remote_settings: RemoteSettings,
    request_recorder: Arc<RecordingReplicatorRequests>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DDataTcpShutdownObservation {
    pub route_count_before_shutdown: usize,
    pub connector_stopped: bool,
}

impl DDataTcpExampleNode {
    pub fn bind(
        system_name: &str,
        node_uid: u64,
        system_uid: u64,
        connector_name: &str,
    ) -> Result<Self, Box<dyn Error>> {
        Self::bind_with_remote_replica(
            system_name,
            node_uid,
            system_uid,
            connector_name,
            ReplicaId::new("example-peer"),
        )
    }

    pub fn bind_with_remote_replica(
        system_name: &str,
        node_uid: u64,
        system_uid: u64,
        connector_name: &str,
        remote_replica: ReplicaId,
    ) -> Result<Self, Box<dyn Error>> {
        Self::bind_with_requests(
            system_name,
            node_uid,
            system_uid,
            connector_name,
            Arc::new(RecordingReplicatorRequests::default()),
            remote_replica,
        )
    }

    fn bind_with_requests(
        system_name: &str,
        node_uid: u64,
        system_uid: u64,
        connector_name: &str,
        request_recorder: Arc<RecordingReplicatorRequests>,
        remote_replica: ReplicaId,
    ) -> Result<Self, Box<dyn Error>> {
        let system = ActorSystem::builder(system_name).build()?;
        let publisher_node = UniqueAddress::new(Address::local(system_name), node_uid);
        let publisher = system.spawn(
            "cluster-events",
            Props::new(move || ClusterEventPublisher::new(publisher_node.clone())),
        )?;
        let cluster = Cluster::new(publisher.clone());
        let registry = ddata_registry()?;
        let settings = ReplicatorTcpPeerBootstrapSettings::new(RemoteSettings::new("127.0.0.1", 0))
            .with_connector_name(connector_name)
            .with_connector_settings(
                ReplicatorTcpPeerConnectorSettings::new(Duration::from_millis(100))?
                    .with_automatic_retry_ticks(false),
            )
            .with_shutdown_timeout(Duration::from_secs(1));
        let runtime = ReplicatorTcpPeerRuntime::bind(
            system_name,
            node_uid,
            system_uid,
            remote_replica,
            settings.runtime_settings().remote().clone(),
            request_recorder.clone() as Arc<dyn ReplicatorRemoteRequestReceiver>,
            Arc::new(IgnoreReplicatorReplies) as Arc<dyn ReplicatorRemoteReplyReceiver>,
        )?;
        let association_cache = runtime.association_cache().clone();
        let remote_settings = runtime.runtime().settings().clone();
        let bootstrap =
            ReplicatorTcpPeerBootstrap::spawn_with_runtime(&system, cluster, runtime, settings)?;

        Ok(Self {
            system,
            publisher,
            bootstrap,
            registry,
            association_cache,
            remote_settings,
            request_recorder,
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
    ) -> Result<ReplicatorTcpPeerConnectorSnapshot, Box<dyn Error>> {
        let id = SNAPSHOT_ID.fetch_add(1, Ordering::Relaxed);
        let (reply_to, replies) =
            spawn_one_shot_reply(&self.system, format!("ddata-snapshot-{id}"))?;
        self.bootstrap
            .connector()
            .tell(ReplicatorTcpPeerConnectorMsg::Snapshot { reply_to })?;
        Ok(replies.recv_timeout(timeout)?)
    }

    pub fn wait_for_route_count(
        &self,
        route_count: usize,
        timeout: Duration,
    ) -> Result<ReplicatorTcpPeerConnectorSnapshot, Box<dyn Error>> {
        let deadline = Instant::now() + timeout;
        loop {
            let snapshot = self.connector_snapshot(timeout)?;
            if snapshot.route_count == route_count {
                return Ok(snapshot);
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "timed out waiting for {route_count} distributed-data peer route(s)"
                )
                .into());
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    pub fn send_read_to(
        &self,
        target: &DDataTcpExampleNode,
        key: impl Into<String>,
    ) -> Result<(), Box<dyn Error>> {
        let sender_ref = replicator_actor_ref_for(self.system.name(), &self.remote_settings)?;
        let target_ref = replicator_actor_ref_for(target.system.name(), &target.remote_settings)?;
        let outbound = ReplicatorRemoteEnvelopeOutbound::new(
            ReplicatorRemoteTarget::new(ReplicaId::from(target.self_node()), target_ref),
            Some(sender_ref),
            self.registry.clone(),
            ReplicatorRemoteAssociationCacheOutbound::new(self.association_cache.clone()),
        );
        outbound.send(&ReplicatorRead {
            key: key.into(),
            from: Some(ReplicaId::from(self.self_node())),
        })?;
        Ok(())
    }

    pub fn wait_for_request_count(
        &self,
        len: usize,
        timeout: Duration,
    ) -> Vec<(ReplicaId, RemoteEnvelope)> {
        self.request_recorder.wait_for_len(len, timeout)
    }

    pub fn decode_read(&self, envelope: RemoteEnvelope) -> Result<ReplicatorRead, Box<dyn Error>> {
        Ok(self
            .registry
            .deserialize::<ReplicatorRead>(envelope.message)?)
    }

    pub fn shutdown(self, timeout: Duration) -> Result<(), ActorError> {
        self.system
            .run_coordinated_shutdown("ddata tcp example complete", timeout)
    }

    pub fn shutdown_with_observation(
        self,
        timeout: Duration,
    ) -> Result<DDataTcpShutdownObservation, ActorError> {
        let connector = self.bootstrap.connector().clone();
        let route_count_before_shutdown = self
            .connector_snapshot(timeout)
            .map_err(|error| ActorError::Message(error.to_string()))?
            .route_count;
        self.system
            .run_coordinated_shutdown("ddata tcp example complete", timeout)?;
        Ok(DDataTcpShutdownObservation {
            route_count_before_shutdown,
            connector_stopped: connector.wait_for_stop(timeout),
        })
    }
}

pub fn bind_two_nodes() -> Result<(DDataTcpExampleNode, DDataTcpExampleNode), Box<dyn Error>> {
    Ok((
        DDataTcpExampleNode::bind_with_remote_replica(
            "ddata-node-a",
            1,
            11,
            "ddata-node-a-peers",
            ReplicaId::new("ddata-node-b"),
        )?,
        DDataTcpExampleNode::bind_with_remote_replica(
            "ddata-node-b",
            2,
            22,
            "ddata-node-b-peers",
            ReplicaId::new("ddata-node-a"),
        )?,
    ))
}

pub fn bind_three_nodes() -> Result<
    (
        DDataTcpExampleNode,
        DDataTcpExampleNode,
        DDataTcpExampleNode,
    ),
    Box<dyn Error>,
> {
    Ok((
        DDataTcpExampleNode::bind("ddata-three-node-a", 1, 11, "ddata-three-node-a-peers")?,
        DDataTcpExampleNode::bind("ddata-three-node-b", 2, 22, "ddata-three-node-b-peers")?,
        DDataTcpExampleNode::bind("ddata-three-node-c", 3, 33, "ddata-three-node-c-peers")?,
    ))
}

fn up_member(node: UniqueAddress) -> Member {
    Member::new(node, vec![]).with_status(MemberStatus::Up)
}

fn ddata_registry() -> Result<Arc<Registry>, Box<dyn Error>> {
    let mut registry = Registry::new();
    register_ddata_protocol_codecs(&mut registry)?;
    Ok(Arc::new(registry))
}
