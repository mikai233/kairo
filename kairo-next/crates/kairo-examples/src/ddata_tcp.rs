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
    DataEnvelope, DeltaReplicatedData, GCounter, GCounterCodec, GetResponse, ReadConsistency,
    ReplicaId, ReplicatorActor, ReplicatorActorMsg, ReplicatorKey, ReplicatorRead,
    ReplicatorRemoteAssociationCacheOutbound, ReplicatorRemoteEnvelopeOutbound,
    ReplicatorRemoteReplyError, ReplicatorRemoteReplyReceiver, ReplicatorRemoteRequestError,
    ReplicatorRemoteRequestInbound, ReplicatorRemoteRequestReceiver, ReplicatorRemoteTarget,
    ReplicatorTcpPeerBootstrap, ReplicatorTcpPeerBootstrapSettings, ReplicatorTcpPeerConnectorMsg,
    ReplicatorTcpPeerConnectorSettings, ReplicatorTcpPeerConnectorSnapshot,
    ReplicatorTcpPeerRuntime, ReplicatorWireCodecs, encode_write, register_ddata_protocol_codecs,
    replicator_actor_ref_for,
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

#[derive(Default)]
struct DeferredReplicatorRequests {
    inner: Mutex<Option<Arc<dyn ReplicatorRemoteRequestReceiver>>>,
}

impl DeferredReplicatorRequests {
    fn set(&self, receiver: Arc<dyn ReplicatorRemoteRequestReceiver>) {
        *self.inner.lock().expect("deferred requests poisoned") = Some(receiver);
    }
}

impl ReplicatorRemoteRequestReceiver for DeferredReplicatorRequests {
    fn receive_request_from(
        &self,
        from: ReplicaId,
        envelope: RemoteEnvelope,
    ) -> Result<(), ReplicatorRemoteRequestError> {
        let receiver = self
            .inner
            .lock()
            .expect("deferred requests poisoned")
            .as_ref()
            .cloned()
            .ok_or_else(|| {
                ReplicatorRemoteRequestError::Send(
                    "deferred remote request receiver is not initialized".to_string(),
                )
            })?;
        receiver.receive_request_from(from, envelope)
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
    replicator: Option<ActorRef<ReplicatorActorMsg<GCounter>>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DDataTcpShutdownObservation {
    pub route_count_before_shutdown: usize,
    pub route_count_after_shutdown: usize,
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
            replicator: None,
        })
    }

    pub fn bind_with_replicator(
        system_name: &str,
        node_uid: u64,
        system_uid: u64,
        connector_name: &str,
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
        let deferred_requests = Arc::new(DeferredReplicatorRequests::default());
        let runtime = ReplicatorTcpPeerRuntime::bind(
            system_name,
            node_uid,
            system_uid,
            remote_replica,
            settings.runtime_settings().remote().clone(),
            deferred_requests.clone() as Arc<dyn ReplicatorRemoteRequestReceiver>,
            Arc::new(IgnoreReplicatorReplies) as Arc<dyn ReplicatorRemoteReplyReceiver>,
        )?;
        let association_cache = runtime.association_cache().clone();
        let remote_settings = runtime.runtime().settings().clone();
        let receiver_ref = replicator_actor_ref_for(system_name, &remote_settings)?;
        let replicator =
            system.spawn_system("ddata", Props::new(ReplicatorActor::<GCounter>::new))?;
        let inbound = ReplicatorRemoteRequestInbound::new(
            system.clone(),
            receiver_ref.clone(),
            Some(receiver_ref),
            registry.clone(),
            replicator.clone(),
            ReplicatorWireCodecs::new(Arc::new(GCounterCodec), Arc::new(GCounterCodec)),
            ReplicatorRemoteAssociationCacheOutbound::new(association_cache.clone()),
        );
        deferred_requests.set(Arc::new(inbound) as Arc<dyn ReplicatorRemoteRequestReceiver>);
        let bootstrap =
            ReplicatorTcpPeerBootstrap::spawn_with_runtime(&system, cluster, runtime, settings)?;

        Ok(Self {
            system,
            publisher,
            bootstrap,
            registry,
            association_cache,
            remote_settings,
            request_recorder: Arc::new(RecordingReplicatorRequests::default()),
            replicator: Some(replicator),
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
            let Some(remaining) = remaining_until(deadline) else {
                return Err(format!(
                    "timed out waiting for {route_count} distributed-data peer route(s): no snapshot observed"
                )
                .into());
            };
            let snapshot = self.connector_snapshot(remaining)?;
            if snapshot.route_count == route_count {
                return Ok(snapshot);
            }
            if !sleep_until_next_poll(deadline) {
                return Err(format!(
                    "timed out waiting for {route_count} distributed-data peer route(s): {snapshot:?}"
                )
                .into());
            }
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

    pub fn send_counter_write_to(
        &self,
        target: &DDataTcpExampleNode,
        key: impl Into<String>,
        amount: u128,
    ) -> Result<(), Box<dyn Error>> {
        let sender_ref = replicator_actor_ref_for(self.system.name(), &self.remote_settings)?;
        let target_ref = replicator_actor_ref_for(target.system.name(), &target.remote_settings)?;
        let outbound = ReplicatorRemoteEnvelopeOutbound::new(
            ReplicatorRemoteTarget::new(ReplicaId::from(target.self_node()), target_ref),
            Some(sender_ref),
            self.registry.clone(),
            ReplicatorRemoteAssociationCacheOutbound::new(self.association_cache.clone()),
        );
        let key = ReplicatorKey::new(key);
        let counter = GCounter::new()
            .increment(ReplicaId::from(self.self_node()), amount)?
            .reset_delta();
        outbound.send(&encode_write(
            &key,
            Some(ReplicaId::from(self.self_node())),
            &DataEnvelope::new(counter),
            &GCounterCodec,
        )?)?;
        Ok(())
    }

    pub fn wait_for_counter_value(
        &self,
        key: impl AsRef<str>,
        expected: u128,
        timeout: Duration,
    ) -> Result<(), Box<dyn Error>> {
        let replicator = self
            .replicator
            .as_ref()
            .ok_or("node was not bound with a local ReplicatorActor")?;
        let key = ReplicatorKey::new(key.as_ref());
        let deadline = Instant::now() + timeout;
        loop {
            let Some(remaining) = remaining_until(deadline) else {
                return Err(format!(
                    "timed out waiting for distributed-data counter `{}` to reach {expected}",
                    key.as_str()
                )
                .into());
            };
            let id = SNAPSHOT_ID.fetch_add(1, Ordering::Relaxed);
            let (reply_to, replies) =
                spawn_one_shot_reply(&self.system, format!("ddata-counter-read-{id}"))?;
            replicator.tell(ReplicatorActorMsg::Get {
                key: key.clone(),
                consistency: ReadConsistency::local(),
                reply_to,
            })?;
            match replies.recv_timeout(remaining.min(Duration::from_millis(100))) {
                Ok(GetResponse::Success { data, .. }) if data.value()? == expected => return Ok(()),
                Ok(_) | Err(_) => {}
            }
            if !sleep_until_next_poll(deadline) {
                return Err(format!(
                    "timed out waiting for distributed-data counter `{}` to reach {expected}",
                    key.as_str()
                )
                .into());
            }
        }
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
        let deadline = shutdown_deadline(timeout);
        let connector = self.bootstrap.connector().clone();
        let route_count_before_shutdown = self
            .connector_snapshot(remaining_shutdown_time(deadline)?)
            .map_err(|error| ActorError::Message(error.to_string()))?
            .route_count;
        self.system.run_coordinated_shutdown(
            "ddata tcp example complete",
            remaining_shutdown_time(deadline)?,
        )?;
        let route_count_after_shutdown = self.association_cache.route_count();
        Ok(DDataTcpShutdownObservation {
            route_count_before_shutdown,
            route_count_after_shutdown,
            connector_stopped: connector.wait_for_stop(remaining_shutdown_time(deadline)?),
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
