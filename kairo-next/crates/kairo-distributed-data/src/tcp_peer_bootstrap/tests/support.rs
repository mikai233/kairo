use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use kairo_actor::{ActorRef, PHASE_BEFORE_CLUSTER_SHUTDOWN, Props};
use kairo_cluster::{
    ClusterEventPublisher, ClusterEventPublisherMsg, Gossip, Member, MemberStatus, UniqueAddress,
};
use kairo_remote::{RemoteAssociationCache, RemoteSettings};
use kairo_serialization::{Registry, RemoteEnvelope};
use kairo_testkit::{ActorSystemTestKit, TestProbe, await_assert};

use crate::{
    ReplicaId, ReplicatorRemoteAssociationCacheOutbound, ReplicatorRemoteEnvelopeOutbound,
    ReplicatorRemoteReplyError, ReplicatorRemoteReplyReceiver, ReplicatorRemoteRequestError,
    ReplicatorRemoteRequestReceiver, ReplicatorRemoteTarget, ReplicatorTcpPeerConnectorMsg,
    ReplicatorTcpPeerConnectorSnapshot, ReplicatorTcpPeerRuntime, register_ddata_protocol_codecs,
};

#[derive(Default)]
pub(super) struct IgnoreRequests;

impl ReplicatorRemoteRequestReceiver for IgnoreRequests {
    fn receive_request_from(
        &self,
        _from: ReplicaId,
        _envelope: RemoteEnvelope,
    ) -> Result<(), ReplicatorRemoteRequestError> {
        Ok(())
    }
}

#[derive(Default)]
pub(super) struct IgnoreReplies;

impl ReplicatorRemoteReplyReceiver for IgnoreReplies {
    fn receive_reply_from(
        &self,
        _from: ReplicaId,
        _envelope: RemoteEnvelope,
    ) -> Result<(), ReplicatorRemoteReplyError> {
        Ok(())
    }
}

#[derive(Default)]
pub(super) struct RecordingRequests {
    received: Mutex<Vec<(ReplicaId, RemoteEnvelope)>>,
    changed: Condvar,
}

impl RecordingRequests {
    pub(super) fn wait_for_len(
        &self,
        len: usize,
        timeout: Duration,
    ) -> Vec<(ReplicaId, RemoteEnvelope)> {
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

impl ReplicatorRemoteRequestReceiver for RecordingRequests {
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

pub(super) fn await_connector_no_routes(
    connector: &ActorRef<ReplicatorTcpPeerConnectorMsg>,
    snapshots: &TestProbe<ReplicatorTcpPeerConnectorSnapshot>,
) {
    await_assert(
        Duration::from_secs(1),
        Duration::from_millis(10),
        || -> Result<(), String> {
            connector
                .tell(ReplicatorTcpPeerConnectorMsg::Snapshot {
                    reply_to: snapshots.actor_ref(),
                })
                .map_err(|error| error.reason().to_string())?;
            let snapshot = snapshots
                .expect_msg(Duration::from_millis(100))
                .map_err(|error| error.to_string())?;
            if snapshot.route_count == 0 && snapshot.active_targets.is_empty() {
                Ok(())
            } else {
                Err(format!("unexpected connector snapshot: {snapshot:?}"))
            }
        },
    )
    .unwrap();
}

pub(super) fn await_connector_no_routes_or_pending(
    connector: &ActorRef<ReplicatorTcpPeerConnectorMsg>,
    snapshots: &TestProbe<ReplicatorTcpPeerConnectorSnapshot>,
) {
    await_assert(
        Duration::from_secs(1),
        Duration::from_millis(10),
        || -> Result<(), String> {
            connector
                .tell(ReplicatorTcpPeerConnectorMsg::Snapshot {
                    reply_to: snapshots.actor_ref(),
                })
                .map_err(|error| error.reason().to_string())?;
            let snapshot = snapshots
                .expect_msg(Duration::from_millis(100))
                .map_err(|error| error.to_string())?;
            if snapshot.route_count == 0
                && snapshot.active_targets.is_empty()
                && snapshot.pending_reconnects.is_empty()
            {
                Ok(())
            } else {
                Err(format!("unexpected connector snapshot: {snapshot:?}"))
            }
        },
    )
    .unwrap();
}

pub(super) fn await_connector_pending_reconnect(
    connector: &ActorRef<ReplicatorTcpPeerConnectorMsg>,
    snapshots: &TestProbe<ReplicatorTcpPeerConnectorSnapshot>,
    expected_peer: &UniqueAddress,
) {
    await_assert(
        Duration::from_secs(1),
        Duration::from_millis(10),
        || -> Result<(), String> {
            connector
                .tell(ReplicatorTcpPeerConnectorMsg::Snapshot {
                    reply_to: snapshots.actor_ref(),
                })
                .map_err(|error| error.reason().to_string())?;
            let snapshot = snapshots
                .expect_msg(Duration::from_millis(100))
                .map_err(|error| error.to_string())?;
            let has_expected_pending = snapshot
                .pending_reconnects
                .iter()
                .any(|pending| pending.target.node() == expected_peer);
            if snapshot.route_count == 0 && has_expected_pending {
                Ok(())
            } else {
                Err(format!("unexpected connector snapshot: {snapshot:?}"))
            }
        },
    )
    .unwrap();
}

pub(super) fn bind_runtime(
    system: &str,
    node_uid: u64,
    system_uid: u64,
    remote_replica: ReplicaId,
) -> ReplicatorTcpPeerRuntime {
    ReplicatorTcpPeerRuntime::bind(
        system,
        node_uid,
        system_uid,
        remote_replica,
        RemoteSettings::new("127.0.0.1", 0).with_connect_timeout(Duration::from_millis(10)),
        Arc::new(IgnoreRequests) as Arc<dyn ReplicatorRemoteRequestReceiver>,
        Arc::new(IgnoreReplies) as Arc<dyn ReplicatorRemoteReplyReceiver>,
    )
    .unwrap()
}

pub(super) fn bind_runtime_with_requests(
    system: &str,
    node_uid: u64,
    system_uid: u64,
    remote_replica: ReplicaId,
    requests: Arc<dyn ReplicatorRemoteRequestReceiver>,
) -> ReplicatorTcpPeerRuntime {
    ReplicatorTcpPeerRuntime::bind(
        system,
        node_uid,
        system_uid,
        remote_replica,
        RemoteSettings::new("127.0.0.1", 0).with_connect_timeout(Duration::from_millis(10)),
        requests,
        Arc::new(IgnoreReplies) as Arc<dyn ReplicatorRemoteReplyReceiver>,
    )
    .unwrap()
}

pub(super) fn registry() -> Arc<Registry> {
    let mut registry = Registry::new();
    register_ddata_protocol_codecs(&mut registry).unwrap();
    Arc::new(registry)
}

pub(super) fn outbound(
    target: ReplicaId,
    recipient: kairo_serialization::ActorRefWireData,
    sender: kairo_serialization::ActorRefWireData,
    registry: Arc<Registry>,
    cache: RemoteAssociationCache,
) -> ReplicatorRemoteEnvelopeOutbound {
    ReplicatorRemoteEnvelopeOutbound::new(
        ReplicatorRemoteTarget::new(target, recipient),
        Some(sender),
        registry,
        ReplicatorRemoteAssociationCacheOutbound::new(cache),
    )
}

pub(super) fn spawn_publisher(
    kit: &ActorSystemTestKit,
    name: &str,
    self_node: UniqueAddress,
) -> ActorRef<ClusterEventPublisherMsg> {
    kit.system()
        .spawn(
            name,
            Props::new(move || ClusterEventPublisher::new(self_node.clone())),
        )
        .unwrap()
}

pub(super) fn up_gossip(nodes: impl IntoIterator<Item = UniqueAddress>) -> Gossip {
    Gossip::from_members(
        nodes
            .into_iter()
            .map(|node| Member::new(node, Vec::new()).with_status(MemberStatus::Up)),
    )
}

pub(super) fn publish_gossip(publisher: &ActorRef<ClusterEventPublisherMsg>, gossip: Gossip) {
    publisher
        .tell(ClusterEventPublisherMsg::PublishChanges(gossip))
        .unwrap();
}

pub(super) fn await_connector_route(
    connector: &ActorRef<ReplicatorTcpPeerConnectorMsg>,
    snapshots: &TestProbe<ReplicatorTcpPeerConnectorSnapshot>,
    expected_peer: &UniqueAddress,
) {
    await_connector_routes(connector, snapshots, std::slice::from_ref(expected_peer));
}

pub(super) fn await_connector_routes(
    connector: &ActorRef<ReplicatorTcpPeerConnectorMsg>,
    snapshots: &TestProbe<ReplicatorTcpPeerConnectorSnapshot>,
    expected_peers: &[UniqueAddress],
) {
    await_assert(
        Duration::from_secs(1),
        Duration::from_millis(10),
        || -> Result<(), String> {
            connector
                .tell(ReplicatorTcpPeerConnectorMsg::Snapshot {
                    reply_to: snapshots.actor_ref(),
                })
                .map_err(|error| error.reason().to_string())?;
            let snapshot = snapshots
                .expect_msg(Duration::from_millis(100))
                .map_err(|error| error.to_string())?;
            let has_all_expected_peers = expected_peers.iter().all(|expected_peer| {
                snapshot
                    .active_targets
                    .iter()
                    .any(|target| target.node() == expected_peer)
            });
            if snapshot.route_count == expected_peers.len() && has_all_expected_peers {
                Ok(())
            } else {
                retry_pending_connector_routes(connector, &snapshot)?;
                Err(format!("unexpected connector snapshot: {snapshot:?}"))
            }
        },
    )
    .unwrap();
}

fn retry_pending_connector_routes(
    connector: &ActorRef<ReplicatorTcpPeerConnectorMsg>,
    snapshot: &ReplicatorTcpPeerConnectorSnapshot,
) -> Result<(), String> {
    if let Some(now) = snapshot
        .pending_reconnects
        .iter()
        .map(|pending| pending.next_retry_at)
        .max()
    {
        connector
            .tell(ReplicatorTcpPeerConnectorMsg::RetryDuePeerRoutes { now })
            .map_err(|error| error.reason().to_string())?;
    }
    Ok(())
}

pub(super) fn run_bootstrap_shutdown(
    kit: &ActorSystemTestKit,
    connector: &ActorRef<ReplicatorTcpPeerConnectorMsg>,
) {
    kit.system()
        .coordinated_shutdown()
        .run_from("test", Some(PHASE_BEFORE_CLUSTER_SHUTDOWN))
        .unwrap();
    assert!(connector.wait_for_stop(Duration::from_secs(1)));
}

pub(super) fn bootstrap_socket_test_lock() -> MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap()
}
