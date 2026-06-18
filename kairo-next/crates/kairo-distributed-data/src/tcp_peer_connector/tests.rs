use std::net::TcpListener;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use kairo_actor::{Address, Props};
use kairo_cluster::{
    ClusterEventPublisher, ClusterEventPublisherMsg, CurrentClusterState, Gossip, Member,
    MemberStatus, Reachability,
};
use kairo_remote::{RemoteError, RemoteSettings};
use kairo_serialization::RemoteEnvelope;
use kairo_serialization::{ActorRefWireData, Registry, RemoteMessage};
use kairo_testkit::{ActorSystemTestKit, TestProbe, await_assert};

use super::*;
use crate::{
    ReplicaId, ReplicatorRead, ReplicatorRemoteReplyError, ReplicatorRemoteReplyReceiver,
    ReplicatorRemoteRequestError, ReplicatorRemoteRequestReceiver, ReplicatorTcpAssociationRuntime,
    ReplicatorTcpPeerReconnectSettings, ReplicatorTcpPeerRuntimeSettings,
    register_ddata_protocol_codecs, test_support::ddata_socket_test_lock,
};

#[derive(Default)]
struct IgnoreRequests;

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
struct IgnoreReplies;

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
struct RecordingRequests {
    received: Mutex<Vec<(ReplicaId, RemoteEnvelope)>>,
    changed: Condvar,
}

impl RecordingRequests {
    fn wait_for_len(&self, len: usize, timeout: Duration) -> Vec<(ReplicaId, RemoteEnvelope)> {
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

fn member(node: UniqueAddress) -> Member {
    Member::new(node, vec![]).with_status(MemberStatus::Up)
}

fn node(system: &str, port: u16, uid: u64) -> UniqueAddress {
    UniqueAddress::new(
        Address::new("kairo", system, Some("127.0.0.1".to_string()), Some(port)),
        uid,
    )
}

fn unused_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

fn bind_peer_runtime(
    name: &str,
    node_uid: u64,
    system_uid: u64,
    remote_replica: ReplicaId,
    retry_interval: Duration,
) -> ReplicatorTcpPeerRuntime {
    ReplicatorTcpPeerRuntime::bind_with_settings(
        name,
        node_uid,
        system_uid,
        remote_replica,
        ReplicatorTcpPeerRuntimeSettings::new(
            RemoteSettings::new("127.0.0.1", 0).with_connect_timeout(Duration::from_millis(10)),
        )
        .with_reconnect(ReplicatorTcpPeerReconnectSettings::new(retry_interval).unwrap()),
        Arc::new(IgnoreRequests) as Arc<dyn ReplicatorRemoteRequestReceiver>,
        Arc::new(IgnoreReplies) as Arc<dyn ReplicatorRemoteReplyReceiver>,
    )
    .unwrap()
}

fn bind_association_runtime_on_port(
    name: &str,
    local: ReplicaId,
    remote: ReplicaId,
    system_uid: u64,
    port: u16,
) -> ReplicatorTcpAssociationRuntime {
    bind_association_runtime_on_port_with_requests(
        name,
        local,
        remote,
        system_uid,
        port,
        Arc::new(IgnoreRequests) as Arc<dyn ReplicatorRemoteRequestReceiver>,
    )
}

fn bind_association_runtime_on_port_with_requests(
    name: &str,
    local: ReplicaId,
    remote: ReplicaId,
    system_uid: u64,
    port: u16,
    requests: Arc<dyn ReplicatorRemoteRequestReceiver>,
) -> ReplicatorTcpAssociationRuntime {
    ReplicatorTcpAssociationRuntime::bind(
        name,
        local,
        remote,
        system_uid,
        RemoteSettings::new("127.0.0.1", port),
        requests,
        Arc::new(IgnoreReplies) as Arc<dyn ReplicatorRemoteReplyReceiver>,
    )
    .unwrap()
}

fn registry() -> Registry {
    let mut registry = Registry::new();
    register_ddata_protocol_codecs(&mut registry).unwrap();
    registry
}

fn replicator_ref(system: &str, port: u16) -> ActorRefWireData {
    ActorRefWireData::new(format!(
        "kairo://{system}@127.0.0.1:{port}/system/replicator"
    ))
    .unwrap()
}

fn spawn_publisher(
    kit: &ActorSystemTestKit,
    self_node: UniqueAddress,
) -> ActorRef<ClusterEventPublisherMsg> {
    kit.system()
        .spawn(
            "publisher",
            Props::new(move || ClusterEventPublisher::new(self_node.clone())),
        )
        .unwrap()
}

fn expect_snapshot(
    connector: &ActorRef<ReplicatorTcpPeerConnectorMsg>,
    probe: &TestProbe<ReplicatorTcpPeerConnectorSnapshot>,
) -> ReplicatorTcpPeerConnectorSnapshot {
    connector
        .tell(ReplicatorTcpPeerConnectorMsg::Snapshot {
            reply_to: probe.actor_ref(),
        })
        .unwrap();
    probe.expect_msg(Duration::from_secs(1)).unwrap()
}

fn eventually_snapshot(
    connector: &ActorRef<ReplicatorTcpPeerConnectorMsg>,
    probe: &TestProbe<ReplicatorTcpPeerConnectorSnapshot>,
    predicate: impl Fn(&ReplicatorTcpPeerConnectorSnapshot) -> bool,
) -> ReplicatorTcpPeerConnectorSnapshot {
    await_assert(
        Duration::from_secs(1),
        Duration::from_millis(5),
        || -> Result<ReplicatorTcpPeerConnectorSnapshot, String> {
            let snapshot = expect_snapshot(connector, probe);
            if predicate(&snapshot) {
                Ok(snapshot)
            } else {
                Err(format!("unexpected connector snapshot: {snapshot:?}"))
            }
        },
    )
    .unwrap()
}

fn publish_changes_and_wait(
    kit: &ActorSystemTestKit,
    publisher: &ActorRef<ClusterEventPublisherMsg>,
    gossip: Gossip,
    probe_name: &str,
) {
    publisher
        .tell(ClusterEventPublisherMsg::PublishChanges(gossip))
        .unwrap();
    let state = kit.create_probe::<CurrentClusterState>(probe_name).unwrap();
    publisher
        .tell(ClusterEventPublisherMsg::SendCurrentState {
            reply_to: state.actor_ref(),
        })
        .unwrap();
    state.expect_msg(Duration::from_secs(1)).unwrap();
}

#[test]
fn connector_subscribes_to_cluster_and_applies_tcp_peer_routes() {
    let _guard = ddata_socket_test_lock();
    let sender_kit = ActorSystemTestKit::new("ddata-tcp-peer-connector-sender").unwrap();
    let receiver_kit = ActorSystemTestKit::new("ddata-tcp-peer-connector-receiver").unwrap();
    let retry_interval = Duration::from_millis(25);
    let receiver_port = unused_port();
    let receiver_node = node("receiver", receiver_port, 2);
    let sender_runtime = bind_peer_runtime(
        "sender",
        1,
        11,
        ReplicaId::from(&receiver_node),
        retry_interval,
    );
    let sender_node = sender_runtime.self_node().clone();
    let publisher = spawn_publisher(&sender_kit, sender_node.clone());
    let cluster = Cluster::new(publisher.clone());
    let snapshots = sender_kit
        .create_probe::<ReplicatorTcpPeerConnectorSnapshot>("snapshots")
        .unwrap();

    publisher
        .tell(ClusterEventPublisherMsg::PublishChanges(
            Gossip::from_members([member(sender_node.clone()), member(receiver_node.clone())]),
        ))
        .unwrap();
    let connector = sender_kit
        .system()
        .spawn(
            "tcp-peer-connector",
            Props::new(move || {
                ReplicatorTcpPeerConnector::with_settings(
                    cluster,
                    sender_runtime,
                    ReplicatorTcpPeerConnectorSettings::new(retry_interval)
                        .unwrap()
                        .with_automatic_retry_ticks(false),
                )
            }),
        )
        .unwrap();

    let snapshot = eventually_snapshot(&connector, &snapshots, |snapshot| {
        snapshot.pending_reconnects.len() == 1
    });
    assert_eq!(snapshot.route_count, 0);
    assert!(snapshot.last_error.is_some());
    assert_eq!(snapshot.pending_reconnects[0].target.node(), &receiver_node);

    let receiver_runtime = bind_association_runtime_on_port(
        "receiver",
        ReplicaId::from(&receiver_node),
        ReplicaId::from(&sender_node),
        22,
        receiver_port,
    );
    connector
        .tell(ReplicatorTcpPeerConnectorMsg::RetryDuePeerRoutes {
            now: retry_interval,
        })
        .unwrap();
    let snapshot =
        eventually_snapshot(&connector, &snapshots, |snapshot| snapshot.route_count == 1);
    assert_eq!(snapshot.active_targets[0].node(), &receiver_node);
    assert!(snapshot.pending_reconnects.is_empty());

    publisher
        .tell(ClusterEventPublisherMsg::PublishChanges(
            Gossip::from_members([member(sender_node.clone()), member(receiver_node.clone())])
                .with_reachability(
                    Reachability::new().unreachable(sender_node.clone(), receiver_node.clone()),
                ),
        ))
        .unwrap();
    let snapshot =
        eventually_snapshot(&connector, &snapshots, |snapshot| snapshot.route_count == 0);
    assert!(snapshot.active_targets.is_empty());
    assert!(snapshot.last_error.is_none());

    sender_kit.system().stop(&connector);
    assert!(connector.wait_for_stop(Duration::from_secs(1)));
    receiver_runtime.shutdown().unwrap();
    sender_kit.shutdown(Duration::from_secs(1)).unwrap();
    receiver_kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn connector_preserves_successful_route_when_later_snapshot_dial_fails() {
    let _guard = ddata_socket_test_lock();
    let sender_kit = ActorSystemTestKit::new("ddata-tcp-peer-connector-partial-sender").unwrap();
    let bound_kit = ActorSystemTestKit::new("ddata-tcp-peer-connector-partial-bound").unwrap();
    let missing_kit = ActorSystemTestKit::new("ddata-tcp-peer-connector-partial-missing").unwrap();
    let retry_interval = Duration::from_millis(25);
    let bound_port = unused_port();
    let missing_port = unused_port();
    let bound_node = node("partial-bound", bound_port, 2);
    let missing_node = node("partial-missing", missing_port, 3);
    let sender_runtime = bind_peer_runtime(
        "partial-sender",
        1,
        11,
        ReplicaId::from(&bound_node),
        retry_interval,
    );
    let sender_cache = sender_runtime.association_cache().clone();
    let sender_node = sender_runtime.self_node().clone();
    let bound_runtime = bind_association_runtime_on_port(
        "partial-bound",
        ReplicaId::from(&bound_node),
        ReplicaId::from(&sender_node),
        22,
        bound_port,
    );
    let publisher = spawn_publisher(&sender_kit, sender_node.clone());
    let cluster = Cluster::new(publisher.clone());
    let snapshots = sender_kit
        .create_probe::<ReplicatorTcpPeerConnectorSnapshot>("partial-snapshots")
        .unwrap();

    publisher
        .tell(ClusterEventPublisherMsg::PublishChanges(
            Gossip::from_members([
                member(sender_node.clone()),
                member(bound_node.clone()),
                member(missing_node.clone()),
            ]),
        ))
        .unwrap();
    let connector = sender_kit
        .system()
        .spawn(
            "tcp-peer-connector",
            Props::new(move || {
                ReplicatorTcpPeerConnector::with_settings(
                    cluster,
                    sender_runtime,
                    ReplicatorTcpPeerConnectorSettings::new(retry_interval)
                        .unwrap()
                        .with_automatic_retry_ticks(false),
                )
            }),
        )
        .unwrap();

    let snapshot = eventually_snapshot(&connector, &snapshots, |snapshot| {
        snapshot.route_count == 1 && snapshot.pending_reconnects.len() == 1
    });
    assert_eq!(snapshot.active_targets[0].node(), &bound_node);
    assert_eq!(snapshot.pending_reconnects[0].target.node(), &missing_node);
    assert!(snapshot.last_error.is_some());
    assert_eq!(sender_cache.route_count(), 1);

    let missing_runtime = bind_association_runtime_on_port(
        "partial-missing",
        ReplicaId::from(&missing_node),
        ReplicaId::from(&sender_node),
        33,
        missing_port,
    );
    connector
        .tell(ReplicatorTcpPeerConnectorMsg::RetryDuePeerRoutes {
            now: retry_interval,
        })
        .unwrap();
    let snapshot =
        eventually_snapshot(&connector, &snapshots, |snapshot| snapshot.route_count == 2);
    assert!(
        snapshot
            .active_targets
            .iter()
            .any(|target| target.node() == &bound_node)
    );
    assert!(
        snapshot
            .active_targets
            .iter()
            .any(|target| target.node() == &missing_node)
    );
    assert!(snapshot.pending_reconnects.is_empty());
    assert!(snapshot.last_error.is_none());
    assert_eq!(sender_cache.route_count(), 2);

    sender_kit.system().stop(&connector);
    assert!(connector.wait_for_stop(Duration::from_secs(1)));
    assert_eq!(sender_cache.route_count(), 0);
    bound_runtime.shutdown().unwrap();
    missing_runtime.shutdown().unwrap();
    sender_kit.shutdown(Duration::from_secs(1)).unwrap();
    bound_kit.shutdown(Duration::from_secs(1)).unwrap();
    missing_kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn connector_keeps_remaining_route_delivering_after_member_removed_event() {
    let _guard = ddata_socket_test_lock();
    let sender_kit =
        ActorSystemTestKit::new("ddata-tcp-peer-connector-event-remove-sender").unwrap();
    let second_kit =
        ActorSystemTestKit::new("ddata-tcp-peer-connector-event-remove-second").unwrap();
    let third_kit = ActorSystemTestKit::new("ddata-tcp-peer-connector-event-remove-third").unwrap();
    let retry_interval = Duration::from_millis(25);
    let registry = registry();
    let second_port = unused_port();
    let third_port = unused_port();
    let second_node = node("event-remove-second", second_port, 2);
    let third_node = node("event-remove-third", third_port, 3);
    let second_requests = Arc::new(RecordingRequests::default());
    let third_requests = Arc::new(RecordingRequests::default());
    let sender_runtime = bind_peer_runtime(
        "event-remove-sender",
        1,
        11,
        ReplicaId::from(&second_node),
        retry_interval,
    );
    let sender_cache = sender_runtime.association_cache().clone();
    let sender_node = sender_runtime.self_node().clone();
    let second_runtime = bind_association_runtime_on_port_with_requests(
        "event-remove-second",
        ReplicaId::from(&second_node),
        ReplicaId::from(&sender_node),
        22,
        second_port,
        second_requests.clone() as Arc<dyn ReplicatorRemoteRequestReceiver>,
    );
    let third_runtime = bind_association_runtime_on_port_with_requests(
        "event-remove-third",
        ReplicaId::from(&third_node),
        ReplicaId::from(&sender_node),
        33,
        third_port,
        third_requests.clone() as Arc<dyn ReplicatorRemoteRequestReceiver>,
    );
    let publisher = spawn_publisher(&sender_kit, sender_node.clone());
    let cluster = Cluster::new(publisher.clone());
    let snapshots = sender_kit
        .create_probe::<ReplicatorTcpPeerConnectorSnapshot>("event-remove-snapshots")
        .unwrap();

    publisher
        .tell(ClusterEventPublisherMsg::PublishChanges(
            Gossip::from_members([
                member(sender_node.clone()),
                member(second_node.clone()),
                member(third_node.clone()),
            ]),
        ))
        .unwrap();
    let connector = sender_kit
        .system()
        .spawn(
            "tcp-peer-connector",
            Props::new(move || {
                ReplicatorTcpPeerConnector::with_settings(
                    cluster,
                    sender_runtime,
                    ReplicatorTcpPeerConnectorSettings::new(retry_interval)
                        .unwrap()
                        .with_automatic_retry_ticks(false),
                )
            }),
        )
        .unwrap();

    let snapshot =
        eventually_snapshot(&connector, &snapshots, |snapshot| snapshot.route_count == 2);
    assert!(
        snapshot
            .active_targets
            .iter()
            .any(|target| target.node() == &second_node)
    );
    assert!(
        snapshot
            .active_targets
            .iter()
            .any(|target| target.node() == &third_node)
    );
    assert_eq!(sender_cache.route_count(), 2);

    publisher
        .tell(ClusterEventPublisherMsg::PublishChanges(
            Gossip::from_members([member(sender_node.clone()), member(second_node.clone())]),
        ))
        .unwrap();
    let snapshot =
        eventually_snapshot(&connector, &snapshots, |snapshot| snapshot.route_count == 1);
    assert_eq!(snapshot.active_targets.len(), 1);
    assert_eq!(snapshot.active_targets[0].node(), &second_node);
    assert_eq!(sender_cache.route_count(), 1);
    let report = snapshot
        .last_report
        .expect("member removal should record a route report");
    assert_eq!(report.removed.len(), 1);
    assert_eq!(report.removed[0].node(), &third_node);

    let second_recipient = replicator_ref("event-remove-second", second_port);
    let sender_ref = replicator_ref(
        sender_node.address.system(),
        sender_node.address.port().unwrap(),
    );
    let read = ReplicatorRead {
        key: "counter-after-connector-member-removed".to_string(),
        from: Some(ReplicaId::from(&sender_node)),
    };
    sender_cache
        .send_to_recipient(RemoteEnvelope::new(
            second_recipient.clone(),
            Some(sender_ref.clone()),
            registry.serialize(&read).unwrap(),
        ))
        .unwrap();

    let received = second_requests.wait_for_len(1, Duration::from_secs(1));
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].0, ReplicaId::from(&sender_node));
    assert_eq!(received[0].1.recipient, second_recipient);
    assert_eq!(received[0].1.sender, Some(sender_ref.clone()));
    assert_eq!(
        received[0].1.message.manifest.as_str(),
        ReplicatorRead::MANIFEST
    );
    let decoded = registry
        .deserialize::<ReplicatorRead>(received[0].1.message.clone())
        .unwrap();
    assert_eq!(decoded, read);

    let removed_read = ReplicatorRead {
        key: "counter-after-connector-removed-member".to_string(),
        from: Some(ReplicaId::from(&sender_node)),
    };
    let removed_error = sender_cache
        .send_to_recipient(RemoteEnvelope::new(
            replicator_ref("event-remove-third", third_port),
            Some(sender_ref),
            registry.serialize(&removed_read).unwrap(),
        ))
        .expect_err("removed member route should reject delivery");
    assert!(matches!(
        removed_error,
        RemoteError::AssociationUnavailable { .. }
    ));
    assert!(
        third_requests
            .wait_for_len(1, Duration::from_millis(50))
            .is_empty(),
        "removed member must not receive connector-routed requests"
    );

    sender_kit.system().stop(&connector);
    assert!(connector.wait_for_stop(Duration::from_secs(1)));
    assert_eq!(sender_cache.route_count(), 0);
    second_runtime.shutdown().unwrap();
    third_runtime.shutdown().unwrap();
    sender_kit.shutdown(Duration::from_secs(1)).unwrap();
    second_kit.shutdown(Duration::from_secs(1)).unwrap();
    third_kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn connector_clears_pending_reconnect_when_peer_leaves_membership() {
    let _guard = ddata_socket_test_lock();
    let sender_kit =
        ActorSystemTestKit::new("ddata-tcp-peer-connector-remove-pending-sender").unwrap();
    let retry_interval = Duration::from_millis(25);
    let receiver_port = unused_port();
    let receiver_node = node("receiver", receiver_port, 2);
    let sender_runtime = bind_peer_runtime(
        "sender",
        1,
        11,
        ReplicaId::from(&receiver_node),
        retry_interval,
    );
    let sender_node = sender_runtime.self_node().clone();
    let publisher = spawn_publisher(&sender_kit, sender_node.clone());
    let cluster = Cluster::new(publisher.clone());
    let snapshots = sender_kit
        .create_probe::<ReplicatorTcpPeerConnectorSnapshot>("remove-pending-snapshots")
        .unwrap();

    publisher
        .tell(ClusterEventPublisherMsg::PublishChanges(
            Gossip::from_members([member(sender_node.clone()), member(receiver_node.clone())]),
        ))
        .unwrap();
    let connector = sender_kit
        .system()
        .spawn(
            "tcp-peer-connector",
            Props::new(move || {
                ReplicatorTcpPeerConnector::with_settings(
                    cluster,
                    sender_runtime,
                    ReplicatorTcpPeerConnectorSettings::new(retry_interval)
                        .unwrap()
                        .with_automatic_retry_ticks(false),
                )
            }),
        )
        .unwrap();

    let snapshot = eventually_snapshot(&connector, &snapshots, |snapshot| {
        snapshot.pending_reconnects.len() == 1
    });
    assert_eq!(snapshot.route_count, 0);
    assert_eq!(snapshot.pending_reconnects[0].target.node(), &receiver_node);

    publisher
        .tell(ClusterEventPublisherMsg::PublishChanges(
            Gossip::from_members([member(sender_node)]),
        ))
        .unwrap();
    let snapshot = eventually_snapshot(&connector, &snapshots, |snapshot| {
        snapshot.pending_reconnects.is_empty() && snapshot.route_count == 0
    });

    assert!(snapshot.active_targets.is_empty());
    assert!(snapshot.last_error.is_none());

    sender_kit.system().stop(&connector);
    assert!(connector.wait_for_stop(Duration::from_secs(1)));
    sender_kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn connector_clear_routes_removes_active_peer_routes() {
    let _guard = ddata_socket_test_lock();
    let sender_kit = ActorSystemTestKit::new("ddata-tcp-peer-connector-clear-sender").unwrap();
    let receiver_kit = ActorSystemTestKit::new("ddata-tcp-peer-connector-clear-receiver").unwrap();
    let retry_interval = Duration::from_millis(25);
    let receiver_port = unused_port();
    let receiver_node = node("receiver", receiver_port, 2);
    let sender_runtime = bind_peer_runtime(
        "sender",
        1,
        11,
        ReplicaId::from(&receiver_node),
        retry_interval,
    );
    let sender_cache = sender_runtime.association_cache().clone();
    let sender_node = sender_runtime.self_node().clone();
    let publisher = spawn_publisher(&sender_kit, sender_node.clone());
    let cluster = Cluster::new(publisher.clone());
    let snapshots = sender_kit
        .create_probe::<ReplicatorTcpPeerConnectorSnapshot>("clear-snapshots")
        .unwrap();

    let receiver_runtime = bind_association_runtime_on_port(
        "receiver",
        ReplicaId::from(&receiver_node),
        ReplicaId::from(&sender_node),
        22,
        receiver_port,
    );
    publisher
        .tell(ClusterEventPublisherMsg::PublishChanges(
            Gossip::from_members([member(sender_node.clone()), member(receiver_node.clone())]),
        ))
        .unwrap();
    let connector = sender_kit
        .system()
        .spawn(
            "tcp-peer-connector",
            Props::new(move || {
                ReplicatorTcpPeerConnector::with_settings(
                    cluster,
                    sender_runtime,
                    ReplicatorTcpPeerConnectorSettings::new(retry_interval)
                        .unwrap()
                        .with_automatic_retry_ticks(false),
                )
            }),
        )
        .unwrap();
    let snapshot =
        eventually_snapshot(&connector, &snapshots, |snapshot| snapshot.route_count == 1);
    assert_eq!(snapshot.active_targets[0].node(), &receiver_node);

    connector
        .tell(ReplicatorTcpPeerConnectorMsg::ClearRoutes)
        .unwrap();
    let snapshot =
        eventually_snapshot(&connector, &snapshots, |snapshot| snapshot.route_count == 0);

    assert!(snapshot.active_targets.is_empty());
    let report = snapshot
        .last_report
        .expect("clear routes should record a report");
    assert_eq!(report.removed.len(), 1);
    assert_eq!(report.removed[0].node(), &receiver_node);
    assert!(report.dialed.is_empty());
    assert!(report.skipped.is_empty());
    assert!(snapshot.last_error.is_none());

    sender_kit.system().stop(&connector);
    assert!(connector.wait_for_stop(Duration::from_secs(1)));
    publish_changes_and_wait(
        &sender_kit,
        &publisher,
        Gossip::from_members([member(sender_node)]),
        "clear-after-stop-state",
    );
    assert!(sender_kit.system().dead_letters().is_empty());
    assert_eq!(sender_cache.route_count(), 0);
    receiver_runtime.shutdown().unwrap();
    sender_kit.shutdown(Duration::from_secs(1)).unwrap();
    receiver_kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn connector_stop_clears_pending_reconnect_and_unsubscribes_from_cluster() {
    let _guard = ddata_socket_test_lock();
    let sender_kit =
        ActorSystemTestKit::new("ddata-tcp-peer-connector-stop-pending-sender").unwrap();
    let receiver_kit =
        ActorSystemTestKit::new("ddata-tcp-peer-connector-stop-pending-receiver").unwrap();
    let retry_interval = Duration::from_millis(25);
    let receiver_port = unused_port();
    let receiver_node = node("receiver", receiver_port, 2);
    let sender_runtime = bind_peer_runtime(
        "sender",
        1,
        11,
        ReplicaId::from(&receiver_node),
        retry_interval,
    );
    let sender_cache = sender_runtime.association_cache().clone();
    let sender_node = sender_runtime.self_node().clone();
    let publisher = spawn_publisher(&sender_kit, sender_node.clone());
    let cluster = Cluster::new(publisher.clone());
    let snapshots = sender_kit
        .create_probe::<ReplicatorTcpPeerConnectorSnapshot>("stop-pending-snapshots")
        .unwrap();

    publisher
        .tell(ClusterEventPublisherMsg::PublishChanges(
            Gossip::from_members([member(sender_node.clone()), member(receiver_node.clone())]),
        ))
        .unwrap();
    let connector = sender_kit
        .system()
        .spawn(
            "tcp-peer-connector",
            Props::new(move || {
                ReplicatorTcpPeerConnector::with_settings(
                    cluster,
                    sender_runtime,
                    ReplicatorTcpPeerConnectorSettings::new(retry_interval)
                        .unwrap()
                        .with_automatic_retry_ticks(false),
                )
            }),
        )
        .unwrap();

    let snapshot = eventually_snapshot(&connector, &snapshots, |snapshot| {
        snapshot.pending_reconnects.len() == 1
    });
    assert_eq!(snapshot.route_count, 0);
    assert_eq!(snapshot.pending_reconnects[0].target.node(), &receiver_node);

    sender_kit.system().stop(&connector);
    assert!(connector.wait_for_stop(Duration::from_secs(1)));

    let receiver_runtime = bind_association_runtime_on_port(
        "receiver",
        ReplicaId::from(&receiver_node),
        ReplicaId::from(&sender_node),
        22,
        receiver_port,
    );
    publish_changes_and_wait(
        &sender_kit,
        &publisher,
        Gossip::from_members([member(sender_node), member(receiver_node)]),
        "stop-pending-after-stop-state",
    );

    assert!(sender_kit.system().dead_letters().is_empty());
    assert_eq!(sender_cache.route_count(), 0);

    receiver_runtime.shutdown().unwrap();
    sender_kit.shutdown(Duration::from_secs(1)).unwrap();
    receiver_kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn connector_automatic_retry_timer_drives_due_peer_routes() {
    let _guard = ddata_socket_test_lock();
    assert_eq!(
        ReplicatorTcpPeerConnectorSettings::new(Duration::ZERO).unwrap_err(),
        ReplicatorTcpPeerConnectorSettingsError::ZeroRetryInterval
    );

    let (sender_kit, time) =
        ActorSystemTestKit::with_manual_time("ddata-tcp-peer-connector-timer").unwrap();
    let receiver_kit = ActorSystemTestKit::new("ddata-tcp-peer-connector-timer-receiver").unwrap();
    let retry_interval = Duration::from_millis(25);
    let receiver_port = unused_port();
    let receiver_node = node("receiver", receiver_port, 2);
    let sender_runtime = bind_peer_runtime(
        "sender",
        1,
        11,
        ReplicaId::from(&receiver_node),
        retry_interval,
    );
    let sender_node = sender_runtime.self_node().clone();
    let publisher = spawn_publisher(&sender_kit, sender_node.clone());
    let cluster = Cluster::new(publisher.clone());
    let snapshots = sender_kit
        .create_probe::<ReplicatorTcpPeerConnectorSnapshot>("timer-snapshots")
        .unwrap();

    publisher
        .tell(ClusterEventPublisherMsg::PublishChanges(
            Gossip::from_members([member(sender_node.clone()), member(receiver_node.clone())]),
        ))
        .unwrap();
    let connector = sender_kit
        .system()
        .spawn(
            "tcp-peer-connector",
            Props::new(move || {
                ReplicatorTcpPeerConnector::with_settings(
                    cluster,
                    sender_runtime,
                    ReplicatorTcpPeerConnectorSettings::new(retry_interval).unwrap(),
                )
            }),
        )
        .unwrap();
    eventually_snapshot(&connector, &snapshots, |snapshot| {
        snapshot.pending_reconnects.len() == 1
    });

    let receiver_runtime = bind_association_runtime_on_port(
        "receiver",
        ReplicaId::from(&receiver_node),
        ReplicaId::from(&sender_node),
        22,
        receiver_port,
    );
    time.advance(retry_interval);

    let snapshot =
        eventually_snapshot(&connector, &snapshots, |snapshot| snapshot.route_count == 1);
    assert_eq!(snapshot.active_targets[0].node(), &receiver_node);
    assert!(snapshot.pending_reconnects.is_empty());
    assert!(snapshot.last_error.is_none());

    sender_kit.system().stop(&connector);
    assert!(connector.wait_for_stop(Duration::from_secs(1)));
    receiver_runtime.shutdown().unwrap();
    sender_kit.shutdown(Duration::from_secs(1)).unwrap();
    receiver_kit.shutdown(Duration::from_secs(1)).unwrap();
}
