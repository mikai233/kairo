use std::net::TcpListener;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use kairo_actor::{Address, Props};
use kairo_cluster::{
    ClusterEventPublisher, ClusterEventPublisherMsg, Gossip, Member, MemberStatus, Reachability,
};
use kairo_remote::RemoteSettings;
use kairo_serialization::RemoteEnvelope;
use kairo_testkit::{ActorSystemTestKit, TestProbe};

use super::*;
use crate::{
    ReplicaId, ReplicatorRemoteReplyError, ReplicatorRemoteReplyReceiver,
    ReplicatorRemoteRequestError, ReplicatorRemoteRequestReceiver, ReplicatorTcpAssociationRuntime,
    ReplicatorTcpPeerReconnectSettings, ReplicatorTcpPeerRuntimeSettings,
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
    ReplicatorTcpAssociationRuntime::bind(
        name,
        local,
        remote,
        system_uid,
        RemoteSettings::new("127.0.0.1", port),
        Arc::new(IgnoreRequests) as Arc<dyn ReplicatorRemoteRequestReceiver>,
        Arc::new(IgnoreReplies) as Arc<dyn ReplicatorRemoteReplyReceiver>,
    )
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
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        let snapshot = expect_snapshot(connector, probe);
        if predicate(&snapshot) {
            return snapshot;
        }
        assert!(Instant::now() < deadline, "timed out waiting for snapshot");
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn connector_subscribes_to_cluster_and_applies_tcp_peer_routes() {
    let _guard = connector_socket_test_lock();
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
fn connector_clears_pending_reconnect_when_peer_leaves_membership() {
    let _guard = connector_socket_test_lock();
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
    let _guard = connector_socket_test_lock();
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
            Gossip::from_members([member(sender_node), member(receiver_node.clone())]),
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
    receiver_runtime.shutdown().unwrap();
    sender_kit.shutdown(Duration::from_secs(1)).unwrap();
    receiver_kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn connector_automatic_retry_timer_drives_due_peer_routes() {
    let _guard = connector_socket_test_lock();
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

fn connector_socket_test_lock() -> MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap()
}
