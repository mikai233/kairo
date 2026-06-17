use std::net::TcpListener;
use std::sync::Arc;
use std::time::{Duration, Instant};

use kairo_actor::{Address, Props};
use kairo_remote::{RemoteAssociationCache, RemoteOutbound, RemoteSettings};
use kairo_serialization::{ActorRefWireData, Registry};
use kairo_testkit::{ActorSystemTestKit, TestProbe};

use super::*;
use crate::{
    ClusterEventPublisher, ClusterEventPublisherMsg, ClusterMembershipMsg,
    ClusterMembershipWireInbound, ClusterSystemInbound, ClusterTcpAssociationRuntime,
    ClusterTcpPeerReconnectSettings, DEFAULT_CLUSTER_HEARTBEAT_RECEIVER_PATH,
    DEFAULT_CLUSTER_HEARTBEAT_SENDER_PATH, Gossip, HeartbeatRemoteReceiverInbound,
    HeartbeatRemoteResponseInbound, HeartbeatSenderMsg, Member, MemberStatus, Reachability,
    register_cluster_protocol_codecs, test_support::cluster_socket_test_lock,
};

fn registry() -> Arc<Registry> {
    let mut registry = Registry::new();
    register_cluster_protocol_codecs(&mut registry).unwrap();
    Arc::new(registry)
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

fn wire_for(node: &UniqueAddress, path: &str) -> ActorRefWireData {
    ActorRefWireData::new(format!("{}{}", node.address, path)).unwrap()
}

fn bind_peer_runtime(
    name: &str,
    uid: u64,
    system_uid: u64,
    settings: RemoteSettings,
    retry_interval: Duration,
    kit: &ActorSystemTestKit,
    registry: Arc<Registry>,
) -> ClusterTcpPeerRuntime {
    ClusterTcpPeerRuntime::bind_with_reconnect(
        name,
        uid,
        system_uid,
        settings.with_connect_timeout(Duration::from_millis(10)),
        ClusterTcpPeerReconnectSettings::new(retry_interval).unwrap(),
        move |self_node, cache| inbound_for(name, kit, registry, self_node, cache),
    )
    .unwrap()
}

fn bind_association_runtime_on_port(
    name: &str,
    uid: u64,
    system_uid: u64,
    port: u16,
    kit: &ActorSystemTestKit,
    registry: Arc<Registry>,
) -> ClusterTcpAssociationRuntime {
    ClusterTcpAssociationRuntime::bind(
        name,
        uid,
        system_uid,
        RemoteSettings::new("127.0.0.1", port),
        move |self_node, cache| inbound_for(name, kit, registry, self_node, cache),
    )
    .unwrap()
}

fn inbound_for(
    name: &str,
    kit: &ActorSystemTestKit,
    registry: Arc<Registry>,
    self_node: UniqueAddress,
    cache: RemoteAssociationCache,
) -> ClusterSystemInbound {
    let membership = kit
        .create_probe::<ClusterMembershipMsg>(format!("{name}-membership"))
        .unwrap();
    let heartbeat_sender = kit
        .create_probe::<HeartbeatSenderMsg>(format!("{name}-heartbeat-sender"))
        .unwrap();
    ClusterSystemInbound::new(self_node.clone())
        .with_membership(ClusterMembershipWireInbound::new(
            self_node.clone(),
            registry.clone(),
            membership.actor_ref(),
        ))
        .with_heartbeat_receiver(
            HeartbeatRemoteReceiverInbound::from_arc(
                self_node.clone(),
                registry.clone(),
                Arc::new(cache.clone()) as Arc<dyn RemoteOutbound>,
            )
            .with_sender(Some(wire_for(
                &self_node,
                DEFAULT_CLUSTER_HEARTBEAT_RECEIVER_PATH,
            ))),
        )
        .with_heartbeat_response(HeartbeatRemoteResponseInbound::new(
            wire_for(&self_node, DEFAULT_CLUSTER_HEARTBEAT_SENDER_PATH),
            registry,
            heartbeat_sender.actor_ref(),
        ))
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
    connector: &ActorRef<ClusterTcpPeerConnectorMsg>,
    probe: &TestProbe<ClusterTcpPeerConnectorSnapshot>,
) -> ClusterTcpPeerConnectorSnapshot {
    connector
        .tell(ClusterTcpPeerConnectorMsg::Snapshot {
            reply_to: probe.actor_ref(),
        })
        .unwrap();
    probe.expect_msg(Duration::from_secs(1)).unwrap()
}

fn eventually_snapshot(
    connector: &ActorRef<ClusterTcpPeerConnectorMsg>,
    probe: &TestProbe<ClusterTcpPeerConnectorSnapshot>,
    predicate: impl Fn(&ClusterTcpPeerConnectorSnapshot) -> bool,
) -> ClusterTcpPeerConnectorSnapshot {
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
    let sender_kit = ActorSystemTestKit::new("cluster-tcp-peer-connector-sender").unwrap();
    let receiver_kit = ActorSystemTestKit::new("cluster-tcp-peer-connector-receiver").unwrap();
    let registry = registry();
    let retry_interval = Duration::from_millis(25);
    let sender_runtime = bind_peer_runtime(
        "sender",
        1,
        11,
        RemoteSettings::new("127.0.0.1", 0),
        retry_interval,
        &sender_kit,
        registry.clone(),
    );
    let receiver_port = unused_port();
    let sender_node = sender_runtime.self_node().clone();
    let receiver_node = node("receiver", receiver_port, 2);
    let publisher = spawn_publisher(&sender_kit, sender_node.clone());
    let cluster = Cluster::new(publisher.clone());
    let snapshots = sender_kit
        .create_probe::<ClusterTcpPeerConnectorSnapshot>("snapshots")
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
                ClusterTcpPeerConnector::with_settings(
                    cluster,
                    sender_runtime,
                    ClusterTcpPeerConnectorSettings::new(retry_interval)
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

    let receiver_runtime =
        bind_association_runtime_on_port("receiver", 2, 22, receiver_port, &receiver_kit, registry);
    connector
        .tell(ClusterTcpPeerConnectorMsg::RetryDuePeerRoutes {
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
        ActorSystemTestKit::new("cluster-tcp-peer-connector-remove-pending-sender").unwrap();
    let registry = registry();
    let retry_interval = Duration::from_millis(25);
    let sender_runtime = bind_peer_runtime(
        "sender",
        1,
        11,
        RemoteSettings::new("127.0.0.1", 0),
        retry_interval,
        &sender_kit,
        registry,
    );
    let receiver_port = unused_port();
    let sender_node = sender_runtime.self_node().clone();
    let receiver_node = node("receiver", receiver_port, 2);
    let publisher = spawn_publisher(&sender_kit, sender_node.clone());
    let cluster = Cluster::new(publisher.clone());
    let snapshots = sender_kit
        .create_probe::<ClusterTcpPeerConnectorSnapshot>("remove-pending-snapshots")
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
                ClusterTcpPeerConnector::with_settings(
                    cluster,
                    sender_runtime,
                    ClusterTcpPeerConnectorSettings::new(retry_interval)
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
    let sender_kit = ActorSystemTestKit::new("cluster-tcp-peer-connector-clear-sender").unwrap();
    let receiver_kit =
        ActorSystemTestKit::new("cluster-tcp-peer-connector-clear-receiver").unwrap();
    let registry = registry();
    let retry_interval = Duration::from_millis(25);
    let sender_runtime = bind_peer_runtime(
        "sender",
        1,
        11,
        RemoteSettings::new("127.0.0.1", 0),
        retry_interval,
        &sender_kit,
        registry.clone(),
    );
    let sender_cache = sender_runtime.association_cache().clone();
    let receiver_port = unused_port();
    let sender_node = sender_runtime.self_node().clone();
    let receiver_node = node("receiver", receiver_port, 2);
    let publisher = spawn_publisher(&sender_kit, sender_node.clone());
    let cluster = Cluster::new(publisher.clone());
    let snapshots = sender_kit
        .create_probe::<ClusterTcpPeerConnectorSnapshot>("clear-snapshots")
        .unwrap();

    let receiver_runtime =
        bind_association_runtime_on_port("receiver", 2, 22, receiver_port, &receiver_kit, registry);
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
                ClusterTcpPeerConnector::with_settings(
                    cluster,
                    sender_runtime,
                    ClusterTcpPeerConnectorSettings::new(retry_interval)
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
        .tell(ClusterTcpPeerConnectorMsg::ClearRoutes)
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
    publisher
        .tell(ClusterEventPublisherMsg::PublishChanges(
            Gossip::from_members([member(sender_node)]),
        ))
        .unwrap();
    std::thread::sleep(Duration::from_millis(50));
    assert!(sender_kit.system().dead_letters().is_empty());
    assert_eq!(sender_cache.route_count(), 0);
    receiver_runtime.shutdown().unwrap();
    sender_kit.shutdown(Duration::from_secs(1)).unwrap();
    receiver_kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn connector_stop_clears_pending_reconnect_and_unsubscribes_from_cluster() {
    let _guard = connector_socket_test_lock();
    let sender_kit =
        ActorSystemTestKit::new("cluster-tcp-peer-connector-stop-pending-sender").unwrap();
    let receiver_kit =
        ActorSystemTestKit::new("cluster-tcp-peer-connector-stop-pending-receiver").unwrap();
    let registry = registry();
    let retry_interval = Duration::from_millis(25);
    let sender_runtime = bind_peer_runtime(
        "sender",
        1,
        11,
        RemoteSettings::new("127.0.0.1", 0),
        retry_interval,
        &sender_kit,
        registry.clone(),
    );
    let sender_cache = sender_runtime.association_cache().clone();
    let receiver_port = unused_port();
    let sender_node = sender_runtime.self_node().clone();
    let receiver_node = node("receiver", receiver_port, 2);
    let publisher = spawn_publisher(&sender_kit, sender_node.clone());
    let cluster = Cluster::new(publisher.clone());
    let snapshots = sender_kit
        .create_probe::<ClusterTcpPeerConnectorSnapshot>("stop-pending-snapshots")
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
                ClusterTcpPeerConnector::with_settings(
                    cluster,
                    sender_runtime,
                    ClusterTcpPeerConnectorSettings::new(retry_interval)
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

    let receiver_runtime =
        bind_association_runtime_on_port("receiver", 2, 22, receiver_port, &receiver_kit, registry);
    publisher
        .tell(ClusterEventPublisherMsg::PublishChanges(
            Gossip::from_members([member(sender_node), member(receiver_node)]),
        ))
        .unwrap();
    std::thread::sleep(Duration::from_millis(50));

    assert!(sender_kit.system().dead_letters().is_empty());
    assert_eq!(sender_cache.route_count(), 0);

    receiver_runtime.shutdown().unwrap();
    sender_kit.shutdown(Duration::from_secs(1)).unwrap();
    receiver_kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn connector_automatic_retry_timer_drives_due_peer_routes() {
    let _guard = connector_socket_test_lock();
    assert_eq!(
        ClusterTcpPeerConnectorSettings::new(Duration::ZERO).unwrap_err(),
        ClusterTcpPeerConnectorSettingsError::ZeroRetryInterval
    );

    let (sender_kit, time) =
        ActorSystemTestKit::with_manual_time("cluster-tcp-peer-connector-timer").unwrap();
    let receiver_kit =
        ActorSystemTestKit::new("cluster-tcp-peer-connector-timer-receiver").unwrap();
    let registry = registry();
    let retry_interval = Duration::from_millis(25);
    let sender_runtime = bind_peer_runtime(
        "sender",
        1,
        11,
        RemoteSettings::new("127.0.0.1", 0),
        retry_interval,
        &sender_kit,
        registry.clone(),
    );
    let receiver_port = unused_port();
    let sender_node = sender_runtime.self_node().clone();
    let receiver_node = node("receiver", receiver_port, 2);
    let publisher = spawn_publisher(&sender_kit, sender_node.clone());
    let cluster = Cluster::new(publisher.clone());
    let snapshots = sender_kit
        .create_probe::<ClusterTcpPeerConnectorSnapshot>("timer-snapshots")
        .unwrap();

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
                ClusterTcpPeerConnector::with_settings(
                    cluster,
                    sender_runtime,
                    ClusterTcpPeerConnectorSettings::new(retry_interval).unwrap(),
                )
            }),
        )
        .unwrap();
    eventually_snapshot(&connector, &snapshots, |snapshot| {
        snapshot.pending_reconnects.len() == 1
    });

    let receiver_runtime =
        bind_association_runtime_on_port("receiver", 2, 22, receiver_port, &receiver_kit, registry);
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

fn connector_socket_test_lock() -> crate::test_support::SocketTestGuard {
    cluster_socket_test_lock()
}
