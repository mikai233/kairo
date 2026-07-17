use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use kairo_actor::{Address, Props};
use kairo_remote::{
    RemoteAssociationAddress, RemoteAssociationCache, RemoteOutbound, RemoteSettings,
};
use kairo_serialization::{ActorRefWireData, Registry, RemoteEnvelope};
use kairo_testkit::{ActorSystemTestKit, TestProbe, await_assert};

use super::*;
use crate::{
    ClusterEventPublisher, ClusterEventPublisherMsg, ClusterMembershipMsg,
    ClusterMembershipRemoteEnvelopeOutbound, ClusterMembershipWireInbound,
    ClusterMembershipWireOutbound, ClusterSystemInbound, ClusterTcpAssociationRuntime,
    ClusterTcpPeerReconnectSettings, CurrentClusterState, DEFAULT_CLUSTER_HEARTBEAT_RECEIVER_PATH,
    DEFAULT_CLUSTER_HEARTBEAT_SENDER_PATH, Gossip, HeartbeatRemoteReceiverInbound,
    HeartbeatRemoteResponseInbound, HeartbeatSenderMsg, Join, Member, MemberStatus, Reachability,
    register_cluster_protocol_codecs, test_support::cluster_socket_test_lock,
};

struct ClusterInboundProbes {
    membership: TestProbe<ClusterMembershipMsg>,
}

#[derive(Default)]
struct NoopOutbound;

impl RemoteOutbound for NoopOutbound {
    fn send(&self, _envelope: RemoteEnvelope) -> kairo_remote::Result<()> {
        Ok(())
    }
}

struct LateRouteOnClose {
    cache: RemoteAssociationCache,
    late_address: RemoteAssociationAddress,
}

impl RemoteOutbound for LateRouteOnClose {
    fn send(&self, _envelope: RemoteEnvelope) -> kairo_remote::Result<()> {
        Ok(())
    }

    fn close(&self, _reason: &str) -> kairo_remote::Result<()> {
        self.cache
            .insert_route(self.late_address.clone(), Arc::new(NoopOutbound));
        Ok(())
    }
}

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
    bind_association_runtime_on_port_with_probes(name, uid, system_uid, port, kit, registry).0
}

fn bind_association_runtime_on_port_with_probes(
    name: &str,
    uid: u64,
    system_uid: u64,
    port: u16,
    kit: &ActorSystemTestKit,
    registry: Arc<Registry>,
) -> (ClusterTcpAssociationRuntime, ClusterInboundProbes) {
    let membership = kit
        .create_probe::<ClusterMembershipMsg>(format!("{name}-membership"))
        .unwrap();
    let heartbeat_sender = kit
        .create_probe::<HeartbeatSenderMsg>(format!("{name}-heartbeat-sender"))
        .unwrap();
    let membership_ref = membership.actor_ref();
    let heartbeat_sender_ref = heartbeat_sender.actor_ref();
    ClusterTcpAssociationRuntime::bind(
        name,
        uid,
        system_uid,
        RemoteSettings::new("127.0.0.1", port),
        move |self_node, cache| {
            ClusterSystemInbound::new(self_node.clone())
                .with_membership(ClusterMembershipWireInbound::new(
                    self_node.clone(),
                    registry.clone(),
                    membership_ref.clone(),
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
                    registry.clone(),
                    heartbeat_sender_ref.clone(),
                ))
        },
    )
    .map(|runtime| (runtime, ClusterInboundProbes { membership }))
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
    await_assert(
        Duration::from_secs(1),
        Duration::from_millis(5),
        || -> Result<ClusterTcpPeerConnectorSnapshot, String> {
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
fn connector_stop_does_not_wait_for_in_flight_runtime_command() {
    let _guard = connector_socket_test_lock();
    let kit = ActorSystemTestKit::new("cluster-tcp-peer-stop-in-flight").unwrap();
    let runtime = bind_peer_runtime(
        "stop-in-flight",
        1,
        11,
        RemoteSettings::new("127.0.0.1", 0),
        Duration::from_millis(25),
        &kit,
        registry(),
    );
    let cache = runtime.association_cache().clone();
    cache.insert_route(
        RemoteAssociationAddress::new("kairo", "stop-in-flight-route", "127.0.0.1", Some(2552))
            .unwrap(),
        Arc::new(NoopOutbound),
    );
    let self_node = runtime.self_node().clone();
    let publisher = spawn_publisher(&kit, self_node);
    let cluster = Cluster::new(publisher);
    let started = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let released = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let gate = ClusterTcpPeerConnectorRuntimeCommandGate {
        started: Arc::clone(&started),
        released: Arc::clone(&released),
    };
    let connector = kit
        .system()
        .spawn(
            "tcp-peer-connector",
            Props::new(move || {
                ClusterTcpPeerConnector::with_settings(
                    cluster,
                    runtime,
                    ClusterTcpPeerConnectorSettings::default().with_automatic_retry_ticks(false),
                )
                .with_runtime_command_gate(gate)
            }),
        )
        .unwrap();

    await_assert(Duration::from_secs(1), Duration::from_millis(1), || {
        started
            .load(std::sync::atomic::Ordering::SeqCst)
            .then_some(())
            .ok_or_else(|| "runtime command has not acquired ownership".to_string())
    })
    .unwrap();
    kit.system().stop(&connector);
    let stopped_before_release = connector.wait_for_stop(Duration::from_millis(100));
    released.store(true, std::sync::atomic::Ordering::SeqCst);

    assert!(
        stopped_before_release,
        "connector stop must not wait for blocking transport work"
    );
    await_assert(Duration::from_secs(1), Duration::from_millis(1), || {
        (cache.route_count() == 0)
            .then_some(())
            .ok_or_else(|| "stopped connector runtime has not cleared routes".to_string())
    })
    .unwrap();
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn connector_shutdown_waits_for_in_flight_command_cleanup_off_turn() {
    let _guard = connector_socket_test_lock();
    let kit = ActorSystemTestKit::new("cluster-tcp-peer-shutdown-in-flight").unwrap();
    let runtime = bind_peer_runtime(
        "shutdown-in-flight",
        1,
        11,
        RemoteSettings::new("127.0.0.1", 0),
        Duration::from_millis(25),
        &kit,
        registry(),
    );
    let cache = runtime.association_cache().clone();
    cache.insert_route(
        RemoteAssociationAddress::new("kairo", "shutdown-in-flight-route", "127.0.0.1", Some(2552))
            .unwrap(),
        Arc::new(NoopOutbound),
    );
    let self_node = runtime.self_node().clone();
    let publisher = spawn_publisher(&kit, self_node);
    let cluster = Cluster::new(publisher);
    let started = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let released = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let gate = ClusterTcpPeerConnectorRuntimeCommandGate {
        started: Arc::clone(&started),
        released: Arc::clone(&released),
    };
    let connector = kit
        .system()
        .spawn(
            "tcp-peer-connector",
            Props::new(move || {
                ClusterTcpPeerConnector::with_settings(
                    cluster,
                    runtime,
                    ClusterTcpPeerConnectorSettings::default().with_automatic_retry_ticks(false),
                )
                .with_runtime_command_gate(gate)
            }),
        )
        .unwrap();

    await_assert(Duration::from_secs(1), Duration::from_millis(1), || {
        started
            .load(std::sync::atomic::Ordering::SeqCst)
            .then_some(())
            .ok_or_else(|| "runtime command has not acquired ownership".to_string())
    })
    .unwrap();
    connector
        .tell(ClusterTcpPeerConnectorMsg::Shutdown {
            timeout: Duration::from_secs(1),
        })
        .unwrap();
    assert!(!connector.wait_for_stop(Duration::from_millis(100)));
    released.store(true, std::sync::atomic::Ordering::SeqCst);

    assert!(connector.wait_for_stop(Duration::from_secs(2)));
    assert_eq!(cache.route_count(), 0);
    kit.shutdown(Duration::from_secs(1)).unwrap();
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
fn connector_rejects_non_remote_member_without_pending_reconnect() {
    let _guard = connector_socket_test_lock();
    let sender_kit = ActorSystemTestKit::new("cluster-tcp-peer-connector-local-only").unwrap();
    let registry = registry();
    let retry_interval = Duration::from_millis(25);
    let sender_runtime = bind_peer_runtime(
        "local-only-sender",
        1,
        11,
        RemoteSettings::new("127.0.0.1", 0),
        retry_interval,
        &sender_kit,
        registry,
    );
    let sender_node = sender_runtime.self_node().clone();
    let local_only = UniqueAddress::new(Address::local("local-only"), 2);
    let publisher = spawn_publisher(&sender_kit, sender_node.clone());
    let cluster = Cluster::new(publisher.clone());
    let snapshots = sender_kit
        .create_probe::<ClusterTcpPeerConnectorSnapshot>("local-only-snapshots")
        .unwrap();

    publisher
        .tell(ClusterEventPublisherMsg::PublishChanges(
            Gossip::from_members([member(sender_node), member(local_only)]),
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
        snapshot.last_error.is_some()
    });
    assert_eq!(snapshot.route_count, 0);
    assert!(snapshot.active_targets.is_empty());
    assert!(snapshot.pending_reconnects.is_empty());
    assert!(
        snapshot
            .last_error
            .as_deref()
            .is_some_and(|error| error.contains("has no remote host"))
    );

    sender_kit.system().stop(&connector);
    assert!(connector.wait_for_stop(Duration::from_secs(1)));
    sender_kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn connector_preserves_successful_route_when_later_snapshot_dial_fails() {
    let _guard = connector_socket_test_lock();
    let sender_kit = ActorSystemTestKit::new("cluster-tcp-peer-connector-partial-sender").unwrap();
    let bound_kit = ActorSystemTestKit::new("cluster-tcp-peer-connector-partial-bound").unwrap();
    let missing_kit =
        ActorSystemTestKit::new("cluster-tcp-peer-connector-partial-missing").unwrap();
    let registry = registry();
    let retry_interval = Duration::from_millis(25);
    let sender_runtime = bind_peer_runtime(
        "partial-sender",
        1,
        11,
        RemoteSettings::new("127.0.0.1", 0),
        retry_interval,
        &sender_kit,
        registry.clone(),
    );
    let sender_cache = sender_runtime.association_cache().clone();
    let bound_port = unused_port();
    let missing_port = unused_port();
    let sender_node = sender_runtime.self_node().clone();
    let bound_node = node("partial-bound", bound_port, 2);
    let missing_node = node("partial-missing", missing_port, 3);
    let (bound_runtime, bound_probes) = bind_association_runtime_on_port_with_probes(
        "partial-bound",
        2,
        22,
        bound_port,
        &bound_kit,
        registry.clone(),
    );
    let publisher = spawn_publisher(&sender_kit, sender_node.clone());
    let cluster = Cluster::new(publisher.clone());
    let snapshots = sender_kit
        .create_probe::<ClusterTcpPeerConnectorSnapshot>("partial-snapshots")
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
        snapshot.route_count == 1 && snapshot.pending_reconnects.len() == 1
    });
    assert_eq!(snapshot.active_targets[0].node(), &bound_node);
    assert_eq!(snapshot.pending_reconnects[0].target.node(), &missing_node);
    assert!(snapshot.last_error.is_some());
    assert_eq!(sender_cache.route_count(), 1);

    let membership_outbound = ClusterMembershipWireOutbound::new(
        bound_node.clone(),
        registry.clone(),
        ClusterMembershipRemoteEnvelopeOutbound::from_arc(
            Arc::new(sender_cache.clone()) as Arc<dyn RemoteOutbound>
        ),
    );
    membership_outbound
        .send_membership(ClusterMembershipMsg::Join {
            join: Join {
                node: sender_node.clone(),
                roles: vec!["partial-active-route".to_string()],
                app_version: crate::ApplicationVersion::default(),
            },
            reply_to: None,
        })
        .unwrap();
    match bound_probes
        .membership
        .expect_msg(Duration::from_secs(1))
        .unwrap()
    {
        ClusterMembershipMsg::Join { join, reply_to } => {
            assert_eq!(join.node, sender_node.clone());
            assert_eq!(join.roles, vec!["partial-active-route".to_string()]);
            assert!(reply_to.is_none());
        }
        other => panic!("expected cluster join, got {other:?}"),
    }

    let missing_runtime = bind_association_runtime_on_port(
        "partial-missing",
        3,
        33,
        missing_port,
        &missing_kit,
        registry,
    );
    connector
        .tell(ClusterTcpPeerConnectorMsg::RetryDuePeerRoutes {
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
fn connector_keeps_route_and_clears_pending_reconnect_when_peer_leaves_membership() {
    let _guard = connector_socket_test_lock();
    let sender_kit =
        ActorSystemTestKit::new("cluster-tcp-peer-connector-mixed-shrink-sender").unwrap();
    let bound_kit =
        ActorSystemTestKit::new("cluster-tcp-peer-connector-mixed-shrink-bound").unwrap();
    let registry = registry();
    let retry_interval = Duration::from_millis(25);
    let sender_runtime = bind_peer_runtime(
        "mixed-shrink-sender",
        1,
        11,
        RemoteSettings::new("127.0.0.1", 0),
        retry_interval,
        &sender_kit,
        registry.clone(),
    );
    let sender_cache = sender_runtime.association_cache().clone();
    let bound_port = unused_port();
    let missing_port = unused_port();
    let sender_node = sender_runtime.self_node().clone();
    let bound_node = node("mixed-shrink-bound", bound_port, 2);
    let missing_node = node("mixed-shrink-missing", missing_port, 3);
    let (bound_runtime, bound_probes) = bind_association_runtime_on_port_with_probes(
        "mixed-shrink-bound",
        2,
        22,
        bound_port,
        &bound_kit,
        registry.clone(),
    );
    let publisher = spawn_publisher(&sender_kit, sender_node.clone());
    let cluster = Cluster::new(publisher.clone());
    let snapshots = sender_kit
        .create_probe::<ClusterTcpPeerConnectorSnapshot>("mixed-shrink-snapshots")
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
        snapshot.route_count == 1 && snapshot.pending_reconnects.len() == 1
    });
    assert_eq!(snapshot.active_targets[0].node(), &bound_node);
    assert_eq!(snapshot.pending_reconnects[0].target.node(), &missing_node);
    assert_eq!(sender_cache.route_count(), 1);

    publisher
        .tell(ClusterEventPublisherMsg::PublishChanges(
            Gossip::from_members([member(sender_node.clone()), member(bound_node.clone())]),
        ))
        .unwrap();
    let snapshot = eventually_snapshot(&connector, &snapshots, |snapshot| {
        snapshot.route_count == 1 && snapshot.pending_reconnects.is_empty()
    });
    assert_eq!(snapshot.active_targets.len(), 1);
    assert_eq!(snapshot.active_targets[0].node(), &bound_node);
    assert!(snapshot.last_error.is_none());
    let report = snapshot
        .last_report
        .expect("membership shrink should record a route report");
    assert!(report.removed.is_empty());
    assert_eq!(report.skipped.len(), 1);
    assert_eq!(report.skipped[0].node(), &missing_node);
    assert_eq!(sender_cache.route_count(), 1);

    let membership_outbound = ClusterMembershipWireOutbound::new(
        bound_node.clone(),
        registry,
        ClusterMembershipRemoteEnvelopeOutbound::from_arc(
            Arc::new(sender_cache.clone()) as Arc<dyn RemoteOutbound>
        ),
    );
    membership_outbound
        .send_membership(ClusterMembershipMsg::Join {
            join: Join {
                node: sender_node.clone(),
                roles: vec!["mixed-shrink-active-route".to_string()],
                app_version: crate::ApplicationVersion::default(),
            },
            reply_to: None,
        })
        .unwrap();
    match bound_probes
        .membership
        .expect_msg(Duration::from_secs(1))
        .unwrap()
    {
        ClusterMembershipMsg::Join { join, reply_to } => {
            assert_eq!(join.node, sender_node);
            assert_eq!(join.roles, vec!["mixed-shrink-active-route".to_string()]);
            assert!(reply_to.is_none());
        }
        other => panic!("expected cluster join, got {other:?}"),
    }

    sender_kit.system().stop(&connector);
    assert!(connector.wait_for_stop(Duration::from_secs(1)));
    assert_eq!(sender_cache.route_count(), 0);
    bound_runtime.shutdown().unwrap();
    sender_kit.shutdown(Duration::from_secs(1)).unwrap();
    bound_kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn connector_keeps_remaining_membership_route_delivering_after_member_removed_event() {
    let _guard = connector_socket_test_lock();
    let sender_kit =
        ActorSystemTestKit::new("cluster-tcp-peer-connector-event-remove-sender").unwrap();
    let second_kit =
        ActorSystemTestKit::new("cluster-tcp-peer-connector-event-remove-second").unwrap();
    let third_kit =
        ActorSystemTestKit::new("cluster-tcp-peer-connector-event-remove-third").unwrap();
    let registry = registry();
    let retry_interval = Duration::from_millis(25);
    let sender_runtime = bind_peer_runtime(
        "event-remove-sender",
        1,
        11,
        RemoteSettings::new("127.0.0.1", 0),
        retry_interval,
        &sender_kit,
        registry.clone(),
    );
    let sender_cache = sender_runtime.association_cache().clone();
    let second_port = unused_port();
    let third_port = unused_port();
    let sender_node = sender_runtime.self_node().clone();
    let second_node = node("event-remove-second", second_port, 2);
    let third_node = node("event-remove-third", third_port, 3);
    let (second_runtime, second_probes) = bind_association_runtime_on_port_with_probes(
        "event-remove-second",
        2,
        22,
        second_port,
        &second_kit,
        registry.clone(),
    );
    let (third_runtime, third_probes) = bind_association_runtime_on_port_with_probes(
        "event-remove-third",
        3,
        33,
        third_port,
        &third_kit,
        registry.clone(),
    );
    let publisher = spawn_publisher(&sender_kit, sender_node.clone());
    let cluster = Cluster::new(publisher.clone());
    let snapshots = sender_kit
        .create_probe::<ClusterTcpPeerConnectorSnapshot>("event-remove-snapshots")
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

    let sender_outbound = Arc::new(sender_cache.clone()) as Arc<dyn RemoteOutbound>;
    let second_outbound = ClusterMembershipWireOutbound::new(
        second_node.clone(),
        registry.clone(),
        ClusterMembershipRemoteEnvelopeOutbound::from_arc(sender_outbound.clone()),
    );
    let third_outbound = ClusterMembershipWireOutbound::new(
        third_node.clone(),
        registry,
        ClusterMembershipRemoteEnvelopeOutbound::from_arc(sender_outbound),
    );

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

    second_outbound
        .send_membership(ClusterMembershipMsg::Join {
            join: Join {
                node: sender_node.clone(),
                roles: vec!["after-connector-member-removed".to_string()],
                app_version: crate::ApplicationVersion::default(),
            },
            reply_to: None,
        })
        .unwrap();
    match second_probes
        .membership
        .expect_msg(Duration::from_secs(1))
        .unwrap()
    {
        ClusterMembershipMsg::Join { join, reply_to } => {
            assert_eq!(join.node, sender_node.clone());
            assert_eq!(
                join.roles,
                vec!["after-connector-member-removed".to_string()]
            );
            assert!(reply_to.is_none());
        }
        other => panic!("expected cluster join, got {other:?}"),
    }

    let removed_error = third_outbound
        .send_membership(ClusterMembershipMsg::Join {
            join: Join {
                node: sender_node,
                roles: vec!["after-connector-removed-member".to_string()],
                app_version: crate::ApplicationVersion::default(),
            },
            reply_to: None,
        })
        .expect_err("removed member route should reject delivery");
    assert!(
        removed_error
            .to_string()
            .contains("no remote association route"),
        "unexpected removed-peer send error: {removed_error:?}"
    );
    third_probes
        .membership
        .expect_no_msg(Duration::from_millis(100))
        .unwrap();

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
fn connector_clear_routes_removes_multiple_active_peer_routes() {
    let _guard = connector_socket_test_lock();
    let sender_kit =
        ActorSystemTestKit::new("cluster-tcp-peer-connector-clear-multi-sender").unwrap();
    let second_kit =
        ActorSystemTestKit::new("cluster-tcp-peer-connector-clear-multi-second").unwrap();
    let third_kit =
        ActorSystemTestKit::new("cluster-tcp-peer-connector-clear-multi-third").unwrap();
    let registry = registry();
    let retry_interval = Duration::from_millis(25);
    let sender_runtime = bind_peer_runtime(
        "clear-multi-sender",
        1,
        11,
        RemoteSettings::new("127.0.0.1", 0),
        retry_interval,
        &sender_kit,
        registry.clone(),
    );
    let sender_cache = sender_runtime.association_cache().clone();
    let second_port = unused_port();
    let third_port = unused_port();
    let sender_node = sender_runtime.self_node().clone();
    let second_node = node("clear-multi-second", second_port, 2);
    let third_node = node("clear-multi-third", third_port, 3);
    let publisher = spawn_publisher(&sender_kit, sender_node.clone());
    let cluster = Cluster::new(publisher.clone());
    let snapshots = sender_kit
        .create_probe::<ClusterTcpPeerConnectorSnapshot>("clear-multi-snapshots")
        .unwrap();

    let second_runtime = bind_association_runtime_on_port(
        "clear-multi-second",
        2,
        22,
        second_port,
        &second_kit,
        registry.clone(),
    );
    let third_runtime = bind_association_runtime_on_port(
        "clear-multi-third",
        3,
        33,
        third_port,
        &third_kit,
        registry,
    );
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

    connector
        .tell(ClusterTcpPeerConnectorMsg::ClearRoutes)
        .unwrap();
    let snapshot =
        eventually_snapshot(&connector, &snapshots, |snapshot| snapshot.route_count == 0);

    assert!(snapshot.active_targets.is_empty());
    let report = snapshot
        .last_report
        .expect("clear routes should record a report");
    assert_eq!(report.removed.len(), 2);
    assert!(
        report
            .removed
            .iter()
            .any(|target| target.node() == &second_node)
    );
    assert!(
        report
            .removed
            .iter()
            .any(|target| target.node() == &third_node)
    );
    assert!(report.dialed.is_empty());
    assert!(report.skipped.is_empty());
    assert!(snapshot.last_error.is_none());
    assert_eq!(sender_cache.route_count(), 0);

    sender_kit.system().stop(&connector);
    assert!(connector.wait_for_stop(Duration::from_secs(1)));
    publish_changes_and_wait(
        &sender_kit,
        &publisher,
        Gossip::from_members([member(sender_node)]),
        "clear-multi-after-stop-state",
    );
    assert!(sender_kit.system().dead_letters().is_empty());
    second_runtime.shutdown().unwrap();
    third_runtime.shutdown().unwrap();
    sender_kit.shutdown(Duration::from_secs(1)).unwrap();
    second_kit.shutdown(Duration::from_secs(1)).unwrap();
    third_kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn connector_clear_routes_preserves_pending_reconnects() {
    let _guard = connector_socket_test_lock();
    let sender_kit =
        ActorSystemTestKit::new("cluster-tcp-peer-connector-clear-pending-sender").unwrap();
    let registry = registry();
    let retry_interval = Duration::from_millis(25);
    let sender_runtime = bind_peer_runtime(
        "clear-pending-sender",
        1,
        11,
        RemoteSettings::new("127.0.0.1", 0),
        retry_interval,
        &sender_kit,
        registry,
    );
    let sender_cache = sender_runtime.association_cache().clone();
    let receiver_port = unused_port();
    let sender_node = sender_runtime.self_node().clone();
    let receiver_node = node("clear-pending-receiver", receiver_port, 2);
    let publisher = spawn_publisher(&sender_kit, sender_node.clone());
    let cluster = Cluster::new(publisher.clone());
    let snapshots = sender_kit
        .create_probe::<ClusterTcpPeerConnectorSnapshot>("clear-pending-snapshots")
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
    assert!(snapshot.last_error.is_some());

    connector
        .tell(ClusterTcpPeerConnectorMsg::ClearRoutes)
        .unwrap();
    let snapshot = eventually_snapshot(&connector, &snapshots, |snapshot| {
        snapshot.last_report.is_some() && snapshot.last_error.is_none()
    });

    assert_eq!(snapshot.route_count, 0);
    assert!(snapshot.active_targets.is_empty());
    assert_eq!(snapshot.pending_reconnects.len(), 1);
    assert_eq!(snapshot.pending_reconnects[0].target.node(), &receiver_node);
    let report = snapshot
        .last_report
        .expect("clear routes should record an empty active-route report");
    assert!(report.dialed.is_empty());
    assert!(report.removed.is_empty());
    assert!(report.skipped.is_empty());
    assert_eq!(sender_cache.route_count(), 0);

    sender_kit.system().stop(&connector);
    assert!(connector.wait_for_stop(Duration::from_secs(1)));
    sender_kit.shutdown(Duration::from_secs(1)).unwrap();
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
fn connector_stop_clears_late_routes_registered_during_shutdown() {
    let _guard = connector_socket_test_lock();
    let sender_kit =
        ActorSystemTestKit::new("cluster-tcp-peer-connector-late-route-sender").unwrap();
    let receiver_kit =
        ActorSystemTestKit::new("cluster-tcp-peer-connector-late-route-receiver").unwrap();
    let registry = registry();
    let retry_interval = Duration::from_millis(25);
    let sender_runtime = bind_peer_runtime(
        "late-route-sender",
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
    let receiver_node = node("late-route-receiver", receiver_port, 2);
    let publisher = spawn_publisher(&sender_kit, sender_node.clone());
    let cluster = Cluster::new(publisher.clone());
    let snapshots = sender_kit
        .create_probe::<ClusterTcpPeerConnectorSnapshot>("late-route-snapshots")
        .unwrap();

    let receiver_runtime = bind_association_runtime_on_port(
        "late-route-receiver",
        2,
        22,
        receiver_port,
        &receiver_kit,
        registry,
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
    let initial_address =
        RemoteAssociationAddress::new("kairo", "connector-initial", "127.0.0.1", Some(2552))
            .unwrap();
    let late_address =
        RemoteAssociationAddress::new("kairo", "connector-late", "127.0.0.1", Some(2553)).unwrap();
    sender_cache.insert_route(
        initial_address,
        Arc::new(LateRouteOnClose {
            cache: sender_cache.clone(),
            late_address,
        }),
    );
    assert_eq!(sender_cache.route_count(), 2);

    sender_kit.system().stop(&connector);
    assert!(connector.wait_for_stop(Duration::from_secs(1)));

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
