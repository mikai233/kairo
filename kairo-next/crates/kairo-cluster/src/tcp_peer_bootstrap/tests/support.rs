use std::sync::Arc;
use std::time::Duration;

use kairo_actor::{ActorRef, PHASE_BEFORE_CLUSTER_SHUTDOWN, Props};
use kairo_remote::{RemoteAssociationCache, RemoteOutbound, RemoteSettings};
use kairo_serialization::{ActorRefWireData, Registry};
use kairo_testkit::{ActorSystemTestKit, TestProbe, await_assert};

use crate::{
    ClusterEventPublisher, ClusterEventPublisherMsg, ClusterMembershipMsg,
    ClusterMembershipWireInbound, ClusterSystemInbound, ClusterTcpAssociationRuntime,
    ClusterTcpPeerConnectorMsg, ClusterTcpPeerConnectorSnapshot, ClusterTcpPeerRuntime,
    CurrentClusterState, DEFAULT_CLUSTER_HEARTBEAT_RECEIVER_PATH,
    DEFAULT_CLUSTER_HEARTBEAT_SENDER_PATH, Gossip, HeartbeatRemoteReceiverInbound,
    HeartbeatRemoteResponseInbound, HeartbeatSenderMsg, Member, MemberStatus, UniqueAddress,
    register_cluster_protocol_codecs,
};

pub(super) struct ClusterInboundProbes {
    pub(super) membership: TestProbe<ClusterMembershipMsg>,
}

pub(super) fn registry() -> Arc<Registry> {
    let mut registry = Registry::new();
    register_cluster_protocol_codecs(&mut registry).unwrap();
    Arc::new(registry)
}

pub(super) fn bind_runtime(
    system: &str,
    node_uid: u64,
    system_uid: u64,
    kit: &ActorSystemTestKit,
) -> ClusterTcpPeerRuntime {
    bind_runtime_with_probes(system, node_uid, system_uid, kit).0
}

pub(super) fn bind_runtime_with_probes(
    system: &str,
    node_uid: u64,
    system_uid: u64,
    kit: &ActorSystemTestKit,
) -> (ClusterTcpPeerRuntime, ClusterInboundProbes) {
    let registry = registry();
    let membership = kit
        .create_probe::<ClusterMembershipMsg>("membership")
        .unwrap();
    let heartbeat_sender = kit
        .create_probe::<HeartbeatSenderMsg>("heartbeat-sender")
        .unwrap();
    let membership_ref = membership.actor_ref();
    let heartbeat_sender_ref = heartbeat_sender.actor_ref();
    let runtime = ClusterTcpPeerRuntime::bind(
        system,
        node_uid,
        system_uid,
        RemoteSettings::new("127.0.0.1", 0).with_connect_timeout(Duration::from_millis(10)),
        move |self_node, cache| {
            ClusterSystemInbound::new(self_node.clone())
                .with_membership(ClusterMembershipWireInbound::new(
                    self_node.clone(),
                    registry.clone(),
                    membership_ref,
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
                    heartbeat_sender_ref,
                ))
        },
    )
    .unwrap();
    (runtime, ClusterInboundProbes { membership })
}

pub(super) fn bind_association_runtime_on_port(
    system: &str,
    node_uid: u64,
    system_uid: u64,
    port: u16,
    kit: &ActorSystemTestKit,
    registry: Arc<Registry>,
) -> ClusterTcpAssociationRuntime {
    bind_association_runtime_on_port_with_probes(system, node_uid, system_uid, port, kit, registry)
        .0
}

pub(super) fn bind_association_runtime_on_port_with_probes(
    system: &str,
    node_uid: u64,
    system_uid: u64,
    port: u16,
    kit: &ActorSystemTestKit,
    registry: Arc<Registry>,
) -> (ClusterTcpAssociationRuntime, ClusterInboundProbes) {
    let membership = kit
        .create_probe::<ClusterMembershipMsg>(format!("{system}-membership"))
        .unwrap();
    let heartbeat_sender = kit
        .create_probe::<HeartbeatSenderMsg>(format!("{system}-heartbeat-sender"))
        .unwrap();
    let membership_ref = membership.actor_ref();
    let heartbeat_sender_ref = heartbeat_sender.actor_ref();
    ClusterTcpAssociationRuntime::bind(
        system,
        node_uid,
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

fn wire_for(node: &UniqueAddress, path: &str) -> ActorRefWireData {
    ActorRefWireData::new(format!("{}{}", node.address, path)).unwrap()
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

pub(super) fn publish_gossip_and_wait(
    kit: &ActorSystemTestKit,
    publisher: &ActorRef<ClusterEventPublisherMsg>,
    gossip: Gossip,
    probe_name: &str,
) {
    publish_gossip(publisher, gossip);
    let state = kit.create_probe::<CurrentClusterState>(probe_name).unwrap();
    publisher
        .tell(ClusterEventPublisherMsg::SendCurrentState {
            reply_to: state.actor_ref(),
        })
        .unwrap();
    state.expect_msg(Duration::from_secs(1)).unwrap();
}

pub(super) fn await_cache_route_count(cache: &RemoteAssociationCache, expected: usize) {
    await_assert(
        Duration::from_secs(5),
        Duration::from_millis(10),
        || -> Result<(), String> {
            let actual = cache.route_count();
            if actual == expected {
                Ok(())
            } else {
                Err(format!(
                    "expected {expected} association routes, found {actual}: {:?}",
                    cache.route_addresses()
                ))
            }
        },
    )
    .unwrap();
}

pub(super) fn await_connector_no_routes(
    connector: &ActorRef<ClusterTcpPeerConnectorMsg>,
    snapshots: &TestProbe<ClusterTcpPeerConnectorSnapshot>,
) {
    await_assert(
        Duration::from_secs(1),
        Duration::from_millis(10),
        || -> Result<(), String> {
            connector
                .tell(ClusterTcpPeerConnectorMsg::Snapshot {
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
    connector: &ActorRef<ClusterTcpPeerConnectorMsg>,
    snapshots: &TestProbe<ClusterTcpPeerConnectorSnapshot>,
) {
    await_assert(
        Duration::from_secs(1),
        Duration::from_millis(10),
        || -> Result<(), String> {
            connector
                .tell(ClusterTcpPeerConnectorMsg::Snapshot {
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
    connector: &ActorRef<ClusterTcpPeerConnectorMsg>,
    snapshots: &TestProbe<ClusterTcpPeerConnectorSnapshot>,
    expected_peer: &UniqueAddress,
) {
    await_assert(
        Duration::from_secs(1),
        Duration::from_millis(10),
        || -> Result<(), String> {
            connector
                .tell(ClusterTcpPeerConnectorMsg::Snapshot {
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

pub(super) fn await_connector_routes_and_pending_reconnect(
    connector: &ActorRef<ClusterTcpPeerConnectorMsg>,
    snapshots: &TestProbe<ClusterTcpPeerConnectorSnapshot>,
    expected_routes: &[UniqueAddress],
    expected_pending: &UniqueAddress,
) {
    await_assert(
        Duration::from_secs(1),
        Duration::from_millis(10),
        || -> Result<(), String> {
            connector
                .tell(ClusterTcpPeerConnectorMsg::Snapshot {
                    reply_to: snapshots.actor_ref(),
                })
                .map_err(|error| error.reason().to_string())?;
            let snapshot = snapshots
                .expect_msg(Duration::from_millis(100))
                .map_err(|error| error.to_string())?;
            let has_all_expected_routes = expected_routes.iter().all(|expected_peer| {
                snapshot
                    .active_targets
                    .iter()
                    .any(|target| target.node() == expected_peer)
            });
            let has_expected_pending = snapshot
                .pending_reconnects
                .iter()
                .any(|pending| pending.target.node() == expected_pending);
            if snapshot.route_count == expected_routes.len()
                && has_all_expected_routes
                && has_expected_pending
            {
                Ok(())
            } else {
                Err(format!("unexpected connector snapshot: {snapshot:?}"))
            }
        },
    )
    .unwrap();
}

pub(super) fn await_connector_routes_without_pending(
    connector: &ActorRef<ClusterTcpPeerConnectorMsg>,
    snapshots: &TestProbe<ClusterTcpPeerConnectorSnapshot>,
    expected_routes: &[UniqueAddress],
) -> ClusterTcpPeerConnectorSnapshot {
    let mut matched = None;
    await_assert(
        Duration::from_secs(1),
        Duration::from_millis(10),
        || -> Result<(), String> {
            connector
                .tell(ClusterTcpPeerConnectorMsg::Snapshot {
                    reply_to: snapshots.actor_ref(),
                })
                .map_err(|error| error.reason().to_string())?;
            let snapshot = snapshots
                .expect_msg(Duration::from_millis(100))
                .map_err(|error| error.to_string())?;
            let has_all_expected_routes = expected_routes.iter().all(|expected_peer| {
                snapshot
                    .active_targets
                    .iter()
                    .any(|target| target.node() == expected_peer)
            });
            if snapshot.route_count == expected_routes.len()
                && has_all_expected_routes
                && snapshot.pending_reconnects.is_empty()
            {
                matched = Some(snapshot);
                Ok(())
            } else {
                Err(format!("unexpected connector snapshot: {snapshot:?}"))
            }
        },
    )
    .unwrap();
    matched.expect("matching connector snapshot should be captured")
}

pub(super) fn await_connector_route(
    connector: &ActorRef<ClusterTcpPeerConnectorMsg>,
    snapshots: &TestProbe<ClusterTcpPeerConnectorSnapshot>,
    expected_peer: &UniqueAddress,
) {
    await_connector_routes(connector, snapshots, std::slice::from_ref(expected_peer));
}

pub(super) fn await_connector_routes(
    connector: &ActorRef<ClusterTcpPeerConnectorMsg>,
    snapshots: &TestProbe<ClusterTcpPeerConnectorSnapshot>,
    expected_peers: &[UniqueAddress],
) {
    await_assert(
        Duration::from_secs(1),
        Duration::from_millis(10),
        || -> Result<(), String> {
            connector
                .tell(ClusterTcpPeerConnectorMsg::Snapshot {
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
    connector: &ActorRef<ClusterTcpPeerConnectorMsg>,
    snapshot: &ClusterTcpPeerConnectorSnapshot,
) -> Result<(), String> {
    if let Some(now) = snapshot
        .pending_reconnects
        .iter()
        .map(|pending| pending.next_retry_at)
        .max()
    {
        connector
            .tell(ClusterTcpPeerConnectorMsg::RetryDuePeerRoutes { now })
            .map_err(|error| error.reason().to_string())?;
    }
    Ok(())
}

pub(super) fn run_bootstrap_shutdown(
    kit: &ActorSystemTestKit,
    connector: &ActorRef<ClusterTcpPeerConnectorMsg>,
) {
    kit.system()
        .coordinated_shutdown()
        .run_from("test", Some(PHASE_BEFORE_CLUSTER_SHUTDOWN))
        .unwrap();
    assert!(connector.wait_for_stop(Duration::from_secs(1)));
}

pub(super) fn bootstrap_socket_test_lock() -> crate::test_support::SocketTestGuard {
    crate::test_support::cluster_socket_test_lock()
}
