use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use kairo_actor::{ActorRef, PHASE_BEFORE_CLUSTER_SHUTDOWN, Props};
use kairo_cluster::{
    ClusterEventPublisher, ClusterEventPublisherMsg, CurrentClusterState, Gossip, Member,
    MemberStatus, UniqueAddress,
};
use kairo_remote::{RemoteAssociationCache, RemoteSettings};
use kairo_serialization::{MessageCodec, Registry, RemoteMessage, SerializationRegistry};
use kairo_testkit::{ActorSystemTestKit, TestProbe, await_assert};

use crate::{
    ClusterToolsSystemInbound, ClusterToolsTcpAssociationRuntime, ClusterToolsTcpPeerConnectorMsg,
    ClusterToolsTcpPeerConnectorSnapshot, ClusterToolsTcpPeerRuntime, DistributedPubSubMediatorMsg,
    PubSubGossipMsg, PubSubGossipWireInbound, PubSubRemoteDeliveryInbound, SingletonManagerMsg,
    SingletonManagerRemoteInbound, register_cluster_tools_protocol_codecs,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TestMessage {
    pub(super) value: u8,
}

impl RemoteMessage for TestMessage {
    const MANIFEST: &'static str = "kairo.cluster-tools.test.peer-bootstrap-message";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, Copy)]
struct TestMessageCodec;

impl MessageCodec<TestMessage> for TestMessageCodec {
    fn serializer_id(&self) -> u32 {
        59_205
    }

    fn encode(&self, message: &TestMessage) -> kairo_serialization::Result<Bytes> {
        Ok(Bytes::from(vec![message.value]))
    }

    fn decode(&self, payload: Bytes, _version: u16) -> kairo_serialization::Result<TestMessage> {
        Ok(TestMessage { value: payload[0] })
    }
}

pub(super) struct ClusterToolsInboundProbes {
    pub(super) mediator: TestProbe<DistributedPubSubMediatorMsg<TestMessage>>,
}

pub(super) fn registry() -> Arc<Registry> {
    let mut registry = Registry::new();
    register_cluster_tools_protocol_codecs(&mut registry).unwrap();
    registry
        .register::<TestMessage, _>(TestMessageCodec)
        .unwrap();
    Arc::new(registry)
}

pub(super) fn inbound_for(
    name: &str,
    kit: &ActorSystemTestKit,
    registry: Arc<Registry>,
    self_node: UniqueAddress,
) -> ClusterToolsSystemInbound<TestMessage> {
    let gossip = kit
        .create_probe::<PubSubGossipMsg>(format!("{name}-gossip"))
        .unwrap();
    let mediator = kit
        .create_probe::<DistributedPubSubMediatorMsg<TestMessage>>(format!("{name}-mediator"))
        .unwrap();
    let manager = kit
        .create_probe::<SingletonManagerMsg>(format!("{name}-singleton-manager"))
        .unwrap();
    inbound_from_refs(
        self_node,
        registry,
        gossip.actor_ref(),
        mediator.actor_ref(),
        manager.actor_ref(),
    )
}

pub(super) fn bind_runtime(
    system: &str,
    node_uid: u64,
    system_uid: u64,
    kit: &ActorSystemTestKit,
    registry: Arc<Registry>,
) -> ClusterToolsTcpPeerRuntime<TestMessage> {
    bind_runtime_with_probes(system, node_uid, system_uid, kit, registry).0
}

pub(super) fn bind_runtime_with_probes(
    system: &str,
    node_uid: u64,
    system_uid: u64,
    kit: &ActorSystemTestKit,
    registry: Arc<Registry>,
) -> (
    ClusterToolsTcpPeerRuntime<TestMessage>,
    ClusterToolsInboundProbes,
) {
    let gossip = kit
        .create_probe::<PubSubGossipMsg>(format!("{system}-gossip"))
        .unwrap();
    let mediator = kit
        .create_probe::<DistributedPubSubMediatorMsg<TestMessage>>(format!("{system}-mediator"))
        .unwrap();
    let manager = kit
        .create_probe::<SingletonManagerMsg>(format!("{system}-singleton-manager"))
        .unwrap();
    let gossip_ref = gossip.actor_ref();
    let mediator_ref = mediator.actor_ref();
    let manager_ref = manager.actor_ref();
    let runtime = ClusterToolsTcpPeerRuntime::bind(
        system,
        node_uid,
        system_uid,
        RemoteSettings::new("127.0.0.1", 0).with_connect_timeout(Duration::from_millis(10)),
        move |self_node| {
            inbound_from_refs(self_node, registry, gossip_ref, mediator_ref, manager_ref)
        },
    )
    .unwrap();
    (runtime, ClusterToolsInboundProbes { mediator })
}

pub(super) fn bind_association_runtime_on_port(
    system: &str,
    node_uid: u64,
    system_uid: u64,
    port: u16,
    kit: &ActorSystemTestKit,
    registry: Arc<Registry>,
) -> ClusterToolsTcpAssociationRuntime<TestMessage> {
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
) -> (
    ClusterToolsTcpAssociationRuntime<TestMessage>,
    ClusterToolsInboundProbes,
) {
    let gossip = kit
        .create_probe::<PubSubGossipMsg>(format!("{system}-gossip"))
        .unwrap();
    let mediator = kit
        .create_probe::<DistributedPubSubMediatorMsg<TestMessage>>(format!("{system}-mediator"))
        .unwrap();
    let manager = kit
        .create_probe::<SingletonManagerMsg>(format!("{system}-singleton-manager"))
        .unwrap();
    let gossip_ref = gossip.actor_ref();
    let mediator_ref = mediator.actor_ref();
    let manager_ref = manager.actor_ref();
    ClusterToolsTcpAssociationRuntime::bind(
        system,
        node_uid,
        system_uid,
        RemoteSettings::new("127.0.0.1", port),
        move |self_node| {
            inbound_from_refs(
                self_node,
                registry.clone(),
                gossip_ref.clone(),
                mediator_ref.clone(),
                manager_ref.clone(),
            )
        },
    )
    .map(|runtime| (runtime, ClusterToolsInboundProbes { mediator }))
    .unwrap()
}

fn inbound_from_refs(
    self_node: UniqueAddress,
    registry: Arc<Registry>,
    gossip: ActorRef<PubSubGossipMsg>,
    mediator: ActorRef<DistributedPubSubMediatorMsg<TestMessage>>,
    manager: ActorRef<SingletonManagerMsg>,
) -> ClusterToolsSystemInbound<TestMessage> {
    ClusterToolsSystemInbound::new(self_node.clone())
        .with_pubsub_gossip(PubSubGossipWireInbound::new(
            self_node.clone(),
            registry.clone(),
            gossip,
        ))
        .with_pubsub_delivery(PubSubRemoteDeliveryInbound::new(
            self_node.clone(),
            registry.clone(),
            mediator,
        ))
        .with_singleton_manager(SingletonManagerRemoteInbound::new(
            self_node, registry, manager,
        ))
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
    connector: &ActorRef<ClusterToolsTcpPeerConnectorMsg>,
    snapshots: &TestProbe<ClusterToolsTcpPeerConnectorSnapshot>,
) {
    await_assert(
        Duration::from_secs(1),
        Duration::from_millis(10),
        || -> Result<(), String> {
            connector
                .tell(ClusterToolsTcpPeerConnectorMsg::Snapshot {
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
    connector: &ActorRef<ClusterToolsTcpPeerConnectorMsg>,
    snapshots: &TestProbe<ClusterToolsTcpPeerConnectorSnapshot>,
) {
    await_assert(
        Duration::from_secs(1),
        Duration::from_millis(10),
        || -> Result<(), String> {
            connector
                .tell(ClusterToolsTcpPeerConnectorMsg::Snapshot {
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
    connector: &ActorRef<ClusterToolsTcpPeerConnectorMsg>,
    snapshots: &TestProbe<ClusterToolsTcpPeerConnectorSnapshot>,
    expected_peer: &UniqueAddress,
) {
    await_assert(
        Duration::from_secs(1),
        Duration::from_millis(10),
        || -> Result<(), String> {
            connector
                .tell(ClusterToolsTcpPeerConnectorMsg::Snapshot {
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
    connector: &ActorRef<ClusterToolsTcpPeerConnectorMsg>,
    snapshots: &TestProbe<ClusterToolsTcpPeerConnectorSnapshot>,
    expected_routes: &[UniqueAddress],
    expected_pending: &UniqueAddress,
) {
    await_assert(
        Duration::from_secs(1),
        Duration::from_millis(10),
        || -> Result<(), String> {
            connector
                .tell(ClusterToolsTcpPeerConnectorMsg::Snapshot {
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
    connector: &ActorRef<ClusterToolsTcpPeerConnectorMsg>,
    snapshots: &TestProbe<ClusterToolsTcpPeerConnectorSnapshot>,
    expected_routes: &[UniqueAddress],
) -> ClusterToolsTcpPeerConnectorSnapshot {
    let mut matched = None;
    await_assert(
        Duration::from_secs(1),
        Duration::from_millis(10),
        || -> Result<(), String> {
            connector
                .tell(ClusterToolsTcpPeerConnectorMsg::Snapshot {
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
    connector: &ActorRef<ClusterToolsTcpPeerConnectorMsg>,
    snapshots: &TestProbe<ClusterToolsTcpPeerConnectorSnapshot>,
    expected_peer: &UniqueAddress,
) {
    await_connector_routes(connector, snapshots, std::slice::from_ref(expected_peer));
}

pub(super) fn await_connector_routes(
    connector: &ActorRef<ClusterToolsTcpPeerConnectorMsg>,
    snapshots: &TestProbe<ClusterToolsTcpPeerConnectorSnapshot>,
    expected_peers: &[UniqueAddress],
) {
    await_assert(
        Duration::from_secs(1),
        Duration::from_millis(10),
        || -> Result<(), String> {
            connector
                .tell(ClusterToolsTcpPeerConnectorMsg::Snapshot {
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
    connector: &ActorRef<ClusterToolsTcpPeerConnectorMsg>,
    snapshot: &ClusterToolsTcpPeerConnectorSnapshot,
) -> Result<(), String> {
    if let Some(now) = snapshot
        .pending_reconnects
        .iter()
        .map(|pending| pending.next_retry_at)
        .max()
    {
        connector
            .tell(ClusterToolsTcpPeerConnectorMsg::RetryDuePeerRoutes { now })
            .map_err(|error| error.reason().to_string())?;
    }
    Ok(())
}

pub(super) fn run_bootstrap_shutdown(
    kit: &ActorSystemTestKit,
    connector: &ActorRef<ClusterToolsTcpPeerConnectorMsg>,
) {
    kit.system()
        .coordinated_shutdown()
        .run_from("test", Some(PHASE_BEFORE_CLUSTER_SHUTDOWN))
        .unwrap();
    assert!(connector.wait_for_stop(Duration::from_secs(1)));
}

pub(super) fn bootstrap_socket_test_lock() -> crate::test_support::SocketTestGuard {
    crate::test_support::cluster_tools_socket_test_lock()
}
