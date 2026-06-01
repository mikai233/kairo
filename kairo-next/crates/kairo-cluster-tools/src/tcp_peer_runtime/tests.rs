use std::net::TcpListener;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use kairo_actor::Address;
use kairo_cluster::{CurrentClusterState, Member, MemberStatus, ReachabilityEvent, UniqueAddress};
use kairo_remote::RemoteSettings;
use kairo_serialization::{MessageCodec, Registry, SerializationRegistry};
use kairo_testkit::ActorSystemTestKit;

use super::*;
use crate::{
    ClusterToolsSystemInbound, DistributedPubSubMediatorMsg, PubSubGossipMsg,
    PubSubGossipWireInbound, PubSubRemoteDeliveryInbound, SingletonManagerMsg,
    SingletonManagerRemoteInbound, register_cluster_tools_protocol_codecs,
};

#[derive(Debug, Clone, PartialEq, Eq)]
struct TestMessage {
    value: u8,
}

impl RemoteMessage for TestMessage {
    const MANIFEST: &'static str = "kairo.cluster-tools.test.peer-runtime-message";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, Copy)]
struct TestMessageCodec;

impl MessageCodec<TestMessage> for TestMessageCodec {
    fn serializer_id(&self) -> u32 {
        59_203
    }

    fn encode(&self, message: &TestMessage) -> kairo_serialization::Result<Bytes> {
        Ok(Bytes::from(vec![message.value]))
    }

    fn decode(&self, payload: Bytes, _version: u16) -> kairo_serialization::Result<TestMessage> {
        Ok(TestMessage { value: payload[0] })
    }
}

fn registry() -> Arc<Registry> {
    let mut registry = Registry::new();
    register_cluster_tools_protocol_codecs(&mut registry).unwrap();
    registry
        .register::<TestMessage, _>(TestMessageCodec)
        .unwrap();
    Arc::new(registry)
}

fn node(system: &str, port: u16, uid: u64) -> UniqueAddress {
    UniqueAddress::new(
        Address::new("kairo", system, Some("127.0.0.1".to_string()), Some(port)),
        uid,
    )
}

fn member(node: UniqueAddress) -> Member {
    Member::new(node, vec![]).with_status(MemberStatus::Up)
}

fn state(members: Vec<Member>, unreachable: Vec<Member>) -> CurrentClusterState {
    CurrentClusterState {
        members,
        unreachable,
        seen_by: std::collections::HashSet::new(),
        leader: None,
        role_leaders: std::collections::HashMap::new(),
        member_tombstones: std::collections::HashSet::new(),
    }
}

fn unused_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

fn wait_for_route(runtime: &ClusterToolsTcpAssociationRuntime<TestMessage>) {
    let deadline = Instant::now() + Duration::from_secs(1);
    while runtime.association_cache().route_count() == 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(runtime.association_cache().route_count(), 1);
}

fn inbound_for(
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
    ClusterToolsSystemInbound::new(self_node.clone())
        .with_pubsub_gossip(PubSubGossipWireInbound::new(
            self_node.clone(),
            registry.clone(),
            gossip.actor_ref(),
        ))
        .with_pubsub_delivery(PubSubRemoteDeliveryInbound::new(
            self_node.clone(),
            registry.clone(),
            mediator.actor_ref(),
        ))
        .with_singleton_manager(SingletonManagerRemoteInbound::new(
            self_node,
            registry,
            manager.actor_ref(),
        ))
}

fn bind_peer_runtime(
    name: &str,
    uid: u64,
    system_uid: u64,
    kit: &ActorSystemTestKit,
    registry: Arc<Registry>,
) -> ClusterToolsTcpPeerRuntime<TestMessage> {
    ClusterToolsTcpPeerRuntime::bind(
        name,
        uid,
        system_uid,
        RemoteSettings::new("127.0.0.1", 0),
        move |self_node| inbound_for(name, kit, registry, self_node),
    )
    .unwrap()
}

fn bind_peer_runtime_with_reconnect(
    name: &str,
    uid: u64,
    system_uid: u64,
    settings: RemoteSettings,
    reconnect_settings: ClusterToolsTcpPeerReconnectSettings,
    kit: &ActorSystemTestKit,
    registry: Arc<Registry>,
) -> ClusterToolsTcpPeerRuntime<TestMessage> {
    ClusterToolsTcpPeerRuntime::bind_with_reconnect(
        name,
        uid,
        system_uid,
        settings,
        reconnect_settings,
        move |self_node| inbound_for(name, kit, registry, self_node),
    )
    .unwrap()
}

fn bind_association_runtime(
    name: &str,
    uid: u64,
    system_uid: u64,
    kit: &ActorSystemTestKit,
    registry: Arc<Registry>,
) -> ClusterToolsTcpAssociationRuntime<TestMessage> {
    ClusterToolsTcpAssociationRuntime::bind(
        name,
        uid,
        system_uid,
        RemoteSettings::new("127.0.0.1", 0),
        move |self_node| inbound_for(name, kit, registry, self_node),
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
) -> ClusterToolsTcpAssociationRuntime<TestMessage> {
    ClusterToolsTcpAssociationRuntime::bind(
        name,
        uid,
        system_uid,
        RemoteSettings::new("127.0.0.1", port),
        move |self_node| inbound_for(name, kit, registry, self_node),
    )
    .unwrap()
}

#[test]
fn peer_runtime_applies_snapshot_and_reachability_event_to_live_routes() {
    let sender_kit = ActorSystemTestKit::new("cluster-tools-peer-runtime-sender").unwrap();
    let receiver_kit = ActorSystemTestKit::new("cluster-tools-peer-runtime-receiver").unwrap();
    let registry = registry();
    let mut sender = bind_peer_runtime("sender", 1, 11, &sender_kit, registry.clone());
    let receiver = bind_association_runtime("receiver", 2, 22, &receiver_kit, registry);

    let report = sender
        .apply_snapshot(state(
            vec![
                member(sender.self_node().clone()),
                member(receiver.self_node().clone()),
            ],
            vec![],
        ))
        .unwrap();
    assert_eq!(report.dialed.len(), 1);
    assert_eq!(sender.peer_route_count(), 1);
    assert_eq!(sender.association_cache().route_count(), 1);
    wait_for_route(&receiver);

    let report = sender
        .apply_event(ClusterEvent::Reachability(ReachabilityEvent::Unreachable(
            member(receiver.self_node().clone()),
        )))
        .unwrap();
    assert_eq!(report.removed.len(), 1);
    assert_eq!(sender.peer_route_count(), 0);
    assert_eq!(sender.association_cache().route_count(), 0);

    let sender_report = sender.shutdown().unwrap();
    assert_eq!(sender_report.peer_routes.removed.len(), 0);
    assert_eq!(sender_report.listener.accepted_associations, 0);
    let receiver_report = receiver.shutdown().unwrap();
    assert_eq!(receiver_report.accepted_associations, 1);
    sender_kit.shutdown(Duration::from_secs(1)).unwrap();
    receiver_kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn peer_runtime_retries_failed_peer_dial_after_retry_interval() {
    let sender_kit = ActorSystemTestKit::new("cluster-tools-peer-runtime-retry-sender").unwrap();
    let receiver_kit =
        ActorSystemTestKit::new("cluster-tools-peer-runtime-retry-receiver").unwrap();
    let registry = registry();
    let receiver_port = unused_port();
    let retry_interval = Duration::from_millis(25);
    let mut sender = bind_peer_runtime_with_reconnect(
        "sender",
        1,
        11,
        RemoteSettings::new("127.0.0.1", 0),
        ClusterToolsTcpPeerReconnectSettings::new(retry_interval).unwrap(),
        &sender_kit,
        registry.clone(),
    );
    let receiver_node = node("receiver", receiver_port, 2);

    let error = sender
        .apply_snapshot_at(
            state(
                vec![
                    member(sender.self_node().clone()),
                    member(receiver_node.clone()),
                ],
                vec![],
            ),
            Duration::ZERO,
        )
        .unwrap_err();

    assert!(matches!(error, ClusterToolsTcpPeerRuntimeError::Route(_)));
    assert_eq!(sender.peer_route_count(), 0);
    assert_eq!(sender.pending_peer_reconnect_count(), 1);
    let pending = sender.pending_peer_reconnects();
    assert_eq!(pending[0].target.node(), &receiver_node);
    assert_eq!(pending[0].attempts, 1);
    assert_eq!(pending[0].next_retry_at, retry_interval);

    let report = sender
        .retry_due_peer_routes(retry_interval - Duration::from_millis(1))
        .unwrap();
    assert!(report.is_empty());
    assert_eq!(sender.pending_peer_reconnect_count(), 1);

    let receiver =
        bind_association_runtime_on_port("receiver", 2, 22, receiver_port, &receiver_kit, registry);
    let report = sender.retry_due_peer_routes(retry_interval).unwrap();

    assert_eq!(report.dialed.len(), 1);
    assert_eq!(sender.peer_route_count(), 1);
    assert_eq!(sender.pending_peer_reconnect_count(), 0);
    wait_for_route(&receiver);

    let sender_report = sender.shutdown().unwrap();
    assert_eq!(sender_report.peer_routes.removed.len(), 1);
    assert!(sender_report.pending_reconnects.is_empty());
    let receiver_report = receiver.shutdown().unwrap();
    assert_eq!(receiver_report.accepted_associations, 1);
    sender_kit.shutdown(Duration::from_secs(1)).unwrap();
    receiver_kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn peer_runtime_shutdown_clears_pending_reconnects_after_failed_dial() {
    let kit =
        ActorSystemTestKit::new("cluster-tools-peer-runtime-shutdown-pending-reconnect").unwrap();
    let registry = registry();
    let receiver_port = unused_port();
    let retry_interval = Duration::from_millis(25);
    let mut runtime = bind_peer_runtime_with_reconnect(
        "sender",
        1,
        11,
        RemoteSettings::new("127.0.0.1", 0),
        ClusterToolsTcpPeerReconnectSettings::new(retry_interval).unwrap(),
        &kit,
        registry,
    );
    let receiver_node = node("receiver", receiver_port, 2);

    runtime
        .apply_snapshot_at(
            state(
                vec![
                    member(runtime.self_node().clone()),
                    member(receiver_node.clone()),
                ],
                vec![],
            ),
            Duration::ZERO,
        )
        .unwrap_err();

    assert_eq!(runtime.peer_route_count(), 0);
    assert_eq!(runtime.pending_peer_reconnect_count(), 1);

    let report = runtime.shutdown().unwrap();

    assert!(report.peer_routes.is_empty());
    assert_eq!(report.pending_reconnects.cleared.len(), 1);
    assert_eq!(report.pending_reconnects.cleared[0].node(), &receiver_node);
    assert!(report.pending_reconnects.scheduled.is_empty());
    assert_eq!(report.listener.accepted_associations, 0);
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn peer_runtime_clears_pending_reconnect_when_peer_is_removed() {
    let kit = ActorSystemTestKit::new("cluster-tools-peer-runtime-retry-removed").unwrap();
    let registry = registry();
    let receiver_port = unused_port();
    let mut runtime = bind_peer_runtime_with_reconnect(
        "sender",
        1,
        11,
        RemoteSettings::new("127.0.0.1", 0),
        ClusterToolsTcpPeerReconnectSettings::new(Duration::from_millis(25)).unwrap(),
        &kit,
        registry,
    );
    let receiver_node = node("receiver", receiver_port, 2);

    runtime
        .apply_snapshot_at(
            state(
                vec![
                    member(runtime.self_node().clone()),
                    member(receiver_node.clone()),
                ],
                vec![],
            ),
            Duration::ZERO,
        )
        .unwrap_err();
    assert_eq!(runtime.pending_peer_reconnect_count(), 1);

    let report = runtime
        .apply_event(ClusterEvent::Reachability(ReachabilityEvent::Unreachable(
            member(receiver_node),
        )))
        .unwrap();

    assert_eq!(report.skipped.len(), 1);
    assert_eq!(runtime.pending_peer_reconnect_count(), 0);
    runtime.shutdown().unwrap();
    kit.shutdown(Duration::from_secs(1)).unwrap();
}
