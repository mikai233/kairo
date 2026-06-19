use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use kairo_actor::{Address, Recipient};
use kairo_cluster::{
    CurrentClusterState, Member, MemberEvent, MemberStatus, Reachability, ReachabilityEvent,
    UniqueAddress,
};
use kairo_remote::{
    RemoteAssociationAddress, RemoteAssociationCache, RemoteOutbound, RemoteSettings,
};
use kairo_serialization::{MessageCodec, Registry, SerializationRegistry};
use kairo_testkit::{ActorSystemTestKit, TestProbe, await_assert};

use super::*;
use crate::{
    ClusterToolsSystemInbound, DistributedPubSubMediatorMsg, LocalPubSubMsg, PubSubGossipMsg,
    PubSubGossipWireInbound, PubSubRemoteDeliveryInbound, PubSubRemoteDeliveryOutbound,
    SingletonManagerMsg, SingletonManagerRemoteInbound, TopicName, TopicPublishMode,
    register_cluster_tools_protocol_codecs, test_support::cluster_tools_socket_test_lock,
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

#[derive(Default)]
struct NoopOutbound;

impl RemoteOutbound for NoopOutbound {
    fn send(&self, _envelope: kairo_serialization::RemoteEnvelope) -> kairo_remote::Result<()> {
        Ok(())
    }
}

struct LateRouteOnClose {
    cache: RemoteAssociationCache,
    late_address: RemoteAssociationAddress,
}

impl RemoteOutbound for LateRouteOnClose {
    fn send(&self, _envelope: kairo_serialization::RemoteEnvelope) -> kairo_remote::Result<()> {
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

struct ClusterToolsInboundProbes {
    mediator: TestProbe<DistributedPubSubMediatorMsg<TestMessage>>,
}

fn unused_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

fn assert_pubsub_publish(
    probes: &ClusterToolsInboundProbes,
    expected_topic: TopicName,
    expected_message: TestMessage,
) {
    match probes.mediator.expect_msg(Duration::from_secs(1)).unwrap() {
        DistributedPubSubMediatorMsg::LocalDelivery(LocalPubSubMsg::Publish {
            topic,
            message,
            mode,
            reply_to,
        }) => {
            assert_eq!(topic, expected_topic);
            assert_eq!(message, expected_message);
            assert_eq!(mode, TopicPublishMode::Broadcast);
            assert!(reply_to.is_none());
        }
        _ => panic!("expected pubsub publish delivery"),
    }
}

fn wait_for_route(runtime: &ClusterToolsTcpAssociationRuntime<TestMessage>) {
    await_assert(
        Duration::from_secs(1),
        Duration::from_millis(1),
        || -> Result<(), String> {
            let actual = runtime.association_cache().route_count();
            if actual == 1 {
                Ok(())
            } else {
                Err(format!("expected 1 association route, found {actual}"))
            }
        },
    )
    .unwrap();
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

fn bind_association_runtime_with_probes(
    name: &str,
    uid: u64,
    system_uid: u64,
    kit: &ActorSystemTestKit,
    registry: Arc<Registry>,
) -> (
    ClusterToolsTcpAssociationRuntime<TestMessage>,
    ClusterToolsInboundProbes,
) {
    let gossip = kit
        .create_probe::<PubSubGossipMsg>(format!("{name}-gossip"))
        .unwrap();
    let mediator = kit
        .create_probe::<DistributedPubSubMediatorMsg<TestMessage>>(format!("{name}-mediator"))
        .unwrap();
    let manager = kit
        .create_probe::<SingletonManagerMsg>(format!("{name}-singleton-manager"))
        .unwrap();
    let gossip_ref = gossip.actor_ref();
    let mediator_ref = mediator.actor_ref();
    let manager_ref = manager.actor_ref();
    let runtime = ClusterToolsTcpAssociationRuntime::bind(
        name,
        uid,
        system_uid,
        RemoteSettings::new("127.0.0.1", 0),
        move |self_node| {
            ClusterToolsSystemInbound::new(self_node.clone())
                .with_pubsub_gossip(PubSubGossipWireInbound::new(
                    self_node.clone(),
                    registry.clone(),
                    gossip_ref,
                ))
                .with_pubsub_delivery(PubSubRemoteDeliveryInbound::new(
                    self_node.clone(),
                    registry.clone(),
                    mediator_ref,
                ))
                .with_singleton_manager(SingletonManagerRemoteInbound::new(
                    self_node,
                    registry,
                    manager_ref,
                ))
        },
    )
    .unwrap();
    (runtime, ClusterToolsInboundProbes { mediator })
}

#[test]
fn peer_runtime_applies_snapshot_and_reachability_event_to_live_routes() {
    let _guard = cluster_tools_socket_test_lock();
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
fn peer_runtime_keeps_remaining_route_when_one_peer_is_removed() {
    let _guard = cluster_tools_socket_test_lock();
    let sender_kit = ActorSystemTestKit::new("cluster-tools-peer-runtime-reduce-sender").unwrap();
    let second_kit = ActorSystemTestKit::new("cluster-tools-peer-runtime-reduce-second").unwrap();
    let third_kit = ActorSystemTestKit::new("cluster-tools-peer-runtime-reduce-third").unwrap();
    let registry = registry();
    let mut sender = bind_peer_runtime("reduce-sender", 1, 11, &sender_kit, registry.clone());
    let (second, second_probes) =
        bind_association_runtime_with_probes("reduce-second", 2, 22, &second_kit, registry.clone());
    let (third, third_probes) =
        bind_association_runtime_with_probes("reduce-third", 3, 33, &third_kit, registry.clone());
    let sender_node = sender.self_node().clone();
    let second_node = second.self_node().clone();
    let third_node = third.self_node().clone();
    let outbound = Arc::new(sender.association_cache().clone()) as Arc<dyn RemoteOutbound>;
    let second_outbound = PubSubRemoteDeliveryOutbound::<TestMessage>::from_arc(
        second_node.clone(),
        registry.clone(),
        outbound.clone(),
    );
    let third_outbound = PubSubRemoteDeliveryOutbound::<TestMessage>::from_arc(
        third_node.clone(),
        registry,
        outbound,
    );

    let report = sender
        .apply_snapshot(state(
            vec![
                member(sender_node.clone()),
                member(second_node.clone()),
                member(third_node.clone()),
            ],
            vec![],
        ))
        .unwrap();
    assert_eq!(report.dialed.len(), 2);
    assert_eq!(sender.peer_route_count(), 2);
    assert_eq!(sender.association_cache().route_count(), 2);
    wait_for_route(&second);
    wait_for_route(&third);
    second_outbound
        .tell(LocalPubSubMsg::Publish {
            topic: TopicName::new("orders"),
            message: TestMessage { value: 21 },
            mode: TopicPublishMode::Broadcast,
            reply_to: None,
        })
        .unwrap();
    third_outbound
        .tell(LocalPubSubMsg::Publish {
            topic: TopicName::new("invoices"),
            message: TestMessage { value: 34 },
            mode: TopicPublishMode::Broadcast,
            reply_to: None,
        })
        .unwrap();
    assert_pubsub_publish(
        &second_probes,
        TopicName::new("orders"),
        TestMessage { value: 21 },
    );
    assert_pubsub_publish(
        &third_probes,
        TopicName::new("invoices"),
        TestMessage { value: 34 },
    );

    let report = sender
        .apply_snapshot(state(
            vec![member(sender_node), member(second_node.clone())],
            vec![],
        ))
        .unwrap();
    assert_eq!(report.removed.len(), 1);
    assert_eq!(report.removed[0].node(), &third_node);
    assert_eq!(sender.peer_route_count(), 1);
    assert_eq!(sender.association_cache().route_count(), 1);
    assert!(
        sender
            .active_peer_targets()
            .iter()
            .any(|target| target.node() == &second_node)
    );
    let removed_peer_error = third_outbound
        .tell(LocalPubSubMsg::Publish {
            topic: TopicName::new("invoices"),
            message: TestMessage { value: 89 },
            mode: TopicPublishMode::Broadcast,
            reply_to: None,
        })
        .expect_err("removed peer route should reject sends");
    assert!(
        removed_peer_error
            .reason()
            .contains("no remote association route"),
        "unexpected removed-peer send error: {removed_peer_error:?}"
    );
    third_probes
        .mediator
        .expect_no_msg(Duration::from_millis(100))
        .unwrap();
    second_outbound
        .tell(LocalPubSubMsg::Publish {
            topic: TopicName::new("orders"),
            message: TestMessage { value: 55 },
            mode: TopicPublishMode::Broadcast,
            reply_to: None,
        })
        .unwrap();
    assert_pubsub_publish(
        &second_probes,
        TopicName::new("orders"),
        TestMessage { value: 55 },
    );

    let sender_report = sender.shutdown().unwrap();
    assert_eq!(sender_report.peer_routes.removed.len(), 1);
    assert!(sender_report.pending_reconnects.is_empty());
    assert_eq!(sender_report.listener.accepted_associations, 0);
    let second_report = second.shutdown().unwrap();
    assert_eq!(second_report.accepted_associations, 1);
    let third_report = third.shutdown().unwrap();
    assert_eq!(third_report.accepted_associations, 1);
    sender_kit.shutdown(Duration::from_secs(1)).unwrap();
    second_kit.shutdown(Duration::from_secs(1)).unwrap();
    third_kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn peer_runtime_replaces_routes_on_reachability_changed_self_observer_set() {
    let _guard = cluster_tools_socket_test_lock();
    let sender_kit =
        ActorSystemTestKit::new("cluster-tools-peer-runtime-reachability-change-sender").unwrap();
    let first_kit =
        ActorSystemTestKit::new("cluster-tools-peer-runtime-reachability-change-first").unwrap();
    let second_kit =
        ActorSystemTestKit::new("cluster-tools-peer-runtime-reachability-change-second").unwrap();
    let registry = registry();
    let mut sender = bind_peer_runtime(
        "reachability-change-sender",
        1,
        11,
        &sender_kit,
        registry.clone(),
    );
    let (first, first_probes) = bind_association_runtime_with_probes(
        "reachability-change-first",
        2,
        22,
        &first_kit,
        registry.clone(),
    );
    let (second, second_probes) = bind_association_runtime_with_probes(
        "reachability-change-second",
        3,
        33,
        &second_kit,
        registry.clone(),
    );
    let sender_node = sender.self_node().clone();
    let first_node = first.self_node().clone();
    let second_node = second.self_node().clone();
    let observer = node("reachability-change-observer", 2662, 4);
    let outbound = Arc::new(sender.association_cache().clone()) as Arc<dyn RemoteOutbound>;
    let first_outbound = PubSubRemoteDeliveryOutbound::<TestMessage>::from_arc(
        first_node.clone(),
        registry.clone(),
        outbound.clone(),
    );
    let second_outbound = PubSubRemoteDeliveryOutbound::<TestMessage>::from_arc(
        second_node.clone(),
        registry,
        outbound,
    );

    let report = sender
        .apply_snapshot(state(
            vec![
                member(sender_node.clone()),
                member(first_node.clone()),
                member(second_node.clone()),
            ],
            vec![],
        ))
        .unwrap();
    assert_eq!(report.dialed.len(), 2);
    assert_eq!(sender.peer_route_count(), 2);
    assert_eq!(sender.association_cache().route_count(), 2);
    wait_for_route(&first);
    wait_for_route(&second);

    let report = sender
        .apply_event(ClusterEvent::ReachabilityChanged {
            reachability: Reachability::new()
                .unreachable(sender_node.clone(), first_node.clone())
                .unreachable(observer, second_node.clone()),
        })
        .unwrap();
    assert_eq!(report.removed.len(), 1);
    assert_eq!(report.removed[0].node(), &first_node);
    assert!(report.dialed.is_empty());
    assert_eq!(sender.peer_route_count(), 1);
    assert_eq!(sender.association_cache().route_count(), 1);
    second_outbound
        .tell(LocalPubSubMsg::Publish {
            topic: TopicName::new("orders"),
            message: TestMessage { value: 81 },
            mode: TopicPublishMode::Broadcast,
            reply_to: None,
        })
        .unwrap();
    assert_pubsub_publish(
        &second_probes,
        TopicName::new("orders"),
        TestMessage { value: 81 },
    );
    let first_error = first_outbound
        .tell(LocalPubSubMsg::Publish {
            topic: TopicName::new("orders"),
            message: TestMessage { value: 82 },
            mode: TopicPublishMode::Broadcast,
            reply_to: None,
        })
        .expect_err("self-unreachable peer route should reject sends");
    assert!(
        first_error.reason().contains("no remote association route"),
        "unexpected first-peer send error: {first_error:?}"
    );
    first_probes
        .mediator
        .expect_no_msg(Duration::from_millis(100))
        .unwrap();

    let report = sender
        .apply_event(ClusterEvent::ReachabilityChanged {
            reachability: Reachability::new()
                .unreachable(sender_node.clone(), second_node.clone())
                .unreachable(first_node.clone(), second_node.clone()),
        })
        .unwrap();
    assert_eq!(report.removed.len(), 1);
    assert_eq!(report.removed[0].node(), &second_node);
    assert_eq!(report.dialed.len(), 1);
    assert_eq!(report.dialed[0].node(), &first_node);
    assert_eq!(sender.peer_route_count(), 1);
    assert_eq!(sender.association_cache().route_count(), 1);
    first_outbound
        .tell(LocalPubSubMsg::Publish {
            topic: TopicName::new("orders"),
            message: TestMessage { value: 83 },
            mode: TopicPublishMode::Broadcast,
            reply_to: None,
        })
        .unwrap();
    assert_pubsub_publish(
        &first_probes,
        TopicName::new("orders"),
        TestMessage { value: 83 },
    );
    let second_error = second_outbound
        .tell(LocalPubSubMsg::Publish {
            topic: TopicName::new("orders"),
            message: TestMessage { value: 84 },
            mode: TopicPublishMode::Broadcast,
            reply_to: None,
        })
        .expect_err("new self-unreachable peer route should reject sends");
    assert!(
        second_error
            .reason()
            .contains("no remote association route"),
        "unexpected second-peer send error: {second_error:?}"
    );
    second_probes
        .mediator
        .expect_no_msg(Duration::from_millis(100))
        .unwrap();

    let sender_report = sender.shutdown().unwrap();
    assert_eq!(sender_report.peer_routes.removed.len(), 1);
    assert!(sender_report.pending_reconnects.is_empty());
    assert_eq!(sender_report.listener.accepted_associations, 0);
    let first_report = first.shutdown().unwrap();
    assert_eq!(first_report.accepted_associations, 2);
    let second_report = second.shutdown().unwrap();
    assert_eq!(second_report.accepted_associations, 1);
    sender_kit.shutdown(Duration::from_secs(1)).unwrap();
    first_kit.shutdown(Duration::from_secs(1)).unwrap();
    second_kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn peer_runtime_keeps_remaining_route_delivering_after_member_removed_event() {
    let _guard = cluster_tools_socket_test_lock();
    let sender_kit =
        ActorSystemTestKit::new("cluster-tools-peer-runtime-event-remove-sender").unwrap();
    let second_kit =
        ActorSystemTestKit::new("cluster-tools-peer-runtime-event-remove-second").unwrap();
    let third_kit =
        ActorSystemTestKit::new("cluster-tools-peer-runtime-event-remove-third").unwrap();
    let registry = registry();
    let mut sender = bind_peer_runtime("event-remove-sender", 1, 11, &sender_kit, registry.clone());
    let (second, second_probes) = bind_association_runtime_with_probes(
        "event-remove-second",
        2,
        22,
        &second_kit,
        registry.clone(),
    );
    let (third, third_probes) = bind_association_runtime_with_probes(
        "event-remove-third",
        3,
        33,
        &third_kit,
        registry.clone(),
    );
    let sender_node = sender.self_node().clone();
    let second_node = second.self_node().clone();
    let third_node = third.self_node().clone();
    let outbound = Arc::new(sender.association_cache().clone()) as Arc<dyn RemoteOutbound>;
    let second_outbound = PubSubRemoteDeliveryOutbound::<TestMessage>::from_arc(
        second_node.clone(),
        registry.clone(),
        outbound.clone(),
    );
    let third_outbound = PubSubRemoteDeliveryOutbound::<TestMessage>::from_arc(
        third_node.clone(),
        registry,
        outbound,
    );

    let report = sender
        .apply_snapshot(state(
            vec![
                member(sender_node.clone()),
                member(second_node.clone()),
                member(third_node.clone()),
            ],
            vec![],
        ))
        .unwrap();
    assert_eq!(report.dialed.len(), 2);
    assert_eq!(sender.peer_route_count(), 2);
    assert_eq!(sender.association_cache().route_count(), 2);
    wait_for_route(&second);
    wait_for_route(&third);

    let report = sender
        .apply_event(ClusterEvent::Member(MemberEvent::Removed {
            member: member(third_node.clone()).with_status(MemberStatus::Removed),
            previous_status: MemberStatus::Up,
        }))
        .unwrap();
    assert_eq!(report.removed.len(), 1);
    assert_eq!(report.removed[0].node(), &third_node);
    assert_eq!(sender.peer_route_count(), 1);
    assert_eq!(sender.association_cache().route_count(), 1);
    assert!(
        sender
            .active_peer_targets()
            .iter()
            .any(|target| target.node() == &second_node)
    );

    second_outbound
        .tell(LocalPubSubMsg::Publish {
            topic: TopicName::new("orders"),
            message: TestMessage { value: 64 },
            mode: TopicPublishMode::Broadcast,
            reply_to: None,
        })
        .unwrap();
    assert_pubsub_publish(
        &second_probes,
        TopicName::new("orders"),
        TestMessage { value: 64 },
    );
    let removed_peer_error = third_outbound
        .tell(LocalPubSubMsg::Publish {
            topic: TopicName::new("invoices"),
            message: TestMessage { value: 91 },
            mode: TopicPublishMode::Broadcast,
            reply_to: None,
        })
        .expect_err("removed member route should reject sends");
    assert!(
        removed_peer_error
            .reason()
            .contains("no remote association route"),
        "unexpected removed-member send error: {removed_peer_error:?}"
    );
    third_probes
        .mediator
        .expect_no_msg(Duration::from_millis(100))
        .unwrap();

    let sender_report = sender.shutdown().unwrap();
    assert_eq!(sender_report.peer_routes.removed.len(), 1);
    assert!(sender_report.pending_reconnects.is_empty());
    assert_eq!(sender_report.listener.accepted_associations, 0);
    let second_report = second.shutdown().unwrap();
    assert_eq!(second_report.accepted_associations, 1);
    let third_report = third.shutdown().unwrap();
    assert_eq!(third_report.accepted_associations, 1);
    sender_kit.shutdown(Duration::from_secs(1)).unwrap();
    second_kit.shutdown(Duration::from_secs(1)).unwrap();
    third_kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn peer_runtime_clears_routes_when_self_member_is_removed() {
    let _guard = cluster_tools_socket_test_lock();
    let sender_kit =
        ActorSystemTestKit::new("cluster-tools-peer-runtime-self-remove-sender").unwrap();
    let receiver_kit =
        ActorSystemTestKit::new("cluster-tools-peer-runtime-self-remove-receiver").unwrap();
    let registry = registry();
    let mut sender = bind_peer_runtime("self-remove-sender", 1, 11, &sender_kit, registry.clone());
    let (receiver, receiver_probes) = bind_association_runtime_with_probes(
        "self-remove-receiver",
        2,
        22,
        &receiver_kit,
        registry.clone(),
    );
    let sender_node = sender.self_node().clone();
    let receiver_node = receiver.self_node().clone();
    let outbound = PubSubRemoteDeliveryOutbound::<TestMessage>::from_arc(
        receiver_node.clone(),
        registry,
        Arc::new(sender.association_cache().clone()) as Arc<dyn RemoteOutbound>,
    );

    sender
        .apply_snapshot(state(
            vec![member(sender_node.clone()), member(receiver_node.clone())],
            vec![],
        ))
        .unwrap();
    assert_eq!(sender.peer_route_count(), 1);
    assert_eq!(sender.association_cache().route_count(), 1);
    wait_for_route(&receiver);

    outbound
        .tell(LocalPubSubMsg::Publish {
            topic: TopicName::new("orders"),
            message: TestMessage { value: 21 },
            mode: TopicPublishMode::Broadcast,
            reply_to: None,
        })
        .unwrap();
    assert_pubsub_publish(
        &receiver_probes,
        TopicName::new("orders"),
        TestMessage { value: 21 },
    );

    let report = sender
        .apply_event(ClusterEvent::Member(MemberEvent::Removed {
            member: member(sender_node).with_status(MemberStatus::Removed),
            previous_status: MemberStatus::Up,
        }))
        .unwrap();

    assert_eq!(report.removed.len(), 1);
    assert_eq!(report.removed[0].node(), &receiver_node);
    assert_eq!(sender.peer_route_count(), 0);
    assert_eq!(sender.association_cache().route_count(), 0);

    let removed_route_error = outbound
        .tell(LocalPubSubMsg::Publish {
            topic: TopicName::new("orders"),
            message: TestMessage { value: 22 },
            mode: TopicPublishMode::Broadcast,
            reply_to: None,
        })
        .expect_err("self-removed peer runtime should clear outbound routes");
    assert!(
        removed_route_error
            .reason()
            .contains("no remote association route"),
        "unexpected self-removal send error: {removed_route_error:?}"
    );
    receiver_probes
        .mediator
        .expect_no_msg(Duration::from_millis(100))
        .unwrap();

    let sender_report = sender.shutdown().unwrap();
    assert_eq!(sender_report.peer_routes.removed.len(), 0);
    assert!(sender_report.pending_reconnects.is_empty());
    assert_eq!(sender_report.listener.accepted_associations, 0);
    let receiver_report = receiver.shutdown().unwrap();
    assert_eq!(receiver_report.accepted_associations, 1);
    sender_kit.shutdown(Duration::from_secs(1)).unwrap();
    receiver_kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn peer_runtime_shutdown_clears_active_peer_routes_before_listener_stop() {
    let _guard = cluster_tools_socket_test_lock();
    let sender_kit = ActorSystemTestKit::new("cluster-tools-peer-runtime-shutdown-sender").unwrap();
    let receiver_kit =
        ActorSystemTestKit::new("cluster-tools-peer-runtime-shutdown-receiver").unwrap();
    let registry = registry();
    let mut sender = bind_peer_runtime("sender", 1, 11, &sender_kit, registry.clone());
    let receiver = bind_association_runtime("receiver", 2, 22, &receiver_kit, registry);

    sender
        .apply_snapshot(state(
            vec![
                member(sender.self_node().clone()),
                member(receiver.self_node().clone()),
            ],
            vec![],
        ))
        .unwrap();
    assert_eq!(sender.peer_route_count(), 1);
    assert_eq!(sender.association_cache().route_count(), 1);
    wait_for_route(&receiver);

    let sender_report = sender.shutdown().unwrap();

    assert_eq!(sender_report.peer_routes.removed.len(), 1);
    assert!(sender_report.pending_reconnects.is_empty());
    assert_eq!(sender_report.listener.accepted_associations, 0);
    let receiver_report = receiver.shutdown().unwrap();
    assert_eq!(receiver_report.accepted_associations, 1);
    sender_kit.shutdown(Duration::from_secs(1)).unwrap();
    receiver_kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn peer_runtime_shutdown_clears_late_routes_registered_during_shutdown() {
    let _guard = cluster_tools_socket_test_lock();
    let sender_kit =
        ActorSystemTestKit::new("cluster-tools-peer-runtime-late-route-sender").unwrap();
    let receiver_kit =
        ActorSystemTestKit::new("cluster-tools-peer-runtime-late-route-receiver").unwrap();
    let registry = registry();
    let mut sender = bind_peer_runtime("sender", 1, 11, &sender_kit, registry.clone());
    let receiver = bind_association_runtime("receiver", 2, 22, &receiver_kit, registry);

    sender
        .apply_snapshot(state(
            vec![
                member(sender.self_node().clone()),
                member(receiver.self_node().clone()),
            ],
            vec![],
        ))
        .unwrap();
    wait_for_route(&receiver);
    let cache = sender.association_cache().clone();
    let initial_address =
        RemoteAssociationAddress::new("kairo", "initial", "127.0.0.1", Some(2552)).unwrap();
    let late_address =
        RemoteAssociationAddress::new("kairo", "late", "127.0.0.1", Some(2553)).unwrap();
    cache.insert_route(
        initial_address,
        Arc::new(LateRouteOnClose {
            cache: cache.clone(),
            late_address,
        }),
    );
    assert_eq!(sender.peer_route_count(), 1);
    assert_eq!(cache.route_count(), 2);

    let sender_report = sender.shutdown().unwrap();

    assert_eq!(sender_report.peer_routes.removed.len(), 1);
    assert!(sender_report.pending_reconnects.is_empty());
    assert_eq!(sender_report.listener.accepted_associations, 0);
    assert_eq!(cache.route_count(), 0);
    let receiver_report = receiver.shutdown().unwrap();
    assert_eq!(receiver_report.accepted_associations, 1);
    sender_kit.shutdown(Duration::from_secs(1)).unwrap();
    receiver_kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn peer_runtime_shutdown_clears_multiple_active_peer_routes() {
    let _guard = cluster_tools_socket_test_lock();
    let sender_kit =
        ActorSystemTestKit::new("cluster-tools-peer-runtime-multi-shutdown-sender").unwrap();
    let second_kit =
        ActorSystemTestKit::new("cluster-tools-peer-runtime-multi-shutdown-second").unwrap();
    let third_kit =
        ActorSystemTestKit::new("cluster-tools-peer-runtime-multi-shutdown-third").unwrap();
    let registry = registry();
    let mut sender = bind_peer_runtime(
        "multi-shutdown-sender",
        1,
        11,
        &sender_kit,
        registry.clone(),
    );
    let second = bind_association_runtime(
        "multi-shutdown-second",
        2,
        22,
        &second_kit,
        registry.clone(),
    );
    let third = bind_association_runtime("multi-shutdown-third", 3, 33, &third_kit, registry);
    let second_node = second.self_node().clone();
    let third_node = third.self_node().clone();

    sender
        .apply_snapshot(state(
            vec![
                member(sender.self_node().clone()),
                member(second_node.clone()),
                member(third_node.clone()),
            ],
            vec![],
        ))
        .unwrap();
    assert_eq!(sender.peer_route_count(), 2);
    assert_eq!(sender.association_cache().route_count(), 2);
    wait_for_route(&second);
    wait_for_route(&third);

    let sender_report = sender.shutdown().unwrap();

    assert_eq!(sender_report.peer_routes.removed.len(), 2);
    assert!(
        sender_report
            .peer_routes
            .removed
            .iter()
            .any(|target| target.node() == &second_node)
    );
    assert!(
        sender_report
            .peer_routes
            .removed
            .iter()
            .any(|target| target.node() == &third_node)
    );
    assert!(sender_report.pending_reconnects.is_empty());
    assert_eq!(sender_report.listener.accepted_associations, 0);
    let second_report = second.shutdown().unwrap();
    assert_eq!(second_report.accepted_associations, 1);
    let third_report = third.shutdown().unwrap();
    assert_eq!(third_report.accepted_associations, 1);
    sender_kit.shutdown(Duration::from_secs(1)).unwrap();
    second_kit.shutdown(Duration::from_secs(1)).unwrap();
    third_kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn peer_runtime_rejects_non_remote_peer_snapshot_without_dialing() {
    let _guard = cluster_tools_socket_test_lock();
    let kit = ActorSystemTestKit::new("cluster-tools-peer-runtime-local-only").unwrap();
    let registry = registry();
    let mut runtime = bind_peer_runtime("local", 1, 11, &kit, registry);
    let local_only = UniqueAddress::new(Address::local("local-only"), 2);

    let error = runtime
        .apply_snapshot(state(vec![member(local_only)], vec![]))
        .unwrap_err();

    assert!(matches!(
        error,
        ClusterToolsTcpPeerRuntimeError::Peer(
            kairo_cluster::ClusterAssociationPeerError::MissingRemoteHost { .. }
        )
    ));
    assert_eq!(runtime.peer_route_count(), 0);
    assert_eq!(runtime.association_cache().route_count(), 0);
    runtime.shutdown().unwrap();
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn peer_runtime_retries_failed_peer_dial_after_retry_interval() {
    let _guard = cluster_tools_socket_test_lock();
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
fn peer_runtime_preserves_successful_routes_when_later_snapshot_dial_fails() {
    let _guard = cluster_tools_socket_test_lock();
    let sender_kit = ActorSystemTestKit::new("cluster-tools-peer-runtime-partial-sender").unwrap();
    let bound_kit = ActorSystemTestKit::new("cluster-tools-peer-runtime-partial-bound").unwrap();
    let missing_kit =
        ActorSystemTestKit::new("cluster-tools-peer-runtime-partial-missing").unwrap();
    let registry = registry();
    let bound_port = unused_port();
    let missing_port = unused_port();
    let bound_node = node("partial-bound", bound_port, 2);
    let missing_node = node("partial-missing", missing_port, 3);
    let retry_interval = Duration::from_millis(25);
    let mut sender = bind_peer_runtime_with_reconnect(
        "partial-sender",
        1,
        11,
        RemoteSettings::new("127.0.0.1", 0),
        ClusterToolsTcpPeerReconnectSettings::new(retry_interval).unwrap(),
        &sender_kit,
        registry.clone(),
    );
    let sender_node = sender.self_node().clone();
    let bound = bind_association_runtime_on_port(
        "partial-bound",
        2,
        22,
        bound_port,
        &bound_kit,
        registry.clone(),
    );

    let error = sender
        .apply_snapshot_at(
            state(
                vec![
                    member(sender_node),
                    member(bound_node.clone()),
                    member(missing_node.clone()),
                ],
                vec![],
            ),
            Duration::ZERO,
        )
        .unwrap_err();

    assert!(matches!(error, ClusterToolsTcpPeerRuntimeError::Route(_)));
    assert_eq!(sender.peer_route_count(), 1);
    assert_eq!(sender.association_cache().route_count(), 1);
    wait_for_route(&bound);
    let pending = sender.pending_peer_reconnects();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].target.node(), &missing_node);
    assert_eq!(pending[0].attempts, 1);
    assert_eq!(pending[0].next_retry_at, retry_interval);

    let missing = bind_association_runtime_on_port(
        "partial-missing",
        3,
        33,
        missing_port,
        &missing_kit,
        registry,
    );
    let report = sender.retry_due_peer_routes(retry_interval).unwrap();

    assert_eq!(report.dialed.len(), 1);
    assert_eq!(report.dialed[0].node(), &missing_node);
    assert_eq!(sender.peer_route_count(), 2);
    assert_eq!(sender.association_cache().route_count(), 2);
    assert_eq!(sender.pending_peer_reconnect_count(), 0);
    wait_for_route(&missing);

    let sender_report = sender.shutdown().unwrap();
    assert_eq!(sender_report.peer_routes.removed.len(), 2);
    assert!(sender_report.pending_reconnects.is_empty());
    let bound_report = bound.shutdown().unwrap();
    assert_eq!(bound_report.accepted_associations, 1);
    let missing_report = missing.shutdown().unwrap();
    assert_eq!(missing_report.accepted_associations, 1);
    sender_kit.shutdown(Duration::from_secs(1)).unwrap();
    bound_kit.shutdown(Duration::from_secs(1)).unwrap();
    missing_kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn peer_runtime_clear_pending_reconnects_preserves_active_routes() {
    let _guard = cluster_tools_socket_test_lock();
    let sender_kit =
        ActorSystemTestKit::new("cluster-tools-peer-runtime-clear-pending-sender").unwrap();
    let bound_kit =
        ActorSystemTestKit::new("cluster-tools-peer-runtime-clear-pending-bound").unwrap();
    let registry = registry();
    let bound_port = unused_port();
    let missing_port = unused_port();
    let bound_node = node("clear-pending-bound", bound_port, 2);
    let missing_node = node("clear-pending-missing", missing_port, 3);
    let retry_interval = Duration::from_millis(25);
    let mut sender = bind_peer_runtime_with_reconnect(
        "clear-pending-sender",
        1,
        11,
        RemoteSettings::new("127.0.0.1", 0),
        ClusterToolsTcpPeerReconnectSettings::new(retry_interval).unwrap(),
        &sender_kit,
        registry.clone(),
    );
    let sender_node = sender.self_node().clone();
    let bound = bind_association_runtime_on_port(
        "clear-pending-bound",
        2,
        22,
        bound_port,
        &bound_kit,
        registry,
    );

    sender
        .apply_snapshot_at(
            state(
                vec![
                    member(sender_node),
                    member(bound_node.clone()),
                    member(missing_node.clone()),
                ],
                vec![],
            ),
            Duration::ZERO,
        )
        .unwrap_err();

    assert_eq!(sender.peer_route_count(), 1);
    assert_eq!(sender.association_cache().route_count(), 1);
    wait_for_route(&bound);
    let pending = sender.pending_peer_reconnects();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].target.node(), &missing_node);

    let report = sender.clear_pending_peer_reconnects();

    assert_eq!(report.cleared.len(), 1);
    assert_eq!(report.cleared[0].node(), &missing_node);
    assert!(report.scheduled.is_empty());
    assert_eq!(sender.pending_peer_reconnect_count(), 0);
    assert_eq!(sender.peer_route_count(), 1);
    assert_eq!(sender.association_cache().route_count(), 1);

    let sender_report = sender.shutdown().unwrap();
    assert_eq!(sender_report.peer_routes.removed.len(), 1);
    assert!(sender_report.pending_reconnects.is_empty());
    let bound_report = bound.shutdown().unwrap();
    assert_eq!(bound_report.accepted_associations, 1);
    sender_kit.shutdown(Duration::from_secs(1)).unwrap();
    bound_kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn peer_runtime_shutdown_clears_pending_reconnects_after_failed_dial() {
    let _guard = cluster_tools_socket_test_lock();
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
fn peer_runtime_clear_routes_preserves_pending_reconnects() {
    let _guard = cluster_tools_socket_test_lock();
    let kit =
        ActorSystemTestKit::new("cluster-tools-peer-runtime-clear-pending-reconnect").unwrap();
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

    let report = runtime.clear_peer_routes();

    assert!(report.is_empty());
    assert_eq!(runtime.peer_route_count(), 0);
    let pending = runtime.pending_peer_reconnects();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].target.node(), &receiver_node);
    assert_eq!(pending[0].attempts, 1);
    assert_eq!(pending[0].next_retry_at, retry_interval);

    let shutdown = runtime.shutdown().unwrap();
    assert!(shutdown.peer_routes.is_empty());
    assert_eq!(shutdown.pending_reconnects.cleared.len(), 1);
    assert_eq!(
        shutdown.pending_reconnects.cleared[0].node(),
        &receiver_node
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn peer_runtime_clears_pending_reconnect_when_peer_is_removed() {
    let _guard = cluster_tools_socket_test_lock();
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
