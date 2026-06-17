use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use kairo_actor::{Address, Props, Recipient};
use kairo_cluster::UniqueAddress;
use kairo_remote::{RemoteAssociationAddress, RemoteAssociationCache, RemoteOutbound, Result};
use kairo_serialization::{
    ActorRefWireData, Manifest, MessageCodec, Registry, RemoteEnvelope, RemoteMessage,
    SerializationRegistry, SerializedMessage,
};
use kairo_testkit::ActorSystemTestKit;

use super::*;
use crate::{
    DistributedPubSubMediatorActor, DistributedPubSubMediatorMsg, LocalPubSubMsg,
    PUBSUB_PATH_SERIALIZER_ID, PUBSUB_PUBLISH_SERIALIZER_ID, PubSubPathEnvelope,
    PubSubPublishEnvelope, PubSubRegistryState, PubSubRemoteTarget, TopicName, TopicPublishMode,
    register_cluster_tools_protocol_codecs,
};

#[derive(Debug, Clone, PartialEq, Eq)]
struct TestMessage {
    value: u8,
}

impl RemoteMessage for TestMessage {
    const MANIFEST: &'static str = "kairo.cluster-tools.test.pubsub-message";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, Copy)]
struct TestMessageCodec;

impl MessageCodec<TestMessage> for TestMessageCodec {
    fn serializer_id(&self) -> u32 {
        59_001
    }

    fn encode(&self, message: &TestMessage) -> kairo_serialization::Result<Bytes> {
        Ok(Bytes::from(vec![message.value]))
    }

    fn decode(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<TestMessage> {
        assert_eq!(version, TestMessage::VERSION);
        Ok(TestMessage { value: payload[0] })
    }
}

#[derive(Default)]
struct CollectingRemoteOutbound {
    sent: Mutex<Vec<RemoteEnvelope>>,
}

impl CollectingRemoteOutbound {
    fn sent(&self) -> Vec<RemoteEnvelope> {
        self.sent
            .lock()
            .expect("collecting remote outbound poisoned")
            .clone()
    }
}

impl RemoteOutbound for CollectingRemoteOutbound {
    fn send(&self, envelope: RemoteEnvelope) -> Result<()> {
        self.sent
            .lock()
            .expect("collecting remote outbound poisoned")
            .push(envelope);
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

fn node(name: &str, uid: u64) -> UniqueAddress {
    UniqueAddress::new(
        Address::new(
            "kairo",
            "pubsub",
            Some(format!("{name}.example.test")),
            Some(2552),
        ),
        uid,
    )
}

fn recipient_for(node: &UniqueAddress) -> ActorRefWireData {
    ActorRefWireData::new(format!("{}{}", node.address, DEFAULT_PUBSUB_REMOTE_PATH)).unwrap()
}

#[test]
fn remote_delivery_outbound_wraps_broadcast_publish_for_target_mediator() {
    let registry = registry();
    let target = node("target", 2);
    let collecting = Arc::new(CollectingRemoteOutbound::default());
    let outbound = PubSubRemoteDeliveryOutbound::<TestMessage>::from_arc(
        target.clone(),
        registry.clone(),
        collecting.clone() as Arc<dyn RemoteOutbound>,
    );

    outbound
        .tell(LocalPubSubMsg::Publish {
            topic: TopicName::new("orders"),
            message: TestMessage { value: 42 },
            mode: TopicPublishMode::Broadcast,
            reply_to: None,
        })
        .unwrap();

    let sent = collecting.sent();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].recipient, recipient_for(&target));
    assert_eq!(sent[0].message.serializer_id, PUBSUB_PUBLISH_SERIALIZER_ID);

    let envelope = registry
        .deserialize::<PubSubPublishEnvelope>(sent[0].message.clone())
        .unwrap();
    assert_eq!(envelope.topic, TopicName::new("orders"));
    assert_eq!(envelope.group, None);
    assert_eq!(envelope.message.manifest.as_str(), TestMessage::MANIFEST);
    assert_eq!(
        registry
            .deserialize::<TestMessage>(envelope.message)
            .unwrap(),
        TestMessage { value: 42 }
    );
}

#[test]
fn remote_delivery_outbound_wraps_group_publish_for_target_mediator() {
    let registry = registry();
    let target = node("target", 2);
    let collecting = Arc::new(CollectingRemoteOutbound::default());
    let outbound = PubSubRemoteDeliveryOutbound::<TestMessage>::from_arc(
        target,
        registry.clone(),
        collecting.clone() as Arc<dyn RemoteOutbound>,
    );

    outbound
        .tell(LocalPubSubMsg::PublishGroup {
            topic: TopicName::new("jobs"),
            group: "workers".to_string(),
            message: TestMessage { value: 7 },
            reply_to: None,
        })
        .unwrap();

    let sent = collecting.sent();
    let envelope = registry
        .deserialize::<PubSubPublishEnvelope>(sent[0].message.clone())
        .unwrap();
    assert_eq!(envelope.topic, TopicName::new("jobs"));
    assert_eq!(envelope.group, Some("workers".to_string()));
}

#[test]
fn remote_delivery_outbound_wraps_path_messages_for_target_mediator() {
    let registry = registry();
    let target = node("target", 2);
    let collecting = Arc::new(CollectingRemoteOutbound::default());
    let outbound = PubSubRemoteDeliveryOutbound::<TestMessage>::from_arc(
        target.clone(),
        registry.clone(),
        collecting.clone() as Arc<dyn RemoteOutbound>,
    );

    outbound
        .tell(LocalPubSubMsg::Send {
            path: "/user/worker".to_string(),
            message: TestMessage { value: 8 },
            reply_to: None,
        })
        .unwrap();
    outbound
        .tell(LocalPubSubMsg::SendToAll {
            path: "/user/worker".to_string(),
            message: TestMessage { value: 9 },
            reply_to: None,
        })
        .unwrap();

    let sent = collecting.sent();
    assert_eq!(sent.len(), 2);
    assert_eq!(sent[0].recipient, recipient_for(&target));
    assert_eq!(sent[0].message.serializer_id, PUBSUB_PATH_SERIALIZER_ID);
    let send = registry
        .deserialize::<PubSubPathEnvelope>(sent[0].message.clone())
        .unwrap();
    assert_eq!(send.path, "/user/worker");
    assert!(!send.all);
    assert_eq!(
        registry.deserialize::<TestMessage>(send.message).unwrap(),
        TestMessage { value: 8 }
    );

    let send_to_all = registry
        .deserialize::<PubSubPathEnvelope>(sent[1].message.clone())
        .unwrap();
    assert_eq!(send_to_all.path, "/user/worker");
    assert!(send_to_all.all);
    assert_eq!(
        registry
            .deserialize::<TestMessage>(send_to_all.message)
            .unwrap(),
        TestMessage { value: 9 }
    );
}

#[test]
fn remote_delivery_outbound_can_use_association_cache() {
    let registry = registry();
    let target = node("target", 2);
    let cache = RemoteAssociationCache::new();
    let collecting = Arc::new(CollectingRemoteOutbound::default());
    cache.insert_route(
        RemoteAssociationAddress::new("kairo", "pubsub", "target.example.test", Some(2552))
            .unwrap(),
        collecting.clone() as Arc<dyn RemoteOutbound>,
    );
    let outbound =
        PubSubRemoteDeliveryOutbound::<TestMessage>::new(target.clone(), registry, cache);

    outbound
        .tell(LocalPubSubMsg::Publish {
            topic: TopicName::new("orders"),
            message: TestMessage { value: 1 },
            mode: TopicPublishMode::Broadcast,
            reply_to: None,
        })
        .unwrap();

    let sent = collecting.sent();
    assert_eq!(sent.len(), 1);
    assert_eq!(
        sent[0].recipient.path(),
        "kairo://pubsub@target.example.test:2552/system/pubsub"
    );
}

#[test]
fn mediator_can_publish_through_remote_delivery_target() {
    let kit = ActorSystemTestKit::new("pubsub-remote-delivery-mediator").unwrap();
    let registry = registry();
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let collecting = Arc::new(CollectingRemoteOutbound::default());
    let remote_outbound = PubSubRemoteDeliveryOutbound::<TestMessage>::from_arc(
        node_b.clone(),
        registry.clone(),
        collecting.clone() as Arc<dyn RemoteOutbound>,
    );
    let mut remote_registry = PubSubRegistryState::new(node_b.clone());
    remote_registry.register_local_topic(TopicName::new("orders"));
    let delta = remote_registry.collect_delta(&Default::default(), 10);
    let reports = kit
        .create_probe::<crate::DistributedPubSubPublishReport>("reports")
        .unwrap();
    let mediator = kit
        .system()
        .spawn(
            "mediator",
            Props::new({
                let node_a = node_a.clone();
                move || DistributedPubSubMediatorActor::<TestMessage>::new(node_a.clone())
            }),
        )
        .unwrap();

    mediator
        .tell(DistributedPubSubMediatorMsg::AddRemoteTarget {
            target: PubSubRemoteTarget::new(node_b.clone(), remote_outbound),
        })
        .unwrap();
    mediator
        .tell(DistributedPubSubMediatorMsg::MergeDelta { delta })
        .unwrap();
    mediator
        .tell(DistributedPubSubMediatorMsg::Publish {
            topic: TopicName::new("orders"),
            message: TestMessage { value: 11 },
            mode: TopicPublishMode::Broadcast,
            reply_to: Some(reports.actor_ref()),
        })
        .unwrap();

    let report = reports.expect_msg(Duration::from_secs(1)).unwrap();
    assert!(report.delivery.is_success());
    let sent = collecting.sent();
    assert_eq!(sent.len(), 1);
    let envelope = registry
        .deserialize::<PubSubPublishEnvelope>(sent[0].message.clone())
        .unwrap();
    assert_eq!(envelope.topic, TopicName::new("orders"));
    assert_eq!(
        registry
            .deserialize::<TestMessage>(envelope.message)
            .unwrap(),
        TestMessage { value: 11 }
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn mediator_can_send_path_through_remote_delivery_target() {
    let kit = ActorSystemTestKit::new("pubsub-remote-path-delivery-mediator").unwrap();
    let registry = registry();
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let collecting = Arc::new(CollectingRemoteOutbound::default());
    let remote_outbound = PubSubRemoteDeliveryOutbound::<TestMessage>::from_arc(
        node_b.clone(),
        registry.clone(),
        collecting.clone() as Arc<dyn RemoteOutbound>,
    );
    let mut remote_registry = PubSubRegistryState::new(node_b.clone());
    remote_registry.register_local_path("/user/worker");
    let delta = remote_registry.collect_delta(&Default::default(), 10);
    let reports = kit
        .create_probe::<crate::DistributedPubSubSendReport>("reports")
        .unwrap();
    let mediator = kit
        .system()
        .spawn(
            "mediator",
            Props::new({
                let node_a = node_a.clone();
                move || DistributedPubSubMediatorActor::<TestMessage>::new(node_a.clone())
            }),
        )
        .unwrap();

    mediator
        .tell(DistributedPubSubMediatorMsg::AddRemoteTarget {
            target: PubSubRemoteTarget::new(node_b.clone(), remote_outbound),
        })
        .unwrap();
    mediator
        .tell(DistributedPubSubMediatorMsg::MergeDelta { delta })
        .unwrap();
    mediator
        .tell(DistributedPubSubMediatorMsg::SendToAll {
            path: "/user/worker".to_string(),
            message: TestMessage { value: 12 },
            all_but_self: false,
            reply_to: Some(reports.actor_ref()),
        })
        .unwrap();

    let report = reports.expect_msg(Duration::from_secs(1)).unwrap();
    assert!(report.delivery.is_success());
    let sent = collecting.sent();
    assert_eq!(sent.len(), 1);
    let envelope = registry
        .deserialize::<PubSubPathEnvelope>(sent[0].message.clone())
        .unwrap();
    assert_eq!(envelope.path, "/user/worker");
    assert!(envelope.all);
    assert_eq!(
        registry
            .deserialize::<TestMessage>(envelope.message)
            .unwrap(),
        TestMessage { value: 12 }
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn remote_delivery_inbound_delivers_publish_to_mediator_actor_ref() {
    let kit = ActorSystemTestKit::new("pubsub-remote-delivery-in").unwrap();
    let registry = registry();
    let self_node = node("self", 1);
    let mediator = kit
        .create_probe::<DistributedPubSubMediatorMsg<TestMessage>>("mediator")
        .unwrap();
    let inbound =
        PubSubRemoteDeliveryInbound::new(self_node.clone(), registry.clone(), mediator.actor_ref());
    let envelope = PubSubPublishEnvelope {
        topic: TopicName::new("jobs"),
        group: Some("workers".to_string()),
        message: registry
            .serialize(&TestMessage { value: 9 })
            .expect("test message serializes"),
    };

    inbound
        .receive(RemoteEnvelope::new(
            recipient_for(&self_node),
            None,
            registry.serialize(&envelope).unwrap(),
        ))
        .unwrap();

    let msg = mediator.expect_msg(Duration::from_secs(1)).unwrap();
    match msg {
        DistributedPubSubMediatorMsg::LocalDelivery(LocalPubSubMsg::PublishGroup {
            topic,
            group,
            message,
            reply_to,
        }) => {
            assert_eq!(topic, TopicName::new("jobs"));
            assert_eq!(group, "workers");
            assert_eq!(message, TestMessage { value: 9 });
            assert!(reply_to.is_none());
        }
        _ => panic!("expected remote publish to be delivered to mediator local delivery"),
    }
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn remote_delivery_inbound_delivers_path_messages_to_mediator_actor_ref() {
    let kit = ActorSystemTestKit::new("pubsub-remote-path-delivery-in").unwrap();
    let registry = registry();
    let self_node = node("self", 1);
    let mediator = kit
        .create_probe::<DistributedPubSubMediatorMsg<TestMessage>>("mediator")
        .unwrap();
    let inbound =
        PubSubRemoteDeliveryInbound::new(self_node.clone(), registry.clone(), mediator.actor_ref());

    for all in [false, true] {
        let envelope = PubSubPathEnvelope {
            path: "/user/worker".to_string(),
            all,
            message: registry
                .serialize(&TestMessage {
                    value: if all { 2 } else { 1 },
                })
                .expect("test message serializes"),
        };

        inbound
            .receive(RemoteEnvelope::new(
                recipient_for(&self_node),
                None,
                registry.serialize(&envelope).unwrap(),
            ))
            .unwrap();

        let msg = mediator.expect_msg(Duration::from_secs(1)).unwrap();
        match (all, msg) {
            (
                false,
                DistributedPubSubMediatorMsg::LocalDelivery(LocalPubSubMsg::Send {
                    path,
                    message,
                    reply_to,
                }),
            ) => {
                assert_eq!(path, "/user/worker");
                assert_eq!(message, TestMessage { value: 1 });
                assert!(reply_to.is_none());
            }
            (
                true,
                DistributedPubSubMediatorMsg::LocalDelivery(LocalPubSubMsg::SendToAll {
                    path,
                    message,
                    reply_to,
                }),
            ) => {
                assert_eq!(path, "/user/worker");
                assert_eq!(message, TestMessage { value: 2 });
                assert!(reply_to.is_none());
            }
            _ => panic!("expected remote path envelope to be delivered to mediator local delivery"),
        }
    }
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn remote_delivery_inbound_rejects_wrong_recipient_and_unknown_manifest() {
    let kit = ActorSystemTestKit::new("pubsub-remote-delivery-reject").unwrap();
    let registry = registry();
    let self_node = node("self", 1);
    let mediator = kit
        .create_probe::<DistributedPubSubMediatorMsg<TestMessage>>("mediator")
        .unwrap();
    let inbound =
        PubSubRemoteDeliveryInbound::new(self_node.clone(), registry.clone(), mediator.actor_ref());
    let publish = PubSubPublishEnvelope {
        topic: TopicName::new("orders"),
        group: None,
        message: registry.serialize(&TestMessage { value: 1 }).unwrap(),
    };

    let wrong = RemoteEnvelope::new(
        recipient_for(&node("other", 9)),
        None,
        registry.serialize(&publish).unwrap(),
    );
    assert!(matches!(
        inbound.receive(wrong).unwrap_err(),
        PubSubRemoteDeliveryError::WrongRecipient { .. }
    ));

    let unknown = RemoteEnvelope::new(
        recipient_for(&self_node),
        None,
        SerializedMessage::new(
            9_999,
            Manifest::new("kairo.cluster-tools.pubsub.unknown"),
            1,
            Bytes::new(),
        ),
    );
    assert!(matches!(
        inbound.receive(unknown).unwrap_err(),
        PubSubRemoteDeliveryError::UnsupportedManifest(_)
    ));
    kit.shutdown(Duration::from_secs(1)).unwrap();
}
