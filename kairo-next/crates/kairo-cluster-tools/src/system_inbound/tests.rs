use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use kairo_actor::Address;
use kairo_cluster::UniqueAddress;
use kairo_remote::{RemoteFrameHandler, RemoteStreamId, encode_remote_envelope_frame};
use kairo_serialization::{
    ActorRefWireData, MessageCodec, Registry, RemoteEnvelope, RemoteMessage, SerializationRegistry,
};
use kairo_testkit::ActorSystemTestKit;

use super::*;
use crate::{
    CLUSTER_TOOLS_SYSTEM_MANIFESTS, DEFAULT_PUBSUB_REMOTE_PATH,
    DEFAULT_SINGLETON_MANAGER_REMOTE_PATH, DistributedPubSubMediatorMsg, LocalPubSubMsg,
    PubSubGossipMsg, PubSubGossipWireInbound, PubSubPathEnvelope, PubSubPublishEnvelope,
    PubSubRemoteDeliveryInbound, PubSubStatus, SingletonHandOverToMe, SingletonManagerMsg,
    SingletonManagerRemoteInbound, TopicName, TopicPublishMode,
    register_cluster_tools_protocol_codecs,
};

#[derive(Debug, Clone, PartialEq, Eq)]
struct TestMessage {
    value: u8,
}

impl RemoteMessage for TestMessage {
    const MANIFEST: &'static str = "kairo.cluster-tools.test.system-inbound-message";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, Copy)]
struct TestMessageCodec;

impl MessageCodec<TestMessage> for TestMessageCodec {
    fn serializer_id(&self) -> u32 {
        59_101
    }

    fn encode(&self, message: &TestMessage) -> kairo_serialization::Result<Bytes> {
        Ok(Bytes::from(vec![message.value]))
    }

    fn decode(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<TestMessage> {
        assert_eq!(version, TestMessage::VERSION);
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

fn node(name: &str, uid: u64) -> UniqueAddress {
    UniqueAddress::new(
        Address::new(
            "kairo",
            "cluster-tools",
            Some(format!("{name}.example.test")),
            Some(2552),
        ),
        uid,
    )
}

fn recipient_for(node: &UniqueAddress, path: &str) -> ActorRefWireData {
    ActorRefWireData::new(format!("{}{}", node.address, path)).unwrap()
}

#[test]
fn system_inbound_routes_pubsub_gossip_envelopes() {
    let kit = ActorSystemTestKit::new("cluster-tools-system-pubsub-gossip").unwrap();
    let registry = registry();
    let self_node = node("self", 1);
    let peer = node("peer", 2);
    let gossip = kit.create_probe::<PubSubGossipMsg>("gossip").unwrap();
    let inbound =
        ClusterToolsSystemInbound::<TestMessage>::new(self_node.clone()).with_pubsub_gossip(
            PubSubGossipWireInbound::new(self_node.clone(), registry.clone(), gossip.actor_ref()),
        );
    let status = PubSubStatus {
        from: peer.clone(),
        versions: Default::default(),
        reply: true,
    };

    inbound
        .receive(RemoteEnvelope::new(
            recipient_for(&self_node, DEFAULT_PUBSUB_REMOTE_PATH),
            None,
            registry.serialize(&status).unwrap(),
        ))
        .unwrap();

    match gossip.expect_msg(Duration::from_secs(1)).unwrap() {
        PubSubGossipMsg::Status {
            from,
            versions,
            reply,
        } => {
            assert_eq!(from, peer);
            assert!(versions.is_empty());
            assert!(reply);
        }
        _ => panic!("expected pubsub status to route to gossip actor"),
    }
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn system_inbound_routes_pubsub_publish_envelopes() {
    let kit = ActorSystemTestKit::new("cluster-tools-system-pubsub-publish").unwrap();
    let registry = registry();
    let self_node = node("self", 1);
    let mediator = kit
        .create_probe::<DistributedPubSubMediatorMsg<TestMessage>>("mediator")
        .unwrap();
    let inbound = ClusterToolsSystemInbound::new(self_node.clone()).with_pubsub_delivery(
        PubSubRemoteDeliveryInbound::new(self_node.clone(), registry.clone(), mediator.actor_ref()),
    );
    let publish = PubSubPublishEnvelope {
        topic: TopicName::new("orders"),
        group: None,
        message: registry.serialize(&TestMessage { value: 3 }).unwrap(),
    };

    inbound
        .receive(RemoteEnvelope::new(
            recipient_for(&self_node, DEFAULT_PUBSUB_REMOTE_PATH),
            None,
            registry.serialize(&publish).unwrap(),
        ))
        .unwrap();

    match mediator.expect_msg(Duration::from_secs(1)).unwrap() {
        DistributedPubSubMediatorMsg::LocalDelivery(LocalPubSubMsg::Publish {
            topic,
            message,
            mode,
            reply_to,
        }) => {
            assert_eq!(topic, TopicName::new("orders"));
            assert_eq!(message, TestMessage { value: 3 });
            assert_eq!(mode, TopicPublishMode::Broadcast);
            assert!(reply_to.is_none());
        }
        _ => panic!("expected pubsub publish to route to mediator local delivery"),
    }

    let path = PubSubPathEnvelope {
        path: "/user/worker".to_string(),
        all: true,
        message: registry.serialize(&TestMessage { value: 4 }).unwrap(),
    };
    inbound
        .receive(RemoteEnvelope::new(
            recipient_for(&self_node, DEFAULT_PUBSUB_REMOTE_PATH),
            None,
            registry.serialize(&path).unwrap(),
        ))
        .unwrap();

    match mediator.expect_msg(Duration::from_secs(1)).unwrap() {
        DistributedPubSubMediatorMsg::LocalDelivery(LocalPubSubMsg::SendToAll {
            path,
            message,
            reply_to,
        }) => {
            assert_eq!(path, "/user/worker");
            assert_eq!(message, TestMessage { value: 4 });
            assert!(reply_to.is_none());
        }
        _ => panic!("expected pubsub path envelope to route to mediator local delivery"),
    }
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn system_inbound_routes_singleton_manager_envelopes() {
    let kit = ActorSystemTestKit::new("cluster-tools-system-singleton").unwrap();
    let registry = registry();
    let self_node = node("self", 1);
    let peer = node("peer", 2);
    let manager = kit
        .create_probe::<SingletonManagerMsg>("singleton-manager")
        .unwrap();
    let inbound = ClusterToolsSystemInbound::<TestMessage>::new(self_node.clone())
        .with_singleton_manager(SingletonManagerRemoteInbound::new(
            self_node.clone(),
            registry.clone(),
            manager.actor_ref(),
        ));

    inbound
        .receive(RemoteEnvelope::new(
            recipient_for(&self_node, DEFAULT_SINGLETON_MANAGER_REMOTE_PATH),
            None,
            registry
                .serialize(&SingletonHandOverToMe { from: peer.clone() })
                .unwrap(),
        ))
        .unwrap();

    match manager.expect_msg(Duration::from_secs(1)).unwrap() {
        SingletonManagerMsg::HandOverToMe { from, reply_to } => {
            assert_eq!(from, peer);
            assert!(reply_to.is_none());
        }
        _ => panic!("expected singleton handover to route to manager"),
    }
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn system_inbound_rejects_wrong_recipient_missing_handler_and_unknown_manifest() {
    let kit = ActorSystemTestKit::new("cluster-tools-system-reject").unwrap();
    let registry = registry();
    let self_node = node("self", 1);
    let gossip = kit.create_probe::<PubSubGossipMsg>("gossip").unwrap();
    let inbound =
        ClusterToolsSystemInbound::<TestMessage>::new(self_node.clone()).with_pubsub_gossip(
            PubSubGossipWireInbound::new(self_node.clone(), registry.clone(), gossip.actor_ref()),
        );
    let status = PubSubStatus {
        from: node("peer", 2),
        versions: Default::default(),
        reply: false,
    };

    let wrong_recipient = RemoteEnvelope::new(
        recipient_for(&node("other", 9), DEFAULT_PUBSUB_REMOTE_PATH),
        None,
        registry.serialize(&status).unwrap(),
    );
    assert!(matches!(
        inbound.receive(wrong_recipient).unwrap_err(),
        ClusterToolsSystemInboundError::WrongRecipient { .. }
    ));

    let no_publish_handler = RemoteEnvelope::new(
        recipient_for(&self_node, DEFAULT_PUBSUB_REMOTE_PATH),
        None,
        registry
            .serialize(&PubSubPublishEnvelope {
                topic: TopicName::new("orders"),
                group: None,
                message: registry.serialize(&TestMessage { value: 1 }).unwrap(),
            })
            .unwrap(),
    );
    assert!(matches!(
        inbound.receive(no_publish_handler).unwrap_err(),
        ClusterToolsSystemInboundError::MissingHandler("pubsub delivery")
    ));

    let no_gossip_handler = RemoteEnvelope::new(
        recipient_for(&self_node, DEFAULT_PUBSUB_REMOTE_PATH),
        None,
        registry.serialize(&status).unwrap(),
    );
    assert!(matches!(
        ClusterToolsSystemInbound::<TestMessage>::new(self_node.clone())
            .receive(no_gossip_handler)
            .unwrap_err(),
        ClusterToolsSystemInboundError::MissingHandler("pubsub gossip")
    ));

    let peer = node("peer", 2);
    let no_singleton_handler = RemoteEnvelope::new(
        recipient_for(&self_node, DEFAULT_SINGLETON_MANAGER_REMOTE_PATH),
        None,
        registry
            .serialize(&SingletonHandOverToMe { from: peer })
            .unwrap(),
    );
    assert!(matches!(
        ClusterToolsSystemInbound::<TestMessage>::new(self_node.clone())
            .receive(no_singleton_handler)
            .unwrap_err(),
        ClusterToolsSystemInboundError::MissingHandler("singleton manager")
    ));

    let unknown = RemoteEnvelope::new(
        recipient_for(&self_node, DEFAULT_PUBSUB_REMOTE_PATH),
        None,
        kairo_serialization::SerializedMessage::new(
            9_999,
            kairo_serialization::Manifest::new("kairo.cluster-tools.unknown"),
            1,
            Bytes::new(),
        ),
    );
    assert!(matches!(
        inbound.receive(unknown).unwrap_err(),
        ClusterToolsSystemInboundError::UnsupportedManifest(_)
    ));
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn system_inbound_decodes_remote_frames() {
    let kit = ActorSystemTestKit::new("cluster-tools-system-frame").unwrap();
    let registry = registry();
    let self_node = node("self", 1);
    let peer = node("peer", 2);
    let gossip = kit.create_probe::<PubSubGossipMsg>("gossip").unwrap();
    let inbound =
        ClusterToolsSystemInbound::<TestMessage>::new(self_node.clone()).with_pubsub_gossip(
            PubSubGossipWireInbound::new(self_node.clone(), registry.clone(), gossip.actor_ref()),
        );
    let status = PubSubStatus {
        from: peer.clone(),
        versions: Default::default(),
        reply: false,
    };
    let frame = encode_remote_envelope_frame(&RemoteEnvelope::new(
        recipient_for(&self_node, DEFAULT_PUBSUB_REMOTE_PATH),
        None,
        registry.serialize(&status).unwrap(),
    ))
    .unwrap();

    inbound
        .handle_frame(RemoteStreamId::Control, frame)
        .unwrap();

    match gossip.expect_msg(Duration::from_secs(1)).unwrap() {
        PubSubGossipMsg::Status { from, reply, .. } => {
            assert_eq!(from, peer);
            assert!(!reply);
        }
        _ => panic!("expected framed pubsub status to route to gossip actor"),
    }
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn cluster_tools_manifest_helper_matches_system_protocols() {
    for manifest in CLUSTER_TOOLS_SYSTEM_MANIFESTS {
        assert!(is_cluster_tools_system_manifest(manifest));
    }
    assert!(!is_cluster_tools_system_manifest("kairo.remote.watch"));
}
