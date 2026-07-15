use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use kairo_actor::{ActorRef, Props};
use kairo_remote::{RemoteAssociationAddress, RemoteSettings, TcpRemoteActorRuntime};
use kairo_serialization::{MessageCodec, Registry, RemoteMessage, SerializationRegistry};
use kairo_testkit::ActorSystemTestKit;

use super::register_cluster_tools_system_inbound;
use crate::{
    ClusterToolsSystemInbound, DEFAULT_PUBSUB_REMOTE_PATH, DEFAULT_SINGLETON_MANAGER_REMOTE_PATH,
    DistributedPubSubMediatorActor, DistributedPubSubMediatorMsg, PubSubGossipMsg,
    PubSubGossipWireInbound, PubSubPublishEnvelope, PubSubRemoteDeliveryInbound, PubSubStatus,
    PubSubSubscribeAck, SingletonHandOverToMe, SingletonManagerMsg, SingletonManagerRemoteInbound,
    TopicName, register_cluster_tools_protocol_codecs,
};

#[derive(Debug, Clone, PartialEq, Eq)]
struct Business {
    value: u8,
}

impl RemoteMessage for Business {
    const MANIFEST: &'static str = "kairo.cluster-tools.test.shared-runtime-business";
    const VERSION: u16 = 1;
}

struct BusinessCodec;

impl MessageCodec<Business> for BusinessCodec {
    fn serializer_id(&self) -> u32 {
        4_993
    }

    fn encode(&self, message: &Business) -> kairo_serialization::Result<Bytes> {
        Ok(Bytes::from(vec![message.value]))
    }

    fn decode(&self, payload: Bytes, _version: u16) -> kairo_serialization::Result<Business> {
        Ok(Business { value: payload[0] })
    }
}

fn registry() -> Arc<Registry> {
    let mut registry = Registry::new();
    registry.register::<Business, _>(BusinessCodec).unwrap();
    kairo_remote::register_remote_protocol_codecs(&mut registry).unwrap();
    register_cluster_tools_protocol_codecs(&mut registry).unwrap();
    Arc::new(registry)
}

fn node_for(runtime: &TcpRemoteActorRuntime, uid: u64) -> kairo_cluster::UniqueAddress {
    kairo_cluster::UniqueAddress::new(
        kairo_actor::Address::new(
            runtime.system().address().protocol(),
            runtime.system().name(),
            Some(runtime.settings().canonical_hostname.clone()),
            Some(runtime.settings().canonical_port),
        ),
        uid,
    )
}

#[test]
fn shared_remote_runtime_routes_pubsub_and_singleton_system_traffic() {
    let receiver = ActorSystemTestKit::new("cluster-tools-shared-receiver").unwrap();
    let sender = ActorSystemTestKit::new("cluster-tools-shared-sender").unwrap();
    let registry = registry();
    let gossip_probe = receiver
        .create_probe::<PubSubGossipMsg>("pubsub-gossip")
        .unwrap();
    let singleton_probe = receiver
        .create_probe::<SingletonManagerMsg>("singleton-manager")
        .unwrap();
    let subscriber = receiver.create_probe::<Business>("subscriber").unwrap();
    let subscribe_ack = receiver
        .create_probe::<PubSubSubscribeAck>("subscribe-ack")
        .unwrap();
    let mediator_slot: Arc<Mutex<Option<ActorRef<DistributedPubSubMediatorMsg<Business>>>>> =
        Arc::new(Mutex::new(None));

    let receiver_system = receiver.system().clone();
    let receiver_registry = registry.clone();
    let receiver_gossip = gossip_probe.actor_ref();
    let receiver_singleton = singleton_probe.actor_ref();
    let receiver_mediator_slot = Arc::clone(&mediator_slot);
    let mut receiver_builder = TcpRemoteActorRuntime::builder(
        receiver.system().clone(),
        registry.clone(),
        RemoteSettings::new("127.0.0.1", 0),
        22,
    );
    register_cluster_tools_system_inbound::<Business, _>(
        &mut receiver_builder,
        2,
        move |self_node, _| {
            let mediator = receiver_system
                .spawn_system(
                    "pubsub",
                    Props::new({
                        let self_node = self_node.clone();
                        move || DistributedPubSubMediatorActor::new(self_node)
                    }),
                )
                .unwrap();
            *receiver_mediator_slot.lock().unwrap() = Some(mediator.clone());
            ClusterToolsSystemInbound::new(self_node.clone())
                .with_pubsub_gossip(PubSubGossipWireInbound::new(
                    self_node.clone(),
                    receiver_registry.clone(),
                    receiver_gossip.clone(),
                ))
                .with_pubsub_delivery(PubSubRemoteDeliveryInbound::new(
                    self_node.clone(),
                    receiver_registry.clone(),
                    mediator,
                ))
                .with_singleton_manager(SingletonManagerRemoteInbound::new(
                    self_node,
                    receiver_registry.clone(),
                    receiver_singleton.clone(),
                ))
        },
    )
    .unwrap();
    let receiver_remote = receiver_builder.bind().unwrap();

    let mut sender_builder = TcpRemoteActorRuntime::builder(
        sender.system().clone(),
        registry.clone(),
        RemoteSettings::new("127.0.0.1", 0),
        11,
    );
    register_cluster_tools_system_inbound::<Business, _>(&mut sender_builder, 1, |self_node, _| {
        ClusterToolsSystemInbound::new(self_node)
    })
    .unwrap();
    let sender_remote = sender_builder.bind().unwrap();

    let mediator = mediator_slot.lock().unwrap().clone().unwrap();
    let topic = TopicName::new("orders");
    mediator
        .tell(DistributedPubSubMediatorMsg::Subscribe {
            topic: topic.clone(),
            subscriber: subscriber.actor_ref(),
            reply_to: Some(subscribe_ack.actor_ref()),
        })
        .unwrap();
    subscribe_ack.expect_msg(Duration::from_secs(1)).unwrap();

    sender_remote
        .dial(
            RemoteAssociationAddress::new(
                "kairo",
                receiver_remote.system().name(),
                receiver_remote.settings().canonical_hostname.clone(),
                Some(receiver_remote.settings().canonical_port),
            )
            .unwrap(),
        )
        .unwrap();

    let receiver_node = node_for(&receiver_remote, 2);
    let sender_node = node_for(&sender_remote, 1);
    let pubsub_path = format!("{}{}", receiver_node.address, DEFAULT_PUBSUB_REMOTE_PATH);
    sender_remote
        .resolve::<PubSubStatus>(&pubsub_path)
        .unwrap()
        .tell(PubSubStatus {
            from: sender_node.clone(),
            versions: BTreeMap::new(),
            reply: false,
        })
        .unwrap();
    sender_remote
        .resolve::<PubSubPublishEnvelope>(&pubsub_path)
        .unwrap()
        .tell(PubSubPublishEnvelope {
            topic: topic.clone(),
            group: None,
            message: registry.serialize(&Business { value: 7 }).unwrap(),
        })
        .unwrap();
    sender_remote
        .resolve::<SingletonHandOverToMe>(format!(
            "{}{}",
            receiver_node.address, DEFAULT_SINGLETON_MANAGER_REMOTE_PATH
        ))
        .unwrap()
        .tell(SingletonHandOverToMe {
            from: sender_node.clone(),
        })
        .unwrap();

    assert!(matches!(
        gossip_probe.expect_msg(Duration::from_secs(1)).unwrap(),
        PubSubGossipMsg::Status { from, reply: false, .. } if from == sender_node
    ));
    subscriber
        .expect_msg_eq(Business { value: 7 }, Duration::from_secs(1))
        .unwrap();
    assert!(matches!(
        singleton_probe.expect_msg(Duration::from_secs(1)).unwrap(),
        SingletonManagerMsg::HandOverToMe { from, reply_to: None } if from == sender_node
    ));

    assert_eq!(sender_remote.association_cache().route_count(), 1);
    sender_remote.shutdown().unwrap();
    receiver_remote.shutdown().unwrap();
}
