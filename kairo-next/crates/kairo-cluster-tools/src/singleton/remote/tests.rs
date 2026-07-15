use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use kairo_actor::Address;
use kairo_cluster::UniqueAddress;
use kairo_remote::{RemoteAssociationAddress, RemoteAssociationCache, Result};
use kairo_serialization::{
    ActorRefWireData, Manifest, Registry, RemoteEnvelope, RemoteMessage, SerializedMessage,
};
use kairo_testkit::ActorSystemTestKit;

use super::*;
use crate::{
    SINGLETON_HAND_OVER_TO_ME_SERIALIZER_ID, SingletonHandOverToMe, SingletonManagerEffect,
    SingletonManagerMsg, SingletonTakeOverFromMe, register_cluster_tools_protocol_codecs,
};

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

impl kairo_remote::RemoteOutbound for CollectingRemoteOutbound {
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
    Arc::new(registry)
}

fn node(name: &str, uid: u64) -> UniqueAddress {
    UniqueAddress::new(
        Address::new(
            "kairo",
            "singleton",
            Some(format!("{name}.example.test")),
            Some(2552),
        ),
        uid,
    )
}

fn recipient_for(node: &UniqueAddress) -> ActorRefWireData {
    ActorRefWireData::new(format!(
        "{}{}",
        node.address, DEFAULT_SINGLETON_MANAGER_REMOTE_PATH
    ))
    .unwrap()
}

#[test]
fn remote_outbound_wraps_handover_effects_for_target_manager() {
    let registry = registry();
    let self_node = node("oldest", 1);
    let next = node("next", 2);
    let collecting = Arc::new(CollectingRemoteOutbound::default());
    let outbound = SingletonManagerRemoteOutbound::from_arc(
        self_node.clone(),
        registry.clone(),
        collecting.clone() as Arc<dyn kairo_remote::RemoteOutbound>,
    );

    outbound
        .send_effect(&SingletonManagerEffect::SendTakeOverFromMe { to: next.clone() })
        .unwrap();

    let sent = collecting.sent();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].recipient, recipient_for(&next));
    assert_eq!(sent[0].sender, None);
    assert_eq!(
        sent[0].message.manifest.as_str(),
        SingletonTakeOverFromMe::MANIFEST
    );
    assert_eq!(
        registry
            .deserialize::<SingletonTakeOverFromMe>(sent[0].message.clone())
            .unwrap()
            .from,
        self_node
    );
}

#[test]
fn remote_outbound_can_use_association_cache() {
    let cache = RemoteAssociationCache::new();
    let collecting = Arc::new(CollectingRemoteOutbound::default());
    cache.insert_route(
        RemoteAssociationAddress::new("kairo", "singleton", "next.example.test", Some(2552))
            .unwrap(),
        collecting.clone() as Arc<dyn kairo_remote::RemoteOutbound>,
    );
    let outbound = SingletonManagerRemoteOutbound::new(node("self", 1), registry(), cache);

    outbound
        .send_effect(&SingletonManagerEffect::SendHandOverToMe {
            to: node("next", 2),
        })
        .unwrap();

    let sent = collecting.sent();
    assert_eq!(sent.len(), 1);
    assert_eq!(
        sent[0].recipient.path(),
        "kairo://singleton@next.example.test:2552/system/singleton/manager"
    );
    assert_eq!(
        sent[0].message.serializer_id,
        SINGLETON_HAND_OVER_TO_ME_SERIALIZER_ID
    );
}

#[test]
fn remote_inbound_delivers_takeover_to_manager_actor_ref() {
    let kit = ActorSystemTestKit::new("singleton-manager-remote-in").unwrap();
    let registry = registry();
    let self_node = node("next", 2);
    let previous = node("previous", 1);
    let manager = kit.create_probe::<SingletonManagerMsg>("manager").unwrap();
    let inbound = SingletonManagerRemoteInbound::new(
        self_node.clone(),
        registry.clone(),
        manager.actor_ref(),
    );
    let envelope = RemoteEnvelope::new(
        recipient_for(&self_node),
        None,
        registry
            .serialize(&SingletonTakeOverFromMe {
                from: previous.clone(),
            })
            .unwrap(),
    );

    inbound.receive(envelope).unwrap();

    let msg = manager.expect_msg(Duration::from_secs(1)).unwrap();
    match msg {
        SingletonManagerMsg::TakeOverFromMe { from, reply_to } => {
            assert_eq!(from, previous);
            assert!(reply_to.is_none());
        }
        _ => panic!("expected remote takeover to be delivered to singleton manager"),
    }
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn local_manager_remote_inbound_delivers_handover_to_typed_manager() {
    let kit = ActorSystemTestKit::new("local-singleton-manager-remote-in").unwrap();
    let registry = registry();
    let self_node = node("next", 2);
    let previous = node("previous", 1);
    let manager = kit
        .create_probe::<crate::LocalSingletonManagerMsg<String>>("manager")
        .unwrap();
    let inbound = LocalSingletonManagerRemoteInbound::new(
        self_node.clone(),
        registry.clone(),
        manager.actor_ref(),
    );
    let envelope = RemoteEnvelope::new(
        recipient_for(&self_node),
        None,
        registry
            .serialize(&SingletonHandOverToMe {
                from: previous.clone(),
            })
            .unwrap(),
    );

    inbound.receive(envelope).unwrap();

    assert!(matches!(
        manager.expect_msg(Duration::from_secs(1)).unwrap(),
        crate::LocalSingletonManagerMsg::HandOverToMe { from, reply_to: None }
            if from == previous
    ));
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn remote_inbound_rejects_wrong_recipient_and_unknown_manifest() {
    let kit = ActorSystemTestKit::new("singleton-manager-remote-reject").unwrap();
    let registry = registry();
    let self_node = node("self", 1);
    let manager = kit.create_probe::<SingletonManagerMsg>("manager").unwrap();
    let inbound = SingletonManagerRemoteInbound::new(
        self_node.clone(),
        registry.clone(),
        manager.actor_ref(),
    );
    let wrong = RemoteEnvelope::new(
        recipient_for(&node("other", 9)),
        None,
        registry
            .serialize(&SingletonHandOverToMe {
                from: node("sender", 2),
            })
            .unwrap(),
    );

    assert!(matches!(
        inbound.receive(wrong).unwrap_err(),
        SingletonManagerRemoteError::WrongRecipient { .. }
    ));

    let unknown = RemoteEnvelope::new(
        recipient_for(&self_node),
        None,
        SerializedMessage::new(
            9_999,
            Manifest::new("kairo.cluster-tools.singleton.unknown"),
            1,
            Bytes::new(),
        ),
    );
    assert!(matches!(
        inbound.receive(unknown).unwrap_err(),
        SingletonManagerRemoteError::UnsupportedManifest(_)
    ));
    kit.shutdown(Duration::from_secs(1)).unwrap();
}
