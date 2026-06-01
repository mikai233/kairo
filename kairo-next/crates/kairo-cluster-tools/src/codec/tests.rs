use std::collections::BTreeMap;

use bytes::Bytes;
use kairo_actor::Address;
use kairo_cluster::UniqueAddress;
use kairo_serialization::{Manifest, Registry, RemoteMessage, SerializedMessage};

use super::{
    PUBSUB_DELTA_SERIALIZER_ID, PUBSUB_PUBLISH_SERIALIZER_ID, PUBSUB_STATUS_SERIALIZER_ID,
    SINGLETON_HAND_OVER_DONE_SERIALIZER_ID, SINGLETON_HAND_OVER_IN_PROGRESS_SERIALIZER_ID,
    SINGLETON_HAND_OVER_TO_ME_SERIALIZER_ID, SINGLETON_TAKE_OVER_FROM_ME_SERIALIZER_ID,
    register_cluster_tools_protocol_codecs,
};
use crate::{
    PubSubDelta, PubSubPublishEnvelope, PubSubRegistryState, PubSubStatus, SingletonHandOverDone,
    SingletonHandOverInProgress, SingletonHandOverToMe, SingletonTakeOverFromMe, TopicName,
};

fn registry() -> Registry {
    let mut registry = Registry::new();
    register_cluster_tools_protocol_codecs(&mut registry).unwrap();
    registry
}

fn unique(system: &str, uid: u64) -> UniqueAddress {
    UniqueAddress::new(
        Address::new("kairo", system, Some("127.0.0.1".to_string()), Some(25520)),
        uid,
    )
}

#[test]
fn cluster_tools_codecs_round_trip_pubsub_status() {
    let registry = registry();
    let node = unique("a", 1);
    let status = PubSubStatus {
        from: node.clone(),
        versions: BTreeMap::from([(node.ordering_key(), 7)]),
        reply: true,
    };

    let serialized = registry.serialize(&status).unwrap();

    assert_eq!(serialized.serializer_id, PUBSUB_STATUS_SERIALIZER_ID);
    assert_eq!(serialized.manifest.as_str(), PubSubStatus::MANIFEST);
    assert_eq!(
        registry.deserialize::<PubSubStatus>(serialized).unwrap(),
        status
    );
}

#[test]
fn cluster_tools_codecs_round_trip_pubsub_delta() {
    let registry = registry();
    let node = unique("a", 1);
    let mut state = PubSubRegistryState::new(node.clone());
    state.register_local_topic(TopicName::new("orders"));
    state.register_local_group(TopicName::new("jobs"), "workers");
    let delta = PubSubDelta {
        from: node,
        delta: state.collect_delta(&BTreeMap::new(), 10),
    };

    let serialized = registry.serialize(&delta).unwrap();

    assert_eq!(serialized.serializer_id, PUBSUB_DELTA_SERIALIZER_ID);
    assert_eq!(serialized.manifest.as_str(), PubSubDelta::MANIFEST);
    assert_eq!(
        registry.deserialize::<PubSubDelta>(serialized).unwrap(),
        delta
    );
}

#[test]
fn cluster_tools_codecs_round_trip_pubsub_publish_envelope() {
    let registry = registry();
    let inner = SerializedMessage::new(
        77,
        Manifest::new("example.business.message"),
        3,
        Bytes::from_static(&[1, 2, 3]),
    );
    let envelope = PubSubPublishEnvelope {
        topic: TopicName::new("orders"),
        group: Some("workers".to_string()),
        message: inner,
    };

    let serialized = registry.serialize(&envelope).unwrap();

    assert_eq!(serialized.serializer_id, PUBSUB_PUBLISH_SERIALIZER_ID);
    assert_eq!(
        serialized.manifest.as_str(),
        PubSubPublishEnvelope::MANIFEST
    );
    assert_eq!(
        registry
            .deserialize::<PubSubPublishEnvelope>(serialized)
            .unwrap(),
        envelope
    );
}

#[test]
fn cluster_tools_codecs_round_trip_singleton_handover_messages() {
    let registry = registry();
    let node = unique("singleton", 9);

    let hand_over_to_me = SingletonHandOverToMe { from: node.clone() };
    let serialized = registry.serialize(&hand_over_to_me).unwrap();
    assert_eq!(
        serialized.serializer_id,
        SINGLETON_HAND_OVER_TO_ME_SERIALIZER_ID
    );
    assert_eq!(
        serialized.manifest.as_str(),
        SingletonHandOverToMe::MANIFEST
    );
    assert_eq!(
        registry
            .deserialize::<SingletonHandOverToMe>(serialized)
            .unwrap(),
        hand_over_to_me
    );

    let in_progress = SingletonHandOverInProgress { from: node.clone() };
    let serialized = registry.serialize(&in_progress).unwrap();
    assert_eq!(
        serialized.serializer_id,
        SINGLETON_HAND_OVER_IN_PROGRESS_SERIALIZER_ID
    );
    assert_eq!(
        registry
            .deserialize::<SingletonHandOverInProgress>(serialized)
            .unwrap(),
        in_progress
    );

    let done = SingletonHandOverDone { from: node.clone() };
    let serialized = registry.serialize(&done).unwrap();
    assert_eq!(
        serialized.serializer_id,
        SINGLETON_HAND_OVER_DONE_SERIALIZER_ID
    );
    assert_eq!(
        registry
            .deserialize::<SingletonHandOverDone>(serialized)
            .unwrap(),
        done
    );

    let take_over = SingletonTakeOverFromMe { from: node };
    let serialized = registry.serialize(&take_over).unwrap();
    assert_eq!(
        serialized.serializer_id,
        SINGLETON_TAKE_OVER_FROM_ME_SERIALIZER_ID
    );
    assert_eq!(
        registry
            .deserialize::<SingletonTakeOverFromMe>(serialized)
            .unwrap(),
        take_over
    );
}

#[test]
fn cluster_tools_codecs_reject_unknown_versions() {
    let registry = registry();
    let status = PubSubStatus {
        from: unique("a", 1),
        versions: BTreeMap::new(),
        reply: false,
    };
    let wire = SerializedMessage::new(
        PUBSUB_STATUS_SERIALIZER_ID,
        Manifest::new(PubSubStatus::MANIFEST),
        PubSubStatus::VERSION + 1,
        registry.serialize(&status).unwrap().payload,
    );

    let error = registry
        .deserialize::<PubSubStatus>(wire)
        .expect_err("unknown version should fail");

    assert!(error.to_string().contains("unsupported"));
}
