use std::collections::BTreeMap;

use bytes::Bytes;
use kairo_actor::Address;
use kairo_cluster::UniqueAddress;
use kairo_cluster_tools::{
    PUBSUB_DELTA_SERIALIZER_ID, PubSubBucket, PubSubDelta, PubSubPathEnvelope,
    PubSubPublishEnvelope, PubSubRegistryDelta, PubSubRegistryEntry, PubSubRegistryKey,
    PubSubStatus, SINGLETON_HAND_OVER_DONE_SERIALIZER_ID,
    SINGLETON_HAND_OVER_IN_PROGRESS_SERIALIZER_ID, SINGLETON_HAND_OVER_TO_ME_SERIALIZER_ID,
    SINGLETON_MESSAGE_SERIALIZER_ID, SINGLETON_TAKE_OVER_FROM_ME_SERIALIZER_ID,
    SingletonHandOverDone, SingletonHandOverInProgress, SingletonHandOverToMe,
    SingletonMessageEnvelope, SingletonTakeOverFromMe, TopicName,
    register_cluster_tools_protocol_codecs,
};
use kairo_serialization::{Manifest, Registry, RemoteMessage, SerializedMessage};

const PUBSUB_DELTA_V1_FIXTURE: &str = include_str!("fixtures/pubsub-delta-v1.hex");
const PUBSUB_STATUS_V1_FIXTURE: &str = include_str!("fixtures/pubsub-status-v1.hex");
const PUBSUB_PUBLISH_V1_FIXTURE: &str = include_str!("fixtures/pubsub-publish-v1.hex");
const PUBSUB_PATH_V1_FIXTURE: &str = include_str!("fixtures/pubsub-path-v1.hex");
const SINGLETON_HANDOVER_V1_FIXTURE: &str = include_str!("fixtures/singleton-handover-v1.hex");
const SINGLETON_MESSAGE_V1_FIXTURE: &str = include_str!("fixtures/singleton-message-v1.hex");

fn fixture_bytes(hex_fixture: &str) -> Bytes {
    let hex = hex_fixture.split_whitespace().collect::<String>();
    assert_eq!(hex.len() % 2, 0, "wire fixture must contain whole bytes");
    Bytes::from(
        (0..hex.len())
            .step_by(2)
            .map(|offset| {
                u8::from_str_radix(&hex[offset..offset + 2], 16)
                    .expect("wire fixture must contain hexadecimal bytes")
            })
            .collect::<Vec<_>>(),
    )
}

fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn registry() -> Registry {
    let mut registry = Registry::new();
    register_cluster_tools_protocol_codecs(&mut registry).unwrap();
    registry
}

fn node(system: &str, port: u16, uid: u64) -> UniqueAddress {
    UniqueAddress::new(
        Address::new("kairo", system, Some("127.0.0.1".to_string()), Some(port)),
        uid,
    )
}

fn entry(key: PubSubRegistryKey, version: u64, present: bool) -> PubSubRegistryEntry {
    PubSubRegistryEntry {
        version,
        key,
        present,
    }
}

fn pubsub_delta_v1() -> PubSubDelta {
    let from = node("alpha", 25_520, 0x0102_0304_0506_0708);
    let beta = node("beta", 25_521, 0x1112_1314_1516_1718);
    let gamma = node("gamma", 25_522, 0x2122_2324_2526_2728);

    let orders = PubSubRegistryKey::topic(TopicName::new("orders"));
    let workers = PubSubRegistryKey::group(TopicName::new("jobs"), "workers");
    let worker_path = PubSubRegistryKey::path("/user/worker");
    let alerts = PubSubRegistryKey::topic(TopicName::new("alerts"));

    PubSubDelta {
        from,
        delta: PubSubRegistryDelta {
            buckets: vec![
                PubSubBucket {
                    owner: beta,
                    version: 4,
                    entries: BTreeMap::from([
                        (orders.clone(), entry(orders, 1, true)),
                        (workers.clone(), entry(workers, 2, true)),
                        (worker_path.clone(), entry(worker_path, 4, false)),
                    ]),
                },
                PubSubBucket {
                    owner: gamma,
                    version: 2,
                    entries: BTreeMap::from([(alerts.clone(), entry(alerts, 2, true))]),
                },
            ],
        },
    }
}

fn pubsub_status_v1() -> PubSubStatus {
    let alpha = node("alpha", 25_520, 0x0102_0304_0506_0708);
    let beta = node("beta", 25_521, 0x1112_1314_1516_1718);
    PubSubStatus {
        from: alpha.clone(),
        versions: BTreeMap::from([(alpha.ordering_key(), 7), (beta.ordering_key(), 11)]),
        reply: true,
    }
}

fn pubsub_business_message_v1() -> SerializedMessage {
    SerializedMessage::new(
        0x0102_0304,
        Manifest::new("example.pubsub.command"),
        0x0203,
        Bytes::from_static(&[0xde, 0xad, 0xbe, 0xef, 0x01]),
    )
}

fn pubsub_publish_v1() -> PubSubPublishEnvelope {
    PubSubPublishEnvelope {
        topic: TopicName::new("orders"),
        group: Some("workers".to_string()),
        message: pubsub_business_message_v1(),
    }
}

fn pubsub_path_v1() -> PubSubPathEnvelope {
    PubSubPathEnvelope {
        path: "/user/workers".to_string(),
        all: true,
        message: pubsub_business_message_v1(),
    }
}

fn assert_v1_fixture<M>(expected: &M, serializer_id: u32, manifest: &'static str, fixture: &str)
where
    M: RemoteMessage + std::fmt::Debug + PartialEq,
{
    assert_eq!(M::MANIFEST, manifest);
    assert_eq!(M::VERSION, 1);

    let serialized = registry().serialize(expected).unwrap();
    let fixture = fixture_bytes(fixture);
    assert_eq!(serialized.serializer_id, serializer_id);
    assert_eq!(serialized.manifest.as_str(), manifest);
    assert_eq!(serialized.version, 1);
    assert_eq!(
        serialized.payload,
        fixture,
        "{manifest} v1 payload: {}",
        encode_hex(&serialized.payload)
    );

    let decoded = registry()
        .deserialize::<M>(SerializedMessage::new(
            serializer_id,
            Manifest::new(manifest),
            1,
            fixture,
        ))
        .unwrap();
    assert_eq!(&decoded, expected);
}

#[test]
fn pubsub_status_v1_matches_checked_fixture() {
    assert_v1_fixture(
        &pubsub_status_v1(),
        5_000,
        "kairo.cluster-tools.pubsub.status",
        PUBSUB_STATUS_V1_FIXTURE,
    );
}

#[test]
fn pubsub_remote_delivery_v1_matches_checked_fixtures() {
    assert_v1_fixture(
        &pubsub_publish_v1(),
        5_002,
        "kairo.cluster-tools.pubsub.publish",
        PUBSUB_PUBLISH_V1_FIXTURE,
    );
    assert_v1_fixture(
        &pubsub_path_v1(),
        5_003,
        "kairo.cluster-tools.pubsub.path",
        PUBSUB_PATH_V1_FIXTURE,
    );
}

#[test]
fn pubsub_delta_v1_encoding_matches_checked_fixture() {
    let delta = pubsub_delta_v1();
    let serialized = registry().serialize(&delta).unwrap();
    let fixture = fixture_bytes(PUBSUB_DELTA_V1_FIXTURE);

    assert_eq!(serialized.serializer_id, PUBSUB_DELTA_SERIALIZER_ID);
    assert_eq!(serialized.manifest.as_str(), PubSubDelta::MANIFEST);
    assert_eq!(serialized.version, PubSubDelta::VERSION);
    assert_eq!(
        serialized.payload,
        fixture,
        "pubsub-delta v1 payload: {}",
        encode_hex(&serialized.payload)
    );
}

#[test]
fn pubsub_delta_v1_fixture_decodes_registry_versions_and_tombstones() {
    let serialized = SerializedMessage::new(
        PUBSUB_DELTA_SERIALIZER_ID,
        Manifest::new(PubSubDelta::MANIFEST),
        PubSubDelta::VERSION,
        fixture_bytes(PUBSUB_DELTA_V1_FIXTURE),
    );

    assert_eq!(
        registry().deserialize::<PubSubDelta>(serialized).unwrap(),
        pubsub_delta_v1()
    );
}

fn singleton_handover_node() -> UniqueAddress {
    node("singleton-a", 25_530, 0x3132_3334_3536_3738)
}

fn assert_singleton_wire_metadata<M: RemoteMessage>(
    serialized: &SerializedMessage,
    serializer_id: u32,
    fixture: &Bytes,
) {
    assert_eq!(serialized.serializer_id, serializer_id);
    assert_eq!(serialized.manifest.as_str(), M::MANIFEST);
    assert_eq!(serialized.version, M::VERSION);
    assert_eq!(
        &serialized.payload,
        fixture,
        "{} v1 payload: {}",
        M::MANIFEST,
        encode_hex(&serialized.payload)
    );
}

fn fixture_message<M: RemoteMessage>(serializer_id: u32, payload: Bytes) -> SerializedMessage {
    SerializedMessage::new(
        serializer_id,
        Manifest::new(M::MANIFEST),
        M::VERSION,
        payload,
    )
}

#[test]
fn singleton_handover_v1_encoding_matches_checked_fixture() {
    let registry = registry();
    let from = singleton_handover_node();
    let fixture = fixture_bytes(SINGLETON_HANDOVER_V1_FIXTURE);

    let hand_over_to_me = registry
        .serialize(&SingletonHandOverToMe { from: from.clone() })
        .unwrap();
    assert_singleton_wire_metadata::<SingletonHandOverToMe>(
        &hand_over_to_me,
        SINGLETON_HAND_OVER_TO_ME_SERIALIZER_ID,
        &fixture,
    );

    let in_progress = registry
        .serialize(&SingletonHandOverInProgress { from: from.clone() })
        .unwrap();
    assert_singleton_wire_metadata::<SingletonHandOverInProgress>(
        &in_progress,
        SINGLETON_HAND_OVER_IN_PROGRESS_SERIALIZER_ID,
        &fixture,
    );

    let done = registry
        .serialize(&SingletonHandOverDone { from: from.clone() })
        .unwrap();
    assert_singleton_wire_metadata::<SingletonHandOverDone>(
        &done,
        SINGLETON_HAND_OVER_DONE_SERIALIZER_ID,
        &fixture,
    );

    let take_over = registry
        .serialize(&SingletonTakeOverFromMe { from })
        .unwrap();
    assert_singleton_wire_metadata::<SingletonTakeOverFromMe>(
        &take_over,
        SINGLETON_TAKE_OVER_FROM_ME_SERIALIZER_ID,
        &fixture,
    );
}

#[test]
fn singleton_handover_v1_fixture_decodes_exact_sender_incarnation() {
    let registry = registry();
    let from = singleton_handover_node();
    let fixture = fixture_bytes(SINGLETON_HANDOVER_V1_FIXTURE);

    assert_eq!(
        registry
            .deserialize::<SingletonHandOverToMe>(fixture_message::<SingletonHandOverToMe>(
                SINGLETON_HAND_OVER_TO_ME_SERIALIZER_ID,
                fixture.clone(),
            ))
            .unwrap(),
        SingletonHandOverToMe { from: from.clone() }
    );
    assert_eq!(
        registry
            .deserialize::<SingletonHandOverInProgress>(fixture_message::<
                SingletonHandOverInProgress,
            >(
                SINGLETON_HAND_OVER_IN_PROGRESS_SERIALIZER_ID,
                fixture.clone(),
            ))
            .unwrap(),
        SingletonHandOverInProgress { from: from.clone() }
    );
    assert_eq!(
        registry
            .deserialize::<SingletonHandOverDone>(fixture_message::<SingletonHandOverDone>(
                SINGLETON_HAND_OVER_DONE_SERIALIZER_ID,
                fixture.clone(),
            ))
            .unwrap(),
        SingletonHandOverDone { from: from.clone() }
    );
    assert_eq!(
        registry
            .deserialize::<SingletonTakeOverFromMe>(fixture_message::<SingletonTakeOverFromMe>(
                SINGLETON_TAKE_OVER_FROM_ME_SERIALIZER_ID,
                fixture,
            ))
            .unwrap(),
        SingletonTakeOverFromMe { from }
    );
}

fn singleton_message_envelope() -> SingletonMessageEnvelope {
    SingletonMessageEnvelope {
        message: SerializedMessage::new(
            0x0102_0304,
            Manifest::new("example.singleton.command"),
            2,
            Bytes::from_static(&[0x10, 0x20, 0x30, 0x40, 0xff]),
        ),
    }
}

#[test]
fn singleton_message_v1_encoding_matches_checked_fixture() {
    let envelope = singleton_message_envelope();
    let serialized = registry().serialize(&envelope).unwrap();
    let fixture = fixture_bytes(SINGLETON_MESSAGE_V1_FIXTURE);

    assert_singleton_wire_metadata::<SingletonMessageEnvelope>(
        &serialized,
        SINGLETON_MESSAGE_SERIALIZER_ID,
        &fixture,
    );
}

#[test]
fn singleton_message_v1_fixture_decodes_nested_business_metadata() {
    let serialized = fixture_message::<SingletonMessageEnvelope>(
        SINGLETON_MESSAGE_SERIALIZER_ID,
        fixture_bytes(SINGLETON_MESSAGE_V1_FIXTURE),
    );

    assert_eq!(
        registry()
            .deserialize::<SingletonMessageEnvelope>(serialized)
            .unwrap(),
        singleton_message_envelope()
    );
}
