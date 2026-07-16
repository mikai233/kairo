use std::collections::BTreeMap;

use bytes::Bytes;
use kairo_actor::Address;
use kairo_cluster::UniqueAddress;
use kairo_cluster_tools::{
    PUBSUB_DELTA_SERIALIZER_ID, PubSubBucket, PubSubDelta, PubSubRegistryDelta,
    PubSubRegistryEntry, PubSubRegistryKey, TopicName, register_cluster_tools_protocol_codecs,
};
use kairo_serialization::{Manifest, Registry, RemoteMessage, SerializedMessage};

const PUBSUB_DELTA_V1_FIXTURE: &str = include_str!("fixtures/pubsub-delta-v1.hex");

fn fixture_bytes() -> Bytes {
    let hex = PUBSUB_DELTA_V1_FIXTURE
        .split_whitespace()
        .collect::<String>();
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

#[test]
fn pubsub_delta_v1_encoding_matches_checked_fixture() {
    let delta = pubsub_delta_v1();
    let serialized = registry().serialize(&delta).unwrap();
    let fixture = fixture_bytes();

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
        fixture_bytes(),
    );

    assert_eq!(
        registry().deserialize::<PubSubDelta>(serialized).unwrap(),
        pubsub_delta_v1()
    );
}
