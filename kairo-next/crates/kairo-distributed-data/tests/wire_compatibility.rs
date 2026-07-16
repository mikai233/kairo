use bytes::Bytes;
use kairo_distributed_data::{
    CRDT_CODEC_VERSION, GCOUNTER_MANIFEST, GSET_STRING_DELTA_MANIFEST,
    REPLICATOR_DELTA_PROPAGATION_SERIALIZER_ID, ReplicaId, ReplicatorDelta,
    ReplicatorDeltaPropagation, ReplicatorPruningEntry, ReplicatorPruningState,
    register_ddata_protocol_codecs,
};
use kairo_serialization::{Manifest, Registry, RemoteMessage, SerializedMessage};

const DELTA_PROPAGATION_V1_FIXTURE: &str = include_str!("fixtures/delta-propagation-v1.hex");
const DELTA_PROPAGATION_V2_FIXTURE: &str = include_str!("fixtures/delta-propagation-v2.hex");

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
    register_ddata_protocol_codecs(&mut registry).unwrap();
    registry
}

fn delta_propagation_v1() -> ReplicatorDeltaPropagation {
    ReplicatorDeltaPropagation {
        from: ReplicaId::new("remote"),
        reply: false,
        deltas: vec![ReplicatorDelta {
            key: "counter".to_string(),
            crdt_manifest: GCOUNTER_MANIFEST.to_string(),
            crdt_version: CRDT_CODEC_VERSION,
            from_version: 1,
            to_version: 2,
            payload: Bytes::from_static(&[1, 2, 3]),
            pruning: Vec::new(),
        }],
    }
}

fn delta_propagation_v2() -> ReplicatorDeltaPropagation {
    ReplicatorDeltaPropagation {
        from: ReplicaId::new("kairo://alpha@127.0.0.1:25520#1"),
        reply: true,
        deltas: vec![
            ReplicatorDelta {
                key: "counter|orders".to_string(),
                crdt_manifest: GCOUNTER_MANIFEST.to_string(),
                crdt_version: CRDT_CODEC_VERSION,
                from_version: 3,
                to_version: 5,
                payload: Bytes::from_static(&[0xde, 0xad, 0xbe, 0xef]),
                pruning: vec![
                    ReplicatorPruningEntry {
                        removed: ReplicaId::new("replica-old-a"),
                        state: ReplicatorPruningState::Initialized {
                            owner: ReplicaId::new("replica-alpha"),
                            seen: vec![
                                ReplicaId::new("replica-alpha"),
                                ReplicaId::new("replica-beta"),
                            ],
                        },
                    },
                    ReplicatorPruningEntry {
                        removed: ReplicaId::new("replica-old-b"),
                        state: ReplicatorPruningState::Performed {
                            obsolete_at_millis: 0x0102_0304_0506_0708,
                        },
                    },
                ],
            },
            ReplicatorDelta {
                key: "set|users".to_string(),
                crdt_manifest: GSET_STRING_DELTA_MANIFEST.to_string(),
                crdt_version: CRDT_CODEC_VERSION,
                from_version: 9,
                to_version: 9,
                payload: Bytes::from_static(&[0x00, 0x01]),
                pruning: Vec::new(),
            },
        ],
    }
}

#[test]
fn delta_propagation_v1_fixture_decodes_without_pruning_metadata() {
    let serialized = SerializedMessage::new(
        REPLICATOR_DELTA_PROPAGATION_SERIALIZER_ID,
        Manifest::new(ReplicatorDeltaPropagation::MANIFEST),
        1,
        fixture_bytes(DELTA_PROPAGATION_V1_FIXTURE),
    );

    assert_eq!(
        registry()
            .deserialize::<ReplicatorDeltaPropagation>(serialized)
            .unwrap(),
        delta_propagation_v1()
    );
}

#[test]
fn delta_propagation_v2_encoding_matches_checked_fixture() {
    let propagation = delta_propagation_v2();
    let serialized = registry().serialize(&propagation).unwrap();
    let fixture = fixture_bytes(DELTA_PROPAGATION_V2_FIXTURE);

    assert_eq!(
        serialized.serializer_id,
        REPLICATOR_DELTA_PROPAGATION_SERIALIZER_ID
    );
    assert_eq!(
        serialized.manifest.as_str(),
        ReplicatorDeltaPropagation::MANIFEST
    );
    assert_eq!(serialized.version, ReplicatorDeltaPropagation::VERSION);
    assert_eq!(
        serialized.payload,
        fixture,
        "current v2 payload: {}",
        encode_hex(&serialized.payload)
    );
}

#[test]
fn delta_propagation_v2_fixture_decodes_pruning_lifecycle_state() {
    let serialized = SerializedMessage::new(
        REPLICATOR_DELTA_PROPAGATION_SERIALIZER_ID,
        Manifest::new(ReplicatorDeltaPropagation::MANIFEST),
        ReplicatorDeltaPropagation::VERSION,
        fixture_bytes(DELTA_PROPAGATION_V2_FIXTURE),
    );

    assert_eq!(
        registry()
            .deserialize::<ReplicatorDeltaPropagation>(serialized)
            .unwrap(),
        delta_propagation_v2()
    );
}
