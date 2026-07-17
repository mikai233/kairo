use bytes::Bytes;
use kairo_distributed_data::{
    CRDT_CODEC_VERSION, GCOUNTER_MANIFEST, GSET_STRING_DELTA_MANIFEST,
    REPLICATOR_DELTA_PROPAGATION_SERIALIZER_ID, ReplicaId, ReplicatorChanged,
    ReplicatorDataEnvelope, ReplicatorDelta, ReplicatorDeltaAck, ReplicatorDeltaNack,
    ReplicatorDeltaPropagation, ReplicatorGet, ReplicatorGossip, ReplicatorGossipDigest,
    ReplicatorGossipEntry, ReplicatorGossipStatus, ReplicatorPruningEntry, ReplicatorPruningState,
    ReplicatorRead, ReplicatorReadResult, ReplicatorSubscribe, ReplicatorUpdate, ReplicatorWrite,
    ReplicatorWriteAck, ReplicatorWriteNack, register_ddata_protocol_codecs,
};
use kairo_serialization::{ActorRefWireData, Manifest, Registry, RemoteMessage, SerializedMessage};

const CLIENT_REQUEST_V1_FIXTURE: &str = include_str!("fixtures/client-request-v1.hex");
const SUBSCRIBE_V1_FIXTURE: &str = include_str!("fixtures/subscribe-v1.hex");
const CHANGED_V1_FIXTURE: &str = include_str!("fixtures/changed-v1.hex");
const DELTA_PROPAGATION_V1_FIXTURE: &str = include_str!("fixtures/delta-propagation-v1.hex");
const DELTA_PROPAGATION_V2_FIXTURE: &str = include_str!("fixtures/delta-propagation-v2.hex");
const WRITE_V1_FIXTURE: &str = include_str!("fixtures/write-v1.hex");
const WRITE_V2_FIXTURE: &str = include_str!("fixtures/write-v2.hex");
const READ_V1_FIXTURE: &str = include_str!("fixtures/read-v1.hex");
const READ_RESULT_V1_FIXTURE: &str = include_str!("fixtures/read-result-v1.hex");
const READ_RESULT_V2_FIXTURE: &str = include_str!("fixtures/read-result-v2.hex");
const GOSSIP_STATUS_V1_FIXTURE: &str = include_str!("fixtures/gossip-status-v1.hex");
const GOSSIP_V1_FIXTURE: &str = include_str!("fixtures/gossip-v1.hex");
const EMPTY_V1_FIXTURE: &str = "";

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

fn compatibility_key() -> String {
    "counter|orders".to_string()
}

fn source_replica() -> ReplicaId {
    ReplicaId::new("kairo://alpha@127.0.0.1:25520#1")
}

fn client_get_v1() -> ReplicatorGet {
    ReplicatorGet {
        key: compatibility_key(),
        request_id: 0x0102_0304_0506_0708,
    }
}

fn client_update_v1() -> ReplicatorUpdate {
    ReplicatorUpdate {
        key: compatibility_key(),
        request_id: 0x0102_0304_0506_0708,
    }
}

fn subscribe_v1() -> ReplicatorSubscribe {
    ReplicatorSubscribe {
        key: compatibility_key(),
        subscriber: ActorRefWireData::new("kairo://alpha@127.0.0.1:25520/user/subscriber#7")
            .unwrap(),
    }
}

fn changed_v1() -> ReplicatorChanged {
    ReplicatorChanged {
        key: compatibility_key(),
    }
}

fn data_envelope(pruning: bool) -> ReplicatorDataEnvelope {
    ReplicatorDataEnvelope {
        crdt_manifest: GCOUNTER_MANIFEST.to_string(),
        crdt_version: CRDT_CODEC_VERSION,
        payload: Bytes::from_static(&[0xde, 0xad, 0xbe, 0xef]),
        pruning: if pruning {
            vec![ReplicatorPruningEntry {
                removed: ReplicaId::new("replica-old"),
                state: ReplicatorPruningState::Performed {
                    obsolete_at_millis: 0x1112_1314_1516_1718,
                },
            }]
        } else {
            Vec::new()
        },
    }
}

fn write(pruning: bool) -> ReplicatorWrite {
    ReplicatorWrite {
        key: compatibility_key(),
        from: Some(source_replica()),
        envelope: data_envelope(pruning),
    }
}

fn read_v1() -> ReplicatorRead {
    ReplicatorRead {
        key: compatibility_key(),
        from: Some(source_replica()),
    }
}

fn read_result(pruning: bool) -> ReplicatorReadResult {
    ReplicatorReadResult {
        envelope: Some(data_envelope(pruning)),
    }
}

fn gossip_status_v1() -> ReplicatorGossipStatus {
    ReplicatorGossipStatus {
        entries: vec![
            ReplicatorGossipDigest {
                key: compatibility_key(),
                digest: 0x2122_2324_2526_2728,
                used_timestamp_millis: 0x3132_3334_3536_3738,
            },
            ReplicatorGossipDigest {
                key: "set|users".to_string(),
                digest: 0x4142_4344_4546_4748,
                used_timestamp_millis: 0x5152_5354_5556_5758,
            },
        ],
        chunk: 1,
        total_chunks: 3,
        to_system_uid: Some(0x6162_6364_6566_6768),
        from_system_uid: Some(0x7172_7374_7576_7778),
    }
}

fn gossip_v1() -> ReplicatorGossip {
    ReplicatorGossip {
        entries: vec![ReplicatorGossipEntry {
            key: compatibility_key(),
            envelope: data_envelope(true),
            used_timestamp_millis: 0x3132_3334_3536_3738,
        }],
        send_back: true,
        to_system_uid: Some(0x6162_6364_6566_6768),
        from_system_uid: Some(0x7172_7374_7576_7778),
    }
}

fn assert_current_fixture<M>(
    expected: &M,
    serializer_id: u32,
    manifest: &'static str,
    version: u16,
    fixture: &str,
) where
    M: RemoteMessage + std::fmt::Debug + PartialEq,
{
    assert_eq!(M::MANIFEST, manifest);
    assert_eq!(M::VERSION, version);

    let serialized = registry().serialize(expected).unwrap();
    let fixture = fixture_bytes(fixture);
    assert_eq!(serialized.serializer_id, serializer_id);
    assert_eq!(serialized.manifest.as_str(), manifest);
    assert_eq!(serialized.version, version);
    assert_eq!(
        serialized.payload,
        fixture,
        "{manifest} v{version} payload: {}",
        encode_hex(&serialized.payload)
    );

    let decoded = registry()
        .deserialize::<M>(SerializedMessage::new(
            serializer_id,
            Manifest::new(manifest),
            version,
            fixture,
        ))
        .unwrap();
    assert_eq!(&decoded, expected);
}

#[test]
fn client_request_v1_messages_match_checked_fixture() {
    assert_current_fixture(
        &client_get_v1(),
        3_000,
        "kairo.ddata.replicator-get",
        1,
        CLIENT_REQUEST_V1_FIXTURE,
    );
    assert_current_fixture(
        &client_update_v1(),
        3_001,
        "kairo.ddata.replicator-update",
        1,
        CLIENT_REQUEST_V1_FIXTURE,
    );
}

#[test]
fn client_subscription_v1_messages_match_checked_fixtures() {
    assert_current_fixture(
        &subscribe_v1(),
        3_002,
        "kairo.ddata.replicator-subscribe",
        1,
        SUBSCRIBE_V1_FIXTURE,
    );
    assert_current_fixture(
        &changed_v1(),
        3_003,
        "kairo.ddata.replicator-changed",
        1,
        CHANGED_V1_FIXTURE,
    );
}

#[test]
fn empty_acknowledgement_v1_messages_have_stable_metadata() {
    assert_current_fixture(
        &ReplicatorDeltaAck,
        3_005,
        "kairo.ddata.delta-ack",
        1,
        EMPTY_V1_FIXTURE,
    );
    assert_current_fixture(
        &ReplicatorDeltaNack,
        3_006,
        "kairo.ddata.delta-nack",
        1,
        EMPTY_V1_FIXTURE,
    );
    assert_current_fixture(
        &ReplicatorWriteAck,
        3_008,
        "kairo.ddata.write-ack",
        1,
        EMPTY_V1_FIXTURE,
    );
    assert_current_fixture(
        &ReplicatorWriteNack,
        3_009,
        "kairo.ddata.write-nack",
        1,
        EMPTY_V1_FIXTURE,
    );
}

#[test]
fn direct_write_v2_matches_fixture_and_v1_remains_decodable() {
    assert_current_fixture(
        &write(true),
        3_007,
        "kairo.ddata.write",
        2,
        WRITE_V2_FIXTURE,
    );

    let historical = SerializedMessage::new(
        3_007,
        Manifest::new("kairo.ddata.write"),
        1,
        fixture_bytes(WRITE_V1_FIXTURE),
    );
    assert_eq!(
        registry()
            .deserialize::<ReplicatorWrite>(historical)
            .unwrap(),
        write(false)
    );
}

#[test]
fn direct_read_v1_and_result_v2_match_fixtures_with_v1_result_decode() {
    assert_current_fixture(&read_v1(), 3_010, "kairo.ddata.read", 1, READ_V1_FIXTURE);
    assert_current_fixture(
        &read_result(true),
        3_011,
        "kairo.ddata.read-result",
        2,
        READ_RESULT_V2_FIXTURE,
    );

    let historical = SerializedMessage::new(
        3_011,
        Manifest::new("kairo.ddata.read-result"),
        1,
        fixture_bytes(READ_RESULT_V1_FIXTURE),
    );
    assert_eq!(
        registry()
            .deserialize::<ReplicatorReadResult>(historical)
            .unwrap(),
        read_result(false)
    );
}

#[test]
fn full_state_gossip_v1_messages_match_checked_fixtures() {
    assert_current_fixture(
        &gossip_status_v1(),
        3_012,
        "kairo.ddata.gossip-status",
        1,
        GOSSIP_STATUS_V1_FIXTURE,
    );
    assert_current_fixture(
        &gossip_v1(),
        3_013,
        "kairo.ddata.gossip",
        1,
        GOSSIP_V1_FIXTURE,
    );
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
