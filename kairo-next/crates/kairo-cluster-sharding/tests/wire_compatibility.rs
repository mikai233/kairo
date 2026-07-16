use bytes::Bytes;
use kairo_cluster_sharding::{
    ROUTED_SHARD_ENVELOPE_SERIALIZER_ID, RoutedShardEnvelope, SHARD_HOME_SERIALIZER_ID, ShardHome,
    register_sharding_protocol_codecs,
};
use kairo_serialization::{ActorRefWireData, Manifest, Registry, RemoteMessage, SerializedMessage};

const SHARD_HOME_V1_FIXTURE: &str = include_str!("fixtures/shard-home-v1.hex");
const ROUTED_SHARD_ENVELOPE_V1_FIXTURE: &str =
    include_str!("fixtures/routed-shard-envelope-v1.hex");

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
    register_sharding_protocol_codecs(&mut registry).unwrap();
    registry
}

fn shard_home_v1() -> ShardHome {
    ShardHome {
        shard_id: "shard-0042".to_string(),
        region: ActorRefWireData::new(
            "kairo://orders@127.0.0.1:25521/system/sharding-orders-region#9",
        )
        .unwrap(),
    }
}

fn routed_shard_envelope_v1() -> RoutedShardEnvelope {
    RoutedShardEnvelope {
        shard_id: "shard-0042".to_string(),
        entity_id: "account|0007".to_string(),
        message: SerializedMessage::new(
            0x0102_0304,
            Manifest::new("com.example.account.command"),
            0x0203,
            Bytes::from_static(&[0xde, 0xad, 0xbe, 0xef, 0x00, 0x01]),
        ),
    }
}

#[test]
fn shard_home_v1_encoding_matches_checked_fixture() {
    let home = shard_home_v1();
    let serialized = registry().serialize(&home).unwrap();
    let fixture = fixture_bytes(SHARD_HOME_V1_FIXTURE);

    assert_eq!(serialized.serializer_id, SHARD_HOME_SERIALIZER_ID);
    assert_eq!(serialized.manifest.as_str(), ShardHome::MANIFEST);
    assert_eq!(serialized.version, ShardHome::VERSION);
    assert_eq!(
        serialized.payload,
        fixture,
        "shard-home v1 payload: {}",
        encode_hex(&serialized.payload)
    );
}

#[test]
fn shard_home_v1_fixture_decodes_region_ownership() {
    let serialized = SerializedMessage::new(
        SHARD_HOME_SERIALIZER_ID,
        Manifest::new(ShardHome::MANIFEST),
        ShardHome::VERSION,
        fixture_bytes(SHARD_HOME_V1_FIXTURE),
    );

    assert_eq!(
        registry().deserialize::<ShardHome>(serialized).unwrap(),
        shard_home_v1()
    );
}

#[test]
fn routed_shard_envelope_v1_encoding_matches_checked_fixture() {
    let envelope = routed_shard_envelope_v1();
    let serialized = registry().serialize(&envelope).unwrap();
    let fixture = fixture_bytes(ROUTED_SHARD_ENVELOPE_V1_FIXTURE);

    assert_eq!(
        serialized.serializer_id,
        ROUTED_SHARD_ENVELOPE_SERIALIZER_ID
    );
    assert_eq!(serialized.manifest.as_str(), RoutedShardEnvelope::MANIFEST);
    assert_eq!(serialized.version, RoutedShardEnvelope::VERSION);
    assert_eq!(
        serialized.payload,
        fixture,
        "routed-envelope v1 payload: {}",
        encode_hex(&serialized.payload)
    );
}

#[test]
fn routed_shard_envelope_v1_fixture_decodes_nested_business_metadata() {
    let serialized = SerializedMessage::new(
        ROUTED_SHARD_ENVELOPE_SERIALIZER_ID,
        Manifest::new(RoutedShardEnvelope::MANIFEST),
        RoutedShardEnvelope::VERSION,
        fixture_bytes(ROUTED_SHARD_ENVELOPE_V1_FIXTURE),
    );

    assert_eq!(
        registry()
            .deserialize::<RoutedShardEnvelope>(serialized)
            .unwrap(),
        routed_shard_envelope_v1()
    );
}
