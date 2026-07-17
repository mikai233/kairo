use bytes::Bytes;
use kairo_actor::Address;
use kairo_cluster::{
    GOSSIP_ENVELOPE_SERIALIZER_ID, Gossip, GossipEnvelope, HEARTBEAT_RSP_SERIALIZER_ID,
    HEARTBEAT_SERIALIZER_ID, Heartbeat, HeartbeatRsp, Member, MemberStatus, Reachability,
    UniqueAddress, VectorClock, VectorClockNode, register_cluster_protocol_codecs,
};
use kairo_serialization::{Manifest, Registry, RemoteMessage, SerializedMessage};

const GOSSIP_ENVELOPE_V1_FIXTURE: &str = include_str!("fixtures/gossip-envelope-v1.hex");
const HEARTBEAT_V1_FIXTURE: &str = include_str!("fixtures/heartbeat-v1.hex");
const HEARTBEAT_RSP_V1_FIXTURE: &str = include_str!("fixtures/heartbeat-rsp-v1.hex");

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

fn node(system: &str, port: u16, uid: u64) -> UniqueAddress {
    UniqueAddress::new(
        Address::new("kairo", system, Some("127.0.0.1".to_string()), Some(port)),
        uid,
    )
}

fn gossip_envelope_v1() -> GossipEnvelope {
    let alpha = node("alpha", 25_520, 0x0102_0304_0506_0708);
    let beta = node("beta", 25_521, 0x1112_1314_1516_1718);
    let gamma = node("gamma", 25_522, 0x2122_2324_2526_2728);
    let members = vec![
        Member::new(
            alpha.clone(),
            vec!["backend".to_string(), "blue".to_string()],
        )
        .with_status(MemberStatus::Up)
        .with_up_number(7),
        Member::new(beta.clone(), vec!["frontend".to_string()])
            .with_status(MemberStatus::Leaving)
            .with_up_number(8),
    ];
    let reachability = Reachability::new()
        .unreachable(alpha.clone(), beta.clone())
        .terminated(beta.clone(), gamma.clone());
    let version = VectorClock::new()
        .increment(VectorClockNode::new("alpha-clock"))
        .increment(VectorClockNode::new("beta-clock"))
        .increment(VectorClockNode::new("beta-clock"));

    GossipEnvelope {
        from: alpha.clone(),
        to: beta.clone(),
        sequence_nr: 0x3132_3334_3536_3738,
        gossip: Gossip::from_parts(
            members,
            vec![beta, alpha],
            reachability,
            version,
            vec![(gamma, 99)],
        ),
    }
}

fn registry() -> Registry {
    let mut registry = Registry::new();
    register_cluster_protocol_codecs(&mut registry).unwrap();
    registry
}

#[test]
fn gossip_envelope_v1_encoding_matches_checked_fixture() {
    let envelope = gossip_envelope_v1();
    let serialized = registry().serialize(&envelope).unwrap();
    let fixture = fixture_bytes(GOSSIP_ENVELOPE_V1_FIXTURE);

    assert_eq!(serialized.serializer_id, GOSSIP_ENVELOPE_SERIALIZER_ID);
    assert_eq!(serialized.manifest.as_str(), GossipEnvelope::MANIFEST);
    assert_eq!(serialized.version, GossipEnvelope::VERSION);
    assert_eq!(
        serialized.payload,
        fixture,
        "encoded payload: {}",
        encode_hex(&serialized.payload)
    );
}

#[test]
fn gossip_envelope_v1_fixture_decodes_full_cluster_state() {
    let serialized = SerializedMessage::new(
        GOSSIP_ENVELOPE_SERIALIZER_ID,
        Manifest::new(GossipEnvelope::MANIFEST),
        GossipEnvelope::VERSION,
        fixture_bytes(GOSSIP_ENVELOPE_V1_FIXTURE),
    );

    assert_eq!(
        registry()
            .deserialize::<GossipEnvelope>(serialized)
            .unwrap(),
        gossip_envelope_v1()
    );
}

fn heartbeat_v1() -> Heartbeat {
    Heartbeat {
        from: node("heartbeat-a", 25_530, 0x3132_3334_3536_3738),
        sequence_nr: 0x4142_4344_4546_4748,
        creation_time_nanos: 0x5152_5354_5556_5758,
    }
}

fn heartbeat_rsp_v1() -> HeartbeatRsp {
    HeartbeatRsp {
        from: node("heartbeat-b", 25_531, 0x2122_2324_2526_2728),
        sequence_nr: heartbeat_v1().sequence_nr,
        creation_time_nanos: heartbeat_v1().creation_time_nanos,
    }
}

fn assert_v1_fixture<M: RemoteMessage>(
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

#[test]
fn heartbeat_v1_encoding_matches_checked_fixture() {
    let serialized = registry().serialize(&heartbeat_v1()).unwrap();
    assert_v1_fixture::<Heartbeat>(
        &serialized,
        HEARTBEAT_SERIALIZER_ID,
        &fixture_bytes(HEARTBEAT_V1_FIXTURE),
    );
}

#[test]
fn heartbeat_v1_fixture_decodes_request_correlation_fields() {
    let serialized = SerializedMessage::new(
        HEARTBEAT_SERIALIZER_ID,
        Manifest::new(Heartbeat::MANIFEST),
        Heartbeat::VERSION,
        fixture_bytes(HEARTBEAT_V1_FIXTURE),
    );

    assert_eq!(
        registry().deserialize::<Heartbeat>(serialized).unwrap(),
        heartbeat_v1()
    );
}

#[test]
fn heartbeat_rsp_v1_encoding_matches_checked_fixture() {
    let serialized = registry().serialize(&heartbeat_rsp_v1()).unwrap();
    assert_v1_fixture::<HeartbeatRsp>(
        &serialized,
        HEARTBEAT_RSP_SERIALIZER_ID,
        &fixture_bytes(HEARTBEAT_RSP_V1_FIXTURE),
    );
}

#[test]
fn heartbeat_rsp_v1_fixture_decodes_responder_and_echoed_correlation() {
    let serialized = SerializedMessage::new(
        HEARTBEAT_RSP_SERIALIZER_ID,
        Manifest::new(HeartbeatRsp::MANIFEST),
        HeartbeatRsp::VERSION,
        fixture_bytes(HEARTBEAT_RSP_V1_FIXTURE),
    );
    let response = registry().deserialize::<HeartbeatRsp>(serialized).unwrap();
    let request = heartbeat_v1();

    assert_eq!(response, heartbeat_rsp_v1());
    assert_eq!(response.sequence_nr, request.sequence_nr);
    assert_eq!(response.creation_time_nanos, request.creation_time_nanos);
}
