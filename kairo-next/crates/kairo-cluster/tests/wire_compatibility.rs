use bytes::Bytes;
use kairo_actor::Address;
use kairo_cluster::{
    GOSSIP_ENVELOPE_SERIALIZER_ID, Gossip, GossipEnvelope, Member, MemberStatus, Reachability,
    UniqueAddress, VectorClock, VectorClockNode, register_cluster_protocol_codecs,
};
use kairo_serialization::{Manifest, Registry, RemoteMessage, SerializedMessage};

const GOSSIP_ENVELOPE_V1_FIXTURE: &str = include_str!("fixtures/gossip-envelope-v1.hex");

fn fixture_bytes() -> Bytes {
    let hex = GOSSIP_ENVELOPE_V1_FIXTURE
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
    let fixture = fixture_bytes();

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
        fixture_bytes(),
    );

    assert_eq!(
        registry()
            .deserialize::<GossipEnvelope>(serialized)
            .unwrap(),
        gossip_envelope_v1()
    );
}
