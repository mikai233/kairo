use bytes::Bytes;
use kairo_actor::Address;
use kairo_cluster::{
    ApplicationVersion, ClusterConfigCheck, DOWN_SERIALIZER_ID, Down,
    EXITING_CONFIRMED_SERIALIZER_ID, ExitingConfirmed, GOSSIP_ENVELOPE_SERIALIZER_ID,
    GOSSIP_STATUS_SERIALIZER_ID, Gossip, GossipEnvelope, GossipStatus, HEARTBEAT_RSP_SERIALIZER_ID,
    HEARTBEAT_SERIALIZER_ID, Heartbeat, HeartbeatRsp, INIT_JOIN_ACK_SERIALIZER_ID,
    INIT_JOIN_NACK_SERIALIZER_ID, INIT_JOIN_SERIALIZER_ID, InitJoin, InitJoinAck, InitJoinNack,
    JOIN_SERIALIZER_ID, Join, LEAVE_SERIALIZER_ID, Leave, Member, MemberStatus, Reachability,
    UniqueAddress, VectorClock, VectorClockNode, WELCOME_SERIALIZER_ID, Welcome,
    register_cluster_protocol_codecs,
};
use kairo_serialization::{Manifest, Registry, RemoteMessage, SerializedMessage};

const GOSSIP_ENVELOPE_V1_FIXTURE: &str = include_str!("fixtures/gossip-envelope-v1.hex");
const GOSSIP_ENVELOPE_V2_FIXTURE: &str = include_str!("fixtures/gossip-envelope-v2.hex");
const HEARTBEAT_V1_FIXTURE: &str = include_str!("fixtures/heartbeat-v1.hex");
const HEARTBEAT_RSP_V1_FIXTURE: &str = include_str!("fixtures/heartbeat-rsp-v1.hex");
const INIT_JOIN_V1_FIXTURE: &str = include_str!("fixtures/init-join-v1.hex");
const INIT_JOIN_ACK_V1_FIXTURE: &str = include_str!("fixtures/init-join-ack-v1.hex");
const INIT_JOIN_NACK_V1_FIXTURE: &str = include_str!("fixtures/init-join-nack-v1.hex");
const JOIN_V1_FIXTURE: &str = include_str!("fixtures/join-v1.hex");
const JOIN_V2_FIXTURE: &str = include_str!("fixtures/join-v2.hex");
const WELCOME_V1_FIXTURE: &str = include_str!("fixtures/welcome-v1.hex");
const WELCOME_V2_FIXTURE: &str = include_str!("fixtures/welcome-v2.hex");
const GOSSIP_STATUS_V1_FIXTURE: &str = include_str!("fixtures/gossip-status-v1.hex");
const LEAVE_V1_FIXTURE: &str = include_str!("fixtures/leave-v1.hex");
const DOWN_V1_FIXTURE: &str = include_str!("fixtures/down-v1.hex");
const EXITING_CONFIRMED_V1_FIXTURE: &str = include_str!("fixtures/exiting-confirmed-v1.hex");

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

fn gossip_envelope(
    alpha_version: ApplicationVersion,
    beta_version: ApplicationVersion,
) -> GossipEnvelope {
    let alpha = node("alpha", 25_520, 0x0102_0304_0506_0708);
    let beta = node("beta", 25_521, 0x1112_1314_1516_1718);
    let gamma = node("gamma", 25_522, 0x2122_2324_2526_2728);
    let members = vec![
        Member::new(
            alpha.clone(),
            vec!["backend".to_string(), "blue".to_string()],
        )
        .with_status(MemberStatus::Up)
        .with_up_number(7)
        .with_app_version(alpha_version),
        Member::new(beta.clone(), vec!["frontend".to_string()])
            .with_status(MemberStatus::Leaving)
            .with_up_number(8)
            .with_app_version(beta_version),
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

fn gossip_envelope_v1() -> GossipEnvelope {
    gossip_envelope(ApplicationVersion::default(), ApplicationVersion::default())
}

fn gossip_envelope_v2() -> GossipEnvelope {
    gossip_envelope(
        ApplicationVersion::new("2.4.1").unwrap(),
        ApplicationVersion::new("2.5.0-RC1").unwrap(),
    )
}

fn registry() -> Registry {
    let mut registry = Registry::new();
    register_cluster_protocol_codecs(&mut registry).unwrap();
    registry
}

#[test]
fn gossip_envelope_v2_encoding_matches_checked_fixture() {
    let envelope = gossip_envelope_v2();
    let serialized = registry().serialize(&envelope).unwrap();
    let fixture = fixture_bytes(GOSSIP_ENVELOPE_V2_FIXTURE);

    assert_eq!(serialized.serializer_id, GOSSIP_ENVELOPE_SERIALIZER_ID);
    assert_eq!(serialized.manifest.as_str(), GossipEnvelope::MANIFEST);
    assert_eq!(serialized.version, GossipEnvelope::VERSION);
    assert_eq!(
        serialized.payload,
        fixture,
        "encoded v2 payload: {}",
        encode_hex(&serialized.payload)
    );
}

#[test]
fn gossip_envelope_v1_fixture_decodes_full_cluster_state() {
    let serialized = SerializedMessage::new(
        GOSSIP_ENVELOPE_SERIALIZER_ID,
        Manifest::new(GossipEnvelope::MANIFEST),
        1,
        fixture_bytes(GOSSIP_ENVELOPE_V1_FIXTURE),
    );

    assert_eq!(
        registry()
            .deserialize::<GossipEnvelope>(serialized)
            .unwrap(),
        gossip_envelope_v1()
    );
}

#[test]
fn gossip_envelope_v2_fixture_decodes_application_versions() {
    let serialized = SerializedMessage::new(
        GOSSIP_ENVELOPE_SERIALIZER_ID,
        Manifest::new(GossipEnvelope::MANIFEST),
        GossipEnvelope::VERSION,
        fixture_bytes(GOSSIP_ENVELOPE_V2_FIXTURE),
    );

    assert_eq!(
        registry()
            .deserialize::<GossipEnvelope>(serialized)
            .unwrap(),
        gossip_envelope_v2()
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

fn init_join_v1() -> InitJoin {
    InitJoin {
        joining_config_digest: Bytes::from_static(b"\xde\xad\xbe\xef\x01\x23\x45\x67"),
    }
}

fn init_join_ack_v1() -> InitJoinAck {
    InitJoinAck {
        address: Address::new(
            "kairo",
            "seed-ack",
            Some("192.0.2.10".to_string()),
            Some(25_540),
        ),
        config_check: ClusterConfigCheck::Compatible,
    }
}

fn init_join_nack_v1() -> InitJoinNack {
    InitJoinNack {
        address: Address::new(
            "kairo",
            "seed-nack",
            Some("2001:db8::10".to_string()),
            Some(25_541),
        ),
    }
}

fn join_v1() -> Join {
    Join {
        node: node("joining-node", 25_542, 0x6162_6364_6566_6768),
        roles: vec!["backend".to_string(), "dc-a".to_string()],
        app_version: ApplicationVersion::default(),
    }
}

fn join_v2() -> Join {
    Join {
        app_version: ApplicationVersion::new("3.2.1+10-ed316bd024").unwrap(),
        ..join_v1()
    }
}

fn welcome_v1() -> Welcome {
    let envelope = gossip_envelope_v1();
    Welcome {
        from: envelope.from,
        gossip: envelope.gossip,
    }
}

fn welcome_v2() -> Welcome {
    let envelope = gossip_envelope_v2();
    Welcome {
        from: envelope.from,
        gossip: envelope.gossip,
    }
}

fn assert_current_fixture<M: RemoteMessage>(message: &M, serializer_id: u32, fixture: &Bytes) {
    let serialized = registry().serialize(message).unwrap();
    assert_eq!(serialized.serializer_id, serializer_id);
    assert_eq!(serialized.manifest.as_str(), M::MANIFEST);
    assert_eq!(serialized.version, M::VERSION);
    assert_eq!(
        &serialized.payload,
        fixture,
        "{} v{} payload: {}",
        M::MANIFEST,
        M::VERSION,
        encode_hex(&serialized.payload)
    );
}

#[test]
fn join_v1_fixture_decodes_zero_application_version() {
    let serialized = SerializedMessage::new(
        JOIN_SERIALIZER_ID,
        Manifest::new(Join::MANIFEST),
        1,
        fixture_bytes(JOIN_V1_FIXTURE),
    );

    assert_eq!(
        registry().deserialize::<Join>(serialized).unwrap(),
        join_v1()
    );
}

#[test]
fn join_v2_encoding_matches_checked_fixture() {
    assert_current_fixture::<Join>(
        &join_v2(),
        JOIN_SERIALIZER_ID,
        &fixture_bytes(JOIN_V2_FIXTURE),
    );
}

#[test]
fn join_v2_fixture_decodes_application_version() {
    let serialized = SerializedMessage::new(
        JOIN_SERIALIZER_ID,
        Manifest::new(Join::MANIFEST),
        Join::VERSION,
        fixture_bytes(JOIN_V2_FIXTURE),
    );

    assert_eq!(
        registry().deserialize::<Join>(serialized).unwrap(),
        join_v2()
    );
}

#[test]
fn welcome_v1_fixture_decodes_zero_member_application_versions() {
    let serialized = SerializedMessage::new(
        WELCOME_SERIALIZER_ID,
        Manifest::new(Welcome::MANIFEST),
        1,
        fixture_bytes(WELCOME_V1_FIXTURE),
    );

    assert_eq!(
        registry().deserialize::<Welcome>(serialized).unwrap(),
        welcome_v1()
    );
}

#[test]
fn welcome_v2_encoding_matches_checked_fixture() {
    assert_current_fixture::<Welcome>(
        &welcome_v2(),
        WELCOME_SERIALIZER_ID,
        &fixture_bytes(WELCOME_V2_FIXTURE),
    );
}

#[test]
fn welcome_v2_fixture_decodes_member_application_versions() {
    let serialized = SerializedMessage::new(
        WELCOME_SERIALIZER_ID,
        Manifest::new(Welcome::MANIFEST),
        Welcome::VERSION,
        fixture_bytes(WELCOME_V2_FIXTURE),
    );

    assert_eq!(
        registry().deserialize::<Welcome>(serialized).unwrap(),
        welcome_v2()
    );
}

fn gossip_status_v1() -> GossipStatus {
    GossipStatus {
        from: node("status-node", 25_543, 0x7172_7374_7576_7778),
        version: VectorClock::new()
            .increment(VectorClockNode::new("status-alpha"))
            .increment(VectorClockNode::new("status-alpha"))
            .increment(VectorClockNode::new("status-beta")),
        seen_digest: Bytes::from_static(b"\x81\x82\x83\x84\x85"),
    }
}

fn leave_v1() -> Leave {
    Leave {
        address: node("leaving-node", 25_544, 1).address,
    }
}

fn down_v1() -> Down {
    Down {
        address: node("down-node", 25_545, 1).address,
    }
}

fn exiting_confirmed_v1() -> ExitingConfirmed {
    ExitingConfirmed {
        node: node("exiting-node", 25_546, 0x8182_8384_8586_8788),
    }
}

macro_rules! membership_control_v1_fixture_tests {
    (
        $encode_test:ident,
        $decode_test:ident,
        $message_type:ty,
        $message:ident,
        $serializer_id:expr,
        $fixture:ident
    ) => {
        #[test]
        fn $encode_test() {
            let serialized = registry().serialize(&$message()).unwrap();
            assert_v1_fixture::<$message_type>(
                &serialized,
                $serializer_id,
                &fixture_bytes($fixture),
            );
        }

        #[test]
        fn $decode_test() {
            let serialized = SerializedMessage::new(
                $serializer_id,
                Manifest::new(<$message_type>::MANIFEST),
                <$message_type>::VERSION,
                fixture_bytes($fixture),
            );

            assert_eq!(
                registry().deserialize::<$message_type>(serialized).unwrap(),
                $message()
            );
        }
    };
}

membership_control_v1_fixture_tests!(
    init_join_v1_encoding_matches_checked_fixture,
    init_join_v1_fixture_decodes_config_digest,
    InitJoin,
    init_join_v1,
    INIT_JOIN_SERIALIZER_ID,
    INIT_JOIN_V1_FIXTURE
);
membership_control_v1_fixture_tests!(
    init_join_ack_v1_encoding_matches_checked_fixture,
    init_join_ack_v1_fixture_decodes_seed_and_config_check,
    InitJoinAck,
    init_join_ack_v1,
    INIT_JOIN_ACK_SERIALIZER_ID,
    INIT_JOIN_ACK_V1_FIXTURE
);
membership_control_v1_fixture_tests!(
    init_join_nack_v1_encoding_matches_checked_fixture,
    init_join_nack_v1_fixture_decodes_declining_seed,
    InitJoinNack,
    init_join_nack_v1,
    INIT_JOIN_NACK_SERIALIZER_ID,
    INIT_JOIN_NACK_V1_FIXTURE
);
membership_control_v1_fixture_tests!(
    gossip_status_v1_encoding_matches_checked_fixture,
    gossip_status_v1_fixture_decodes_causal_summary,
    GossipStatus,
    gossip_status_v1,
    GOSSIP_STATUS_SERIALIZER_ID,
    GOSSIP_STATUS_V1_FIXTURE
);
membership_control_v1_fixture_tests!(
    leave_v1_encoding_matches_checked_fixture,
    leave_v1_fixture_decodes_canonical_address,
    Leave,
    leave_v1,
    LEAVE_SERIALIZER_ID,
    LEAVE_V1_FIXTURE
);
membership_control_v1_fixture_tests!(
    down_v1_encoding_matches_checked_fixture,
    down_v1_fixture_decodes_canonical_address,
    Down,
    down_v1,
    DOWN_SERIALIZER_ID,
    DOWN_V1_FIXTURE
);
membership_control_v1_fixture_tests!(
    exiting_confirmed_v1_encoding_matches_checked_fixture,
    exiting_confirmed_v1_fixture_decodes_node_incarnation,
    ExitingConfirmed,
    exiting_confirmed_v1,
    EXITING_CONFIRMED_SERIALIZER_ID,
    EXITING_CONFIRMED_V1_FIXTURE
);
