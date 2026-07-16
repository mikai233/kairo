use bytes::Bytes;
use kairo_remote::{
    RELIABLE_SYSTEM_ACK_SERIALIZER_ID, RELIABLE_SYSTEM_ENVELOPE_SERIALIZER_ID,
    RELIABLE_SYSTEM_NACK_SERIALIZER_ID, ReliableSystemAck, ReliableSystemEnvelope,
    ReliableSystemNack, WATCH_REMOTE_SERIALIZER_ID, WatchRemote, decode_remote_envelope_frame,
    encode_remote_envelope_frame, register_remote_protocol_codecs,
};
use kairo_serialization::{
    ActorRefWireData, Manifest, Registry, RemoteEnvelope, RemoteMessage, SerializedMessage,
};

const FRAME_V1_FIXTURE: &str = include_str!("fixtures/remote-envelope-frame-v1.hex");
const RELIABLE_ENVELOPE_V1_FIXTURE: &str = include_str!("fixtures/reliable-system-envelope-v1.hex");
const RELIABLE_REPLY_V1_FIXTURE: &str = include_str!("fixtures/reliable-system-reply-v1.hex");

fn fixture_bytes(fixture: &str) -> Bytes {
    let hex = fixture.split_whitespace().collect::<String>();
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
    register_remote_protocol_codecs(&mut registry).unwrap();
    registry
}

fn frame_v1_envelope() -> RemoteEnvelope {
    RemoteEnvelope::new(
        ActorRefWireData::new("kairo://target@127.0.0.1:25520/user/receiver#1").unwrap(),
        Some(ActorRefWireData::new("kairo://sender@127.0.0.1:25521/user/source#2").unwrap()),
        SerializedMessage::new(
            42,
            Manifest::new("kairo.remote.test.Frame"),
            7,
            Bytes::from_static(&[1, 2, 3, 4]),
        ),
    )
}

fn watch_remote_v1() -> WatchRemote {
    WatchRemote {
        watchee: ActorRefWireData::new("kairo://beta@127.0.0.1:25520/user/worker#22").unwrap(),
        watcher: ActorRefWireData::new("kairo://alpha@127.0.0.1:25521/user/observer#11").unwrap(),
    }
}

fn reliable_system_envelope_v1() -> ReliableSystemEnvelope {
    ReliableSystemEnvelope {
        from_uid: 0x0102_0304_0506_0708,
        to_uid: 0x1112_1314_1516_1718,
        sequence_nr: 0x2122_2324_2526_2728,
        envelope: RemoteEnvelope::new(
            ActorRefWireData::new("kairo://beta@127.0.0.1:25520/system/remote-watcher#22").unwrap(),
            Some(
                ActorRefWireData::new("kairo://alpha@127.0.0.1:25521/system/remote-watcher#11")
                    .unwrap(),
            ),
            registry().serialize(&watch_remote_v1()).unwrap(),
        ),
    }
}

fn reliable_system_ack_v1() -> ReliableSystemAck {
    ReliableSystemAck {
        from_uid: 0x1112_1314_1516_1718,
        to_uid: 0x0102_0304_0506_0708,
        sequence_nr: 0x2122_2324_2526_2728,
    }
}

fn reliable_system_nack_v1() -> ReliableSystemNack {
    ReliableSystemNack {
        from_uid: 0x1112_1314_1516_1718,
        to_uid: 0x0102_0304_0506_0708,
        highest_contiguous_sequence_nr: 0x2122_2324_2526_2728,
    }
}

#[test]
fn frame_v1_fixture_decodes_stable_envelope_metadata() {
    let decoded = decode_remote_envelope_frame(fixture_bytes(FRAME_V1_FIXTURE)).unwrap();

    assert_eq!(decoded, frame_v1_envelope());
}

#[test]
fn frame_v1_encoding_matches_checked_fixture() {
    let encoded = encode_remote_envelope_frame(&frame_v1_envelope()).unwrap();

    assert_eq!(encoded, fixture_bytes(FRAME_V1_FIXTURE));
}

#[test]
fn reliable_system_envelope_v1_encoding_matches_checked_fixture() {
    let serialized = registry()
        .serialize(&reliable_system_envelope_v1())
        .unwrap();
    let fixture = fixture_bytes(RELIABLE_ENVELOPE_V1_FIXTURE);

    assert_eq!(
        serialized.serializer_id,
        RELIABLE_SYSTEM_ENVELOPE_SERIALIZER_ID
    );
    assert_eq!(
        serialized.manifest.as_str(),
        ReliableSystemEnvelope::MANIFEST
    );
    assert_eq!(serialized.version, ReliableSystemEnvelope::VERSION);
    assert_eq!(
        serialized.payload,
        fixture,
        "reliable-system-envelope v1 payload: {}",
        encode_hex(&serialized.payload)
    );
}

#[test]
fn reliable_system_envelope_v1_fixture_decodes_incarnations_sequence_and_watch() {
    let serialized = SerializedMessage::new(
        RELIABLE_SYSTEM_ENVELOPE_SERIALIZER_ID,
        Manifest::new(ReliableSystemEnvelope::MANIFEST),
        ReliableSystemEnvelope::VERSION,
        fixture_bytes(RELIABLE_ENVELOPE_V1_FIXTURE),
    );

    let registry = registry();
    let decoded = registry
        .deserialize::<ReliableSystemEnvelope>(serialized)
        .unwrap();

    assert_eq!(decoded, reliable_system_envelope_v1());
    assert_eq!(
        decoded.envelope.message.serializer_id,
        WATCH_REMOTE_SERIALIZER_ID
    );
    assert_eq!(
        decoded.envelope.message.manifest.as_str(),
        WatchRemote::MANIFEST
    );
    assert_eq!(decoded.envelope.message.version, WatchRemote::VERSION);
    assert_eq!(
        registry
            .deserialize::<WatchRemote>(decoded.envelope.message)
            .unwrap(),
        watch_remote_v1()
    );
}

#[test]
fn reliable_system_ack_and_nack_v1_encoding_matches_checked_reply_fixture() {
    let registry = registry();
    let ack = registry.serialize(&reliable_system_ack_v1()).unwrap();
    let nack = registry.serialize(&reliable_system_nack_v1()).unwrap();
    let fixture = fixture_bytes(RELIABLE_REPLY_V1_FIXTURE);

    assert_eq!(ack.serializer_id, RELIABLE_SYSTEM_ACK_SERIALIZER_ID);
    assert_eq!(ack.manifest.as_str(), ReliableSystemAck::MANIFEST);
    assert_eq!(ack.version, ReliableSystemAck::VERSION);
    assert_eq!(nack.serializer_id, RELIABLE_SYSTEM_NACK_SERIALIZER_ID);
    assert_eq!(nack.manifest.as_str(), ReliableSystemNack::MANIFEST);
    assert_eq!(nack.version, ReliableSystemNack::VERSION);
    assert_eq!(
        ack.payload,
        fixture,
        "reliable-system-reply v1 payload: {}",
        encode_hex(&ack.payload)
    );
    assert_eq!(nack.payload, fixture);
}

#[test]
fn reliable_system_reply_v1_fixture_decodes_as_ack_and_nack() {
    let registry = registry();
    let fixture = fixture_bytes(RELIABLE_REPLY_V1_FIXTURE);
    let ack = SerializedMessage::new(
        RELIABLE_SYSTEM_ACK_SERIALIZER_ID,
        Manifest::new(ReliableSystemAck::MANIFEST),
        ReliableSystemAck::VERSION,
        fixture.clone(),
    );
    let nack = SerializedMessage::new(
        RELIABLE_SYSTEM_NACK_SERIALIZER_ID,
        Manifest::new(ReliableSystemNack::MANIFEST),
        ReliableSystemNack::VERSION,
        fixture,
    );

    assert_eq!(
        registry.deserialize::<ReliableSystemAck>(ack).unwrap(),
        reliable_system_ack_v1()
    );
    assert_eq!(
        registry.deserialize::<ReliableSystemNack>(nack).unwrap(),
        reliable_system_nack_v1()
    );
}
