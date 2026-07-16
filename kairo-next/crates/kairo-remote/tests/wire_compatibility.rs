use bytes::Bytes;
use kairo_remote::{decode_remote_envelope_frame, encode_remote_envelope_frame};
use kairo_serialization::{ActorRefWireData, Manifest, RemoteEnvelope, SerializedMessage};

const FRAME_V1_FIXTURE: &str = include_str!("fixtures/remote-envelope-frame-v1.hex");

fn frame_v1_fixture() -> Bytes {
    let hex = FRAME_V1_FIXTURE.split_whitespace().collect::<String>();
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

#[test]
fn frame_v1_fixture_decodes_stable_envelope_metadata() {
    let decoded = decode_remote_envelope_frame(frame_v1_fixture()).unwrap();

    assert_eq!(decoded, frame_v1_envelope());
}

#[test]
fn frame_v1_encoding_matches_checked_fixture() {
    let encoded = encode_remote_envelope_frame(&frame_v1_envelope()).unwrap();

    assert_eq!(encoded, frame_v1_fixture());
}
