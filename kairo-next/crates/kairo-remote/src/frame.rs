use bytes::Bytes;
use kairo_serialization::{RemoteEnvelope, WireReader, WireWriter};

use crate::{RemoteError, Result};

const REMOTE_ENVELOPE_FRAME_MAGIC: u64 = 0x4b4149524f52454d;
const REMOTE_ENVELOPE_FRAME_VERSION: u16 = 1;

pub fn encode_remote_envelope_frame(envelope: &RemoteEnvelope) -> Result<Bytes> {
    let mut writer = WireWriter::new();
    writer.write_u64(REMOTE_ENVELOPE_FRAME_MAGIC);
    writer.write_u16(REMOTE_ENVELOPE_FRAME_VERSION);
    let mut frame = writer.finish().to_vec();
    frame.extend_from_slice(&envelope.encode_wire()?);
    Ok(Bytes::from(frame))
}

pub fn decode_remote_envelope_frame(bytes: Bytes) -> Result<RemoteEnvelope> {
    let mut reader = WireReader::new(&bytes);
    let magic = reader.read_u64()?;
    if magic != REMOTE_ENVELOPE_FRAME_MAGIC {
        return Err(RemoteError::InvalidFrame("invalid frame magic".to_string()));
    }
    let version = reader.read_u16()?;
    if version != REMOTE_ENVELOPE_FRAME_VERSION {
        return Err(RemoteError::InvalidFrame(format!(
            "unsupported frame version {version}"
        )));
    }
    let envelope_bytes = Bytes::copy_from_slice(reader.read_exact(reader.remaining_len())?);
    Ok(RemoteEnvelope::decode_wire(&envelope_bytes)?)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use kairo_serialization::{
        ActorRefWireData, Manifest, SerializationError, SerializedMessage, WireWriter,
    };

    use super::*;

    fn envelope() -> RemoteEnvelope {
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
    fn remote_envelope_frame_round_trips_wire_metadata() {
        let envelope = envelope();

        let decoded =
            decode_remote_envelope_frame(encode_remote_envelope_frame(&envelope).unwrap()).unwrap();

        assert_eq!(decoded, envelope);
        assert_eq!(decoded.recipient.protocol(), "kairo");
        assert_eq!(decoded.recipient.system(), "target");
        assert_eq!(decoded.recipient.host(), Some("127.0.0.1"));
        assert_eq!(decoded.recipient.port(), Some(25520));
        assert_eq!(decoded.message.serializer_id, 42);
        assert_eq!(decoded.message.manifest.as_str(), "kairo.remote.test.Frame");
        assert_eq!(decoded.message.version, 7);
        assert_eq!(decoded.message.payload, Bytes::from_static(&[1, 2, 3, 4]));
    }

    #[test]
    fn remote_envelope_frame_uses_canonical_envelope_body() {
        let envelope = envelope();
        let frame = encode_remote_envelope_frame(&envelope).unwrap();
        let mut header = WireWriter::new();
        header.write_u64(REMOTE_ENVELOPE_FRAME_MAGIC);
        header.write_u16(REMOTE_ENVELOPE_FRAME_VERSION);
        let header = header.finish();

        assert_eq!(&frame[..header.len()], &header[..]);
        assert_eq!(&frame[header.len()..], &envelope.encode_wire().unwrap()[..]);
    }

    #[test]
    fn remote_envelope_frame_preserves_missing_sender() {
        let mut envelope = envelope();
        envelope.sender = None;

        let decoded =
            decode_remote_envelope_frame(encode_remote_envelope_frame(&envelope).unwrap()).unwrap();

        assert_eq!(decoded.sender, None);
        assert_eq!(decoded, envelope);
    }

    #[test]
    fn remote_envelope_frame_rejects_invalid_magic() {
        let mut frame = encode_remote_envelope_frame(&envelope()).unwrap().to_vec();
        frame[0] = 0;

        let error = decode_remote_envelope_frame(Bytes::from(frame)).unwrap_err();

        assert!(matches!(error, RemoteError::InvalidFrame(_)));
        assert!(error.to_string().contains("magic"));
    }

    #[test]
    fn remote_envelope_frame_rejects_truncated_payloads() {
        let mut frame = encode_remote_envelope_frame(&envelope()).unwrap().to_vec();
        frame.truncate(frame.len() - 2);

        let error = decode_remote_envelope_frame(Bytes::from(frame)).unwrap_err();

        assert!(error.to_string().contains("ended early"));
    }

    #[test]
    fn remote_envelope_frame_rejects_invalid_actor_ref_body() {
        let mut writer = WireWriter::new();
        writer.write_u64(REMOTE_ENVELOPE_FRAME_MAGIC);
        writer.write_u16(REMOTE_ENVELOPE_FRAME_VERSION);
        writer.write_string("/user/receiver").unwrap();
        writer.write_optional_string(None).unwrap();
        writer.write_u32(42);
        writer.write_string("kairo.remote.test.Frame").unwrap();
        writer.write_u16(7);
        writer.write_bytes(&Bytes::from_static(&[1, 2, 3])).unwrap();

        let error = decode_remote_envelope_frame(writer.finish()).unwrap_err();

        assert!(matches!(
            error,
            RemoteError::Serialization(SerializationError::InvalidActorRefPath(_))
        ));
    }

    #[test]
    fn remote_envelope_frame_rejects_empty_manifest_metadata() {
        let mut writer = WireWriter::new();
        writer.write_u64(REMOTE_ENVELOPE_FRAME_MAGIC);
        writer.write_u16(REMOTE_ENVELOPE_FRAME_VERSION);
        writer
            .write_string("kairo://target@127.0.0.1:25520/user/receiver#1")
            .unwrap();
        writer.write_optional_string(None).unwrap();
        writer.write_u32(42);
        writer.write_string("   ").unwrap();
        writer.write_u16(7);
        writer.write_bytes(&Bytes::from_static(&[1, 2, 3])).unwrap();

        let error = decode_remote_envelope_frame(writer.finish()).unwrap_err();

        assert!(matches!(
            error,
            RemoteError::Serialization(SerializationError::InvalidManifest(_))
        ));
    }

    #[test]
    fn remote_envelope_frame_rejects_trailing_bytes() {
        let mut frame = encode_remote_envelope_frame(&envelope()).unwrap().to_vec();
        frame.push(0xff);

        let error = decode_remote_envelope_frame(Bytes::from(frame)).unwrap_err();

        assert!(error.to_string().contains("trailing byte"));
    }
}
