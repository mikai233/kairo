use bytes::Bytes;
use kairo_serialization::{
    ActorRefWireData, Manifest, RemoteEnvelope, SerializedMessage, WireReader, WireWriter,
};

use crate::{RemoteError, Result};

const REMOTE_ENVELOPE_FRAME_MAGIC: u64 = 0x4b4149524f52454d;
const REMOTE_ENVELOPE_FRAME_VERSION: u16 = 1;

pub fn encode_remote_envelope_frame(envelope: &RemoteEnvelope) -> Result<Bytes> {
    let mut writer = WireWriter::new();
    writer.write_u64(REMOTE_ENVELOPE_FRAME_MAGIC);
    writer.write_u16(REMOTE_ENVELOPE_FRAME_VERSION);
    write_actor_ref(&mut writer, &envelope.recipient)?;
    writer.write_bool(envelope.sender.is_some());
    if let Some(sender) = &envelope.sender {
        write_actor_ref(&mut writer, sender)?;
    }
    writer.write_u32(envelope.message.serializer_id);
    writer.write_string(envelope.message.manifest.as_str())?;
    writer.write_u16(envelope.message.version);
    writer.write_bytes(&envelope.message.payload)?;
    Ok(writer.finish())
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
    let recipient = read_actor_ref(&mut reader)?;
    let sender = if reader.read_bool()? {
        Some(read_actor_ref(&mut reader)?)
    } else {
        None
    };
    let serializer_id = reader.read_u32()?;
    let manifest = Manifest::new(reader.read_string()?);
    let version = reader.read_u16()?;
    let payload = reader.read_bytes()?;
    reader.ensure_finished()?;
    Ok(RemoteEnvelope::new(
        recipient,
        sender,
        SerializedMessage::new(serializer_id, manifest, version, payload),
    ))
}

fn write_actor_ref(writer: &mut WireWriter, wire: &ActorRefWireData) -> Result<()> {
    writer.write_string(wire.path())?;
    writer.write_string(wire.protocol())?;
    writer.write_string(wire.system())?;
    writer.write_optional_string(wire.host())?;
    writer.write_optional_u64(wire.port().map(u64::from));
    Ok(())
}

fn read_actor_ref(reader: &mut WireReader<'_>) -> Result<ActorRefWireData> {
    let path = reader.read_string()?;
    let protocol = reader.read_string()?;
    let system = reader.read_string()?;
    let host = reader.read_optional_string()?;
    let port = reader
        .read_optional_u64()?
        .map(u16::try_from)
        .transpose()
        .map_err(|_| RemoteError::InvalidFrame("actor ref port exceeds u16".to_string()))?;
    Ok(ActorRefWireData::from_parts(
        protocol, system, host, port, path,
    )?)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use kairo_serialization::{ActorRefWireData, Manifest, SerializedMessage};

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
    fn remote_envelope_frame_rejects_trailing_bytes() {
        let mut frame = encode_remote_envelope_frame(&envelope()).unwrap().to_vec();
        frame.push(0xff);

        let error = decode_remote_envelope_frame(Bytes::from(frame)).unwrap_err();

        assert!(error.to_string().contains("trailing byte"));
    }
}
