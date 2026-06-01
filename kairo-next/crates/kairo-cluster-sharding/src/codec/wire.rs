use bytes::Bytes;
use kairo_serialization::{
    ActorRefWireData, Manifest, RemoteMessage, SerializationError, SerializedMessage, WireReader,
    WireWriter,
};

pub(super) fn encode_actor_ref(ref_data: &ActorRefWireData) -> kairo_serialization::Result<Bytes> {
    let mut writer = WireWriter::new();
    writer.write_string(ref_data.path())?;
    Ok(writer.finish())
}

pub(super) fn decode_actor_ref(payload: &Bytes) -> kairo_serialization::Result<ActorRefWireData> {
    let mut reader = WireReader::new(payload);
    ActorRefWireData::new(reader.read_string()?)
}

pub(super) fn encode_shard_id(shard_id: &str) -> kairo_serialization::Result<Bytes> {
    let mut writer = WireWriter::new();
    writer.write_string(shard_id)?;
    Ok(writer.finish())
}

pub(super) fn decode_shard_id(payload: &Bytes) -> kairo_serialization::Result<String> {
    let mut reader = WireReader::new(payload);
    reader.read_string()
}

pub(super) fn write_serialized_message(
    writer: &mut WireWriter,
    message: &SerializedMessage,
) -> kairo_serialization::Result<()> {
    writer.write_u32(message.serializer_id);
    writer.write_string(message.manifest.as_str())?;
    writer.write_u16(message.version);
    writer.write_bytes(&message.payload)
}

pub(super) fn read_serialized_message(
    reader: &mut WireReader<'_>,
) -> kairo_serialization::Result<SerializedMessage> {
    Ok(SerializedMessage::new(
        reader.read_u32()?,
        Manifest::try_new(reader.read_string()?)?,
        reader.read_u16()?,
        reader.read_bytes()?,
    ))
}

pub(super) fn ensure_version<M>(version: u16) -> kairo_serialization::Result<()>
where
    M: RemoteMessage,
{
    if version == M::VERSION {
        Ok(())
    } else {
        Err(SerializationError::Message(format!(
            "unsupported {} version {version}",
            M::MANIFEST
        )))
    }
}
