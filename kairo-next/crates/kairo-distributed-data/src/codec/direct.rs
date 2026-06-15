use bytes::Bytes;
use kairo_serialization::{MessageCodec, RemoteMessage, WireReader, WireWriter};

use super::{
    REPLICATOR_READ_RESULT_SERIALIZER_ID, REPLICATOR_READ_SERIALIZER_ID,
    REPLICATOR_WRITE_ACK_SERIALIZER_ID, REPLICATOR_WRITE_NACK_SERIALIZER_ID,
    REPLICATOR_WRITE_SERIALIZER_ID,
    helpers::{
        ensure_empty_payload, ensure_version, ensure_version_range, read_data_envelope,
        write_data_envelope,
    },
};
use crate::{
    ReplicaId, ReplicatorRead, ReplicatorReadResult, ReplicatorWrite, ReplicatorWriteAck,
    ReplicatorWriteNack,
};

#[derive(Debug, Clone, Copy)]
pub struct ReplicatorWriteCodec;

impl MessageCodec<ReplicatorWrite> for ReplicatorWriteCodec {
    fn serializer_id(&self) -> u32 {
        REPLICATOR_WRITE_SERIALIZER_ID
    }

    fn encode(&self, message: &ReplicatorWrite) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        writer.write_string(&message.key)?;
        writer.write_optional_string(message.from.as_ref().map(ReplicaId::as_str))?;
        write_data_envelope(&mut writer, &message.envelope)?;
        Ok(writer.finish())
    }

    fn decode(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<ReplicatorWrite> {
        ensure_version_range(
            ReplicatorWrite::MANIFEST,
            version,
            1,
            ReplicatorWrite::VERSION,
        )?;
        let mut reader = WireReader::new(&payload);
        let message = ReplicatorWrite {
            key: reader.read_string()?,
            from: reader.read_optional_string()?.map(ReplicaId::new),
            envelope: read_data_envelope(&mut reader, version)?,
        };
        reader.ensure_finished()?;
        Ok(message)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ReplicatorWriteAckCodec;

impl MessageCodec<ReplicatorWriteAck> for ReplicatorWriteAckCodec {
    fn serializer_id(&self) -> u32 {
        REPLICATOR_WRITE_ACK_SERIALIZER_ID
    }

    fn encode(&self, _message: &ReplicatorWriteAck) -> kairo_serialization::Result<Bytes> {
        Ok(Bytes::new())
    }

    fn decode(
        &self,
        payload: Bytes,
        version: u16,
    ) -> kairo_serialization::Result<ReplicatorWriteAck> {
        ensure_version::<ReplicatorWriteAck>(version)?;
        ensure_empty_payload(&payload, ReplicatorWriteAck::MANIFEST)?;
        Ok(ReplicatorWriteAck)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ReplicatorWriteNackCodec;

impl MessageCodec<ReplicatorWriteNack> for ReplicatorWriteNackCodec {
    fn serializer_id(&self) -> u32 {
        REPLICATOR_WRITE_NACK_SERIALIZER_ID
    }

    fn encode(&self, _message: &ReplicatorWriteNack) -> kairo_serialization::Result<Bytes> {
        Ok(Bytes::new())
    }

    fn decode(
        &self,
        payload: Bytes,
        version: u16,
    ) -> kairo_serialization::Result<ReplicatorWriteNack> {
        ensure_version::<ReplicatorWriteNack>(version)?;
        ensure_empty_payload(&payload, ReplicatorWriteNack::MANIFEST)?;
        Ok(ReplicatorWriteNack)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ReplicatorReadCodec;

impl MessageCodec<ReplicatorRead> for ReplicatorReadCodec {
    fn serializer_id(&self) -> u32 {
        REPLICATOR_READ_SERIALIZER_ID
    }

    fn encode(&self, message: &ReplicatorRead) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        writer.write_string(&message.key)?;
        writer.write_optional_string(message.from.as_ref().map(ReplicaId::as_str))?;
        Ok(writer.finish())
    }

    fn decode(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<ReplicatorRead> {
        ensure_version::<ReplicatorRead>(version)?;
        let mut reader = WireReader::new(&payload);
        let message = ReplicatorRead {
            key: reader.read_string()?,
            from: reader.read_optional_string()?.map(ReplicaId::new),
        };
        reader.ensure_finished()?;
        Ok(message)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ReplicatorReadResultCodec;

impl MessageCodec<ReplicatorReadResult> for ReplicatorReadResultCodec {
    fn serializer_id(&self) -> u32 {
        REPLICATOR_READ_RESULT_SERIALIZER_ID
    }

    fn encode(&self, message: &ReplicatorReadResult) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        match &message.envelope {
            Some(envelope) => {
                writer.write_bool(true);
                write_data_envelope(&mut writer, envelope)?;
            }
            None => writer.write_bool(false),
        }
        Ok(writer.finish())
    }

    fn decode(
        &self,
        payload: Bytes,
        version: u16,
    ) -> kairo_serialization::Result<ReplicatorReadResult> {
        ensure_version_range(
            ReplicatorReadResult::MANIFEST,
            version,
            1,
            ReplicatorReadResult::VERSION,
        )?;
        let mut reader = WireReader::new(&payload);
        let envelope = if reader.read_bool()? {
            Some(read_data_envelope(&mut reader, version)?)
        } else {
            None
        };
        reader.ensure_finished()?;
        Ok(ReplicatorReadResult { envelope })
    }
}
