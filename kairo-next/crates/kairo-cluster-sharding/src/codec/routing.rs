use bytes::Bytes;
use kairo_serialization::{MessageCodec, WireReader, WireWriter};

use crate::RoutedShardEnvelope;

use super::wire::{ensure_version, read_serialized_message, write_serialized_message};

pub const ROUTED_SHARD_ENVELOPE_SERIALIZER_ID: u32 = 4_010;

#[derive(Debug, Clone, Copy)]
pub struct RoutedShardEnvelopeCodec;

impl MessageCodec<RoutedShardEnvelope> for RoutedShardEnvelopeCodec {
    fn serializer_id(&self) -> u32 {
        ROUTED_SHARD_ENVELOPE_SERIALIZER_ID
    }

    fn encode(&self, message: &RoutedShardEnvelope) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        writer.write_string(&message.shard_id)?;
        writer.write_string(&message.entity_id)?;
        write_serialized_message(&mut writer, &message.message)?;
        Ok(writer.finish())
    }

    fn decode(
        &self,
        payload: Bytes,
        version: u16,
    ) -> kairo_serialization::Result<RoutedShardEnvelope> {
        ensure_version::<RoutedShardEnvelope>(version)?;
        let mut reader = WireReader::new(&payload);
        let envelope = RoutedShardEnvelope {
            shard_id: reader.read_string()?,
            entity_id: reader.read_string()?,
            message: read_serialized_message(&mut reader)?,
        };
        reader.ensure_finished()?;
        Ok(envelope)
    }
}
