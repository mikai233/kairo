use bytes::Bytes;
use kairo_serialization::{MessageCodec, RemoteMessage, WireReader, WireWriter};

use super::{
    REPLICATOR_DELTA_ACK_SERIALIZER_ID, REPLICATOR_DELTA_NACK_SERIALIZER_ID,
    REPLICATOR_DELTA_PROPAGATION_SERIALIZER_ID,
    helpers::{
        ensure_empty_payload, ensure_version, ensure_version_range, len_to_u64, read_delta,
        u64_to_len, write_delta,
    },
};
use crate::{ReplicaId, ReplicatorDeltaAck, ReplicatorDeltaNack, ReplicatorDeltaPropagation};

#[derive(Debug, Clone, Copy)]
/// Codec for version-ranged delta batches and their pruning metadata.
///
/// Decode accepts versions 1 through the current message version. Version 1
/// records contain no per-key pruning metadata.
pub struct ReplicatorDeltaPropagationCodec;

impl MessageCodec<ReplicatorDeltaPropagation> for ReplicatorDeltaPropagationCodec {
    fn serializer_id(&self) -> u32 {
        REPLICATOR_DELTA_PROPAGATION_SERIALIZER_ID
    }

    fn encode(&self, message: &ReplicatorDeltaPropagation) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        writer.write_string(message.from.as_str())?;
        writer.write_bool(message.reply);
        writer.write_u64(len_to_u64(message.deltas.len())?);
        for delta in &message.deltas {
            write_delta(&mut writer, delta)?;
        }
        Ok(writer.finish())
    }

    fn decode(
        &self,
        payload: Bytes,
        version: u16,
    ) -> kairo_serialization::Result<ReplicatorDeltaPropagation> {
        ensure_version_range(
            ReplicatorDeltaPropagation::MANIFEST,
            version,
            1,
            ReplicatorDeltaPropagation::VERSION,
        )?;
        let mut reader = WireReader::new(&payload);
        let from = ReplicaId::new(reader.read_string()?);
        let reply = reader.read_bool()?;
        let len = u64_to_len(reader.read_u64()?)?;
        let mut deltas = Vec::with_capacity(len);
        for _ in 0..len {
            deltas.push(read_delta(&mut reader, version)?);
        }
        let message = ReplicatorDeltaPropagation {
            from,
            reply,
            deltas,
        };
        reader.ensure_finished()?;
        Ok(message)
    }
}

#[derive(Debug, Clone, Copy)]
/// Empty-payload codec for accepted delta propagation.
pub struct ReplicatorDeltaAckCodec;

impl MessageCodec<ReplicatorDeltaAck> for ReplicatorDeltaAckCodec {
    fn serializer_id(&self) -> u32 {
        REPLICATOR_DELTA_ACK_SERIALIZER_ID
    }

    fn encode(&self, _message: &ReplicatorDeltaAck) -> kairo_serialization::Result<Bytes> {
        Ok(Bytes::new())
    }

    fn decode(
        &self,
        payload: Bytes,
        version: u16,
    ) -> kairo_serialization::Result<ReplicatorDeltaAck> {
        ensure_version::<ReplicatorDeltaAck>(version)?;
        ensure_empty_payload(&payload, ReplicatorDeltaAck::MANIFEST)?;
        Ok(ReplicatorDeltaAck)
    }
}

#[derive(Debug, Clone, Copy)]
/// Empty-payload codec for delta propagation requiring full-state retry.
pub struct ReplicatorDeltaNackCodec;

impl MessageCodec<ReplicatorDeltaNack> for ReplicatorDeltaNackCodec {
    fn serializer_id(&self) -> u32 {
        REPLICATOR_DELTA_NACK_SERIALIZER_ID
    }

    fn encode(&self, _message: &ReplicatorDeltaNack) -> kairo_serialization::Result<Bytes> {
        Ok(Bytes::new())
    }

    fn decode(
        &self,
        payload: Bytes,
        version: u16,
    ) -> kairo_serialization::Result<ReplicatorDeltaNack> {
        ensure_version::<ReplicatorDeltaNack>(version)?;
        ensure_empty_payload(&payload, ReplicatorDeltaNack::MANIFEST)?;
        Ok(ReplicatorDeltaNack)
    }
}
