use bytes::Bytes;
use kairo_serialization::{MessageCodec, WireReader, WireWriter};

use super::{
    DATA_ENVELOPE_PRUNING_WIRE_VERSION, REPLICATOR_GOSSIP_SERIALIZER_ID,
    REPLICATOR_GOSSIP_STATUS_SERIALIZER_ID,
    helpers::{ensure_version, len_to_u64, read_data_envelope, u64_to_len, write_data_envelope},
};
use crate::{
    ReplicatorGossip, ReplicatorGossipDigest, ReplicatorGossipEntry, ReplicatorGossipStatus,
};

#[derive(Debug, Clone, Copy)]
pub struct ReplicatorGossipStatusCodec;

impl MessageCodec<ReplicatorGossipStatus> for ReplicatorGossipStatusCodec {
    fn serializer_id(&self) -> u32 {
        REPLICATOR_GOSSIP_STATUS_SERIALIZER_ID
    }

    fn encode(&self, message: &ReplicatorGossipStatus) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        writer.write_u32(message.chunk);
        writer.write_u32(message.total_chunks);
        writer.write_optional_u64(message.to_system_uid);
        writer.write_optional_u64(message.from_system_uid);
        writer.write_u64(len_to_u64(message.entries.len())?);
        for entry in &message.entries {
            writer.write_string(&entry.key)?;
            writer.write_u64(entry.digest);
            writer.write_u64(entry.used_timestamp_millis);
        }
        Ok(writer.finish())
    }

    fn decode(
        &self,
        payload: Bytes,
        version: u16,
    ) -> kairo_serialization::Result<ReplicatorGossipStatus> {
        ensure_version::<ReplicatorGossipStatus>(version)?;
        let mut reader = WireReader::new(&payload);
        let chunk = reader.read_u32()?;
        let total_chunks = reader.read_u32()?;
        let to_system_uid = reader.read_optional_u64()?;
        let from_system_uid = reader.read_optional_u64()?;
        let entry_count = u64_to_len(reader.read_u64()?)?;
        let mut entries = Vec::with_capacity(entry_count);
        for _ in 0..entry_count {
            entries.push(ReplicatorGossipDigest {
                key: reader.read_string()?,
                digest: reader.read_u64()?,
                used_timestamp_millis: reader.read_u64()?,
            });
        }
        let message = ReplicatorGossipStatus {
            entries,
            chunk,
            total_chunks,
            to_system_uid,
            from_system_uid,
        };
        reader.ensure_finished()?;
        Ok(message)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ReplicatorGossipCodec;

impl MessageCodec<ReplicatorGossip> for ReplicatorGossipCodec {
    fn serializer_id(&self) -> u32 {
        REPLICATOR_GOSSIP_SERIALIZER_ID
    }

    fn encode(&self, message: &ReplicatorGossip) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        writer.write_bool(message.send_back);
        writer.write_optional_u64(message.to_system_uid);
        writer.write_optional_u64(message.from_system_uid);
        writer.write_u64(len_to_u64(message.entries.len())?);
        for entry in &message.entries {
            writer.write_string(&entry.key)?;
            writer.write_u64(entry.used_timestamp_millis);
            write_data_envelope(&mut writer, &entry.envelope)?;
        }
        Ok(writer.finish())
    }

    fn decode(
        &self,
        payload: Bytes,
        version: u16,
    ) -> kairo_serialization::Result<ReplicatorGossip> {
        ensure_version::<ReplicatorGossip>(version)?;
        let mut reader = WireReader::new(&payload);
        let send_back = reader.read_bool()?;
        let to_system_uid = reader.read_optional_u64()?;
        let from_system_uid = reader.read_optional_u64()?;
        let entry_count = u64_to_len(reader.read_u64()?)?;
        let mut entries = Vec::with_capacity(entry_count);
        for _ in 0..entry_count {
            entries.push(ReplicatorGossipEntry {
                key: reader.read_string()?,
                used_timestamp_millis: reader.read_u64()?,
                envelope: read_data_envelope(&mut reader, DATA_ENVELOPE_PRUNING_WIRE_VERSION)?,
            });
        }
        let message = ReplicatorGossip {
            entries,
            send_back,
            to_system_uid,
            from_system_uid,
        };
        reader.ensure_finished()?;
        Ok(message)
    }
}
