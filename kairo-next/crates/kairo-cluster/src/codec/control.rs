#![deny(missing_docs)]

use bytes::Bytes;
use kairo_serialization::{MessageCodec, SerializationError, WireReader, WireWriter};

use crate::{Heartbeat, HeartbeatRsp, Join};

use super::wire::{
    ensure_version, read_unique_address, read_vec, write_count, write_unique_address,
};

/// Stable serializer identifier for [`Heartbeat`] payloads.
pub const HEARTBEAT_SERIALIZER_ID: u32 = 2_000;
/// Stable serializer identifier for [`HeartbeatRsp`] payloads.
pub const HEARTBEAT_RSP_SERIALIZER_ID: u32 = 2_001;
/// Stable serializer identifier for [`Join`] payloads.
pub const JOIN_SERIALIZER_ID: u32 = 2_002;

#[derive(Debug, Clone, Copy)]
/// Binary codec for cluster [`Heartbeat`] probes.
pub struct HeartbeatCodec;

impl MessageCodec<Heartbeat> for HeartbeatCodec {
    fn serializer_id(&self) -> u32 {
        HEARTBEAT_SERIALIZER_ID
    }

    fn encode(&self, message: &Heartbeat) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        write_unique_address(&mut writer, &message.from)?;
        writer.write_u64(message.sequence_nr);
        writer.write_u64(message.creation_time_nanos);
        Ok(writer.finish())
    }

    fn decode(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<Heartbeat> {
        ensure_version::<Heartbeat>(version)?;
        let mut reader = WireReader::new(&payload);
        let message = Heartbeat {
            from: read_unique_address(&mut reader)?,
            sequence_nr: reader.read_u64()?,
            creation_time_nanos: reader.read_u64()?,
        };
        reader.ensure_finished()?;
        Ok(message)
    }
}

#[derive(Debug, Clone, Copy)]
/// Binary codec for cluster [`HeartbeatRsp`] replies.
pub struct HeartbeatRspCodec;

impl MessageCodec<HeartbeatRsp> for HeartbeatRspCodec {
    fn serializer_id(&self) -> u32 {
        HEARTBEAT_RSP_SERIALIZER_ID
    }

    fn encode(&self, message: &HeartbeatRsp) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        write_unique_address(&mut writer, &message.from)?;
        writer.write_u64(message.sequence_nr);
        writer.write_u64(message.creation_time_nanos);
        Ok(writer.finish())
    }

    fn decode(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<HeartbeatRsp> {
        ensure_version::<HeartbeatRsp>(version)?;
        let mut reader = WireReader::new(&payload);
        let message = HeartbeatRsp {
            from: read_unique_address(&mut reader)?,
            sequence_nr: reader.read_u64()?,
            creation_time_nanos: reader.read_u64()?,
        };
        reader.ensure_finished()?;
        Ok(message)
    }
}

#[derive(Debug, Clone, Copy)]
/// Binary codec for cluster membership [`Join`] requests.
pub struct JoinCodec;

impl MessageCodec<Join> for JoinCodec {
    fn serializer_id(&self) -> u32 {
        JOIN_SERIALIZER_ID
    }

    fn encode(&self, message: &Join) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        write_unique_address(&mut writer, &message.node)?;
        write_count(&mut writer, message.roles.len())
            .map_err(|_| SerializationError::Message("too many cluster roles".to_string()))?;
        for role in &message.roles {
            writer.write_string(role)?;
        }
        Ok(writer.finish())
    }

    fn decode(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<Join> {
        ensure_version::<Join>(version)?;
        let mut reader = WireReader::new(&payload);
        let message = Join {
            node: read_unique_address(&mut reader)?,
            roles: read_vec(&mut reader, |reader| reader.read_string())?,
        };
        reader.ensure_finished()?;
        Ok(message)
    }
}
