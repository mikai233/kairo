use bytes::Bytes;
use kairo_serialization::{MessageCodec, SerializationError, WireReader, WireWriter};

use crate::{
    ClusterConfigCheck, Down, ExitingConfirmed, GossipStatus, InitJoin, InitJoinAck, InitJoinNack,
    Leave,
};

use super::wire::{
    ensure_version, read_address, read_unique_address, read_vector_clock, write_address,
    write_unique_address, write_vector_clock,
};

pub const INIT_JOIN_SERIALIZER_ID: u32 = 2_005;
pub const INIT_JOIN_ACK_SERIALIZER_ID: u32 = 2_006;
pub const INIT_JOIN_NACK_SERIALIZER_ID: u32 = 2_007;
pub const GOSSIP_STATUS_SERIALIZER_ID: u32 = 2_008;
pub const LEAVE_SERIALIZER_ID: u32 = 2_009;
pub const DOWN_SERIALIZER_ID: u32 = 2_010;
pub const EXITING_CONFIRMED_SERIALIZER_ID: u32 = 2_011;

#[derive(Debug, Clone, Copy)]
pub struct InitJoinCodec;

impl MessageCodec<InitJoin> for InitJoinCodec {
    fn serializer_id(&self) -> u32 {
        INIT_JOIN_SERIALIZER_ID
    }

    fn encode(&self, message: &InitJoin) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        writer.write_bytes(&message.joining_config_digest)?;
        Ok(writer.finish())
    }

    fn decode(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<InitJoin> {
        ensure_version::<InitJoin>(version)?;
        let mut reader = WireReader::new(&payload);
        let message = InitJoin {
            joining_config_digest: reader.read_bytes()?,
        };
        reader.ensure_finished()?;
        Ok(message)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct InitJoinAckCodec;

impl MessageCodec<InitJoinAck> for InitJoinAckCodec {
    fn serializer_id(&self) -> u32 {
        INIT_JOIN_ACK_SERIALIZER_ID
    }

    fn encode(&self, message: &InitJoinAck) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        write_address(&mut writer, &message.address)?;
        writer.write_u8(config_check_code(message.config_check));
        Ok(writer.finish())
    }

    fn decode(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<InitJoinAck> {
        ensure_version::<InitJoinAck>(version)?;
        let mut reader = WireReader::new(&payload);
        let message = InitJoinAck {
            address: read_address(&mut reader)?,
            config_check: config_check_from_code(reader.read_u8()?)?,
        };
        reader.ensure_finished()?;
        Ok(message)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct InitJoinNackCodec;

impl MessageCodec<InitJoinNack> for InitJoinNackCodec {
    fn serializer_id(&self) -> u32 {
        INIT_JOIN_NACK_SERIALIZER_ID
    }

    fn encode(&self, message: &InitJoinNack) -> kairo_serialization::Result<Bytes> {
        encode_address(&message.address)
    }

    fn decode(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<InitJoinNack> {
        ensure_version::<InitJoinNack>(version)?;
        let mut reader = WireReader::new(&payload);
        let message = InitJoinNack {
            address: read_address(&mut reader)?,
        };
        reader.ensure_finished()?;
        Ok(message)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct GossipStatusCodec;

impl MessageCodec<GossipStatus> for GossipStatusCodec {
    fn serializer_id(&self) -> u32 {
        GOSSIP_STATUS_SERIALIZER_ID
    }

    fn encode(&self, message: &GossipStatus) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        write_unique_address(&mut writer, &message.from)?;
        write_vector_clock(&mut writer, &message.version)?;
        writer.write_bytes(&message.seen_digest)?;
        Ok(writer.finish())
    }

    fn decode(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<GossipStatus> {
        ensure_version::<GossipStatus>(version)?;
        let mut reader = WireReader::new(&payload);
        let message = GossipStatus {
            from: read_unique_address(&mut reader)?,
            version: read_vector_clock(&mut reader)?,
            seen_digest: reader.read_bytes()?,
        };
        reader.ensure_finished()?;
        Ok(message)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct LeaveCodec;

impl MessageCodec<Leave> for LeaveCodec {
    fn serializer_id(&self) -> u32 {
        LEAVE_SERIALIZER_ID
    }

    fn encode(&self, message: &Leave) -> kairo_serialization::Result<Bytes> {
        encode_address(&message.address)
    }

    fn decode(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<Leave> {
        ensure_version::<Leave>(version)?;
        let mut reader = WireReader::new(&payload);
        let message = Leave {
            address: read_address(&mut reader)?,
        };
        reader.ensure_finished()?;
        Ok(message)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DownCodec;

impl MessageCodec<Down> for DownCodec {
    fn serializer_id(&self) -> u32 {
        DOWN_SERIALIZER_ID
    }

    fn encode(&self, message: &Down) -> kairo_serialization::Result<Bytes> {
        encode_address(&message.address)
    }

    fn decode(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<Down> {
        ensure_version::<Down>(version)?;
        let mut reader = WireReader::new(&payload);
        let message = Down {
            address: read_address(&mut reader)?,
        };
        reader.ensure_finished()?;
        Ok(message)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ExitingConfirmedCodec;

impl MessageCodec<ExitingConfirmed> for ExitingConfirmedCodec {
    fn serializer_id(&self) -> u32 {
        EXITING_CONFIRMED_SERIALIZER_ID
    }

    fn encode(&self, message: &ExitingConfirmed) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        write_unique_address(&mut writer, &message.node)?;
        Ok(writer.finish())
    }

    fn decode(
        &self,
        payload: Bytes,
        version: u16,
    ) -> kairo_serialization::Result<ExitingConfirmed> {
        ensure_version::<ExitingConfirmed>(version)?;
        let mut reader = WireReader::new(&payload);
        let message = ExitingConfirmed {
            node: read_unique_address(&mut reader)?,
        };
        reader.ensure_finished()?;
        Ok(message)
    }
}

fn encode_address(address: &kairo_actor::Address) -> kairo_serialization::Result<Bytes> {
    let mut writer = WireWriter::new();
    write_address(&mut writer, address)?;
    Ok(writer.finish())
}

fn config_check_code(check: ClusterConfigCheck) -> u8 {
    match check {
        ClusterConfigCheck::Unchecked => 0,
        ClusterConfigCheck::Compatible => 1,
        ClusterConfigCheck::Incompatible => 2,
    }
}

fn config_check_from_code(code: u8) -> kairo_serialization::Result<ClusterConfigCheck> {
    match code {
        0 => Ok(ClusterConfigCheck::Unchecked),
        1 => Ok(ClusterConfigCheck::Compatible),
        2 => Ok(ClusterConfigCheck::Incompatible),
        other => Err(SerializationError::Message(format!(
            "unknown cluster config-check code {other}"
        ))),
    }
}
