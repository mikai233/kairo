use bytes::Bytes;
use kairo_serialization::MessageCodec;

use crate::{BeginHandOff, BeginHandOffAck, HandOff, HostShard, ShardStarted, ShardStopped};

use super::wire::{decode_shard_id, encode_shard_id, ensure_version};

pub const HOST_SHARD_SERIALIZER_ID: u32 = 4_004;
pub const SHARD_STARTED_SERIALIZER_ID: u32 = 4_005;
pub const BEGIN_HANDOFF_SERIALIZER_ID: u32 = 4_006;
pub const BEGIN_HANDOFF_ACK_SERIALIZER_ID: u32 = 4_007;
pub const HANDOFF_SERIALIZER_ID: u32 = 4_008;
pub const SHARD_STOPPED_SERIALIZER_ID: u32 = 4_009;

#[derive(Debug, Clone, Copy)]
pub struct HostShardCodec;

impl MessageCodec<HostShard> for HostShardCodec {
    fn serializer_id(&self) -> u32 {
        HOST_SHARD_SERIALIZER_ID
    }

    fn encode(&self, message: &HostShard) -> kairo_serialization::Result<Bytes> {
        encode_shard_id(&message.shard_id)
    }

    fn decode(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<HostShard> {
        ensure_version::<HostShard>(version)?;
        Ok(HostShard {
            shard_id: decode_shard_id(&payload)?,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ShardStartedCodec;

impl MessageCodec<ShardStarted> for ShardStartedCodec {
    fn serializer_id(&self) -> u32 {
        SHARD_STARTED_SERIALIZER_ID
    }

    fn encode(&self, message: &ShardStarted) -> kairo_serialization::Result<Bytes> {
        encode_shard_id(&message.shard_id)
    }

    fn decode(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<ShardStarted> {
        ensure_version::<ShardStarted>(version)?;
        Ok(ShardStarted {
            shard_id: decode_shard_id(&payload)?,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct BeginHandOffCodec;

impl MessageCodec<BeginHandOff> for BeginHandOffCodec {
    fn serializer_id(&self) -> u32 {
        BEGIN_HANDOFF_SERIALIZER_ID
    }

    fn encode(&self, message: &BeginHandOff) -> kairo_serialization::Result<Bytes> {
        encode_shard_id(&message.shard_id)
    }

    fn decode(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<BeginHandOff> {
        ensure_version::<BeginHandOff>(version)?;
        Ok(BeginHandOff {
            shard_id: decode_shard_id(&payload)?,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct BeginHandOffAckCodec;

impl MessageCodec<BeginHandOffAck> for BeginHandOffAckCodec {
    fn serializer_id(&self) -> u32 {
        BEGIN_HANDOFF_ACK_SERIALIZER_ID
    }

    fn encode(&self, message: &BeginHandOffAck) -> kairo_serialization::Result<Bytes> {
        encode_shard_id(&message.shard_id)
    }

    fn decode(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<BeginHandOffAck> {
        ensure_version::<BeginHandOffAck>(version)?;
        Ok(BeginHandOffAck {
            shard_id: decode_shard_id(&payload)?,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct HandOffCodec;

impl MessageCodec<HandOff> for HandOffCodec {
    fn serializer_id(&self) -> u32 {
        HANDOFF_SERIALIZER_ID
    }

    fn encode(&self, message: &HandOff) -> kairo_serialization::Result<Bytes> {
        encode_shard_id(&message.shard_id)
    }

    fn decode(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<HandOff> {
        ensure_version::<HandOff>(version)?;
        Ok(HandOff {
            shard_id: decode_shard_id(&payload)?,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ShardStoppedCodec;

impl MessageCodec<ShardStopped> for ShardStoppedCodec {
    fn serializer_id(&self) -> u32 {
        SHARD_STOPPED_SERIALIZER_ID
    }

    fn encode(&self, message: &ShardStopped) -> kairo_serialization::Result<Bytes> {
        encode_shard_id(&message.shard_id)
    }

    fn decode(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<ShardStopped> {
        ensure_version::<ShardStopped>(version)?;
        Ok(ShardStopped {
            shard_id: decode_shard_id(&payload)?,
        })
    }
}
