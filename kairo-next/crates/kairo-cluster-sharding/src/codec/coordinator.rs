use bytes::Bytes;
use kairo_serialization::{ActorRefWireData, MessageCodec, WireReader, WireWriter};

use crate::{GetShardHome, Register, RegisterAck, ShardHome};

use super::wire::{
    decode_actor_ref, decode_shard_id, encode_actor_ref, encode_shard_id, ensure_version,
};

pub const REGISTER_SERIALIZER_ID: u32 = 4_000;
pub const REGISTER_ACK_SERIALIZER_ID: u32 = 4_001;
pub const GET_SHARD_HOME_SERIALIZER_ID: u32 = 4_002;
pub const SHARD_HOME_SERIALIZER_ID: u32 = 4_003;

#[derive(Debug, Clone, Copy)]
pub struct RegisterCodec;

impl MessageCodec<Register> for RegisterCodec {
    fn serializer_id(&self) -> u32 {
        REGISTER_SERIALIZER_ID
    }

    fn encode(&self, message: &Register) -> kairo_serialization::Result<Bytes> {
        encode_actor_ref(&message.region)
    }

    fn decode(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<Register> {
        ensure_version::<Register>(version)?;
        Ok(Register {
            region: decode_actor_ref(&payload)?,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RegisterAckCodec;

impl MessageCodec<RegisterAck> for RegisterAckCodec {
    fn serializer_id(&self) -> u32 {
        REGISTER_ACK_SERIALIZER_ID
    }

    fn encode(&self, message: &RegisterAck) -> kairo_serialization::Result<Bytes> {
        encode_actor_ref(&message.coordinator)
    }

    fn decode(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<RegisterAck> {
        ensure_version::<RegisterAck>(version)?;
        Ok(RegisterAck {
            coordinator: decode_actor_ref(&payload)?,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct GetShardHomeCodec;

impl MessageCodec<GetShardHome> for GetShardHomeCodec {
    fn serializer_id(&self) -> u32 {
        GET_SHARD_HOME_SERIALIZER_ID
    }

    fn encode(&self, message: &GetShardHome) -> kairo_serialization::Result<Bytes> {
        encode_shard_id(&message.shard_id)
    }

    fn decode(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<GetShardHome> {
        ensure_version::<GetShardHome>(version)?;
        Ok(GetShardHome {
            shard_id: decode_shard_id(&payload)?,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ShardHomeCodec;

impl MessageCodec<ShardHome> for ShardHomeCodec {
    fn serializer_id(&self) -> u32 {
        SHARD_HOME_SERIALIZER_ID
    }

    fn encode(&self, message: &ShardHome) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        writer.write_string(&message.shard_id)?;
        writer.write_string(message.region.path())?;
        Ok(writer.finish())
    }

    fn decode(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<ShardHome> {
        ensure_version::<ShardHome>(version)?;
        let mut reader = WireReader::new(&payload);
        Ok(ShardHome {
            shard_id: reader.read_string()?,
            region: ActorRefWireData::new(reader.read_string()?)?,
        })
    }
}
