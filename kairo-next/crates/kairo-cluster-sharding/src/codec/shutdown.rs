use bytes::Bytes;
use kairo_serialization::MessageCodec;

use crate::{GracefulShutdownReq, RegionStopped};

use super::wire::{decode_actor_ref, encode_actor_ref, ensure_version};

pub const GRACEFUL_SHUTDOWN_REQ_SERIALIZER_ID: u32 = 4_011;
pub const REGION_STOPPED_SERIALIZER_ID: u32 = 4_012;

#[derive(Debug, Clone, Copy)]
pub struct GracefulShutdownReqCodec;

impl MessageCodec<GracefulShutdownReq> for GracefulShutdownReqCodec {
    fn serializer_id(&self) -> u32 {
        GRACEFUL_SHUTDOWN_REQ_SERIALIZER_ID
    }

    fn encode(&self, message: &GracefulShutdownReq) -> kairo_serialization::Result<Bytes> {
        encode_actor_ref(&message.region)
    }

    fn decode(
        &self,
        payload: Bytes,
        version: u16,
    ) -> kairo_serialization::Result<GracefulShutdownReq> {
        ensure_version::<GracefulShutdownReq>(version)?;
        Ok(GracefulShutdownReq {
            region: decode_actor_ref(&payload)?,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RegionStoppedCodec;

impl MessageCodec<RegionStopped> for RegionStoppedCodec {
    fn serializer_id(&self) -> u32 {
        REGION_STOPPED_SERIALIZER_ID
    }

    fn encode(&self, message: &RegionStopped) -> kairo_serialization::Result<Bytes> {
        encode_actor_ref(&message.region)
    }

    fn decode(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<RegionStopped> {
        ensure_version::<RegionStopped>(version)?;
        Ok(RegionStopped {
            region: decode_actor_ref(&payload)?,
        })
    }
}
