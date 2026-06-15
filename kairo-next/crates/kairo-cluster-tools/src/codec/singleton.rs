use bytes::Bytes;
use kairo_cluster::UniqueAddress;
use kairo_serialization::{MessageCodec, WireReader, WireWriter};

use crate::{
    SingletonHandOverDone, SingletonHandOverInProgress, SingletonHandOverToMe,
    SingletonTakeOverFromMe,
};

use super::wire::{ensure_version, read_unique_address, write_unique_address};

pub const SINGLETON_HAND_OVER_TO_ME_SERIALIZER_ID: u32 = 5_010;
pub const SINGLETON_HAND_OVER_IN_PROGRESS_SERIALIZER_ID: u32 = 5_011;
pub const SINGLETON_HAND_OVER_DONE_SERIALIZER_ID: u32 = 5_012;
pub const SINGLETON_TAKE_OVER_FROM_ME_SERIALIZER_ID: u32 = 5_013;

#[derive(Debug, Clone, Copy)]
pub struct SingletonHandOverToMeCodec;

impl MessageCodec<SingletonHandOverToMe> for SingletonHandOverToMeCodec {
    fn serializer_id(&self) -> u32 {
        SINGLETON_HAND_OVER_TO_ME_SERIALIZER_ID
    }

    fn encode(&self, message: &SingletonHandOverToMe) -> kairo_serialization::Result<Bytes> {
        encode_singleton_handover(&message.from)
    }

    fn decode(
        &self,
        payload: Bytes,
        version: u16,
    ) -> kairo_serialization::Result<SingletonHandOverToMe> {
        ensure_version::<SingletonHandOverToMe>(version)?;
        Ok(SingletonHandOverToMe {
            from: decode_singleton_handover(payload)?,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SingletonHandOverInProgressCodec;

impl MessageCodec<SingletonHandOverInProgress> for SingletonHandOverInProgressCodec {
    fn serializer_id(&self) -> u32 {
        SINGLETON_HAND_OVER_IN_PROGRESS_SERIALIZER_ID
    }

    fn encode(&self, message: &SingletonHandOverInProgress) -> kairo_serialization::Result<Bytes> {
        encode_singleton_handover(&message.from)
    }

    fn decode(
        &self,
        payload: Bytes,
        version: u16,
    ) -> kairo_serialization::Result<SingletonHandOverInProgress> {
        ensure_version::<SingletonHandOverInProgress>(version)?;
        Ok(SingletonHandOverInProgress {
            from: decode_singleton_handover(payload)?,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SingletonHandOverDoneCodec;

impl MessageCodec<SingletonHandOverDone> for SingletonHandOverDoneCodec {
    fn serializer_id(&self) -> u32 {
        SINGLETON_HAND_OVER_DONE_SERIALIZER_ID
    }

    fn encode(&self, message: &SingletonHandOverDone) -> kairo_serialization::Result<Bytes> {
        encode_singleton_handover(&message.from)
    }

    fn decode(
        &self,
        payload: Bytes,
        version: u16,
    ) -> kairo_serialization::Result<SingletonHandOverDone> {
        ensure_version::<SingletonHandOverDone>(version)?;
        Ok(SingletonHandOverDone {
            from: decode_singleton_handover(payload)?,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SingletonTakeOverFromMeCodec;

impl MessageCodec<SingletonTakeOverFromMe> for SingletonTakeOverFromMeCodec {
    fn serializer_id(&self) -> u32 {
        SINGLETON_TAKE_OVER_FROM_ME_SERIALIZER_ID
    }

    fn encode(&self, message: &SingletonTakeOverFromMe) -> kairo_serialization::Result<Bytes> {
        encode_singleton_handover(&message.from)
    }

    fn decode(
        &self,
        payload: Bytes,
        version: u16,
    ) -> kairo_serialization::Result<SingletonTakeOverFromMe> {
        ensure_version::<SingletonTakeOverFromMe>(version)?;
        Ok(SingletonTakeOverFromMe {
            from: decode_singleton_handover(payload)?,
        })
    }
}

fn encode_singleton_handover(from: &UniqueAddress) -> kairo_serialization::Result<Bytes> {
    let mut writer = WireWriter::new();
    write_unique_address(&mut writer, from)?;
    Ok(writer.finish())
}

fn decode_singleton_handover(payload: Bytes) -> kairo_serialization::Result<UniqueAddress> {
    let mut reader = WireReader::new(&payload);
    let from = read_unique_address(&mut reader)?;
    reader.ensure_finished()?;
    Ok(from)
}
