use bytes::Bytes;
use kairo_serialization::{ActorRefWireData, MessageCodec, WireReader, WireWriter};

use super::{
    REPLICATOR_CHANGED_SERIALIZER_ID, REPLICATOR_GET_SERIALIZER_ID,
    REPLICATOR_SUBSCRIBE_SERIALIZER_ID, REPLICATOR_UPDATE_SERIALIZER_ID, helpers::ensure_version,
};
use crate::{ReplicatorChanged, ReplicatorGet, ReplicatorSubscribe, ReplicatorUpdate};

#[derive(Debug, Clone, Copy)]
/// Codec for correlated client get requests.
pub struct ReplicatorGetCodec;

impl MessageCodec<ReplicatorGet> for ReplicatorGetCodec {
    fn serializer_id(&self) -> u32 {
        REPLICATOR_GET_SERIALIZER_ID
    }

    fn encode(&self, message: &ReplicatorGet) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        writer.write_string(&message.key)?;
        writer.write_u64(message.request_id);
        Ok(writer.finish())
    }

    fn decode(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<ReplicatorGet> {
        ensure_version::<ReplicatorGet>(version)?;
        let mut reader = WireReader::new(&payload);
        let message = ReplicatorGet {
            key: reader.read_string()?,
            request_id: reader.read_u64()?,
        };
        reader.ensure_finished()?;
        Ok(message)
    }
}

#[derive(Debug, Clone, Copy)]
/// Codec for correlated client update requests.
pub struct ReplicatorUpdateCodec;

impl MessageCodec<ReplicatorUpdate> for ReplicatorUpdateCodec {
    fn serializer_id(&self) -> u32 {
        REPLICATOR_UPDATE_SERIALIZER_ID
    }

    fn encode(&self, message: &ReplicatorUpdate) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        writer.write_string(&message.key)?;
        writer.write_u64(message.request_id);
        Ok(writer.finish())
    }

    fn decode(
        &self,
        payload: Bytes,
        version: u16,
    ) -> kairo_serialization::Result<ReplicatorUpdate> {
        ensure_version::<ReplicatorUpdate>(version)?;
        let mut reader = WireReader::new(&payload);
        let message = ReplicatorUpdate {
            key: reader.read_string()?,
            request_id: reader.read_u64()?,
        };
        reader.ensure_finished()?;
        Ok(message)
    }
}

#[derive(Debug, Clone, Copy)]
/// Codec for key subscriptions carrying a stable actor-ref path.
pub struct ReplicatorSubscribeCodec;

impl MessageCodec<ReplicatorSubscribe> for ReplicatorSubscribeCodec {
    fn serializer_id(&self) -> u32 {
        REPLICATOR_SUBSCRIBE_SERIALIZER_ID
    }

    fn encode(&self, message: &ReplicatorSubscribe) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        writer.write_string(&message.key)?;
        writer.write_string(message.subscriber.path())?;
        Ok(writer.finish())
    }

    fn decode(
        &self,
        payload: Bytes,
        version: u16,
    ) -> kairo_serialization::Result<ReplicatorSubscribe> {
        ensure_version::<ReplicatorSubscribe>(version)?;
        let mut reader = WireReader::new(&payload);
        let message = ReplicatorSubscribe {
            key: reader.read_string()?,
            subscriber: ActorRefWireData::new(reader.read_string()?)?,
        };
        reader.ensure_finished()?;
        Ok(message)
    }
}

#[derive(Debug, Clone, Copy)]
/// Codec for coalesced key-change notifications.
pub struct ReplicatorChangedCodec;

impl MessageCodec<ReplicatorChanged> for ReplicatorChangedCodec {
    fn serializer_id(&self) -> u32 {
        REPLICATOR_CHANGED_SERIALIZER_ID
    }

    fn encode(&self, message: &ReplicatorChanged) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        writer.write_string(&message.key)?;
        Ok(writer.finish())
    }

    fn decode(
        &self,
        payload: Bytes,
        version: u16,
    ) -> kairo_serialization::Result<ReplicatorChanged> {
        ensure_version::<ReplicatorChanged>(version)?;
        let mut reader = WireReader::new(&payload);
        let message = ReplicatorChanged {
            key: reader.read_string()?,
        };
        reader.ensure_finished()?;
        Ok(message)
    }
}
