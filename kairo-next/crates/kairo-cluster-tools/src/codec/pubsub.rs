use bytes::Bytes;
use kairo_serialization::{MessageCodec, WireReader, WireWriter};

use crate::{PubSubDelta, PubSubPathEnvelope, PubSubPublishEnvelope, PubSubStatus};

use super::wire::{
    ensure_version, read_delta, read_serialized_message, read_topic, read_unique_address,
    read_versions, write_delta, write_serialized_message, write_topic, write_unique_address,
    write_versions,
};

pub const PUBSUB_STATUS_SERIALIZER_ID: u32 = 5_000;
pub const PUBSUB_DELTA_SERIALIZER_ID: u32 = 5_001;
pub const PUBSUB_PUBLISH_SERIALIZER_ID: u32 = 5_002;
pub const PUBSUB_PATH_SERIALIZER_ID: u32 = 5_003;

#[derive(Debug, Clone, Copy)]
pub struct PubSubStatusCodec;

impl MessageCodec<PubSubStatus> for PubSubStatusCodec {
    fn serializer_id(&self) -> u32 {
        PUBSUB_STATUS_SERIALIZER_ID
    }

    fn encode(&self, message: &PubSubStatus) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        write_unique_address(&mut writer, &message.from)?;
        writer.write_bool(message.reply);
        write_versions(&mut writer, &message.versions)?;
        Ok(writer.finish())
    }

    fn decode(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<PubSubStatus> {
        ensure_version::<PubSubStatus>(version)?;
        let mut reader = WireReader::new(&payload);
        let status = PubSubStatus {
            from: read_unique_address(&mut reader)?,
            reply: reader.read_bool()?,
            versions: read_versions(&mut reader)?,
        };
        reader.ensure_finished()?;
        Ok(status)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PubSubDeltaCodec;

impl MessageCodec<PubSubDelta> for PubSubDeltaCodec {
    fn serializer_id(&self) -> u32 {
        PUBSUB_DELTA_SERIALIZER_ID
    }

    fn encode(&self, message: &PubSubDelta) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        write_unique_address(&mut writer, &message.from)?;
        write_delta(&mut writer, &message.delta)?;
        Ok(writer.finish())
    }

    fn decode(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<PubSubDelta> {
        ensure_version::<PubSubDelta>(version)?;
        let mut reader = WireReader::new(&payload);
        let delta = PubSubDelta {
            from: read_unique_address(&mut reader)?,
            delta: read_delta(&mut reader)?,
        };
        reader.ensure_finished()?;
        Ok(delta)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PubSubPublishEnvelopeCodec;

impl MessageCodec<PubSubPublishEnvelope> for PubSubPublishEnvelopeCodec {
    fn serializer_id(&self) -> u32 {
        PUBSUB_PUBLISH_SERIALIZER_ID
    }

    fn encode(&self, message: &PubSubPublishEnvelope) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        write_topic(&mut writer, &message.topic)?;
        writer.write_optional_string(message.group.as_deref())?;
        write_serialized_message(&mut writer, &message.message)?;
        Ok(writer.finish())
    }

    fn decode(
        &self,
        payload: Bytes,
        version: u16,
    ) -> kairo_serialization::Result<PubSubPublishEnvelope> {
        ensure_version::<PubSubPublishEnvelope>(version)?;
        let mut reader = WireReader::new(&payload);
        let envelope = PubSubPublishEnvelope {
            topic: read_topic(&mut reader)?,
            group: reader.read_optional_string()?,
            message: read_serialized_message(&mut reader)?,
        };
        reader.ensure_finished()?;
        Ok(envelope)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PubSubPathEnvelopeCodec;

impl MessageCodec<PubSubPathEnvelope> for PubSubPathEnvelopeCodec {
    fn serializer_id(&self) -> u32 {
        PUBSUB_PATH_SERIALIZER_ID
    }

    fn encode(&self, message: &PubSubPathEnvelope) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        writer.write_string(&message.path)?;
        writer.write_bool(message.all);
        write_serialized_message(&mut writer, &message.message)?;
        Ok(writer.finish())
    }

    fn decode(
        &self,
        payload: Bytes,
        version: u16,
    ) -> kairo_serialization::Result<PubSubPathEnvelope> {
        ensure_version::<PubSubPathEnvelope>(version)?;
        let mut reader = WireReader::new(&payload);
        let envelope = PubSubPathEnvelope {
            path: reader.read_string()?,
            all: reader.read_bool()?,
            message: read_serialized_message(&mut reader)?,
        };
        reader.ensure_finished()?;
        Ok(envelope)
    }
}
