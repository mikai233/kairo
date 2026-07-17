#![deny(missing_docs)]

use bytes::Bytes;
use kairo_serialization::{MessageCodec, RemoteMessage, WireReader, WireWriter};

use crate::{GossipEnvelope, Welcome};

use super::wire::{read_gossip, read_unique_address, write_gossip, write_unique_address};

/// Stable serializer identifier for [`Welcome`] payloads.
pub const WELCOME_SERIALIZER_ID: u32 = 2_003;
/// Stable serializer identifier for [`GossipEnvelope`] payloads.
pub const GOSSIP_ENVELOPE_SERIALIZER_ID: u32 = 2_004;

#[derive(Debug, Clone, Copy)]
/// Binary codec for cluster [`Welcome`] replies and their initial gossip state.
pub struct WelcomeCodec;

impl MessageCodec<Welcome> for WelcomeCodec {
    fn serializer_id(&self) -> u32 {
        WELCOME_SERIALIZER_ID
    }

    fn encode(&self, message: &Welcome) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        write_unique_address(&mut writer, &message.from)?;
        write_gossip(&mut writer, &message.gossip, Welcome::VERSION)?;
        Ok(writer.finish())
    }

    fn decode(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<Welcome> {
        super::wire::ensure_supported_version::<Welcome>(version, 1)?;
        let mut reader = WireReader::new(&payload);
        let message = Welcome {
            from: read_unique_address(&mut reader)?,
            gossip: read_gossip(&mut reader, version)?,
        };
        reader.ensure_finished()?;
        Ok(message)
    }
}

#[derive(Debug, Clone, Copy)]
/// Binary codec for full-state cluster [`GossipEnvelope`] exchanges.
pub struct GossipEnvelopeCodec;

impl MessageCodec<GossipEnvelope> for GossipEnvelopeCodec {
    fn serializer_id(&self) -> u32 {
        GOSSIP_ENVELOPE_SERIALIZER_ID
    }

    fn encode(&self, message: &GossipEnvelope) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        write_unique_address(&mut writer, &message.from)?;
        write_unique_address(&mut writer, &message.to)?;
        writer.write_u64(message.sequence_nr);
        write_gossip(&mut writer, &message.gossip, GossipEnvelope::VERSION)?;
        Ok(writer.finish())
    }

    fn decode(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<GossipEnvelope> {
        super::wire::ensure_supported_version::<GossipEnvelope>(version, 1)?;
        let mut reader = WireReader::new(&payload);
        let message = GossipEnvelope {
            from: read_unique_address(&mut reader)?,
            to: read_unique_address(&mut reader)?,
            sequence_nr: reader.read_u64()?,
            gossip: read_gossip(&mut reader, version)?,
        };
        reader.ensure_finished()?;
        Ok(message)
    }
}
