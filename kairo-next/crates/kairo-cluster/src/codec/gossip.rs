use bytes::Bytes;
use kairo_serialization::{MessageCodec, WireReader, WireWriter};

use crate::{GossipEnvelope, Welcome};

use super::wire::{
    ensure_version, read_gossip, read_unique_address, write_gossip, write_unique_address,
};

pub const WELCOME_SERIALIZER_ID: u32 = 2_003;
pub const GOSSIP_ENVELOPE_SERIALIZER_ID: u32 = 2_004;

#[derive(Debug, Clone, Copy)]
pub struct WelcomeCodec;

impl MessageCodec<Welcome> for WelcomeCodec {
    fn serializer_id(&self) -> u32 {
        WELCOME_SERIALIZER_ID
    }

    fn encode(&self, message: &Welcome) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        write_unique_address(&mut writer, &message.from)?;
        write_gossip(&mut writer, &message.gossip)?;
        Ok(writer.finish())
    }

    fn decode(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<Welcome> {
        ensure_version::<Welcome>(version)?;
        let mut reader = WireReader::new(&payload);
        Ok(Welcome {
            from: read_unique_address(&mut reader)?,
            gossip: read_gossip(&mut reader)?,
        })
    }
}

#[derive(Debug, Clone, Copy)]
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
        write_gossip(&mut writer, &message.gossip)?;
        Ok(writer.finish())
    }

    fn decode(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<GossipEnvelope> {
        ensure_version::<GossipEnvelope>(version)?;
        let mut reader = WireReader::new(&payload);
        Ok(GossipEnvelope {
            from: read_unique_address(&mut reader)?,
            to: read_unique_address(&mut reader)?,
            sequence_nr: reader.read_u64()?,
            gossip: read_gossip(&mut reader)?,
        })
    }
}
