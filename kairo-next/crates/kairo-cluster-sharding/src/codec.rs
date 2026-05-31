use bytes::Bytes;
use kairo_serialization::{
    ActorRefWireData, Manifest, MessageCodec, Registry, SerializationError, SerializationRegistry,
    SerializedMessage, WireReader, WireWriter,
};

use crate::{
    BeginHandOff, BeginHandOffAck, GetShardHome, GracefulShutdownReq, HandOff, HostShard,
    RegionStopped, Register, RegisterAck, RoutedShardEnvelope, ShardHome, ShardStarted,
    ShardStopped,
};

pub const REGISTER_SERIALIZER_ID: u32 = 4_000;
pub const REGISTER_ACK_SERIALIZER_ID: u32 = 4_001;
pub const GET_SHARD_HOME_SERIALIZER_ID: u32 = 4_002;
pub const SHARD_HOME_SERIALIZER_ID: u32 = 4_003;
pub const HOST_SHARD_SERIALIZER_ID: u32 = 4_004;
pub const SHARD_STARTED_SERIALIZER_ID: u32 = 4_005;
pub const BEGIN_HANDOFF_SERIALIZER_ID: u32 = 4_006;
pub const BEGIN_HANDOFF_ACK_SERIALIZER_ID: u32 = 4_007;
pub const HANDOFF_SERIALIZER_ID: u32 = 4_008;
pub const SHARD_STOPPED_SERIALIZER_ID: u32 = 4_009;
pub const ROUTED_SHARD_ENVELOPE_SERIALIZER_ID: u32 = 4_010;
pub const GRACEFUL_SHUTDOWN_REQ_SERIALIZER_ID: u32 = 4_011;
pub const REGION_STOPPED_SERIALIZER_ID: u32 = 4_012;

pub fn register_sharding_protocol_codecs(
    registry: &mut Registry,
) -> kairo_serialization::Result<()> {
    registry.register::<Register, _>(RegisterCodec)?;
    registry.register::<RegisterAck, _>(RegisterAckCodec)?;
    registry.register::<GetShardHome, _>(GetShardHomeCodec)?;
    registry.register::<ShardHome, _>(ShardHomeCodec)?;
    registry.register::<HostShard, _>(HostShardCodec)?;
    registry.register::<ShardStarted, _>(ShardStartedCodec)?;
    registry.register::<BeginHandOff, _>(BeginHandOffCodec)?;
    registry.register::<BeginHandOffAck, _>(BeginHandOffAckCodec)?;
    registry.register::<HandOff, _>(HandOffCodec)?;
    registry.register::<ShardStopped, _>(ShardStoppedCodec)?;
    registry.register::<RoutedShardEnvelope, _>(RoutedShardEnvelopeCodec)?;
    registry.register::<GracefulShutdownReq, _>(GracefulShutdownReqCodec)?;
    registry.register::<RegionStopped, _>(RegionStoppedCodec)?;
    Ok(())
}

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

#[derive(Debug, Clone, Copy)]
pub struct RoutedShardEnvelopeCodec;

impl MessageCodec<RoutedShardEnvelope> for RoutedShardEnvelopeCodec {
    fn serializer_id(&self) -> u32 {
        ROUTED_SHARD_ENVELOPE_SERIALIZER_ID
    }

    fn encode(&self, message: &RoutedShardEnvelope) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        writer.write_string(&message.shard_id)?;
        writer.write_string(&message.entity_id)?;
        write_serialized_message(&mut writer, &message.message)?;
        Ok(writer.finish())
    }

    fn decode(
        &self,
        payload: Bytes,
        version: u16,
    ) -> kairo_serialization::Result<RoutedShardEnvelope> {
        ensure_version::<RoutedShardEnvelope>(version)?;
        let mut reader = WireReader::new(&payload);
        Ok(RoutedShardEnvelope {
            shard_id: reader.read_string()?,
            entity_id: reader.read_string()?,
            message: read_serialized_message(&mut reader)?,
        })
    }
}

fn encode_actor_ref(ref_data: &ActorRefWireData) -> kairo_serialization::Result<Bytes> {
    let mut writer = WireWriter::new();
    writer.write_string(ref_data.path())?;
    Ok(writer.finish())
}

fn decode_actor_ref(payload: &Bytes) -> kairo_serialization::Result<ActorRefWireData> {
    let mut reader = WireReader::new(payload);
    ActorRefWireData::new(reader.read_string()?)
}

fn encode_shard_id(shard_id: &str) -> kairo_serialization::Result<Bytes> {
    let mut writer = WireWriter::new();
    writer.write_string(shard_id)?;
    Ok(writer.finish())
}

fn write_serialized_message(
    writer: &mut WireWriter,
    message: &SerializedMessage,
) -> kairo_serialization::Result<()> {
    writer.write_u32(message.serializer_id);
    writer.write_string(message.manifest.as_str())?;
    writer.write_u16(message.version);
    writer.write_bytes(&message.payload)
}

fn read_serialized_message(
    reader: &mut WireReader<'_>,
) -> kairo_serialization::Result<SerializedMessage> {
    Ok(SerializedMessage::new(
        reader.read_u32()?,
        Manifest::try_new(reader.read_string()?)?,
        reader.read_u16()?,
        reader.read_bytes()?,
    ))
}

fn decode_shard_id(payload: &Bytes) -> kairo_serialization::Result<String> {
    let mut reader = WireReader::new(payload);
    reader.read_string()
}

fn ensure_version<M>(version: u16) -> kairo_serialization::Result<()>
where
    M: kairo_serialization::RemoteMessage,
{
    if version == M::VERSION {
        Ok(())
    } else {
        Err(SerializationError::Message(format!(
            "unsupported {} version {version}",
            M::MANIFEST
        )))
    }
}

#[cfg(test)]
mod tests {
    use kairo_serialization::{RemoteMessage, SerializedMessage};

    use super::*;

    fn registry() -> Registry {
        let mut registry = Registry::new();
        register_sharding_protocol_codecs(&mut registry).unwrap();
        registry
    }

    #[test]
    fn sharding_protocol_codecs_round_trip_registration_messages() {
        let registry = registry();
        let register = Register {
            region: ActorRefWireData::new("kairo://sys@127.0.0.1:25520/user/region#1").unwrap(),
        };
        let ack = RegisterAck {
            coordinator: ActorRefWireData::new("kairo://sys@127.0.0.1:25520/system/sharding#2")
                .unwrap(),
        };

        let serialized_register = registry.serialize(&register).unwrap();
        let serialized_ack = registry.serialize(&ack).unwrap();

        assert_eq!(serialized_register.serializer_id, REGISTER_SERIALIZER_ID);
        assert_eq!(serialized_ack.serializer_id, REGISTER_ACK_SERIALIZER_ID);
        assert_eq!(
            registry
                .deserialize::<Register>(serialized_register)
                .unwrap(),
            register
        );
        assert_eq!(
            registry.deserialize::<RegisterAck>(serialized_ack).unwrap(),
            ack
        );
    }

    #[test]
    fn sharding_protocol_codecs_round_trip_shard_home_messages() {
        let registry = registry();
        let get = GetShardHome {
            shard_id: "12".to_string(),
        };
        let home = ShardHome {
            shard_id: "12".to_string(),
            region: ActorRefWireData::new("kairo://sys@127.0.0.1:25521/user/region#3").unwrap(),
        };

        let serialized_get = registry.serialize(&get).unwrap();
        let serialized_home = registry.serialize(&home).unwrap();

        assert_eq!(serialized_get.serializer_id, GET_SHARD_HOME_SERIALIZER_ID);
        assert_eq!(serialized_home.serializer_id, SHARD_HOME_SERIALIZER_ID);
        assert_eq!(
            registry
                .deserialize::<GetShardHome>(serialized_get)
                .unwrap(),
            get
        );
        assert_eq!(
            registry.deserialize::<ShardHome>(serialized_home).unwrap(),
            home
        );
    }

    #[test]
    fn sharding_protocol_codecs_round_trip_handoff_messages() {
        let registry = registry();
        let host = HostShard {
            shard_id: "42".to_string(),
        };
        let started = ShardStarted {
            shard_id: "42".to_string(),
        };
        let begin = BeginHandOff {
            shard_id: "42".to_string(),
        };
        let begin_ack = BeginHandOffAck {
            shard_id: "42".to_string(),
        };
        let handoff = HandOff {
            shard_id: "42".to_string(),
        };
        let stopped = ShardStopped {
            shard_id: "42".to_string(),
        };
        let shutdown = GracefulShutdownReq {
            region: ActorRefWireData::new("kairo://sys@127.0.0.1:25521/user/region#4").unwrap(),
        };
        let region_stopped = RegionStopped {
            region: ActorRefWireData::new("kairo://sys@127.0.0.1:25521/user/region#4").unwrap(),
        };
        let routed = RoutedShardEnvelope {
            shard_id: "42".to_string(),
            entity_id: "entity-1".to_string(),
            message: SerializedMessage::new(
                777,
                kairo_serialization::Manifest::new("kairo.test.message"),
                3,
                Bytes::from_static(b"payload"),
            ),
        };

        assert_eq!(
            registry
                .deserialize::<HostShard>(registry.serialize(&host).unwrap())
                .unwrap(),
            host
        );
        assert_eq!(
            registry
                .deserialize::<ShardStarted>(registry.serialize(&started).unwrap())
                .unwrap(),
            started
        );
        assert_eq!(
            registry
                .deserialize::<BeginHandOff>(registry.serialize(&begin).unwrap())
                .unwrap(),
            begin
        );
        assert_eq!(
            registry
                .deserialize::<BeginHandOffAck>(registry.serialize(&begin_ack).unwrap())
                .unwrap(),
            begin_ack
        );
        assert_eq!(
            registry
                .deserialize::<HandOff>(registry.serialize(&handoff).unwrap())
                .unwrap(),
            handoff
        );
        assert_eq!(
            registry
                .deserialize::<ShardStopped>(registry.serialize(&stopped).unwrap())
                .unwrap(),
            stopped
        );
        assert_eq!(
            registry
                .deserialize::<GracefulShutdownReq>(registry.serialize(&shutdown).unwrap())
                .unwrap(),
            shutdown
        );
        assert_eq!(
            registry
                .deserialize::<RegionStopped>(registry.serialize(&region_stopped).unwrap())
                .unwrap(),
            region_stopped
        );
        let serialized_routed = registry.serialize(&routed).unwrap();
        assert_eq!(
            serialized_routed.serializer_id,
            ROUTED_SHARD_ENVELOPE_SERIALIZER_ID
        );
        assert_eq!(
            registry
                .deserialize::<RoutedShardEnvelope>(serialized_routed)
                .unwrap(),
            routed
        );
    }

    #[test]
    fn sharding_protocol_codecs_reject_unknown_versions() {
        let registry = registry();
        let wire = SerializedMessage::new(
            GET_SHARD_HOME_SERIALIZER_ID,
            Manifest::new(GetShardHome::MANIFEST),
            GetShardHome::VERSION + 1,
            Bytes::from_static(&[0, 0, 0, 2, b'4', b'2']),
        );

        let error = registry
            .deserialize::<GetShardHome>(wire)
            .expect_err("unknown version should fail");

        assert!(error.to_string().contains("unsupported"));
    }
}
