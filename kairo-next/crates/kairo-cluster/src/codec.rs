use bytes::Bytes;
use kairo_actor::Address;
use kairo_serialization::{
    MessageCodec, Registry, SerializationError, SerializationRegistry, WireReader, WireWriter,
};

use crate::{Heartbeat, HeartbeatRsp, Join, UniqueAddress};

pub const HEARTBEAT_SERIALIZER_ID: u32 = 2_000;
pub const HEARTBEAT_RSP_SERIALIZER_ID: u32 = 2_001;
pub const JOIN_SERIALIZER_ID: u32 = 2_002;

pub fn register_cluster_control_codecs(registry: &mut Registry) -> kairo_serialization::Result<()> {
    registry.register::<Heartbeat, _>(HeartbeatCodec)?;
    registry.register::<HeartbeatRsp, _>(HeartbeatRspCodec)?;
    registry.register::<Join, _>(JoinCodec)?;
    Ok(())
}

#[derive(Debug, Clone, Copy)]
pub struct HeartbeatCodec;

impl MessageCodec<Heartbeat> for HeartbeatCodec {
    fn serializer_id(&self) -> u32 {
        HEARTBEAT_SERIALIZER_ID
    }

    fn encode(&self, message: &Heartbeat) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        write_unique_address(&mut writer, &message.from)?;
        writer.write_u64(message.sequence_nr);
        writer.write_u64(message.creation_time_nanos);
        Ok(writer.finish())
    }

    fn decode(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<Heartbeat> {
        ensure_version::<Heartbeat>(version)?;
        let mut reader = WireReader::new(&payload);
        Ok(Heartbeat {
            from: read_unique_address(&mut reader)?,
            sequence_nr: reader.read_u64()?,
            creation_time_nanos: reader.read_u64()?,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct HeartbeatRspCodec;

impl MessageCodec<HeartbeatRsp> for HeartbeatRspCodec {
    fn serializer_id(&self) -> u32 {
        HEARTBEAT_RSP_SERIALIZER_ID
    }

    fn encode(&self, message: &HeartbeatRsp) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        write_unique_address(&mut writer, &message.from)?;
        writer.write_u64(message.sequence_nr);
        writer.write_u64(message.creation_time_nanos);
        Ok(writer.finish())
    }

    fn decode(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<HeartbeatRsp> {
        ensure_version::<HeartbeatRsp>(version)?;
        let mut reader = WireReader::new(&payload);
        Ok(HeartbeatRsp {
            from: read_unique_address(&mut reader)?,
            sequence_nr: reader.read_u64()?,
            creation_time_nanos: reader.read_u64()?,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct JoinCodec;

impl MessageCodec<Join> for JoinCodec {
    fn serializer_id(&self) -> u32 {
        JOIN_SERIALIZER_ID
    }

    fn encode(&self, message: &Join) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        write_unique_address(&mut writer, &message.node)?;
        let role_count = u64::try_from(message.roles.len())
            .map_err(|_| SerializationError::Message("too many cluster roles".to_string()))?;
        writer.write_u64(role_count);
        for role in &message.roles {
            writer.write_string(role)?;
        }
        Ok(writer.finish())
    }

    fn decode(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<Join> {
        ensure_version::<Join>(version)?;
        let mut reader = WireReader::new(&payload);
        let node = read_unique_address(&mut reader)?;
        let role_count = usize::try_from(reader.read_u64()?).map_err(|_| {
            SerializationError::Message("cluster role count is too large".to_string())
        })?;
        let mut roles = Vec::with_capacity(role_count);
        for _ in 0..role_count {
            roles.push(reader.read_string()?);
        }
        Ok(Join { node, roles })
    }
}

fn write_unique_address(
    writer: &mut WireWriter,
    unique_address: &UniqueAddress,
) -> kairo_serialization::Result<()> {
    writer.write_string(unique_address.address.protocol())?;
    writer.write_string(unique_address.address.system())?;
    writer.write_optional_string(unique_address.address.host())?;
    writer.write_optional_u64(unique_address.address.port().map(u64::from));
    writer.write_u64(unique_address.uid);
    Ok(())
}

fn read_unique_address(reader: &mut WireReader<'_>) -> kairo_serialization::Result<UniqueAddress> {
    let protocol = reader.read_string()?;
    let system = reader.read_string()?;
    let host = reader.read_optional_string()?;
    let port = reader
        .read_optional_u64()?
        .map(u16::try_from)
        .transpose()
        .map_err(|_| SerializationError::Message("cluster address port exceeds u16".to_string()))?;
    let uid = reader.read_u64()?;
    Ok(UniqueAddress::new(
        Address::new(protocol, system, host, port),
        uid,
    ))
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
    use kairo_serialization::{Manifest, RemoteMessage, SerializedMessage};

    use super::*;

    fn registry() -> Registry {
        let mut registry = Registry::new();
        register_cluster_control_codecs(&mut registry).unwrap();
        registry
    }

    fn unique(uid: u64) -> UniqueAddress {
        UniqueAddress::new(
            Address::new("kairo", "sys", Some("127.0.0.1".to_string()), Some(25520)),
            uid,
        )
    }

    #[test]
    fn cluster_control_codecs_round_trip_heartbeat_messages() {
        let registry = registry();
        let heartbeat = Heartbeat {
            from: unique(7),
            sequence_nr: 42,
            creation_time_nanos: 1234,
        };
        let response = HeartbeatRsp {
            from: unique(8),
            sequence_nr: 42,
            creation_time_nanos: 1234,
        };

        let serialized_heartbeat = registry.serialize(&heartbeat).unwrap();
        let serialized_response = registry.serialize(&response).unwrap();

        assert_eq!(serialized_heartbeat.serializer_id, HEARTBEAT_SERIALIZER_ID);
        assert_eq!(
            serialized_response.serializer_id,
            HEARTBEAT_RSP_SERIALIZER_ID
        );
        assert_eq!(
            registry
                .deserialize::<Heartbeat>(serialized_heartbeat)
                .unwrap(),
            heartbeat
        );
        assert_eq!(
            registry
                .deserialize::<HeartbeatRsp>(serialized_response)
                .unwrap(),
            response
        );
    }

    #[test]
    fn cluster_control_codecs_round_trip_join() {
        let registry = registry();
        let join = Join {
            node: unique(9),
            roles: vec!["backend".to_string(), "blue".to_string()],
        };

        let serialized = registry.serialize(&join).unwrap();

        assert_eq!(serialized.serializer_id, JOIN_SERIALIZER_ID);
        assert_eq!(serialized.manifest.as_str(), Join::MANIFEST);
        assert_eq!(registry.deserialize::<Join>(serialized).unwrap(), join);
    }

    #[test]
    fn cluster_control_codecs_reject_unknown_versions() {
        let registry = registry();
        let wire = SerializedMessage::new(
            JOIN_SERIALIZER_ID,
            Manifest::new(Join::MANIFEST),
            Join::VERSION + 1,
            registry
                .serialize(&Join {
                    node: unique(1),
                    roles: vec![],
                })
                .unwrap()
                .payload,
        );

        let error = registry
            .deserialize::<Join>(wire)
            .expect_err("unknown version should fail");

        assert!(error.to_string().contains("unsupported"));
    }
}
