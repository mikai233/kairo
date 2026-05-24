use bytes::Bytes;
use kairo_serialization::{
    ActorRefWireData, MessageCodec, Registry, SerializationError, SerializationRegistry,
    WireReader, WireWriter,
};

use crate::{AddressTerminated, RemoteHeartbeat, RemoteHeartbeatAck, UnwatchRemote, WatchRemote};

pub const WATCH_REMOTE_SERIALIZER_ID: u32 = 1_000;
pub const UNWATCH_REMOTE_SERIALIZER_ID: u32 = 1_001;
pub const REMOTE_HEARTBEAT_SERIALIZER_ID: u32 = 1_002;
pub const REMOTE_HEARTBEAT_ACK_SERIALIZER_ID: u32 = 1_003;
pub const ADDRESS_TERMINATED_SERIALIZER_ID: u32 = 1_004;

pub fn register_remote_protocol_codecs(registry: &mut Registry) -> kairo_serialization::Result<()> {
    registry.register::<WatchRemote, _>(WatchRemoteCodec)?;
    registry.register::<UnwatchRemote, _>(UnwatchRemoteCodec)?;
    registry.register::<RemoteHeartbeat, _>(RemoteHeartbeatCodec)?;
    registry.register::<RemoteHeartbeatAck, _>(RemoteHeartbeatAckCodec)?;
    registry.register::<AddressTerminated, _>(AddressTerminatedCodec)?;
    Ok(())
}

#[derive(Debug, Clone, Copy)]
pub struct WatchRemoteCodec;

impl MessageCodec<WatchRemote> for WatchRemoteCodec {
    fn serializer_id(&self) -> u32 {
        WATCH_REMOTE_SERIALIZER_ID
    }

    fn encode(&self, message: &WatchRemote) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        writer.write_string(message.watchee.path())?;
        writer.write_string(message.watcher.path())?;
        Ok(writer.finish())
    }

    fn decode(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<WatchRemote> {
        ensure_version::<WatchRemote>(version)?;
        let mut reader = WireReader::new(&payload);
        Ok(WatchRemote {
            watchee: ActorRefWireData::new(reader.read_string()?)?,
            watcher: ActorRefWireData::new(reader.read_string()?)?,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct UnwatchRemoteCodec;

impl MessageCodec<UnwatchRemote> for UnwatchRemoteCodec {
    fn serializer_id(&self) -> u32 {
        UNWATCH_REMOTE_SERIALIZER_ID
    }

    fn encode(&self, message: &UnwatchRemote) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        writer.write_string(message.watchee.path())?;
        writer.write_string(message.watcher.path())?;
        Ok(writer.finish())
    }

    fn decode(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<UnwatchRemote> {
        ensure_version::<UnwatchRemote>(version)?;
        let mut reader = WireReader::new(&payload);
        Ok(UnwatchRemote {
            watchee: ActorRefWireData::new(reader.read_string()?)?,
            watcher: ActorRefWireData::new(reader.read_string()?)?,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RemoteHeartbeatCodec;

impl MessageCodec<RemoteHeartbeat> for RemoteHeartbeatCodec {
    fn serializer_id(&self) -> u32 {
        REMOTE_HEARTBEAT_SERIALIZER_ID
    }

    fn encode(&self, message: &RemoteHeartbeat) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        writer.write_u64(message.from_uid);
        Ok(writer.finish())
    }

    fn decode(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<RemoteHeartbeat> {
        ensure_version::<RemoteHeartbeat>(version)?;
        let mut reader = WireReader::new(&payload);
        Ok(RemoteHeartbeat {
            from_uid: reader.read_u64()?,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RemoteHeartbeatAckCodec;

impl MessageCodec<RemoteHeartbeatAck> for RemoteHeartbeatAckCodec {
    fn serializer_id(&self) -> u32 {
        REMOTE_HEARTBEAT_ACK_SERIALIZER_ID
    }

    fn encode(&self, message: &RemoteHeartbeatAck) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        writer.write_u64(message.uid);
        Ok(writer.finish())
    }

    fn decode(
        &self,
        payload: Bytes,
        version: u16,
    ) -> kairo_serialization::Result<RemoteHeartbeatAck> {
        ensure_version::<RemoteHeartbeatAck>(version)?;
        let mut reader = WireReader::new(&payload);
        Ok(RemoteHeartbeatAck {
            uid: reader.read_u64()?,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct AddressTerminatedCodec;

impl MessageCodec<AddressTerminated> for AddressTerminatedCodec {
    fn serializer_id(&self) -> u32 {
        ADDRESS_TERMINATED_SERIALIZER_ID
    }

    fn encode(&self, message: &AddressTerminated) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        writer.write_string(&message.address)?;
        writer.write_optional_u64(message.uid);
        Ok(writer.finish())
    }

    fn decode(
        &self,
        payload: Bytes,
        version: u16,
    ) -> kairo_serialization::Result<AddressTerminated> {
        ensure_version::<AddressTerminated>(version)?;
        let mut reader = WireReader::new(&payload);
        Ok(AddressTerminated {
            address: reader.read_string()?,
            uid: reader.read_optional_u64()?,
        })
    }
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
        register_remote_protocol_codecs(&mut registry).unwrap();
        registry
    }

    #[test]
    fn remote_protocol_codecs_round_trip_watch_messages() {
        let registry = registry();
        let message = WatchRemote {
            watchee: ActorRefWireData::new("kairo://sys@127.0.0.1:25520/user/a#1").unwrap(),
            watcher: ActorRefWireData::new("kairo://sys@127.0.0.1:25521/user/b#2").unwrap(),
        };

        let serialized = registry.serialize(&message).unwrap();

        assert_eq!(serialized.serializer_id, WATCH_REMOTE_SERIALIZER_ID);
        assert_eq!(serialized.manifest.as_str(), WatchRemote::MANIFEST);
        assert_eq!(serialized.version, WatchRemote::VERSION);
        assert_ne!(serialized.payload, Bytes::new());
        assert_eq!(
            registry.deserialize::<WatchRemote>(serialized).unwrap(),
            message
        );
    }

    #[test]
    fn remote_protocol_codecs_round_trip_heartbeat_messages() {
        let registry = registry();
        let heartbeat = registry
            .serialize(&RemoteHeartbeat { from_uid: 42 })
            .unwrap();
        let ack = registry.serialize(&RemoteHeartbeatAck { uid: 42 }).unwrap();

        assert_eq!(heartbeat.serializer_id, REMOTE_HEARTBEAT_SERIALIZER_ID);
        assert_eq!(ack.serializer_id, REMOTE_HEARTBEAT_ACK_SERIALIZER_ID);
        assert_eq!(
            registry.deserialize::<RemoteHeartbeat>(heartbeat).unwrap(),
            RemoteHeartbeat { from_uid: 42 }
        );
        assert_eq!(
            registry.deserialize::<RemoteHeartbeatAck>(ack).unwrap(),
            RemoteHeartbeatAck { uid: 42 }
        );
    }

    #[test]
    fn remote_protocol_codecs_round_trip_address_terminated() {
        let registry = registry();
        let message = AddressTerminated {
            address: "kairo://sys@127.0.0.1:25520".to_string(),
            uid: Some(99),
        };

        let serialized = registry.serialize(&message).unwrap();

        assert_eq!(serialized.serializer_id, ADDRESS_TERMINATED_SERIALIZER_ID);
        assert_eq!(
            registry
                .deserialize::<AddressTerminated>(serialized)
                .unwrap(),
            message
        );
    }

    #[test]
    fn remote_protocol_codecs_reject_unknown_versions() {
        let registry = registry();
        let wire = SerializedMessage::new(
            REMOTE_HEARTBEAT_SERIALIZER_ID,
            Manifest::new(RemoteHeartbeat::MANIFEST),
            RemoteHeartbeat::VERSION + 1,
            Bytes::from_static(&[0, 0, 0, 0, 0, 0, 0, 1]),
        );

        let error = registry
            .deserialize::<RemoteHeartbeat>(wire)
            .expect_err("unknown version should fail");

        assert!(error.to_string().contains("unsupported"));
    }
}
