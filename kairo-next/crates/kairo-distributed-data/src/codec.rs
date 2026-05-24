use bytes::Bytes;
use kairo_serialization::{
    ActorRefWireData, MessageCodec, Registry, SerializationError, SerializationRegistry,
    WireReader, WireWriter,
};

use crate::{ReplicatorChanged, ReplicatorGet, ReplicatorSubscribe, ReplicatorUpdate};

pub const REPLICATOR_GET_SERIALIZER_ID: u32 = 3_000;
pub const REPLICATOR_UPDATE_SERIALIZER_ID: u32 = 3_001;
pub const REPLICATOR_SUBSCRIBE_SERIALIZER_ID: u32 = 3_002;
pub const REPLICATOR_CHANGED_SERIALIZER_ID: u32 = 3_003;

pub fn register_ddata_protocol_codecs(registry: &mut Registry) -> kairo_serialization::Result<()> {
    registry.register::<ReplicatorGet, _>(ReplicatorGetCodec)?;
    registry.register::<ReplicatorUpdate, _>(ReplicatorUpdateCodec)?;
    registry.register::<ReplicatorSubscribe, _>(ReplicatorSubscribeCodec)?;
    registry.register::<ReplicatorChanged, _>(ReplicatorChangedCodec)?;
    Ok(())
}

#[derive(Debug, Clone, Copy)]
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
        Ok(ReplicatorGet {
            key: reader.read_string()?,
            request_id: reader.read_u64()?,
        })
    }
}

#[derive(Debug, Clone, Copy)]
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
        Ok(ReplicatorUpdate {
            key: reader.read_string()?,
            request_id: reader.read_u64()?,
        })
    }
}

#[derive(Debug, Clone, Copy)]
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
        Ok(ReplicatorSubscribe {
            key: reader.read_string()?,
            subscriber: ActorRefWireData::new(reader.read_string()?)?,
        })
    }
}

#[derive(Debug, Clone, Copy)]
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
        Ok(ReplicatorChanged {
            key: reader.read_string()?,
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
        register_ddata_protocol_codecs(&mut registry).unwrap();
        registry
    }

    #[test]
    fn ddata_protocol_codecs_round_trip_get_and_update() {
        let registry = registry();
        let get = ReplicatorGet {
            key: "counter-a".to_string(),
            request_id: 17,
        };
        let update = ReplicatorUpdate {
            key: "counter-a".to_string(),
            request_id: 18,
        };

        let serialized_get = registry.serialize(&get).unwrap();
        let serialized_update = registry.serialize(&update).unwrap();

        assert_eq!(serialized_get.serializer_id, REPLICATOR_GET_SERIALIZER_ID);
        assert_eq!(serialized_get.manifest.as_str(), ReplicatorGet::MANIFEST);
        assert_eq!(
            serialized_update.serializer_id,
            REPLICATOR_UPDATE_SERIALIZER_ID
        );
        assert_eq!(
            registry
                .deserialize::<ReplicatorGet>(serialized_get)
                .unwrap(),
            get
        );
        assert_eq!(
            registry
                .deserialize::<ReplicatorUpdate>(serialized_update)
                .unwrap(),
            update
        );
    }

    #[test]
    fn ddata_protocol_codecs_round_trip_subscribe_and_changed() {
        let registry = registry();
        let subscribe = ReplicatorSubscribe {
            key: "state/*".to_string(),
            subscriber: ActorRefWireData::new("kairo://sys@127.0.0.1:25520/user/sub#1").unwrap(),
        };
        let changed = ReplicatorChanged {
            key: "state/a".to_string(),
        };

        let serialized_subscribe = registry.serialize(&subscribe).unwrap();
        let serialized_changed = registry.serialize(&changed).unwrap();

        assert_eq!(
            serialized_subscribe.serializer_id,
            REPLICATOR_SUBSCRIBE_SERIALIZER_ID
        );
        assert_eq!(
            serialized_changed.serializer_id,
            REPLICATOR_CHANGED_SERIALIZER_ID
        );
        assert_eq!(
            registry
                .deserialize::<ReplicatorSubscribe>(serialized_subscribe)
                .unwrap(),
            subscribe
        );
        assert_eq!(
            registry
                .deserialize::<ReplicatorChanged>(serialized_changed)
                .unwrap(),
            changed
        );
    }

    #[test]
    fn ddata_protocol_codecs_reject_unknown_versions() {
        let registry = registry();
        let wire = SerializedMessage::new(
            REPLICATOR_GET_SERIALIZER_ID,
            Manifest::new(ReplicatorGet::MANIFEST),
            ReplicatorGet::VERSION + 1,
            Bytes::from_static(&[0, 0, 0, 1, b'a', 0, 0, 0, 0, 0, 0, 0, 1]),
        );

        let error = registry
            .deserialize::<ReplicatorGet>(wire)
            .expect_err("unknown version should fail");

        assert!(error.to_string().contains("unsupported"));
    }
}
