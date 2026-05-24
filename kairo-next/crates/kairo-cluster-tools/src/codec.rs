use bytes::Bytes;
use kairo_actor::Address;
use kairo_cluster::UniqueAddress;
use kairo_serialization::{
    MessageCodec, Registry, RemoteMessage, SerializationError, SerializationRegistry, WireReader,
    WireWriter,
};

use crate::{
    PubSubBucket, PubSubDelta, PubSubRegistryDelta, PubSubRegistryEntry, PubSubRegistryKey,
    PubSubStatus, TopicName,
};

pub const PUBSUB_STATUS_SERIALIZER_ID: u32 = 5_000;
pub const PUBSUB_DELTA_SERIALIZER_ID: u32 = 5_001;

pub fn register_cluster_tools_protocol_codecs(
    registry: &mut Registry,
) -> kairo_serialization::Result<()> {
    registry.register::<PubSubStatus, _>(PubSubStatusCodec)?;
    registry.register::<PubSubDelta, _>(PubSubDeltaCodec)?;
    Ok(())
}

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
        Ok(PubSubStatus {
            from: read_unique_address(&mut reader)?,
            reply: reader.read_bool()?,
            versions: read_versions(&mut reader)?,
        })
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
        Ok(PubSubDelta {
            from: read_unique_address(&mut reader)?,
            delta: read_delta(&mut reader)?,
        })
    }
}

fn write_delta(
    writer: &mut WireWriter,
    delta: &PubSubRegistryDelta,
) -> kairo_serialization::Result<()> {
    writer.write_u64(len_to_u64(delta.buckets.len())?);
    for bucket in &delta.buckets {
        write_bucket(writer, bucket)?;
    }
    Ok(())
}

fn read_delta(reader: &mut WireReader<'_>) -> kairo_serialization::Result<PubSubRegistryDelta> {
    let len = u64_to_len(reader.read_u64()?)?;
    let mut buckets = Vec::with_capacity(len);
    for _ in 0..len {
        buckets.push(read_bucket(reader)?);
    }
    Ok(PubSubRegistryDelta { buckets })
}

fn write_bucket(writer: &mut WireWriter, bucket: &PubSubBucket) -> kairo_serialization::Result<()> {
    write_unique_address(writer, &bucket.owner)?;
    writer.write_u64(bucket.version);
    writer.write_u64(len_to_u64(bucket.entries.len())?);
    for entry in bucket.entries.values() {
        write_entry(writer, entry)?;
    }
    Ok(())
}

fn read_bucket(reader: &mut WireReader<'_>) -> kairo_serialization::Result<PubSubBucket> {
    let owner = read_unique_address(reader)?;
    let version = reader.read_u64()?;
    let len = u64_to_len(reader.read_u64()?)?;
    let mut entries = std::collections::BTreeMap::new();
    for _ in 0..len {
        let entry = read_entry(reader)?;
        entries.insert(entry.key.clone(), entry);
    }
    Ok(PubSubBucket {
        owner,
        version,
        entries,
    })
}

fn write_entry(
    writer: &mut WireWriter,
    entry: &PubSubRegistryEntry,
) -> kairo_serialization::Result<()> {
    writer.write_u64(entry.version);
    write_key(writer, &entry.key)?;
    writer.write_bool(entry.present);
    Ok(())
}

fn read_entry(reader: &mut WireReader<'_>) -> kairo_serialization::Result<PubSubRegistryEntry> {
    let version = reader.read_u64()?;
    let key = read_key(reader)?;
    let present = reader.read_bool()?;
    Ok(PubSubRegistryEntry {
        version,
        key,
        present,
    })
}

fn write_key(writer: &mut WireWriter, key: &PubSubRegistryKey) -> kairo_serialization::Result<()> {
    match key {
        PubSubRegistryKey::Topic { topic } => {
            writer.write_u8(0);
            writer.write_string(topic.as_str())?;
        }
        PubSubRegistryKey::Group { topic, group } => {
            writer.write_u8(1);
            writer.write_string(topic.as_str())?;
            writer.write_string(group)?;
        }
    }
    Ok(())
}

fn read_key(reader: &mut WireReader<'_>) -> kairo_serialization::Result<PubSubRegistryKey> {
    match reader.read_u8()? {
        0 => Ok(PubSubRegistryKey::topic(TopicName::new(
            reader.read_string()?,
        ))),
        1 => Ok(PubSubRegistryKey::group(
            TopicName::new(reader.read_string()?),
            reader.read_string()?,
        )),
        other => Err(SerializationError::Message(format!(
            "unknown pubsub registry key tag {other}"
        ))),
    }
}

fn write_versions(
    writer: &mut WireWriter,
    versions: &std::collections::BTreeMap<String, u64>,
) -> kairo_serialization::Result<()> {
    writer.write_u64(len_to_u64(versions.len())?);
    for (owner, version) in versions {
        writer.write_string(owner)?;
        writer.write_u64(*version);
    }
    Ok(())
}

fn read_versions(
    reader: &mut WireReader<'_>,
) -> kairo_serialization::Result<std::collections::BTreeMap<String, u64>> {
    let len = u64_to_len(reader.read_u64()?)?;
    let mut versions = std::collections::BTreeMap::new();
    for _ in 0..len {
        versions.insert(reader.read_string()?, reader.read_u64()?);
    }
    Ok(versions)
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
    let port = match reader.read_optional_u64()? {
        Some(port) => Some(u16::try_from(port).map_err(|_| {
            SerializationError::Message(format!("address port {port} exceeds u16"))
        })?),
        None => None,
    };
    let uid = reader.read_u64()?;
    Ok(UniqueAddress::new(
        Address::new(protocol, system, host, port),
        uid,
    ))
}

fn ensure_version<M>(version: u16) -> kairo_serialization::Result<()>
where
    M: RemoteMessage,
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

fn len_to_u64(len: usize) -> kairo_serialization::Result<u64> {
    u64::try_from(len).map_err(|_| SerializationError::Message("length exceeds u64".to_string()))
}

fn u64_to_len(len: u64) -> kairo_serialization::Result<usize> {
    usize::try_from(len).map_err(|_| {
        SerializationError::Message(format!("wire length {len} exceeds platform usize"))
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use kairo_serialization::{Manifest, SerializedMessage};

    use super::*;
    use crate::PubSubRegistryState;

    fn registry() -> Registry {
        let mut registry = Registry::new();
        register_cluster_tools_protocol_codecs(&mut registry).unwrap();
        registry
    }

    fn unique(system: &str, uid: u64) -> UniqueAddress {
        UniqueAddress::new(
            Address::new("kairo", system, Some("127.0.0.1".to_string()), Some(25520)),
            uid,
        )
    }

    #[test]
    fn cluster_tools_codecs_round_trip_pubsub_status() {
        let registry = registry();
        let node = unique("a", 1);
        let status = PubSubStatus {
            from: node.clone(),
            versions: BTreeMap::from([(node.ordering_key(), 7)]),
            reply: true,
        };

        let serialized = registry.serialize(&status).unwrap();

        assert_eq!(serialized.serializer_id, PUBSUB_STATUS_SERIALIZER_ID);
        assert_eq!(serialized.manifest.as_str(), PubSubStatus::MANIFEST);
        assert_eq!(
            registry.deserialize::<PubSubStatus>(serialized).unwrap(),
            status
        );
    }

    #[test]
    fn cluster_tools_codecs_round_trip_pubsub_delta() {
        let registry = registry();
        let node = unique("a", 1);
        let mut state = PubSubRegistryState::new(node.clone());
        state.register_local_topic(TopicName::new("orders"));
        state.register_local_group(TopicName::new("jobs"), "workers");
        let delta = PubSubDelta {
            from: node,
            delta: state.collect_delta(&BTreeMap::new(), 10),
        };

        let serialized = registry.serialize(&delta).unwrap();

        assert_eq!(serialized.serializer_id, PUBSUB_DELTA_SERIALIZER_ID);
        assert_eq!(serialized.manifest.as_str(), PubSubDelta::MANIFEST);
        assert_eq!(
            registry.deserialize::<PubSubDelta>(serialized).unwrap(),
            delta
        );
    }

    #[test]
    fn cluster_tools_codecs_reject_unknown_versions() {
        let registry = registry();
        let status = PubSubStatus {
            from: unique("a", 1),
            versions: BTreeMap::new(),
            reply: false,
        };
        let wire = SerializedMessage::new(
            PUBSUB_STATUS_SERIALIZER_ID,
            Manifest::new(PubSubStatus::MANIFEST),
            PubSubStatus::VERSION + 1,
            registry.serialize(&status).unwrap().payload,
        );

        let error = registry
            .deserialize::<PubSubStatus>(wire)
            .expect_err("unknown version should fail");

        assert!(error.to_string().contains("unsupported"));
    }
}
