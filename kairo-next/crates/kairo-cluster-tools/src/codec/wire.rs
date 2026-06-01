use kairo_actor::Address;
use kairo_cluster::UniqueAddress;
use kairo_serialization::{
    Manifest, RemoteMessage, SerializationError, SerializedMessage, WireReader, WireWriter,
};

use crate::{PubSubBucket, PubSubRegistryDelta, PubSubRegistryEntry, PubSubRegistryKey, TopicName};

pub(super) fn write_delta(
    writer: &mut WireWriter,
    delta: &PubSubRegistryDelta,
) -> kairo_serialization::Result<()> {
    writer.write_u64(len_to_u64(delta.buckets.len())?);
    for bucket in &delta.buckets {
        write_bucket(writer, bucket)?;
    }
    Ok(())
}

pub(super) fn read_delta(
    reader: &mut WireReader<'_>,
) -> kairo_serialization::Result<PubSubRegistryDelta> {
    let len = u64_to_len(reader.read_u64()?)?;
    let mut buckets = Vec::with_capacity(len);
    for _ in 0..len {
        buckets.push(read_bucket(reader)?);
    }
    Ok(PubSubRegistryDelta { buckets })
}

pub(super) fn write_serialized_message(
    writer: &mut WireWriter,
    message: &SerializedMessage,
) -> kairo_serialization::Result<()> {
    writer.write_u32(message.serializer_id);
    writer.write_string(message.manifest.as_str())?;
    writer.write_u16(message.version);
    writer.write_bytes(&message.payload)
}

pub(super) fn read_serialized_message(
    reader: &mut WireReader<'_>,
) -> kairo_serialization::Result<SerializedMessage> {
    Ok(SerializedMessage::new(
        reader.read_u32()?,
        Manifest::try_new(reader.read_string()?)?,
        reader.read_u16()?,
        reader.read_bytes()?,
    ))
}

pub(super) fn write_topic(
    writer: &mut WireWriter,
    topic: &TopicName,
) -> kairo_serialization::Result<()> {
    writer.write_string(topic.as_str())
}

pub(super) fn read_topic(reader: &mut WireReader<'_>) -> kairo_serialization::Result<TopicName> {
    Ok(TopicName::new(reader.read_string()?))
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

pub(super) fn write_versions(
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

pub(super) fn read_versions(
    reader: &mut WireReader<'_>,
) -> kairo_serialization::Result<std::collections::BTreeMap<String, u64>> {
    let len = u64_to_len(reader.read_u64()?)?;
    let mut versions = std::collections::BTreeMap::new();
    for _ in 0..len {
        versions.insert(reader.read_string()?, reader.read_u64()?);
    }
    Ok(versions)
}

pub(super) fn write_unique_address(
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

pub(super) fn read_unique_address(
    reader: &mut WireReader<'_>,
) -> kairo_serialization::Result<UniqueAddress> {
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

pub(super) fn ensure_version<M>(version: u16) -> kairo_serialization::Result<()>
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
