use bytes::Bytes;
use kairo_serialization::{Manifest, RemoteMessage, SerializationError, WireReader, WireWriter};

use crate::{
    ReplicaId, ReplicatorDataEnvelope, ReplicatorDelta, ReplicatorPruningEntry,
    ReplicatorPruningState,
};

pub(super) fn ensure_version<M>(version: u16) -> kairo_serialization::Result<()>
where
    M: RemoteMessage,
{
    ensure_version_range(M::MANIFEST, version, M::VERSION, M::VERSION)
}

pub(super) fn ensure_version_range(
    manifest: &str,
    version: u16,
    min_version: u16,
    max_version: u16,
) -> kairo_serialization::Result<()> {
    if (min_version..=max_version).contains(&version) {
        Ok(())
    } else {
        Err(SerializationError::Message(format!(
            "unsupported {manifest} version {version}"
        )))
    }
}

pub(super) fn write_delta(
    writer: &mut WireWriter,
    delta: &ReplicatorDelta,
) -> kairo_serialization::Result<()> {
    writer.write_string(&delta.key)?;
    validate_manifest_string(&delta.crdt_manifest)?;
    writer.write_string(&delta.crdt_manifest)?;
    writer.write_u16(delta.crdt_version);
    writer.write_u64(delta.from_version);
    writer.write_u64(delta.to_version);
    writer.write_bytes(&delta.payload)?;
    writer.write_u64(len_to_u64(delta.pruning.len())?);
    for entry in &delta.pruning {
        write_pruning_entry(writer, entry)?;
    }
    Ok(())
}

pub(super) fn read_delta(
    reader: &mut WireReader<'_>,
    version: u16,
) -> kairo_serialization::Result<ReplicatorDelta> {
    let key = reader.read_string()?;
    let crdt_manifest = read_manifest_string(reader)?;
    let crdt_version = reader.read_u16()?;
    let from_version = reader.read_u64()?;
    let to_version = reader.read_u64()?;
    let payload = reader.read_bytes()?;
    let pruning = if version >= 2 {
        let count = u64_to_len(reader.read_u64()?)?;
        (0..count)
            .map(|_| read_pruning_entry(reader))
            .collect::<kairo_serialization::Result<Vec<_>>>()?
    } else {
        Vec::new()
    };
    Ok(ReplicatorDelta {
        key,
        crdt_manifest,
        crdt_version,
        from_version,
        to_version,
        payload,
        pruning,
    })
}

pub(super) fn write_data_envelope(
    writer: &mut WireWriter,
    envelope: &ReplicatorDataEnvelope,
) -> kairo_serialization::Result<()> {
    validate_manifest_string(&envelope.crdt_manifest)?;
    writer.write_string(&envelope.crdt_manifest)?;
    writer.write_u16(envelope.crdt_version);
    writer.write_bytes(&envelope.payload)?;
    writer.write_u64(len_to_u64(envelope.pruning.len())?);
    for entry in &envelope.pruning {
        write_pruning_entry(writer, entry)?;
    }
    Ok(())
}

pub(super) fn read_data_envelope(
    reader: &mut WireReader<'_>,
    version: u16,
) -> kairo_serialization::Result<ReplicatorDataEnvelope> {
    let crdt_manifest = read_manifest_string(reader)?;
    let crdt_version = reader.read_u16()?;
    let payload = reader.read_bytes()?;
    let pruning = if version >= 2 {
        let count = u64_to_len(reader.read_u64()?)?;
        (0..count)
            .map(|_| read_pruning_entry(reader))
            .collect::<kairo_serialization::Result<Vec<_>>>()?
    } else {
        Vec::new()
    };

    Ok(ReplicatorDataEnvelope {
        crdt_manifest,
        crdt_version,
        payload,
        pruning,
    })
}

fn validate_manifest_string(manifest: &str) -> kairo_serialization::Result<()> {
    Manifest::try_new(manifest.to_string()).map(|_| ())
}

fn read_manifest_string(reader: &mut WireReader<'_>) -> kairo_serialization::Result<String> {
    let manifest = reader.read_string()?;
    validate_manifest_string(&manifest)?;
    Ok(manifest)
}

fn write_pruning_entry(
    writer: &mut WireWriter,
    entry: &ReplicatorPruningEntry,
) -> kairo_serialization::Result<()> {
    writer.write_string(entry.removed.as_str())?;
    match &entry.state {
        ReplicatorPruningState::Initialized { owner, seen } => {
            writer.write_u8(1);
            writer.write_string(owner.as_str())?;
            writer.write_u64(len_to_u64(seen.len())?);
            for seen_by in seen {
                writer.write_string(seen_by.as_str())?;
            }
        }
        ReplicatorPruningState::Performed { obsolete_at_millis } => {
            writer.write_u8(2);
            writer.write_u64(*obsolete_at_millis);
        }
    }
    Ok(())
}

fn read_pruning_entry(
    reader: &mut WireReader<'_>,
) -> kairo_serialization::Result<ReplicatorPruningEntry> {
    let removed = ReplicaId::new(reader.read_string()?);
    let state = match reader.read_u8()? {
        1 => {
            let owner = ReplicaId::new(reader.read_string()?);
            let seen_count = u64_to_len(reader.read_u64()?)?;
            let mut seen = Vec::with_capacity(seen_count);
            for _ in 0..seen_count {
                seen.push(ReplicaId::new(reader.read_string()?));
            }
            ReplicatorPruningState::Initialized { owner, seen }
        }
        2 => ReplicatorPruningState::Performed {
            obsolete_at_millis: reader.read_u64()?,
        },
        other => {
            return Err(SerializationError::Message(format!(
                "unknown ddata pruning state tag {other}"
            )));
        }
    };
    Ok(ReplicatorPruningEntry { removed, state })
}

pub(super) fn ensure_empty_payload(
    payload: &Bytes,
    manifest: &str,
) -> kairo_serialization::Result<()> {
    if payload.is_empty() {
        Ok(())
    } else {
        Err(SerializationError::Message(format!(
            "{manifest} payload must be empty"
        )))
    }
}

pub(super) fn len_to_u64(len: usize) -> kairo_serialization::Result<u64> {
    u64::try_from(len)
        .map_err(|_| SerializationError::Message("replicator delta count exceeds u64".to_string()))
}

pub(super) fn u64_to_len(len: u64) -> kairo_serialization::Result<usize> {
    usize::try_from(len).map_err(|_| {
        SerializationError::Message("replicator delta count exceeds usize".to_string())
    })
}
