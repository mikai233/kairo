#![deny(missing_docs)]
//! Stable conversions between typed aggregation values and replicator wire records.

use kairo_serialization::SerializationError;

use crate::{
    CrdtDataCodec, DataEnvelope, PruningState, PruningTable, ReplicaId, ReplicatedData,
    ReplicatorDataEnvelope, ReplicatorKey, ReplicatorPruningEntry, ReplicatorPruningState,
    ReplicatorRead, ReplicatorReadResult, ReplicatorWrite,
};

/// Encodes CRDT data and removed-node pruning metadata into a wire envelope.
///
/// The CRDT codec supplies the stable manifest, version, and payload. Pruning
/// entries are emitted in the deterministic replica order of [`PruningTable`].
pub fn encode_data_envelope<D, Codec>(
    envelope: &DataEnvelope<D>,
    codec: &Codec,
) -> kairo_serialization::Result<ReplicatorDataEnvelope>
where
    D: ReplicatedData,
    Codec: CrdtDataCodec<D> + ?Sized,
{
    Ok(
        ReplicatorDataEnvelope::new(codec.serialize(envelope.data())?)
            .with_pruning(encode_pruning_table(envelope.pruning())),
    )
}

/// Decodes a wire envelope after validating its CRDT manifest and pruning table.
///
/// Duplicate removed-replica pruning entries are rejected instead of being
/// silently overwritten.
pub fn decode_data_envelope<D, Codec>(
    envelope: &ReplicatorDataEnvelope,
    codec: &Codec,
) -> kairo_serialization::Result<DataEnvelope<D>>
where
    D: ReplicatedData,
    Codec: CrdtDataCodec<D> + ?Sized,
{
    if envelope.crdt_manifest != codec.manifest() {
        return Err(SerializationError::Message(format!(
            "expected CRDT manifest {}, got {}",
            codec.manifest(),
            envelope.crdt_manifest
        )));
    }

    let pruning = decode_pruning_table(&envelope.pruning)?;
    Ok(DataEnvelope::with_pruning(
        codec.decode_payload(envelope.payload.clone(), envelope.crdt_version)?,
        pruning,
    ))
}

/// Encodes a direct full-state write for `key` and its optional source replica.
pub fn encode_write<D, Codec>(
    key: &ReplicatorKey,
    from: Option<ReplicaId>,
    envelope: &DataEnvelope<D>,
    codec: &Codec,
) -> kairo_serialization::Result<ReplicatorWrite>
where
    D: ReplicatedData,
    Codec: CrdtDataCodec<D> + ?Sized,
{
    Ok(ReplicatorWrite {
        key: key.as_str().to_string(),
        from,
        envelope: encode_data_envelope(envelope, codec)?,
    })
}

/// Encodes a direct full-state read for `key` and its optional source replica.
pub fn encode_read(key: &ReplicatorKey, from: Option<ReplicaId>) -> ReplicatorRead {
    ReplicatorRead {
        key: key.as_str().to_string(),
        from,
    }
}

/// Encodes an optional full-state value returned by a direct read.
///
/// `None` represents a successful read of a key that is absent on the replica.
pub fn encode_read_result<D, Codec>(
    envelope: Option<&DataEnvelope<D>>,
    codec: &Codec,
) -> kairo_serialization::Result<ReplicatorReadResult>
where
    D: ReplicatedData,
    Codec: CrdtDataCodec<D> + ?Sized,
{
    Ok(ReplicatorReadResult {
        envelope: envelope
            .map(|envelope| encode_data_envelope(envelope, codec))
            .transpose()?,
    })
}

/// Decodes an optional direct-read value and validates its CRDT wire metadata.
///
/// `Ok(None)` preserves successful key absence independently from decode
/// failure.
pub fn decode_read_result<D, Codec>(
    result: &ReplicatorReadResult,
    codec: &Codec,
) -> kairo_serialization::Result<Option<DataEnvelope<D>>>
where
    D: ReplicatedData,
    Codec: CrdtDataCodec<D> + ?Sized,
{
    result
        .envelope
        .as_ref()
        .map(|envelope| decode_data_envelope(envelope, codec))
        .transpose()
}

pub(crate) fn encode_pruning_table(pruning: &PruningTable) -> Vec<ReplicatorPruningEntry> {
    pruning
        .states()
        .iter()
        .map(|(removed, state)| ReplicatorPruningEntry {
            removed: removed.clone(),
            state: match state {
                PruningState::Initialized(initialized) => ReplicatorPruningState::Initialized {
                    owner: initialized.owner().clone(),
                    seen: initialized.seen().iter().cloned().collect(),
                },
                PruningState::Performed(performed) => ReplicatorPruningState::Performed {
                    obsolete_at_millis: performed.obsolete_at_millis(),
                },
            },
        })
        .collect()
}

pub(crate) fn decode_pruning_table(
    entries: &[ReplicatorPruningEntry],
) -> kairo_serialization::Result<PruningTable> {
    let mut pruning = PruningTable::new();
    for entry in entries {
        if pruning.get(&entry.removed).is_some() {
            return Err(SerializationError::Message(format!(
                "duplicate pruning entry for removed replica {}",
                entry.removed.as_str()
            )));
        }

        match &entry.state {
            ReplicatorPruningState::Initialized { owner, seen } => {
                pruning.initialize(entry.removed.clone(), owner.clone());
                for seen_by in seen {
                    pruning.mark_seen(&entry.removed, seen_by.clone());
                }
            }
            ReplicatorPruningState::Performed { obsolete_at_millis } => {
                pruning.mark_performed(entry.removed.clone(), *obsolete_at_millis);
            }
        }
    }
    Ok(pruning)
}
