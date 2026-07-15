#![deny(missing_docs)]

//! Typed delta propagation encoding and decoding.
//!
//! Each encoded range carries stable CRDT codec metadata, inclusive source
//! versions, and the logical key's current removed-replica pruning table.
//! Protocol v2 adds pruning metadata; the message codec continues to decode v1
//! ranges as having an empty pruning table.

use kairo_serialization::SerializationError;

use crate::{
    CrdtDataCodec, DeltaPropagation, PruningTable, ReplicaId, ReplicatedData, ReplicatorDelta,
    ReplicatorDeltaPropagation, ReplicatorKey,
};

#[derive(Debug, Clone, PartialEq, Eq)]
/// One decoded CRDT delta range and its key-level pruning metadata.
pub struct DecodedReplicatorDelta<Delta> {
    key: ReplicatorKey,
    delta: Delta,
    pruning: PruningTable,
    from_version: u64,
    to_version: u64,
}

impl<Delta> DecodedReplicatorDelta<Delta> {
    /// Creates a decoded range with an empty pruning table.
    pub fn new(key: ReplicatorKey, delta: Delta, from_version: u64, to_version: u64) -> Self {
        Self {
            key,
            delta,
            pruning: PruningTable::new(),
            from_version,
            to_version,
        }
    }

    /// Replaces the decoded range's pruning metadata.
    pub fn with_pruning(mut self, pruning: PruningTable) -> Self {
        self.pruning = pruning;
        self
    }

    /// Returns the stable typed-namespace key.
    pub fn key(&self) -> &ReplicatorKey {
        &self.key
    }

    /// Borrows the decoded CRDT delta.
    pub fn delta(&self) -> &Delta {
        &self.delta
    }

    /// Consumes the range and returns its decoded CRDT delta.
    pub fn into_delta(self) -> Delta {
        self.delta
    }

    /// Returns pruning metadata captured for the logical key at publication.
    pub fn pruning(&self) -> &PruningTable {
        &self.pruning
    }

    /// Returns the first source version represented by this range.
    pub fn from_version(&self) -> u64 {
        self.from_version
    }

    /// Returns the last source version represented by this range.
    pub fn to_version(&self) -> u64 {
        self.to_version
    }
}

/// Encodes one selected per-replica delta propagation batch.
///
/// Entries retain the deterministic key order of [`DeltaPropagation`] and use
/// `codec` for payload and stable manifest/version metadata.
pub fn encode_delta_propagation<Delta, Codec>(
    from: ReplicaId,
    reply: bool,
    propagation: &DeltaPropagation<Delta>,
    codec: &Codec,
) -> kairo_serialization::Result<ReplicatorDeltaPropagation>
where
    Delta: ReplicatedData,
    Codec: CrdtDataCodec<Delta>,
{
    let mut deltas = Vec::with_capacity(propagation.entries().len());
    for (key, entry) in propagation.entries() {
        let encoded = codec.serialize(entry.delta())?;
        deltas.push(
            ReplicatorDelta::new(
                key.as_str(),
                encoded,
                entry.from_version(),
                entry.to_version(),
            )
            .with_pruning(crate::aggregation_wire::encode_pruning_table(
                entry.pruning(),
            )),
        );
    }
    Ok(ReplicatorDeltaPropagation {
        from,
        reply,
        deltas,
    })
}

/// Decodes every delta range in propagation order.
///
/// Decoding is all-or-error: an invalid manifest, payload, version, or pruning
/// table prevents a partial vector from being returned.
pub fn decode_delta_propagation<Delta, Codec>(
    propagation: &ReplicatorDeltaPropagation,
    codec: &Codec,
) -> kairo_serialization::Result<Vec<DecodedReplicatorDelta<Delta>>>
where
    Codec: CrdtDataCodec<Delta> + ?Sized,
{
    propagation
        .deltas
        .iter()
        .map(|delta| decode_delta(delta, codec))
        .collect()
}

/// Decodes one manifest-tagged CRDT delta range.
///
/// The supplied codec must own the range's exact CRDT manifest. Duplicate
/// removed-replica pruning entries are rejected.
pub fn decode_delta<Delta, Codec>(
    delta: &ReplicatorDelta,
    codec: &Codec,
) -> kairo_serialization::Result<DecodedReplicatorDelta<Delta>>
where
    Codec: CrdtDataCodec<Delta> + ?Sized,
{
    if delta.crdt_manifest != codec.manifest() {
        return Err(SerializationError::Message(format!(
            "expected CRDT manifest {}, got {}",
            codec.manifest(),
            delta.crdt_manifest
        )));
    }

    let pruning = crate::aggregation_wire::decode_pruning_table(&delta.pruning)?;
    Ok(DecodedReplicatorDelta::new(
        ReplicatorKey::new(delta.key.clone()),
        codec.decode_payload(delta.payload.clone(), delta.crdt_version)?,
        delta.from_version,
        delta.to_version,
    )
    .with_pruning(pruning))
}
