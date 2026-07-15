#![deny(missing_docs)]

//! Core state-, delta-, and removed-replica CRDT contracts.
//!
//! Implementations are immutable values: operations return new state and
//! [`ReplicatedData::merge`] must be associative, commutative, and idempotent.
//! Remote serialization is a separate explicit codec concern.

use std::collections::BTreeSet;

use crate::{CrdtError, ReplicaId};

/// State-based convergent replicated data value.
pub trait ReplicatedData: Clone + Eq {
    /// Monotonically merges another replica state into this value.
    fn merge(&self, other: &Self) -> Self;
}

/// Replicated data that can disseminate accumulated operation deltas.
pub trait DeltaReplicatedData: ReplicatedData {
    /// Delta representation paired with this full-state type.
    type Delta: ReplicatedDelta<Full = Self>;

    /// Returns operations accumulated since the last [`Self::reset_delta`].
    fn delta(&self) -> Option<Self::Delta>;

    /// Merges one received delta into the full state.
    fn merge_delta(&self, delta: &Self::Delta) -> Self;

    /// Returns the same full state with transient accumulated deltas cleared.
    fn reset_delta(&self) -> Self;
}

/// Mergeable delta that can identify its corresponding empty full state.
pub trait ReplicatedDelta: ReplicatedData {
    /// Full-state CRDT reconstructed by applying this delta.
    type Full: DeltaReplicatedData<Delta = Self>;

    /// Returns an empty full state used when the first received value is a delta.
    fn zero(&self) -> Self::Full;
}

/// Replicated data that can remove causal state owned by a departed replica.
pub trait RemovedNodePruning: ReplicatedData {
    /// Returns replica identities whose causal state may require pruning.
    fn modified_by_replica_ids(&self) -> BTreeSet<ReplicaId>;

    /// Reports whether this value still contains state from `removed_replica`.
    fn need_pruning_from(&self, removed_replica: &ReplicaId) -> bool;

    /// Collapses state from `removed_replica` into a surviving replica.
    ///
    /// Returns an error when the CRDT cannot represent the collapsed value.
    fn prune(
        &self,
        removed_replica: &ReplicaId,
        collapse_into: ReplicaId,
    ) -> Result<Self, CrdtError>;

    /// Removes causal metadata for a replica whose state was already pruned.
    fn pruning_cleanup(&self, removed_replica: &ReplicaId) -> Self;
}
