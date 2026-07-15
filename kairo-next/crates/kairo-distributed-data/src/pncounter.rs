#![deny(missing_docs)]

//! Immutable increment/decrement counter composed from two grow-only counters.

use crate::{
    CrdtError, DeltaReplicatedData, GCounter, RemovedNodePruning, ReplicaId, ReplicatedData,
    ReplicatedDelta,
};

#[derive(Debug, Clone, PartialEq, Eq)]
/// Positive/negative counter CRDT backed by increment and decrement GCounters.
///
/// Merge combines each inner counter independently; the exposed value is the
/// checked increment total minus the checked decrement total.
pub struct PNCounter {
    increments: GCounter,
    decrements: GCounter,
}

impl PNCounter {
    /// Creates an empty zero-valued counter.
    pub fn new() -> Self {
        Self {
            increments: GCounter::new(),
            decrements: GCounter::new(),
        }
    }

    /// Creates a counter from explicit positive and negative components.
    pub fn from_counters(increments: GCounter, decrements: GCounter) -> Self {
        Self {
            increments,
            decrements,
        }
    }

    /// Returns the grow-only positive component.
    pub fn increments(&self) -> &GCounter {
        &self.increments
    }

    /// Returns the grow-only negative component.
    pub fn decrements(&self) -> &GCounter {
        &self.decrements
    }

    /// Returns the signed counter value.
    ///
    /// Returns [`CrdtError::CounterValueOutOfRange`] if either unsigned inner
    /// total cannot be represented by `i128`.
    pub fn value(&self) -> Result<i128, CrdtError> {
        let increments: i128 = self
            .increments
            .value()?
            .try_into()
            .map_err(|_| CrdtError::CounterValueOutOfRange)?;
        let decrements: i128 = self
            .decrements
            .value()?
            .try_into()
            .map_err(|_| CrdtError::CounterValueOutOfRange)?;
        Ok(increments - decrements)
    }

    /// Adds `amount` to one replica's positive component.
    pub fn increment(
        &self,
        replica: impl Into<ReplicaId>,
        amount: u128,
    ) -> Result<Self, CrdtError> {
        Ok(Self {
            increments: self.increments.increment(replica, amount)?,
            decrements: self.decrements.clone(),
        })
    }

    /// Adds `amount` to one replica's negative component.
    pub fn decrement(
        &self,
        replica: impl Into<ReplicaId>,
        amount: u128,
    ) -> Result<Self, CrdtError> {
        Ok(Self {
            increments: self.increments.clone(),
            decrements: self.decrements.increment(replica, amount)?,
        })
    }

    /// Applies a signed change, routing negative values to the decrement side.
    pub fn change(&self, replica: impl Into<ReplicaId>, amount: i128) -> Result<Self, CrdtError> {
        if amount >= 0 {
            self.increment(replica, amount as u128)
        } else {
            self.decrement(replica, amount.unsigned_abs())
        }
    }

    /// Reports whether either inner counter contains a departed replica.
    pub fn need_pruning_from(&self, removed_replica: &ReplicaId) -> bool {
        self.increments.need_pruning_from(removed_replica)
            || self.decrements.need_pruning_from(removed_replica)
    }

    /// Collapses both departed-replica components into one survivor.
    pub fn prune(
        &self,
        removed_replica: &ReplicaId,
        collapse_into: impl Into<ReplicaId>,
    ) -> Result<Self, CrdtError> {
        let collapse_into = collapse_into.into();
        Ok(Self {
            increments: self
                .increments
                .prune(removed_replica, collapse_into.clone())?,
            decrements: self.decrements.prune(removed_replica, collapse_into)?,
        })
    }

    /// Removes a departed replica from both components after pruning completes.
    pub fn pruning_cleanup(&self, removed_replica: &ReplicaId) -> Self {
        Self {
            increments: self.increments.pruning_cleanup(removed_replica),
            decrements: self.decrements.pruning_cleanup(removed_replica),
        }
    }
}

impl Default for PNCounter {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplicatedData for PNCounter {
    fn merge(&self, other: &Self) -> Self {
        Self {
            increments: self.increments.merge(&other.increments),
            decrements: self.decrements.merge(&other.decrements),
        }
    }
}

impl DeltaReplicatedData for PNCounter {
    type Delta = PNCounter;

    fn delta(&self) -> Option<Self::Delta> {
        let increments_delta = self.increments.delta().unwrap_or_default();
        let decrements_delta = self.decrements.delta().unwrap_or_default();
        Some(Self {
            increments: increments_delta,
            decrements: decrements_delta,
        })
    }

    fn merge_delta(&self, delta: &Self::Delta) -> Self {
        self.merge(delta)
    }

    fn reset_delta(&self) -> Self {
        Self {
            increments: self.increments.reset_delta(),
            decrements: self.decrements.reset_delta(),
        }
    }
}

impl ReplicatedDelta for PNCounter {
    type Full = PNCounter;

    fn zero(&self) -> Self::Full {
        Self::new()
    }
}

impl RemovedNodePruning for PNCounter {
    fn modified_by_replica_ids(&self) -> std::collections::BTreeSet<ReplicaId> {
        let mut replicas = self.increments.modified_by_replica_ids();
        replicas.extend(self.decrements.modified_by_replica_ids());
        replicas
    }

    fn need_pruning_from(&self, removed_replica: &ReplicaId) -> bool {
        PNCounter::need_pruning_from(self, removed_replica)
    }

    fn prune(
        &self,
        removed_replica: &ReplicaId,
        collapse_into: ReplicaId,
    ) -> Result<Self, CrdtError> {
        PNCounter::prune(self, removed_replica, collapse_into)
    }

    fn pruning_cleanup(&self, removed_replica: &ReplicaId) -> Self {
        PNCounter::pruning_cleanup(self, removed_replica)
    }
}
