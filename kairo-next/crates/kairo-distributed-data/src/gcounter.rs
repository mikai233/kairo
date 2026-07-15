#![deny(missing_docs)]

//! Immutable grow-only counter with one monotonic component per replica.

use std::collections::BTreeMap;

use crate::{
    CrdtError, DeltaReplicatedData, RemovedNodePruning, ReplicaId, ReplicatedData, ReplicatedDelta,
};

#[derive(Debug, Clone, PartialEq, Eq)]
/// Increment-only counter CRDT merged by per-replica maximum.
///
/// The public value is the checked sum of all `u128` replica components.
pub struct GCounter {
    state: BTreeMap<ReplicaId, u128>,
    delta: Option<Box<GCounter>>,
}

impl GCounter {
    /// Creates an empty zero-valued counter.
    pub fn new() -> Self {
        Self {
            state: BTreeMap::new(),
            delta: None,
        }
    }

    /// Creates a counter from replica components, discarding zero entries.
    pub fn from_state(state: impl IntoIterator<Item = (ReplicaId, u128)>) -> Self {
        Self {
            state: state.into_iter().filter(|(_, value)| *value > 0).collect(),
            delta: None,
        }
    }

    /// Returns all non-zero replica components in deterministic order.
    pub fn state(&self) -> &BTreeMap<ReplicaId, u128> {
        &self.state
    }

    /// Returns one replica component, or zero when absent.
    pub fn replica_value(&self, replica: &ReplicaId) -> u128 {
        self.state.get(replica).copied().unwrap_or_default()
    }

    /// Returns the checked sum of every replica component.
    pub fn value(&self) -> Result<u128, CrdtError> {
        self.state
            .values()
            .try_fold(0_u128, |sum, value| sum.checked_add(*value))
            .ok_or(CrdtError::CounterOverflow)
    }

    /// Increases one replica component by `amount` and accumulates its absolute value as a delta.
    ///
    /// Zero is a no-op. Returns [`CrdtError::CounterOverflow`] rather than
    /// wrapping a component beyond `u128`.
    pub fn increment(
        &self,
        replica: impl Into<ReplicaId>,
        amount: u128,
    ) -> Result<Self, CrdtError> {
        if amount == 0 {
            return Ok(self.clone());
        }

        let replica = replica.into();
        let next_value = self
            .replica_value(&replica)
            .checked_add(amount)
            .ok_or(CrdtError::CounterOverflow)?;

        let mut state = self.state.clone();
        state.insert(replica.clone(), next_value);

        let mut delta_state = self
            .delta
            .as_deref()
            .map(|delta| delta.state.clone())
            .unwrap_or_default();
        delta_state.insert(replica, next_value);

        Ok(Self {
            state,
            delta: Some(Box::new(Self {
                state: delta_state,
                delta: None,
            })),
        })
    }

    /// Iterates over replicas with non-zero components.
    pub fn modified_by_replicas(&self) -> impl Iterator<Item = &ReplicaId> {
        self.state.keys()
    }

    /// Reports whether a departed replica still owns a component.
    pub fn need_pruning_from(&self, removed_replica: &ReplicaId) -> bool {
        self.state.contains_key(removed_replica)
    }

    /// Collapses a departed replica's component into `collapse_into`.
    ///
    /// Returns [`CrdtError::CounterOverflow`] when the combined survivor
    /// component exceeds `u128`.
    pub fn prune(
        &self,
        removed_replica: &ReplicaId,
        collapse_into: impl Into<ReplicaId>,
    ) -> Result<Self, CrdtError> {
        let Some(removed_value) = self.state.get(removed_replica).copied() else {
            return Ok(self.clone());
        };

        let collapse_into = collapse_into.into();
        let next_value = self
            .replica_value(&collapse_into)
            .checked_add(removed_value)
            .ok_or(CrdtError::CounterOverflow)?;

        let mut state = self.state.clone();
        state.remove(removed_replica);
        state.insert(collapse_into, next_value);
        Ok(Self { state, delta: None })
    }

    /// Removes a departed replica component after distributed pruning completes.
    pub fn pruning_cleanup(&self, removed_replica: &ReplicaId) -> Self {
        let mut state = self.state.clone();
        state.remove(removed_replica);
        Self { state, delta: None }
    }
}

impl Default for GCounter {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplicatedData for GCounter {
    fn merge(&self, other: &Self) -> Self {
        let mut state = other.state.clone();
        for (replica, this_value) in &self.state {
            let that_value = state.get(replica).copied().unwrap_or_default();
            if *this_value > that_value {
                state.insert(replica.clone(), *this_value);
            }
        }
        Self { state, delta: None }
    }
}

impl DeltaReplicatedData for GCounter {
    type Delta = GCounter;

    fn delta(&self) -> Option<Self::Delta> {
        self.delta.as_deref().cloned()
    }

    fn merge_delta(&self, delta: &Self::Delta) -> Self {
        self.merge(delta)
    }

    fn reset_delta(&self) -> Self {
        Self {
            state: self.state.clone(),
            delta: None,
        }
    }
}

impl ReplicatedDelta for GCounter {
    type Full = GCounter;

    fn zero(&self) -> Self::Full {
        Self::new()
    }
}

impl RemovedNodePruning for GCounter {
    fn modified_by_replica_ids(&self) -> std::collections::BTreeSet<ReplicaId> {
        self.state.keys().cloned().collect()
    }

    fn need_pruning_from(&self, removed_replica: &ReplicaId) -> bool {
        GCounter::need_pruning_from(self, removed_replica)
    }

    fn prune(
        &self,
        removed_replica: &ReplicaId,
        collapse_into: ReplicaId,
    ) -> Result<Self, CrdtError> {
        GCounter::prune(self, removed_replica, collapse_into)
    }

    fn pruning_cleanup(&self, removed_replica: &ReplicaId) -> Self {
        GCounter::pruning_cleanup(self, removed_replica)
    }
}
