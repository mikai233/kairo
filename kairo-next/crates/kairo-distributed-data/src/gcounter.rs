use std::collections::BTreeMap;

use crate::{CrdtError, DeltaReplicatedData, ReplicaId, ReplicatedData, ReplicatedDelta};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GCounter {
    state: BTreeMap<ReplicaId, u128>,
    delta: Option<Box<GCounter>>,
}

impl GCounter {
    pub fn new() -> Self {
        Self {
            state: BTreeMap::new(),
            delta: None,
        }
    }

    pub fn from_state(state: impl IntoIterator<Item = (ReplicaId, u128)>) -> Self {
        Self {
            state: state.into_iter().filter(|(_, value)| *value > 0).collect(),
            delta: None,
        }
    }

    pub fn state(&self) -> &BTreeMap<ReplicaId, u128> {
        &self.state
    }

    pub fn replica_value(&self, replica: &ReplicaId) -> u128 {
        self.state.get(replica).copied().unwrap_or_default()
    }

    pub fn value(&self) -> Result<u128, CrdtError> {
        self.state
            .values()
            .try_fold(0_u128, |sum, value| sum.checked_add(*value))
            .ok_or(CrdtError::CounterOverflow)
    }

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

    pub fn modified_by_replicas(&self) -> impl Iterator<Item = &ReplicaId> {
        self.state.keys()
    }

    pub fn need_pruning_from(&self, removed_replica: &ReplicaId) -> bool {
        self.state.contains_key(removed_replica)
    }

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
