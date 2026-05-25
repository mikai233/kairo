use crate::{
    CrdtError, DeltaReplicatedData, GCounter, RemovedNodePruning, ReplicaId, ReplicatedData,
    ReplicatedDelta,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PNCounter {
    increments: GCounter,
    decrements: GCounter,
}

impl PNCounter {
    pub fn new() -> Self {
        Self {
            increments: GCounter::new(),
            decrements: GCounter::new(),
        }
    }

    pub fn from_counters(increments: GCounter, decrements: GCounter) -> Self {
        Self {
            increments,
            decrements,
        }
    }

    pub fn increments(&self) -> &GCounter {
        &self.increments
    }

    pub fn decrements(&self) -> &GCounter {
        &self.decrements
    }

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

    pub fn change(&self, replica: impl Into<ReplicaId>, amount: i128) -> Result<Self, CrdtError> {
        if amount >= 0 {
            self.increment(replica, amount as u128)
        } else {
            self.decrement(replica, amount.unsigned_abs())
        }
    }

    pub fn need_pruning_from(&self, removed_replica: &ReplicaId) -> bool {
        self.increments.need_pruning_from(removed_replica)
            || self.decrements.need_pruning_from(removed_replica)
    }

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
