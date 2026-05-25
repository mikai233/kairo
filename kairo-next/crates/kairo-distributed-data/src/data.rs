use std::collections::BTreeSet;

use crate::{CrdtError, ReplicaId};

pub trait ReplicatedData: Clone + Eq {
    fn merge(&self, other: &Self) -> Self;
}

pub trait DeltaReplicatedData: ReplicatedData {
    type Delta: ReplicatedDelta<Full = Self>;

    fn delta(&self) -> Option<Self::Delta>;

    fn merge_delta(&self, delta: &Self::Delta) -> Self;

    fn reset_delta(&self) -> Self;
}

pub trait ReplicatedDelta: ReplicatedData {
    type Full: DeltaReplicatedData<Delta = Self>;

    fn zero(&self) -> Self::Full;
}

pub trait RemovedNodePruning: ReplicatedData {
    fn modified_by_replica_ids(&self) -> BTreeSet<ReplicaId>;

    fn need_pruning_from(&self, removed_replica: &ReplicaId) -> bool;

    fn prune(
        &self,
        removed_replica: &ReplicaId,
        collapse_into: ReplicaId,
    ) -> Result<Self, CrdtError>;

    fn pruning_cleanup(&self, removed_replica: &ReplicaId) -> Self;
}
