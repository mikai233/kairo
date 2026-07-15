#![deny(missing_docs)]

//! Replicated data paired with removed-replica pruning metadata.
//!
//! An envelope keeps the CRDT value and its pruning state together while full
//! states and deltas move through the replicator. Pruning-aware merges discard
//! contributions from replicas whose pruning has already been performed, which
//! prevents a late state from resurrecting removed-replica data.

use std::collections::BTreeSet;

use crate::{
    CrdtError, DeltaReplicatedData, PruningPerformed, PruningState, PruningTable,
    RemovedNodePruning, ReplicaId, ReplicatedData,
};

#[derive(Debug, Clone, PartialEq, Eq)]
/// A replicated value and the removed-replica pruning state that belongs to it.
pub struct DataEnvelope<D> {
    data: D,
    pruning: PruningTable,
}

impl<D> DataEnvelope<D>
where
    D: ReplicatedData,
{
    /// Wraps `data` with an empty pruning table.
    pub fn new(data: D) -> Self {
        Self {
            data,
            pruning: PruningTable::new(),
        }
    }

    /// Wraps `data` with an existing pruning table.
    pub fn with_pruning(data: D, pruning: PruningTable) -> Self {
        Self { data, pruning }
    }

    /// Returns the replicated value.
    pub fn data(&self) -> &D {
        &self.data
    }

    /// Returns the removed-replica pruning table.
    pub fn pruning(&self) -> &PruningTable {
        &self.pruning
    }

    /// Consumes the envelope and returns only its replicated value.
    pub fn into_data(self) -> D {
        self.data
    }

    /// Consumes the envelope and returns its value and pruning table.
    pub fn into_parts(self) -> (D, PruningTable) {
        (self.data, self.pruning)
    }

    /// Merges both replicated values and both pruning tables.
    ///
    /// This merge does not expire obsolete performed markers or clean late
    /// removed-replica contributions. Use [`Self::merge_pruned`] when `D`
    /// supports removed-node pruning and a current time is available.
    pub fn merge(&self, other: &Self) -> Self {
        Self {
            data: self.data.merge(&other.data),
            pruning: self.pruning.merge(&other.pruning),
        }
    }

    /// Merges a full replicated value while preserving this envelope's pruning table.
    pub fn merge_data(&self, other: &D) -> Self {
        Self {
            data: self.data.merge(other),
            pruning: self.pruning.clone(),
        }
    }
}

impl<D> DataEnvelope<D>
where
    D: DeltaReplicatedData,
{
    /// Applies a CRDT delta while preserving this envelope's pruning table.
    pub fn merge_delta(&self, delta: &D::Delta) -> Self {
        Self {
            data: self.data.merge_delta(delta),
            pruning: self.pruning.clone(),
        }
    }
}

impl<D> DataEnvelope<D>
where
    D: RemovedNodePruning,
{
    /// Returns every replica identifier still represented by the value.
    pub fn modified_by_replica_ids(&self) -> BTreeSet<ReplicaId> {
        self.data.modified_by_replica_ids()
    }

    /// Reports whether the value still contains a contribution from `removed_replica`.
    pub fn need_pruning_from(&self, removed_replica: &ReplicaId) -> bool {
        self.data.need_pruning_from(removed_replica)
    }

    /// Initializes pruning for `removed`, assigning `owner` to perform it.
    ///
    /// Existing pruning state for `removed` is replaced with a fresh initialized
    /// marker whose seen set is empty.
    pub fn init_removed_node_pruning(&self, removed: ReplicaId, owner: ReplicaId) -> Self {
        let mut pruning = self.pruning.clone();
        pruning.initialize(removed, owner);
        Self {
            data: self.data.clone(),
            pruning,
        }
    }

    /// Transfers `removed`'s contribution to its pruning owner and marks completion.
    ///
    /// The envelope is returned unchanged unless `removed` currently has an
    /// initialized pruning marker.
    ///
    /// # Errors
    ///
    /// Returns the CRDT's pruning error if its removed-replica contribution
    /// cannot be transferred to the recorded owner.
    pub fn prune_removed_node(
        &self,
        removed: &ReplicaId,
        performed: PruningPerformed,
    ) -> Result<Self, CrdtError> {
        let Some(PruningState::Initialized(initialized)) = self.pruning.get(removed) else {
            return Ok(self.clone());
        };

        let data = self.data.prune(removed, initialized.owner().clone())?;
        let mut pruning = self.pruning.clone();
        pruning.mark_performed(removed.clone(), performed.obsolete_at_millis());
        Ok(Self { data, pruning })
    }

    /// Removes `removed`'s contribution from the value without changing metadata.
    pub fn pruning_cleanup(&self, removed: &ReplicaId) -> Self {
        Self {
            data: self.data.pruning_cleanup(removed),
            pruning: self.pruning.clone(),
        }
    }

    /// Records `seen_by` on every initialized pruning marker.
    ///
    /// Performed markers are left unchanged, and the pruning owner is never
    /// added to an initialized marker's seen set.
    pub fn add_pruning_seen(&self, seen_by: ReplicaId) -> Self {
        let mut pruning = self.pruning.clone();
        pruning.mark_all_seen_by(seen_by);
        Self {
            data: self.data.clone(),
            pruning,
        }
    }

    /// Removes performed markers whose obsolete deadline is at or before `now_millis`.
    ///
    /// Returns the updated envelope and the replica identifiers whose markers
    /// were removed. The replicated value is not changed.
    pub fn remove_obsolete_pruning_performed(
        &self,
        now_millis: u64,
    ) -> (Self, BTreeSet<ReplicaId>) {
        let mut pruning = self.pruning.clone();
        let removed = pruning.remove_obsolete_performed(now_millis);
        (
            Self {
                data: self.data.clone(),
                pruning,
            },
            removed,
        )
    }

    /// Merges envelopes with pruning cleanup and performed-marker expiry.
    ///
    /// Pruning tables are merged first and obsolete performed markers are
    /// removed. Before the CRDT values merge, both sides are cleaned of every
    /// removed-replica contribution that still has a retained performed marker.
    pub fn merge_pruned(&self, other: &Self, now_millis: u64) -> Self {
        let pruning = self
            .pruning
            .merge_without_obsolete(&other.pruning, now_millis);
        let data = cleanup_performed(&self.data, &pruning)
            .merge(&cleanup_performed(&other.data, &pruning));
        Self { data, pruning }
    }
}

impl<D> DataEnvelope<D>
where
    D: DeltaReplicatedData + RemovedNodePruning,
{
    /// Applies a delta after merging pruning metadata and expiring old markers.
    ///
    /// Retained performed markers clean removed-replica contributions from the
    /// resulting full value, including contributions carried by the delta.
    pub fn merge_delta_pruned(
        &self,
        delta: &D::Delta,
        pruning: &PruningTable,
        now_millis: u64,
    ) -> Self {
        let pruning = self.pruning.merge_without_obsolete(pruning, now_millis);
        let data = cleanup_performed(&self.data.merge_delta(delta), &pruning);
        Self { data, pruning }
    }
}

fn cleanup_performed<D>(data: &D, pruning: &PruningTable) -> D
where
    D: RemovedNodePruning,
{
    pruning
        .states()
        .iter()
        .filter(|(_, state)| matches!(state, PruningState::Performed(_)))
        .fold(data.clone(), |current, (removed, _)| {
            if current.need_pruning_from(removed) {
                current.pruning_cleanup(removed)
            } else {
                current
            }
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::GCounter;

    fn replica(id: &str) -> ReplicaId {
        ReplicaId::new(id)
    }

    #[test]
    fn envelope_initializes_and_marks_pruning_seen() {
        let removed = replica("removed");
        let owner = replica("owner");
        let peer = replica("peer");
        let envelope = DataEnvelope::new(
            GCounter::new()
                .increment(removed.clone(), 3)
                .unwrap()
                .reset_delta(),
        )
        .init_removed_node_pruning(removed.clone(), owner)
        .add_pruning_seen(peer.clone());

        assert!(envelope.need_pruning_from(&removed));
        let PruningState::Initialized(initialized) = envelope.pruning().get(&removed).unwrap()
        else {
            panic!("expected initialized pruning marker");
        };
        assert!(initialized.seen().contains(&peer));
    }

    #[test]
    fn envelope_prunes_removed_replica_into_marker_owner() {
        let removed = replica("removed");
        let owner = replica("owner");
        let envelope = DataEnvelope::new(
            GCounter::new()
                .increment(removed.clone(), 4)
                .unwrap()
                .increment(owner.clone(), 2)
                .unwrap()
                .reset_delta(),
        )
        .init_removed_node_pruning(removed.clone(), owner.clone())
        .prune_removed_node(&removed, PruningPerformed::new(100))
        .unwrap();

        assert_eq!(envelope.data().replica_value(&removed), 0);
        assert_eq!(envelope.data().replica_value(&owner), 6);
        assert_eq!(
            envelope.pruning().get(&removed),
            Some(&PruningState::Performed(PruningPerformed::new(100)))
        );
    }

    #[test]
    fn envelope_merge_pruned_cleans_performed_removed_replica_from_both_sides() {
        let removed = replica("removed");
        let owner = replica("owner");
        let mut pruning = PruningTable::new();
        pruning.mark_performed(removed.clone(), 100);

        let left = DataEnvelope::with_pruning(
            GCounter::new()
                .increment(owner.clone(), 2)
                .unwrap()
                .reset_delta(),
            pruning,
        );
        let right = DataEnvelope::new(
            GCounter::new()
                .increment(removed.clone(), 7)
                .unwrap()
                .reset_delta(),
        );

        let merged = left.merge_pruned(&right, 99);
        assert_eq!(merged.data().replica_value(&removed), 0);
        assert_eq!(merged.data().replica_value(&owner), 2);
        assert!(merged.pruning().get(&removed).is_some());
    }

    #[test]
    fn envelope_removes_obsolete_performed_markers_without_mutating_data() {
        let removed = replica("removed");
        let mut pruning = PruningTable::new();
        pruning.mark_performed(removed.clone(), 100);
        let envelope = DataEnvelope::with_pruning(
            GCounter::new()
                .increment(removed.clone(), 7)
                .unwrap()
                .reset_delta(),
            pruning,
        );

        let (kept, removed_before_deadline) = envelope.remove_obsolete_pruning_performed(99);
        assert!(removed_before_deadline.is_empty());
        assert!(kept.pruning().get(&removed).is_some());

        let (cleaned, removed_at_deadline) = envelope.remove_obsolete_pruning_performed(100);
        assert_eq!(removed_at_deadline, BTreeSet::from([removed.clone()]));
        assert_eq!(cleaned.data().replica_value(&removed), 7);
        assert!(cleaned.pruning().is_empty());
    }
}
