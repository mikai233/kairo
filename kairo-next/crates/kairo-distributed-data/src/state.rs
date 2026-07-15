#![deny(missing_docs)]

//! In-memory replicated-data state shared by the replicator actor and tests.
//!
//! The state stores full CRDT values with pruning metadata, merges every remote
//! write, and coalesces changed keys until [`ReplicatorState::flush_changes`].
//! Iteration and change delivery are deterministic because keys are ordered.

use std::collections::{BTreeMap, BTreeSet};

use crate::{
    DataEnvelope, DeltaReplicatedData, GetResponse, PruningPerformed, PruningState,
    RemovedNodePruning, RemovedNodePruningFailure, ReplicaId, ReplicatedDelta, ReplicatorChange,
    ReplicatorKey, UpdateOutcome,
};

#[derive(Debug, Clone)]
/// Deterministic in-memory state for one replicated CRDT type.
pub struct ReplicatorState<D>
where
    D: DeltaReplicatedData,
{
    entries: BTreeMap<ReplicatorKey, DataEnvelope<D>>,
    changed: BTreeSet<ReplicatorKey>,
}

impl<D> ReplicatorState<D>
where
    D: DeltaReplicatedData,
{
    /// Creates an empty replicated-data state.
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            changed: BTreeSet::new(),
        }
    }

    /// Returns the number of stored keys.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Reports whether no keys are stored.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Reports whether `key` has a stored envelope.
    pub fn contains_key(&self, key: &ReplicatorKey) -> bool {
        self.entries.contains_key(key)
    }

    /// Iterates over keys in lexical key order.
    pub fn keys(&self) -> impl Iterator<Item = &ReplicatorKey> {
        self.entries.keys()
    }

    /// Iterates over keys and envelopes in lexical key order.
    pub fn entries(&self) -> impl Iterator<Item = (&ReplicatorKey, &DataEnvelope<D>)> {
        self.entries.iter()
    }

    /// Reads only local state without contacting another replica.
    pub fn get_local(&self, key: &ReplicatorKey) -> GetResponse<D> {
        match self.entries.get(key) {
            Some(envelope) => GetResponse::Success {
                key: key.clone(),
                data: envelope.data().clone(),
            },
            None => GetResponse::NotFound { key: key.clone() },
        }
    }

    /// Returns the stored value and pruning metadata for `key`.
    pub fn envelope(&self, key: &ReplicatorKey) -> Option<&DataEnvelope<D>> {
        self.entries.get(key)
    }

    /// Applies a fallible local update and records its key for change delivery.
    ///
    /// The stored value is used when present; otherwise `initial` is passed to
    /// `modify`. On success the transient delta is extracted, the stored full
    /// state has that delta reset, and existing state is merged rather than
    /// overwritten.
    ///
    /// # Errors
    ///
    /// Returns the error from `modify` without changing the state.
    pub fn update_local<E, F>(
        &mut self,
        key: ReplicatorKey,
        initial: D,
        modify: F,
    ) -> Result<UpdateOutcome<D::Delta>, E>
    where
        F: FnOnce(D) -> Result<D, E>,
    {
        let current = self
            .entries
            .get(&key)
            .map(|envelope| envelope.data().clone())
            .unwrap_or(initial);
        let modified = modify(current)?;
        let delta = modified.delta();
        let full_state = modified.reset_delta();

        let next = match self.entries.get(&key) {
            Some(existing) => existing.merge_data(&full_state),
            None => DataEnvelope::new(full_state),
        };
        let changed = self.set_data(key.clone(), next);

        Ok(UpdateOutcome::new(key, changed, delta))
    }

    /// Merges a remote full-state envelope and reports whether state changed.
    ///
    /// This generic path merges pruning metadata but does not expire performed
    /// markers or clean late removed-replica contributions. Cluster-aware
    /// ingress for pruning-capable data uses [`Self::write_full_pruned`].
    pub fn write_full(&mut self, key: ReplicatorKey, envelope: DataEnvelope<D>) -> bool {
        let next = match self.entries.get(&key) {
            Some(existing) => existing.merge(&envelope),
            None => envelope,
        };
        self.set_data(key, next)
    }

    /// Applies a remote delta and reports whether state changed.
    ///
    /// If the key is absent, the delta is applied to its CRDT-defined zero.
    pub fn write_delta(&mut self, key: ReplicatorKey, delta: D::Delta) -> bool {
        let next = match self.entries.get(&key) {
            Some(existing) => existing.merge_delta(&delta),
            None => DataEnvelope::new(delta.zero().merge_delta(&delta)),
        };
        self.set_data(key, next)
    }

    /// Drains coalesced changes as current values in lexical key order.
    ///
    /// Repeated changes to a key before a flush produce one notification. A
    /// second flush without intervening state changes is empty.
    pub fn flush_changes(&mut self) -> Vec<ReplicatorChange<D>> {
        let changed = std::mem::take(&mut self.changed);
        changed
            .into_iter()
            .filter_map(|key| {
                self.entries
                    .get(&key)
                    .map(|envelope| ReplicatorChange::new(key, envelope.data().clone()))
            })
            .collect()
    }

    fn set_data(&mut self, key: ReplicatorKey, envelope: DataEnvelope<D>) -> bool {
        let changed = self
            .entries
            .get(&key)
            .is_none_or(|existing| existing != &envelope);
        self.entries.insert(key.clone(), envelope);
        if changed {
            self.changed.insert(key);
        }
        changed
    }
}

impl<D> ReplicatorState<D>
where
    D: DeltaReplicatedData + RemovedNodePruning,
{
    /// Merges a full envelope after pruning cleanup and marker expiry.
    ///
    /// `now_millis` is compared with performed-marker obsolete deadlines. The
    /// key is queued for change delivery when either CRDT data or pruning
    /// metadata changes.
    pub fn write_full_pruned(
        &mut self,
        key: ReplicatorKey,
        envelope: DataEnvelope<D>,
        now_millis: u64,
    ) -> bool {
        let next = match self.entries.get(&key) {
            Some(existing) => existing.merge_pruned(&envelope, now_millis),
            None => envelope,
        };
        self.set_data(key, next)
    }

    /// Returns every replica identifier still represented across all values.
    pub fn modified_by_replica_ids(&self) -> BTreeSet<ReplicaId> {
        self.entries
            .values()
            .flat_map(DataEnvelope::modified_by_replica_ids)
            .collect()
    }

    /// Marks all initialized pruning entries as seen by `seen_by`.
    ///
    /// Returns the keys whose pruning metadata changed.
    pub fn mark_pruning_seen(&mut self, seen_by: ReplicaId) -> BTreeSet<ReplicatorKey> {
        let updates = self
            .entries
            .iter()
            .filter_map(|(key, envelope)| {
                let next = envelope.add_pruning_seen(seen_by.clone());
                (next != *envelope).then_some((key.clone(), next))
            })
            .collect::<Vec<_>>();
        self.apply_pruning_updates(updates)
    }

    /// Marks initialized pruning entries for `key` as seen by `seen_by`.
    ///
    /// Returns whether the key's pruning metadata changed. Missing keys and
    /// envelopes without an applicable initialized marker are unchanged.
    pub fn mark_key_pruning_seen(&mut self, key: &ReplicatorKey, seen_by: ReplicaId) -> bool {
        let Some(next) = self
            .entries
            .get(key)
            .map(|envelope| envelope.add_pruning_seen(seen_by))
        else {
            return false;
        };
        self.set_data(key.clone(), next)
    }

    /// Initializes pruning on values that still contain `removed`.
    ///
    /// Existing performed markers are retained. An initialized marker with a
    /// different owner is replaced, while one with the same owner is unchanged.
    /// Returns the keys whose metadata changed.
    pub fn init_removed_node_pruning(
        &mut self,
        removed: &ReplicaId,
        owner: &ReplicaId,
    ) -> BTreeSet<ReplicatorKey> {
        let updates = self
            .entries
            .iter()
            .filter_map(|(key, envelope)| {
                if !envelope.need_pruning_from(removed) {
                    return None;
                }

                let should_initialize = match envelope.pruning().get(removed) {
                    None => true,
                    Some(PruningState::Initialized(initialized)) => initialized.owner() != owner,
                    Some(PruningState::Performed(_)) => false,
                };

                should_initialize.then(|| {
                    (
                        key.clone(),
                        envelope.init_removed_node_pruning(removed.clone(), owner.clone()),
                    )
                })
            })
            .collect::<Vec<_>>();
        self.apply_pruning_updates(updates)
    }

    /// Performs every pruning entry owned by `owner` and seen by all live replicas.
    ///
    /// Successful transfers replace initialized markers with `performed_marker`.
    /// Returns changed keys together with per-key CRDT pruning failures; a
    /// failure for one removed replica does not prevent other ready entries from
    /// being attempted.
    pub fn perform_removed_node_pruning(
        &mut self,
        owner: &ReplicaId,
        live_replicas: &BTreeSet<ReplicaId>,
        performed_marker: PruningPerformed,
    ) -> (BTreeSet<ReplicatorKey>, Vec<RemovedNodePruningFailure>) {
        let mut updates = Vec::new();
        let mut failures = Vec::new();

        for (key, envelope) in &self.entries {
            let mut next_envelope = envelope.clone();
            for removed in envelope
                .pruning()
                .ready_to_perform(owner, live_replicas.iter())
            {
                match next_envelope.prune_removed_node(&removed, performed_marker) {
                    Ok(next) => next_envelope = next,
                    Err(error) => failures.push(RemovedNodePruningFailure {
                        key: key.clone(),
                        removed,
                        reason: error.to_string(),
                    }),
                }
            }
            if next_envelope != *envelope {
                updates.push((key.clone(), next_envelope));
            }
        }

        (self.apply_pruning_updates(updates), failures)
    }

    /// Expires performed pruning markers across all envelopes.
    ///
    /// Returns keys whose metadata changed and the union of replica identifiers
    /// whose markers became obsolete at or before `now_millis`.
    pub fn remove_obsolete_pruning_performed(
        &mut self,
        now_millis: u64,
    ) -> (BTreeSet<ReplicatorKey>, BTreeSet<ReplicaId>) {
        let mut updates = Vec::new();
        let mut removed_nodes = BTreeSet::new();

        for (key, envelope) in &self.entries {
            let (next, removed) = envelope.remove_obsolete_pruning_performed(now_millis);
            if next != *envelope {
                updates.push((key.clone(), next));
            }
            removed_nodes.extend(removed);
        }

        (self.apply_pruning_updates(updates), removed_nodes)
    }

    fn apply_pruning_updates(
        &mut self,
        updates: Vec<(ReplicatorKey, DataEnvelope<D>)>,
    ) -> BTreeSet<ReplicatorKey> {
        let mut changed = BTreeSet::new();
        for (key, envelope) in updates {
            if self.set_data(key.clone(), envelope) {
                changed.insert(key);
            }
        }
        changed
    }
}

impl<D> Default for ReplicatorState<D>
where
    D: DeltaReplicatedData,
{
    fn default() -> Self {
        Self::new()
    }
}
