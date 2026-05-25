use std::collections::{BTreeMap, BTreeSet};

use crate::{
    DataEnvelope, DeltaReplicatedData, GetResponse, PruningPerformed, PruningState,
    RemovedNodePruning, RemovedNodePruningFailure, ReplicaId, ReplicatedDelta, ReplicatorChange,
    ReplicatorKey, UpdateOutcome,
};

#[derive(Debug, Clone)]
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
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            changed: BTreeSet::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn contains_key(&self, key: &ReplicatorKey) -> bool {
        self.entries.contains_key(key)
    }

    pub fn keys(&self) -> impl Iterator<Item = &ReplicatorKey> {
        self.entries.keys()
    }

    pub fn get_local(&self, key: &ReplicatorKey) -> GetResponse<D> {
        match self.entries.get(key) {
            Some(envelope) => GetResponse::Success {
                key: key.clone(),
                data: envelope.data().clone(),
            },
            None => GetResponse::NotFound { key: key.clone() },
        }
    }

    pub fn envelope(&self, key: &ReplicatorKey) -> Option<&DataEnvelope<D>> {
        self.entries.get(key)
    }

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

    pub fn write_full(&mut self, key: ReplicatorKey, envelope: DataEnvelope<D>) -> bool {
        let next = match self.entries.get(&key) {
            Some(existing) => existing.merge(&envelope),
            None => envelope,
        };
        self.set_data(key, next)
    }

    pub fn write_delta(&mut self, key: ReplicatorKey, delta: D::Delta) -> bool {
        let next = match self.entries.get(&key) {
            Some(existing) => existing.merge_delta(&delta),
            None => DataEnvelope::new(delta.zero().merge_delta(&delta)),
        };
        self.set_data(key, next)
    }

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
    pub fn modified_by_replica_ids(&self) -> BTreeSet<ReplicaId> {
        self.entries
            .values()
            .flat_map(DataEnvelope::modified_by_replica_ids)
            .collect()
    }

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
