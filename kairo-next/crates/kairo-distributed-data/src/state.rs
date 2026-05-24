use std::collections::{BTreeMap, BTreeSet};

use crate::{
    DataEnvelope, DeltaReplicatedData, GetResponse, ReplicatedDelta, ReplicatorChange,
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

impl<D> Default for ReplicatorState<D>
where
    D: DeltaReplicatedData,
{
    fn default() -> Self {
        Self::new()
    }
}
