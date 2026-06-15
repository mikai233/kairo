use std::collections::{BTreeMap, BTreeSet};

use crate::{
    CrdtError, DeltaReplicatedData, ORSet, ORSetDelta, RemovedNodePruning, ReplicaId,
    ReplicatedData, ReplicatedDelta,
};

#[derive(Clone, PartialEq, Eq)]
pub struct ORMap<K, V>
where
    K: Clone + Ord,
    V: DeltaReplicatedData,
{
    keys: ORSet<K>,
    values: BTreeMap<K, V>,
    delta: Option<Box<ORMapDelta<K, V>>>,
}

impl<K, V> ORMap<K, V>
where
    K: Clone + Eq + Ord,
    V: DeltaReplicatedData,
{
    pub fn new() -> Self {
        Self {
            keys: ORSet::new(),
            values: BTreeMap::new(),
            delta: None,
        }
    }

    pub fn keys(&self) -> BTreeSet<K> {
        self.keys.elements()
    }

    pub fn entries(&self) -> &BTreeMap<K, V> {
        &self.values
    }

    pub(crate) fn from_wire_state(keys: ORSet<K>, values: BTreeMap<K, V>) -> Self {
        Self {
            keys,
            values,
            delta: None,
        }
    }

    pub(crate) fn key_set(&self) -> &ORSet<K> {
        &self.keys
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.values.get(key)
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.values.contains_key(key)
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn put(&self, replica: impl Into<ReplicaId>, key: K, value: V) -> Self {
        let keys = self.keys.reset_delta().add(replica, key.clone());
        let key_delta = keys.delta().expect("ORSet add always records a key delta");
        let mut values = self.values.clone();
        values.insert(key.clone(), value.clone());
        Self {
            keys,
            values,
            delta: Some(Box::new(merge_optional_delta(
                self.delta.as_deref(),
                ORMapDelta::Put {
                    keys: key_delta,
                    key,
                    value,
                },
            ))),
        }
    }

    pub fn updated(
        &self,
        replica: impl Into<ReplicaId>,
        key: K,
        initial: V,
        modify: impl FnOnce(V) -> V,
    ) -> Self {
        let had_value = self.values.contains_key(&key);
        let old_value = self.values.get(&key).cloned().unwrap_or(initial);
        let next_value = modify(old_value.reset_delta());
        let keys = self.keys.reset_delta().add(replica, key.clone());
        let key_delta = keys.delta().expect("ORSet add always records a key delta");

        let mut values = self.values.clone();
        values.insert(key.clone(), next_value.clone());

        let delta_op = match (had_value, next_value.delta()) {
            (true, Some(value_delta)) => ORMapDelta::Update {
                keys: key_delta,
                values: BTreeMap::from([(key, value_delta)]),
            },
            _ => ORMapDelta::Put {
                keys: key_delta,
                key,
                value: next_value,
            },
        };

        Self {
            keys,
            values,
            delta: Some(Box::new(merge_optional_delta(
                self.delta.as_deref(),
                delta_op,
            ))),
        }
    }

    pub fn remove(&self, replica: impl Into<ReplicaId>, key: &K) -> Self {
        if !self.values.contains_key(key) {
            return self.clone();
        }

        let keys = self.keys.reset_delta().remove(replica, key);
        let key_delta = keys
            .delta()
            .expect("ORSet remove of an existing key always records a key delta");
        let mut values = self.values.clone();
        values.remove(key);

        Self {
            keys,
            values,
            delta: Some(Box::new(merge_optional_delta(
                self.delta.as_deref(),
                ORMapDelta::Remove { keys: key_delta },
            ))),
        }
    }

    fn dry_merge_delta(&self, delta: &ORMapDelta<K, V>) -> Self {
        let mut keys = self.keys.clone();
        let mut values = self.values.clone();
        apply_delta(&mut keys, &mut values, delta);
        Self {
            keys,
            values,
            delta: None,
        }
    }
}

impl<K, V> Default for ORMap<K, V>
where
    K: Clone + Eq + Ord,
    V: DeltaReplicatedData,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> ReplicatedData for ORMap<K, V>
where
    K: Clone + Eq + Ord,
    V: DeltaReplicatedData,
{
    fn merge(&self, other: &Self) -> Self {
        let keys = self.keys.merge(&other.keys);
        let mut values = BTreeMap::new();
        for key in keys.elements() {
            match (self.values.get(&key), other.values.get(&key)) {
                (Some(left), Some(right)) => {
                    values.insert(key, left.merge(right));
                }
                (Some(left), None) => {
                    values.insert(key, left.clone());
                }
                (None, Some(right)) => {
                    values.insert(key, right.clone());
                }
                (None, None) => {}
            }
        }
        Self {
            keys,
            values,
            delta: None,
        }
    }
}

impl<K, V> DeltaReplicatedData for ORMap<K, V>
where
    K: Clone + Eq + Ord,
    V: DeltaReplicatedData,
{
    type Delta = ORMapDelta<K, V>;

    fn delta(&self) -> Option<Self::Delta> {
        self.delta.as_deref().cloned()
    }

    fn merge_delta(&self, delta: &Self::Delta) -> Self {
        self.merge(&self.dry_merge_delta(delta))
    }

    fn reset_delta(&self) -> Self {
        Self {
            keys: self.keys.reset_delta(),
            values: self.values.clone(),
            delta: None,
        }
    }
}

impl<K, V> RemovedNodePruning for ORMap<K, V>
where
    K: Clone + Eq + Ord,
    V: DeltaReplicatedData + RemovedNodePruning,
{
    fn modified_by_replica_ids(&self) -> BTreeSet<ReplicaId> {
        self.keys
            .modified_by_replica_ids()
            .into_iter()
            .chain(
                self.values
                    .values()
                    .flat_map(RemovedNodePruning::modified_by_replica_ids),
            )
            .collect()
    }

    fn need_pruning_from(&self, removed_replica: &ReplicaId) -> bool {
        self.keys.need_pruning_from(removed_replica)
            || self
                .values
                .values()
                .any(|value| value.need_pruning_from(removed_replica))
    }

    fn prune(
        &self,
        removed_replica: &ReplicaId,
        collapse_into: ReplicaId,
    ) -> Result<Self, CrdtError> {
        Ok(Self {
            keys: self.keys.prune(removed_replica, collapse_into.clone())?,
            values: self
                .values
                .iter()
                .map(|(key, value)| {
                    let value = if value.need_pruning_from(removed_replica) {
                        value.prune(removed_replica, collapse_into.clone())?
                    } else {
                        value.clone()
                    };
                    Ok((key.clone(), value))
                })
                .collect::<Result<_, CrdtError>>()?,
            delta: None,
        })
    }

    fn pruning_cleanup(&self, removed_replica: &ReplicaId) -> Self {
        Self {
            keys: self.keys.pruning_cleanup(removed_replica),
            values: self
                .values
                .iter()
                .map(|(key, value)| {
                    let value = if value.need_pruning_from(removed_replica) {
                        value.pruning_cleanup(removed_replica)
                    } else {
                        value.clone()
                    };
                    (key.clone(), value)
                })
                .collect(),
            delta: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ORMapDelta<K, V>
where
    K: Clone + Ord,
    V: DeltaReplicatedData,
{
    Put {
        keys: ORSetDelta<K>,
        key: K,
        value: V,
    },
    Update {
        keys: ORSetDelta<K>,
        values: BTreeMap<K, V::Delta>,
    },
    Remove {
        keys: ORSetDelta<K>,
    },
    Group(Vec<ORMapDelta<K, V>>),
}

impl<K, V> ReplicatedData for ORMapDelta<K, V>
where
    K: Clone + Eq + Ord,
    V: DeltaReplicatedData,
{
    fn merge(&self, other: &Self) -> Self {
        match (self, other) {
            (Self::Group(left), Self::Group(right)) => {
                let mut ops = left.clone();
                ops.extend(right.iter().cloned());
                Self::Group(ops)
            }
            (Self::Group(left), right) => {
                let mut ops = left.clone();
                ops.push(right.clone());
                Self::Group(ops)
            }
            (left, Self::Group(right)) => {
                let mut ops = Vec::with_capacity(right.len() + 1);
                ops.push(left.clone());
                ops.extend(right.iter().cloned());
                Self::Group(ops)
            }
            (left, right) => Self::Group(vec![left.clone(), right.clone()]),
        }
    }
}

impl<K, V> ReplicatedDelta for ORMapDelta<K, V>
where
    K: Clone + Eq + Ord,
    V: DeltaReplicatedData,
{
    type Full = ORMap<K, V>;

    fn zero(&self) -> Self::Full {
        ORMap::new()
    }
}

fn apply_delta<K, V>(keys: &mut ORSet<K>, values: &mut BTreeMap<K, V>, delta: &ORMapDelta<K, V>)
where
    K: Clone + Eq + Ord,
    V: DeltaReplicatedData,
{
    match delta {
        ORMapDelta::Put {
            keys: key_delta,
            key,
            value,
        } => {
            *keys = keys.merge_delta(key_delta);
            values.insert(key.clone(), value.clone());
        }
        ORMapDelta::Update {
            keys: key_delta,
            values: value_deltas,
        } => {
            *keys = keys.merge_delta(key_delta);
            for (key, value_delta) in value_deltas {
                if keys.contains(key) {
                    let next_value = values
                        .get(key)
                        .map(|value| value.merge_delta(value_delta))
                        .unwrap_or_else(|| value_delta.zero().merge_delta(value_delta));
                    values.insert(key.clone(), next_value);
                }
            }
        }
        ORMapDelta::Remove { keys: key_delta } => {
            if let ORSetDelta::Remove(remove) = key_delta {
                values.remove(remove.element());
            }
            *keys = keys.merge_delta(key_delta);
        }
        ORMapDelta::Group(ops) => {
            for op in ops {
                apply_delta(keys, values, op);
            }
        }
    }
}

fn merge_optional_delta<K, V>(
    existing: Option<&ORMapDelta<K, V>>,
    next: ORMapDelta<K, V>,
) -> ORMapDelta<K, V>
where
    K: Clone + Eq + Ord,
    V: DeltaReplicatedData,
{
    existing
        .map(|existing| existing.merge(&next))
        .unwrap_or(next)
}
