use std::collections::{BTreeMap, BTreeSet};

use crate::{
    CrdtError, DeltaReplicatedData, RemovedNodePruning, ReplicaId, ReplicatedData, ReplicatedDelta,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ORSet<T> {
    elements: BTreeMap<T, VersionVector>,
    version_vector: VersionVector,
    delta: Option<Box<ORSetDelta<T>>>,
}

impl<T> ORSet<T>
where
    T: Clone + Ord,
{
    pub fn new() -> Self {
        Self {
            elements: BTreeMap::new(),
            version_vector: VersionVector::new(),
            delta: None,
        }
    }

    pub fn elements(&self) -> BTreeSet<T> {
        self.elements.keys().cloned().collect()
    }

    pub fn contains(&self, element: &T) -> bool {
        self.elements.contains_key(element)
    }

    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    pub fn len(&self) -> usize {
        self.elements.len()
    }

    pub fn add(&self, replica: impl Into<ReplicaId>, element: T) -> Self {
        let replica = replica.into();
        let next_version_vector = self.version_vector.increment(&replica);
        let element_dot =
            VersionVector::one(replica.clone(), next_version_vector.version_at(&replica));

        let mut elements = self.elements.clone();
        elements.insert(element.clone(), element_dot.clone());

        let add_delta = ORSetDelta::Add(Self {
            elements: BTreeMap::from([(element, element_dot.clone())]),
            version_vector: element_dot,
            delta: None,
        });
        Self {
            elements,
            version_vector: next_version_vector,
            delta: Some(Box::new(merge_optional_delta(
                self.delta.as_deref(),
                add_delta,
            ))),
        }
    }

    pub fn remove(&self, replica: impl Into<ReplicaId>, element: &T) -> Self {
        let Some(_) = self.elements.get(element) else {
            return self.clone();
        };

        let replica = replica.into();
        let remove_delta = ORSetDelta::Remove(ORSetRemoveDelta {
            element: element.clone(),
            seen: self.version_vector.clone(),
            remove_dot: VersionVector::one(
                replica.clone(),
                self.version_vector.version_at(&replica),
            ),
        });

        let mut elements = self.elements.clone();
        elements.remove(element);
        Self {
            elements,
            version_vector: self.version_vector.clone(),
            delta: Some(Box::new(merge_optional_delta(
                self.delta.as_deref(),
                remove_delta,
            ))),
        }
    }

    pub fn dots_for(&self, element: &T) -> Option<&BTreeMap<ReplicaId, u64>> {
        self.elements.get(element).map(VersionVector::entries)
    }

    pub(crate) fn from_wire_state(
        elements: impl IntoIterator<Item = (T, BTreeMap<ReplicaId, u64>)>,
        version_vector: BTreeMap<ReplicaId, u64>,
    ) -> Self {
        Self {
            elements: elements
                .into_iter()
                .filter(|(_, dots)| !dots.is_empty())
                .map(|(element, dots)| (element, VersionVector(dots)))
                .collect(),
            version_vector: VersionVector(version_vector),
            delta: None,
        }
    }

    pub(crate) fn element_dots(&self) -> impl Iterator<Item = (&T, &BTreeMap<ReplicaId, u64>)> {
        self.elements
            .iter()
            .map(|(element, dots)| (element, dots.entries()))
    }

    pub(crate) fn version_vector_entries(&self) -> &BTreeMap<ReplicaId, u64> {
        self.version_vector.entries()
    }

    fn merge_add_delta(&self, add: &Self) -> Self {
        let mut elements = self.elements.clone();
        for (element, add_dots) in &add.elements {
            let merged = match elements.get(element) {
                Some(existing_dots) => merge_element_dots(
                    existing_dots,
                    add_dots,
                    &self.version_vector,
                    &add.version_vector,
                ),
                None => add_dots.clone(),
            };
            if merged.is_empty() {
                elements.remove(element);
            } else {
                elements.insert(element.clone(), merged);
            }
        }
        Self {
            elements,
            version_vector: self.version_vector.merge(&add.version_vector),
            delta: None,
        }
    }

    fn merge_remove_delta(&self, remove: &ORSetRemoveDelta<T>) -> Self {
        let mut elements = self.elements.clone();
        if let Some(dots) = elements.get(&remove.element)
            && remove.seen.dominates(dots)
        {
            elements.remove(&remove.element);
        }

        Self {
            elements,
            version_vector: self.version_vector.merge(&remove.remove_dot),
            delta: None,
        }
    }
}

impl<T> Default for ORSet<T>
where
    T: Clone + Ord,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T> ReplicatedData for ORSet<T>
where
    T: Clone + Eq + Ord,
{
    fn merge(&self, other: &Self) -> Self {
        let mut elements = BTreeMap::new();
        for element in self
            .elements
            .keys()
            .chain(other.elements.keys())
            .collect::<BTreeSet<_>>()
        {
            let merged_dots = match (self.elements.get(element), other.elements.get(element)) {
                (Some(left), Some(right)) => {
                    merge_element_dots(left, right, &self.version_vector, &other.version_vector)
                }
                (Some(left), None) => subtract_dominated_dots(left, &other.version_vector),
                (None, Some(right)) => subtract_dominated_dots(right, &self.version_vector),
                (None, None) => VersionVector::new(),
            };
            if !merged_dots.is_empty() {
                elements.insert(element.clone(), merged_dots);
            }
        }

        Self {
            elements,
            version_vector: self.version_vector.merge(&other.version_vector),
            delta: None,
        }
    }
}

impl<T> DeltaReplicatedData for ORSet<T>
where
    T: Clone + Eq + Ord,
{
    type Delta = ORSetDelta<T>;

    fn delta(&self) -> Option<Self::Delta> {
        self.delta.as_deref().cloned()
    }

    fn merge_delta(&self, delta: &Self::Delta) -> Self {
        match delta {
            ORSetDelta::Add(add) => self.merge_add_delta(add),
            ORSetDelta::Remove(remove) => self.merge_remove_delta(remove),
            ORSetDelta::Group(ops) => ops
                .iter()
                .fold(self.clone(), |current, op| current.merge_delta(op)),
        }
    }

    fn reset_delta(&self) -> Self {
        Self {
            elements: self.elements.clone(),
            version_vector: self.version_vector.clone(),
            delta: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ORSetDelta<T> {
    Add(ORSet<T>),
    Remove(ORSetRemoveDelta<T>),
    Group(Vec<ORSetDelta<T>>),
}

impl<T> ReplicatedData for ORSetDelta<T>
where
    T: Clone + Eq + Ord,
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

impl<T> ReplicatedDelta for ORSetDelta<T>
where
    T: Clone + Eq + Ord,
{
    type Full = ORSet<T>;

    fn zero(&self) -> Self::Full {
        ORSet::new()
    }
}

impl<T> RemovedNodePruning for ORSet<T>
where
    T: Clone + Eq + Ord,
{
    fn modified_by_replica_ids(&self) -> BTreeSet<ReplicaId> {
        self.version_vector.modified_by_replica_ids()
    }

    fn need_pruning_from(&self, removed_replica: &ReplicaId) -> bool {
        self.version_vector.need_pruning_from(removed_replica)
    }

    fn prune(
        &self,
        removed_replica: &ReplicaId,
        collapse_into: ReplicaId,
    ) -> Result<Self, CrdtError> {
        let mut pruned_elements = BTreeMap::new();
        let mut touched_elements = Vec::new();
        for (element, dots) in &self.elements {
            if dots.need_pruning_from(removed_replica) {
                pruned_elements.insert(
                    element.clone(),
                    dots.prune(removed_replica, collapse_into.clone()),
                );
                touched_elements.push(element.clone());
            } else {
                pruned_elements.insert(element.clone(), dots.clone());
            }
        }

        let base = Self {
            elements: pruned_elements,
            version_vector: self
                .version_vector
                .prune(removed_replica, collapse_into.clone()),
            delta: None,
        };

        Ok(touched_elements.into_iter().fold(base, |current, element| {
            current.add(collapse_into.clone(), element)
        }))
    }

    fn pruning_cleanup(&self, removed_replica: &ReplicaId) -> Self {
        Self {
            elements: self
                .elements
                .iter()
                .map(|(element, dots)| {
                    (
                        element.clone(),
                        if dots.need_pruning_from(removed_replica) {
                            dots.pruning_cleanup(removed_replica)
                        } else {
                            dots.clone()
                        },
                    )
                })
                .filter(|(_, dots)| !dots.is_empty())
                .collect(),
            version_vector: self.version_vector.pruning_cleanup(removed_replica),
            delta: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ORSetRemoveDelta<T> {
    element: T,
    seen: VersionVector,
    remove_dot: VersionVector,
}

impl<T> ORSetRemoveDelta<T> {
    pub fn element(&self) -> &T {
        &self.element
    }
}

fn merge_optional_delta<T>(existing: Option<&ORSetDelta<T>>, next: ORSetDelta<T>) -> ORSetDelta<T>
where
    T: Clone + Eq + Ord,
{
    existing
        .map(|existing| existing.merge(&next))
        .unwrap_or(next)
}

fn merge_element_dots(
    left: &VersionVector,
    right: &VersionVector,
    left_context: &VersionVector,
    right_context: &VersionVector,
) -> VersionVector {
    let mut merged = BTreeMap::new();
    for (replica, version) in left.entries() {
        if right.version_at(replica) == *version || right_context.version_at(replica) < *version {
            merged.insert(replica.clone(), *version);
        }
    }
    for (replica, version) in right.entries() {
        if left_context.version_at(replica) < *version {
            merged.insert(replica.clone(), *version);
        }
    }
    VersionVector(merged)
}

fn subtract_dominated_dots(dots: &VersionVector, context: &VersionVector) -> VersionVector {
    VersionVector(
        dots.entries()
            .iter()
            .filter(|(replica, version)| context.version_at(replica) < **version)
            .map(|(replica, version)| (replica.clone(), *version))
            .collect(),
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct VersionVector(BTreeMap<ReplicaId, u64>);

impl VersionVector {
    fn new() -> Self {
        Self(BTreeMap::new())
    }

    fn one(replica: ReplicaId, version: u64) -> Self {
        if version == 0 {
            Self::new()
        } else {
            Self(BTreeMap::from([(replica, version)]))
        }
    }

    fn entries(&self) -> &BTreeMap<ReplicaId, u64> {
        &self.0
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn version_at(&self, replica: &ReplicaId) -> u64 {
        self.0.get(replica).copied().unwrap_or_default()
    }

    fn increment(&self, replica: &ReplicaId) -> Self {
        let mut entries = self.0.clone();
        let next = self.version_at(replica).saturating_add(1);
        entries.insert(replica.clone(), next);
        Self(entries)
    }

    fn merge(&self, other: &Self) -> Self {
        let mut entries = other.0.clone();
        for (replica, version) in &self.0 {
            let other_version = entries.get(replica).copied().unwrap_or_default();
            if *version > other_version {
                entries.insert(replica.clone(), *version);
            }
        }
        Self(entries)
    }

    fn dominates(&self, dots: &VersionVector) -> bool {
        dots.entries()
            .iter()
            .all(|(replica, version)| self.version_at(replica) >= *version)
    }

    fn modified_by_replica_ids(&self) -> BTreeSet<ReplicaId> {
        self.0.keys().cloned().collect()
    }

    fn need_pruning_from(&self, removed_replica: &ReplicaId) -> bool {
        self.0.contains_key(removed_replica)
    }

    fn prune(&self, removed_replica: &ReplicaId, collapse_into: ReplicaId) -> Self {
        let mut pruned = self.pruning_cleanup(removed_replica);
        pruned = pruned.increment(&collapse_into);
        pruned
    }

    fn pruning_cleanup(&self, removed_replica: &ReplicaId) -> Self {
        let mut entries = self.0.clone();
        entries.remove(removed_replica);
        Self(entries)
    }
}
