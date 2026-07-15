#![deny(missing_docs)]

//! Immutable grow-only set with full-state and same-type delta replication.

use std::collections::BTreeSet;

use crate::{
    CrdtError, DeltaReplicatedData, RemovedNodePruning, ReplicaId, ReplicatedData, ReplicatedDelta,
};

#[derive(Debug, Clone, PartialEq, Eq)]
/// Grow-only set CRDT whose merge is set union.
///
/// Elements cannot be removed. Adds accumulated since the last delta reset are
/// represented by another `GSet<T>`.
pub struct GSet<T> {
    elements: BTreeSet<T>,
    delta: Option<Box<GSet<T>>>,
}

impl<T> GSet<T>
where
    T: Clone + Ord,
{
    /// Creates an empty set with no accumulated delta.
    pub fn new() -> Self {
        Self {
            elements: BTreeSet::new(),
            delta: None,
        }
    }

    /// Creates a deduplicated set with no accumulated delta.
    pub fn from_elements(elements: impl IntoIterator<Item = T>) -> Self {
        Self {
            elements: elements.into_iter().collect(),
            delta: None,
        }
    }

    /// Returns all elements in deterministic order.
    pub fn elements(&self) -> &BTreeSet<T> {
        &self.elements
    }

    /// Reports whether `element` belongs to the set.
    pub fn contains(&self, element: &T) -> bool {
        self.elements.contains(element)
    }

    /// Reports whether the set contains no elements.
    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    /// Returns the number of distinct elements.
    pub fn len(&self) -> usize {
        self.elements.len()
    }

    /// Returns a set containing `element` and records it in the pending delta.
    pub fn add(&self, element: T) -> Self {
        let mut elements = self.elements.clone();
        elements.insert(element.clone());

        let delta = match &self.delta {
            Some(delta) => {
                let mut delta_elements = delta.elements.clone();
                delta_elements.insert(element);
                Some(Box::new(Self {
                    elements: delta_elements,
                    delta: None,
                }))
            }
            None => Some(Box::new(Self::from_elements([element]))),
        };

        Self { elements, delta }
    }
}

impl<T> Default for GSet<T>
where
    T: Clone + Ord,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T> ReplicatedData for GSet<T>
where
    T: Clone + Eq + Ord,
{
    fn merge(&self, other: &Self) -> Self {
        Self {
            elements: self.elements.union(&other.elements).cloned().collect(),
            delta: None,
        }
    }
}

impl<T> DeltaReplicatedData for GSet<T>
where
    T: Clone + Eq + Ord,
{
    type Delta = GSet<T>;

    fn delta(&self) -> Option<Self::Delta> {
        self.delta.as_deref().cloned()
    }

    fn merge_delta(&self, delta: &Self::Delta) -> Self {
        self.merge(delta)
    }

    fn reset_delta(&self) -> Self {
        Self {
            elements: self.elements.clone(),
            delta: None,
        }
    }
}

impl<T> ReplicatedDelta for GSet<T>
where
    T: Clone + Eq + Ord,
{
    type Full = GSet<T>;

    fn zero(&self) -> Self::Full {
        Self::new()
    }
}

impl<T> RemovedNodePruning for GSet<T>
where
    T: Clone + Eq + Ord,
{
    fn modified_by_replica_ids(&self) -> BTreeSet<ReplicaId> {
        BTreeSet::new()
    }

    fn need_pruning_from(&self, _removed_replica: &ReplicaId) -> bool {
        false
    }

    fn prune(
        &self,
        _removed_replica: &ReplicaId,
        _collapse_into: ReplicaId,
    ) -> Result<Self, CrdtError> {
        Ok(self.clone())
    }

    fn pruning_cleanup(&self, _removed_replica: &ReplicaId) -> Self {
        self.clone()
    }
}
