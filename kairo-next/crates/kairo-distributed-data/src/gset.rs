use std::collections::BTreeSet;

use crate::{DeltaReplicatedData, ReplicatedData, ReplicatedDelta};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GSet<T> {
    elements: BTreeSet<T>,
    delta: Option<Box<GSet<T>>>,
}

impl<T> GSet<T>
where
    T: Clone + Ord,
{
    pub fn new() -> Self {
        Self {
            elements: BTreeSet::new(),
            delta: None,
        }
    }

    pub fn from_elements(elements: impl IntoIterator<Item = T>) -> Self {
        Self {
            elements: elements.into_iter().collect(),
            delta: None,
        }
    }

    pub fn elements(&self) -> &BTreeSet<T> {
        &self.elements
    }

    pub fn contains(&self, element: &T) -> bool {
        self.elements.contains(element)
    }

    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    pub fn len(&self) -> usize {
        self.elements.len()
    }

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
