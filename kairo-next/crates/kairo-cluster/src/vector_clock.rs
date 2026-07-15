#![deny(missing_docs)]

use std::collections::BTreeMap;

/// Stable logical node identifier used as one vector-clock dimension.
///
/// Callers must derive names from stable cluster identity rather than process
/// memory or Rust type metadata when clocks cross a wire boundary.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct VectorClockNode(String);

impl VectorClockNode {
    /// Creates a logical clock node from its stable name.
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    /// Returns the stable node name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Causal ordering between two vector clocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorClockOrdering {
    /// Both clocks describe the same causal history.
    Same,
    /// Every counter is less than or equal to the other clock and at least one is less.
    Before,
    /// Every counter is greater than or equal to the other clock and at least one is greater.
    After,
    /// Each clock contains an event absent from the other's causal history.
    Concurrent,
}

/// Immutable vector clock for ordering distributed cluster state changes.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct VectorClock {
    versions: BTreeMap<VectorClockNode, u64>,
}

impl VectorClock {
    /// Creates an empty clock.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a clock from explicit node counters.
    ///
    /// Repeated nodes keep the final entry yielded by the iterator.
    pub fn from_entries(entries: impl IntoIterator<Item = (VectorClockNode, u64)>) -> Self {
        Self {
            versions: entries.into_iter().collect(),
        }
    }

    /// Returns a new clock with `node` incremented by one.
    pub fn increment(&self, node: impl Into<VectorClockNode>) -> Self {
        let mut versions = self.versions.clone();
        let node = node.into();
        let next = versions.get(&node).copied().unwrap_or(0) + 1;
        versions.insert(node, next);
        Self { versions }
    }

    /// Returns `node`'s counter, or zero when the node has no entry.
    pub fn get(&self, node: &VectorClockNode) -> u64 {
        self.versions.get(node).copied().unwrap_or(0)
    }

    /// Iterates over node counters in stable node-name order.
    pub fn entries(&self) -> impl Iterator<Item = (&VectorClockNode, u64)> {
        self.versions.iter().map(|(node, version)| (node, *version))
    }

    /// Returns whether this clock has no causal entries.
    pub fn is_empty(&self) -> bool {
        self.versions.is_empty()
    }

    /// Computes the causal ordering of this clock relative to `other`.
    pub fn compare(&self, other: &Self) -> VectorClockOrdering {
        let mut has_less = false;
        let mut has_greater = false;

        for node in self.versions.keys().chain(other.versions.keys()) {
            let left = self.versions.get(node).copied().unwrap_or(0);
            let right = other.versions.get(node).copied().unwrap_or(0);
            has_less |= left < right;
            has_greater |= left > right;
            if has_less && has_greater {
                return VectorClockOrdering::Concurrent;
            }
        }

        match (has_less, has_greater) {
            (false, false) => VectorClockOrdering::Same,
            (true, false) => VectorClockOrdering::Before,
            (false, true) => VectorClockOrdering::After,
            (true, true) => VectorClockOrdering::Concurrent,
        }
    }

    /// Returns whether this clock is causally before `other`.
    pub fn is_before(&self, other: &Self) -> bool {
        self.compare(other) == VectorClockOrdering::Before
    }

    /// Returns whether this clock is causally after `other`.
    pub fn is_after(&self, other: &Self) -> bool {
        self.compare(other) == VectorClockOrdering::After
    }

    /// Returns whether neither clock causally dominates the other.
    pub fn is_concurrent(&self, other: &Self) -> bool {
        self.compare(other) == VectorClockOrdering::Concurrent
    }

    /// Merges causal histories by retaining the maximum counter for every node.
    pub fn merge(&self, other: &Self) -> Self {
        let mut versions = other.versions.clone();
        for (node, time) in &self.versions {
            let current = versions.get(node).copied().unwrap_or(0);
            if *time > current {
                versions.insert(node.clone(), *time);
            }
        }
        Self { versions }
    }

    /// Returns a clock without the removed node's causal dimension.
    pub fn prune(&self, node: &VectorClockNode) -> Self {
        let mut versions = self.versions.clone();
        versions.remove(node);
        Self { versions }
    }
}

impl From<&str> for VectorClockNode {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for VectorClockNode {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn increment_tracks_node_versions_without_mutating_original() {
        let node = VectorClockNode::new("node-a");
        let clock = VectorClock::new();
        let incremented = clock.increment(node.clone()).increment(node.clone());

        assert!(clock.is_empty());
        assert_eq!(incremented.get(&node), 2);
    }

    #[test]
    fn compare_detects_same_before_after_and_concurrent() {
        let a = VectorClockNode::new("node-a");
        let b = VectorClockNode::new("node-b");
        let base = VectorClock::new();
        let left = base.increment(a.clone());
        let right = left.increment(b.clone());
        let concurrent = base.increment(b);

        assert_eq!(base.compare(&base), VectorClockOrdering::Same);
        assert_eq!(left.compare(&right), VectorClockOrdering::Before);
        assert_eq!(right.compare(&left), VectorClockOrdering::After);
        assert_eq!(left.compare(&concurrent), VectorClockOrdering::Concurrent);
    }

    #[test]
    fn merge_keeps_max_counter_for_each_node() {
        let a = VectorClockNode::new("node-a");
        let b = VectorClockNode::new("node-b");
        let left = VectorClock::new().increment(a.clone()).increment(a.clone());
        let right = VectorClock::new().increment(a.clone()).increment(b.clone());

        let merged = left.merge(&right);

        assert_eq!(merged.get(&a), 2);
        assert_eq!(merged.get(&b), 1);
        assert!(merged.is_after(&right));
        assert!(merged.is_after(&left));
    }

    #[test]
    fn prune_removes_removed_node_version() {
        let a = VectorClockNode::new("node-a");
        let b = VectorClockNode::new("node-b");
        let clock = VectorClock::new().increment(a.clone()).increment(b.clone());

        let pruned = clock.prune(&a);

        assert_eq!(pruned.get(&a), 0);
        assert_eq!(pruned.get(&b), 1);
    }
}
