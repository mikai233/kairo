#![deny(missing_docs)]

//! Immutable last-writer-wins register with explicit clock and writer identity.

use std::collections::BTreeSet;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::{
    CrdtError, DeltaReplicatedData, RemovedNodePruning, ReplicaId, ReplicatedData, ReplicatedDelta,
};

#[derive(Debug, Clone, PartialEq, Eq)]
/// Last-writer-wins register ordered by timestamp then replica identity.
///
/// The greatest timestamp wins; equal timestamps choose the lowest
/// [`ReplicaId`]. Wall-clock timestamps therefore require acceptable clock
/// synchronization when concurrent writers may choose materially different
/// values. A domain version can instead be supplied explicitly.
pub struct LWWRegister<T> {
    node: ReplicaId,
    value: T,
    timestamp: i64,
    delta: Option<Box<LWWRegister<T>>>,
}

impl<T> LWWRegister<T>
where
    T: Clone + Eq,
{
    /// Creates a register with an explicit writer, value, and timestamp.
    pub fn new(node: impl Into<ReplicaId>, value: T, timestamp: i64) -> Self {
        Self {
            node: node.into(),
            value,
            timestamp,
            delta: None,
        }
    }

    /// Creates a register using [`default_lww_clock`] from timestamp zero.
    pub fn with_default_clock(node: impl Into<ReplicaId>, value: T) -> Self {
        Self::new(node, value, default_lww_clock(0))
    }

    /// Returns the replica that wrote the current value.
    pub fn node(&self) -> &ReplicaId {
        &self.node
    }

    /// Returns the current register value.
    pub fn value(&self) -> &T {
        &self.value
    }

    /// Returns the current ordering timestamp.
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    /// Replaces the value at an explicit timestamp and records it as a delta.
    pub fn with_value(&self, node: impl Into<ReplicaId>, value: T, timestamp: i64) -> Self {
        let next = Self::new(node, value, timestamp);
        Self {
            delta: Some(Box::new(next.clone())),
            ..next
        }
    }

    /// Replaces the value using a caller-supplied timestamp function.
    ///
    /// The function receives the current timestamp and proposed value, allowing
    /// domain versions or deterministic test clocks instead of wall time.
    pub fn with_value_by_clock(
        &self,
        node: impl Into<ReplicaId>,
        value: T,
        clock: impl FnOnce(i64, &T) -> i64,
    ) -> Self {
        let timestamp = clock(self.timestamp, &value);
        self.with_value(node, value, timestamp)
    }

    /// Replaces the value using the monotonically increasing default clock.
    pub fn with_value_default_clock(&self, node: impl Into<ReplicaId>, value: T) -> Self {
        self.with_value_by_clock(node, value, |timestamp, _| default_lww_clock(timestamp))
    }

    /// Replaces the value using the decreasing first-write-wins clock.
    pub fn with_value_reverse_clock(&self, node: impl Into<ReplicaId>, value: T) -> Self {
        self.with_value_by_clock(node, value, |timestamp, _| reverse_lww_clock(timestamp))
    }
}

impl<T> ReplicatedData for LWWRegister<T>
where
    T: Clone + Eq,
{
    fn merge(&self, other: &Self) -> Self {
        let winner = match self.timestamp.cmp(&other.timestamp) {
            std::cmp::Ordering::Greater => self,
            std::cmp::Ordering::Less => other,
            std::cmp::Ordering::Equal if self.node <= other.node => self,
            std::cmp::Ordering::Equal => other,
        };
        winner.reset_delta()
    }
}

impl<T> DeltaReplicatedData for LWWRegister<T>
where
    T: Clone + Eq,
{
    type Delta = Self;

    fn delta(&self) -> Option<Self::Delta> {
        self.delta.as_deref().cloned()
    }

    fn merge_delta(&self, delta: &Self::Delta) -> Self {
        self.merge(delta)
    }

    fn reset_delta(&self) -> Self {
        Self {
            node: self.node.clone(),
            value: self.value.clone(),
            timestamp: self.timestamp,
            delta: None,
        }
    }
}

impl<T> ReplicatedDelta for LWWRegister<T>
where
    T: Clone + Eq,
{
    type Full = Self;

    fn zero(&self) -> Self::Full {
        self.reset_delta()
    }
}

impl<T> RemovedNodePruning for LWWRegister<T>
where
    T: Clone + Eq,
{
    fn modified_by_replica_ids(&self) -> BTreeSet<ReplicaId> {
        BTreeSet::from([self.node.clone()])
    }

    fn need_pruning_from(&self, removed_replica: &ReplicaId) -> bool {
        &self.node == removed_replica
    }

    fn prune(
        &self,
        removed_replica: &ReplicaId,
        collapse_into: ReplicaId,
    ) -> Result<Self, CrdtError> {
        if self.need_pruning_from(removed_replica) {
            Ok(Self {
                node: collapse_into,
                value: self.value.clone(),
                timestamp: self.timestamp,
                delta: None,
            })
        } else {
            Ok(self.reset_delta())
        }
    }

    fn pruning_cleanup(&self, _removed_replica: &ReplicaId) -> Self {
        self.reset_delta()
    }
}

/// Returns the later of wall-clock milliseconds and `current_timestamp + 1`.
///
/// Saturating arithmetic avoids overflow at the `i64` boundary.
pub fn default_lww_clock(current_timestamp: i64) -> i64 {
    system_time_millis().max(current_timestamp.saturating_add(1))
}

/// Returns a decreasing timestamp for first-write-wins merge ordering.
///
/// The result is the earlier of negative wall-clock milliseconds and
/// `current_timestamp - 1`, avoiding overflow at the `i64` boundary.
pub fn reverse_lww_clock(current_timestamp: i64) -> i64 {
    system_time_millis()
        .saturating_neg()
        .min(current_timestamp.saturating_sub(1))
}

fn system_time_millis() -> i64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0);
    i64::try_from(millis).unwrap_or(i64::MAX)
}
