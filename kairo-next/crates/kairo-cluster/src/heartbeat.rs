#![deny(missing_docs)]

use std::{collections::HashSet, time::Duration};

use crate::{DeadlineFailureDetectorSettings, FailureDetectorRegistry, UniqueAddress};

/// Invalid heartbeat-ring or sender-state configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeartbeatError {
    /// The ring's node set omitted the local node.
    MissingSelfAddress,
    /// The configured number of monitoring members was zero.
    ZeroMonitoredByNrOfMembers,
}

/// Immutable deterministic ring used to choose heartbeat receivers.
///
/// Nodes are shuffled by a fixed hash of their unique-address ordering key so
/// monitoring does not simply follow physical or lexical address adjacency.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatNodeRing {
    self_node: UniqueAddress,
    nodes: HashSet<UniqueAddress>,
    unreachable: HashSet<UniqueAddress>,
    monitored_by_nr_of_members: usize,
}

impl HeartbeatNodeRing {
    /// Creates a validated heartbeat receiver ring.
    ///
    /// # Errors
    ///
    /// Returns an error when the monitoring count is zero or `nodes` omits
    /// `self_node`.
    pub fn new(
        self_node: UniqueAddress,
        nodes: impl IntoIterator<Item = UniqueAddress>,
        unreachable: impl IntoIterator<Item = UniqueAddress>,
        monitored_by_nr_of_members: usize,
    ) -> Result<Self, HeartbeatError> {
        if monitored_by_nr_of_members == 0 {
            return Err(HeartbeatError::ZeroMonitoredByNrOfMembers);
        }
        let nodes: HashSet<_> = nodes.into_iter().collect();
        if !nodes.contains(&self_node) {
            return Err(HeartbeatError::MissingSelfAddress);
        }
        Ok(Self {
            self_node,
            nodes,
            unreachable: unreachable.into_iter().collect(),
            monitored_by_nr_of_members,
        })
    }

    /// Returns the local unique address anchoring this ring view.
    pub fn self_node(&self) -> &UniqueAddress {
        &self.self_node
    }

    /// Returns all current ring members, including self.
    pub fn nodes(&self) -> &HashSet<UniqueAddress> {
        &self.nodes
    }

    /// Returns members currently marked unreachable in this ring view.
    pub fn unreachable(&self) -> &HashSet<UniqueAddress> {
        &self.unreachable
    }

    /// Returns the receivers selected for the local node.
    pub fn my_receivers(&self) -> HashSet<UniqueAddress> {
        self.receivers(&self.self_node)
    }

    /// Returns the deterministic receiver set for `sender`.
    ///
    /// Selection walks forward around the ring until the configured number of
    /// reachable receivers is chosen. Intermediate unreachable nodes are also
    /// retained so simultaneous failures remain monitored for leader cleanup.
    pub fn receivers(&self, sender: &UniqueAddress) -> HashSet<UniqueAddress> {
        let ring = self.sorted_ring();
        if self.monitored_by_nr_of_members >= ring.len().saturating_sub(1) {
            return ring.into_iter().filter(|node| node != sender).collect();
        }

        let Some(sender_index) = ring.iter().position(|node| node == sender) else {
            return HashSet::new();
        };

        let ordered = ring
            .iter()
            .cycle()
            .skip(sender_index + 1)
            .take(ring.len() - 1);
        let mut reachable_remaining = self.monitored_by_nr_of_members;
        let mut selected = HashSet::new();

        for node in ordered {
            if reachable_remaining == 0 {
                break;
            }
            let is_unreachable = self.unreachable.contains(node);
            if is_unreachable && selected.len() >= self.monitored_by_nr_of_members {
                continue;
            }
            selected.insert(node.clone());
            if !is_unreachable {
                reachable_remaining -= 1;
            }
        }

        selected
    }

    /// Returns a ring containing `node`.
    pub fn add_node(&self, node: UniqueAddress) -> Self {
        if self.nodes.contains(&node) {
            return self.clone();
        }
        let mut changed = self.clone();
        changed.nodes.insert(node);
        changed
    }

    /// Returns a ring without `node` or its unreachable marker.
    pub fn remove_node(&self, node: &UniqueAddress) -> Self {
        if !self.nodes.contains(node) && !self.unreachable.contains(node) {
            return self.clone();
        }
        let mut changed = self.clone();
        changed.nodes.remove(node);
        changed.unreachable.remove(node);
        changed
    }

    /// Returns a ring that marks `node` unreachable.
    pub fn with_unreachable(&self, node: UniqueAddress) -> Self {
        let mut changed = self.clone();
        changed.unreachable.insert(node);
        changed
    }

    /// Returns a ring that clears `node`'s unreachable marker.
    pub fn with_reachable(&self, node: &UniqueAddress) -> Self {
        let mut changed = self.clone();
        changed.unreachable.remove(node);
        changed
    }

    fn sorted_ring(&self) -> Vec<UniqueAddress> {
        let mut nodes: Vec<_> = self.nodes.iter().cloned().collect();
        nodes.sort_by_key(heartbeat_ring_key);
        nodes
    }
}

/// Immutable heartbeat-sender policy state with cloned failure-detector state.
///
/// Receivers that rotate out while unavailable remain active until a heartbeat
/// proves recovery, preventing a ring change from forgetting a failed node.
#[derive(Debug, Clone)]
pub struct HeartbeatSenderState {
    ring: HeartbeatNodeRing,
    old_receivers_now_unreachable: HashSet<UniqueAddress>,
    failure_detector: FailureDetectorRegistry<UniqueAddress>,
}

impl HeartbeatSenderState {
    /// Creates sender state containing only the local node.
    ///
    /// # Errors
    ///
    /// Returns an error when `monitored_by_nr_of_members` is zero.
    pub fn new(
        self_node: UniqueAddress,
        monitored_by_nr_of_members: usize,
        failure_detector_settings: DeadlineFailureDetectorSettings,
    ) -> Result<Self, HeartbeatError> {
        let ring = HeartbeatNodeRing::new(
            self_node.clone(),
            [self_node],
            HashSet::new(),
            monitored_by_nr_of_members,
        )?;
        Ok(Self {
            ring,
            old_receivers_now_unreachable: HashSet::new(),
            failure_detector: FailureDetectorRegistry::new(failure_detector_settings),
        })
    }

    /// Returns the current heartbeat ring.
    pub fn ring(&self) -> &HeartbeatNodeRing {
        &self.ring
    }

    /// Returns former receivers retained because they are still unavailable.
    pub fn old_receivers_now_unreachable(&self) -> &HashSet<UniqueAddress> {
        &self.old_receivers_now_unreachable
    }

    /// Returns the current per-member failure-detector registry.
    pub fn failure_detector(&self) -> &FailureDetectorRegistry<UniqueAddress> {
        &self.failure_detector
    }

    /// Returns current ring receivers plus unavailable receivers rotated out of the ring.
    pub fn active_receivers(&self) -> HashSet<UniqueAddress> {
        let mut active = self.ring.my_receivers();
        active.extend(self.old_receivers_now_unreachable.iter().cloned());
        active
    }

    /// Returns whether `node` is a current ring member.
    pub fn contains(&self, node: &UniqueAddress) -> bool {
        self.ring.nodes.contains(node)
    }

    /// Replaces the membership and unreachable snapshot while retaining detector history.
    ///
    /// The local node is always inserted into the resulting ring.
    pub fn init(
        &self,
        nodes: impl IntoIterator<Item = UniqueAddress>,
        unreachable: impl IntoIterator<Item = UniqueAddress>,
    ) -> Self {
        let mut nodes: HashSet<_> = nodes.into_iter().collect();
        nodes.insert(self.ring.self_node.clone());
        Self {
            ring: HeartbeatNodeRing {
                self_node: self.ring.self_node.clone(),
                nodes,
                unreachable: unreachable.into_iter().collect(),
                monitored_by_nr_of_members: self.ring.monitored_by_nr_of_members,
            },
            old_receivers_now_unreachable: self.old_receivers_now_unreachable.clone(),
            failure_detector: self.failure_detector.clone(),
        }
    }

    /// Returns state after adding one member and reconciling receiver ownership.
    pub fn add_member(&self, node: UniqueAddress, now: Duration) -> Self {
        self.membership_change(self.ring.add_node(node), now)
    }

    /// Returns state after removing a member and all of its detector history.
    pub fn remove_member(&self, node: &UniqueAddress, now: Duration) -> Self {
        let mut changed = self.membership_change(self.ring.remove_node(node), now);
        changed.failure_detector.remove(node);
        changed.old_receivers_now_unreachable.remove(node);
        changed
    }

    /// Returns state after marking a member unreachable and reconciling receivers.
    pub fn unreachable_member(&self, node: UniqueAddress, now: Duration) -> Self {
        self.membership_change(self.ring.with_unreachable(node), now)
    }

    /// Returns state after marking a member reachable and reconciling receivers.
    pub fn reachable_member(&self, node: &UniqueAddress, now: Duration) -> Self {
        self.membership_change(self.ring.with_reachable(node), now)
    }

    /// Records a response from an active receiver at monotonic time `now`.
    ///
    /// A recovered former receiver is released when it is no longer selected by
    /// the current ring.
    pub fn heartbeat_response(&self, from: &UniqueAddress, now: Duration) -> Self {
        if !self.active_receivers().contains(from) {
            return self.clone();
        }

        let mut changed = self.clone();
        changed.failure_detector.heartbeat(from.clone(), now);
        if changed.old_receivers_now_unreachable.remove(from)
            && !changed.ring.my_receivers().contains(from)
        {
            changed.failure_detector.remove(from);
        }
        changed
    }

    /// Starts failure detection when an active receiver's first response is overdue.
    ///
    /// Inactive or already monitored receivers leave the state unchanged.
    pub fn trigger_expected_first_heartbeat(&self, from: &UniqueAddress, now: Duration) -> Self {
        if !self.active_receivers().contains(from) || self.failure_detector.is_monitoring(from) {
            return self.clone();
        }

        let mut changed = self.clone();
        changed.failure_detector.heartbeat(from.clone(), now);
        changed
    }

    /// Clears all detector history and retained former receivers.
    pub fn reset_failure_detector(&self) -> Self {
        let mut changed = self.clone();
        changed.failure_detector.reset();
        changed.old_receivers_now_unreachable.clear();
        changed
    }

    fn membership_change(&self, new_ring: HeartbeatNodeRing, now: Duration) -> Self {
        let old_receivers = self.ring.my_receivers();
        let new_receivers = new_ring.my_receivers();
        let removed_receivers: HashSet<_> =
            old_receivers.difference(&new_receivers).cloned().collect();
        let mut old_receivers_now_unreachable = self.old_receivers_now_unreachable.clone();
        let mut failure_detector = self.failure_detector.clone();

        for node in removed_receivers {
            if failure_detector.is_available(&node, now) {
                failure_detector.remove(&node);
            } else {
                old_receivers_now_unreachable.insert(node);
            }
        }

        Self {
            ring: new_ring,
            old_receivers_now_unreachable,
            failure_detector,
        }
    }
}

fn heartbeat_ring_key(node: &UniqueAddress) -> (u64, String) {
    (
        stable_hash64(node.ordering_key().as_bytes()),
        node.ordering_key(),
    )
}

fn stable_hash64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

#[cfg(test)]
mod tests;
