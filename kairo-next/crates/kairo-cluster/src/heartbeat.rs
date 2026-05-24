use std::{collections::HashSet, time::Duration};

use crate::{DeadlineFailureDetectorSettings, FailureDetectorRegistry, UniqueAddress};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeartbeatError {
    MissingSelfAddress,
    ZeroMonitoredByNrOfMembers,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatNodeRing {
    self_node: UniqueAddress,
    nodes: HashSet<UniqueAddress>,
    unreachable: HashSet<UniqueAddress>,
    monitored_by_nr_of_members: usize,
}

impl HeartbeatNodeRing {
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

    pub fn self_node(&self) -> &UniqueAddress {
        &self.self_node
    }

    pub fn nodes(&self) -> &HashSet<UniqueAddress> {
        &self.nodes
    }

    pub fn unreachable(&self) -> &HashSet<UniqueAddress> {
        &self.unreachable
    }

    pub fn my_receivers(&self) -> HashSet<UniqueAddress> {
        self.receivers(&self.self_node)
    }

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

    pub fn add_node(&self, node: UniqueAddress) -> Self {
        if self.nodes.contains(&node) {
            return self.clone();
        }
        let mut changed = self.clone();
        changed.nodes.insert(node);
        changed
    }

    pub fn remove_node(&self, node: &UniqueAddress) -> Self {
        if !self.nodes.contains(node) && !self.unreachable.contains(node) {
            return self.clone();
        }
        let mut changed = self.clone();
        changed.nodes.remove(node);
        changed.unreachable.remove(node);
        changed
    }

    pub fn with_unreachable(&self, node: UniqueAddress) -> Self {
        let mut changed = self.clone();
        changed.unreachable.insert(node);
        changed
    }

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

#[derive(Debug, Clone)]
pub struct HeartbeatSenderState {
    ring: HeartbeatNodeRing,
    old_receivers_now_unreachable: HashSet<UniqueAddress>,
    failure_detector: FailureDetectorRegistry<UniqueAddress>,
}

impl HeartbeatSenderState {
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

    pub fn ring(&self) -> &HeartbeatNodeRing {
        &self.ring
    }

    pub fn old_receivers_now_unreachable(&self) -> &HashSet<UniqueAddress> {
        &self.old_receivers_now_unreachable
    }

    pub fn failure_detector(&self) -> &FailureDetectorRegistry<UniqueAddress> {
        &self.failure_detector
    }

    pub fn active_receivers(&self) -> HashSet<UniqueAddress> {
        let mut active = self.ring.my_receivers();
        active.extend(self.old_receivers_now_unreachable.iter().cloned());
        active
    }

    pub fn contains(&self, node: &UniqueAddress) -> bool {
        self.ring.nodes.contains(node)
    }

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

    pub fn add_member(&self, node: UniqueAddress, now: Duration) -> Self {
        self.membership_change(self.ring.add_node(node), now)
    }

    pub fn remove_member(&self, node: &UniqueAddress, now: Duration) -> Self {
        let mut changed = self.membership_change(self.ring.remove_node(node), now);
        changed.failure_detector.remove(node);
        changed.old_receivers_now_unreachable.remove(node);
        changed
    }

    pub fn unreachable_member(&self, node: UniqueAddress, now: Duration) -> Self {
        self.membership_change(self.ring.with_unreachable(node), now)
    }

    pub fn reachable_member(&self, node: &UniqueAddress, now: Duration) -> Self {
        self.membership_change(self.ring.with_reachable(node), now)
    }

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

    pub fn trigger_expected_first_heartbeat(&self, from: &UniqueAddress, now: Duration) -> Self {
        if !self.active_receivers().contains(from) || self.failure_detector.is_monitoring(from) {
            return self.clone();
        }

        let mut changed = self.clone();
        changed.failure_detector.heartbeat(from.clone(), now);
        changed
    }

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
mod tests {
    use std::{collections::HashSet, time::Duration};

    use kairo_actor::Address;

    use super::*;
    use crate::DeadlineFailureDetectorSettings;

    #[test]
    fn active_receivers_empty_when_only_self_exists() {
        assert!(empty_state("self").active_receivers().is_empty());
    }

    #[test]
    fn init_adds_self_and_uses_members_as_receivers() {
        let self_node = node("a", 1);
        let node_b = node("b", 2);
        let node_c = node("c", 3);
        let state =
            empty_state_with_self(self_node.clone()).init([node_b.clone(), node_c.clone()], []);

        assert!(state.contains(&self_node));
        assert_eq!(state.active_receivers(), HashSet::from([node_b, node_c]));
    }

    #[test]
    fn active_receivers_use_configured_limit() {
        let self_node = node("a", 1);
        let node_b = node("b", 2);
        let node_c = node("c", 3);
        let node_d = node("d", 4);
        let state = HeartbeatSenderState::new(self_node.clone(), 2, settings())
            .unwrap()
            .init(
                [
                    self_node.clone(),
                    node_b.clone(),
                    node_c.clone(),
                    node_d.clone(),
                ],
                [],
            );

        assert_eq!(state.active_receivers().len(), 2);
        assert!(!state.active_receivers().contains(&self_node));
    }

    #[test]
    fn unreachable_receivers_are_included_beyond_reachable_limit() {
        let self_node = node("a", 1);
        let mut state = HeartbeatSenderState::new(self_node.clone(), 2, settings())
            .unwrap()
            .init(
                [
                    self_node.clone(),
                    node("b", 2),
                    node("c", 3),
                    node("d", 4),
                    node("e", 5),
                ],
                [],
            );
        let selected = state.active_receivers();
        let unreachable = selected.iter().next().cloned().unwrap();

        state = state.unreachable_member(unreachable.clone(), ms(0));

        assert!(state.active_receivers().contains(&unreachable));
        assert!(state.active_receivers().len() >= 2);
    }

    #[test]
    fn membership_change_keeps_removed_unavailable_receivers_active() {
        let (changed, removed_receiver) = state_with_removed_unavailable_receiver();

        assert!(
            changed
                .old_receivers_now_unreachable()
                .contains(&removed_receiver)
        );
        assert!(changed.active_receivers().contains(&removed_receiver));
    }

    #[test]
    fn heartbeat_response_removes_old_receiver_after_recovery() {
        let (changed, removed_receiver) = state_with_removed_unavailable_receiver();

        let recovered = changed.heartbeat_response(&removed_receiver, ms(4_100));

        assert!(
            !recovered
                .old_receivers_now_unreachable()
                .contains(&removed_receiver)
        );
        if !recovered.ring().my_receivers().contains(&removed_receiver) {
            assert!(
                !recovered
                    .failure_detector()
                    .is_monitoring(&removed_receiver)
            );
        }
    }

    #[test]
    fn remove_member_forgets_failure_detector_and_old_unreachable_state() {
        let (state, removed_receiver) = state_with_removed_unavailable_receiver();

        let changed = state.remove_member(&removed_receiver, ms(4_100));

        assert!(!changed.active_receivers().contains(&removed_receiver));
        assert!(
            !changed
                .old_receivers_now_unreachable()
                .contains(&removed_receiver)
        );
        assert!(!changed.failure_detector().is_monitoring(&removed_receiver));
    }

    #[test]
    fn expected_first_heartbeat_starts_monitoring_active_receiver() {
        let self_node = node("a", 1);
        let node_b = node("b", 2);
        let state = HeartbeatSenderState::new(self_node, 2, settings())
            .unwrap()
            .add_member(node_b.clone(), ms(0));

        let changed = state.trigger_expected_first_heartbeat(&node_b, ms(500));

        assert!(changed.failure_detector().is_monitoring(&node_b));
        assert_eq!(
            changed
                .failure_detector()
                .detector(&node_b)
                .and_then(|detector| detector.latest_heartbeat_at()),
            Some(ms(500))
        );
    }

    #[test]
    fn expected_first_heartbeat_ignores_inactive_receiver() {
        let state = empty_state("a");
        let node_b = node("b", 2);

        let changed = state.trigger_expected_first_heartbeat(&node_b, ms(500));

        assert!(!changed.failure_detector().is_monitoring(&node_b));
    }

    #[test]
    fn reset_failure_detector_forgets_all_monitored_receivers() {
        let self_node = node("a", 1);
        let node_b = node("b", 2);
        let state = HeartbeatSenderState::new(self_node, 2, settings())
            .unwrap()
            .add_member(node_b.clone(), ms(0))
            .heartbeat_response(&node_b, ms(100));

        let changed = state.reset_failure_detector();

        assert!(!changed.failure_detector().is_monitoring(&node_b));
        assert!(changed.old_receivers_now_unreachable().is_empty());
    }

    #[test]
    fn ring_rejects_missing_self_and_zero_monitor_count() {
        let self_node = node("a", 1);

        assert_eq!(
            HeartbeatNodeRing::new(self_node.clone(), [], [], 1),
            Err(HeartbeatError::MissingSelfAddress)
        );
        assert_eq!(
            HeartbeatNodeRing::new(self_node.clone(), [self_node], [], 0),
            Err(HeartbeatError::ZeroMonitoredByNrOfMembers)
        );
    }

    fn empty_state(system: &str) -> HeartbeatSenderState {
        empty_state_with_self(node(system, 1))
    }

    fn empty_state_with_self(self_node: UniqueAddress) -> HeartbeatSenderState {
        HeartbeatSenderState::new(self_node, 3, settings()).unwrap()
    }

    fn state_with_removed_unavailable_receiver() -> (HeartbeatSenderState, UniqueAddress) {
        let self_node = node("a", 1);
        let node_b = node("b", 2);
        let node_c = node("c", 3);
        let state = HeartbeatSenderState::new(self_node, 2, settings())
            .unwrap()
            .add_member(node_b.clone(), ms(0))
            .add_member(node_c.clone(), ms(0))
            .heartbeat_response(&node_b, ms(0))
            .heartbeat_response(&node_c, ms(0));
        let old_receivers = state.ring().my_receivers();
        let unavailable_at = ms(4_000);
        assert!(
            old_receivers
                .iter()
                .all(|node| !state.failure_detector().is_available(node, unavailable_at))
        );

        for uid in 4..64 {
            let changed = state.add_member(node(&format!("n{uid}"), uid), unavailable_at);
            if let Some(removed_receiver) = old_receivers
                .difference(&changed.ring().my_receivers())
                .next()
                .cloned()
            {
                return (changed, removed_receiver);
            }
        }

        panic!("expected a candidate member to rotate an unavailable receiver out of the ring");
    }

    fn settings() -> DeadlineFailureDetectorSettings {
        DeadlineFailureDetectorSettings::new(ms(1_000), ms(3_000)).unwrap()
    }

    fn ms(value: u64) -> Duration {
        Duration::from_millis(value)
    }

    fn node(system: &str, uid: u64) -> UniqueAddress {
        UniqueAddress::new(Address::local(system), uid)
    }
}
