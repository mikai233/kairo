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
    let state = empty_state_with_self(self_node.clone()).init([node_b.clone(), node_c.clone()], []);

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
