use kairo_actor::Address;
use kairo_cluster::{ClusterEvent, Member, MemberEvent, MemberStatus, UniqueAddress};

use crate::{SingletonOldestChange, SingletonOldestTracker, SingletonScope};

#[test]
fn singleton_oldest_tracker_filters_by_role_and_initial_age() {
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let node_c = node("c", 3);

    let (_tracker, observation) = SingletonOldestTracker::from_members(
        node_c.clone(),
        SingletonScope::for_role("backend"),
        [
            member_with_roles(node_a.clone(), MemberStatus::Up, 1, ["backend"]),
            member_with_roles(node_b, MemberStatus::Up, 2, ["frontend"]),
            member_with_roles(node_c.clone(), MemberStatus::Up, 3, ["backend"]),
            Member::new(node("joining", 4), vec!["backend".to_string()])
                .with_status(MemberStatus::Joining),
        ],
    );

    assert_eq!(observation.oldest(), Some(&node_a));
    assert_eq!(
        observation.older_or_self(),
        &[node_a.clone(), node_c.clone()]
    );
    assert!(observation.safe_to_be_oldest());
}

#[test]
fn singleton_oldest_tracker_marks_takeover_unsafe_when_older_member_is_leaving() {
    let node_a = node("a", 1);
    let node_b = node("b", 2);

    let (_tracker, observation) = SingletonOldestTracker::from_members(
        node_b,
        SingletonScope::all(),
        [
            member(node_a.clone(), MemberStatus::Leaving, 1),
            member(node("b", 2), MemberStatus::Up, 2),
        ],
    );

    assert_eq!(observation.oldest(), Some(&node_a));
    assert!(!observation.safe_to_be_oldest());
}

#[test]
fn singleton_oldest_tracker_reports_oldest_change_for_member_up_and_removed() {
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let node_c = node("c", 3);

    let (mut tracker, _observation) = SingletonOldestTracker::from_members(
        node_c.clone(),
        SingletonScope::all(),
        [
            member(node_b.clone(), MemberStatus::Up, 2),
            member(node_c, MemberStatus::Up, 3),
        ],
    );

    assert_eq!(
        tracker.apply_cluster_event(&ClusterEvent::Member(MemberEvent::Up(member(
            node_a.clone(),
            MemberStatus::Up,
            1,
        )))),
        Some(SingletonOldestChange::OldestChanged(Some(node_a.clone())))
    );
    assert_eq!(tracker.current_oldest(), Some(&node_a));

    assert_eq!(
        tracker.apply_member_event(&MemberEvent::Removed {
            member: member(node_a.clone(), MemberStatus::Removed, 1),
            previous_status: MemberStatus::Up,
        }),
        Some(SingletonOldestChange::OldestChanged(Some(node_b.clone())))
    );
    assert_eq!(tracker.current_oldest(), Some(&node_b));
}

#[test]
fn singleton_oldest_tracker_ignores_self_exited_and_non_matching_role() {
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let node_c = node("c", 3);

    let (mut tracker, _observation) = SingletonOldestTracker::from_members(
        node_b.clone(),
        SingletonScope::for_role("backend"),
        [member_with_roles(
            node_b.clone(),
            MemberStatus::Up,
            2,
            ["backend"],
        )],
    );

    assert_eq!(
        tracker.apply_member_event(&MemberEvent::Up(member_with_roles(
            node_a,
            MemberStatus::Up,
            1,
            ["frontend"],
        ))),
        None
    );
    assert_eq!(tracker.current_oldest(), Some(&node_b));

    assert_eq!(
        tracker.apply_member_event(&MemberEvent::Exited(member_with_roles(
            node_b.clone(),
            MemberStatus::Exiting,
            2,
            ["backend"],
        ))),
        None
    );
    assert_eq!(tracker.current_oldest(), Some(&node_b));

    assert_eq!(
        tracker.apply_member_event(&MemberEvent::Up(member_with_roles(
            node_c.clone(),
            MemberStatus::Up,
            3,
            ["backend"],
        ))),
        None
    );
    assert_eq!(
        tracker
            .members_by_age()
            .iter()
            .map(|member| member.unique_address.clone())
            .collect::<Vec<_>>(),
        vec![node_b, node_c]
    );
}

fn member(unique_address: UniqueAddress, status: MemberStatus, up_number: u64) -> Member {
    Member::new(unique_address, Vec::new())
        .with_status(status)
        .with_up_number(up_number)
}

fn member_with_roles(
    unique_address: UniqueAddress,
    status: MemberStatus,
    up_number: u64,
    roles: impl IntoIterator<Item = &'static str>,
) -> Member {
    Member::new(
        unique_address,
        roles.into_iter().map(String::from).collect(),
    )
    .with_status(status)
    .with_up_number(up_number)
}

fn node(system: &str, uid: u64) -> UniqueAddress {
    UniqueAddress::new(Address::local(system), uid)
}
