use kairo_actor::Address;

use super::*;
use crate::{Gossip, LeaderActions, Member, MemberStatus, Reachability, UniqueAddress};

#[test]
fn diff_emits_removed_members_before_changed_members() {
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let old = Gossip::from_members([
        member(node_a.clone(), MemberStatus::Up),
        member(node_b.clone(), MemberStatus::Exiting),
    ])
    .seen(node_a.clone());
    let new = LeaderActions::on_convergence(&old, &node_a, 11, [node_b.clone()])
        .unwrap()
        .gossip;

    let events = ClusterEvents::diff(&old, &new, &node_a);

    assert!(matches!(
        events.first(),
        Some(ClusterEvent::MemberTombstonesChanged { .. })
    ));
    assert!(matches!(
        events.get(1),
        Some(ClusterEvent::Member(MemberEvent::Removed {
            previous_status: MemberStatus::Exiting,
            ..
        }))
    ));
}

#[test]
fn diff_emits_member_event_for_status_or_age_change() {
    let node_a = node("a", 1);
    let old =
        Gossip::from_members([member(node_a.clone(), MemberStatus::Joining)]).seen(node_a.clone());
    let new = LeaderActions::on_convergence(&old, &node_a, 11, [])
        .unwrap()
        .gossip;

    let events = ClusterEvents::diff(&old, &new, &node_a);

    assert!(events.iter().any(|event| {
        matches!(
            event,
            ClusterEvent::Member(MemberEvent::Up(member))
                if member.unique_address == node_a && member.up_number == Some(1)
        )
    }));
}

#[test]
fn diff_emits_unreachable_and_reachable_events_for_subject_changes() {
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let base = Gossip::from_members([
        member(node_a.clone(), MemberStatus::Up),
        member(node_b.clone(), MemberStatus::Up),
    ])
    .seen(node_a.clone())
    .seen(node_b.clone());
    let unreachable =
        base.with_reachability(Reachability::new().unreachable(node_a.clone(), node_b.clone()));

    let unreachable_events = ClusterEvents::diff(&base, &unreachable, &node_a);
    let reachable_events = ClusterEvents::diff(&unreachable, &base, &node_a);

    assert!(unreachable_events.iter().any(|event| {
        matches!(
            event,
            ClusterEvent::Reachability(ReachabilityEvent::Unreachable(member))
                if member.unique_address == node_b
        )
    }));
    assert!(reachable_events.iter().any(|event| {
        matches!(
            event,
            ClusterEvent::Reachability(ReachabilityEvent::Reachable(member))
                if member.unique_address == node_b
        )
    }));
}

#[test]
fn diff_excludes_self_reachability_events() {
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let base = Gossip::from_members([
        member(node_a.clone(), MemberStatus::Up),
        member(node_b.clone(), MemberStatus::Up),
    ]);
    let unreachable =
        base.with_reachability(Reachability::new().unreachable(node_b, node_a.clone()));

    let events = ClusterEvents::diff(&base, &unreachable, &node_a);

    assert!(!events.iter().any(|event| {
        matches!(
            event,
            ClusterEvent::Reachability(ReachabilityEvent::Unreachable(member))
                if member.unique_address == node_a
        )
    }));
}

#[test]
fn diff_emits_leader_and_role_leader_changes() {
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let old = Gossip::from_members([
        member_with_roles(node_a.clone(), MemberStatus::Up, ["backend"]),
        member_with_roles(node_b.clone(), MemberStatus::Down, ["backend"]),
    ]);
    let new =
        old.update_members([
            member_with_roles(node_b.clone(), MemberStatus::Up, ["backend"]).with_up_number(0),
        ]);

    let events = ClusterEvents::diff(&old, &new, &node_a);

    assert!(events.iter().any(|event| {
        matches!(
            event,
            ClusterEvent::LeaderChanged { leader: Some(leader) } if *leader == node_b
        )
    }));
    assert!(events.iter().any(|event| {
        matches!(
            event,
            ClusterEvent::RoleLeaderChanged { role, leader: Some(leader) }
                if role == "backend" && *leader == node_b
        )
    }));
}

#[test]
fn diff_emits_seen_and_reachability_summary_changes() {
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let old = Gossip::from_members([
        member(node_a.clone(), MemberStatus::Up),
        member(node_b.clone(), MemberStatus::Up),
    ])
    .seen(node_a.clone());
    let new = old.seen(node_b.clone());

    let events = ClusterEvents::diff(&old, &new, &node_a);

    assert!(events.iter().any(|event| {
        matches!(
            event,
            ClusterEvent::SeenChanged { converged: true, seen_by }
                if seen_by.contains(&node_a) && seen_by.contains(&node_b)
        )
    }));
}

fn member(unique_address: UniqueAddress, status: MemberStatus) -> Member {
    Member::new(unique_address, Vec::new()).with_status(status)
}

fn member_with_roles(
    unique_address: UniqueAddress,
    status: MemberStatus,
    roles: impl IntoIterator<Item = &'static str>,
) -> Member {
    Member::new(
        unique_address,
        roles.into_iter().map(String::from).collect(),
    )
    .with_status(status)
}

fn node(system: &str, uid: u64) -> UniqueAddress {
    UniqueAddress::new(Address::local(system), uid)
}
