use std::collections::{HashMap, HashSet};

use crate::{
    Convergence, Gossip, LeaderSelection, Member, MemberStatus, Reachability, UniqueAddress,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterEvent {
    Member(MemberEvent),
    Reachability(ReachabilityEvent),
    LeaderChanged {
        leader: Option<UniqueAddress>,
    },
    RoleLeaderChanged {
        role: String,
        leader: Option<UniqueAddress>,
    },
    SeenChanged {
        converged: bool,
        seen_by: HashSet<UniqueAddress>,
    },
    ReachabilityChanged {
        reachability: Reachability,
    },
    MemberTombstonesChanged {
        tombstones: HashSet<UniqueAddress>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemberEvent {
    Joined(Member),
    WeaklyUp(Member),
    Up(Member),
    Left(Member),
    Exited(Member),
    Downed(Member),
    Removed {
        member: Member,
        previous_status: MemberStatus,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReachabilityEvent {
    Unreachable(Member),
    Reachable(Member),
}

pub struct ClusterEvents;

impl ClusterEvents {
    pub fn diff(old: &Gossip, new: &Gossip, self_node: &UniqueAddress) -> Vec<ClusterEvent> {
        let mut events = Vec::new();

        events.extend(diff_tombstones(old, new));
        events.extend(diff_member_events(old, new));
        events.extend(diff_unreachable(old, new, self_node));
        events.extend(diff_reachable(old, new, self_node));
        events.extend(diff_leader(old, new, self_node));
        events.extend(diff_role_leaders(old, new, self_node));
        events.extend(diff_seen(old, new, self_node));
        events.extend(diff_reachability(old, new));

        events
    }
}

fn diff_tombstones(old: &Gossip, new: &Gossip) -> Vec<ClusterEvent> {
    if old.tombstones() == new.tombstones() {
        Vec::new()
    } else {
        vec![ClusterEvent::MemberTombstonesChanged {
            tombstones: new.tombstones().keys().cloned().collect(),
        }]
    }
}

fn diff_member_events(old: &Gossip, new: &Gossip) -> Vec<ClusterEvent> {
    let old_members = members_by_address(old);
    let new_members = members_by_address(new);
    let mut removed = Vec::new();
    let mut changed_or_added = Vec::new();

    for member in old_members.values() {
        if !new_members.contains_key(&member.unique_address) {
            removed.push(ClusterEvent::Member(MemberEvent::Removed {
                member: member.clone().with_status(MemberStatus::Removed),
                previous_status: member.status,
            }));
        }
    }

    for member in new_members.values() {
        match old_members.get(&member.unique_address) {
            None => {
                if let Some(event) = member_event_for_status(member.clone()) {
                    changed_or_added.push(ClusterEvent::Member(event));
                }
            }
            Some(old_member)
                if old_member.status != member.status
                    || old_member.up_number != member.up_number =>
            {
                if let Some(event) = member_event_for_status(member.clone()) {
                    changed_or_added.push(ClusterEvent::Member(event));
                }
            }
            Some(_) => {}
        }
    }

    removed.sort_by_key(event_node_key);
    changed_or_added.sort_by_key(event_node_key);
    removed.extend(changed_or_added);
    removed
}

fn diff_unreachable(old: &Gossip, new: &Gossip, self_node: &UniqueAddress) -> Vec<ClusterEvent> {
    let old_unreachable = old.reachability().all_unreachable_or_terminated();
    let mut events: Vec<_> = new
        .reachability()
        .all_unreachable_or_terminated()
        .into_iter()
        .filter(|node| node != self_node && !old_unreachable.contains(node))
        .filter_map(|node| {
            new.member(&node)
                .cloned()
                .map(|member| ClusterEvent::Reachability(ReachabilityEvent::Unreachable(member)))
        })
        .collect();
    events.sort_by_key(event_node_key);
    events
}

fn diff_reachable(old: &Gossip, new: &Gossip, self_node: &UniqueAddress) -> Vec<ClusterEvent> {
    let mut events: Vec<_> = old
        .reachability()
        .all_unreachable()
        .into_iter()
        .filter(|node| {
            node != self_node
                && new.reachability().status_of(node) == crate::ReachabilityStatus::Reachable
        })
        .filter_map(|node| {
            new.member(&node)
                .cloned()
                .map(|member| ClusterEvent::Reachability(ReachabilityEvent::Reachable(member)))
        })
        .collect();
    events.sort_by_key(event_node_key);
    events
}

fn diff_leader(old: &Gossip, new: &Gossip, self_node: &UniqueAddress) -> Vec<ClusterEvent> {
    let old_leader = LeaderSelection::for_gossip(old, self_node)
        .leader()
        .cloned();
    let new_leader = LeaderSelection::for_gossip(new, self_node)
        .leader()
        .cloned();
    if old_leader == new_leader {
        Vec::new()
    } else {
        vec![ClusterEvent::LeaderChanged { leader: new_leader }]
    }
}

fn diff_role_leaders(old: &Gossip, new: &Gossip, self_node: &UniqueAddress) -> Vec<ClusterEvent> {
    let mut roles = gossip_roles(old);
    roles.extend(gossip_roles(new));
    let mut events: Vec<_> = roles
        .into_iter()
        .filter_map(|role| {
            let old_leader = LeaderSelection::for_role(old, self_node, &role)
                .leader()
                .cloned();
            let new_leader = LeaderSelection::for_role(new, self_node, &role)
                .leader()
                .cloned();
            (old_leader != new_leader).then_some(ClusterEvent::RoleLeaderChanged {
                role,
                leader: new_leader,
            })
        })
        .collect();
    events.sort_by(|left, right| event_role(left).cmp(event_role(right)));
    events
}

fn diff_seen(old: &Gossip, new: &Gossip, self_node: &UniqueAddress) -> Vec<ClusterEvent> {
    let old_converged = Convergence::check(old, self_node).is_converged();
    let new_converged = Convergence::check(new, self_node).is_converged();
    if old_converged == new_converged && old.seen_by() == new.seen_by() {
        Vec::new()
    } else {
        vec![ClusterEvent::SeenChanged {
            converged: new_converged,
            seen_by: new.seen_by().clone(),
        }]
    }
}

fn diff_reachability(old: &Gossip, new: &Gossip) -> Vec<ClusterEvent> {
    if old.reachability() == new.reachability() {
        Vec::new()
    } else {
        vec![ClusterEvent::ReachabilityChanged {
            reachability: new.reachability().clone(),
        }]
    }
}

fn member_event_for_status(member: Member) -> Option<MemberEvent> {
    match member.status {
        MemberStatus::Joining => Some(MemberEvent::Joined(member)),
        MemberStatus::WeaklyUp => Some(MemberEvent::WeaklyUp(member)),
        MemberStatus::Up => Some(MemberEvent::Up(member)),
        MemberStatus::Leaving => Some(MemberEvent::Left(member)),
        MemberStatus::Exiting => Some(MemberEvent::Exited(member)),
        MemberStatus::Down => Some(MemberEvent::Downed(member)),
        MemberStatus::Removed => None,
    }
}

fn members_by_address(gossip: &Gossip) -> HashMap<UniqueAddress, Member> {
    gossip
        .members()
        .iter()
        .cloned()
        .map(|member| (member.unique_address.clone(), member))
        .collect()
}

fn gossip_roles(gossip: &Gossip) -> HashSet<String> {
    gossip
        .members()
        .iter()
        .flat_map(|member| member.roles.iter().cloned())
        .collect()
}

fn event_node_key(event: &ClusterEvent) -> String {
    match event {
        ClusterEvent::Member(event) => match event {
            MemberEvent::Joined(member)
            | MemberEvent::WeaklyUp(member)
            | MemberEvent::Up(member)
            | MemberEvent::Left(member)
            | MemberEvent::Exited(member)
            | MemberEvent::Downed(member) => member.unique_address.ordering_key(),
            MemberEvent::Removed { member, .. } => member.unique_address.ordering_key(),
        },
        ClusterEvent::Reachability(event) => match event {
            ReachabilityEvent::Unreachable(member) | ReachabilityEvent::Reachable(member) => {
                member.unique_address.ordering_key()
            }
        },
        ClusterEvent::LeaderChanged { leader } => leader
            .as_ref()
            .map(UniqueAddress::ordering_key)
            .unwrap_or_default(),
        ClusterEvent::RoleLeaderChanged { role, .. } => role.clone(),
        ClusterEvent::SeenChanged { .. }
        | ClusterEvent::ReachabilityChanged { .. }
        | ClusterEvent::MemberTombstonesChanged { .. } => String::new(),
    }
}

fn event_role(event: &ClusterEvent) -> &str {
    match event {
        ClusterEvent::RoleLeaderChanged { role, .. } => role,
        _ => "",
    }
}

#[cfg(test)]
mod tests {
    use kairo_actor::Address;

    use super::*;
    use crate::{LeaderActions, Reachability};

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
        let old = Gossip::from_members([member(node_a.clone(), MemberStatus::Joining)])
            .seen(node_a.clone());
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
}
