#![deny(missing_docs)]

use std::collections::{HashMap, HashSet};

use super::{ClusterEvent, MemberEvent, ReachabilityEvent};
use crate::{
    Convergence, Gossip, LeaderSelection, Member, MemberStatus, ReachabilityStatus, UniqueAddress,
};

/// Derives ordered cluster-domain events from immutable gossip snapshots.
pub struct ClusterEvents;

impl ClusterEvents {
    /// Returns observable changes from `old` to `new` in publication order.
    ///
    /// Tombstones are reported first, followed by removed members, member
    /// transitions, unreachable and reachable members, leader changes, seen
    /// convergence, and the complete reachability-table change. Events within
    /// an unordered member or role set are sorted deterministically.
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
            node != self_node && new.reachability().status_of(node) == ReachabilityStatus::Reachable
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
