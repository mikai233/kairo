#![deny(missing_docs)]

use std::collections::{HashMap, HashSet};

use crate::{ClusterEvent, Gossip, LeaderSelection, Member, UniqueAddress};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Initial-state mode for subscribers that receive raw [`ClusterEvent`] values.
pub enum SubscriptionInitialState {
    /// Deliver only events published after subscription.
    None,
    /// Replay the current gossip view as domain events before live delivery.
    Events,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Initial-state mode for [`ClusterSubscriptionEvent`] subscribers.
pub enum ClusterSubscriptionInitialState {
    /// Deliver only events published after subscription.
    None,
    /// Deliver one [`CurrentClusterState`] before live events.
    Snapshot,
    /// Replay the current gossip view as domain events before live delivery.
    Events,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Snapshot-or-event stream used by the high-level cluster subscription API.
pub enum ClusterSubscriptionEvent {
    /// Full state emitted when a subscription requests snapshot initialization.
    CurrentState(CurrentClusterState),
    /// One domain event from a state transition.
    Event(ClusterEvent),
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Operator-visible snapshot derived from the latest gossip state.
pub struct CurrentClusterState {
    /// All live members in deterministic gossip order.
    pub members: Vec<Member>,
    /// Unreachable or terminated members, excluding the local node.
    pub unreachable: Vec<Member>,
    /// Node incarnations that have seen the latest gossip version.
    pub seen_by: HashSet<UniqueAddress>,
    /// Current deterministic cluster leader, if any.
    pub leader: Option<UniqueAddress>,
    /// Current deterministic leader for every advertised role.
    pub role_leaders: HashMap<String, Option<UniqueAddress>>,
    /// Removed node incarnations retained as gossip tombstones.
    pub member_tombstones: HashSet<UniqueAddress>,
}

impl CurrentClusterState {
    /// Derives a full public snapshot from `gossip` as observed by `self_node`.
    pub fn from_gossip(gossip: &Gossip, self_node: &UniqueAddress) -> Self {
        let unreachable = gossip
            .reachability()
            .all_unreachable_or_terminated()
            .into_iter()
            .filter(|node| node != self_node)
            .filter_map(|node| gossip.member(&node).cloned())
            .collect();
        let roles: HashSet<_> = gossip
            .members()
            .iter()
            .flat_map(|member| member.roles.iter().cloned())
            .collect();
        let role_leaders = roles
            .into_iter()
            .map(|role| {
                let leader = LeaderSelection::for_role(gossip, self_node, &role)
                    .leader()
                    .cloned();
                (role, leader)
            })
            .collect();

        Self {
            members: gossip.members().to_vec(),
            unreachable,
            seen_by: gossip.seen_by().clone(),
            leader: LeaderSelection::for_gossip(gossip, self_node)
                .leader()
                .cloned(),
            role_leaders,
            member_tombstones: gossip.tombstones().keys().cloned().collect(),
        }
    }
}
