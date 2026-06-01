use std::collections::{HashMap, HashSet};

use crate::{ClusterEvent, Gossip, LeaderSelection, Member, UniqueAddress};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriptionInitialState {
    None,
    Events,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterSubscriptionInitialState {
    None,
    Snapshot,
    Events,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterSubscriptionEvent {
    CurrentState(CurrentClusterState),
    Event(ClusterEvent),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentClusterState {
    pub members: Vec<Member>,
    pub unreachable: Vec<Member>,
    pub seen_by: HashSet<UniqueAddress>,
    pub leader: Option<UniqueAddress>,
    pub role_leaders: HashMap<String, Option<UniqueAddress>>,
    pub member_tombstones: HashSet<UniqueAddress>,
}

impl CurrentClusterState {
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
