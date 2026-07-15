#![deny(missing_docs)]

mod lease_majority;
mod split_brain;

use crate::{Gossip, Member, MemberStatus, UniqueAddress};
use std::time::Duration;

pub use lease_majority::{
    LeaseMajorityHook, LeaseMajorityLease, LeaseMajoritySettings, LeaseMajoritySettingsError,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Abstract partition decision produced by a cluster downing policy.
pub enum DowningDecision {
    /// Leave all membership state unchanged.
    NoAction,
    /// Down the currently reachable side of the observed partition.
    DownReachable,
    /// Down the currently unreachable side of the observed partition.
    DownUnreachable,
    /// Down every member still eligible for downing.
    DownAll,
    /// Down nodes identified as indirectly connected by asymmetric observations.
    DownIndirectlyConnected,
    /// Apply the lease-denied reverse plan for an indirectly connected topology.
    ReverseDownIndirectlyConnected,
    /// Down only the local node after a remote quarantine observation.
    DownSelfQuarantinedByRemote,
}

/// Pluggable synchronous policy for deriving downing actions from gossip.
///
/// Implementations may consult external tie-breakers, but gossip and
/// reachability remain the source of membership and partition evidence.
pub trait DowningHook {
    /// Chooses an abstract decision for the current gossip view.
    fn decide(&self, gossip: &Gossip, self_node: &UniqueAddress) -> DowningDecision;

    /// Returns an additional delay after stable-after elapses.
    fn decision_delay(&self, _gossip: &Gossip, _self_node: &UniqueAddress) -> Duration {
        Duration::ZERO
    }

    /// Resolves the abstract decision into a deterministic node plan.
    fn plan(&self, gossip: &Gossip, self_node: &UniqueAddress) -> DowningPlan {
        DowningPlan::from_decision(self.decide(gossip, self_node), gossip, self_node)
    }
}

#[derive(Debug, Clone, Copy, Default)]
/// Downing policy that never changes membership.
pub struct NoDowning;

impl DowningHook for NoDowning {
    fn decide(&self, _gossip: &Gossip, _self_node: &UniqueAddress) -> DowningDecision {
        DowningDecision::NoAction
    }
}

#[derive(Debug, Clone, Copy)]
/// Test and integration hook that always returns one configured decision.
pub struct StaticDowningHook {
    decision: DowningDecision,
}

impl StaticDowningHook {
    /// Creates a hook that always returns `decision`.
    pub fn new(decision: DowningDecision) -> Self {
        Self { decision }
    }
}

impl DowningHook for StaticDowningHook {
    fn decide(&self, _gossip: &Gossip, _self_node: &UniqueAddress) -> DowningDecision {
        self.decision
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Supported synchronous split-brain resolution strategies.
pub enum SplitBrainStrategy {
    /// Down every eligible member on both sides.
    DownAll,
    /// Keep the larger role-filtered side, using the lowest address to break ties.
    KeepMajority {
        /// Optional role used when counting each side.
        role: Option<String>,
    },
    /// Keep the side containing the oldest role-filtered member.
    KeepOldest {
        /// Optional role used to select the oldest member.
        role: Option<String>,
        /// Whether an oldest member alone against at least two peers is downed.
        down_if_alone: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Pekko-aligned split-brain policy derived from gossip reachability.
pub struct SplitBrainResolverHook {
    strategy: SplitBrainStrategy,
}

impl SplitBrainResolverHook {
    /// Creates a strategy that downs every eligible member.
    pub fn down_all() -> Self {
        Self {
            strategy: SplitBrainStrategy::DownAll,
        }
    }

    /// Creates a role-aware keep-majority strategy.
    pub fn keep_majority(role: Option<String>) -> Self {
        Self {
            strategy: SplitBrainStrategy::KeepMajority { role },
        }
    }

    /// Creates a role-aware keep-oldest strategy.
    pub fn keep_oldest(role: Option<String>, down_if_alone: bool) -> Self {
        Self {
            strategy: SplitBrainStrategy::KeepOldest {
                role,
                down_if_alone,
            },
        }
    }

    /// Returns the configured strategy.
    pub fn strategy(&self) -> &SplitBrainStrategy {
        &self.strategy
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Deterministic set of node incarnations resolved from a downing decision.
pub struct DowningPlan {
    decision: DowningDecision,
    nodes_to_down: Vec<UniqueAddress>,
    down_self: bool,
}

impl DowningPlan {
    /// Builds a plan through `hook`, preserving hook-specific node selection.
    pub fn from_hook(hook: &impl DowningHook, gossip: &Gossip, self_node: &UniqueAddress) -> Self {
        hook.plan(gossip, self_node)
    }

    /// Resolves an abstract decision against the supplied gossip view.
    pub fn from_decision(
        decision: DowningDecision,
        gossip: &Gossip,
        self_node: &UniqueAddress,
    ) -> Self {
        let mut nodes_to_down = match decision {
            DowningDecision::NoAction => Vec::new(),
            DowningDecision::DownUnreachable => downable_nodes(gossip)
                .into_iter()
                .filter(|node| {
                    gossip
                        .reachability()
                        .all_unreachable_or_terminated()
                        .contains(node)
                })
                .collect(),
            DowningDecision::DownReachable => {
                let unreachable = gossip.reachability().all_unreachable_or_terminated();
                downable_nodes(gossip)
                    .into_iter()
                    .filter(|node| !unreachable.contains(node))
                    .collect()
            }
            DowningDecision::DownAll => downable_nodes(gossip),
            DowningDecision::DownIndirectlyConnected => {
                split_brain::nodes_to_down_for_decision(decision, gossip)
            }
            DowningDecision::ReverseDownIndirectlyConnected => {
                split_brain::nodes_to_down_for_decision(decision, gossip)
            }
            DowningDecision::DownSelfQuarantinedByRemote => {
                if gossip.has_member(self_node) {
                    vec![self_node.clone()]
                } else {
                    Vec::new()
                }
            }
        };
        nodes_to_down.sort_by_key(UniqueAddress::ordering_key);
        nodes_to_down.dedup();
        let down_self = nodes_to_down.iter().any(|node| node == self_node);

        Self::new(decision, nodes_to_down, down_self)
    }

    fn new(
        decision: DowningDecision,
        mut nodes_to_down: Vec<UniqueAddress>,
        down_self: bool,
    ) -> Self {
        nodes_to_down.sort_by_key(UniqueAddress::ordering_key);
        nodes_to_down.dedup();
        Self {
            decision,
            nodes_to_down,
            down_self,
        }
    }

    /// Returns the abstract decision that produced this plan.
    pub fn decision(&self) -> DowningDecision {
        self.decision
    }

    /// Returns the sorted, deduplicated node incarnations to down.
    pub fn nodes_to_down(&self) -> &[UniqueAddress] {
        &self.nodes_to_down
    }

    /// Returns whether the local node is included in the plan.
    pub fn down_self(&self) -> bool {
        self.down_self
    }

    /// Applies this plan to gossip as one local causal update.
    ///
    /// When any status changes, the result increments `self_node`'s vector-clock
    /// entry and clears the seen table except for the local node.
    pub fn apply_to(&self, gossip: &Gossip, self_node: &UniqueAddress) -> Gossip {
        let mut changed = gossip.clone();
        let mut did_change = false;

        for node in &self.nodes_to_down {
            if changed
                .member(node)
                .is_some_and(|member| member.status != MemberStatus::Down)
            {
                changed = changed.mark_down(node);
                did_change = true;
            }
        }

        if did_change {
            changed
                .increment_version(self_node)
                .only_seen(self_node.clone())
        } else {
            changed
        }
    }
}

impl DowningHook for SplitBrainResolverHook {
    fn decide(&self, gossip: &Gossip, _self_node: &UniqueAddress) -> DowningDecision {
        split_brain::decision(gossip, &self.strategy)
    }

    fn plan(&self, gossip: &Gossip, self_node: &UniqueAddress) -> DowningPlan {
        let decision = self.decide(gossip, self_node);
        if decision != DowningDecision::DownIndirectlyConnected {
            return DowningPlan::from_decision(decision, gossip, self_node);
        }

        let nodes_to_down = split_brain::indirectly_connected_nodes_to_down(gossip, &self.strategy)
            .into_iter()
            .collect::<Vec<_>>();
        let down_self = nodes_to_down.iter().any(|node| node == self_node);
        DowningPlan::new(decision, nodes_to_down, down_self)
    }
}

pub(super) fn downable_nodes(gossip: &Gossip) -> Vec<UniqueAddress> {
    gossip
        .members()
        .iter()
        .filter(|member| {
            !matches!(
                member.status,
                MemberStatus::Down | MemberStatus::Exiting | MemberStatus::Removed
            )
        })
        .map(|member| member.unique_address.clone())
        .collect()
}

pub(super) fn keep_majority_decision(gossip: &Gossip, role: &Option<String>) -> DowningDecision {
    let members = decision_members(gossip, role);
    if members.is_empty() {
        return DowningDecision::DownAll;
    }

    let unreachable = gossip.reachability().all_unreachable_or_terminated();
    let reachable_size = members
        .iter()
        .filter(|member| !unreachable.contains(&member.unique_address))
        .count();
    let unreachable_size = members.len() - reachable_size;
    let lowest = members
        .iter()
        .min_by_key(|member| member.unique_address.ordering_key())
        .expect("members is not empty");

    majority_decision(reachable_size, unreachable_size, &unreachable, lowest)
}

fn majority_decision(
    reachable_size: usize,
    unreachable_size: usize,
    unreachable: &std::collections::HashSet<UniqueAddress>,
    lowest: &Member,
) -> DowningDecision {
    if reachable_size == unreachable_size {
        if unreachable.contains(&lowest.unique_address) {
            DowningDecision::DownReachable
        } else {
            DowningDecision::DownUnreachable
        }
    } else if reachable_size > unreachable_size {
        DowningDecision::DownUnreachable
    } else {
        DowningDecision::DownReachable
    }
}

pub(super) fn keep_oldest_decision(
    gossip: &Gossip,
    role: &Option<String>,
    down_if_alone: bool,
) -> DowningDecision {
    let members = decision_members(gossip, role);
    if members.is_empty() {
        return DowningDecision::DownAll;
    }

    let unreachable = gossip.reachability().all_unreachable_or_terminated();
    let oldest = members
        .iter()
        .min_by_key(|member| member_age_key(member))
        .expect("members is not empty");
    let oldest_is_reachable = !unreachable.contains(&oldest.unique_address);
    let reachable_count = members
        .iter()
        .filter(|member| !unreachable.contains(&member.unique_address))
        .count();
    let unreachable_count = members.len() - reachable_count;

    if oldest_is_reachable {
        if down_if_alone && reachable_count == 1 && unreachable_count >= 2 {
            DowningDecision::DownReachable
        } else {
            DowningDecision::DownUnreachable
        }
    } else if down_if_alone && unreachable_count == 1 && reachable_count >= 2 {
        DowningDecision::DownUnreachable
    } else {
        DowningDecision::DownReachable
    }
}

pub(super) fn decision_members(gossip: &Gossip, role: &Option<String>) -> Vec<Member> {
    let mut members: Vec<_> = gossip
        .members()
        .iter()
        .filter(|member| {
            !matches!(
                member.status,
                MemberStatus::Joining
                    | MemberStatus::WeaklyUp
                    | MemberStatus::Down
                    | MemberStatus::Exiting
                    | MemberStatus::Removed
            )
        })
        .filter(|member| role.as_ref().is_none_or(|role| member.has_role(role)))
        .cloned()
        .collect();
    members.sort_by_key(|member| member.unique_address.ordering_key());
    members
}

fn member_age_key(member: &Member) -> (u64, String) {
    (
        member.up_number.unwrap_or(u64::MAX),
        member.unique_address.ordering_key(),
    )
}

#[cfg(test)]
mod tests;
