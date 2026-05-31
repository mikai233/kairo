mod split_brain;

use crate::{Gossip, Member, MemberStatus, UniqueAddress};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DowningDecision {
    NoAction,
    DownReachable,
    DownUnreachable,
    DownAll,
    DownIndirectlyConnected,
    DownSelfQuarantinedByRemote,
}

pub trait DowningHook {
    fn decide(&self, gossip: &Gossip, self_node: &UniqueAddress) -> DowningDecision;

    fn plan(&self, gossip: &Gossip, self_node: &UniqueAddress) -> DowningPlan {
        DowningPlan::from_decision(self.decide(gossip, self_node), gossip, self_node)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct NoDowning;

impl DowningHook for NoDowning {
    fn decide(&self, _gossip: &Gossip, _self_node: &UniqueAddress) -> DowningDecision {
        DowningDecision::NoAction
    }
}

#[derive(Debug, Clone, Copy)]
pub struct StaticDowningHook {
    decision: DowningDecision,
}

impl StaticDowningHook {
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
pub enum SplitBrainStrategy {
    DownAll,
    KeepMajority {
        role: Option<String>,
    },
    KeepOldest {
        role: Option<String>,
        down_if_alone: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SplitBrainResolverHook {
    strategy: SplitBrainStrategy,
}

impl SplitBrainResolverHook {
    pub fn down_all() -> Self {
        Self {
            strategy: SplitBrainStrategy::DownAll,
        }
    }

    pub fn keep_majority(role: Option<String>) -> Self {
        Self {
            strategy: SplitBrainStrategy::KeepMajority { role },
        }
    }

    pub fn keep_oldest(role: Option<String>, down_if_alone: bool) -> Self {
        Self {
            strategy: SplitBrainStrategy::KeepOldest {
                role,
                down_if_alone,
            },
        }
    }

    pub fn strategy(&self) -> &SplitBrainStrategy {
        &self.strategy
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DowningPlan {
    decision: DowningDecision,
    nodes_to_down: Vec<UniqueAddress>,
    down_self: bool,
}

impl DowningPlan {
    pub fn from_hook(hook: &impl DowningHook, gossip: &Gossip, self_node: &UniqueAddress) -> Self {
        hook.plan(gossip, self_node)
    }

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

    pub fn decision(&self) -> DowningDecision {
        self.decision
    }

    pub fn nodes_to_down(&self) -> &[UniqueAddress] {
        &self.nodes_to_down
    }

    pub fn down_self(&self) -> bool {
        self.down_self
    }

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

fn decision_members(gossip: &Gossip, role: &Option<String>) -> Vec<Member> {
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
mod tests {
    use std::collections::HashSet;

    use kairo_actor::Address;

    use super::*;
    use crate::{Member, Reachability};

    #[test]
    fn no_downing_hook_produces_no_action_plan() {
        let node_a = node("a", 1);
        let gossip = Gossip::from_members([member(node_a.clone(), MemberStatus::Up)]);

        let plan = DowningPlan::from_hook(&NoDowning, &gossip, &node_a);

        assert_eq!(plan.decision(), DowningDecision::NoAction);
        assert!(plan.nodes_to_down().is_empty());
        assert!(!plan.down_self());
    }

    #[test]
    fn down_unreachable_targets_only_unreachable_downable_members() {
        let node_a = node("a", 1);
        let node_b = node("b", 2);
        let node_c = node("c", 3);
        let gossip = Gossip::from_members([
            member(node_a.clone(), MemberStatus::Up),
            member(node_b.clone(), MemberStatus::Up),
            member(node_c.clone(), MemberStatus::Exiting),
        ])
        .with_reachability(
            Reachability::new()
                .unreachable(node_a.clone(), node_b.clone())
                .unreachable(node_a.clone(), node_c),
        );

        let plan = DowningPlan::from_decision(DowningDecision::DownUnreachable, &gossip, &node_a);

        assert_eq!(plan.nodes_to_down(), &[node_b]);
        assert!(!plan.down_self());
    }

    #[test]
    fn down_reachable_targets_reachable_downable_members() {
        let node_a = node("a", 1);
        let node_b = node("b", 2);
        let node_c = node("c", 3);
        let gossip = Gossip::from_members([
            member(node_a.clone(), MemberStatus::Up),
            member(node_b.clone(), MemberStatus::Up),
            member(node_c.clone(), MemberStatus::Joining),
        ])
        .with_reachability(Reachability::new().unreachable(node_a.clone(), node_b));

        let plan = DowningPlan::from_decision(DowningDecision::DownReachable, &gossip, &node_a);

        assert_eq!(plan.nodes_to_down(), &[node_a.clone(), node_c]);
        assert!(plan.down_self());
    }

    #[test]
    fn down_all_excludes_members_already_down_or_exiting() {
        let node_a = node("a", 1);
        let node_b = node("b", 2);
        let node_c = node("c", 3);
        let gossip = Gossip::from_members([
            member(node_a.clone(), MemberStatus::Up),
            member(node_b, MemberStatus::Down),
            member(node_c, MemberStatus::Exiting),
        ]);

        let plan = DowningPlan::from_decision(DowningDecision::DownAll, &gossip, &node_a);

        assert_eq!(plan.nodes_to_down(), &[node_a]);
    }

    #[test]
    fn down_self_quarantined_by_remote_targets_only_self() {
        let node_a = node("a", 1);
        let node_b = node("b", 2);
        let gossip = Gossip::from_members([
            member(node_a.clone(), MemberStatus::Up),
            member(node_b, MemberStatus::Up),
        ]);

        let plan = DowningPlan::from_decision(
            DowningDecision::DownSelfQuarantinedByRemote,
            &gossip,
            &node_a,
        );

        assert_eq!(plan.nodes_to_down(), &[node_a]);
        assert!(plan.down_self());
    }

    #[test]
    fn applying_plan_marks_nodes_down_resets_seen_and_stamps_version() {
        let node_a = node("a", 1);
        let node_b = node("b", 2);
        let gossip = Gossip::from_members([
            member(node_a.clone(), MemberStatus::Up),
            member(node_b.clone(), MemberStatus::Up),
        ])
        .seen(node_b.clone());
        let plan = DowningPlan::from_decision(
            DowningDecision::DownUnreachable,
            &gossip
                .with_reachability(Reachability::new().unreachable(node_a.clone(), node_b.clone())),
            &node_a,
        );

        let changed = plan.apply_to(&gossip, &node_a);

        assert_eq!(
            changed.member(&node_b).map(|member| member.status),
            Some(MemberStatus::Down)
        );
        assert_eq!(changed.seen_by(), &HashSet::from([node_a.clone()]));
        assert_eq!(
            changed.version().get(&crate::VectorClockNode::new(format!(
                "{}-{}",
                node_a.address, node_a.uid
            ))),
            1
        );
    }

    #[test]
    fn split_brain_down_all_strategy_downs_all_downable_members() {
        let node_a = node("a", 1);
        let node_b = node("b", 2);
        let hook = SplitBrainResolverHook::down_all();
        let gossip = Gossip::from_members([
            member(node_a.clone(), MemberStatus::Up),
            member(node_b.clone(), MemberStatus::Joining),
        ]);

        let plan = DowningPlan::from_hook(&hook, &gossip, &node_a);

        assert_eq!(plan.decision(), DowningDecision::DownAll);
        assert_eq!(plan.nodes_to_down(), &[node_a, node_b]);
    }

    #[test]
    fn keep_majority_downs_unreachable_when_self_side_is_larger() {
        let node_a = node("a", 1);
        let node_b = node("b", 2);
        let node_c = node("c", 3);
        let gossip = Gossip::from_members([
            member(node_a.clone(), MemberStatus::Up),
            member(node_b.clone(), MemberStatus::Up),
            member(node_c.clone(), MemberStatus::Up),
        ])
        .with_reachability(Reachability::new().unreachable(node_a.clone(), node_c.clone()));
        let hook = SplitBrainResolverHook::keep_majority(None);

        let plan = DowningPlan::from_hook(&hook, &gossip, &node_a);

        assert_eq!(plan.decision(), DowningDecision::DownUnreachable);
        assert_eq!(plan.nodes_to_down(), &[node_c]);
    }

    #[test]
    fn keep_majority_downs_reachable_on_minority_side() {
        let node_a = node("a", 1);
        let node_b = node("b", 2);
        let node_c = node("c", 3);
        let gossip = Gossip::from_members([
            member(node_a.clone(), MemberStatus::Up),
            member(node_b.clone(), MemberStatus::Up),
            member(node_c.clone(), MemberStatus::Up),
        ])
        .with_reachability(
            Reachability::new()
                .unreachable(node_a.clone(), node_b)
                .unreachable(node_a.clone(), node_c),
        );
        let hook = SplitBrainResolverHook::keep_majority(None);

        let plan = DowningPlan::from_hook(&hook, &gossip, &node_a);

        assert_eq!(plan.decision(), DowningDecision::DownReachable);
        assert_eq!(plan.nodes_to_down(), &[node_a]);
        assert!(plan.down_self());
    }

    #[test]
    fn keep_majority_tie_keeps_lowest_address_side() {
        let node_a = node("a", 1);
        let node_b = node("b", 2);
        let gossip = Gossip::from_members([
            member(node_a.clone(), MemberStatus::Up),
            member(node_b.clone(), MemberStatus::Up),
        ])
        .with_reachability(Reachability::new().unreachable(node_b.clone(), node_a.clone()));
        let hook = SplitBrainResolverHook::keep_majority(None);

        let plan = DowningPlan::from_hook(&hook, &gossip, &node_b);

        assert_eq!(plan.decision(), DowningDecision::DownReachable);
        assert_eq!(plan.nodes_to_down(), &[node_b]);
    }

    #[test]
    fn keep_majority_role_counts_only_members_with_role() {
        let node_a = node("a", 1);
        let node_b = node("b", 2);
        let node_c = node("c", 3);
        let gossip = Gossip::from_members([
            member_with_roles(node_a.clone(), MemberStatus::Up, ["backend"]),
            member_with_roles(node_b.clone(), MemberStatus::Up, ["frontend"]),
            member_with_roles(node_c.clone(), MemberStatus::Up, ["backend"]),
        ])
        .with_reachability(Reachability::new().unreachable(node_a.clone(), node_c.clone()));
        let hook = SplitBrainResolverHook::keep_majority(Some("backend".to_string()));

        let plan = DowningPlan::from_hook(&hook, &gossip, &node_a);

        assert_eq!(plan.decision(), DowningDecision::DownUnreachable);
        assert_eq!(plan.nodes_to_down(), &[node_c]);
    }

    #[test]
    fn keep_majority_with_no_matching_role_downs_all_downable_members() {
        let node_a = node("a", 1);
        let gossip = Gossip::from_members([member(node_a.clone(), MemberStatus::Up)]);
        let hook = SplitBrainResolverHook::keep_majority(Some("missing".to_string()));

        let plan = DowningPlan::from_hook(&hook, &gossip, &node_a);

        assert_eq!(plan.decision(), DowningDecision::DownAll);
        assert_eq!(plan.nodes_to_down(), &[node_a]);
    }

    #[test]
    fn keep_majority_downs_indirectly_connected_cycle() {
        let node_a = node("a", 1);
        let node_b = node("b", 2);
        let node_c = node("c", 3);
        let gossip = Gossip::from_members([
            member(node_a.clone(), MemberStatus::Up),
            member(node_b.clone(), MemberStatus::Up),
            member(node_c, MemberStatus::Up),
        ])
        .with_reachability(
            Reachability::new()
                .unreachable(node_a.clone(), node_b.clone())
                .unreachable(node_b.clone(), node_a.clone()),
        );
        let hook = SplitBrainResolverHook::keep_majority(None);

        let plan = DowningPlan::from_hook(&hook, &gossip, &node_a);

        assert_eq!(plan.decision(), DowningDecision::DownIndirectlyConnected);
        assert_eq!(plan.nodes_to_down(), &[node_a, node_b]);
        assert!(plan.down_self());
    }

    #[test]
    fn keep_majority_combines_indirect_cycle_with_clean_partition_decision() {
        let node_a = node("a", 1);
        let node_b = node("b", 2);
        let node_c = node("c", 3);
        let node_d = node("d", 4);
        let gossip = Gossip::from_members([
            member(node_a.clone(), MemberStatus::Up),
            member(node_b.clone(), MemberStatus::Up),
            member(node_c, MemberStatus::Up),
            member(node_d.clone(), MemberStatus::Up),
        ])
        .with_reachability(
            Reachability::new()
                .unreachable(node_a.clone(), node_b.clone())
                .unreachable(node_b.clone(), node_a.clone())
                .unreachable(node_a.clone(), node_d.clone()),
        );
        let hook = SplitBrainResolverHook::keep_majority(None);

        let plan = DowningPlan::from_hook(&hook, &gossip, &node_a);

        assert_eq!(plan.decision(), DowningDecision::DownIndirectlyConnected);
        assert_eq!(plan.nodes_to_down(), &[node_a, node_b, node_d]);
    }

    #[test]
    fn keep_oldest_treats_seen_unreachable_node_as_indirectly_connected() {
        let node_a = node("a", 1);
        let node_b = node("b", 2);
        let node_c = node("c", 3);
        let gossip = Gossip::from_members([
            member_with_age(node_a.clone(), MemberStatus::Up, 1),
            member_with_age(node_b.clone(), MemberStatus::Up, 2),
            member_with_age(node_c, MemberStatus::Up, 3),
        ])
        .with_reachability(Reachability::new().unreachable(node_a.clone(), node_b.clone()))
        .seen(node_b.clone());
        let hook = SplitBrainResolverHook::keep_oldest(None, false);

        let plan = DowningPlan::from_hook(&hook, &gossip, &node_a);

        assert_eq!(plan.decision(), DowningDecision::DownIndirectlyConnected);
        assert_eq!(plan.nodes_to_down(), &[node_a, node_b]);
        assert!(plan.down_self());
    }

    #[test]
    fn keep_oldest_keeps_side_with_oldest_member() {
        let node_a = node("a", 1);
        let node_b = node("b", 2);
        let node_c = node("c", 3);
        let gossip = Gossip::from_members([
            member_with_age(node_a.clone(), MemberStatus::Up, 1),
            member_with_age(node_b.clone(), MemberStatus::Up, 2),
            member_with_age(node_c.clone(), MemberStatus::Up, 3),
        ])
        .with_reachability(
            Reachability::new()
                .unreachable(node_b.clone(), node_a.clone())
                .unreachable(node_b.clone(), node_c.clone()),
        );
        let hook = SplitBrainResolverHook::keep_oldest(None, false);

        let plan = DowningPlan::from_hook(&hook, &gossip, &node_b);

        assert_eq!(plan.decision(), DowningDecision::DownReachable);
        assert_eq!(plan.nodes_to_down(), &[node_b]);
    }

    #[test]
    fn keep_oldest_down_if_alone_downs_oldest_when_it_is_alone_against_larger_side() {
        let node_a = node("a", 1);
        let node_b = node("b", 2);
        let node_c = node("c", 3);
        let gossip = Gossip::from_members([
            member_with_age(node_a.clone(), MemberStatus::Up, 1),
            member_with_age(node_b.clone(), MemberStatus::Up, 2),
            member_with_age(node_c.clone(), MemberStatus::Up, 3),
        ])
        .with_reachability(
            Reachability::new()
                .unreachable(node_a.clone(), node_b)
                .unreachable(node_a.clone(), node_c),
        );
        let hook = SplitBrainResolverHook::keep_oldest(None, true);

        let plan = DowningPlan::from_hook(&hook, &gossip, &node_a);

        assert_eq!(plan.decision(), DowningDecision::DownReachable);
        assert_eq!(plan.nodes_to_down(), &[node_a]);
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

    fn member_with_age(
        unique_address: UniqueAddress,
        status: MemberStatus,
        up_number: u64,
    ) -> Member {
        Member::new(unique_address, Vec::new())
            .with_status(status)
            .with_up_number(up_number)
    }

    fn node(system: &str, uid: u64) -> UniqueAddress {
        UniqueAddress::new(Address::local(system), uid)
    }
}
