use crate::{Gossip, MemberStatus, UniqueAddress};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DowningDecision {
    NoAction,
    DownReachable,
    DownUnreachable,
    DownAll,
    DownSelfQuarantinedByRemote,
}

pub trait DowningHook {
    fn decide(&self, gossip: &Gossip, self_node: &UniqueAddress) -> DowningDecision;
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
pub struct DowningPlan {
    decision: DowningDecision,
    nodes_to_down: Vec<UniqueAddress>,
    down_self: bool,
}

impl DowningPlan {
    pub fn from_hook(hook: &impl DowningHook, gossip: &Gossip, self_node: &UniqueAddress) -> Self {
        Self::from_decision(hook.decide(gossip, self_node), gossip, self_node)
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

fn downable_nodes(gossip: &Gossip) -> Vec<UniqueAddress> {
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

    fn member(unique_address: UniqueAddress, status: MemberStatus) -> Member {
        Member::new(unique_address, Vec::new()).with_status(status)
    }

    fn node(system: &str, uid: u64) -> UniqueAddress {
        UniqueAddress::new(Address::local(system), uid)
    }
}
