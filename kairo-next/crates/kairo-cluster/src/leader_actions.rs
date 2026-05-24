use std::collections::HashSet;

use crate::{Convergence, ConvergenceBlocker, Gossip, Member, MemberStatus, UniqueAddress};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaderActionOutcome {
    pub gossip: Gossip,
    pub changed_members: Vec<Member>,
    pub removed_members: Vec<UniqueAddress>,
}

impl LeaderActionOutcome {
    pub fn changed(&self) -> bool {
        !self.changed_members.is_empty() || !self.removed_members.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaderActionError {
    NotConverged { blockers: Vec<ConvergenceBlocker> },
}

pub struct LeaderActions;

impl LeaderActions {
    pub fn on_convergence(
        gossip: &Gossip,
        self_node: &UniqueAddress,
        removal_timestamp: u64,
        exiting_confirmed: impl IntoIterator<Item = UniqueAddress>,
    ) -> Result<LeaderActionOutcome, LeaderActionError> {
        let exiting_confirmed: HashSet<_> = exiting_confirmed.into_iter().collect();
        let convergence =
            Convergence::check_with_exiting_confirmed(gossip, self_node, exiting_confirmed.clone());
        if !convergence.is_converged() {
            return Err(LeaderActionError::NotConverged {
                blockers: convergence.blockers().to_vec(),
            });
        }

        let unreachable = gossip.reachability().all_unreachable_or_terminated();
        let removed_members: Vec<_> = gossip
            .members()
            .iter()
            .filter(|member| {
                member.status.can_skip_unreachable_for_convergence()
                    && (unreachable.contains(&member.unique_address)
                        || (member.status == MemberStatus::Exiting
                            && exiting_confirmed.contains(&member.unique_address)))
            })
            .map(|member| member.unique_address.clone())
            .collect();

        let changed_members = changed_members(gossip);

        if removed_members.is_empty() && changed_members.is_empty() {
            return Ok(LeaderActionOutcome {
                gossip: gossip.clone(),
                changed_members,
                removed_members,
            });
        }

        let mut changed_gossip = gossip.update_members(changed_members.clone());
        for node in &removed_members {
            changed_gossip = changed_gossip.remove(node, removal_timestamp);
        }
        changed_gossip = changed_gossip
            .increment_version(self_node)
            .only_seen(self_node.clone());

        Ok(LeaderActionOutcome {
            gossip: changed_gossip,
            changed_members,
            removed_members,
        })
    }
}

fn changed_members(gossip: &Gossip) -> Vec<Member> {
    let mut next_up_number = next_up_number(gossip);
    let mut changed = Vec::new();

    for member in gossip.members() {
        match member.status {
            MemberStatus::Joining | MemberStatus::WeaklyUp => {
                let updated = member
                    .clone()
                    .with_status(MemberStatus::Up)
                    .with_up_number(next_up_number);
                next_up_number += 1;
                changed.push(updated);
            }
            MemberStatus::Leaving => {
                changed.push(member.clone().with_status(MemberStatus::Exiting));
            }
            MemberStatus::Up
            | MemberStatus::Exiting
            | MemberStatus::Down
            | MemberStatus::Removed => {}
        }
    }

    changed
}

fn next_up_number(gossip: &Gossip) -> u64 {
    gossip
        .members()
        .iter()
        .filter_map(|member| member.up_number)
        .max()
        .unwrap_or(0)
        + 1
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use kairo_actor::Address;

    use super::*;
    use crate::Reachability;

    #[test]
    fn rejects_actions_when_gossip_has_not_converged() {
        let node_a = node("a", 1);
        let node_b = node("b", 2);
        let gossip = Gossip::from_members([
            member(node_a.clone(), MemberStatus::Up, Some(1)),
            member(node_b.clone(), MemberStatus::Up, Some(2)),
        ])
        .seen(node_a.clone());

        let error = LeaderActions::on_convergence(&gossip, &node_a, 10, []).unwrap_err();

        assert!(matches!(error, LeaderActionError::NotConverged { .. }));
    }

    #[test]
    fn promotes_joining_and_weakly_up_members_with_next_up_numbers() {
        let node_a = node("a", 1);
        let node_b = node("b", 2);
        let node_c = node("c", 3);
        let gossip = Gossip::from_members([
            member(node_a.clone(), MemberStatus::Up, Some(4)),
            member(node_b.clone(), MemberStatus::Joining, None),
            member(node_c.clone(), MemberStatus::WeaklyUp, None),
        ])
        .seen(node_a.clone());

        let outcome = LeaderActions::on_convergence(&gossip, &node_a, 10, []).unwrap();

        let node_b = outcome.gossip.member(&node_b).unwrap();
        let node_c = outcome.gossip.member(&node_c).unwrap();
        assert_eq!(node_b.status, MemberStatus::Up);
        assert_eq!(node_b.up_number, Some(5));
        assert_eq!(node_c.status, MemberStatus::Up);
        assert_eq!(node_c.up_number, Some(6));
        assert_eq!(outcome.gossip.seen_by(), &HashSet::from([node_a]));
    }

    #[test]
    fn moves_leaving_members_to_exiting_on_convergence() {
        let node_a = node("a", 1);
        let node_b = node("b", 2);
        let gossip = Gossip::from_members([
            member(node_a.clone(), MemberStatus::Up, Some(1)),
            member(node_b.clone(), MemberStatus::Leaving, Some(2)),
        ])
        .seen(node_a.clone())
        .seen(node_b.clone());

        let outcome = LeaderActions::on_convergence(&gossip, &node_a, 10, []).unwrap();

        assert_eq!(
            outcome.gossip.member(&node_b).unwrap().status,
            MemberStatus::Exiting
        );
    }

    #[test]
    fn removes_unreachable_down_and_exiting_members() {
        let node_a = node("a", 1);
        let node_b = node("b", 2);
        let node_c = node("c", 3);
        let gossip = Gossip::from_members([
            member(node_a.clone(), MemberStatus::Up, Some(1)),
            member(node_b.clone(), MemberStatus::Down, Some(2)),
            member(node_c.clone(), MemberStatus::Exiting, Some(3)),
        ])
        .seen(node_a.clone())
        .with_reachability(
            Reachability::new()
                .unreachable(node_a.clone(), node_b.clone())
                .terminated(node_a.clone(), node_c.clone()),
        );

        let outcome = LeaderActions::on_convergence(&gossip, &node_a, 10, []).unwrap();

        assert!(!outcome.gossip.has_member(&node_b));
        assert!(!outcome.gossip.has_member(&node_c));
        assert_eq!(outcome.gossip.tombstones().get(&node_b), Some(&10));
        assert_eq!(outcome.gossip.tombstones().get(&node_c), Some(&10));
    }

    #[test]
    fn removes_confirmed_exiting_members_without_reachability_record() {
        let node_a = node("a", 1);
        let node_b = node("b", 2);
        let gossip = Gossip::from_members([
            member(node_a.clone(), MemberStatus::Up, Some(1)),
            member(node_b.clone(), MemberStatus::Exiting, Some(2)),
        ])
        .seen(node_a.clone());

        let outcome =
            LeaderActions::on_convergence(&gossip, &node_a, 10, [node_b.clone()]).unwrap();

        assert!(!outcome.gossip.has_member(&node_b));
        assert_eq!(outcome.gossip.tombstones().get(&node_b), Some(&10));
    }

    fn member(
        unique_address: UniqueAddress,
        status: MemberStatus,
        up_number: Option<u64>,
    ) -> Member {
        let member = Member::new(unique_address, Vec::new()).with_status(status);
        if let Some(up_number) = up_number {
            member.with_up_number(up_number)
        } else {
            member
        }
    }

    fn node(system: &str, uid: u64) -> UniqueAddress {
        UniqueAddress::new(Address::local(system), uid)
    }
}
