use std::collections::HashSet;

use crate::{Gossip, ReachabilityStatus, UniqueAddress};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Convergence {
    blockers: Vec<ConvergenceBlocker>,
}

impl Convergence {
    pub fn check(gossip: &Gossip, self_node: &UniqueAddress) -> Self {
        Self::check_with_exiting_confirmed(gossip, self_node, HashSet::new())
    }

    pub fn check_with_exiting_confirmed(
        gossip: &Gossip,
        self_node: &UniqueAddress,
        exiting_confirmed: impl IntoIterator<Item = UniqueAddress>,
    ) -> Self {
        let exiting_confirmed: HashSet<_> = exiting_confirmed.into_iter().collect();
        let first_converging_member = !gossip
            .members()
            .iter()
            .any(|member| member.status.participates_in_convergence());
        let mut blockers = Vec::new();

        for member in gossip.members() {
            let participates = if first_converging_member {
                member.status.participates_in_first_convergence()
            } else {
                member.status.participates_in_convergence()
            };
            if participates
                && !gossip.seen_by().contains(&member.unique_address)
                && !exiting_confirmed.contains(&member.unique_address)
            {
                blockers.push(ConvergenceBlocker::NotSeen {
                    node: member.unique_address.clone(),
                    status: member.status,
                });
            }
        }

        let down_observers: HashSet<_> = gossip
            .members()
            .iter()
            .filter(|member| !member.status.observes_convergence_reachability())
            .map(|member| member.unique_address.clone())
            .collect();
        let live_members: HashSet<_> = gossip
            .members()
            .iter()
            .map(|member| member.unique_address.clone())
            .collect();

        let unreachable_subjects: HashSet<_> = gossip
            .reachability()
            .records()
            .iter()
            .filter(|record| {
                !down_observers.contains(&record.observer)
                    && live_members.contains(&record.subject)
                    && record.subject != *self_node
                    && !exiting_confirmed.contains(&record.subject)
                    && matches!(
                        record.status,
                        ReachabilityStatus::Unreachable | ReachabilityStatus::Terminated
                    )
            })
            .map(|record| record.subject.clone())
            .collect();

        for node in unreachable_subjects {
            if let Some(member) = gossip.member(&node)
                && !member.status.can_skip_unreachable_for_convergence()
            {
                blockers.push(ConvergenceBlocker::Unreachable {
                    node,
                    status: member.status,
                });
            }
        }

        Self { blockers }
    }

    pub fn is_converged(&self) -> bool {
        self.blockers.is_empty()
    }

    pub fn blockers(&self) -> &[ConvergenceBlocker] {
        &self.blockers
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConvergenceBlocker {
    NotSeen {
        node: UniqueAddress,
        status: crate::MemberStatus,
    },
    Unreachable {
        node: UniqueAddress,
        status: crate::MemberStatus,
    },
}

#[cfg(test)]
mod tests {
    use kairo_actor::Address;

    use super::*;
    use crate::{Member, MemberStatus, Reachability};

    #[test]
    fn converges_when_all_up_and_leaving_members_have_seen_the_gossip() {
        let node_a = node("a", 1);
        let node_b = node("b", 2);
        let node_c = node("c", 3);
        let gossip = Gossip::from_members([
            member(node_a.clone(), MemberStatus::Up),
            member(node_b.clone(), MemberStatus::Leaving),
            member(node_c.clone(), MemberStatus::Joining),
        ])
        .seen(node_a.clone())
        .seen(node_b.clone());

        assert!(Convergence::check(&gossip, &node_a).is_converged());
    }

    #[test]
    fn first_convergence_requires_joining_and_weakly_up_members_to_see_gossip() {
        let node_a = node("a", 1);
        let node_b = node("b", 2);
        let gossip = Gossip::from_members([
            member(node_a.clone(), MemberStatus::Joining),
            member(node_b.clone(), MemberStatus::WeaklyUp),
        ])
        .seen(node_a.clone());

        let convergence = Convergence::check(&gossip, &node_a);

        assert_eq!(
            convergence.blockers(),
            &[ConvergenceBlocker::NotSeen {
                node: node_b,
                status: MemberStatus::WeaklyUp
            }]
        );
    }

    #[test]
    fn confirmed_exiting_member_does_not_block_seen_convergence() {
        let node_a = node("a", 1);
        let node_b = node("b", 2);
        let gossip = Gossip::from_members([
            member(node_a.clone(), MemberStatus::Up),
            member(node_b.clone(), MemberStatus::Leaving),
        ])
        .seen(node_a.clone());

        assert!(
            Convergence::check_with_exiting_confirmed(&gossip, &node_a, [node_b]).is_converged()
        );
    }

    #[test]
    fn unreachable_up_member_blocks_convergence() {
        let node_a = node("a", 1);
        let node_b = node("b", 2);
        let gossip = Gossip::from_members([
            member(node_a.clone(), MemberStatus::Up),
            member(node_b.clone(), MemberStatus::Up),
        ])
        .seen(node_a.clone())
        .seen(node_b.clone())
        .with_reachability(Reachability::new().unreachable(node_a.clone(), node_b.clone()));

        let convergence = Convergence::check(&gossip, &node_a);

        assert_eq!(
            convergence.blockers(),
            &[ConvergenceBlocker::Unreachable {
                node: node_b,
                status: MemberStatus::Up
            }]
        );
    }

    #[test]
    fn down_and_exiting_unreachable_members_do_not_block_convergence() {
        let node_a = node("a", 1);
        let node_b = node("b", 2);
        let node_c = node("c", 3);
        let gossip = Gossip::from_members([
            member(node_a.clone(), MemberStatus::Up),
            member(node_b.clone(), MemberStatus::Down),
            member(node_c.clone(), MemberStatus::Exiting),
        ])
        .seen(node_a.clone())
        .with_reachability(
            Reachability::new()
                .unreachable(node_a.clone(), node_b)
                .terminated(node_a.clone(), node_c),
        );

        assert!(Convergence::check(&gossip, &node_a).is_converged());
    }

    #[test]
    fn observations_from_down_members_do_not_block_convergence() {
        let node_a = node("a", 1);
        let node_b = node("b", 2);
        let node_c = node("c", 3);
        let gossip = Gossip::from_members([
            member(node_a.clone(), MemberStatus::Up),
            member(node_b.clone(), MemberStatus::Down),
            member(node_c.clone(), MemberStatus::Up),
        ])
        .seen(node_a.clone())
        .seen(node_c.clone())
        .with_reachability(Reachability::new().unreachable(node_b, node_c));

        assert!(Convergence::check(&gossip, &node_a).is_converged());
    }

    fn member(unique_address: UniqueAddress, status: MemberStatus) -> Member {
        Member::new(unique_address, Vec::new()).with_status(status)
    }

    fn node(system: &str, uid: u64) -> UniqueAddress {
        UniqueAddress::new(Address::local(system), uid)
    }
}
