use crate::{Gossip, ReachabilityStatus, UniqueAddress};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaderSelection {
    leader: Option<UniqueAddress>,
}

impl LeaderSelection {
    pub fn for_gossip(gossip: &Gossip, self_node: &UniqueAddress) -> Self {
        Self::for_members(gossip, self_node, gossip.members())
    }

    pub fn for_role(gossip: &Gossip, self_node: &UniqueAddress, role: &str) -> Self {
        let members: Vec<_> = gossip
            .members()
            .iter()
            .filter(|member| member.has_role(role))
            .cloned()
            .collect();
        Self::for_members(gossip, self_node, &members)
    }

    pub fn leader(&self) -> Option<&UniqueAddress> {
        self.leader.as_ref()
    }

    pub fn is_leader(&self, node: &UniqueAddress) -> bool {
        self.leader.as_ref() == Some(node)
    }

    fn for_members(gossip: &Gossip, self_node: &UniqueAddress, members: &[crate::Member]) -> Self {
        let mut reachable_members: Vec<_> = members
            .iter()
            .filter(|member| member.status != crate::MemberStatus::Down)
            .filter(|member| {
                gossip.reachability().is_all_reachable()
                    || member.unique_address == *self_node
                    || gossip.reachability().status_of(&member.unique_address)
                        == ReachabilityStatus::Reachable
            })
            .collect();

        reachable_members.sort_by(|left, right| {
            let left_key = (
                left.up_number.unwrap_or(u64::MAX),
                left.unique_address.ordering_key(),
            );
            let right_key = (
                right.up_number.unwrap_or(u64::MAX),
                right.unique_address.ordering_key(),
            );
            left_key.cmp(&right_key)
        });

        let leader = reachable_members
            .iter()
            .find(|member| member.status.participates_in_leader_selection())
            .or_else(|| {
                reachable_members
                    .iter()
                    .min_by_key(|member| member.leader_fallback_key())
            })
            .map(|member| member.unique_address.clone());

        Self { leader }
    }
}

#[cfg(test)]
mod tests {
    use kairo_actor::Address;

    use super::*;
    use crate::{Member, MemberStatus, Reachability};

    #[test]
    fn selects_oldest_reachable_up_or_leaving_member() {
        let node_a = node("a", 1);
        let node_b = node("b", 2);
        let node_c = node("c", 3);
        let gossip = Gossip::from_members([
            member(node_a.clone(), MemberStatus::Up, 3),
            member(node_b.clone(), MemberStatus::Leaving, 1),
            member(node_c, MemberStatus::Up, 2),
        ]);

        let selection = LeaderSelection::for_gossip(&gossip, &node_a);

        assert_eq!(selection.leader(), Some(&node_b));
    }

    #[test]
    fn excludes_down_members_from_leader_selection() {
        let node_a = node("a", 1);
        let node_b = node("b", 2);
        let gossip = Gossip::from_members([
            member(node_a.clone(), MemberStatus::Down, 1),
            member(node_b.clone(), MemberStatus::Up, 2),
        ]);

        let selection = LeaderSelection::for_gossip(&gossip, &node_b);

        assert_eq!(selection.leader(), Some(&node_b));
    }

    #[test]
    fn excludes_unreachable_members_except_self() {
        let node_a = node("a", 1);
        let node_b = node("b", 2);
        let gossip = Gossip::from_members([
            member(node_a.clone(), MemberStatus::Up, 1),
            member(node_b.clone(), MemberStatus::Up, 2),
        ])
        .with_reachability(Reachability::new().unreachable(node_b.clone(), node_a.clone()));

        let selection = LeaderSelection::for_gossip(&gossip, &node_b);

        assert_eq!(selection.leader(), Some(&node_b));
    }

    #[test]
    fn falls_back_to_weakly_up_before_joining_and_exiting_when_no_up_member_exists() {
        let node_a = node("a", 1);
        let node_b = node("b", 2);
        let node_c = node("c", 3);
        let gossip = Gossip::from_members([
            member(node_a.clone(), MemberStatus::Joining, 1),
            member(node_b.clone(), MemberStatus::WeaklyUp, 2),
            member(node_c, MemberStatus::Exiting, 3),
        ]);

        let selection = LeaderSelection::for_gossip(&gossip, &node_a);

        assert_eq!(selection.leader(), Some(&node_b));
    }

    #[test]
    fn role_leader_filters_members_by_role() {
        let node_a = node("a", 1);
        let node_b = node("b", 2);
        let gossip = Gossip::from_members([
            member(node_a.clone(), MemberStatus::Up, 1),
            member_with_roles(node_b.clone(), MemberStatus::Up, 2, ["backend"]),
        ]);

        let selection = LeaderSelection::for_role(&gossip, &node_a, "backend");

        assert_eq!(selection.leader(), Some(&node_b));
    }

    fn member(unique_address: UniqueAddress, status: MemberStatus, up_number: u64) -> Member {
        Member::new(unique_address, Vec::new())
            .with_status(status)
            .with_up_number(up_number)
    }

    fn member_with_roles(
        unique_address: UniqueAddress,
        status: MemberStatus,
        up_number: u64,
        roles: impl IntoIterator<Item = &'static str>,
    ) -> Member {
        Member::new(
            unique_address,
            roles.into_iter().map(String::from).collect(),
        )
        .with_status(status)
        .with_up_number(up_number)
    }

    fn node(system: &str, uid: u64) -> UniqueAddress {
        UniqueAddress::new(Address::local(system), uid)
    }
}
