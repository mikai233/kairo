use std::collections::{HashMap, HashSet};

use crate::{Member, MemberStatus, Reachability, UniqueAddress, VectorClock, VectorClockNode};

pub type Timestamp = u64;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Gossip {
    members: Vec<Member>,
    seen: HashSet<UniqueAddress>,
    reachability: Reachability,
    version: VectorClock,
    tombstones: HashMap<UniqueAddress, Timestamp>,
}

impl Gossip {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_members(members: impl IntoIterator<Item = Member>) -> Self {
        Self {
            members: normalize_members(members),
            ..Self::default()
        }
    }

    pub fn members(&self) -> &[Member] {
        &self.members
    }

    pub fn member(&self, node: &UniqueAddress) -> Option<&Member> {
        self.members
            .iter()
            .find(|member| &member.unique_address == node)
    }

    pub fn has_member(&self, node: &UniqueAddress) -> bool {
        self.member(node).is_some()
    }

    pub fn seen_by(&self) -> &HashSet<UniqueAddress> {
        &self.seen
    }

    pub fn reachability(&self) -> &Reachability {
        &self.reachability
    }

    pub fn version(&self) -> &VectorClock {
        &self.version
    }

    pub fn tombstones(&self) -> &HashMap<UniqueAddress, Timestamp> {
        &self.tombstones
    }

    pub fn increment_version(&self, node: &UniqueAddress) -> Self {
        let mut changed = self.clone();
        changed.version = changed.version.increment(vclock_node(node));
        changed
    }

    pub fn add_member(&self, member: Member) -> Self {
        let mut members = self.members.clone();
        members.push(member);
        Self {
            members: normalize_members(members),
            ..self.clone()
        }
    }

    pub fn update_members(&self, changed_members: impl IntoIterator<Item = Member>) -> Self {
        let changed_members: HashMap<_, _> = changed_members
            .into_iter()
            .map(|member| (member.unique_address.clone(), member))
            .collect();
        if changed_members.is_empty() {
            return self.clone();
        }

        let members: Vec<_> = self
            .members
            .iter()
            .cloned()
            .map(|member| {
                changed_members
                    .get(&member.unique_address)
                    .cloned()
                    .unwrap_or(member)
            })
            .collect();

        Self {
            members: normalize_members(members),
            ..self.clone()
        }
    }

    pub fn seen(&self, node: UniqueAddress) -> Self {
        if self.seen.contains(&node) {
            return self.clone();
        }
        let mut seen = self.seen.clone();
        seen.insert(node);
        Self {
            seen,
            ..self.clone()
        }
    }

    pub fn only_seen(&self, node: UniqueAddress) -> Self {
        Self {
            seen: HashSet::from([node]),
            ..self.clone()
        }
    }

    pub fn clear_seen(&self) -> Self {
        Self {
            seen: HashSet::new(),
            ..self.clone()
        }
    }

    pub fn with_reachability(&self, reachability: Reachability) -> Self {
        Self {
            reachability,
            ..self.clone()
        }
    }

    pub fn mark_down(&self, node: &UniqueAddress) -> Self {
        let members = self
            .members
            .iter()
            .cloned()
            .map(|member| {
                if &member.unique_address == node {
                    member.with_status(MemberStatus::Down)
                } else {
                    member
                }
            })
            .collect();
        let mut seen = self.seen.clone();
        seen.remove(node);
        Self {
            members,
            seen,
            ..self.clone()
        }
    }

    pub fn remove(&self, node: &UniqueAddress, timestamp: Timestamp) -> Self {
        let removed = HashSet::from([node.clone()]);
        let members = self
            .members
            .iter()
            .filter(|member| &member.unique_address != node)
            .cloned()
            .collect();
        let mut seen = self.seen.clone();
        seen.remove(node);
        let mut tombstones = self.tombstones.clone();
        tombstones.insert(node.clone(), timestamp);
        Self {
            members,
            seen,
            reachability: self.reachability.remove(&removed),
            version: self.version.prune(&vclock_node(node)),
            tombstones,
        }
    }

    pub fn merge(&self, other: &Self) -> Self {
        let mut tombstones = self.tombstones.clone();
        tombstones.extend(other.tombstones.clone());

        let version = tombstones
            .keys()
            .fold(self.version.merge(&other.version), |clock, node| {
                clock.prune(&vclock_node(node))
            });

        let members = merge_members(&self.members, &other.members, &tombstones);
        let allowed: HashSet<_> = members
            .iter()
            .map(|member| member.unique_address.clone())
            .collect();
        let reachability = self.reachability.merge(&allowed, &other.reachability);

        Self {
            members,
            seen: HashSet::new(),
            reachability,
            version,
            tombstones,
        }
    }
}

fn normalize_members(members: impl IntoIterator<Item = Member>) -> Vec<Member> {
    let mut by_address: HashMap<UniqueAddress, Member> = HashMap::new();
    for member in members {
        by_address
            .entry(member.unique_address.clone())
            .and_modify(|existing| {
                *existing = Member::highest_priority(existing, &member).clone();
            })
            .or_insert(member);
    }
    let mut members: Vec<_> = by_address.into_values().collect();
    members.sort_by_key(|member| member.unique_address.ordering_key());
    members
}

fn merge_members(
    left: &[Member],
    right: &[Member],
    tombstones: &HashMap<UniqueAddress, Timestamp>,
) -> Vec<Member> {
    let mut grouped: HashMap<UniqueAddress, Vec<Member>> = HashMap::new();
    for member in left.iter().chain(right.iter()) {
        grouped
            .entry(member.unique_address.clone())
            .or_default()
            .push(member.clone());
    }

    normalize_members(grouped.into_iter().filter_map(|(node, members)| {
        if members.len() > 1 {
            let selected = members
                .iter()
                .reduce(Member::highest_priority)
                .expect("group has at least one member")
                .clone();
            Some(selected)
        } else {
            let member = members.into_iter().next().expect("group has one member");
            if tombstones.contains_key(&node) || member.status.is_removed_by_single_sided_merge() {
                None
            } else {
                Some(member)
            }
        }
    }))
}

fn vclock_node(node: &UniqueAddress) -> VectorClockNode {
    VectorClockNode::new(format!("{}-{}", node.address, node.uid))
}

#[cfg(test)]
mod tests {
    use kairo_actor::Address;

    use super::*;
    use crate::ReachabilityStatus;

    #[test]
    fn seen_only_seen_and_clear_seen_update_seen_table() {
        let node_a = node("a", 1);
        let node_b = node("b", 2);
        let gossip = Gossip::new().seen(node_a.clone()).seen(node_b.clone());

        assert!(gossip.seen_by().contains(&node_a));
        assert!(gossip.seen_by().contains(&node_b));
        assert_eq!(
            gossip.only_seen(node_a.clone()).seen_by(),
            &HashSet::from([node_a])
        );
        assert!(gossip.clear_seen().seen_by().is_empty());
    }

    #[test]
    fn merge_picks_highest_member_status_and_clears_seen() {
        let node_a = node("a", 1);
        let left = Gossip::from_members([member(node_a.clone(), MemberStatus::Joining)])
            .seen(node_a.clone());
        let right = Gossip::from_members([member(node_a.clone(), MemberStatus::Up)]);

        let merged = left.merge(&right);

        assert_eq!(merged.members()[0].status, MemberStatus::Up);
        assert!(merged.seen_by().is_empty());
    }

    #[test]
    fn merge_uses_tombstones_to_prevent_removed_member_reintroduction() {
        let node_a = node("a", 1);
        let left =
            Gossip::from_members([member(node_a.clone(), MemberStatus::Up)]).remove(&node_a, 42);
        let right = Gossip::from_members([member(node_a.clone(), MemberStatus::Up)]);

        let merged = left.merge(&right);

        assert!(merged.members().is_empty());
        assert_eq!(merged.tombstones().get(&node_a), Some(&42));
    }

    #[test]
    fn merge_filters_reachability_to_live_members() {
        let node_a = node("a", 1);
        let node_b = node("b", 2);
        let node_c = node("c", 3);
        let left = Gossip::from_members([
            member(node_a.clone(), MemberStatus::Up),
            member(node_b.clone(), MemberStatus::Up),
        ])
        .with_reachability(Reachability::new().unreachable(node_a.clone(), node_b.clone()));
        let right = Gossip::from_members([member(node_c.clone(), MemberStatus::Up)]);

        let merged = left.merge(&right);

        assert_eq!(
            merged.reachability().status(&node_a, &node_b),
            ReachabilityStatus::Unreachable
        );
        assert_eq!(
            merged.reachability().status(&node_a, &node_c),
            ReachabilityStatus::Reachable
        );
    }

    #[test]
    fn remove_prunes_member_seen_reachability_and_vector_clock() {
        let node_a = node("a", 1);
        let node_b = node("b", 2);
        let gossip = Gossip::from_members([
            member(node_a.clone(), MemberStatus::Up),
            member(node_b.clone(), MemberStatus::Up),
        ])
        .seen(node_b.clone())
        .with_reachability(Reachability::new().unreachable(node_a.clone(), node_b.clone()))
        .increment_version(&node_b);

        let removed = gossip.remove(&node_b, 10);

        assert_eq!(removed.members().len(), 1);
        assert!(!removed.seen_by().contains(&node_b));
        assert_eq!(
            removed.reachability().status(&node_a, &node_b),
            ReachabilityStatus::Reachable
        );
        assert_eq!(removed.version().get(&vclock_node(&node_b)), 0);
    }

    fn member(unique_address: UniqueAddress, status: MemberStatus) -> Member {
        Member::new(unique_address, Vec::new()).with_status(status)
    }

    fn node(system: &str, uid: u64) -> UniqueAddress {
        UniqueAddress::new(Address::local(system), uid)
    }
}
