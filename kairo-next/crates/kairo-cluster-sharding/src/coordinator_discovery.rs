#![deny(missing_docs)]

use std::cmp::Ordering;
use std::collections::BTreeSet;

use kairo_cluster::{
    ClusterEvent, CurrentClusterState, Member, MemberEvent, MemberStatus, UniqueAddress,
};

/// Role constraints used when locating eligible shard-coordinator nodes.
///
/// A candidate must advertise every configured role.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CoordinatorDiscoverySettings {
    required_roles: BTreeSet<String>,
}

impl CoordinatorDiscoverySettings {
    /// Creates settings from the roles every coordinator candidate must have.
    pub fn new(required_roles: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            required_roles: required_roles.into_iter().map(Into::into).collect(),
        }
    }

    /// Adds one required coordinator role.
    pub fn with_required_role(mut self, role: impl Into<String>) -> Self {
        self.required_roles.insert(role.into());
        self
    }

    /// Returns all roles required of a coordinator candidate.
    pub fn required_roles(&self) -> &BTreeSet<String> {
        &self.required_roles
    }
}

/// Describes how a membership update changed the oldest eligible node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoordinatorDiscoveryChange {
    /// Oldest eligible node before the update.
    pub previous_oldest: Option<UniqueAddress>,
    /// Oldest eligible node after the update.
    pub current_oldest: Option<UniqueAddress>,
    /// Whether the oldest eligible node changed.
    pub coordinator_moved: bool,
}

impl CoordinatorDiscoveryChange {
    fn unchanged(oldest: Option<UniqueAddress>) -> Self {
        Self {
            previous_oldest: oldest.clone(),
            current_oldest: oldest,
            coordinator_moved: false,
        }
    }

    fn from_oldest_change(
        previous_oldest: Option<UniqueAddress>,
        current_oldest: Option<UniqueAddress>,
    ) -> Self {
        let coordinator_moved = previous_oldest != current_oldest;
        Self {
            previous_oldest,
            current_oldest,
            coordinator_moved,
        }
    }
}

/// Membership projection used to locate likely shard-coordinator nodes.
///
/// Candidates have every required role, are `Up`, `Leaving`, or `Exiting`,
/// and are ordered by cluster age. Keeping departing nodes until removal lets
/// regions contact both the old singleton location and its likely successor
/// while coordinator ownership moves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoordinatorDiscoveryState {
    settings: CoordinatorDiscoverySettings,
    members: Vec<Member>,
}

impl CoordinatorDiscoveryState {
    /// Creates an empty membership projection with `settings`.
    pub fn new(settings: CoordinatorDiscoverySettings) -> Self {
        Self {
            settings,
            members: Vec::new(),
        }
    }

    /// Returns the candidate eligibility settings.
    pub fn settings(&self) -> &CoordinatorDiscoverySettings {
        &self.settings
    }

    /// Replaces the membership projection from a cluster snapshot.
    ///
    /// Returns the resulting oldest-candidate transition.
    pub fn apply_snapshot(&mut self, state: &CurrentClusterState) -> CoordinatorDiscoveryChange {
        let previous_oldest = self.oldest().cloned();
        self.members = state
            .members
            .iter()
            .filter(|member| self.is_candidate(member))
            .cloned()
            .collect();
        self.sort_members();
        CoordinatorDiscoveryChange::from_oldest_change(previous_oldest, self.oldest().cloned())
    }

    /// Applies one cluster event to the membership projection.
    ///
    /// Non-membership events leave discovery unchanged.
    pub fn apply_event(&mut self, event: &ClusterEvent) -> CoordinatorDiscoveryChange {
        let previous_oldest = self.oldest().cloned();
        match event {
            ClusterEvent::Member(member_event) => self.apply_member_event(member_event),
            ClusterEvent::Reachability(_)
            | ClusterEvent::LeaderChanged { .. }
            | ClusterEvent::RoleLeaderChanged { .. }
            | ClusterEvent::SeenChanged { .. }
            | ClusterEvent::ReachabilityChanged { .. }
            | ClusterEvent::MemberTombstonesChanged { .. } => {
                return CoordinatorDiscoveryChange::unchanged(previous_oldest);
            }
        }
        self.sort_members();
        CoordinatorDiscoveryChange::from_oldest_change(previous_oldest, self.oldest().cloned())
    }

    /// Returns all eligible members in oldest-first order.
    pub fn candidates(&self) -> Vec<UniqueAddress> {
        self.members_by_age()
            .into_iter()
            .map(|member| member.unique_address.clone())
            .collect()
    }

    /// Returns the likely live coordinator locations in contact order.
    ///
    /// Starting at the oldest member, discovery retains departing candidates
    /// through the first `Up` member and reverses that prefix. This preserves
    /// Pekko's ability to contact a newly started successor promptly while
    /// still trying the departing coordinator locations.
    pub fn coordinator_candidates(&self) -> Vec<UniqueAddress> {
        let mut candidates = Vec::new();
        for member in self.members_by_age() {
            candidates.insert(0, member.unique_address.clone());
            if member.status == MemberStatus::Up {
                break;
            }
        }
        candidates
    }

    /// Returns the oldest eligible member, if one is known.
    pub fn oldest(&self) -> Option<&UniqueAddress> {
        self.members_by_age()
            .first()
            .map(|member| &member.unique_address)
    }

    /// Returns whether no eligible members are known.
    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    fn apply_member_event(&mut self, event: &MemberEvent) {
        match event {
            MemberEvent::Up(member) | MemberEvent::Left(member) | MemberEvent::Exited(member) => {
                if self.is_candidate(member) {
                    self.upsert_member(member.clone());
                } else {
                    self.remove_member(&member.unique_address);
                }
            }
            MemberEvent::Joined(member)
            | MemberEvent::WeaklyUp(member)
            | MemberEvent::Downed(member) => self.remove_member(&member.unique_address),
            MemberEvent::Removed { member, .. } => self.remove_member(&member.unique_address),
        }
    }

    fn upsert_member(&mut self, member: Member) {
        match self
            .members
            .iter_mut()
            .find(|existing| existing.unique_address == member.unique_address)
        {
            Some(existing) => *existing = member,
            None => self.members.push(member),
        }
    }

    fn remove_member(&mut self, node: &UniqueAddress) {
        self.members.retain(|member| member.unique_address != *node);
    }

    fn is_candidate(&self, member: &Member) -> bool {
        matches!(
            member.status,
            MemberStatus::Up | MemberStatus::Leaving | MemberStatus::Exiting
        ) && self
            .settings
            .required_roles
            .iter()
            .all(|role| member.has_role(role))
    }

    fn members_by_age(&self) -> Vec<&Member> {
        let mut members: Vec<_> = self.members.iter().collect();
        members.sort_by(|left, right| compare_member_age(left, right));
        members
    }

    fn sort_members(&mut self) {
        self.members.sort_by(compare_member_age);
    }
}

fn compare_member_age(left: &Member, right: &Member) -> Ordering {
    if left.is_older_than(right) {
        Ordering::Less
    } else if right.is_older_than(left) {
        Ordering::Greater
    } else {
        Ordering::Equal
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use kairo_actor::Address;
    use kairo_cluster::{ClusterEvent, CurrentClusterState, Member, MemberEvent, MemberStatus};

    use super::*;

    #[test]
    fn coordinator_discovery_snapshot_filters_roles_and_sorts_by_age() {
        let older = member(node("old", 1), MemberStatus::Up, ["sharding"], 1);
        let wrong_role = member(node("wrong-role", 2), MemberStatus::Up, ["backend"], 2);
        let younger = member(node("young", 3), MemberStatus::Up, ["sharding"], 3);
        let joining = member(node("joining", 4), MemberStatus::Joining, ["sharding"], 0);
        let snapshot = state(vec![younger.clone(), joining, wrong_role, older.clone()]);
        let mut discovery = CoordinatorDiscoveryState::new(
            CoordinatorDiscoverySettings::default().with_required_role("sharding"),
        );

        let change = discovery.apply_snapshot(&snapshot);

        assert_eq!(
            discovery.candidates(),
            vec![older.unique_address.clone(), younger.unique_address.clone()]
        );
        assert_eq!(change.previous_oldest, None);
        assert_eq!(change.current_oldest, Some(older.unique_address.clone()));
        assert!(change.coordinator_moved);
    }

    #[test]
    fn coordinator_discovery_candidates_include_leaving_oldest_until_first_up() {
        let leaving_oldest = member(node("leaving", 1), MemberStatus::Leaving, ["sharding"], 1);
        let exiting_middle = member(node("exiting", 2), MemberStatus::Exiting, ["sharding"], 2);
        let first_up = member(node("up", 3), MemberStatus::Up, ["sharding"], 3);
        let later_up = member(node("later", 4), MemberStatus::Up, ["sharding"], 4);
        let mut discovery = CoordinatorDiscoveryState::new(CoordinatorDiscoverySettings::default());

        discovery.apply_snapshot(&state(vec![
            later_up,
            first_up.clone(),
            exiting_middle.clone(),
            leaving_oldest.clone(),
        ]));

        assert_eq!(
            discovery.coordinator_candidates(),
            vec![
                first_up.unique_address.clone(),
                exiting_middle.unique_address.clone(),
                leaving_oldest.unique_address.clone(),
            ]
        );
    }

    #[test]
    fn coordinator_discovery_reports_moved_when_oldest_changes() {
        let older = member(node("older", 1), MemberStatus::Up, ["sharding"], 1);
        let younger = member(node("younger", 2), MemberStatus::Up, ["sharding"], 2);
        let mut discovery = CoordinatorDiscoveryState::new(CoordinatorDiscoverySettings::default());
        discovery.apply_snapshot(&state(vec![older.clone(), younger.clone()]));

        let change = discovery.apply_event(&ClusterEvent::Member(MemberEvent::Removed {
            member: older.clone().with_status(MemberStatus::Removed),
            previous_status: MemberStatus::Up,
        }));

        assert_eq!(change.previous_oldest, Some(older.unique_address));
        assert_eq!(change.current_oldest, Some(younger.unique_address));
        assert!(change.coordinator_moved);
    }

    #[test]
    fn coordinator_discovery_removes_down_or_removed_candidates() {
        let node_a = node("a", 1);
        let node_b = node("b", 2);
        let member_a = member_for(node_a.clone(), MemberStatus::Up, ["sharding"], 1);
        let member_b = member_for(node_b.clone(), MemberStatus::Up, ["sharding"], 2);
        let mut discovery = CoordinatorDiscoveryState::new(CoordinatorDiscoverySettings::default());
        discovery.apply_snapshot(&state(vec![member_a.clone(), member_b.clone()]));

        let down_change = discovery.apply_event(&ClusterEvent::Member(MemberEvent::Downed(
            member_a.with_status(MemberStatus::Down),
        )));
        let removed_change = discovery.apply_event(&ClusterEvent::Member(MemberEvent::Removed {
            member: member_b.with_status(MemberStatus::Removed),
            previous_status: MemberStatus::Up,
        }));

        assert_eq!(down_change.previous_oldest, Some(node_a.clone()));
        assert_eq!(down_change.current_oldest, Some(node_b.clone()));
        assert!(down_change.coordinator_moved);
        assert_eq!(removed_change.previous_oldest, Some(node_b));
        assert_eq!(removed_change.current_oldest, None);
        assert!(removed_change.coordinator_moved);
        assert!(discovery.is_empty());
    }

    fn member(
        unique_address: UniqueAddress,
        status: MemberStatus,
        roles: impl IntoIterator<Item = &'static str>,
        up_number: u64,
    ) -> Member {
        member_for(unique_address, status, roles, up_number)
    }

    fn member_for(
        unique_address: UniqueAddress,
        status: MemberStatus,
        roles: impl IntoIterator<Item = &'static str>,
        up_number: u64,
    ) -> Member {
        Member::new(
            unique_address,
            roles.into_iter().map(ToString::to_string).collect(),
        )
        .with_status(status)
        .with_up_number(up_number)
    }

    fn node(system: &str, uid: u64) -> UniqueAddress {
        UniqueAddress::new(
            Address::new(
                "kairo",
                system,
                Some("127.0.0.1".to_string()),
                Some(2550 + uid as u16),
            ),
            uid,
        )
    }

    fn state(members: Vec<Member>) -> CurrentClusterState {
        CurrentClusterState {
            members,
            unreachable: Vec::new(),
            seen_by: HashSet::new(),
            leader: None,
            role_leaders: HashMap::new(),
            member_tombstones: HashSet::new(),
        }
    }
}
