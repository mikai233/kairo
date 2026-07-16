#![deny(missing_docs)]

use kairo_cluster::{ClusterEvent, Member, MemberEvent, MemberStatus, UniqueAddress};

/// Eligibility scope used when selecting the oldest singleton host.
///
/// The scope filters cluster membership observations only. It never supplies
/// membership itself, so singleton ownership remains derived from cluster
/// gossip and local failure-detector decisions.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SingletonScope {
    role: Option<String>,
}

impl SingletonScope {
    /// Selects from every cluster member with an assigned up number.
    pub fn all() -> Self {
        Self::default()
    }

    /// Selects only members carrying `role`.
    pub fn for_role(role: impl Into<String>) -> Self {
        Self {
            role: Some(role.into()),
        }
    }

    /// Returns the required role, or `None` for the all-members scope.
    pub fn role(&self) -> Option<&str> {
        self.role.as_deref()
    }

    /// Returns whether `member` is eligible for this scope.
    pub fn includes(&self, member: &Member) -> bool {
        self.role.as_ref().is_none_or(|role| member.has_role(role))
    }
}

/// Initial oldest-member view used to safely initialize a manager.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SingletonOldestObservation {
    older_or_self: Vec<UniqueAddress>,
    safe_to_be_oldest: bool,
}

impl SingletonOldestObservation {
    /// Returns eligible members no younger than self, in deterministic age order.
    pub fn older_or_self(&self) -> &[UniqueAddress] {
        &self.older_or_self
    }

    /// Returns the oldest eligible member in the initial view.
    pub fn oldest(&self) -> Option<&UniqueAddress> {
        self.older_or_self.first()
    }

    /// Returns whether no older-or-self member is leaving, exiting, or down.
    ///
    /// A self-oldest manager starts immediately only when this is true;
    /// otherwise it waits for handover or removal evidence.
    pub fn safe_to_be_oldest(&self) -> bool {
        self.safe_to_be_oldest
    }
}

/// Ownership-relevant change emitted by [`SingletonOldestTracker`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SingletonOldestChange {
    /// The oldest eligible member changed, or no eligible member remains.
    OldestChanged(Option<UniqueAddress>),
    /// This exact member incarnation was removed from the cluster.
    SelfRemoved,
    /// This exact member incarnation was explicitly downed.
    SelfDowned,
}

/// Deterministic, role-scoped oldest-member tracker.
///
/// Members are ordered by cluster-assigned up number and then by stable unique
/// address key. Only `Up` events add active members; leaving, exiting, and
/// removal events take members out of ownership selection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SingletonOldestTracker {
    self_node: UniqueAddress,
    scope: SingletonScope,
    members_by_age: Vec<Member>,
}

impl SingletonOldestTracker {
    /// Creates an empty tracker for this exact member and eligibility scope.
    pub fn new(self_node: UniqueAddress, scope: SingletonScope) -> Self {
        Self {
            self_node,
            scope,
            members_by_age: Vec::new(),
        }
    }

    /// Builds a tracker and initial safety observation from a membership snapshot.
    ///
    /// Snapshot members must have an assigned up number and match the scope to
    /// participate. Their current status is retained for takeover safety.
    pub fn from_members(
        self_node: UniqueAddress,
        scope: SingletonScope,
        members: impl IntoIterator<Item = Member>,
    ) -> (Self, SingletonOldestObservation) {
        let mut tracker = Self::new(self_node, scope);
        for member in members {
            tracker.add_or_update_initial_member(member);
        }
        let observation = tracker.initial_observation();
        (tracker, observation)
    }

    /// Returns the exact local member incarnation.
    pub fn self_node(&self) -> &UniqueAddress {
        &self.self_node
    }

    /// Returns the role/all-members eligibility scope.
    pub fn scope(&self) -> &SingletonScope {
        &self.scope
    }

    /// Returns tracked eligible members in deterministic oldest-first order.
    pub fn members_by_age(&self) -> &[Member] {
        &self.members_by_age
    }

    /// Returns the currently selected oldest eligible member.
    pub fn current_oldest(&self) -> Option<&UniqueAddress> {
        self.members_by_age
            .first()
            .map(|member| &member.unique_address)
    }

    /// Derives the manager's initial older-or-self set and takeover safety.
    pub fn initial_observation(&self) -> SingletonOldestObservation {
        let self_up_number = self
            .members_by_age
            .iter()
            .find(|member| member.unique_address == self.self_node)
            .and_then(|member| member.up_number)
            .unwrap_or(u64::MAX);

        let older_or_self_members: Vec<_> = self
            .members_by_age
            .iter()
            .filter(|member| member.up_number.unwrap_or(u64::MAX) <= self_up_number)
            .collect();
        let safe_to_be_oldest = !older_or_self_members.iter().any(|member| {
            matches!(
                member.status,
                MemberStatus::Leaving | MemberStatus::Exiting | MemberStatus::Down
            )
        });
        let older_or_self = older_or_self_members
            .into_iter()
            .map(|member| member.unique_address.clone())
            .collect();

        SingletonOldestObservation {
            older_or_self,
            safe_to_be_oldest,
        }
    }

    /// Applies one authoritative cluster event and returns an ownership change.
    ///
    /// Reachability and leader observations do not independently change
    /// ownership. Only member lifecycle events update the tracker.
    pub fn apply_cluster_event(&mut self, event: &ClusterEvent) -> Option<SingletonOldestChange> {
        match event {
            ClusterEvent::Member(event) => self.apply_member_event(event),
            ClusterEvent::Reachability(_)
            | ClusterEvent::LeaderChanged { .. }
            | ClusterEvent::RoleLeaderChanged { .. }
            | ClusterEvent::SeenChanged { .. }
            | ClusterEvent::ReachabilityChanged { .. }
            | ClusterEvent::MemberTombstonesChanged { .. } => None,
        }
    }

    /// Applies one member lifecycle event and returns an ownership change.
    ///
    /// `Up` adds a scoped member. `Left`, `Exited`, and `Removed` remove it.
    /// Self-down and self-removal produce terminal manager observations; a
    /// remote down is retained until cluster removal establishes finality.
    pub fn apply_member_event(&mut self, event: &MemberEvent) -> Option<SingletonOldestChange> {
        let before = self.current_oldest().cloned();
        match event {
            MemberEvent::Up(member) => self.add_or_update_active_member(member.clone()),
            MemberEvent::Downed(member) if member.unique_address == self.self_node => {
                return Some(SingletonOldestChange::SelfDowned);
            }
            MemberEvent::Left(member) | MemberEvent::Exited(member) => {
                self.remove_member(&member.unique_address)
            }
            MemberEvent::Removed { member, .. } if member.unique_address == self.self_node => {
                self.remove_member(&member.unique_address);
                return Some(SingletonOldestChange::SelfRemoved);
            }
            MemberEvent::Removed { member, .. } => self.remove_member(&member.unique_address),
            MemberEvent::Joined(_) | MemberEvent::WeaklyUp(_) | MemberEvent::Downed(_) => {}
        }
        let after = self.current_oldest().cloned();

        (before != after).then_some(SingletonOldestChange::OldestChanged(after))
    }

    fn add_or_update_initial_member(&mut self, member: Member) {
        if member.up_number.is_some() && self.scope.includes(&member) {
            self.add_or_update_member(member);
        }
    }

    fn add_or_update_active_member(&mut self, member: Member) {
        if member.up_number.is_some() && self.scope.includes(&member) {
            self.add_or_update_member(member);
        }
    }

    fn add_or_update_member(&mut self, member: Member) {
        self.remove_member(&member.unique_address);
        self.members_by_age.push(member);
        self.sort_members();
    }

    fn remove_member(&mut self, node: &UniqueAddress) {
        self.members_by_age
            .retain(|member| &member.unique_address != node);
    }

    fn sort_members(&mut self) {
        self.members_by_age.sort_by(|left, right| {
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
    }
}
