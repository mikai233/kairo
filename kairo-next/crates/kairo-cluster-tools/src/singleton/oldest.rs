use kairo_cluster::{ClusterEvent, Member, MemberEvent, MemberStatus, UniqueAddress};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SingletonScope {
    role: Option<String>,
}

impl SingletonScope {
    pub fn all() -> Self {
        Self::default()
    }

    pub fn for_role(role: impl Into<String>) -> Self {
        Self {
            role: Some(role.into()),
        }
    }

    pub fn role(&self) -> Option<&str> {
        self.role.as_deref()
    }

    pub fn includes(&self, member: &Member) -> bool {
        self.role.as_ref().is_none_or(|role| member.has_role(role))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SingletonOldestObservation {
    older_or_self: Vec<UniqueAddress>,
    safe_to_be_oldest: bool,
}

impl SingletonOldestObservation {
    pub fn older_or_self(&self) -> &[UniqueAddress] {
        &self.older_or_self
    }

    pub fn oldest(&self) -> Option<&UniqueAddress> {
        self.older_or_self.first()
    }

    pub fn safe_to_be_oldest(&self) -> bool {
        self.safe_to_be_oldest
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SingletonOldestChange {
    OldestChanged(Option<UniqueAddress>),
    SelfRemoved,
    SelfDowned,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SingletonOldestTracker {
    self_node: UniqueAddress,
    scope: SingletonScope,
    members_by_age: Vec<Member>,
}

impl SingletonOldestTracker {
    pub fn new(self_node: UniqueAddress, scope: SingletonScope) -> Self {
        Self {
            self_node,
            scope,
            members_by_age: Vec::new(),
        }
    }

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

    pub fn self_node(&self) -> &UniqueAddress {
        &self.self_node
    }

    pub fn scope(&self) -> &SingletonScope {
        &self.scope
    }

    pub fn members_by_age(&self) -> &[Member] {
        &self.members_by_age
    }

    pub fn current_oldest(&self) -> Option<&UniqueAddress> {
        self.members_by_age
            .first()
            .map(|member| &member.unique_address)
    }

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
