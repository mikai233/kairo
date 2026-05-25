use std::collections::{BTreeMap, BTreeSet};

use kairo_cluster::{
    ClusterEvent, CurrentClusterState, Member, MemberEvent, MemberStatus, ReachabilityEvent,
    UniqueAddress,
};

use crate::ReplicaId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorClusterRoutes {
    self_node: UniqueAddress,
    required_roles: BTreeSet<String>,
    joining: BTreeSet<ReplicaId>,
    weakly_up: BTreeSet<ReplicaId>,
    up: BTreeSet<ReplicaId>,
    exiting: BTreeSet<ReplicaId>,
    unreachable: BTreeSet<ReplicaId>,
    nodes_by_replica: BTreeMap<ReplicaId, UniqueAddress>,
    leader: Option<ReplicaId>,
}

impl ReplicatorClusterRoutes {
    pub fn new(self_node: UniqueAddress) -> Self {
        Self {
            self_node,
            required_roles: BTreeSet::new(),
            joining: BTreeSet::new(),
            weakly_up: BTreeSet::new(),
            up: BTreeSet::new(),
            exiting: BTreeSet::new(),
            unreachable: BTreeSet::new(),
            nodes_by_replica: BTreeMap::new(),
            leader: None,
        }
    }

    pub fn with_required_roles(
        self_node: UniqueAddress,
        roles: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        let mut routes = Self::new(self_node);
        routes.required_roles = roles.into_iter().map(Into::into).collect();
        routes
    }

    pub fn from_current_state(
        self_node: UniqueAddress,
        state: &CurrentClusterState,
        roles: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        let mut routes = Self::with_required_roles(self_node, roles);
        for member in &state.members {
            if routes.matches_remote(member) {
                routes.add_member_from_status(member);
            }
        }
        for member in &state.unreachable {
            if routes.matches_remote(member) {
                routes
                    .unreachable
                    .insert(ReplicaId::from(&member.unique_address));
            }
        }
        routes.leader = state.leader.as_ref().map(ReplicaId::from);
        routes
    }

    pub fn apply_event(&mut self, event: &ClusterEvent) -> ReplicatorClusterRouteUpdate {
        let mut removed_replicas = BTreeSet::new();

        match event {
            ClusterEvent::Member(MemberEvent::Removed { member, .. }) => {
                if self.matches_remote(member) {
                    let replica = ReplicaId::from(&member.unique_address);
                    self.remove_live_replica(&replica);
                    self.unreachable.remove(&replica);
                    removed_replicas.insert(replica);
                }
            }
            ClusterEvent::Member(event) => {
                let member = member_from_event(event);
                if self.matches_remote(member) {
                    self.add_member_from_status(member);
                }
            }
            ClusterEvent::Reachability(ReachabilityEvent::Unreachable(member)) => {
                if self.matches_remote(member) {
                    self.unreachable
                        .insert(ReplicaId::from(&member.unique_address));
                }
            }
            ClusterEvent::Reachability(ReachabilityEvent::Reachable(member)) => {
                if self.matches_remote(member) {
                    self.unreachable
                        .remove(&ReplicaId::from(&member.unique_address));
                }
            }
            ClusterEvent::LeaderChanged { leader } => {
                self.leader = leader.as_ref().map(ReplicaId::from);
            }
            ClusterEvent::RoleLeaderChanged { .. }
            | ClusterEvent::SeenChanged { .. }
            | ClusterEvent::ReachabilityChanged { .. }
            | ClusterEvent::MemberTombstonesChanged { .. } => {}
        }

        ReplicatorClusterRouteUpdate {
            remote_replicas: self.remote_replicas(),
            unreachable_replicas: self.unreachable_replicas(),
            removed_replicas,
            is_leader: self.is_leader(),
        }
    }

    pub fn update(&self) -> ReplicatorClusterRouteUpdate {
        ReplicatorClusterRouteUpdate {
            remote_replicas: self.remote_replicas(),
            unreachable_replicas: self.unreachable_replicas(),
            removed_replicas: BTreeSet::new(),
            is_leader: self.is_leader(),
        }
    }

    pub fn self_node(&self) -> &UniqueAddress {
        &self.self_node
    }

    pub fn remote_replicas(&self) -> Vec<ReplicaId> {
        self.up.union(&self.weakly_up).cloned().collect::<Vec<_>>()
    }

    pub fn remote_nodes(&self) -> Vec<UniqueAddress> {
        self.remote_replicas()
            .into_iter()
            .filter_map(|replica| self.nodes_by_replica.get(&replica).cloned())
            .collect()
    }

    pub fn unreachable_replicas(&self) -> BTreeSet<ReplicaId> {
        let live = self.remote_replicas().into_iter().collect::<BTreeSet<_>>();
        self.unreachable
            .intersection(&live)
            .cloned()
            .collect::<BTreeSet<_>>()
    }

    pub fn is_leader(&self) -> bool {
        self.leader
            .as_ref()
            .is_some_and(|leader| leader == &ReplicaId::from(&self.self_node))
    }

    fn add_member_from_status(&mut self, member: &Member) {
        let replica = ReplicaId::from(&member.unique_address);
        if !matches!(member.status, MemberStatus::Removed) {
            self.nodes_by_replica
                .insert(replica.clone(), member.unique_address.clone());
        }
        match member.status {
            MemberStatus::Joining => {
                self.joining.insert(replica.clone());
                self.weakly_up.remove(&replica);
                self.up.remove(&replica);
                self.exiting.remove(&replica);
            }
            MemberStatus::WeaklyUp => {
                self.joining.remove(&replica);
                self.weakly_up.insert(replica.clone());
                self.up.remove(&replica);
                self.exiting.remove(&replica);
            }
            MemberStatus::Up | MemberStatus::Leaving => {
                self.joining.remove(&replica);
                self.weakly_up.remove(&replica);
                self.up.insert(replica.clone());
                self.exiting.remove(&replica);
            }
            MemberStatus::Exiting => {
                self.joining.remove(&replica);
                self.weakly_up.remove(&replica);
                self.up.insert(replica.clone());
                self.exiting.insert(replica.clone());
            }
            MemberStatus::Down | MemberStatus::Removed => {
                self.remove_live_replica(&replica);
            }
        }
    }

    fn remove_live_replica(&mut self, replica: &ReplicaId) {
        self.joining.remove(replica);
        self.weakly_up.remove(replica);
        self.up.remove(replica);
        self.exiting.remove(replica);
        self.nodes_by_replica.remove(replica);
    }

    fn matches_remote(&self, member: &Member) -> bool {
        member.unique_address.address != self.self_node.address
            && self
                .required_roles
                .iter()
                .all(|role| member.roles.iter().any(|member_role| member_role == role))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorClusterRouteUpdate {
    pub remote_replicas: Vec<ReplicaId>,
    pub unreachable_replicas: BTreeSet<ReplicaId>,
    pub removed_replicas: BTreeSet<ReplicaId>,
    pub is_leader: bool,
}

impl ReplicatorClusterRouteUpdate {
    pub fn new(
        remote_replicas: impl IntoIterator<Item = ReplicaId>,
        unreachable_replicas: impl IntoIterator<Item = ReplicaId>,
        removed_replicas: impl IntoIterator<Item = ReplicaId>,
        is_leader: bool,
    ) -> Self {
        Self {
            remote_replicas: sorted_unique(remote_replicas),
            unreachable_replicas: unreachable_replicas.into_iter().collect(),
            removed_replicas: removed_replicas.into_iter().collect(),
            is_leader,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorClusterRouteReport {
    pub remote_replicas: Vec<ReplicaId>,
    pub unreachable_replicas: BTreeSet<ReplicaId>,
    pub recorded_removed: BTreeSet<ReplicaId>,
}

fn member_from_event(event: &MemberEvent) -> &Member {
    match event {
        MemberEvent::Joined(member)
        | MemberEvent::WeaklyUp(member)
        | MemberEvent::Up(member)
        | MemberEvent::Left(member)
        | MemberEvent::Exited(member)
        | MemberEvent::Downed(member) => member,
        MemberEvent::Removed { member, .. } => member,
    }
}

fn sorted_unique(nodes: impl IntoIterator<Item = ReplicaId>) -> Vec<ReplicaId> {
    nodes
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use kairo_actor::Address;
    use kairo_cluster::MemberStatus;

    use super::*;

    #[test]
    fn route_snapshot_uses_up_and_weakly_up_matching_remote_members() {
        let self_node = node("self", 1);
        let up = member(node("up", 2), MemberStatus::Up, ["ddata"]);
        let weak = member(node("weak", 3), MemberStatus::WeaklyUp, ["ddata"]);
        let joining = member(node("joining", 4), MemberStatus::Joining, ["ddata"]);
        let other_role = member(node("other", 5), MemberStatus::Up, ["other"]);
        let unreachable = member(node("weak", 3), MemberStatus::WeaklyUp, ["ddata"]);
        let state = CurrentClusterState {
            members: vec![
                member(self_node.clone(), MemberStatus::Up, ["ddata"]),
                joining,
                other_role,
                weak.clone(),
                up.clone(),
            ],
            unreachable: vec![unreachable],
            seen_by: HashSet::new(),
            leader: Some(self_node.clone()),
            role_leaders: HashMap::new(),
            member_tombstones: HashSet::new(),
        };

        let routes =
            ReplicatorClusterRoutes::from_current_state(self_node.clone(), &state, ["ddata"]);

        assert_eq!(
            routes.remote_replicas(),
            vec![
                ReplicaId::from(&up.unique_address),
                ReplicaId::from(&weak.unique_address),
            ]
        );
        assert_eq!(
            routes.unreachable_replicas(),
            BTreeSet::from([ReplicaId::from(&weak.unique_address)])
        );
        assert!(routes.is_leader());
    }

    #[test]
    fn route_events_track_reachability_and_removed_replicas() {
        let self_node = node("self", 1);
        let peer = member(node("peer", 2), MemberStatus::Up, ["ddata"]);
        let mut routes = ReplicatorClusterRoutes::with_required_roles(self_node, ["ddata"]);

        let update = routes.apply_event(&ClusterEvent::Member(MemberEvent::Up(peer.clone())));
        assert_eq!(
            update.remote_replicas,
            vec![ReplicaId::from(&peer.unique_address)]
        );
        assert!(update.unreachable_replicas.is_empty());

        let unreachable = routes.apply_event(&ClusterEvent::Reachability(
            ReachabilityEvent::Unreachable(peer.clone()),
        ));
        assert_eq!(
            unreachable.unreachable_replicas,
            BTreeSet::from([ReplicaId::from(&peer.unique_address)])
        );

        let removed = routes.apply_event(&ClusterEvent::Member(MemberEvent::Removed {
            member: peer.clone().with_status(MemberStatus::Removed),
            previous_status: MemberStatus::Up,
        }));
        assert!(removed.remote_replicas.is_empty());
        assert!(removed.unreachable_replicas.is_empty());
        assert_eq!(
            removed.removed_replicas,
            BTreeSet::from([ReplicaId::from(&peer.unique_address)])
        );
    }

    fn node(name: &str, uid: u64) -> UniqueAddress {
        UniqueAddress::new(
            Address::new(
                "kairo",
                "routes",
                Some(format!("{name}.example.test")),
                Some(2552),
            ),
            uid,
        )
    }

    fn member(
        node: UniqueAddress,
        status: MemberStatus,
        roles: impl IntoIterator<Item = &'static str>,
    ) -> Member {
        Member::new(node, roles.into_iter().map(str::to_string).collect()).with_status(status)
    }
}
