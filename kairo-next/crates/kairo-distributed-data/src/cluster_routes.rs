#![deny(missing_docs)]
//! Cluster membership projection for distributed-data routing and pruning.
//!
//! This module consumes immutable cluster snapshots and domain events. It
//! derives role-matching remote replica routes, preserves reachability as a
//! separate observation, reports final removals, and selects the Pekko-style
//! role-scoped replicator leader. It never writes cluster state and is not a
//! membership authority.

use std::collections::{BTreeMap, BTreeSet};

use kairo_cluster::{
    ClusterEvent, CurrentClusterState, Member, MemberEvent, MemberStatus, ReachabilityEvent,
    UniqueAddress,
};

use crate::ReplicaId;

#[derive(Debug, Clone, PartialEq, Eq)]
/// Role-filtered cluster state used by one distributed-data replicator.
///
/// Remote routes include `WeaklyUp`, `Up`, `Leaving`, and `Exiting` replicas.
/// A `Down` event preserves an already-live route until the corresponding
/// `Removed` event, matching Pekko's separation between downing and final
/// membership removal. Required roles use intersection semantics: a member
/// must advertise every configured role.
pub struct ReplicatorClusterRoutes {
    self_node: UniqueAddress,
    required_roles: BTreeSet<String>,
    joining: BTreeSet<ReplicaId>,
    weakly_up: BTreeSet<ReplicaId>,
    up: BTreeSet<ReplicaId>,
    exiting: BTreeSet<ReplicaId>,
    unreachable: BTreeSet<ReplicaId>,
    nodes_by_replica: BTreeMap<ReplicaId, UniqueAddress>,
    members_by_replica: BTreeMap<ReplicaId, Member>,
}

impl ReplicatorClusterRoutes {
    /// Creates an empty route projection with no role requirement.
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
            members_by_replica: BTreeMap::new(),
        }
    }

    /// Creates an empty route projection requiring every supplied role.
    pub fn with_required_roles(
        self_node: UniqueAddress,
        roles: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        let mut routes = Self::new(self_node);
        routes.required_roles = roles.into_iter().map(Into::into).collect();
        routes
    }

    /// Builds a complete route projection from the current cluster snapshot.
    ///
    /// Down members present only in the snapshot are not made routable, while
    /// their reachability observations are retained so pruning time cannot
    /// advance through an unhealthy role-scoped cluster view.
    pub fn from_current_state(
        self_node: UniqueAddress,
        state: &CurrentClusterState,
        roles: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        let mut routes = Self::with_required_roles(self_node, roles);
        for member in &state.members {
            if routes.matches_roles(member) {
                routes.record_snapshot_leader_candidate(member);
                if routes.matches_remote(member) {
                    routes.add_member_from_status(member);
                }
            }
        }
        for member in &state.unreachable {
            if routes.matches_remote(member) {
                routes
                    .unreachable
                    .insert(ReplicaId::from(&member.unique_address));
            }
        }
        routes
    }

    /// Applies one cluster event and returns the resulting replicator update.
    ///
    /// Only a final [`MemberEvent::Removed`] contributes to
    /// [`ReplicatorClusterRouteUpdate::removed_replicas`]. Global and
    /// single-role leader events are ignored because required roles may be an
    /// intersection; leader ownership is derived from the matching members.
    pub fn apply_event(&mut self, event: &ClusterEvent) -> ReplicatorClusterRouteUpdate {
        let mut removed_replicas = BTreeSet::new();

        match event {
            ClusterEvent::Member(MemberEvent::Removed { member, .. }) => {
                if self.matches_roles(member) {
                    self.remove_member(&ReplicaId::from(&member.unique_address));
                }
                if self.matches_remote(member) {
                    let replica = ReplicaId::from(&member.unique_address);
                    self.remove_live_replica(&replica);
                    self.unreachable.remove(&replica);
                    removed_replicas.insert(replica);
                }
            }
            ClusterEvent::Member(event) => {
                let member = member_from_event(event);
                if self.matches_roles(member) {
                    self.record_event_leader_candidate(event);
                    if self.matches_remote(member) {
                        self.add_member_from_status(member);
                    }
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
            ClusterEvent::LeaderChanged { .. }
            | ClusterEvent::RoleLeaderChanged { .. }
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

    /// Returns the current route update without reporting any new removals.
    pub fn update(&self) -> ReplicatorClusterRouteUpdate {
        ReplicatorClusterRouteUpdate {
            remote_replicas: self.remote_replicas(),
            unreachable_replicas: self.unreachable_replicas(),
            removed_replicas: BTreeSet::new(),
            is_leader: self.is_leader(),
        }
    }

    /// Returns the local cluster incarnation represented by this projection.
    pub fn self_node(&self) -> &UniqueAddress {
        &self.self_node
    }

    /// Returns routable remote replicas in deterministic identity order.
    ///
    /// Joining members are excluded. An existing downed replica remains until
    /// final membership removal.
    pub fn remote_replicas(&self) -> Vec<ReplicaId> {
        self.up.union(&self.weakly_up).cloned().collect::<Vec<_>>()
    }

    /// Returns routable remote unique addresses in replica identity order.
    pub fn remote_nodes(&self) -> Vec<UniqueAddress> {
        self.remote_replicas()
            .into_iter()
            .filter_map(|replica| self.nodes_by_replica.get(&replica).cloned())
            .collect()
    }

    /// Returns all unreachable role-matching remote cluster members.
    ///
    /// The result can include joining or snapshot-only down members that are
    /// not routable. Keeping those observations pauses the all-reachable clock
    /// used for removed-node pruning.
    pub fn unreachable_replicas(&self) -> BTreeSet<ReplicaId> {
        self.unreachable.clone()
    }

    /// Returns whether the local node is the role-scoped replicator leader.
    ///
    /// Candidates enter after reaching `Up` and are ordered by replica address.
    /// `Leaving` and `Down` update an existing candidate, while `Exiting`
    /// deliberately retains the previous `Leaving` fact until final removal.
    /// The selected member must itself be `Up`; a lower ordered leaving member
    /// therefore blocks pruning leadership until it is removed.
    pub fn is_leader(&self) -> bool {
        self.members_by_replica
            .values()
            .min_by_key(|member| replicator_leader_key(member))
            .is_some_and(|leader| {
                leader.status == MemberStatus::Up && leader.unique_address == self.self_node
            })
    }

    fn add_member_from_status(&mut self, member: &Member) {
        let replica = ReplicaId::from(&member.unique_address);
        match member.status {
            MemberStatus::Joining => {
                self.nodes_by_replica
                    .insert(replica.clone(), member.unique_address.clone());
                self.joining.insert(replica.clone());
                self.weakly_up.remove(&replica);
                self.up.remove(&replica);
                self.exiting.remove(&replica);
            }
            MemberStatus::WeaklyUp => {
                self.nodes_by_replica
                    .insert(replica.clone(), member.unique_address.clone());
                self.joining.remove(&replica);
                self.weakly_up.insert(replica.clone());
                self.up.remove(&replica);
                self.exiting.remove(&replica);
            }
            MemberStatus::Up | MemberStatus::Leaving => {
                self.nodes_by_replica
                    .insert(replica.clone(), member.unique_address.clone());
                self.joining.remove(&replica);
                self.weakly_up.remove(&replica);
                self.up.insert(replica.clone());
                self.exiting.remove(&replica);
            }
            MemberStatus::Exiting => {
                self.nodes_by_replica
                    .insert(replica.clone(), member.unique_address.clone());
                self.joining.remove(&replica);
                self.weakly_up.remove(&replica);
                self.up.insert(replica.clone());
                self.exiting.insert(replica.clone());
            }
            MemberStatus::Down => {}
            MemberStatus::Removed => {
                self.remove_live_replica(&replica);
            }
        }
    }

    fn record_snapshot_leader_candidate(&mut self, member: &Member) {
        if matches!(member.status, MemberStatus::Up | MemberStatus::Leaving) {
            self.members_by_replica
                .insert(ReplicaId::from(&member.unique_address), member.clone());
        }
    }

    fn record_event_leader_candidate(&mut self, event: &MemberEvent) {
        let member = member_from_event(event);
        let replica = ReplicaId::from(&member.unique_address);
        match event {
            MemberEvent::Up(_) | MemberEvent::Left(_) => {
                self.members_by_replica.insert(replica, member.clone());
            }
            MemberEvent::Downed(_) => {
                if self.members_by_replica.contains_key(&replica) {
                    self.members_by_replica.insert(replica, member.clone());
                }
            }
            MemberEvent::Joined(_) | MemberEvent::WeaklyUp(_) | MemberEvent::Exited(_) => {}
            MemberEvent::Removed { .. } => {
                self.remove_member(&replica);
            }
        }
    }

    fn remove_member(&mut self, replica: &ReplicaId) {
        self.members_by_replica.remove(replica);
    }

    fn remove_live_replica(&mut self, replica: &ReplicaId) {
        self.joining.remove(replica);
        self.weakly_up.remove(replica);
        self.up.remove(replica);
        self.exiting.remove(replica);
        self.nodes_by_replica.remove(replica);
    }

    fn matches_remote(&self, member: &Member) -> bool {
        member.unique_address.address != self.self_node.address && self.matches_roles(member)
    }

    fn matches_roles(&self, member: &Member) -> bool {
        self.required_roles
            .iter()
            .all(|role| member.roles.iter().any(|member_role| member_role == role))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// A complete route/reachability view plus removals observed in one transition.
pub struct ReplicatorClusterRouteUpdate {
    /// Routable remote replicas in deterministic identity order.
    pub remote_replicas: Vec<ReplicaId>,
    /// Unreachable role-matching remote members, including non-routable states.
    pub unreachable_replicas: BTreeSet<ReplicaId>,
    /// Replicas that reached final cluster membership removal in this transition.
    pub removed_replicas: BTreeSet<ReplicaId>,
    /// Whether the local member is the role-scoped `Up` replicator leader.
    pub is_leader: bool,
}

impl ReplicatorClusterRouteUpdate {
    /// Creates a normalized route update from explicit replica collections.
    ///
    /// Remote replicas are sorted and deduplicated. The constructor does not
    /// require unreachable replicas to be routable because joining/down
    /// observations intentionally participate in pruning-clock health.
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
/// The replicator actor's observable result after applying a route update.
pub struct ReplicatorClusterRouteReport {
    /// Remote replicas installed in the replicator.
    pub remote_replicas: Vec<ReplicaId>,
    /// Unreachable role-matching members installed in the replicator.
    pub unreachable_replicas: BTreeSet<ReplicaId>,
    /// Newly recorded removed replicas; already-known removals are omitted.
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

fn replicator_leader_key(member: &Member) -> (u8, ReplicaId) {
    let status_rank = match member.status {
        MemberStatus::Up | MemberStatus::Leaving => 0,
        MemberStatus::WeaklyUp => 1,
        MemberStatus::Joining => 2,
        MemberStatus::Exiting => 3,
        MemberStatus::Down => 4,
        MemberStatus::Removed => 5,
    };
    (status_rank, ReplicaId::from(&member.unique_address))
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

    #[test]
    fn downed_member_remains_routable_and_unreachable_until_removed() {
        let self_node = node("self", 1);
        let peer = member(node("peer", 2), MemberStatus::Up, ["ddata"]);
        let peer_replica = ReplicaId::from(&peer.unique_address);
        let mut routes = ReplicatorClusterRoutes::with_required_roles(self_node, ["ddata"]);

        routes.apply_event(&ClusterEvent::Member(MemberEvent::Up(peer.clone())));
        routes.apply_event(&ClusterEvent::Reachability(ReachabilityEvent::Unreachable(
            peer.clone(),
        )));
        let downed = routes.apply_event(&ClusterEvent::Member(MemberEvent::Downed(
            peer.clone().with_status(MemberStatus::Down),
        )));

        assert_eq!(downed.remote_replicas, vec![peer_replica.clone()]);
        assert_eq!(
            downed.unreachable_replicas,
            BTreeSet::from([peer_replica.clone()])
        );
        assert!(downed.removed_replicas.is_empty());

        let removed = routes.apply_event(&ClusterEvent::Member(MemberEvent::Removed {
            member: peer.with_status(MemberStatus::Removed),
            previous_status: MemberStatus::Down,
        }));

        assert!(removed.remote_replicas.is_empty());
        assert!(removed.unreachable_replicas.is_empty());
        assert_eq!(removed.removed_replicas, BTreeSet::from([peer_replica]));
    }

    #[test]
    fn joining_and_snapshot_only_down_unreachability_pause_the_role_view() {
        let self_node = node("self", 1);
        let joining = member(node("joining", 2), MemberStatus::Joining, ["ddata"]);
        let down = member(node("down", 3), MemberStatus::Down, ["ddata"]);
        let state = CurrentClusterState {
            members: vec![
                member(self_node.clone(), MemberStatus::Up, ["ddata"]),
                joining.clone(),
                down.clone(),
            ],
            unreachable: vec![joining.clone(), down.clone()],
            seen_by: HashSet::new(),
            leader: Some(self_node.clone()),
            role_leaders: HashMap::new(),
            member_tombstones: HashSet::new(),
        };

        let routes = ReplicatorClusterRoutes::from_current_state(self_node, &state, ["ddata"]);

        assert!(routes.remote_replicas().is_empty());
        assert_eq!(
            routes.unreachable_replicas(),
            BTreeSet::from([
                ReplicaId::from(&down.unique_address),
                ReplicaId::from(&joining.unique_address),
            ])
        );
    }

    #[test]
    fn replicator_leader_is_derived_from_all_matching_roles_not_global_leader() {
        let self_node = node("b-self", 1);
        let matching_peer = member(node("z-peer", 2), MemberStatus::Up, ["ddata", "blue"]);
        let global_leader = member(node("a-global", 3), MemberStatus::Up, ["other"]);
        let state = CurrentClusterState {
            members: vec![
                member(self_node.clone(), MemberStatus::Up, ["ddata", "blue"]),
                matching_peer,
                global_leader.clone(),
            ],
            unreachable: vec![],
            seen_by: HashSet::new(),
            leader: Some(global_leader.unique_address),
            role_leaders: HashMap::new(),
            member_tombstones: HashSet::new(),
        };
        let routes =
            ReplicatorClusterRoutes::from_current_state(self_node, &state, ["ddata", "blue"]);

        assert!(routes.is_leader());

        let later_self = node("z-self", 4);
        let earlier_peer = member(node("b-peer", 5), MemberStatus::Up, ["ddata", "blue"]);
        let state = CurrentClusterState {
            members: vec![
                member(later_self.clone(), MemberStatus::Up, ["ddata", "blue"]),
                earlier_peer,
            ],
            unreachable: vec![],
            seen_by: HashSet::new(),
            leader: Some(later_self.clone()),
            role_leaders: HashMap::new(),
            member_tombstones: HashSet::new(),
        };
        let mut routes = ReplicatorClusterRoutes::from_current_state(
            later_self.clone(),
            &state,
            ["ddata", "blue"],
        );

        assert!(!routes.is_leader());
        let update = routes.apply_event(&ClusterEvent::LeaderChanged {
            leader: Some(later_self),
        });
        assert!(!update.is_leader);
    }

    #[test]
    fn exiting_lower_order_candidate_blocks_leadership_until_removal() {
        let self_node = node("z-self", 1);
        let peer = member(node("a-peer", 2), MemberStatus::Up, ["ddata"]);
        let mut routes = ReplicatorClusterRoutes::with_required_roles(self_node.clone(), ["ddata"]);
        routes.apply_event(&ClusterEvent::Member(MemberEvent::Up(member(
            self_node,
            MemberStatus::Up,
            ["ddata"],
        ))));
        routes.apply_event(&ClusterEvent::Member(MemberEvent::Up(peer.clone())));
        routes.apply_event(&ClusterEvent::Member(MemberEvent::Left(
            peer.clone().with_status(MemberStatus::Leaving),
        )));

        assert!(!routes.is_leader());

        let exited = routes.apply_event(&ClusterEvent::Member(MemberEvent::Exited(
            peer.clone().with_status(MemberStatus::Exiting),
        )));

        assert!(!exited.is_leader);
        assert_eq!(
            exited.remote_replicas,
            vec![ReplicaId::from(&peer.unique_address)]
        );

        let removed = routes.apply_event(&ClusterEvent::Member(MemberEvent::Removed {
            member: peer.with_status(MemberStatus::Removed),
            previous_status: MemberStatus::Exiting,
        }));

        assert!(removed.is_leader);
        assert!(removed.remote_replicas.is_empty());
    }

    #[test]
    fn route_snapshot_excludes_same_address_replacement_self() {
        let self_node = node("self", 1);
        let replacement_self = UniqueAddress::new(self_node.address.clone(), 2);
        let peer = member(node("peer", 3), MemberStatus::Up, ["ddata"]);
        let state = CurrentClusterState {
            members: vec![
                member(self_node.clone(), MemberStatus::Up, ["ddata"]),
                member(replacement_self, MemberStatus::Up, ["ddata"]),
                peer.clone(),
            ],
            unreachable: vec![],
            seen_by: HashSet::new(),
            leader: Some(self_node.clone()),
            role_leaders: HashMap::new(),
            member_tombstones: HashSet::new(),
        };

        let routes =
            ReplicatorClusterRoutes::from_current_state(self_node.clone(), &state, ["ddata"]);

        assert_eq!(
            routes.remote_replicas(),
            vec![ReplicaId::from(&peer.unique_address)]
        );
        assert_eq!(routes.remote_nodes(), vec![peer.unique_address]);
    }

    #[test]
    fn route_events_ignore_same_address_replacement_self() {
        let self_node = node("self", 1);
        let replacement_self = UniqueAddress::new(self_node.address.clone(), 2);
        let peer = member(node("peer", 3), MemberStatus::Up, ["ddata"]);
        let mut routes = ReplicatorClusterRoutes::with_required_roles(self_node.clone(), ["ddata"]);

        let replacement_update = routes.apply_event(&ClusterEvent::Member(MemberEvent::Up(
            member(replacement_self, MemberStatus::Up, ["ddata"]),
        )));
        assert!(replacement_update.remote_replicas.is_empty());

        routes.apply_event(&ClusterEvent::Member(MemberEvent::Up(peer.clone())));
        let removed_self = routes.apply_event(&ClusterEvent::Member(MemberEvent::Removed {
            member: member(self_node, MemberStatus::Removed, ["ddata"]),
            previous_status: MemberStatus::Up,
        }));

        assert_eq!(
            removed_self.remote_replicas,
            vec![ReplicaId::from(&peer.unique_address)]
        );
        assert!(removed_self.removed_replicas.is_empty());
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
