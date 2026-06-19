use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Display, Formatter};

use kairo_remote::{RemoteAssociationAddress, RemoteError};

use crate::{
    ClusterEvent, CurrentClusterState, Member, MemberEvent, MemberStatus, Reachability,
    ReachabilityEvent, ReachabilityStatus, UniqueAddress,
};

#[derive(Debug)]
pub enum ClusterAssociationPeerError {
    MissingRemoteHost { node: String },
    Remote(RemoteError),
}

impl Display for ClusterAssociationPeerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingRemoteHost { node } => {
                write!(f, "cluster association peer {node} has no remote host")
            }
            Self::Remote(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for ClusterAssociationPeerError {}

impl From<RemoteError> for ClusterAssociationPeerError {
    fn from(error: RemoteError) -> Self {
        Self::Remote(error)
    }
}

pub type ClusterAssociationPeerResult<T> = Result<T, ClusterAssociationPeerError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterAssociationPeerTarget {
    node: UniqueAddress,
    association: RemoteAssociationAddress,
}

impl ClusterAssociationPeerTarget {
    pub fn new(node: UniqueAddress) -> ClusterAssociationPeerResult<Self> {
        let host =
            node.address
                .host()
                .ok_or_else(|| ClusterAssociationPeerError::MissingRemoteHost {
                    node: node.ordering_key(),
                })?;
        let association = RemoteAssociationAddress::new(
            node.address.protocol(),
            node.address.system(),
            host,
            node.address.port(),
        )?;
        Ok(Self { node, association })
    }

    pub fn node(&self) -> &UniqueAddress {
        &self.node
    }

    pub fn association(&self) -> &RemoteAssociationAddress {
        &self.association
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterAssociationPeerChange {
    Dial(ClusterAssociationPeerTarget),
    Remove(ClusterAssociationPeerTarget),
}

#[derive(Debug, Clone)]
pub struct ClusterAssociationPeerState {
    self_node: UniqueAddress,
    members: BTreeMap<String, Member>,
    locally_unreachable: BTreeSet<String>,
    active: BTreeMap<String, ClusterAssociationPeerTarget>,
}

impl ClusterAssociationPeerState {
    pub fn new(self_node: UniqueAddress) -> Self {
        Self {
            self_node,
            members: BTreeMap::new(),
            locally_unreachable: BTreeSet::new(),
            active: BTreeMap::new(),
        }
    }

    pub fn self_node(&self) -> &UniqueAddress {
        &self.self_node
    }

    pub fn active_targets(&self) -> Vec<ClusterAssociationPeerTarget> {
        self.active.values().cloned().collect()
    }

    pub fn apply_snapshot(
        &mut self,
        snapshot: CurrentClusterState,
    ) -> ClusterAssociationPeerResult<Vec<ClusterAssociationPeerChange>> {
        self.members = snapshot
            .members
            .into_iter()
            .filter(|member| !self.is_self_address(&member.unique_address))
            .map(|member| (member.unique_address.ordering_key(), member))
            .collect();
        self.locally_unreachable = snapshot
            .unreachable
            .into_iter()
            .map(|member| member.unique_address.ordering_key())
            .collect();
        self.reconcile()
    }

    pub fn apply_event(
        &mut self,
        event: ClusterEvent,
    ) -> ClusterAssociationPeerResult<Vec<ClusterAssociationPeerChange>> {
        match event {
            ClusterEvent::Member(member_event) => self.apply_member_event(member_event),
            ClusterEvent::Reachability(ReachabilityEvent::Unreachable(member)) => {
                self.locally_unreachable
                    .insert(member.unique_address.ordering_key());
            }
            ClusterEvent::Reachability(ReachabilityEvent::Reachable(member)) => {
                self.locally_unreachable
                    .remove(&member.unique_address.ordering_key());
            }
            ClusterEvent::ReachabilityChanged { reachability } => {
                self.apply_reachability(reachability);
            }
            ClusterEvent::LeaderChanged { .. }
            | ClusterEvent::RoleLeaderChanged { .. }
            | ClusterEvent::SeenChanged { .. }
            | ClusterEvent::MemberTombstonesChanged { .. } => {}
        }
        self.reconcile()
    }

    fn apply_member_event(&mut self, event: MemberEvent) {
        match event {
            MemberEvent::Removed { member, .. } => {
                if self.is_self_address(&member.unique_address) {
                    self.members.clear();
                    self.locally_unreachable.clear();
                } else {
                    self.members.remove(&member.unique_address.ordering_key());
                    self.locally_unreachable
                        .remove(&member.unique_address.ordering_key());
                }
            }
            MemberEvent::Joined(member)
            | MemberEvent::WeaklyUp(member)
            | MemberEvent::Up(member)
            | MemberEvent::Left(member)
            | MemberEvent::Exited(member)
            | MemberEvent::Downed(member) => {
                if !self.is_self_address(&member.unique_address) {
                    self.members
                        .insert(member.unique_address.ordering_key(), member);
                }
            }
        }
    }

    fn apply_reachability(&mut self, reachability: Reachability) {
        self.locally_unreachable.clear();
        for member in self.members.values() {
            if reachability.status(&self.self_node, &member.unique_address)
                != ReachabilityStatus::Reachable
            {
                self.locally_unreachable
                    .insert(member.unique_address.ordering_key());
            }
        }
    }

    fn reconcile(&mut self) -> ClusterAssociationPeerResult<Vec<ClusterAssociationPeerChange>> {
        let desired = self.desired_targets()?;
        let mut changes = Vec::new();

        for (key, target) in &self.active {
            if !desired.contains_key(key) {
                changes.push(ClusterAssociationPeerChange::Remove(target.clone()));
            }
        }

        for (key, target) in &desired {
            if self.active.get(key) != Some(target) {
                changes.push(ClusterAssociationPeerChange::Dial(target.clone()));
            }
        }

        self.active = desired;
        Ok(changes)
    }

    fn desired_targets(
        &self,
    ) -> ClusterAssociationPeerResult<BTreeMap<String, ClusterAssociationPeerTarget>> {
        let mut desired = BTreeMap::new();
        for member in self.members.values() {
            let key = member.unique_address.ordering_key();
            if member.status == MemberStatus::Removed
                || self.is_self_address(&member.unique_address)
                || self.locally_unreachable.contains(&key)
            {
                continue;
            }
            desired.insert(
                key,
                ClusterAssociationPeerTarget::new(member.unique_address.clone())?,
            );
        }
        Ok(desired)
    }

    fn is_self_address(&self, node: &UniqueAddress) -> bool {
        node.address == self.self_node.address
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use kairo_actor::Address;

    use super::*;

    fn node(system: &str, port: u16, uid: u64) -> UniqueAddress {
        UniqueAddress::new(
            Address::new("kairo", system, Some("127.0.0.1".to_string()), Some(port)),
            uid,
        )
    }

    fn local_node(system: &str, uid: u64) -> UniqueAddress {
        UniqueAddress::new(Address::local(system), uid)
    }

    fn member(node: UniqueAddress) -> Member {
        Member::new(node, vec![]).with_status(MemberStatus::Up)
    }

    fn state(members: Vec<Member>, unreachable: Vec<Member>) -> CurrentClusterState {
        CurrentClusterState {
            members,
            unreachable,
            seen_by: HashSet::new(),
            leader: None,
            role_leaders: HashMap::new(),
            member_tombstones: HashSet::new(),
        }
    }

    fn dialed_nodes(changes: &[ClusterAssociationPeerChange]) -> Vec<UniqueAddress> {
        changes
            .iter()
            .filter_map(|change| match change {
                ClusterAssociationPeerChange::Dial(target) => Some(target.node().clone()),
                ClusterAssociationPeerChange::Remove(_) => None,
            })
            .collect()
    }

    fn removed_nodes(changes: &[ClusterAssociationPeerChange]) -> Vec<UniqueAddress> {
        changes
            .iter()
            .filter_map(|change| match change {
                ClusterAssociationPeerChange::Dial(_) => None,
                ClusterAssociationPeerChange::Remove(target) => Some(target.node().clone()),
            })
            .collect()
    }

    #[test]
    fn snapshot_dials_reachable_remote_members_and_excludes_self_or_unreachable() {
        let self_node = node("self", 2551, 1);
        let reachable = node("reachable", 2552, 2);
        let unreachable = node("unreachable", 2553, 3);
        let mut peers = ClusterAssociationPeerState::new(self_node.clone());

        let changes = peers
            .apply_snapshot(state(
                vec![
                    member(self_node),
                    member(reachable.clone()),
                    member(unreachable.clone()),
                ],
                vec![member(unreachable.clone())],
            ))
            .unwrap();

        assert_eq!(dialed_nodes(&changes), vec![reachable.clone()]);
        assert_eq!(peers.active_targets()[0].association().port(), Some(2552));
        assert_eq!(peers.active_targets()[0].node(), &reachable);
    }

    #[test]
    fn snapshot_excludes_replacement_incarnation_with_self_address() {
        let self_node = node("self", 2551, 1);
        let replacement_self = UniqueAddress::new(self_node.address.clone(), 11);
        let reachable = node("reachable", 2552, 2);
        let mut peers = ClusterAssociationPeerState::new(self_node);

        let changes = peers
            .apply_snapshot(state(
                vec![member(replacement_self), member(reachable.clone())],
                vec![],
            ))
            .unwrap();

        assert_eq!(dialed_nodes(&changes), vec![reachable.clone()]);
        assert_eq!(
            peers
                .active_targets()
                .into_iter()
                .map(|target| target.node().clone())
                .collect::<Vec<_>>(),
            vec![reachable]
        );
    }

    #[test]
    fn reachability_events_remove_and_redial_peer() {
        let self_node = node("self", 2551, 1);
        let peer = node("peer", 2552, 2);
        let mut peers = ClusterAssociationPeerState::new(self_node);
        peers
            .apply_snapshot(state(vec![member(peer.clone())], vec![]))
            .unwrap();

        let removed = peers
            .apply_event(ClusterEvent::Reachability(ReachabilityEvent::Unreachable(
                member(peer.clone()),
            )))
            .unwrap();
        assert_eq!(removed_nodes(&removed), vec![peer.clone()]);

        let dialed = peers
            .apply_event(ClusterEvent::Reachability(ReachabilityEvent::Reachable(
                member(peer.clone()),
            )))
            .unwrap();
        assert_eq!(dialed_nodes(&dialed), vec![peer]);
    }

    #[test]
    fn reachability_changed_uses_only_self_observer_row() {
        let self_node = node("self", 2551, 1);
        let peer = node("peer", 2552, 2);
        let other = node("other", 2553, 3);
        let mut peers = ClusterAssociationPeerState::new(self_node.clone());
        peers
            .apply_snapshot(state(
                vec![member(peer.clone()), member(other.clone())],
                vec![],
            ))
            .unwrap();

        let no_change = peers
            .apply_event(ClusterEvent::ReachabilityChanged {
                reachability: Reachability::new().unreachable(other, peer.clone()),
            })
            .unwrap();
        assert!(no_change.is_empty());

        let removed = peers
            .apply_event(ClusterEvent::ReachabilityChanged {
                reachability: Reachability::new().unreachable(self_node, peer.clone()),
            })
            .unwrap();
        assert_eq!(removed_nodes(&removed), vec![peer]);
    }

    #[test]
    fn reachability_changed_replaces_self_observer_unreachable_set() {
        let self_node = node("self", 2551, 1);
        let first_peer = node("first-peer", 2552, 2);
        let second_peer = node("second-peer", 2553, 3);
        let observer = node("observer", 2554, 4);
        let mut peers = ClusterAssociationPeerState::new(self_node.clone());
        peers
            .apply_snapshot(state(
                vec![member(first_peer.clone()), member(second_peer.clone())],
                vec![],
            ))
            .unwrap();
        peers
            .apply_event(ClusterEvent::ReachabilityChanged {
                reachability: Reachability::new()
                    .unreachable(self_node.clone(), first_peer.clone())
                    .unreachable(observer, second_peer.clone()),
            })
            .unwrap();

        let changes = peers
            .apply_event(ClusterEvent::ReachabilityChanged {
                reachability: Reachability::new()
                    .unreachable(self_node, second_peer.clone())
                    .unreachable(first_peer.clone(), second_peer.clone()),
            })
            .unwrap();

        assert_eq!(
            changes,
            vec![
                ClusterAssociationPeerChange::Remove(
                    ClusterAssociationPeerTarget::new(second_peer).unwrap()
                ),
                ClusterAssociationPeerChange::Dial(
                    ClusterAssociationPeerTarget::new(first_peer).unwrap()
                )
            ]
        );
    }

    #[test]
    fn member_removal_removes_active_peer_and_new_uid_redials() {
        let self_node = node("self", 2551, 1);
        let peer_v1 = node("peer", 2552, 2);
        let peer_v2 = node("peer", 2552, 22);
        let mut peers = ClusterAssociationPeerState::new(self_node);
        peers
            .apply_snapshot(state(vec![member(peer_v1.clone())], vec![]))
            .unwrap();

        let removed = peers
            .apply_event(ClusterEvent::Member(MemberEvent::Removed {
                member: member(peer_v1.clone()).with_status(MemberStatus::Removed),
                previous_status: MemberStatus::Up,
            }))
            .unwrap();
        assert_eq!(removed_nodes(&removed), vec![peer_v1]);

        let dialed = peers
            .apply_event(ClusterEvent::Member(MemberEvent::Up(member(
                peer_v2.clone(),
            ))))
            .unwrap();
        assert_eq!(dialed_nodes(&dialed), vec![peer_v2]);
    }

    #[test]
    fn self_member_removal_removes_all_active_peers() {
        let self_node = node("self", 2551, 1);
        let first_peer = node("first-peer", 2552, 2);
        let second_peer = node("second-peer", 2553, 3);
        let mut peers = ClusterAssociationPeerState::new(self_node.clone());
        peers
            .apply_snapshot(state(
                vec![
                    member(self_node.clone()),
                    member(first_peer.clone()),
                    member(second_peer.clone()),
                ],
                vec![],
            ))
            .unwrap();

        let removed = peers
            .apply_event(ClusterEvent::Member(MemberEvent::Removed {
                member: member(self_node).with_status(MemberStatus::Removed),
                previous_status: MemberStatus::Up,
            }))
            .unwrap();

        assert_eq!(removed_nodes(&removed), vec![first_peer, second_peer]);
        assert!(peers.active_targets().is_empty());
    }

    #[test]
    fn self_member_removal_by_address_removes_all_active_peers() {
        let self_node = node("self", 2551, 1);
        let replacement_self = UniqueAddress::new(self_node.address.clone(), 11);
        let first_peer = node("first-peer", 2552, 2);
        let second_peer = node("second-peer", 2553, 3);
        let mut peers = ClusterAssociationPeerState::new(self_node.clone());
        peers
            .apply_snapshot(state(
                vec![
                    member(self_node),
                    member(first_peer.clone()),
                    member(second_peer.clone()),
                ],
                vec![],
            ))
            .unwrap();

        let removed = peers
            .apply_event(ClusterEvent::Member(MemberEvent::Removed {
                member: member(replacement_self).with_status(MemberStatus::Removed),
                previous_status: MemberStatus::Up,
            }))
            .unwrap();

        assert_eq!(removed_nodes(&removed), vec![first_peer, second_peer]);
        assert!(peers.active_targets().is_empty());
    }

    #[test]
    fn snapshot_replaces_old_uid_before_dialing_new_uid_for_same_address() {
        let self_node = node("self", 2551, 1);
        let peer_v1 = node("peer", 2552, 2);
        let peer_v2 = node("peer", 2552, 22);
        let mut peers = ClusterAssociationPeerState::new(self_node);
        peers
            .apply_snapshot(state(vec![member(peer_v1.clone())], vec![]))
            .unwrap();

        let changes = peers
            .apply_snapshot(state(vec![member(peer_v2.clone())], vec![]))
            .unwrap();

        assert_eq!(
            changes,
            vec![
                ClusterAssociationPeerChange::Remove(
                    ClusterAssociationPeerTarget::new(peer_v1).unwrap()
                ),
                ClusterAssociationPeerChange::Dial(
                    ClusterAssociationPeerTarget::new(peer_v2).unwrap()
                )
            ]
        );
    }

    #[test]
    fn non_self_local_only_peer_is_rejected() {
        let self_node = node("self", 2551, 1);
        let mut peers = ClusterAssociationPeerState::new(self_node);
        let error = peers
            .apply_snapshot(state(vec![member(local_node("local-peer", 2))], vec![]))
            .unwrap_err();

        assert!(matches!(
            error,
            ClusterAssociationPeerError::MissingRemoteHost { .. }
        ));
    }
}
