#![deny(missing_docs)]

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Display, Formatter};

use kairo_remote::{RemoteAssociationAddress, RemoteError};

use crate::{
    ClusterEvent, CurrentClusterState, Member, MemberEvent, MemberStatus, Reachability,
    ReachabilityEvent, ReachabilityStatus, UniqueAddress,
};

#[derive(Debug)]
/// Failure while deriving a remote association target from cluster membership.
pub enum ClusterAssociationPeerError {
    /// A non-self cluster member has no host and therefore cannot be dialed remotely.
    MissingRemoteHost {
        /// Stable diagnostic identity of the member.
        node: String,
    },
    /// The remote association address is invalid.
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

/// Result of deriving or reconciling cluster-managed association peers.
pub type ClusterAssociationPeerResult<T> = Result<T, ClusterAssociationPeerError>;

#[derive(Debug, Clone, PartialEq, Eq)]
/// A cluster member paired with the transport address used to reach its actor system.
pub struct ClusterAssociationPeerTarget {
    node: UniqueAddress,
    association: RemoteAssociationAddress,
}

impl ClusterAssociationPeerTarget {
    /// Derives a dialable transport target from the member's canonical address.
    ///
    /// Local-only addresses are rejected because cluster peers must be remotely reachable.
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

    /// Returns the exact member incarnation represented by this target.
    pub fn node(&self) -> &UniqueAddress {
        &self.node
    }

    /// Returns the actor-system transport address used for association management.
    pub fn association(&self) -> &RemoteAssociationAddress {
        &self.association
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Transport intent emitted when the membership-derived peer set changes.
pub enum ClusterAssociationPeerChange {
    /// Establish or adopt an association for the target member.
    Dial(ClusterAssociationPeerTarget),
    /// Remove the managed association for the target member.
    Remove(ClusterAssociationPeerTarget),
}

#[derive(Debug, Clone)]
/// Deterministic projection from cluster snapshots and events to remote peer intent.
///
/// Gossip remains the membership authority. This state only tracks the peers implied by that
/// membership view, excludes every incarnation at the local canonical address, and emits removals
/// before dials so an address can safely move to a replacement UID.
pub struct ClusterAssociationPeerState {
    self_node: UniqueAddress,
    members: BTreeMap<String, Member>,
    locally_unreachable: BTreeSet<String>,
    active: BTreeMap<String, ClusterAssociationPeerTarget>,
    retain_unreachable: bool,
}

impl ClusterAssociationPeerState {
    /// Creates an empty projection for `self_node` that excludes locally unreachable peers.
    pub fn new(self_node: UniqueAddress) -> Self {
        Self {
            self_node,
            members: BTreeMap::new(),
            locally_unreachable: BTreeSet::new(),
            active: BTreeMap::new(),
            retain_unreachable: false,
        }
    }

    /// Controls whether locally unreachable members remain desired transport peers.
    ///
    /// Retention is useful in composed cluster runtimes because keeping the association gives
    /// heartbeat traffic a route on which to observe recovery. It does not make the member
    /// reachable in gossip.
    pub fn with_unreachable_peers_retained(mut self, retain: bool) -> Self {
        self.retain_unreachable = retain;
        self
    }

    /// Returns the local node identity used for self filtering and reachability observation.
    pub fn self_node(&self) -> &UniqueAddress {
        &self.self_node
    }

    /// Returns the current desired peer targets in deterministic member order.
    pub fn active_targets(&self) -> Vec<ClusterAssociationPeerTarget> {
        self.active.values().cloned().collect()
    }

    /// Replaces the projected membership view and returns the required transport changes.
    ///
    /// Only the snapshot's locally unreachable set is considered; observations made by other
    /// members do not govern this node's transport routes.
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

    /// Applies one cluster-domain event and returns the required transport changes.
    ///
    /// Removing the local canonical address clears every desired peer. A full reachability event
    /// replaces the local observer row rather than accumulating stale subjects.
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
                || (!self.retain_unreachable && self.locally_unreachable.contains(&key))
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
    fn composed_mode_retains_unreachable_peer_for_heartbeat_recovery() {
        let self_node = node("self", 2551, 1);
        let peer = node("peer", 2552, 2);
        let mut peers =
            ClusterAssociationPeerState::new(self_node).with_unreachable_peers_retained(true);
        peers
            .apply_snapshot(state(vec![member(peer.clone())], vec![]))
            .unwrap();

        let changes = peers
            .apply_event(ClusterEvent::Reachability(ReachabilityEvent::Unreachable(
                member(peer.clone()),
            )))
            .unwrap();

        assert!(changes.is_empty());
        assert_eq!(peers.active_targets()[0].node(), &peer);
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
