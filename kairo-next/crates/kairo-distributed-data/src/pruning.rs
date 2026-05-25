use std::collections::{BTreeMap, BTreeSet};

use crate::{ReplicaId, ReplicatorKey};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PruningState {
    Initialized(PruningInitialized),
    Performed(PruningPerformed),
}

impl PruningState {
    pub fn initialized(owner: impl Into<ReplicaId>) -> Self {
        Self::Initialized(PruningInitialized::new(owner))
    }

    pub fn performed(obsolete_at_millis: u64) -> Self {
        Self::Performed(PruningPerformed::new(obsolete_at_millis))
    }

    pub fn add_seen(&self, node: impl Into<ReplicaId>) -> Self {
        match self {
            Self::Initialized(initialized) => Self::Initialized(initialized.add_seen(node.into())),
            Self::Performed(_) => self.clone(),
        }
    }

    pub fn merge(&self, other: &Self) -> Self {
        match (self, other) {
            (Self::Performed(left), Self::Performed(right)) => {
                if left.obsolete_at_millis >= right.obsolete_at_millis {
                    self.clone()
                } else {
                    other.clone()
                }
            }
            (Self::Performed(_), _) => self.clone(),
            (_, Self::Performed(_)) => other.clone(),
            (Self::Initialized(left), Self::Initialized(right)) => {
                Self::Initialized(left.merge(right))
            }
        }
    }

    pub fn is_obsolete(&self, now_millis: u64) -> bool {
        matches!(self, Self::Performed(performed) if performed.is_obsolete(now_millis))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PruningInitialized {
    owner: ReplicaId,
    seen: BTreeSet<ReplicaId>,
}

impl PruningInitialized {
    pub fn new(owner: impl Into<ReplicaId>) -> Self {
        Self {
            owner: owner.into(),
            seen: BTreeSet::new(),
        }
    }

    pub fn owner(&self) -> &ReplicaId {
        &self.owner
    }

    pub fn seen(&self) -> &BTreeSet<ReplicaId> {
        &self.seen
    }

    pub fn add_seen(&self, node: ReplicaId) -> Self {
        if node == self.owner || self.seen.contains(&node) {
            return self.clone();
        }

        let mut seen = self.seen.clone();
        seen.insert(node);
        Self {
            owner: self.owner.clone(),
            seen,
        }
    }

    pub fn is_seen_by_all<'a>(
        &self,
        live_replicas: impl IntoIterator<Item = &'a ReplicaId>,
    ) -> bool {
        live_replicas
            .into_iter()
            .all(|replica| replica == &self.owner || self.seen.contains(replica))
    }

    fn merge(&self, other: &Self) -> Self {
        if self.owner == other.owner {
            let mut seen = self.seen.clone();
            seen.extend(other.seen.iter().cloned());
            return Self {
                owner: self.owner.clone(),
                seen,
            };
        }

        if self.owner < other.owner {
            self.clone()
        } else {
            other.clone()
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PruningPerformed {
    obsolete_at_millis: u64,
}

impl PruningPerformed {
    pub fn new(obsolete_at_millis: u64) -> Self {
        Self { obsolete_at_millis }
    }

    pub fn obsolete_at_millis(self) -> u64 {
        self.obsolete_at_millis
    }

    pub fn is_obsolete(self, now_millis: u64) -> bool {
        self.obsolete_at_millis <= now_millis
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PruningTable {
    states: BTreeMap<ReplicaId, PruningState>,
}

impl PruningTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.states.len()
    }

    pub fn is_empty(&self) -> bool {
        self.states.is_empty()
    }

    pub fn states(&self) -> &BTreeMap<ReplicaId, PruningState> {
        &self.states
    }

    pub fn get(&self, removed: &ReplicaId) -> Option<&PruningState> {
        self.states.get(removed)
    }

    pub fn initialize(&mut self, removed: ReplicaId, owner: ReplicaId) -> bool {
        let next = PruningState::initialized(owner);
        self.set_state(removed, next)
    }

    pub fn mark_seen(&mut self, removed: &ReplicaId, seen_by: ReplicaId) -> bool {
        let Some(current) = self.states.get(removed).cloned() else {
            return false;
        };
        self.set_state(removed.clone(), current.add_seen(seen_by))
    }

    pub fn mark_all_seen_by(&mut self, seen_by: ReplicaId) -> bool {
        let mut changed = false;
        let removed_nodes = self.states.keys().cloned().collect::<Vec<_>>();
        for removed in removed_nodes {
            changed |= self.mark_seen(&removed, seen_by.clone());
        }
        changed
    }

    pub fn ready_to_perform<'a>(
        &self,
        owner: &ReplicaId,
        live_replicas: impl IntoIterator<Item = &'a ReplicaId>,
    ) -> BTreeSet<ReplicaId> {
        let live_replicas = live_replicas.into_iter().cloned().collect::<Vec<_>>();
        self.states
            .iter()
            .filter_map(|(removed, state)| match state {
                PruningState::Initialized(initialized)
                    if initialized.owner() == owner
                        && initialized.is_seen_by_all(live_replicas.iter()) =>
                {
                    Some(removed.clone())
                }
                _ => None,
            })
            .collect()
    }

    pub fn mark_performed(&mut self, removed: ReplicaId, obsolete_at_millis: u64) -> bool {
        self.set_state(removed, PruningState::performed(obsolete_at_millis))
    }

    pub fn remove_obsolete_performed(&mut self, now_millis: u64) -> BTreeSet<ReplicaId> {
        let obsolete = self
            .states
            .iter()
            .filter_map(|(removed, state)| state.is_obsolete(now_millis).then_some(removed.clone()))
            .collect::<BTreeSet<_>>();
        for removed in &obsolete {
            self.states.remove(removed);
        }
        obsolete
    }

    pub fn merge(&self, other: &Self) -> Self {
        let mut states = other.states.clone();
        for (removed, this_state) in &self.states {
            states
                .entry(removed.clone())
                .and_modify(|that_state| *that_state = this_state.merge(that_state))
                .or_insert_with(|| this_state.clone());
        }
        Self { states }
    }

    pub fn merge_without_obsolete(&self, other: &Self, now_millis: u64) -> Self {
        let mut merged = self.merge(other);
        merged.remove_obsolete_performed(now_millis);
        merged
    }

    fn set_state(&mut self, removed: ReplicaId, next: PruningState) -> bool {
        let changed = self.states.get(&removed) != Some(&next);
        self.states.insert(removed, next);
        changed
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RemovedNodePruningTracker {
    removed_nodes: BTreeMap<ReplicaId, u64>,
}

impl RemovedNodePruningTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn removed_nodes(&self) -> &BTreeMap<ReplicaId, u64> {
        &self.removed_nodes
    }

    pub fn contains(&self, node: &ReplicaId) -> bool {
        self.removed_nodes.contains_key(node)
    }

    pub fn record_removed(&mut self, node: ReplicaId, all_reachable_time_nanos: u64) -> bool {
        self.removed_nodes
            .insert(node, all_reachable_time_nanos)
            .is_none()
    }

    pub fn forget_removed(&mut self, node: &ReplicaId) -> bool {
        self.removed_nodes.remove(node).is_some()
    }

    pub fn record_unknown_modified_nodes<'a>(
        &mut self,
        modified_by: impl IntoIterator<Item = &'a ReplicaId>,
        known_nodes: &BTreeSet<ReplicaId>,
        self_node: &ReplicaId,
        all_reachable_time_nanos: u64,
    ) -> BTreeSet<ReplicaId> {
        let mut recorded = BTreeSet::new();
        for node in modified_by {
            if node != self_node
                && !known_nodes.contains(node)
                && self.record_removed(node.clone(), all_reachable_time_nanos)
            {
                recorded.insert(node.clone());
            }
        }
        recorded
    }

    pub fn ready_to_initialize(
        &self,
        all_reachable_time_nanos: u64,
        max_pruning_dissemination_nanos: u64,
    ) -> BTreeSet<ReplicaId> {
        self.removed_nodes
            .iter()
            .filter_map(|(node, first_seen_at)| {
                let elapsed = all_reachable_time_nanos.saturating_sub(*first_seen_at);
                (elapsed > max_pruning_dissemination_nanos).then_some(node.clone())
            })
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemovedNodePruningTick {
    pub self_replica: ReplicaId,
    pub live_replicas: BTreeSet<ReplicaId>,
    pub unreachable_replicas: BTreeSet<ReplicaId>,
    pub all_reachable_time_nanos: u64,
    pub max_pruning_dissemination_nanos: u64,
    pub now_millis: u64,
    pub pruning_marker_ttl_millis: u64,
    pub is_leader: bool,
}

impl RemovedNodePruningTick {
    pub fn pruning_performed(&self) -> PruningPerformed {
        PruningPerformed::new(
            self.now_millis
                .saturating_add(self.pruning_marker_ttl_millis),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RemovedNodePruningTickReport {
    pub skipped_unreachable: bool,
    pub collected_removed: BTreeSet<ReplicaId>,
    pub initialized: BTreeSet<ReplicatorKey>,
    pub performed: BTreeSet<ReplicatorKey>,
    pub obsolete_markers: BTreeSet<ReplicatorKey>,
    pub forgotten_removed: BTreeSet<ReplicaId>,
    pub failures: Vec<RemovedNodePruningFailure>,
}

impl RemovedNodePruningTickReport {
    pub fn skipped_unreachable() -> Self {
        Self {
            skipped_unreachable: true,
            ..Self::default()
        }
    }

    pub fn changed_key_count(&self) -> usize {
        self.initialized
            .union(&self.performed)
            .chain(self.obsolete_markers.iter())
            .collect::<BTreeSet<_>>()
            .len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemovedNodePruningFailure {
    pub key: ReplicatorKey,
    pub removed: ReplicaId,
    pub reason: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn replica(id: &str) -> ReplicaId {
        ReplicaId::new(id)
    }

    #[test]
    fn initialized_add_seen_ignores_owner_and_duplicates() {
        let owner = replica("owner");
        let peer = replica("peer");
        let initialized = PruningInitialized::new(owner.clone());

        let with_owner = initialized.add_seen(owner);
        assert!(with_owner.seen().is_empty());

        let with_peer = with_owner.add_seen(peer.clone()).add_seen(peer.clone());
        assert_eq!(
            with_peer.seen().iter().cloned().collect::<Vec<_>>(),
            vec![peer]
        );
    }

    #[test]
    fn pruning_state_merge_preserves_pekko_ordering_rules() {
        let removed = replica("removed");
        let owner_a = replica("a");
        let owner_b = replica("b");
        let peer = replica("peer");

        let mut left = PruningTable::new();
        left.initialize(removed.clone(), owner_a.clone());
        left.mark_seen(&removed, peer.clone());

        let mut same_owner = PruningTable::new();
        same_owner.initialize(removed.clone(), owner_a);
        same_owner.mark_seen(&removed, replica("other"));
        let merged = left.merge(&same_owner);
        let PruningState::Initialized(initialized) = merged.get(&removed).unwrap() else {
            panic!("expected initialized pruning state");
        };
        assert!(initialized.seen().contains(&peer));
        assert!(initialized.seen().contains(&replica("other")));

        let mut conflicting_owner = PruningTable::new();
        conflicting_owner.initialize(removed.clone(), owner_b);
        let merged = merged.merge(&conflicting_owner);
        let PruningState::Initialized(initialized) = merged.get(&removed).unwrap() else {
            panic!("expected initialized pruning state");
        };
        assert_eq!(initialized.owner(), &replica("a"));

        let mut performed = PruningTable::new();
        performed.mark_performed(removed.clone(), 20);
        let merged = merged.merge(&performed);
        assert_eq!(
            merged.get(&removed),
            Some(&PruningState::Performed(PruningPerformed::new(20)))
        );
    }

    #[test]
    fn ready_to_perform_requires_all_live_replicas_to_have_seen_marker() {
        let removed = replica("removed");
        let owner = replica("owner");
        let peer_a = replica("peer-a");
        let peer_b = replica("peer-b");
        let live = [peer_a.clone(), peer_b.clone()];

        let mut table = PruningTable::new();
        table.initialize(removed.clone(), owner.clone());
        table.mark_seen(&removed, peer_a);

        assert!(table.ready_to_perform(&owner, live.iter()).is_empty());
        table.mark_seen(&removed, peer_b);
        assert_eq!(
            table.ready_to_perform(&owner, live.iter()),
            BTreeSet::from([removed])
        );
    }

    #[test]
    fn performed_marker_obsolete_time_is_inclusive() {
        let mut table = PruningTable::new();
        let removed = replica("removed");
        table.mark_performed(removed.clone(), 100);

        assert!(table.remove_obsolete_performed(99).is_empty());
        assert_eq!(
            table.remove_obsolete_performed(100),
            BTreeSet::from([removed])
        );
        assert!(table.is_empty());
    }

    #[test]
    fn removed_node_tracker_uses_all_reachable_clock_threshold() {
        let mut tracker = RemovedNodePruningTracker::new();
        let removed = replica("removed");
        tracker.record_removed(removed.clone(), 10);

        assert!(tracker.ready_to_initialize(20, 10).is_empty());
        assert_eq!(
            tracker.ready_to_initialize(21, 10),
            BTreeSet::from([removed])
        );
    }

    #[test]
    fn removed_node_tracker_collects_unknown_modified_replicas() {
        let mut tracker = RemovedNodePruningTracker::new();
        let self_node = replica("self");
        let known = BTreeSet::from([replica("known")]);
        let unknown = replica("unknown");
        let modified_by = [self_node.clone(), replica("known"), unknown.clone()];

        let recorded =
            tracker.record_unknown_modified_nodes(modified_by.iter(), &known, &self_node, 42);

        assert_eq!(recorded, BTreeSet::from([unknown.clone()]));
        assert_eq!(tracker.removed_nodes().get(&unknown), Some(&42));
    }
}
