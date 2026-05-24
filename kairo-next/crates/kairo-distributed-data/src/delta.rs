use std::collections::{BTreeMap, BTreeSet};

use crate::{ReplicaId, ReplicatedData, ReplicatorKey};

const DEFAULT_GOSSIP_INTERVAL_DIVISOR: usize = 5;
const MIN_NODE_SLICE_SIZE: usize = 2;
const MAX_NODE_SLICE_SIZE: usize = 10;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaPropagation<Delta> {
    entries: BTreeMap<ReplicatorKey, DeltaPropagationEntry<Delta>>,
}

impl<Delta> DeltaPropagation<Delta> {
    pub fn entries(&self) -> &BTreeMap<ReplicatorKey, DeltaPropagationEntry<Delta>> {
        &self.entries
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaPropagationEntry<Delta> {
    delta: Delta,
    from_version: u64,
    to_version: u64,
}

impl<Delta> DeltaPropagationEntry<Delta> {
    pub fn new(delta: Delta, from_version: u64, to_version: u64) -> Self {
        Self {
            delta,
            from_version,
            to_version,
        }
    }

    pub fn delta(&self) -> &Delta {
        &self.delta
    }

    pub fn from_version(&self) -> u64 {
        self.from_version
    }

    pub fn to_version(&self) -> u64 {
        self.to_version
    }
}

#[derive(Debug, Clone)]
pub struct DeltaPropagationLog<Delta> {
    nodes: Vec<ReplicaId>,
    versions: BTreeMap<ReplicatorKey, u64>,
    entries: BTreeMap<ReplicatorKey, BTreeMap<u64, Option<Delta>>>,
    sent_to_node: BTreeMap<ReplicatorKey, BTreeMap<ReplicaId, u64>>,
    node_round_robin_counter: u64,
    propagation_count: u64,
    gossip_interval_divisor: usize,
    max_delta_versions: usize,
}

impl<Delta> DeltaPropagationLog<Delta>
where
    Delta: ReplicatedData,
{
    pub fn new(nodes: impl IntoIterator<Item = ReplicaId>) -> Self {
        Self {
            nodes: sorted_unique_nodes(nodes),
            versions: BTreeMap::new(),
            entries: BTreeMap::new(),
            sent_to_node: BTreeMap::new(),
            node_round_robin_counter: 0,
            propagation_count: 0,
            gossip_interval_divisor: DEFAULT_GOSSIP_INTERVAL_DIVISOR,
            max_delta_versions: usize::MAX,
        }
    }

    pub fn with_gossip_interval_divisor(mut self, divisor: usize) -> Self {
        self.gossip_interval_divisor = divisor.max(1);
        self
    }

    pub fn with_max_delta_versions(mut self, max_delta_versions: usize) -> Self {
        self.max_delta_versions = max_delta_versions.max(1);
        self
    }

    pub fn set_nodes(&mut self, nodes: impl IntoIterator<Item = ReplicaId>) {
        let nodes = sorted_unique_nodes(nodes);
        let live: BTreeSet<_> = nodes.iter().cloned().collect();
        for sent_by_node in self.sent_to_node.values_mut() {
            sent_by_node.retain(|node, _| live.contains(node));
        }
        self.nodes = nodes;
    }

    pub fn nodes(&self) -> &[ReplicaId] {
        &self.nodes
    }

    pub fn current_version(&self, key: &ReplicatorKey) -> u64 {
        self.versions.get(key).copied().unwrap_or_default()
    }

    pub fn propagation_count(&self) -> u64 {
        self.propagation_count
    }

    pub fn has_delta_entries(&self, key: &ReplicatorKey) -> bool {
        self.entries
            .get(key)
            .is_some_and(|entries| !entries.is_empty())
    }

    pub fn record_delta(&mut self, key: ReplicatorKey, delta: Option<Delta>) -> u64 {
        let version = self.current_version(&key) + 1;
        self.versions.insert(key.clone(), version);
        self.entries.entry(key).or_default().insert(version, delta);
        version
    }

    pub fn delete_key(&mut self, key: &ReplicatorKey) {
        self.versions.remove(key);
        self.entries.remove(key);
        self.sent_to_node.remove(key);
    }

    pub fn cleanup_removed_node(&mut self, node: &ReplicaId) {
        for sent_by_node in self.sent_to_node.values_mut() {
            sent_by_node.remove(node);
        }
    }

    pub fn collect_propagations(&mut self) -> BTreeMap<ReplicaId, DeltaPropagation<Delta>> {
        self.propagation_count += 1;
        if self.nodes.is_empty() {
            return BTreeMap::new();
        }

        let selected_nodes = self.selected_nodes();
        let mut propagations = BTreeMap::new();
        let mut cache: BTreeMap<(ReplicatorKey, u64, u64), Option<Delta>> = BTreeMap::new();

        for node in selected_nodes {
            let mut entries_for_node = BTreeMap::new();
            for (key, entries) in &self.entries {
                let sent_version = self
                    .sent_to_node
                    .get(key)
                    .and_then(|sent| sent.get(&node))
                    .copied()
                    .unwrap_or_default();
                let unsent = entries_after(entries, sent_version);
                if unsent.is_empty() {
                    continue;
                }

                let from_version = *unsent.first_key_value().expect("not empty").0;
                let to_version = *unsent.last_key_value().expect("not empty").0;
                let cache_key = (key.clone(), from_version, to_version);
                let merged_delta = match cache.get(&cache_key) {
                    Some(delta) => delta.clone(),
                    None => {
                        let delta = merge_delta_group(unsent, self.max_delta_versions);
                        cache.insert(cache_key, delta.clone());
                        delta
                    }
                };

                self.sent_to_node
                    .entry(key.clone())
                    .or_default()
                    .insert(node.clone(), to_version);

                if let Some(delta) = merged_delta {
                    entries_for_node.insert(
                        key.clone(),
                        DeltaPropagationEntry::new(delta, from_version, to_version),
                    );
                }
            }

            if !entries_for_node.is_empty() {
                propagations.insert(
                    node,
                    DeltaPropagation {
                        entries: entries_for_node,
                    },
                );
            }
        }

        propagations
    }

    pub fn cleanup_delta_entries(&mut self) {
        if self.nodes.is_empty() {
            self.entries.clear();
            return;
        }

        let nodes = self.nodes.clone();
        let sent_to_node = self.sent_to_node.clone();
        for (key, entries) in &mut self.entries {
            let min_sent = smallest_version_propagated_to_all(key, &nodes, &sent_to_node);
            *entries = entries_after(entries, min_sent);
        }
    }

    fn selected_nodes(&mut self) -> Vec<ReplicaId> {
        let slice_size = node_slice_size(self.nodes.len(), self.gossip_interval_divisor);
        if self.nodes.len() <= slice_size {
            return self.nodes.clone();
        }

        let start = (self.node_round_robin_counter % self.nodes.len() as u64) as usize;
        self.node_round_robin_counter += slice_size as u64;

        (0..slice_size)
            .map(|offset| self.nodes[(start + offset) % self.nodes.len()].clone())
            .collect()
    }
}

fn sorted_unique_nodes(nodes: impl IntoIterator<Item = ReplicaId>) -> Vec<ReplicaId> {
    nodes
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn node_slice_size(all_nodes_size: usize, divisor: usize) -> usize {
    let divisor = divisor.max(1);
    let target = (all_nodes_size / divisor) + 1;
    target
        .max(MIN_NODE_SLICE_SIZE)
        .min(all_nodes_size.min(MAX_NODE_SLICE_SIZE))
}

fn entries_after<Delta>(
    entries: &BTreeMap<u64, Option<Delta>>,
    version: u64,
) -> BTreeMap<u64, Option<Delta>>
where
    Delta: Clone,
{
    entries
        .range((version + 1)..)
        .map(|(version, delta)| (*version, delta.clone()))
        .collect()
}

fn merge_delta_group<Delta>(
    entries: BTreeMap<u64, Option<Delta>>,
    max_delta_versions: usize,
) -> Option<Delta>
where
    Delta: ReplicatedData,
{
    if entries.len() >= max_delta_versions {
        return None;
    }

    let mut values = entries.into_values();
    let first = values.next()??;
    values.try_fold(first, |acc, next| next.map(|delta| acc.merge(&delta)))
}

fn smallest_version_propagated_to_all(
    key: &ReplicatorKey,
    nodes: &[ReplicaId],
    sent_to_node: &BTreeMap<ReplicatorKey, BTreeMap<ReplicaId, u64>>,
) -> u64 {
    let Some(sent_for_key) = sent_to_node.get(key) else {
        return 0;
    };
    if sent_for_key.is_empty() || nodes.iter().any(|node| !sent_for_key.contains_key(node)) {
        return 0;
    }
    sent_for_key.values().copied().min().unwrap_or_default()
}

impl<Delta> Default for DeltaPropagationLog<Delta>
where
    Delta: ReplicatedData,
{
    fn default() -> Self {
        Self::new([])
    }
}
