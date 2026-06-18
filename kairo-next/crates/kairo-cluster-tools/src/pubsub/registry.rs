use std::collections::{BTreeMap, BTreeSet};

use kairo_cluster::UniqueAddress;

use crate::TopicName;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PubSubRegistryKey {
    Topic { topic: TopicName },
    Group { topic: TopicName, group: String },
    Path { path: String },
}

impl PubSubRegistryKey {
    pub fn topic(topic: TopicName) -> Self {
        Self::Topic { topic }
    }

    pub fn group(topic: TopicName, group: impl Into<String>) -> Self {
        Self::Group {
            topic,
            group: group.into(),
        }
    }

    pub fn path(path: impl Into<String>) -> Self {
        Self::Path { path: path.into() }
    }

    pub fn topic_name(&self) -> Option<&TopicName> {
        match self {
            Self::Topic { topic } | Self::Group { topic, .. } => Some(topic),
            Self::Path { .. } => None,
        }
    }

    pub fn group_name(&self) -> Option<&str> {
        match self {
            Self::Topic { .. } => None,
            Self::Group { group, .. } => Some(group),
            Self::Path { .. } => None,
        }
    }

    pub fn path_name(&self) -> Option<&str> {
        match self {
            Self::Path { path } => Some(path),
            Self::Topic { .. } | Self::Group { .. } => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubSubRegistryEntry {
    pub version: u64,
    pub key: PubSubRegistryKey,
    pub present: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubSubBucket {
    pub owner: UniqueAddress,
    pub version: u64,
    pub entries: BTreeMap<PubSubRegistryKey, PubSubRegistryEntry>,
}

impl PubSubBucket {
    fn new(owner: UniqueAddress) -> Self {
        Self {
            owner,
            version: 0,
            entries: BTreeMap::new(),
        }
    }

    fn is_present(&self, key: &PubSubRegistryKey) -> bool {
        self.entries.get(key).is_some_and(|entry| entry.present)
    }

    fn put(&mut self, key: PubSubRegistryKey, present: bool) {
        self.version += 1;
        self.entries.insert(
            key.clone(),
            PubSubRegistryEntry {
                version: self.version,
                key,
                present,
            },
        );
    }

    fn merge(&mut self, incoming: &PubSubBucket) {
        self.version = self.version.max(incoming.version);
        for (key, incoming_entry) in &incoming.entries {
            let should_replace = self
                .entries
                .get(key)
                .is_none_or(|current| incoming_entry.version > current.version);
            if should_replace {
                self.entries.insert(key.clone(), incoming_entry.clone());
            }
        }
    }

    fn delta_since(&self, seen_version: u64, remaining_entries: usize) -> Option<Self> {
        if self.version <= seen_version || remaining_entries == 0 {
            return None;
        }
        let entries: BTreeMap<_, _> = self
            .entries
            .iter()
            .filter(|(_, entry)| entry.version > seen_version)
            .take(remaining_entries)
            .map(|(key, entry)| (key.clone(), entry.clone()))
            .collect();
        if entries.is_empty() {
            None
        } else {
            let version = entries
                .values()
                .map(|entry| entry.version)
                .max()
                .unwrap_or(self.version);
            Some(Self {
                owner: self.owner.clone(),
                version,
                entries,
            })
        }
    }

    fn prune_tombstones_older_than(&mut self, retained_version_gap: u64) {
        let current_version = self.version;
        self.entries.retain(|_, entry| {
            entry.present || current_version.saturating_sub(entry.version) <= retained_version_gap
        });
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubSubRegistryDelta {
    pub buckets: Vec<PubSubBucket>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubSubRegistryState {
    self_node: UniqueAddress,
    buckets: BTreeMap<String, PubSubBucket>,
}

impl PubSubRegistryState {
    pub fn new(self_node: UniqueAddress) -> Self {
        let mut state = Self {
            self_node: self_node.clone(),
            buckets: BTreeMap::new(),
        };
        state
            .buckets
            .insert(node_key(&self_node), PubSubBucket::new(self_node));
        state
    }

    pub fn self_node(&self) -> &UniqueAddress {
        &self.self_node
    }

    pub fn versions(&self) -> BTreeMap<String, u64> {
        self.buckets
            .iter()
            .map(|(owner, bucket)| (owner.clone(), bucket.version))
            .collect()
    }

    pub fn bucket(&self, owner: &UniqueAddress) -> Option<&PubSubBucket> {
        self.buckets.get(&node_key(owner))
    }

    pub fn remove_node(&mut self, owner: &UniqueAddress) {
        if owner != &self.self_node {
            self.buckets.remove(&node_key(owner));
        }
    }

    pub fn register_local_topic(&mut self, topic: TopicName) {
        self.put_local(PubSubRegistryKey::topic(topic), true);
    }

    pub fn unregister_local_topic(&mut self, topic: TopicName) {
        self.put_local(PubSubRegistryKey::topic(topic), false);
    }

    pub fn register_local_group(&mut self, topic: TopicName, group: impl Into<String>) {
        let group = group.into();
        self.register_local_topic(topic.clone());
        self.put_local(PubSubRegistryKey::group(topic, group), true);
    }

    pub fn unregister_local_group(&mut self, topic: TopicName, group: impl Into<String>) {
        self.put_local(PubSubRegistryKey::group(topic, group), false);
    }

    pub fn register_local_path(&mut self, path: impl Into<String>) {
        self.put_local(PubSubRegistryKey::path(path), true);
    }

    pub fn unregister_local_path(&mut self, path: impl Into<String>) {
        self.put_local(PubSubRegistryKey::path(path), false);
    }

    pub fn merge_delta(&mut self, delta: PubSubRegistryDelta) {
        for incoming in delta.buckets {
            if incoming.owner.address == self.self_node.address {
                continue;
            }
            self.buckets
                .entry(node_key(&incoming.owner))
                .or_insert_with(|| PubSubBucket::new(incoming.owner.clone()))
                .merge(&incoming);
        }
    }

    pub fn collect_delta(
        &self,
        peer_versions: &BTreeMap<String, u64>,
        max_entries: usize,
    ) -> PubSubRegistryDelta {
        let mut remaining = max_entries;
        let mut buckets = Vec::new();
        for (owner, bucket) in &self.buckets {
            if remaining == 0 {
                break;
            }
            let seen_version = peer_versions.get(owner).copied().unwrap_or(0);
            if let Some(delta_bucket) = bucket.delta_since(seen_version, remaining) {
                remaining -= delta_bucket.entries.len();
                buckets.push(delta_bucket);
            }
        }
        PubSubRegistryDelta { buckets }
    }

    pub fn prune_tombstones_older_than(&mut self, retained_version_gap: u64) {
        for bucket in self.buckets.values_mut() {
            bucket.prune_tombstones_older_than(retained_version_gap);
        }
    }

    pub fn broadcast_targets(&self, topic: &TopicName, include_self: bool) -> Vec<UniqueAddress> {
        let key = PubSubRegistryKey::topic(topic.clone());
        self.buckets
            .values()
            .filter(|bucket| include_self || bucket.owner != self.self_node)
            .filter(|bucket| bucket.is_present(&key))
            .map(|bucket| bucket.owner.clone())
            .collect()
    }

    pub fn one_per_group_targets(&self, topic: &TopicName) -> BTreeMap<String, UniqueAddress> {
        let mut groups: BTreeMap<String, Vec<UniqueAddress>> = BTreeMap::new();
        for bucket in self.buckets.values() {
            for entry in bucket.entries.values() {
                if !entry.present || entry.key.topic_name() != Some(topic) {
                    continue;
                }
                if let Some(group) = entry.key.group_name() {
                    groups
                        .entry(group.to_string())
                        .or_default()
                        .push(bucket.owner.clone());
                }
            }
        }
        groups
            .into_iter()
            .filter_map(|(group, mut nodes)| {
                nodes.sort_by_key(UniqueAddress::ordering_key);
                nodes.into_iter().next().map(|node| (group, node))
            })
            .collect()
    }

    pub fn path_targets(&self, path: &str, include_self: bool) -> Vec<UniqueAddress> {
        let key = PubSubRegistryKey::path(path);
        self.buckets
            .values()
            .filter(|bucket| include_self || bucket.owner != self.self_node)
            .filter(|bucket| bucket.is_present(&key))
            .map(|bucket| bucket.owner.clone())
            .collect()
    }

    pub fn current_topics(&self) -> BTreeSet<TopicName> {
        self.buckets
            .values()
            .flat_map(|bucket| bucket.entries.values())
            .filter(|entry| entry.present)
            .filter_map(|entry| entry.key.topic_name().cloned())
            .collect()
    }

    fn put_local(&mut self, key: PubSubRegistryKey, present: bool) {
        let owner = self.self_node.clone();
        self.buckets
            .entry(node_key(&owner))
            .or_insert_with(|| PubSubBucket::new(owner))
            .put(key, present);
    }
}

fn node_key(node: &UniqueAddress) -> String {
    node.ordering_key()
}
