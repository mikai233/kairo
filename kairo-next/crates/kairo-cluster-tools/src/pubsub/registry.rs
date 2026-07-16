#![deny(missing_docs)]

use std::collections::{BTreeMap, BTreeSet};

use kairo_cluster::UniqueAddress;

use crate::TopicName;

/// Stable logical key stored in a node-owned pubsub registry bucket.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PubSubRegistryKey {
    /// Presence of direct subscribers for a topic on the owning node.
    Topic {
        /// Topic identity.
        topic: TopicName,
    },
    /// Presence of a named subscriber group for a topic on the owning node.
    Group {
        /// Topic identity.
        topic: TopicName,
        /// Group identity within the topic.
        group: String,
    },
    /// Presence of an actor registered at a logical application path.
    Path {
        /// Address-independent logical actor path.
        path: String,
    },
}

impl PubSubRegistryKey {
    /// Creates a direct-topic presence key.
    pub fn topic(topic: TopicName) -> Self {
        Self::Topic { topic }
    }

    /// Creates a named-group presence key.
    pub fn group(topic: TopicName, group: impl Into<String>) -> Self {
        Self::Group {
            topic,
            group: group.into(),
        }
    }

    /// Creates a logical-path presence key.
    pub fn path(path: impl Into<String>) -> Self {
        Self::Path { path: path.into() }
    }

    /// Returns the topic for topic/group keys, or `None` for path keys.
    pub fn topic_name(&self) -> Option<&TopicName> {
        match self {
            Self::Topic { topic } | Self::Group { topic, .. } => Some(topic),
            Self::Path { .. } => None,
        }
    }

    /// Returns the group for a group key, or `None` otherwise.
    pub fn group_name(&self) -> Option<&str> {
        match self {
            Self::Topic { .. } => None,
            Self::Group { group, .. } => Some(group),
            Self::Path { .. } => None,
        }
    }

    /// Returns the logical path for a path key, or `None` otherwise.
    pub fn path_name(&self) -> Option<&str> {
        match self {
            Self::Path { path } => Some(path),
            Self::Topic { .. } | Self::Group { .. } => None,
        }
    }
}

/// Versioned present value or removal tombstone for one registry key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubSubRegistryEntry {
    /// Owner-local monotonic version assigned to this key update.
    pub version: u64,
    /// Logical registration key.
    pub key: PubSubRegistryKey,
    /// `true` for presence and `false` for a removal tombstone.
    pub present: bool,
}

/// Versioned registry entries owned by one exact cluster incarnation.
///
/// A delta bucket may carry a lower `version` than the owner's complete bucket
/// when a bounded response includes only an older prefix of unseen entries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubSubBucket {
    /// Exact node incarnation that owns all entries in this bucket.
    pub owner: UniqueAddress,
    /// Highest entry version represented by this bucket payload.
    pub version: u64,
    /// Entries indexed in deterministic logical-key order.
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
        let mut eligible: Vec<_> = self
            .entries
            .iter()
            .filter(|(_, entry)| entry.version > seen_version)
            .collect();
        eligible.sort_by(|(left_key, left_entry), (right_key, right_entry)| {
            left_entry
                .version
                .cmp(&right_entry.version)
                .then_with(|| left_key.cmp(right_key))
        });
        let entries: BTreeMap<_, _> = eligible
            .into_iter()
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

/// Bounded collection of node-owned registry bucket updates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubSubRegistryDelta {
    /// Bucket updates in deterministic owner order when locally collected.
    pub buckets: Vec<PubSubBucket>,
}

/// Convergent versioned pubsub registrations known by one node.
///
/// Each node is authoritative for its own bucket. Remote deltas merge only
/// newer per-key versions, retain removal tombstones, and cannot overwrite a
/// bucket whose canonical address matches the receiving node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubSubRegistryState {
    self_node: UniqueAddress,
    buckets: BTreeMap<String, PubSubBucket>,
}

impl PubSubRegistryState {
    /// Creates registry state with an empty bucket owned by `self_node`.
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

    /// Returns the exact local cluster incarnation.
    pub fn self_node(&self) -> &UniqueAddress {
        &self.self_node
    }

    /// Returns the highest known bucket version by stable owner ordering key.
    pub fn versions(&self) -> BTreeMap<String, u64> {
        self.buckets
            .iter()
            .map(|(owner, bucket)| (owner.clone(), bucket.version))
            .collect()
    }

    /// Returns the bucket owned by `owner`, when known.
    pub fn bucket(&self, owner: &UniqueAddress) -> Option<&PubSubBucket> {
        self.buckets.get(&node_key(owner))
    }

    /// Removes a remote node's complete bucket while preserving the local bucket.
    pub fn remove_node(&mut self, owner: &UniqueAddress) {
        if owner != &self.self_node {
            self.buckets.remove(&node_key(owner));
        }
    }

    /// Records local direct-topic presence with a new owner-local version.
    pub fn register_local_topic(&mut self, topic: TopicName) {
        self.put_local(PubSubRegistryKey::topic(topic), true);
    }

    /// Records a local direct-topic removal tombstone.
    pub fn unregister_local_topic(&mut self, topic: TopicName) {
        self.put_local(PubSubRegistryKey::topic(topic), false);
    }

    /// Records local group presence and ensures the containing topic is present.
    pub fn register_local_group(&mut self, topic: TopicName, group: impl Into<String>) {
        let group = group.into();
        self.register_local_topic(topic.clone());
        self.put_local(PubSubRegistryKey::group(topic, group), true);
    }

    /// Records a local group removal tombstone without removing the topic key.
    pub fn unregister_local_group(&mut self, topic: TopicName, group: impl Into<String>) {
        self.put_local(PubSubRegistryKey::group(topic, group), false);
    }

    /// Records local logical-path presence.
    pub fn register_local_path(&mut self, path: impl Into<String>) {
        self.put_local(PubSubRegistryKey::path(path), true);
    }

    /// Records a local logical-path removal tombstone.
    pub fn unregister_local_path(&mut self, path: impl Into<String>) {
        self.put_local(PubSubRegistryKey::path(path), false);
    }

    /// Merges newer entries from remote buckets in `delta`.
    ///
    /// Buckets using the receiver's canonical address are ignored even when
    /// they carry another UID, preserving local ownership across reincarnation.
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

    /// Collects at most `max_entries` updates unseen by a peer.
    ///
    /// Buckets are visited in stable owner order. Within each bucket, the
    /// lowest unseen entry versions are emitted first and logical key order
    /// breaks version ties, preventing bounded gossip from skipping history.
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

    /// Drops removal tombstones older than the retained owner-version gap.
    ///
    /// Present entries are never pruned by this operation.
    pub fn prune_tombstones_older_than(&mut self, retained_version_gap: u64) {
        for bucket in self.buckets.values_mut() {
            bucket.prune_tombstones_older_than(retained_version_gap);
        }
    }

    /// Returns nodes that advertise direct subscribers for `topic`.
    ///
    /// Results follow deterministic owner order; `include_self` controls the
    /// local bucket independently of remote targets.
    pub fn broadcast_targets(&self, topic: &TopicName, include_self: bool) -> Vec<UniqueAddress> {
        let key = PubSubRegistryKey::topic(topic.clone());
        self.buckets
            .values()
            .filter(|bucket| include_self || bucket.owner != self.self_node)
            .filter(|bucket| bucket.is_present(&key))
            .map(|bucket| bucket.owner.clone())
            .collect()
    }

    /// Selects the lowest ordered advertising node for each group of `topic`.
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

    /// Returns nodes that advertise `path`, optionally including the local node.
    pub fn path_targets(&self, path: &str, include_self: bool) -> Vec<UniqueAddress> {
        let key = PubSubRegistryKey::path(path);
        self.buckets
            .values()
            .filter(|bucket| include_self || bucket.owner != self.self_node)
            .filter(|bucket| bucket.is_present(&key))
            .map(|bucket| bucket.owner.clone())
            .collect()
    }

    /// Returns every topic with a present direct or group registration.
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
