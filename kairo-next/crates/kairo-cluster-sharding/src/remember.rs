#![deny(missing_docs)]

//! Pure state and stable key partitioning for remembered shards and entities.
//!
//! Coordinator state records shard existence additively. Each shard stores its
//! entity identifiers across a fixed number of keys using Pekko-compatible
//! Java string hashing, so every node derives the same storage key without
//! forcing entity identifiers into business messages.

use std::collections::{BTreeMap, BTreeSet};

use crate::{EntityId, ShardId, ShardingError};

/// Fixed number of distributed-data keys used for each remembered shard.
///
/// Pekko fixes this value at five to keep individual ORSet payloads bounded.
/// It is intentionally not configurable because every node must derive the
/// same key for an entity identifier.
pub const REMEMBER_ENTITY_SHARD_KEY_COUNT: usize = 5;

#[derive(Debug, Clone, PartialEq, Eq)]
/// One atomic logical change to a shard's remembered entity identifiers.
pub struct RememberShardUpdate {
    started: BTreeSet<EntityId>,
    stopped: BTreeSet<EntityId>,
}

impl RememberShardUpdate {
    /// Creates an update, deduplicating and deterministically ordering each set.
    pub fn new(
        started: impl IntoIterator<Item = EntityId>,
        stopped: impl IntoIterator<Item = EntityId>,
    ) -> Self {
        Self {
            started: started.into_iter().collect(),
            stopped: stopped.into_iter().collect(),
        }
    }

    /// Returns entity identifiers to add to remembered state.
    pub fn started(&self) -> &BTreeSet<EntityId> {
        &self.started
    }

    /// Returns entity identifiers to remove from remembered state.
    pub fn stopped(&self) -> &BTreeSet<EntityId> {
        &self.stopped
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Acknowledges a completed remembered-entity update.
pub struct RememberShardUpdateDone {
    /// Entity identifiers that were added.
    pub started: BTreeSet<EntityId>,
    /// Entity identifiers that were removed.
    pub stopped: BTreeSet<EntityId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// In-memory model of one shard's partitioned remembered entities.
pub struct RememberShardStoreState {
    type_name: String,
    shard_id: ShardId,
    entities_by_key: BTreeMap<usize, BTreeSet<EntityId>>,
}

impl RememberShardStoreState {
    /// Creates empty remembered state for one entity type and shard.
    pub fn new(type_name: impl Into<String>, shard_id: impl Into<ShardId>) -> Self {
        Self::with_entities(type_name, shard_id, std::iter::empty::<EntityId>())
    }

    /// Creates state preloaded with remembered entity identifiers.
    ///
    /// Identifiers are assigned to the same five stable partitions used by the
    /// distributed-data shard store.
    pub fn with_entities(
        type_name: impl Into<String>,
        shard_id: impl Into<ShardId>,
        entities: impl IntoIterator<Item = EntityId>,
    ) -> Self {
        let mut state = Self {
            type_name: type_name.into(),
            shard_id: shard_id.into(),
            entities_by_key: empty_remember_entity_keys(),
        };
        for entity in entities {
            state.insert_entity(entity);
        }
        state
    }

    /// Returns the cluster-wide entity type name.
    pub fn type_name(&self) -> &str {
        &self.type_name
    }

    /// Returns the logical shard identifier.
    pub fn shard_id(&self) -> &ShardId {
        &self.shard_id
    }

    /// Derives the stable storage key for partition `index`.
    pub fn key_name(&self, index: usize) -> Result<String, ShardingError> {
        remember_entity_shard_key(&self.type_name, &self.shard_id, index)
    }

    /// Returns the remembered identifiers in partition `index`, when valid.
    pub fn entities_for_key(&self, index: usize) -> Option<&BTreeSet<EntityId>> {
        self.entities_by_key.get(&index)
    }

    /// Returns the union of all remembered entity partitions.
    pub fn remembered_entities(&self) -> BTreeSet<EntityId> {
        self.entities_by_key
            .values()
            .flat_map(|entities| entities.iter().cloned())
            .collect()
    }

    /// Applies stopped identifiers followed by started identifiers.
    ///
    /// Removing an unknown identifier is idempotent. If an identifier appears
    /// in both sets, the started operation wins and the identifier remains
    /// remembered.
    pub fn apply_update(
        &mut self,
        update: RememberShardUpdate,
    ) -> Result<RememberShardUpdateDone, ShardingError> {
        for entity in &update.stopped {
            self.remove_entity(entity);
        }
        for entity in &update.started {
            self.insert_entity(entity.clone());
        }
        Ok(RememberShardUpdateDone {
            started: update.started,
            stopped: update.stopped,
        })
    }

    fn insert_entity(&mut self, entity: EntityId) {
        let index = remember_entity_key_index(&entity);
        self.entities_by_key
            .entry(index)
            .or_default()
            .insert(entity);
    }

    fn remove_entity(&mut self, entity: &EntityId) {
        let index = remember_entity_key_index(entity);
        if let Some(entities) = self.entities_by_key.get_mut(&index) {
            entities.remove(entity);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
/// Additive in-memory model of shard identifiers known by a coordinator.
pub struct RememberCoordinatorStoreState {
    shards: BTreeSet<ShardId>,
}

impl RememberCoordinatorStoreState {
    /// Creates an empty coordinator store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a coordinator store preloaded with shard identifiers.
    pub fn with_shards(shards: impl IntoIterator<Item = ShardId>) -> Self {
        Self {
            shards: shards.into_iter().collect(),
        }
    }

    /// Returns every remembered shard identifier.
    pub fn remembered_shards(&self) -> &BTreeSet<ShardId> {
        &self.shards
    }

    /// Adds a shard idempotently and returns its acknowledgement.
    pub fn add_shard(&mut self, shard: impl Into<ShardId>) -> RememberCoordinatorUpdateDone {
        let shard = shard.into();
        self.shards.insert(shard.clone());
        RememberCoordinatorUpdateDone { shard }
    }

    /// Clones all remembered shard identifiers into a reply value.
    pub fn get_shards(&self) -> RememberedShards {
        RememberedShards {
            shards: self.shards.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Acknowledges that a coordinator store recorded one shard.
pub struct RememberCoordinatorUpdateDone {
    /// Recorded shard identifier.
    pub shard: ShardId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Reply containing every shard identifier known to a coordinator store.
pub struct RememberedShards {
    /// Remembered shard identifiers.
    pub shards: BTreeSet<ShardId>,
}

/// Derives a Pekko-compatible storage key for one shard partition.
///
/// Returns [`ShardingError::InvalidRememberEntityKeyIndex`] when `index` is
/// outside [`REMEMBER_ENTITY_SHARD_KEY_COUNT`].
pub fn remember_entity_shard_key(
    type_name: &str,
    shard_id: &str,
    index: usize,
) -> Result<String, ShardingError> {
    if index >= REMEMBER_ENTITY_SHARD_KEY_COUNT {
        return Err(ShardingError::InvalidRememberEntityKeyIndex {
            index,
            key_count: REMEMBER_ENTITY_SHARD_KEY_COUNT,
        });
    }
    Ok(format!("shard-{type_name}-{shard_id}-{index}"))
}

/// Selects one of the five stable partitions for `entity_id`.
pub fn remember_entity_key_index(entity_id: &str) -> usize {
    remember_entity_key_index_for(entity_id, REMEMBER_ENTITY_SHARD_KEY_COUNT)
        .expect("default remember entity key count is non-zero")
}

/// Selects a stable partition for `entity_id` and an explicit positive count.
///
/// The calculation uses Java's UTF-16 `String.hashCode` algorithm followed by
/// absolute modulo, matching Pekko for cross-node key compatibility. Returns
/// [`ShardingError::InvalidRememberEntityKeyCount`] for zero.
pub fn remember_entity_key_index_for(
    entity_id: &str,
    key_count: usize,
) -> Result<usize, ShardingError> {
    if key_count == 0 {
        return Err(ShardingError::InvalidRememberEntityKeyCount);
    }
    let hash = java_string_hash(entity_id);
    Ok(i64::from(hash).unsigned_abs() as usize % key_count)
}

fn empty_remember_entity_keys() -> BTreeMap<usize, BTreeSet<EntityId>> {
    (0..REMEMBER_ENTITY_SHARD_KEY_COUNT)
        .map(|index| (index, BTreeSet::new()))
        .collect()
}

fn java_string_hash(value: &str) -> i32 {
    value.encode_utf16().fold(0_i32, |hash, code_unit| {
        hash.wrapping_mul(31).wrapping_add(code_unit as i32)
    })
}
