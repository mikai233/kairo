use std::collections::{BTreeMap, BTreeSet};

use crate::{EntityId, ShardId, ShardingError};

pub const REMEMBER_ENTITY_SHARD_KEY_COUNT: usize = 5;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RememberShardUpdate {
    started: BTreeSet<EntityId>,
    stopped: BTreeSet<EntityId>,
}

impl RememberShardUpdate {
    pub fn new(
        started: impl IntoIterator<Item = EntityId>,
        stopped: impl IntoIterator<Item = EntityId>,
    ) -> Self {
        Self {
            started: started.into_iter().collect(),
            stopped: stopped.into_iter().collect(),
        }
    }

    pub fn started(&self) -> &BTreeSet<EntityId> {
        &self.started
    }

    pub fn stopped(&self) -> &BTreeSet<EntityId> {
        &self.stopped
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RememberShardUpdateDone {
    pub started: BTreeSet<EntityId>,
    pub stopped: BTreeSet<EntityId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RememberShardStoreState {
    type_name: String,
    shard_id: ShardId,
    entities_by_key: BTreeMap<usize, BTreeSet<EntityId>>,
}

impl RememberShardStoreState {
    pub fn new(type_name: impl Into<String>, shard_id: impl Into<ShardId>) -> Self {
        Self::with_entities(type_name, shard_id, std::iter::empty::<EntityId>())
    }

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

    pub fn type_name(&self) -> &str {
        &self.type_name
    }

    pub fn shard_id(&self) -> &ShardId {
        &self.shard_id
    }

    pub fn key_name(&self, index: usize) -> Result<String, ShardingError> {
        remember_entity_shard_key(&self.type_name, &self.shard_id, index)
    }

    pub fn entities_for_key(&self, index: usize) -> Option<&BTreeSet<EntityId>> {
        self.entities_by_key.get(&index)
    }

    pub fn remembered_entities(&self) -> BTreeSet<EntityId> {
        self.entities_by_key
            .values()
            .flat_map(|entities| entities.iter().cloned())
            .collect()
    }

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
pub struct RememberCoordinatorStoreState {
    shards: BTreeSet<ShardId>,
}

impl RememberCoordinatorStoreState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_shards(shards: impl IntoIterator<Item = ShardId>) -> Self {
        Self {
            shards: shards.into_iter().collect(),
        }
    }

    pub fn remembered_shards(&self) -> &BTreeSet<ShardId> {
        &self.shards
    }

    pub fn add_shard(&mut self, shard: impl Into<ShardId>) -> RememberCoordinatorUpdateDone {
        let shard = shard.into();
        self.shards.insert(shard.clone());
        RememberCoordinatorUpdateDone { shard }
    }

    pub fn get_shards(&self) -> RememberedShards {
        RememberedShards {
            shards: self.shards.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RememberCoordinatorUpdateDone {
    pub shard: ShardId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RememberedShards {
    pub shards: BTreeSet<ShardId>,
}

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

pub fn remember_entity_key_index(entity_id: &str) -> usize {
    remember_entity_key_index_for(entity_id, REMEMBER_ENTITY_SHARD_KEY_COUNT)
        .expect("default remember entity key count is non-zero")
}

pub fn remember_entity_key_index_for(
    entity_id: &str,
    key_count: usize,
) -> Result<usize, ShardingError> {
    if key_count == 0 {
        return Err(ShardingError::InvalidRememberEntityKeyCount);
    }
    let hash = java_string_hash(entity_id);
    Ok((hash % key_count as i32).unsigned_abs() as usize)
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
