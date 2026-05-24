use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use kairo_actor::{ActorError, ActorRef, Context};

use crate::{EntityId, RememberShardStoreState, ShardActor, ShardId, ShardMsg, ShardRegionMsg};

pub(crate) struct LocalShardSpawner {
    shard_buffer_capacity: usize,
    remember_store: Option<LocalShardRememberStores>,
}

impl LocalShardSpawner {
    pub(crate) fn plain(shard_buffer_capacity: usize) -> Self {
        Self {
            shard_buffer_capacity,
            remember_store: None,
        }
    }

    pub(crate) fn with_local_remember_stores(
        type_name: impl Into<String>,
        shard_buffer_capacity: usize,
        remembered_entities_by_shard: BTreeMap<ShardId, BTreeSet<EntityId>>,
        timeout: Duration,
    ) -> Self {
        Self {
            shard_buffer_capacity,
            remember_store: Some(LocalShardRememberStores {
                type_name: type_name.into(),
                remembered_entities_by_shard,
                timeout,
            }),
        }
    }

    pub(crate) fn spawn<M>(
        &self,
        ctx: &Context<ShardRegionMsg<M>>,
        shard: &ShardId,
    ) -> Result<ActorRef<ShardMsg<M>>, ActorError>
    where
        M: Send + 'static,
    {
        let props = match &self.remember_store {
            Some(remember_store) => {
                let state = remember_store.store_state(shard);
                ShardActor::props_with_local_remember_store(
                    self.shard_buffer_capacity,
                    state,
                    remember_store.timeout,
                )
            }
            None => ShardActor::props(shard.clone(), self.shard_buffer_capacity),
        };
        ctx.spawn(shard_actor_name(shard), props)
    }
}

struct LocalShardRememberStores {
    type_name: String,
    remembered_entities_by_shard: BTreeMap<ShardId, BTreeSet<EntityId>>,
    timeout: Duration,
}

impl LocalShardRememberStores {
    fn store_state(&self, shard: &ShardId) -> RememberShardStoreState {
        RememberShardStoreState::with_entities(
            self.type_name.clone(),
            shard.clone(),
            self.remembered_entities_by_shard
                .get(shard)
                .cloned()
                .unwrap_or_default(),
        )
    }
}

fn shard_actor_name(shard: &str) -> String {
    let mut name = String::from("shard-");
    for byte in shard.as_bytes() {
        name.push(hex_digit(byte >> 4));
        name.push(hex_digit(byte & 0x0f));
    }
    name
}

fn hex_digit(value: u8) -> char {
    match value {
        0..=9 => (b'0' + value) as char,
        10..=15 => (b'a' + value - 10) as char,
        _ => unreachable!("hex digit must be in 0..=15"),
    }
}
