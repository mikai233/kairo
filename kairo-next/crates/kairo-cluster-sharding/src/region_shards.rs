use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use kairo_actor::{ActorError, ActorRef, Context};

use crate::{
    EntityActorFactory, EntityId, EntityShardActor, RememberShardStoreState, ShardActor, ShardId,
    ShardMsg, ShardRegionMsg,
};

type SpawnShard<M> = dyn Fn(&Context<ShardRegionMsg<M>>, &ShardId, usize) -> Result<ActorRef<ShardMsg<M>>, ActorError>
    + Send
    + Sync;

pub(crate) struct LocalShardSpawner<M>
where
    M: Send + 'static,
{
    shard_buffer_capacity: usize,
    spawn: Arc<SpawnShard<M>>,
}

impl<M> LocalShardSpawner<M>
where
    M: Send + 'static,
{
    pub(crate) fn plain(shard_buffer_capacity: usize) -> Self {
        Self {
            shard_buffer_capacity,
            spawn: Arc::new(|ctx, shard, shard_buffer_capacity| {
                ctx.spawn(
                    shard_actor_name(shard),
                    ShardActor::props(shard.clone(), shard_buffer_capacity),
                )
            }),
        }
    }

    pub(crate) fn entity_backed(
        shard_buffer_capacity: usize,
        entity_factory: EntityActorFactory<M>,
    ) -> Self
    where
        M: Clone,
    {
        Self {
            shard_buffer_capacity,
            spawn: Arc::new(move |ctx, shard, shard_buffer_capacity| {
                ctx.spawn(
                    shard_actor_name(shard),
                    EntityShardActor::props(
                        shard.clone(),
                        shard_buffer_capacity,
                        entity_factory.clone(),
                    ),
                )
            }),
        }
    }

    pub(crate) fn with_local_remember_stores(
        type_name: impl Into<String>,
        shard_buffer_capacity: usize,
        remembered_entities_by_shard: BTreeMap<ShardId, BTreeSet<EntityId>>,
        timeout: Duration,
    ) -> Self {
        let remember_store = LocalShardRememberStores {
            type_name: type_name.into(),
            remembered_entities_by_shard,
            timeout,
        };
        Self {
            shard_buffer_capacity,
            spawn: Arc::new(move |ctx, shard, shard_buffer_capacity| {
                let state = remember_store.store_state(shard);
                ctx.spawn(
                    shard_actor_name(shard),
                    ShardActor::props_with_local_remember_store(
                        shard_buffer_capacity,
                        state,
                        remember_store.timeout,
                    ),
                )
            }),
        }
    }

    pub(crate) fn spawn(
        &self,
        ctx: &Context<ShardRegionMsg<M>>,
        shard: &ShardId,
    ) -> Result<ActorRef<ShardMsg<M>>, ActorError> {
        (self.spawn)(ctx, shard, self.shard_buffer_capacity)
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
