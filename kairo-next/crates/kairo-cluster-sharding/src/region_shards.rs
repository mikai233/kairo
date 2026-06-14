use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use kairo_actor::{ActorError, ActorRef, Context};

use crate::{
    EntityActorFactory, EntityId, EntityShardActor, RememberShardStoreMsg, RememberShardStoreState,
    ShardActor, ShardId, ShardMsg, ShardRegionMsg,
};

const DEFAULT_REMEMBER_SHARD_FAILURE_BACKOFF: Duration = Duration::from_secs(10);

type SpawnShard<M> = dyn Fn(&Context<ShardRegionMsg<M>>, &ShardId, usize) -> Result<ActorRef<ShardMsg<M>>, ActorError>
    + Send
    + Sync;

pub(crate) struct LocalShardSpawner<M>
where
    M: Send + 'static,
{
    shard_buffer_capacity: usize,
    failure_backoff: Option<Duration>,
    spawn: Arc<SpawnShard<M>>,
}

impl<M> LocalShardSpawner<M>
where
    M: Send + 'static,
{
    pub(crate) fn plain(shard_buffer_capacity: usize) -> Self {
        Self {
            shard_buffer_capacity,
            failure_backoff: None,
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
            failure_backoff: None,
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
            failure_backoff: Some(DEFAULT_REMEMBER_SHARD_FAILURE_BACKOFF),
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

    pub(crate) fn with_remember_store_refs(
        shard_buffer_capacity: usize,
        remember_stores_by_shard: BTreeMap<ShardId, ActorRef<RememberShardStoreMsg>>,
        timeout: Duration,
    ) -> Self {
        Self {
            shard_buffer_capacity,
            failure_backoff: Some(DEFAULT_REMEMBER_SHARD_FAILURE_BACKOFF),
            spawn: Arc::new(move |ctx, shard, shard_buffer_capacity| {
                if let Some(store) = remember_stores_by_shard.get(shard).cloned() {
                    ctx.spawn(
                        shard_actor_name(shard),
                        ShardActor::props_with_remember_store(
                            shard.clone(),
                            shard_buffer_capacity,
                            store,
                            timeout,
                        ),
                    )
                } else {
                    ctx.spawn(
                        shard_actor_name(shard),
                        ShardActor::props_with_remember_entities(
                            shard.clone(),
                            shard_buffer_capacity,
                        ),
                    )
                }
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

    pub(crate) fn failure_backoff(&self) -> Option<Duration> {
        self.failure_backoff
    }

    pub(crate) fn set_failure_backoff(&mut self, backoff: Duration) {
        if self.failure_backoff.is_some() {
            self.failure_backoff = Some(backoff);
        }
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
