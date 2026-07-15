#![deny(missing_docs)]

//! Typed local actors for in-memory remember-shard and remember-entity state.
//!
//! These actors implement the same local request/reply shapes consumed by the
//! coordinator and shard runtimes, but their state is process-local and is not
//! durable across actor-system termination. Distributed deployments normally
//! use the distributed-data adapters.

use std::collections::{BTreeMap, BTreeSet};

use kairo_actor::{Actor, ActorRef, ActorResult, Context, Props};

use crate::{
    EntityId, RememberCoordinatorStoreState, RememberCoordinatorUpdateDone,
    RememberShardStoreState, RememberShardUpdate, RememberShardUpdateDone, RememberedShards,
    ShardId, ShardingError,
};

/// Process-local actor storing the coordinator's additive shard-id set.
pub struct RememberCoordinatorStoreActor {
    state: RememberCoordinatorStoreState,
}

impl RememberCoordinatorStoreActor {
    /// Creates a coordinator store actor from existing in-memory state.
    pub fn new(state: RememberCoordinatorStoreState) -> Self {
        Self { state }
    }

    /// Creates repeatable actor properties from existing coordinator state.
    pub fn props(state: RememberCoordinatorStoreState) -> Props<Self> {
        Props::new(move || Self::new(state))
    }

    /// Returns the actor's current state for direct, unspawned inspection.
    pub fn state(&self) -> &RememberCoordinatorStoreState {
        &self.state
    }
}

/// Local request/reply protocol for [`RememberCoordinatorStoreActor`].
pub enum RememberCoordinatorStoreMsg {
    /// Records one shard identifier idempotently.
    AddShard {
        /// Shard identifier to record.
        shard: ShardId,
        /// Actor that receives the successful update acknowledgement.
        reply_to: ActorRef<RememberCoordinatorUpdateDone>,
    },
    /// Requests every remembered shard identifier.
    GetShards {
        /// Actor that receives the remembered shard set.
        reply_to: ActorRef<RememberedShards>,
    },
    /// Requests a diagnostic snapshot of the local store.
    GetState {
        /// Actor that receives the diagnostic snapshot.
        reply_to: ActorRef<RememberCoordinatorStoreSnapshot>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Diagnostic snapshot of a process-local coordinator store.
pub struct RememberCoordinatorStoreSnapshot {
    /// Remembered shard identifiers.
    pub shards: BTreeSet<ShardId>,
}

impl Actor for RememberCoordinatorStoreActor {
    type Msg = RememberCoordinatorStoreMsg;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            RememberCoordinatorStoreMsg::AddShard { shard, reply_to } => {
                let done = self.state.add_shard(shard);
                let _ = reply_to.tell(done);
            }
            RememberCoordinatorStoreMsg::GetShards { reply_to } => {
                let _ = reply_to.tell(self.state.get_shards());
            }
            RememberCoordinatorStoreMsg::GetState { reply_to } => {
                let _ = reply_to.tell(RememberCoordinatorStoreSnapshot::from(&self.state));
            }
        }
        Ok(())
    }
}

impl From<&RememberCoordinatorStoreState> for RememberCoordinatorStoreSnapshot {
    fn from(state: &RememberCoordinatorStoreState) -> Self {
        Self {
            shards: state.remembered_shards().clone(),
        }
    }
}

/// Process-local actor storing one shard's partitioned entity identifiers.
pub struct RememberShardStoreActor {
    state: RememberShardStoreState,
}

impl RememberShardStoreActor {
    /// Creates a shard store actor from existing in-memory state.
    pub fn new(state: RememberShardStoreState) -> Self {
        Self { state }
    }

    /// Creates repeatable actor properties from existing shard state.
    pub fn props(state: RememberShardStoreState) -> Props<Self> {
        Props::new(move || Self::new(state))
    }

    /// Returns the actor's current state for direct, unspawned inspection.
    pub fn state(&self) -> &RememberShardStoreState {
        &self.state
    }
}

/// Local request/reply protocol for [`RememberShardStoreActor`].
pub enum RememberShardStoreMsg {
    /// Requests the union of all remembered entity partitions.
    GetEntities {
        /// Actor that receives the remembered entity identifiers.
        reply_to: ActorRef<RememberedEntities>,
    },
    /// Applies one logical started/stopped entity update.
    Update {
        /// Partitioned entity update to apply.
        update: RememberShardUpdate,
        /// Actor that receives completion or a store error.
        reply_to: ActorRef<Result<RememberShardUpdateDone, ShardingError>>,
    },
    /// Requests a diagnostic snapshot of the local store.
    GetState {
        /// Actor that receives the diagnostic snapshot.
        reply_to: ActorRef<RememberShardStoreSnapshot>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Reply containing all entity identifiers remembered by one shard.
pub struct RememberedEntities {
    /// Remembered entity identifiers.
    pub entities: BTreeSet<EntityId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Diagnostic snapshot of a process-local shard store.
pub struct RememberShardStoreSnapshot {
    /// Cluster-wide entity type name.
    pub type_name: String,
    /// Logical shard identifier.
    pub shard_id: ShardId,
    /// Remembered identifiers grouped by stable storage-key index.
    pub entities_by_key: BTreeMap<usize, BTreeSet<EntityId>>,
}

impl Actor for RememberShardStoreActor {
    type Msg = RememberShardStoreMsg;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            RememberShardStoreMsg::GetEntities { reply_to } => {
                let remembered = RememberedEntities {
                    entities: self.state.remembered_entities(),
                };
                let _ = reply_to.tell(remembered);
            }
            RememberShardStoreMsg::Update { update, reply_to } => {
                let result = self.state.apply_update(update);
                let _ = reply_to.tell(result);
            }
            RememberShardStoreMsg::GetState { reply_to } => {
                let _ = reply_to.tell(RememberShardStoreSnapshot::from(&self.state));
            }
        }
        Ok(())
    }
}

impl From<&RememberShardStoreState> for RememberShardStoreSnapshot {
    fn from(state: &RememberShardStoreState) -> Self {
        let entities_by_key = (0..crate::REMEMBER_ENTITY_SHARD_KEY_COUNT)
            .map(|index| {
                (
                    index,
                    state.entities_for_key(index).cloned().unwrap_or_default(),
                )
            })
            .collect();

        Self {
            type_name: state.type_name().to_string(),
            shard_id: state.shard_id().clone(),
            entities_by_key,
        }
    }
}
