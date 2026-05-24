use std::collections::{BTreeMap, BTreeSet};

use kairo_actor::{Actor, ActorRef, ActorResult, Context, Props};

use crate::{
    EntityId, RememberCoordinatorStoreState, RememberCoordinatorUpdateDone,
    RememberShardStoreState, RememberShardUpdate, RememberShardUpdateDone, RememberedShards,
    ShardId, ShardingError,
};

pub struct RememberCoordinatorStoreActor {
    state: RememberCoordinatorStoreState,
}

impl RememberCoordinatorStoreActor {
    pub fn new(state: RememberCoordinatorStoreState) -> Self {
        Self { state }
    }

    pub fn props(state: RememberCoordinatorStoreState) -> Props<Self> {
        Props::new(move || Self::new(state))
    }

    pub fn state(&self) -> &RememberCoordinatorStoreState {
        &self.state
    }
}

pub enum RememberCoordinatorStoreMsg {
    AddShard {
        shard: ShardId,
        reply_to: ActorRef<RememberCoordinatorUpdateDone>,
    },
    GetShards {
        reply_to: ActorRef<RememberedShards>,
    },
    GetState {
        reply_to: ActorRef<RememberCoordinatorStoreSnapshot>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RememberCoordinatorStoreSnapshot {
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

pub struct RememberShardStoreActor {
    state: RememberShardStoreState,
}

impl RememberShardStoreActor {
    pub fn new(state: RememberShardStoreState) -> Self {
        Self { state }
    }

    pub fn props(state: RememberShardStoreState) -> Props<Self> {
        Props::new(move || Self::new(state))
    }

    pub fn state(&self) -> &RememberShardStoreState {
        &self.state
    }
}

pub enum RememberShardStoreMsg {
    GetEntities {
        reply_to: ActorRef<RememberedEntities>,
    },
    Update {
        update: RememberShardUpdate,
        reply_to: ActorRef<Result<RememberShardUpdateDone, ShardingError>>,
    },
    GetState {
        reply_to: ActorRef<RememberShardStoreSnapshot>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RememberedEntities {
    pub entities: BTreeSet<EntityId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RememberShardStoreSnapshot {
    pub type_name: String,
    pub shard_id: ShardId,
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
