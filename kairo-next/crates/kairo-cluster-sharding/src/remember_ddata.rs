use std::collections::BTreeSet;

use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, Context, Props, SendError};
use kairo_distributed_data::{
    GSet, GetResponse, ReadConsistency, ReplicatorActorMsg, ReplicatorKey, UpdateResponse,
    WriteConsistency,
};

use crate::{RememberCoordinatorUpdateDone, RememberedShards, ShardId, ShardingError};

pub struct RememberCoordinatorDDataStoreActor {
    type_name: String,
    replicator: ActorRef<ReplicatorActorMsg<GSet<String>>>,
    read_consistency: ReadConsistency,
    write_consistency: WriteConsistency,
}

impl RememberCoordinatorDDataStoreActor {
    pub fn new(
        type_name: impl Into<String>,
        replicator: ActorRef<ReplicatorActorMsg<GSet<String>>>,
    ) -> Self {
        Self::with_consistency(
            type_name,
            replicator,
            ReadConsistency::local(),
            WriteConsistency::local(),
        )
    }

    pub fn with_consistency(
        type_name: impl Into<String>,
        replicator: ActorRef<ReplicatorActorMsg<GSet<String>>>,
        read_consistency: ReadConsistency,
        write_consistency: WriteConsistency,
    ) -> Self {
        Self {
            type_name: type_name.into(),
            replicator,
            read_consistency,
            write_consistency,
        }
    }

    pub fn props(
        type_name: impl Into<String>,
        replicator: ActorRef<ReplicatorActorMsg<GSet<String>>>,
    ) -> Props<Self> {
        let type_name = type_name.into();
        Props::new(move || Self::new(type_name, replicator))
    }

    pub fn props_with_consistency(
        type_name: impl Into<String>,
        replicator: ActorRef<ReplicatorActorMsg<GSet<String>>>,
        read_consistency: ReadConsistency,
        write_consistency: WriteConsistency,
    ) -> Props<Self> {
        let type_name = type_name.into();
        Props::new(move || {
            Self::with_consistency(type_name, replicator, read_consistency, write_consistency)
        })
    }

    pub fn type_name(&self) -> &str {
        &self.type_name
    }

    pub fn key(&self) -> ReplicatorKey {
        remember_coordinator_shards_key(&self.type_name)
    }
}

pub enum RememberCoordinatorDDataStoreMsg {
    AddShard {
        shard: ShardId,
        reply_to: ActorRef<Result<RememberCoordinatorUpdateDone, ShardingError>>,
    },
    GetShards {
        reply_to: ActorRef<Result<RememberedShards, ShardingError>>,
    },
    GetState {
        reply_to: ActorRef<RememberCoordinatorDDataStoreSnapshot>,
    },
    #[doc(hidden)]
    ReplicatorGet {
        response: GetResponse<GSet<String>>,
        reply_to: ActorRef<Result<RememberedShards, ShardingError>>,
    },
    #[doc(hidden)]
    ReplicatorUpdate {
        shard: ShardId,
        response: UpdateResponse<GSet<String>>,
        reply_to: ActorRef<Result<RememberCoordinatorUpdateDone, ShardingError>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RememberCoordinatorDDataStoreSnapshot {
    pub type_name: String,
    pub key: String,
    pub read_consistency: ReadConsistency,
    pub write_consistency: WriteConsistency,
}

impl Actor for RememberCoordinatorDDataStoreActor {
    type Msg = RememberCoordinatorDDataStoreMsg;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            RememberCoordinatorDDataStoreMsg::AddShard { shard, reply_to } => {
                self.add_shard(ctx, shard, reply_to)?;
            }
            RememberCoordinatorDDataStoreMsg::GetShards { reply_to } => {
                self.get_shards(ctx, reply_to)?;
            }
            RememberCoordinatorDDataStoreMsg::GetState { reply_to } => {
                reply_to
                    .tell(self.snapshot())
                    .map_err(send_error_to_actor_error)?;
            }
            RememberCoordinatorDDataStoreMsg::ReplicatorGet { response, reply_to } => {
                reply_to
                    .tell(map_get_response(response))
                    .map_err(send_error_to_actor_error)?;
            }
            RememberCoordinatorDDataStoreMsg::ReplicatorUpdate {
                shard,
                response,
                reply_to,
            } => {
                reply_to
                    .tell(map_update_response(shard, response))
                    .map_err(send_error_to_actor_error)?;
            }
        }
        Ok(())
    }
}

impl RememberCoordinatorDDataStoreActor {
    fn get_shards(
        &self,
        ctx: &Context<RememberCoordinatorDDataStoreMsg>,
        reply_to: ActorRef<Result<RememberedShards, ShardingError>>,
    ) -> ActorResult {
        let adapter = ctx.message_adapter({
            let reply_to = reply_to.clone();
            move |response| RememberCoordinatorDDataStoreMsg::ReplicatorGet {
                response,
                reply_to: reply_to.clone(),
            }
        })?;
        self.replicator
            .tell(ReplicatorActorMsg::Get {
                key: self.key(),
                consistency: self.read_consistency.clone(),
                reply_to: adapter,
            })
            .map_err(send_error_to_actor_error)?;
        Ok(())
    }

    fn add_shard(
        &self,
        ctx: &Context<RememberCoordinatorDDataStoreMsg>,
        shard: ShardId,
        reply_to: ActorRef<Result<RememberCoordinatorUpdateDone, ShardingError>>,
    ) -> ActorResult {
        let adapter = ctx.message_adapter({
            let shard = shard.clone();
            let reply_to = reply_to.clone();
            move |response| RememberCoordinatorDDataStoreMsg::ReplicatorUpdate {
                shard: shard.clone(),
                response,
                reply_to: reply_to.clone(),
            }
        })?;
        self.replicator
            .tell(ReplicatorActorMsg::Update {
                key: self.key(),
                initial: GSet::new(),
                consistency: self.write_consistency.clone(),
                modify: Box::new(move |set| Ok(set.add(shard))),
                reply_to: adapter,
            })
            .map_err(send_error_to_actor_error)?;
        Ok(())
    }

    fn snapshot(&self) -> RememberCoordinatorDDataStoreSnapshot {
        RememberCoordinatorDDataStoreSnapshot {
            type_name: self.type_name.clone(),
            key: self.key().as_str().to_string(),
            read_consistency: self.read_consistency.clone(),
            write_consistency: self.write_consistency.clone(),
        }
    }
}

pub fn remember_coordinator_shards_key(type_name: &str) -> ReplicatorKey {
    ReplicatorKey::new(format!("shard-{type_name}-all"))
}

fn map_get_response(
    response: GetResponse<GSet<String>>,
) -> Result<RememberedShards, ShardingError> {
    match response {
        GetResponse::Success { data, .. } => Ok(RememberedShards {
            shards: data.elements().clone(),
        }),
        GetResponse::NotFound { .. } => Ok(RememberedShards {
            shards: BTreeSet::new(),
        }),
        GetResponse::Failure { key, reason } => Err(ShardingError::RememberStoreReadFailed {
            key: key.as_str().to_string(),
            reason,
        }),
    }
}

fn map_update_response(
    shard: ShardId,
    response: UpdateResponse<GSet<String>>,
) -> Result<RememberCoordinatorUpdateDone, ShardingError> {
    match response {
        UpdateResponse::Success(_) => Ok(RememberCoordinatorUpdateDone { shard }),
        UpdateResponse::Timeout { key } => Err(ShardingError::RememberStoreUpdateFailed {
            key: key.as_str().to_string(),
            reason: format!("timed out while adding shard {shard}"),
        }),
        UpdateResponse::ModifyFailure { key, reason } | UpdateResponse::Failure { key, reason } => {
            Err(ShardingError::RememberStoreUpdateFailed {
                key: key.as_str().to_string(),
                reason,
            })
        }
    }
}

fn send_error_to_actor_error<M>(error: SendError<M>) -> ActorError {
    ActorError::Message(error.to_string())
}
