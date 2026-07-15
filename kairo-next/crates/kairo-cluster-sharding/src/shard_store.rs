use std::fmt::{self, Display, Formatter};
use std::time::Duration;

use kairo_actor::{ActorError, ActorRef, ActorResult, AskError, Context};
use kairo_distributed_data::{ORSet, ReplicaId, ReplicatorActorMsg};

use crate::{
    RememberShardDDataStoreActor, RememberShardDDataStoreMsg, RememberShardStoreActor,
    RememberShardStoreMsg, RememberShardStoreState, RememberShardUpdate, RememberShardUpdateDone,
    RememberedEntities, ShardId, ShardMsg, ShardingError,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShardRememberStoreError {
    Ask(AskError),
    Store(ShardingError),
}

impl Display for ShardRememberStoreError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ask(error) => write!(f, "{error}"),
            Self::Store(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for ShardRememberStoreError {}

#[derive(Clone)]
enum ShardRememberStoreTarget {
    Actor(ActorRef<RememberShardStoreMsg>),
    DistributedData(ActorRef<RememberShardDDataStoreMsg>),
}

#[derive(Clone)]
pub(crate) struct ShardRememberStore {
    target: ShardRememberStoreTarget,
    timeout: Duration,
}

impl ShardRememberStore {
    pub(crate) fn new(store: ActorRef<RememberShardStoreMsg>, timeout: Duration) -> Self {
        Self {
            target: ShardRememberStoreTarget::Actor(store),
            timeout,
        }
    }

    pub(crate) fn from_distributed_data(
        store: ActorRef<RememberShardDDataStoreMsg>,
        timeout: Duration,
    ) -> Self {
        Self {
            target: ShardRememberStoreTarget::DistributedData(store),
            timeout,
        }
    }

    pub(crate) fn load<M>(&self, ctx: &Context<ShardMsg<M>>) -> ActorResult
    where
        M: Send + 'static,
    {
        match &self.target {
            ShardRememberStoreTarget::Actor(store) => ctx.ask(
                store.clone(),
                self.timeout,
                |reply_to: ActorRef<RememberedEntities>| RememberShardStoreMsg::GetEntities {
                    reply_to,
                },
                |result| ShardMsg::RememberStoreLoadResult {
                    result: result.map_err(ShardRememberStoreError::Ask),
                },
            ),
            ShardRememberStoreTarget::DistributedData(store) => ctx.ask(
                store.clone(),
                self.timeout,
                |reply_to: ActorRef<Result<RememberedEntities, ShardingError>>| {
                    RememberShardDDataStoreMsg::GetEntities { reply_to }
                },
                |result| ShardMsg::RememberStoreLoadResult {
                    result: flatten_store_result(result),
                },
            ),
        }
    }

    pub(crate) fn update<M>(
        &self,
        ctx: &Context<ShardMsg<M>>,
        update: RememberShardUpdate,
    ) -> ActorResult
    where
        M: Send + 'static,
    {
        let sent_update = update.clone();
        match &self.target {
            ShardRememberStoreTarget::Actor(store) => ctx.ask(
                store.clone(),
                self.timeout,
                move |reply_to: ActorRef<Result<RememberShardUpdateDone, ShardingError>>| {
                    RememberShardStoreMsg::Update { update, reply_to }
                },
                move |result| ShardMsg::RememberStoreUpdateResult {
                    update: sent_update,
                    result: flatten_store_result(result),
                },
            ),
            ShardRememberStoreTarget::DistributedData(store) => ctx.ask(
                store.clone(),
                self.timeout,
                move |reply_to: ActorRef<Result<RememberShardUpdateDone, ShardingError>>| {
                    RememberShardDDataStoreMsg::Update { update, reply_to }
                },
                move |result| ShardMsg::RememberStoreUpdateResult {
                    update: sent_update,
                    result: flatten_store_result(result),
                },
            ),
        }
    }
}

fn flatten_store_result<T>(
    result: Result<Result<T, ShardingError>, AskError>,
) -> Result<T, ShardRememberStoreError> {
    result
        .map_err(ShardRememberStoreError::Ask)?
        .map_err(ShardRememberStoreError::Store)
}

pub(crate) struct LocalShardRememberStoreProvider {
    state: Option<RememberShardStoreState>,
    timeout: Duration,
}

impl LocalShardRememberStoreProvider {
    pub(crate) fn new(state: RememberShardStoreState, timeout: Duration) -> Self {
        Self {
            state: Some(state),
            timeout,
        }
    }

    pub(crate) fn spawn<M>(
        &mut self,
        ctx: &Context<ShardMsg<M>>,
    ) -> Result<ShardRememberStore, ActorError>
    where
        M: Send + 'static,
    {
        let state = self
            .state
            .take()
            .expect("local remember store provider can spawn only once");
        let store = ctx.spawn("remember-store", RememberShardStoreActor::props(state))?;
        Ok(ShardRememberStore::new(store, self.timeout))
    }
}

pub(crate) struct LocalDDataShardRememberStoreProvider {
    type_name: String,
    shard_id: ShardId,
    replica_id: ReplicaId,
    replicator: ActorRef<ReplicatorActorMsg<ORSet<String>>>,
    timeout: Duration,
}

impl LocalDDataShardRememberStoreProvider {
    pub(crate) fn new(
        type_name: impl Into<String>,
        shard_id: impl Into<ShardId>,
        replica_id: impl Into<ReplicaId>,
        replicator: ActorRef<ReplicatorActorMsg<ORSet<String>>>,
        timeout: Duration,
    ) -> Self {
        Self {
            type_name: type_name.into(),
            shard_id: shard_id.into(),
            replica_id: replica_id.into(),
            replicator,
            timeout,
        }
    }

    pub(crate) fn spawn<M>(
        &self,
        ctx: &Context<ShardMsg<M>>,
    ) -> Result<ShardRememberStore, ActorError>
    where
        M: Send + 'static,
    {
        let store = ctx.spawn(
            "remember-store",
            RememberShardDDataStoreActor::props(
                self.type_name.clone(),
                self.shard_id.clone(),
                self.replica_id.clone(),
                self.replicator.clone(),
            ),
        )?;
        Ok(ShardRememberStore::from_distributed_data(
            store,
            self.timeout,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ddata_store_result_preserves_ask_failure() {
        let timeout = Duration::from_millis(25);

        assert_eq!(
            flatten_store_result::<RememberedEntities>(Err(AskError::Timeout { timeout })),
            Err(ShardRememberStoreError::Ask(AskError::Timeout { timeout }))
        );
    }

    #[test]
    fn ddata_store_result_preserves_store_failure() {
        let error = ShardingError::RememberStoreUpdateFailed {
            key: "shard-orders-1-0".to_string(),
            reason: "replicator unavailable".to_string(),
        };

        assert_eq!(
            flatten_store_result::<RememberShardUpdateDone>(Ok(Err(error.clone()))),
            Err(ShardRememberStoreError::Store(error))
        );
    }
}
