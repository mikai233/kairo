use std::fmt::{self, Display, Formatter};
use std::time::Duration;

use kairo_actor::{ActorError, ActorRef, ActorResult, AskError, Context};

use crate::{
    RememberCoordinatorDDataStoreMsg, RememberCoordinatorStoreActor, RememberCoordinatorStoreMsg,
    RememberCoordinatorStoreState, RememberCoordinatorUpdateDone, RememberedShards,
    ShardCoordinatorMsg, ShardId, ShardingError,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoordinatorRememberStoreError {
    Ask(AskError),
    Store(ShardingError),
}

impl Display for CoordinatorRememberStoreError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ask(error) => write!(f, "{error}"),
            Self::Store(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for CoordinatorRememberStoreError {}

#[derive(Clone)]
enum CoordinatorRememberStoreTarget {
    Actor(ActorRef<RememberCoordinatorStoreMsg>),
    DistributedData(ActorRef<RememberCoordinatorDDataStoreMsg>),
}

#[derive(Clone)]
pub(crate) struct CoordinatorRememberStore {
    target: CoordinatorRememberStoreTarget,
    timeout: Duration,
}

impl CoordinatorRememberStore {
    pub(crate) fn new(store: ActorRef<RememberCoordinatorStoreMsg>, timeout: Duration) -> Self {
        Self {
            target: CoordinatorRememberStoreTarget::Actor(store),
            timeout,
        }
    }

    pub(crate) fn from_distributed_data(
        store: ActorRef<RememberCoordinatorDDataStoreMsg>,
        timeout: Duration,
    ) -> Self {
        Self {
            target: CoordinatorRememberStoreTarget::DistributedData(store),
            timeout,
        }
    }

    pub(crate) fn load<M>(&self, ctx: &Context<ShardCoordinatorMsg<M>>) -> ActorResult
    where
        M: Send + 'static,
    {
        match &self.target {
            CoordinatorRememberStoreTarget::Actor(store) => ctx.ask(
                store.clone(),
                self.timeout,
                |reply_to: ActorRef<RememberedShards>| RememberCoordinatorStoreMsg::GetShards {
                    reply_to,
                },
                |result| ShardCoordinatorMsg::RememberStoreLoadResult {
                    result: result.map_err(CoordinatorRememberStoreError::Ask),
                },
            ),
            CoordinatorRememberStoreTarget::DistributedData(store) => ctx.ask(
                store.clone(),
                self.timeout,
                |reply_to: ActorRef<Result<RememberedShards, ShardingError>>| {
                    RememberCoordinatorDDataStoreMsg::GetShards { reply_to }
                },
                |result| ShardCoordinatorMsg::RememberStoreLoadResult {
                    result: flatten_store_result(result),
                },
            ),
        }
    }

    pub(crate) fn add_shard(
        &self,
        ctx: &Context<ShardCoordinatorMsg<impl Send + 'static>>,
        shard: ShardId,
    ) -> ActorResult {
        match &self.target {
            CoordinatorRememberStoreTarget::Actor(store) => ctx.ask(
                store.clone(),
                self.timeout,
                move |reply_to: ActorRef<RememberCoordinatorUpdateDone>| {
                    RememberCoordinatorStoreMsg::AddShard { shard, reply_to }
                },
                |result| ShardCoordinatorMsg::RememberStoreUpdateResult {
                    result: result.map_err(CoordinatorRememberStoreError::Ask),
                },
            ),
            CoordinatorRememberStoreTarget::DistributedData(store) => ctx.ask(
                store.clone(),
                self.timeout,
                move |reply_to: ActorRef<Result<RememberCoordinatorUpdateDone, ShardingError>>| {
                    RememberCoordinatorDDataStoreMsg::AddShard { shard, reply_to }
                },
                |result| ShardCoordinatorMsg::RememberStoreUpdateResult {
                    result: flatten_store_result(result),
                },
            ),
        }
    }
}

fn flatten_store_result<T>(
    result: Result<Result<T, ShardingError>, AskError>,
) -> Result<T, CoordinatorRememberStoreError> {
    result
        .map_err(CoordinatorRememberStoreError::Ask)?
        .map_err(CoordinatorRememberStoreError::Store)
}

pub(crate) struct LocalCoordinatorRememberStoreProvider {
    state: Option<RememberCoordinatorStoreState>,
    timeout: Duration,
}

impl LocalCoordinatorRememberStoreProvider {
    pub(crate) fn new(state: RememberCoordinatorStoreState, timeout: Duration) -> Self {
        Self {
            state: Some(state),
            timeout,
        }
    }

    pub(crate) fn spawn(
        &mut self,
        ctx: &Context<ShardCoordinatorMsg<impl Send + 'static>>,
    ) -> Result<CoordinatorRememberStore, ActorError> {
        let state = self
            .state
            .take()
            .expect("local coordinator remember store provider can spawn only once");
        let store = ctx.spawn(
            "remember-coordinator-store",
            RememberCoordinatorStoreActor::props(state),
        )?;
        Ok(CoordinatorRememberStore::new(store, self.timeout))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ddata_store_result_preserves_ask_failure() {
        let timeout = Duration::from_millis(25);

        assert_eq!(
            flatten_store_result::<RememberedShards>(Err(AskError::Timeout { timeout })),
            Err(CoordinatorRememberStoreError::Ask(AskError::Timeout {
                timeout
            }))
        );
    }

    #[test]
    fn ddata_store_result_preserves_store_failure() {
        let error = ShardingError::RememberStoreReadFailed {
            key: "ordersCoordinatorState".to_string(),
            reason: "replicator unavailable".to_string(),
        };

        assert_eq!(
            flatten_store_result::<RememberedShards>(Ok(Err(error.clone()))),
            Err(CoordinatorRememberStoreError::Store(error))
        );
    }
}
