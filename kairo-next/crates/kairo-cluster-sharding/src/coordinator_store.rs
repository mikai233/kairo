use std::time::Duration;

use kairo_actor::{ActorError, ActorRef, ActorResult, Context};

use crate::{
    RememberCoordinatorStoreActor, RememberCoordinatorStoreMsg, RememberCoordinatorStoreState,
    RememberCoordinatorUpdateDone, RememberedShards, ShardCoordinatorMsg, ShardId,
};

#[derive(Clone)]
pub(crate) struct CoordinatorRememberStore {
    store: ActorRef<RememberCoordinatorStoreMsg>,
    timeout: Duration,
}

impl CoordinatorRememberStore {
    pub(crate) fn new(store: ActorRef<RememberCoordinatorStoreMsg>, timeout: Duration) -> Self {
        Self { store, timeout }
    }

    pub(crate) fn load<M>(&self, ctx: &Context<ShardCoordinatorMsg<M>>) -> ActorResult
    where
        M: Send + 'static,
    {
        let store = self.store.clone();
        ctx.ask(
            store,
            self.timeout,
            |reply_to: ActorRef<RememberedShards>| RememberCoordinatorStoreMsg::GetShards {
                reply_to,
            },
            |result| ShardCoordinatorMsg::RememberStoreLoadResult { result },
        )
    }

    pub(crate) fn add_shard(
        &self,
        ctx: &Context<ShardCoordinatorMsg<impl Send + 'static>>,
        shard: ShardId,
    ) -> ActorResult {
        let store = self.store.clone();
        ctx.ask(
            store,
            self.timeout,
            move |reply_to: ActorRef<RememberCoordinatorUpdateDone>| {
                RememberCoordinatorStoreMsg::AddShard { shard, reply_to }
            },
            |result| ShardCoordinatorMsg::RememberStoreUpdateResult { result },
        )
    }
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
