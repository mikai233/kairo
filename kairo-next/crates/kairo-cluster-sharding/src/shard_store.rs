use std::time::Duration;

use kairo_actor::{ActorError, ActorRef, ActorResult, Context};

use crate::{
    RememberShardStoreActor, RememberShardStoreMsg, RememberShardStoreState, RememberShardUpdate,
    RememberShardUpdateDone, RememberedEntities, ShardMsg, ShardingError,
};

#[derive(Clone)]
pub(crate) struct ShardRememberStore {
    store: ActorRef<RememberShardStoreMsg>,
    timeout: Duration,
}

impl ShardRememberStore {
    pub(crate) fn new(store: ActorRef<RememberShardStoreMsg>, timeout: Duration) -> Self {
        Self { store, timeout }
    }

    pub(crate) fn load<M>(&self, ctx: &Context<ShardMsg<M>>) -> ActorResult
    where
        M: Send + 'static,
    {
        ctx.ask(
            self.store.clone(),
            self.timeout,
            |reply_to: ActorRef<RememberedEntities>| RememberShardStoreMsg::GetEntities {
                reply_to,
            },
            |result| ShardMsg::RememberStoreLoadResult { result },
        )
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
        ctx.ask(
            self.store.clone(),
            self.timeout,
            move |reply_to: ActorRef<Result<RememberShardUpdateDone, ShardingError>>| {
                RememberShardStoreMsg::Update { update, reply_to }
            },
            move |result| ShardMsg::RememberStoreUpdateResult {
                update: sent_update,
                result,
            },
        )
    }
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
