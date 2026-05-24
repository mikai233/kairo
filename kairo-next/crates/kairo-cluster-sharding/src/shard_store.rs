use std::time::Duration;

use kairo_actor::{ActorRef, ActorResult, Context};

use crate::{
    RememberShardStoreMsg, RememberShardUpdate, RememberShardUpdateDone, RememberedEntities,
    ShardMsg, ShardingError,
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
