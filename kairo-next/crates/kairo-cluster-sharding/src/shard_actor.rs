use std::collections::VecDeque;
use std::time::Duration;

use kairo_actor::{Actor, ActorRef, ActorResult, AskError, Context, Props};

use crate::shard_loading::ShardRememberLoadState;
use crate::shard_store::{LocalShardRememberStoreProvider, ShardRememberStore};
use crate::{
    EntityId, EntityTerminatedPlan, PassivatePlan, RememberShardStoreMsg, RememberShardStoreState,
    RememberShardUpdate, RememberShardUpdateDone, RememberUpdateDonePlan, RememberedEntities,
    RememberedEntitiesPlan, ShardDeliverPlan, ShardHandOffPlan, ShardId, ShardRuntime,
    ShardingEnvelope, ShardingError,
};

pub struct ShardActor<M> {
    runtime: ShardRuntime<M>,
    remember_load: ShardRememberLoadState<M>,
    remember_store: Option<ShardRememberStore>,
    local_remember_store_provider: Option<LocalShardRememberStoreProvider>,
}

impl<M> ShardActor<M> {
    pub fn new(shard_id: impl Into<ShardId>, buffer_capacity: usize) -> Self {
        Self {
            runtime: ShardRuntime::new(shard_id, buffer_capacity),
            remember_load: ShardRememberLoadState::ready(),
            remember_store: None,
            local_remember_store_provider: None,
        }
    }

    pub fn new_with_remember_entities(
        shard_id: impl Into<ShardId>,
        buffer_capacity: usize,
    ) -> Self {
        Self {
            runtime: ShardRuntime::new_with_remember_entities(shard_id, buffer_capacity),
            remember_load: ShardRememberLoadState::ready(),
            remember_store: None,
            local_remember_store_provider: None,
        }
    }

    pub fn new_loading_remembered_entities(
        shard_id: impl Into<ShardId>,
        buffer_capacity: usize,
    ) -> Self {
        Self {
            runtime: ShardRuntime::new_with_remember_entities(shard_id, buffer_capacity),
            remember_load: ShardRememberLoadState::loading(),
            remember_store: None,
            local_remember_store_provider: None,
        }
    }

    pub fn new_with_remember_store(
        shard_id: impl Into<ShardId>,
        buffer_capacity: usize,
        remember_store: ActorRef<RememberShardStoreMsg>,
        timeout: Duration,
    ) -> Self {
        Self {
            runtime: ShardRuntime::new_with_remember_entities(shard_id, buffer_capacity),
            remember_load: ShardRememberLoadState::loading(),
            remember_store: Some(ShardRememberStore::new(remember_store, timeout)),
            local_remember_store_provider: None,
        }
    }

    pub fn new_with_local_remember_store(
        buffer_capacity: usize,
        store_state: RememberShardStoreState,
        timeout: Duration,
    ) -> Self {
        let shard_id = store_state.shard_id().clone();
        Self {
            runtime: ShardRuntime::new_with_remember_entities(shard_id, buffer_capacity),
            remember_load: ShardRememberLoadState::loading(),
            remember_store: None,
            local_remember_store_provider: Some(LocalShardRememberStoreProvider::new(
                store_state,
                timeout,
            )),
        }
    }

    pub fn props(shard_id: impl Into<ShardId>, buffer_capacity: usize) -> Props<Self>
    where
        M: Send + 'static,
    {
        let shard_id = shard_id.into();
        Props::new(move || Self::new(shard_id, buffer_capacity))
    }

    pub fn props_with_remember_entities(
        shard_id: impl Into<ShardId>,
        buffer_capacity: usize,
    ) -> Props<Self>
    where
        M: Send + 'static,
    {
        let shard_id = shard_id.into();
        Props::new(move || Self::new_with_remember_entities(shard_id, buffer_capacity))
    }

    pub fn props_loading_remembered_entities(
        shard_id: impl Into<ShardId>,
        buffer_capacity: usize,
    ) -> Props<Self>
    where
        M: Send + 'static,
    {
        let shard_id = shard_id.into();
        Props::new(move || Self::new_loading_remembered_entities(shard_id, buffer_capacity))
    }

    pub fn props_with_remember_store(
        shard_id: impl Into<ShardId>,
        buffer_capacity: usize,
        remember_store: ActorRef<RememberShardStoreMsg>,
        timeout: Duration,
    ) -> Props<Self>
    where
        M: Send + 'static,
    {
        let shard_id = shard_id.into();
        Props::new(move || {
            Self::new_with_remember_store(shard_id, buffer_capacity, remember_store, timeout)
        })
    }

    pub fn props_with_local_remember_store(
        buffer_capacity: usize,
        store_state: RememberShardStoreState,
        timeout: Duration,
    ) -> Props<Self>
    where
        M: Send + 'static,
    {
        Props::new(move || {
            Self::new_with_local_remember_store(buffer_capacity, store_state, timeout)
        })
    }

    pub fn runtime(&self) -> &ShardRuntime<M> {
        &self.runtime
    }
}

pub enum ShardMsg<M> {
    Deliver {
        message: ShardingEnvelope<M>,
        reply_to: ActorRef<ShardDeliverPlan<M>>,
    },
    Passivate {
        entity_id: EntityId,
        stop_message: M,
        reply_to: ActorRef<PassivatePlan<M>>,
    },
    EntityTerminated {
        entity_id: EntityId,
        reply_to: ActorRef<EntityTerminatedPlan<M>>,
    },
    HandOff {
        stop_message: M,
        reply_to: ActorRef<ShardHandOffPlan<M>>,
    },
    HandOffStopperTerminated {
        reply_to: ActorRef<bool>,
    },
    RecoverRememberedEntities {
        entities: Vec<EntityId>,
        reply_to: ActorRef<RememberedEntitiesPlan>,
    },
    RememberedEntitiesLoaded {
        entities: Vec<EntityId>,
        reply_to: ActorRef<RememberedEntitiesPlan>,
    },
    RememberStoreLoadResult {
        result: Result<RememberedEntities, AskError>,
    },
    RememberUpdateDone {
        update: RememberShardUpdate,
        reply_to: ActorRef<RememberUpdateDonePlan<M>>,
    },
    RememberStoreUpdateResult {
        update: RememberShardUpdate,
        result: Result<Result<RememberShardUpdateDone, ShardingError>, AskError>,
    },
    SetPreparingForShutdown {
        preparing: bool,
    },
    GetState {
        reply_to: ActorRef<ShardSnapshot>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardSnapshot {
    pub shard_id: ShardId,
    pub active_entities: Vec<EntityId>,
    pub entity_count: usize,
    pub total_buffered: usize,
    pub handoff_in_progress: bool,
}

impl<M> Actor for ShardActor<M>
where
    M: Send + 'static,
{
    type Msg = ShardMsg<M>;

    fn started(&mut self, ctx: &mut Context<Self::Msg>) -> ActorResult {
        self.spawn_local_remember_store_if_needed(ctx)?;
        self.request_remember_store_load(ctx)
    }

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        if self.remember_load.is_loading() {
            return self.receive_while_loading(ctx, msg);
        }

        self.receive_initialized(ctx, msg)
    }
}

impl<M> ShardActor<M>
where
    M: Send + 'static,
{
    fn receive_while_loading(
        &mut self,
        ctx: &mut Context<ShardMsg<M>>,
        msg: ShardMsg<M>,
    ) -> ActorResult {
        match msg {
            ShardMsg::RecoverRememberedEntities { entities, reply_to }
            | ShardMsg::RememberedEntitiesLoaded { entities, reply_to } => {
                let plan = self.runtime.recover_remembered_entities(entities);
                let _ = reply_to.tell(plan);
                let stashed = self.remember_load.mark_ready();
                self.replay_stashed(ctx, stashed)
            }
            ShardMsg::RememberStoreLoadResult { result } => {
                let remembered = match result {
                    Ok(remembered) => remembered,
                    Err(_) => {
                        return self.stop_for_remember_store_failure(ctx);
                    }
                };
                let _ = self
                    .runtime
                    .recover_remembered_entities(remembered.entities);
                let stashed = self.remember_load.mark_ready();
                self.replay_stashed(ctx, stashed)
            }
            other => {
                self.remember_load.stash(other);
                Ok(())
            }
        }
    }

    fn replay_stashed(
        &mut self,
        ctx: &mut Context<ShardMsg<M>>,
        mut stashed: VecDeque<ShardMsg<M>>,
    ) -> ActorResult {
        while let Some(message) = stashed.pop_front() {
            self.receive_initialized(ctx, message)?;
        }
        Ok(())
    }

    fn receive_initialized(
        &mut self,
        ctx: &mut Context<ShardMsg<M>>,
        msg: ShardMsg<M>,
    ) -> ActorResult {
        match msg {
            ShardMsg::Deliver { message, reply_to } => {
                let plan = self.runtime.deliver(message);
                self.send_deliver_plan_store_effect(ctx, &plan)?;
                let _ = reply_to.tell(plan);
            }
            ShardMsg::Passivate {
                entity_id,
                stop_message,
                reply_to,
            } => {
                let plan = self.runtime.passivate(entity_id, stop_message);
                let _ = reply_to.tell(plan);
            }
            ShardMsg::EntityTerminated {
                entity_id,
                reply_to,
            } => {
                let plan = self.runtime.entity_terminated(entity_id);
                self.send_entity_terminated_store_effect(ctx, &plan)?;
                let _ = reply_to.tell(plan);
            }
            ShardMsg::HandOff {
                stop_message,
                reply_to,
            } => {
                let plan = self.runtime.handoff(stop_message);
                let _ = reply_to.tell(plan);
            }
            ShardMsg::HandOffStopperTerminated { reply_to } => {
                let was_in_progress = self.runtime.handoff_stopper_terminated();
                let _ = reply_to.tell(was_in_progress);
            }
            ShardMsg::RecoverRememberedEntities { entities, reply_to } => {
                let plan = self.runtime.recover_remembered_entities(entities);
                let _ = reply_to.tell(plan);
            }
            ShardMsg::RememberedEntitiesLoaded { entities, reply_to } => {
                let plan = self.runtime.recover_remembered_entities(entities);
                let _ = reply_to.tell(plan);
            }
            ShardMsg::RememberStoreLoadResult { result } => {
                if result.is_err() {
                    return self.stop_for_remember_store_failure(ctx);
                }
            }
            ShardMsg::RememberUpdateDone { update, reply_to } => {
                let plan = self.runtime.remember_update_done(update);
                self.send_next_remember_update(ctx, &plan)?;
                let _ = reply_to.tell(plan);
            }
            ShardMsg::RememberStoreUpdateResult { update: _, result } => {
                let done = match result {
                    Ok(Ok(done)) => done,
                    Ok(Err(_)) | Err(_) => {
                        return self.stop_for_remember_store_failure(ctx);
                    }
                };
                let completed = RememberShardUpdate::new(done.started, done.stopped);
                let plan = self.runtime.remember_update_done(completed);
                self.send_next_remember_update(ctx, &plan)?;
            }
            ShardMsg::SetPreparingForShutdown { preparing } => {
                self.runtime.set_preparing_for_shutdown(preparing);
            }
            ShardMsg::GetState { reply_to } => {
                let _ = reply_to.tell(ShardSnapshot::from(&self.runtime));
            }
        }
        Ok(())
    }

    fn request_remember_store_load(&self, ctx: &Context<ShardMsg<M>>) -> ActorResult {
        if let Some(store) = &self.remember_store {
            store.load(ctx)?;
        }
        Ok(())
    }

    fn spawn_local_remember_store_if_needed(&mut self, ctx: &Context<ShardMsg<M>>) -> ActorResult {
        if self.remember_store.is_some() {
            return Ok(());
        }
        let Some(provider) = &mut self.local_remember_store_provider else {
            return Ok(());
        };
        self.remember_store = Some(provider.spawn(ctx)?);
        Ok(())
    }

    fn send_deliver_plan_store_effect(
        &self,
        ctx: &Context<ShardMsg<M>>,
        plan: &ShardDeliverPlan<M>,
    ) -> ActorResult {
        if let ShardDeliverPlan::RememberUpdate { update } = plan {
            self.send_remember_update(ctx, update.clone())?;
        }
        Ok(())
    }

    fn send_entity_terminated_store_effect(
        &self,
        ctx: &Context<ShardMsg<M>>,
        plan: &EntityTerminatedPlan<M>,
    ) -> ActorResult {
        if let EntityTerminatedPlan::RememberUpdate { update } = plan {
            self.send_remember_update(ctx, update.clone())?;
        }
        Ok(())
    }

    fn send_next_remember_update(
        &self,
        ctx: &Context<ShardMsg<M>>,
        plan: &RememberUpdateDonePlan<M>,
    ) -> ActorResult {
        if let Some(update) = &plan.next_update {
            self.send_remember_update(ctx, update.clone())?;
        }
        Ok(())
    }

    fn send_remember_update(
        &self,
        ctx: &Context<ShardMsg<M>>,
        update: RememberShardUpdate,
    ) -> ActorResult {
        if let Some(store) = &self.remember_store {
            store.update(ctx, update)?;
        }
        Ok(())
    }

    fn stop_for_remember_store_failure(&mut self, ctx: &mut Context<ShardMsg<M>>) -> ActorResult {
        ctx.stop(ctx.myself())
    }
}

impl<M> From<&ShardRuntime<M>> for ShardSnapshot {
    fn from(runtime: &ShardRuntime<M>) -> Self {
        Self {
            shard_id: runtime.shard_id().clone(),
            active_entities: runtime.active_entity_ids(),
            entity_count: runtime.entity_count(),
            total_buffered: runtime.total_buffered_count(),
            handoff_in_progress: runtime.handoff_in_progress(),
        }
    }
}
