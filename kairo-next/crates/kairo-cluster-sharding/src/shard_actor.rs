use std::collections::VecDeque;

use kairo_actor::{Actor, ActorRef, ActorResult, Context, Props};

use crate::shard_loading::ShardRememberLoadState;
use crate::{
    EntityId, EntityTerminatedPlan, PassivatePlan, RememberShardUpdate, RememberUpdateDonePlan,
    RememberedEntitiesPlan, ShardDeliverPlan, ShardHandOffPlan, ShardId, ShardRuntime,
    ShardingEnvelope,
};

pub struct ShardActor<M> {
    runtime: ShardRuntime<M>,
    remember_load: ShardRememberLoadState<M>,
}

impl<M> ShardActor<M> {
    pub fn new(shard_id: impl Into<ShardId>, buffer_capacity: usize) -> Self {
        Self {
            runtime: ShardRuntime::new(shard_id, buffer_capacity),
            remember_load: ShardRememberLoadState::ready(),
        }
    }

    pub fn new_with_remember_entities(
        shard_id: impl Into<ShardId>,
        buffer_capacity: usize,
    ) -> Self {
        Self {
            runtime: ShardRuntime::new_with_remember_entities(shard_id, buffer_capacity),
            remember_load: ShardRememberLoadState::ready(),
        }
    }

    pub fn new_loading_remembered_entities(
        shard_id: impl Into<ShardId>,
        buffer_capacity: usize,
    ) -> Self {
        Self {
            runtime: ShardRuntime::new_with_remember_entities(shard_id, buffer_capacity),
            remember_load: ShardRememberLoadState::loading(),
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
    RememberUpdateDone {
        update: RememberShardUpdate,
        reply_to: ActorRef<RememberUpdateDonePlan<M>>,
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
        _ctx: &mut Context<ShardMsg<M>>,
        msg: ShardMsg<M>,
    ) -> ActorResult {
        match msg {
            ShardMsg::Deliver { message, reply_to } => {
                let plan = self.runtime.deliver(message);
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
            ShardMsg::RememberUpdateDone { update, reply_to } => {
                let plan = self.runtime.remember_update_done(update);
                let _ = reply_to.tell(plan);
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
