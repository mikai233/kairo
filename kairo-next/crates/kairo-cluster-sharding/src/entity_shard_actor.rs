use std::collections::BTreeMap;

use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, Context, Props};

use crate::{
    EntityActorFactory, EntityDelivery, EntityId, EntityTerminatedPlan, PassivatePlan,
    ShardDeliverPlan, ShardHandOffPlan, ShardId, ShardMsg, ShardRuntime, ShardSnapshot,
};

pub struct EntityShardActor<M>
where
    M: Clone + Send + 'static,
{
    runtime: ShardRuntime<M>,
    entity_factory: EntityActorFactory<M>,
    entity_refs: BTreeMap<EntityId, ActorRef<M>>,
}

impl<M> EntityShardActor<M>
where
    M: Clone + Send + 'static,
{
    pub fn new(
        shard_id: impl Into<ShardId>,
        buffer_capacity: usize,
        entity_factory: EntityActorFactory<M>,
    ) -> Self {
        Self {
            runtime: ShardRuntime::new(shard_id, buffer_capacity),
            entity_factory,
            entity_refs: BTreeMap::new(),
        }
    }

    pub fn props(
        shard_id: impl Into<ShardId>,
        buffer_capacity: usize,
        entity_factory: EntityActorFactory<M>,
    ) -> Props<Self> {
        let shard_id = shard_id.into();
        Props::new(move || Self::new(shard_id, buffer_capacity, entity_factory))
    }
}

impl<M> Actor for EntityShardActor<M>
where
    M: Clone + Send + 'static,
{
    type Msg = ShardMsg<M>;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            ShardMsg::Deliver { message, reply_to } => {
                let plan = self.runtime.deliver(message);
                self.apply_deliver_plan(ctx, &plan)?;
                let _ = reply_to.tell(plan);
            }
            ShardMsg::Passivate {
                entity_id,
                stop_message,
                reply_to,
            } => {
                let plan = self.runtime.passivate(entity_id, stop_message);
                self.apply_passivate_plan(&plan)?;
                let _ = reply_to.tell(plan);
            }
            ShardMsg::EntityTerminated {
                entity_id,
                reply_to,
            } => {
                self.entity_refs.remove(&entity_id);
                let plan = self.runtime.entity_terminated(entity_id);
                self.apply_termination_plan(ctx, &plan)?;
                let _ = reply_to.tell(plan);
            }
            ShardMsg::ObservedEntityTerminated { entity_id } => {
                self.entity_refs.remove(&entity_id);
                let plan = self.runtime.entity_terminated(entity_id);
                self.apply_termination_plan(ctx, &plan)?;
            }
            ShardMsg::RestartRememberedEntity {
                entity_id,
                reply_to,
            } => {
                let plan = self.runtime.restart_remembered_entity(entity_id);
                let _ = reply_to.tell(plan);
            }
            ShardMsg::HandOff {
                stop_message,
                reply_to,
            } => {
                let plan = self.runtime.handoff(stop_message);
                self.apply_handoff_plan(&plan)?;
                let _ = reply_to.tell(plan);
            }
            ShardMsg::HandOffStopperTerminated { reply_to } => {
                let completed = self.runtime.handoff_in_progress() && self.entity_refs.is_empty();
                if completed {
                    self.runtime.handoff_stopper_terminated();
                }
                let _ = reply_to.tell(completed);
            }
            ShardMsg::SetPreparingForShutdown { preparing } => {
                self.runtime.set_preparing_for_shutdown(preparing);
            }
            ShardMsg::GetState { reply_to } => {
                let _ = reply_to.tell(ShardSnapshot::from(&self.runtime));
            }
            ShardMsg::RecoverRememberedEntities { entities, reply_to }
            | ShardMsg::RememberedEntitiesLoaded { entities, reply_to } => {
                let plan = self.runtime.recover_remembered_entities(entities);
                let _ = reply_to.tell(plan);
            }
            ShardMsg::RememberStoreLoadResult { result: _ }
            | ShardMsg::RememberUpdateDone { .. }
            | ShardMsg::RememberStoreUpdateResult { .. } => {}
        }
        Ok(())
    }
}

impl<M> EntityShardActor<M>
where
    M: Clone + Send + 'static,
{
    fn apply_deliver_plan(
        &mut self,
        ctx: &mut Context<ShardMsg<M>>,
        plan: &ShardDeliverPlan<M>,
    ) -> Result<(), ActorError> {
        match plan {
            ShardDeliverPlan::StartEntity { delivery } | ShardDeliverPlan::Deliver { delivery } => {
                self.deliver_to_entity(ctx, delivery)
            }
            ShardDeliverPlan::RememberUpdate { .. }
            | ShardDeliverPlan::Buffered { .. }
            | ShardDeliverPlan::Dropped { .. } => Ok(()),
        }
    }

    fn apply_passivate_plan(&self, plan: &PassivatePlan<M>) -> Result<(), ActorError> {
        match plan {
            PassivatePlan::SendStop {
                entity_id,
                stop_message,
            } => {
                if let Some(entity) = self.entity_refs.get(entity_id) {
                    entity
                        .tell(stop_message.clone())
                        .map_err(|error| ActorError::Message(error.reason().to_string()))?;
                }
                Ok(())
            }
            PassivatePlan::Ignored { .. } => Ok(()),
        }
    }

    fn apply_termination_plan(
        &mut self,
        ctx: &mut Context<ShardMsg<M>>,
        plan: &EntityTerminatedPlan<M>,
    ) -> Result<(), ActorError> {
        match plan {
            EntityTerminatedPlan::Restart { buffered } => {
                for delivery in buffered {
                    self.deliver_to_entity(ctx, delivery)?;
                }
                Ok(())
            }
            EntityTerminatedPlan::Removed { .. }
            | EntityTerminatedPlan::RestartRemembered { .. }
            | EntityTerminatedPlan::RememberUpdate { .. }
            | EntityTerminatedPlan::RememberUpdateQueued { .. }
            | EntityTerminatedPlan::IgnoredUnknown { .. } => Ok(()),
        }
    }

    fn apply_handoff_plan(&self, plan: &ShardHandOffPlan<M>) -> Result<(), ActorError> {
        match plan {
            ShardHandOffPlan::StartEntityStopper {
                entities,
                stop_message,
                ..
            }
            | ShardHandOffPlan::StopImmediately {
                entities,
                stop_message,
                ..
            } => {
                for entity_id in entities {
                    if let Some(entity) = self.entity_refs.get(entity_id) {
                        entity
                            .tell(stop_message.clone())
                            .map_err(|error| ActorError::Message(error.reason().to_string()))?;
                    }
                }
                Ok(())
            }
            ShardHandOffPlan::ReplyShardStopped { .. }
            | ShardHandOffPlan::AlreadyInProgress { .. } => Ok(()),
        }
    }

    fn deliver_to_entity(
        &mut self,
        ctx: &mut Context<ShardMsg<M>>,
        delivery: &EntityDelivery<M>,
    ) -> Result<(), ActorError> {
        let entity_id = delivery.entity_id().to_string();
        let entity = if let Some(entity) = self.entity_refs.get(&entity_id) {
            entity.clone()
        } else {
            let entity = self.entity_factory.spawn(ctx, &entity_id)?;
            ctx.watch_with(
                &entity,
                ShardMsg::ObservedEntityTerminated {
                    entity_id: entity_id.clone(),
                },
            )?;
            self.entity_refs.insert(entity_id, entity.clone());
            entity
        };
        entity
            .tell(delivery.message().clone())
            .map_err(|error| ActorError::Message(error.reason().to_string()))
    }
}
