use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;

use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, Context, Props};
use kairo_distributed_data::{ORSet, ReplicaId, ReplicatorActorMsg};

use crate::shard_loading::ShardRememberLoadState;
use crate::shard_store::{LocalDDataShardRememberStoreProvider, ShardRememberStore};
use crate::{
    EntityActorFactory, EntityDelivery, EntityId, EntityTerminatedPlan,
    MovedRememberedEntitiesPlan, PassivatePlan, RememberShardUpdate, RememberUpdateDonePlan,
    RememberedEntitiesPlan, ShardDeliverPlan, ShardHandOffPlan, ShardId, ShardMsg, ShardRuntime,
    ShardSnapshot,
};

pub struct EntityShardActor<M>
where
    M: Clone + Send + 'static,
{
    runtime: ShardRuntime<M>,
    entity_factory: EntityActorFactory<M>,
    entity_refs: BTreeMap<EntityId, ActorRef<M>>,
    remember_load: ShardRememberLoadState<M>,
    remember_store: Option<ShardRememberStore>,
    local_ddata_remember_store_provider: Option<LocalDDataShardRememberStoreProvider>,
    pending_handoffs: VecDeque<PendingShardHandOff<M>>,
    handoff_completion_waiters: Vec<ActorRef<bool>>,
}

struct PendingShardHandOff<M> {
    stop_message: M,
    reply_to: ActorRef<ShardHandOffPlan<M>>,
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
            remember_load: ShardRememberLoadState::ready(),
            remember_store: None,
            local_ddata_remember_store_provider: None,
            pending_handoffs: VecDeque::new(),
            handoff_completion_waiters: Vec::new(),
        }
    }

    pub fn new_with_remember_entities(
        shard_id: impl Into<ShardId>,
        buffer_capacity: usize,
        entity_factory: EntityActorFactory<M>,
    ) -> Self {
        Self {
            runtime: ShardRuntime::new_with_remember_entities(shard_id, buffer_capacity),
            entity_factory,
            entity_refs: BTreeMap::new(),
            remember_load: ShardRememberLoadState::ready(),
            remember_store: None,
            local_ddata_remember_store_provider: None,
            pending_handoffs: VecDeque::new(),
            handoff_completion_waiters: Vec::new(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_ddata_remember_store(
        type_name: impl Into<String>,
        shard_id: impl Into<ShardId>,
        buffer_capacity: usize,
        entity_factory: EntityActorFactory<M>,
        replica_id: impl Into<ReplicaId>,
        replicator: ActorRef<ReplicatorActorMsg<ORSet<String>>>,
        timeout: Duration,
    ) -> Self {
        let shard_id = shard_id.into();
        Self {
            runtime: ShardRuntime::new_with_remember_entities(shard_id.clone(), buffer_capacity),
            entity_factory,
            entity_refs: BTreeMap::new(),
            remember_load: ShardRememberLoadState::loading(),
            remember_store: None,
            local_ddata_remember_store_provider: Some(LocalDDataShardRememberStoreProvider::new(
                type_name, shard_id, replica_id, replicator, timeout,
            )),
            pending_handoffs: VecDeque::new(),
            handoff_completion_waiters: Vec::new(),
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

    pub fn props_with_remember_entities(
        shard_id: impl Into<ShardId>,
        buffer_capacity: usize,
        entity_factory: EntityActorFactory<M>,
    ) -> Props<Self> {
        let shard_id = shard_id.into();
        Props::new(move || {
            Self::new_with_remember_entities(shard_id, buffer_capacity, entity_factory)
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn props_with_ddata_remember_store(
        type_name: impl Into<String>,
        shard_id: impl Into<ShardId>,
        buffer_capacity: usize,
        entity_factory: EntityActorFactory<M>,
        replica_id: impl Into<ReplicaId>,
        replicator: ActorRef<ReplicatorActorMsg<ORSet<String>>>,
        timeout: Duration,
    ) -> Props<Self> {
        let type_name = type_name.into();
        let shard_id = shard_id.into();
        let replica_id = replica_id.into();
        Props::new(move || {
            Self::new_with_ddata_remember_store(
                type_name.clone(),
                shard_id.clone(),
                buffer_capacity,
                entity_factory.clone(),
                replica_id.clone(),
                replicator.clone(),
                timeout,
            )
        })
    }
}

impl<M> Actor for EntityShardActor<M>
where
    M: Clone + Send + 'static,
{
    type Msg = ShardMsg<M>;

    fn started(&mut self, ctx: &mut Context<Self::Msg>) -> ActorResult {
        self.spawn_ddata_remember_store_if_needed(ctx)?;
        self.request_remember_store_load(ctx)
    }

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        if self.remember_load.is_loading() {
            return self.receive_while_loading(ctx, msg);
        }

        self.receive_initialized(ctx, msg)
    }
}

impl<M> EntityShardActor<M>
where
    M: Clone + Send + 'static,
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
                self.apply_remembered_entities_plan(ctx, &plan)?;
                let _ = reply_to.tell(plan);
                let stashed = self.remember_load.mark_ready();
                self.replay_stashed(ctx, stashed)
            }
            ShardMsg::RememberStoreLoadResult { result } => {
                let remembered = match result {
                    Ok(remembered) => remembered,
                    Err(_) => return ctx.stop(ctx.myself()),
                };
                let plan = self
                    .runtime
                    .recover_remembered_entities(remembered.entities);
                self.apply_remembered_entities_plan(ctx, &plan)?;
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
                self.apply_deliver_plan(ctx, &plan)?;
                self.send_deliver_plan_store_effect(ctx, &plan)?;
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
                self.send_entity_terminated_store_effect(ctx, &plan)?;
                self.complete_handoff_if_ready();
                let _ = reply_to.tell(plan);
            }
            ShardMsg::ObservedEntityTerminated {
                entity_id,
                entity_path,
            } => {
                let current_incarnation = self
                    .entity_refs
                    .get(&entity_id)
                    .is_some_and(|entity| entity.path() == &entity_path);
                if current_incarnation {
                    self.entity_refs.remove(&entity_id);
                    let plan = self.runtime.entity_terminated(entity_id);
                    self.apply_termination_plan(ctx, &plan)?;
                    self.send_entity_terminated_store_effect(ctx, &plan)?;
                    self.complete_handoff_if_ready();
                }
            }
            ShardMsg::RestartRememberedEntity {
                entity_id,
                reply_to,
            } => {
                let plan = self.runtime.restart_remembered_entity(entity_id);
                self.apply_restart_remembered_entity_plan(ctx, &plan)?;
                let _ = reply_to.tell(plan);
            }
            ShardMsg::RememberedEntitiesMovedToOtherShard { entities, reply_to } => {
                let plan = self
                    .runtime
                    .remembered_entities_moved_to_other_shard(entities);
                self.apply_moved_remembered_entities_plan(ctx, &plan)?;
                self.send_moved_entities_store_effect(ctx, &plan)?;
                let _ = reply_to.tell(plan);
            }
            ShardMsg::HandOff {
                stop_message,
                reply_to,
            } => {
                self.apply_handoff(stop_message, reply_to)?;
            }
            ShardMsg::HandOffStopperTerminated { reply_to } => {
                if !self.runtime.handoff_in_progress() {
                    let _ = reply_to.tell(false);
                } else if self.entity_refs.is_empty() {
                    self.runtime.handoff_stopper_terminated();
                    let _ = reply_to.tell(true);
                    self.complete_handoff_waiters();
                } else {
                    self.handoff_completion_waiters.push(reply_to);
                }
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
                self.apply_remembered_entities_plan(ctx, &plan)?;
                let _ = reply_to.tell(plan);
            }
            ShardMsg::RememberStoreLoadResult { result } if result.is_err() => {
                return ctx.stop(ctx.myself());
            }
            ShardMsg::RememberStoreLoadResult { .. } => {}
            ShardMsg::RememberUpdateDone { update, reply_to } => {
                let plan = self.runtime.remember_update_done(update);
                self.apply_remember_update_done_plan(ctx, &plan)?;
                self.send_next_remember_update(ctx, &plan)?;
                let _ = reply_to.tell(plan);
                self.drain_pending_handoffs()?;
            }
            ShardMsg::RememberStoreUpdateResult { update: _, result } => {
                let done = match result {
                    Ok(done) => done,
                    Err(_) => return ctx.stop(ctx.myself()),
                };
                let completed = RememberShardUpdate::new(done.started, done.stopped);
                let plan = self.runtime.remember_update_done(completed);
                self.apply_remember_update_done_plan(ctx, &plan)?;
                self.send_next_remember_update(ctx, &plan)?;
                self.drain_pending_handoffs()?;
            }
        }
        Ok(())
    }

    fn spawn_ddata_remember_store_if_needed(&mut self, ctx: &Context<ShardMsg<M>>) -> ActorResult {
        if self.remember_store.is_some() {
            return Ok(());
        }
        let Some(provider) = &self.local_ddata_remember_store_provider else {
            return Ok(());
        };
        self.remember_store = Some(provider.spawn(ctx)?);
        Ok(())
    }

    fn request_remember_store_load(&self, ctx: &Context<ShardMsg<M>>) -> ActorResult {
        if let Some(store) = &self.remember_store {
            store.load(ctx)?;
        }
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

    fn send_moved_entities_store_effect(
        &self,
        ctx: &Context<ShardMsg<M>>,
        plan: &MovedRememberedEntitiesPlan,
    ) -> ActorResult {
        if let Some(update) = &plan.update {
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

    fn apply_remember_update_done_plan(
        &mut self,
        ctx: &mut Context<ShardMsg<M>>,
        plan: &RememberUpdateDonePlan<M>,
    ) -> ActorResult {
        for delivery in &plan.deliveries {
            self.deliver_to_entity(ctx, delivery)?;
        }
        Ok(())
    }

    fn apply_handoff(
        &mut self,
        stop_message: M,
        reply_to: ActorRef<ShardHandOffPlan<M>>,
    ) -> ActorResult {
        if self.runtime.remember_update_in_progress() {
            self.pending_handoffs.push_back(PendingShardHandOff {
                stop_message,
                reply_to,
            });
            return Ok(());
        }

        let plan = self.runtime.handoff(stop_message);
        self.apply_handoff_plan(&plan)?;
        let _ = reply_to.tell(plan);
        Ok(())
    }

    fn drain_pending_handoffs(&mut self) -> ActorResult {
        if self.runtime.remember_update_in_progress() {
            return Ok(());
        }

        while let Some(pending) = self.pending_handoffs.pop_front() {
            let plan = self.runtime.handoff(pending.stop_message);
            self.apply_handoff_plan(&plan)?;
            let _ = pending.reply_to.tell(plan);
        }
        Ok(())
    }

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
            EntityTerminatedPlan::RestartRemembered { entity_id } => {
                let plan = self.runtime.restart_remembered_entity(entity_id.clone());
                self.apply_restart_remembered_entity_plan(ctx, &plan)
            }
            EntityTerminatedPlan::Removed { .. }
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

    fn apply_restart_remembered_entity_plan(
        &mut self,
        ctx: &mut Context<ShardMsg<M>>,
        plan: &crate::RestartRememberedEntityPlan,
    ) -> Result<(), ActorError> {
        match plan {
            crate::RestartRememberedEntityPlan::Started { entity_id } => {
                self.ensure_entity_child(ctx, entity_id)?;
                Ok(())
            }
            crate::RestartRememberedEntityPlan::AlreadyActive { .. }
            | crate::RestartRememberedEntityPlan::Ignored { .. } => Ok(()),
        }
    }

    fn apply_remembered_entities_plan(
        &mut self,
        ctx: &mut Context<ShardMsg<M>>,
        plan: &RememberedEntitiesPlan,
    ) -> Result<(), ActorError> {
        for entity_id in &plan.started {
            self.ensure_entity_child(ctx, entity_id)?;
        }
        Ok(())
    }

    fn apply_moved_remembered_entities_plan(
        &mut self,
        ctx: &mut Context<ShardMsg<M>>,
        plan: &MovedRememberedEntitiesPlan,
    ) -> Result<(), ActorError> {
        for entity_id in &plan.removed {
            if let Some(entity) = self.entity_refs.remove(entity_id) {
                ctx.stop(entity)?;
            }
        }
        Ok(())
    }

    fn deliver_to_entity(
        &mut self,
        ctx: &mut Context<ShardMsg<M>>,
        delivery: &EntityDelivery<M>,
    ) -> Result<(), ActorError> {
        let entity_id = delivery.entity_id().to_string();
        let entity = self.ensure_entity_child(ctx, &entity_id)?;
        entity
            .tell(delivery.message().clone())
            .map_err(|error| ActorError::Message(error.reason().to_string()))
    }

    fn ensure_entity_child(
        &mut self,
        ctx: &mut Context<ShardMsg<M>>,
        entity_id: &EntityId,
    ) -> Result<ActorRef<M>, ActorError> {
        if let Some(entity) = self.entity_refs.get(entity_id) {
            return Ok(entity.clone());
        }

        let entity = self.entity_factory.spawn(ctx, entity_id)?;
        ctx.watch_with(
            &entity,
            ShardMsg::ObservedEntityTerminated {
                entity_id: entity_id.to_string(),
                entity_path: entity.path().clone(),
            },
        )?;
        self.entity_refs
            .insert(entity_id.to_string(), entity.clone());
        Ok(entity)
    }

    fn complete_handoff_if_ready(&mut self) {
        if !self.runtime.handoff_in_progress()
            || !self.entity_refs.is_empty()
            || self.handoff_completion_waiters.is_empty()
        {
            return;
        }
        self.runtime.handoff_stopper_terminated();
        self.complete_handoff_waiters();
    }

    fn complete_handoff_waiters(&mut self) {
        for reply_to in self.handoff_completion_waiters.drain(..) {
            let _ = reply_to.tell(true);
        }
    }
}
