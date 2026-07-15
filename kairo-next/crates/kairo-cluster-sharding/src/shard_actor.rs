#![deny(missing_docs)]
//! Actor orchestration for deterministic shard lifecycle plans and remember storage.

use std::collections::VecDeque;
use std::time::Duration;

use kairo_actor::{Actor, ActorRef, ActorResult, Context, Props};

use crate::shard_loading::ShardRememberLoadState;
use crate::shard_store::{
    LocalShardRememberStoreProvider, ShardRememberStore, ShardRememberStoreError,
};
use crate::{
    EntityId, EntityTerminatedPlan, MovedRememberedEntitiesPlan, PassivatePlan,
    RememberShardStoreMsg, RememberShardStoreState, RememberShardUpdate, RememberShardUpdateDone,
    RememberUpdateDonePlan, RememberedEntities, RememberedEntitiesPlan,
    RestartRememberedEntityPlan, ShardDeliverPlan, ShardHandOffPlan, ShardId, ShardRuntime,
    ShardingEnvelope,
};

/// Actor-backed boundary around [`ShardRuntime`].
///
/// The actor serializes shard commands, stashes traffic until remembered entity
/// loading completes, translates runtime plans into remember-store effects, and
/// defers handoff until persistent updates finish. Entity child ownership and
/// business-message delivery are composed separately by `EntityShardActor`.
pub struct ShardActor<M> {
    runtime: ShardRuntime<M>,
    remember_load: ShardRememberLoadState<M>,
    remember_store: Option<ShardRememberStore>,
    local_remember_store_provider: Option<LocalShardRememberStoreProvider>,
    pending_handoffs: VecDeque<PendingShardHandOff<M>>,
}

struct PendingShardHandOff<M> {
    stop_message: M,
    reply_to: ActorRef<ShardHandOffPlan<M>>,
}

impl<M> ShardActor<M> {
    /// Creates an initialized shard actor with remember-entities disabled.
    pub fn new(shard_id: impl Into<ShardId>, buffer_capacity: usize) -> Self {
        Self {
            runtime: ShardRuntime::new(shard_id, buffer_capacity),
            remember_load: ShardRememberLoadState::ready(),
            remember_store: None,
            local_remember_store_provider: None,
            pending_handoffs: VecDeque::new(),
        }
    }

    /// Creates an initialized planner-mode shard with remember-entities enabled.
    ///
    /// This mode has no store actor. It returns remember-update plans to callers,
    /// which must acknowledge them with [`ShardMsg::RememberUpdateDone`]. Use
    /// [`Self::new_with_remember_store`] for actor-owned persistence.
    pub fn new_with_remember_entities(
        shard_id: impl Into<ShardId>,
        buffer_capacity: usize,
    ) -> Self {
        Self {
            runtime: ShardRuntime::new_with_remember_entities(shard_id, buffer_capacity),
            remember_load: ShardRememberLoadState::ready(),
            remember_store: None,
            local_remember_store_provider: None,
            pending_handoffs: VecDeque::new(),
        }
    }

    /// Creates a planner-mode shard that stashes commands until recovery is supplied.
    ///
    /// The caller must finish initialization with
    /// [`ShardMsg::RecoverRememberedEntities`] or
    /// [`ShardMsg::RememberedEntitiesLoaded`].
    pub fn new_loading_remembered_entities(
        shard_id: impl Into<ShardId>,
        buffer_capacity: usize,
    ) -> Self {
        Self {
            runtime: ShardRuntime::new_with_remember_entities(shard_id, buffer_capacity),
            remember_load: ShardRememberLoadState::loading(),
            remember_store: None,
            local_remember_store_provider: None,
            pending_handoffs: VecDeque::new(),
        }
    }

    /// Creates a shard that loads and persists entities through `remember_store`.
    ///
    /// The shard stops itself when a store ask fails or exceeds `timeout`, leaving
    /// restart and backoff policy to its parent actor.
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
            pending_handoffs: VecDeque::new(),
        }
    }

    /// Creates a shard that spawns and owns a local remember-store child.
    ///
    /// The shard identifier is taken from `store_state`. Store asks use `timeout`,
    /// and any load or update failure stops the shard.
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
            pending_handoffs: VecDeque::new(),
        }
    }

    /// Builds props for an initialized shard with remember-entities disabled.
    pub fn props(shard_id: impl Into<ShardId>, buffer_capacity: usize) -> Props<Self>
    where
        M: Send + 'static,
    {
        let shard_id = shard_id.into();
        Props::new(move || Self::new(shard_id, buffer_capacity))
    }

    /// Builds props for initialized planner-mode remember-entity orchestration.
    ///
    /// Callers own completion of returned remember updates; no store child is
    /// created by these props.
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

    /// Builds props for planner-mode recovery before ordinary commands are replayed.
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

    /// Builds props for a shard using the supplied remember-store actor.
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

    /// Builds props for a shard that creates its own local remember-store child.
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

    /// Borrows the deterministic shard lifecycle state.
    pub fn runtime(&self) -> &ShardRuntime<M> {
        &self.runtime
    }
}

/// Commands accepted by [`ShardActor`].
///
/// Reply references make every externally driven transition explicit. The
/// `Observed*` and `RememberStore*Result` variants are internal actor adapters
/// kept in the same typed protocol so watch and async ask results re-enter the
/// synchronous receive loop.
pub enum ShardMsg<M> {
    /// Routes one business envelope through the shard runtime.
    Deliver {
        /// Entity-addressed business message.
        message: ShardingEnvelope<M>,
        /// Recipient for the selected delivery plan.
        reply_to: ActorRef<ShardDeliverPlan<M>>,
    },
    /// Requests passivation of one entity.
    Passivate {
        /// Entity that requested passivation.
        entity_id: EntityId,
        /// Application-defined message used to stop the entity.
        stop_message: M,
        /// Recipient for the passivation plan.
        reply_to: ActorRef<PassivatePlan<M>>,
    },
    /// Reports entity termination and requests the resulting plan.
    EntityTerminated {
        /// Terminated entity identifier.
        entity_id: EntityId,
        /// Recipient for removal, restart, or persistence work.
        reply_to: ActorRef<EntityTerminatedPlan<M>>,
    },
    /// Reports termination from an actor watch without exposing a reply channel.
    ObservedEntityTerminated {
        /// Terminated entity identifier.
        entity_id: EntityId,
    },
    /// Triggers restart of an unexpectedly terminated remembered entity.
    RestartRememberedEntity {
        /// Remembered entity waiting for restart.
        entity_id: EntityId,
        /// Recipient for the idempotent restart result.
        reply_to: ActorRef<RestartRememberedEntityPlan>,
    },
    /// Removes remembered entities that now map to a different shard.
    RememberedEntitiesMovedToOtherShard {
        /// Candidate entity identifiers, deduplicated by the runtime.
        entities: Vec<EntityId>,
        /// Recipient for removed, ignored, and persistence results.
        reply_to: ActorRef<MovedRememberedEntitiesPlan>,
    },
    /// Begins coordinator-directed shard handoff.
    HandOff {
        /// Application-defined message used to stop running entities.
        stop_message: M,
        /// Recipient for the handoff plan after pending store writes complete.
        reply_to: ActorRef<ShardHandOffPlan<M>>,
    },
    /// Reports termination of the handoff entity-stopper child.
    HandOffStopperTerminated {
        /// Recipient told whether a handoff was actually in progress.
        reply_to: ActorRef<bool>,
    },
    /// Supplies remembered entities through the explicit planner recovery path.
    RecoverRememberedEntities {
        /// Loaded entity identifiers.
        entities: Vec<EntityId>,
        /// Recipient for the deterministic recovery result.
        reply_to: ActorRef<RememberedEntitiesPlan>,
    },
    /// Supplies remembered entities through the external-load completion path.
    RememberedEntitiesLoaded {
        /// Loaded entity identifiers.
        entities: Vec<EntityId>,
        /// Recipient for the deterministic recovery result.
        reply_to: ActorRef<RememberedEntitiesPlan>,
    },
    /// Returns an actor-owned remember-store load ask to the shard mailbox.
    RememberStoreLoadResult {
        /// Loaded identifiers or the failure that makes the shard stop.
        result: Result<RememberedEntities, ShardRememberStoreError>,
    },
    /// Acknowledges a planner-mode remember update.
    RememberUpdateDone {
        /// Completed start and stop set.
        update: RememberShardUpdate,
        /// Recipient for released deliveries and any next update batch.
        reply_to: ActorRef<RememberUpdateDonePlan<M>>,
    },
    /// Returns an actor-owned remember-store update ask to the shard mailbox.
    RememberStoreUpdateResult {
        /// Submitted update retained for correlation and diagnostics.
        update: RememberShardUpdate,
        /// Store-confirmed update or the failure that makes the shard stop.
        result: Result<RememberShardUpdateDone, ShardRememberStoreError>,
    },
    /// Marks whether the hosting node is preparing for coordinated shutdown.
    SetPreparingForShutdown {
        /// Whether shutdown preparation is active.
        preparing: bool,
    },
    /// Requests a diagnostic snapshot of shard lifecycle state.
    GetState {
        /// Recipient for the snapshot.
        reply_to: ActorRef<ShardSnapshot>,
    },
}

/// Diagnostic state exposed by [`ShardMsg::GetState`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardSnapshot {
    /// Stable shard identifier.
    pub shard_id: ShardId,
    /// Running entity identifiers in deterministic order.
    pub active_entities: Vec<EntityId>,
    /// Number of retained entity lifecycle records, including non-running states.
    pub entity_count: usize,
    /// Number of business messages buffered across all entities.
    pub total_buffered: usize,
    /// Whether an entity-stopper handoff is active.
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
            ShardMsg::ObservedEntityTerminated { entity_id } => {
                let plan = self.runtime.entity_terminated(entity_id);
                self.send_entity_terminated_store_effect(ctx, &plan)?;
            }
            ShardMsg::RestartRememberedEntity {
                entity_id,
                reply_to,
            } => {
                let plan = self.runtime.restart_remembered_entity(entity_id);
                let _ = reply_to.tell(plan);
            }
            ShardMsg::RememberedEntitiesMovedToOtherShard { entities, reply_to } => {
                let plan = self
                    .runtime
                    .remembered_entities_moved_to_other_shard(entities);
                self.send_moved_entities_store_effect(ctx, &plan)?;
                let _ = reply_to.tell(plan);
            }
            ShardMsg::HandOff {
                stop_message,
                reply_to,
            } => {
                self.apply_handoff(stop_message, reply_to);
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
                self.drain_pending_handoffs();
            }
            ShardMsg::RememberStoreUpdateResult { update: _, result } => {
                let done = match result {
                    Ok(done) => done,
                    Err(_) => {
                        return self.stop_for_remember_store_failure(ctx);
                    }
                };
                let completed = RememberShardUpdate::new(done.started, done.stopped);
                let plan = self.runtime.remember_update_done(completed);
                self.send_next_remember_update(ctx, &plan)?;
                self.drain_pending_handoffs();
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

    fn apply_handoff(&mut self, stop_message: M, reply_to: ActorRef<ShardHandOffPlan<M>>) {
        if self.runtime.remember_update_in_progress() {
            self.pending_handoffs.push_back(PendingShardHandOff {
                stop_message,
                reply_to,
            });
            return;
        }

        let plan = self.runtime.handoff(stop_message);
        let _ = reply_to.tell(plan);
    }

    fn drain_pending_handoffs(&mut self) {
        if self.runtime.remember_update_in_progress() {
            return;
        }

        while let Some(pending) = self.pending_handoffs.pop_front() {
            let plan = self.runtime.handoff(pending.stop_message);
            let _ = pending.reply_to.tell(plan);
        }
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
