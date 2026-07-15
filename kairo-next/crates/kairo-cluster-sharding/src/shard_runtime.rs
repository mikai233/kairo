#![deny(missing_docs)]
//! Deterministic entity lifecycle, buffering, persistence, and handoff decisions.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::shard_remember::ShardRememberState;
use crate::{EntityId, RememberShardUpdate, ShardId, ShardStopped, ShardingEnvelope};

/// A business message paired with the entity that should receive it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntityDelivery<M> {
    entity_id: EntityId,
    message: M,
}

impl<M> EntityDelivery<M> {
    /// Creates a delivery for `entity_id`.
    pub fn new(entity_id: impl Into<EntityId>, message: M) -> Self {
        Self {
            entity_id: entity_id.into(),
            message,
        }
    }

    /// Returns the destination entity identifier.
    pub fn entity_id(&self) -> &str {
        &self.entity_id
    }

    /// Borrows the business message.
    pub fn message(&self) -> &M {
        &self.message
    }

    /// Splits the delivery into its entity identifier and business message.
    pub fn into_parts(self) -> (EntityId, M) {
        (self.entity_id, self.message)
    }
}

/// The lifecycle state retained for an entity owned by a shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardEntityState {
    /// The entity is running and accepts direct delivery.
    Active,
    /// The entity has received its passivation stop message.
    Passivating,
    /// The entity stopped after passivation and its remember-store removal is pending.
    RememberingStop,
    /// A remembered entity stopped unexpectedly and is awaiting restart.
    WaitingForRestart,
}

/// The side effect selected for one shard-envelope delivery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShardDeliverPlan<M> {
    /// Starts an unremembered or previously stopped entity and delivers the message.
    StartEntity {
        /// The first delivery for the entity incarnation.
        delivery: EntityDelivery<M>,
    },
    /// Persists a remembered-entity start before delivering its buffered message.
    RememberUpdate {
        /// The remember-store update to write.
        update: RememberShardUpdate,
    },
    /// Delivers directly to an active entity.
    Deliver {
        /// The delivery for the active entity.
        delivery: EntityDelivery<M>,
    },
    /// Retains the message while the entity is stopping or awaiting a store update.
    Buffered {
        /// The entity whose FIFO buffer accepted the message.
        entity_id: EntityId,
    },
    /// Drops a message that cannot enter the shard delivery path.
    Dropped {
        /// The destination entity, or `None` when the envelope identifier was empty.
        entity_id: Option<EntityId>,
        /// Why the message was dropped.
        reason: ShardDropReason,
        /// The undelivered business message.
        message: M,
    },
}

/// The reason a shard rejected a business message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardDropReason {
    /// The envelope did not name an entity.
    EmptyEntityId,
    /// The shard-wide buffered-message capacity was exhausted.
    BufferFull,
}

/// The side effect selected for an entity passivation request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PassivatePlan<M> {
    /// Sends the supplied stop message to the active entity.
    SendStop {
        /// The entity entering passivation.
        entity_id: EntityId,
        /// The application-defined message that asks the entity to stop.
        stop_message: M,
    },
    /// Leaves the lifecycle unchanged because passivation cannot start.
    Ignored {
        /// The requested entity identifier.
        entity_id: EntityId,
        /// Why the request was ignored.
        reason: PassivateIgnoreReason,
    },
}

/// The reason an entity passivation request was ignored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PassivateIgnoreReason {
    /// No running entity exists for the identifier.
    UnknownEntity,
    /// The entity is already passivating or waiting for its remembered stop update.
    AlreadyPassivating,
}

/// The side effect selected after an entity child terminates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntityTerminatedPlan<M> {
    /// Removes an entity that has no further lifecycle work.
    Removed {
        /// The removed entity identifier.
        entity_id: EntityId,
    },
    /// Restarts a passivated, unremembered entity and replays its FIFO buffer.
    Restart {
        /// The buffered deliveries, in arrival order.
        buffered: Vec<EntityDelivery<M>>,
    },
    /// Restarts an unexpectedly terminated remembered entity without removing it from storage.
    RestartRemembered {
        /// The remembered entity to restart.
        entity_id: EntityId,
    },
    /// Persists a remembered-entity stop after passivation.
    RememberUpdate {
        /// The remember-store update to write.
        update: RememberShardUpdate,
    },
    /// Waits for the current remember-store update before writing the entity stop.
    RememberUpdateQueued {
        /// The entity whose stop is queued.
        entity_id: EntityId,
    },
    /// Ignores a termination for an entity not owned by the shard.
    IgnoredUnknown {
        /// The unknown entity identifier.
        entity_id: EntityId,
    },
}

/// The side effect selected for a remembered-entity restart trigger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RestartRememberedEntityPlan {
    /// Marks the waiting remembered entity active so its child can be started.
    Started {
        /// The entity to start.
        entity_id: EntityId,
    },
    /// Acknowledges an idempotent restart after the entity is already active.
    AlreadyActive {
        /// The already-active entity.
        entity_id: EntityId,
    },
    /// Leaves the lifecycle unchanged because a remembered restart is inapplicable.
    Ignored {
        /// The requested entity identifier.
        entity_id: EntityId,
        /// Why the restart was ignored.
        reason: RestartRememberedEntityIgnoreReason,
    },
}

/// The reason a remembered-entity restart trigger was ignored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestartRememberedEntityIgnoreReason {
    /// Remember-entities is disabled for this shard.
    NotRememberingEntities,
    /// The entity is not in the waiting-for-restart state.
    NotWaitingForRestart,
}

/// The result of removing remembered entities whose shard assignment changed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MovedRememberedEntitiesPlan {
    /// Entity identifiers removed from this shard's runtime state.
    pub removed: Vec<EntityId>,
    /// Empty, unknown, stopping, or otherwise inapplicable entity identifiers.
    pub ignored: Vec<EntityId>,
    /// The first remember-store stop update to write, if no update was already active.
    pub update: Option<RememberShardUpdate>,
}

/// The side effect selected when a coordinator hands this shard off.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShardHandOffPlan<M> {
    /// Starts a stopper that waits for every currently running entity to terminate.
    StartEntityStopper {
        /// The shard being handed off.
        shard: ShardId,
        /// Running entities that must stop before handoff completes.
        entities: Vec<EntityId>,
        /// The application-defined stop message for each entity.
        stop_message: M,
    },
    /// Stops entities and acknowledges immediately during coordinated node shutdown.
    StopImmediately {
        /// The shard being handed off.
        shard: ShardId,
        /// Running entities to stop without waiting for a stopper protocol.
        entities: Vec<EntityId>,
        /// The application-defined stop message for each entity.
        stop_message: M,
        /// The completion acknowledgement to return to the coordinator.
        stopped: ShardStopped,
    },
    /// Acknowledges immediately because no running entities remain.
    ReplyShardStopped {
        /// The shard being handed off.
        shard: ShardId,
        /// The completion acknowledgement to return to the coordinator.
        stopped: ShardStopped,
    },
    /// Leaves an existing handoff attempt in control.
    AlreadyInProgress {
        /// The shard whose handoff is already active.
        shard: ShardId,
    },
}

/// The deterministic startup result after loading remembered entity identifiers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RememberedEntitiesPlan {
    /// Remembered entities newly marked active.
    pub started: Vec<EntityId>,
    /// Remembered entities already represented in runtime state.
    pub already_active: Vec<EntityId>,
    /// The number of empty identifiers discarded from the loaded set.
    pub ignored_empty: usize,
}

/// The side effects released after a remember-store update completes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RememberUpdateDonePlan<M> {
    /// Buffered messages now eligible for entity delivery, in FIFO order per entity.
    pub deliveries: Vec<EntityDelivery<M>>,
    /// The next batched remember-store update, if pending changes remain.
    pub next_update: Option<RememberShardUpdate>,
}

/// Pure shard lifecycle state used by the actor-backed shard boundary.
///
/// The runtime owns entity states, a shard-wide bounded collection of per-entity
/// FIFO buffers, remember-store update sequencing, and handoff preparation. Its
/// return plans keep actor spawning, message delivery, persistence, and replies
/// outside the state machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardRuntime<M> {
    shard_id: ShardId,
    buffer_capacity: usize,
    entities: BTreeMap<EntityId, ShardEntityState>,
    message_buffers: BTreeMap<EntityId, VecDeque<M>>,
    remember: ShardRememberState,
    preparing_for_shutdown: bool,
    handoff_in_progress: bool,
}

impl<M> ShardRuntime<M> {
    /// Creates a shard with remember-entities disabled.
    ///
    /// `buffer_capacity` bounds the total number of buffered business messages
    /// across all entity identifiers, matching Pekko's shard-wide limit.
    pub fn new(shard_id: impl Into<ShardId>, buffer_capacity: usize) -> Self {
        Self {
            shard_id: shard_id.into(),
            buffer_capacity,
            entities: BTreeMap::new(),
            message_buffers: BTreeMap::new(),
            remember: ShardRememberState::disabled(),
            preparing_for_shutdown: false,
            handoff_in_progress: false,
        }
    }

    /// Creates a shard whose entity starts and passivation stops are persisted.
    pub fn new_with_remember_entities(
        shard_id: impl Into<ShardId>,
        buffer_capacity: usize,
    ) -> Self {
        Self {
            remember: ShardRememberState::enabled(),
            ..Self::new(shard_id, buffer_capacity)
        }
    }

    /// Returns this shard's stable identifier.
    pub fn shard_id(&self) -> &ShardId {
        &self.shard_id
    }

    /// Returns the retained lifecycle state for `entity_id`.
    pub fn entity_state(&self, entity_id: &EntityId) -> Option<ShardEntityState> {
        self.entities.get(entity_id).copied()
    }

    /// Returns running entity identifiers in deterministic order.
    ///
    /// Passivating entities remain running until their child terminates, while
    /// entities waiting on persistence or restart are excluded.
    pub fn active_entity_ids(&self) -> Vec<EntityId> {
        self.entities
            .iter()
            .filter_map(|(entity_id, state)| match state {
                ShardEntityState::Active | ShardEntityState::Passivating => Some(entity_id.clone()),
                ShardEntityState::RememberingStop | ShardEntityState::WaitingForRestart => None,
            })
            .collect()
    }

    /// Returns the number of entity lifecycle records retained by the shard.
    pub fn entity_count(&self) -> usize {
        self.entities.len()
    }

    /// Returns the number of messages buffered for `entity_id`.
    pub fn buffered_count(&self, entity_id: &EntityId) -> usize {
        self.message_buffers.get(entity_id).map_or(0, VecDeque::len)
    }

    /// Returns the number of messages buffered across every entity.
    pub fn total_buffered_count(&self) -> usize {
        self.message_buffers.values().map(VecDeque::len).sum()
    }

    /// Reports whether an entity-stopper handoff is active.
    pub fn handoff_in_progress(&self) -> bool {
        self.handoff_in_progress
    }

    /// Reports whether entity identifiers are persisted by this shard.
    pub fn remember_entities(&self) -> bool {
        self.remember.is_enabled()
    }

    /// Reports whether a remember-store update is awaiting completion.
    pub fn remember_update_in_progress(&self) -> bool {
        self.remember.update_in_progress()
    }

    /// Marks whether the host node is preparing for coordinated shutdown.
    ///
    /// Handoff during shutdown skips the ordinary entity-stopper wait and
    /// returns an immediate stopped acknowledgement after issuing stop messages.
    pub fn set_preparing_for_shutdown(&mut self, preparing: bool) {
        self.preparing_for_shutdown = preparing;
    }

    /// Installs entity identifiers loaded from remember storage as active.
    ///
    /// Input is deduplicated and ordered, empty identifiers are counted rather
    /// than started, and existing lifecycle records are preserved.
    pub fn recover_remembered_entities(
        &mut self,
        entities: impl IntoIterator<Item = EntityId>,
    ) -> RememberedEntitiesPlan {
        let mut started = Vec::new();
        let mut already_active = Vec::new();
        let mut ignored_empty = 0;

        for entity_id in entities.into_iter().collect::<BTreeSet<_>>() {
            if entity_id.is_empty() {
                ignored_empty += 1;
                continue;
            }
            match self.entities.get(&entity_id) {
                Some(_) => already_active.push(entity_id),
                None => {
                    self.entities
                        .insert(entity_id.clone(), ShardEntityState::Active);
                    started.push(entity_id);
                }
            }
        }

        RememberedEntitiesPlan {
            started,
            already_active,
            ignored_empty,
        }
    }

    /// Routes one entity envelope through the shard lifecycle.
    ///
    /// Active entities receive direct deliveries. Messages for passivating or
    /// remember-store-blocked entities enter per-entity FIFO buffers subject to
    /// the global capacity. A new remembered entity is buffered until its start
    /// update completes; an unremembered entity can start immediately.
    pub fn deliver(&mut self, envelope: ShardingEnvelope<M>) -> ShardDeliverPlan<M> {
        let (entity_id, message) = envelope.into_parts();
        if entity_id.is_empty() {
            return ShardDeliverPlan::Dropped {
                entity_id: None,
                reason: ShardDropReason::EmptyEntityId,
                message,
            };
        }

        match self.entities.get(&entity_id) {
            Some(ShardEntityState::Active) => ShardDeliverPlan::Deliver {
                delivery: EntityDelivery::new(entity_id, message),
            },
            Some(ShardEntityState::WaitingForRestart) => self.start_entity(entity_id, message),
            Some(ShardEntityState::Passivating | ShardEntityState::RememberingStop) => {
                match self.buffer_message(&entity_id, message) {
                    Ok(()) => ShardDeliverPlan::Buffered { entity_id },
                    Err(message) => ShardDeliverPlan::Dropped {
                        entity_id: Some(entity_id),
                        reason: ShardDropReason::BufferFull,
                        message,
                    },
                }
            }
            None if self.remember_entities() => {
                self.buffer_new_remembered_entity(entity_id, message)
            }
            None => self.start_entity(entity_id, message),
        }
    }

    /// Applies a completed remember-store update and releases subsequent work.
    ///
    /// Completed starts activate their entities and drain buffered messages.
    /// Completed stops remove the entity; a buffered message races the stop by
    /// scheduling a fresh remembered start. Pending store changes are returned
    /// as one deterministic follow-up batch.
    pub fn remember_update_done(
        &mut self,
        update: RememberShardUpdate,
    ) -> RememberUpdateDonePlan<M> {
        let mut deliveries = Vec::new();
        for entity_id in update.started() {
            if entity_id.is_empty() {
                continue;
            }
            self.entities
                .insert(entity_id.clone(), ShardEntityState::Active);
            deliveries.extend(
                self.drain_buffer(entity_id)
                    .into_iter()
                    .map(|message| EntityDelivery::new(entity_id.clone(), message)),
            );
        }

        for entity_id in update.stopped() {
            let has_buffered_messages = self.buffered_count(entity_id) > 0;
            self.entities.remove(entity_id);
            if self.remember_entities() && has_buffered_messages {
                let _ = self.remember.record_start(entity_id.clone());
            } else {
                self.message_buffers.remove(entity_id);
            }
        }

        let next_update = self.remember.complete_update(&update);
        RememberUpdateDonePlan {
            deliveries,
            next_update,
        }
    }

    /// Begins passivation for a running entity.
    ///
    /// The entity remains in the running set until termination is observed, and
    /// messages arriving meanwhile are buffered by [`Self::deliver`]. Duplicate
    /// or unknown requests are idempotently ignored.
    pub fn passivate(
        &mut self,
        entity_id: impl Into<EntityId>,
        stop_message: M,
    ) -> PassivatePlan<M> {
        let entity_id = entity_id.into();
        match self.entities.get_mut(&entity_id) {
            Some(state @ ShardEntityState::Active) => {
                *state = ShardEntityState::Passivating;
                PassivatePlan::SendStop {
                    entity_id,
                    stop_message,
                }
            }
            Some(ShardEntityState::Passivating | ShardEntityState::RememberingStop) => {
                PassivatePlan::Ignored {
                    entity_id,
                    reason: PassivateIgnoreReason::AlreadyPassivating,
                }
            }
            Some(ShardEntityState::WaitingForRestart) => PassivatePlan::Ignored {
                entity_id,
                reason: PassivateIgnoreReason::UnknownEntity,
            },
            None => PassivatePlan::Ignored {
                entity_id,
                reason: PassivateIgnoreReason::UnknownEntity,
            },
        }
    }

    /// Applies an observed entity-child termination.
    ///
    /// Unexpected remembered termination retains the entity for restart without
    /// deleting its store entry. Expected passivation either persists a stop or,
    /// when remembering is disabled, restarts only if buffered traffic exists.
    /// During handoff every termination only removes local state so no entity is
    /// restarted while the shard is leaving its region.
    pub fn entity_terminated(&mut self, entity_id: impl Into<EntityId>) -> EntityTerminatedPlan<M> {
        let entity_id = entity_id.into();
        match self.entities.get(&entity_id).copied() {
            Some(_) if self.handoff_in_progress => {
                self.entities.remove(&entity_id);
                self.message_buffers.remove(&entity_id);
                EntityTerminatedPlan::Removed { entity_id }
            }
            Some(ShardEntityState::Active) if self.remember_entities() => {
                self.entities
                    .insert(entity_id.clone(), ShardEntityState::WaitingForRestart);
                self.message_buffers.remove(&entity_id);
                EntityTerminatedPlan::RestartRemembered { entity_id }
            }
            Some(ShardEntityState::Active) => {
                self.entities.remove(&entity_id);
                self.message_buffers.remove(&entity_id);
                EntityTerminatedPlan::Removed { entity_id }
            }
            Some(ShardEntityState::Passivating) if self.remember_entities() => {
                self.entities
                    .insert(entity_id.clone(), ShardEntityState::RememberingStop);
                match self.remember.record_stop(entity_id.clone()) {
                    Some(update) => EntityTerminatedPlan::RememberUpdate { update },
                    None => EntityTerminatedPlan::RememberUpdateQueued { entity_id },
                }
            }
            Some(ShardEntityState::Passivating) => {
                self.entities.remove(&entity_id);
                let buffered = self.drain_buffer(&entity_id);
                if buffered.is_empty() {
                    EntityTerminatedPlan::Removed { entity_id }
                } else {
                    self.entities
                        .insert(entity_id.clone(), ShardEntityState::Active);
                    EntityTerminatedPlan::Restart {
                        buffered: buffered
                            .into_iter()
                            .map(|message| EntityDelivery::new(entity_id.clone(), message))
                            .collect(),
                    }
                }
            }
            Some(ShardEntityState::RememberingStop) => {
                EntityTerminatedPlan::RememberUpdateQueued { entity_id }
            }
            Some(ShardEntityState::WaitingForRestart) => {
                EntityTerminatedPlan::RestartRemembered { entity_id }
            }
            None => EntityTerminatedPlan::IgnoredUnknown { entity_id },
        }
    }

    /// Activates a remembered entity that was waiting after unexpected termination.
    ///
    /// The operation is idempotent for an already active entity and never writes
    /// remember storage because the identifier was not removed on termination.
    pub fn restart_remembered_entity(
        &mut self,
        entity_id: impl Into<EntityId>,
    ) -> RestartRememberedEntityPlan {
        let entity_id = entity_id.into();
        if !self.remember_entities() {
            return RestartRememberedEntityPlan::Ignored {
                entity_id,
                reason: RestartRememberedEntityIgnoreReason::NotRememberingEntities,
            };
        }

        match self.entities.get(&entity_id).copied() {
            Some(ShardEntityState::WaitingForRestart) => {
                self.entities
                    .insert(entity_id.clone(), ShardEntityState::Active);
                RestartRememberedEntityPlan::Started { entity_id }
            }
            Some(ShardEntityState::Active) => {
                RestartRememberedEntityPlan::AlreadyActive { entity_id }
            }
            Some(ShardEntityState::Passivating | ShardEntityState::RememberingStop) | None => {
                RestartRememberedEntityPlan::Ignored {
                    entity_id,
                    reason: RestartRememberedEntityIgnoreReason::NotWaitingForRestart,
                }
            }
        }
    }

    /// Removes remembered entities reassigned to a different shard.
    ///
    /// Input is deduplicated and ordered. Active and waiting-for-restart entities
    /// are removed locally and recorded as remembered stops; stopping, unknown,
    /// empty, or non-remembering entries are reported as ignored.
    pub fn remembered_entities_moved_to_other_shard(
        &mut self,
        entities: impl IntoIterator<Item = EntityId>,
    ) -> MovedRememberedEntitiesPlan {
        let mut removed = Vec::new();
        let mut ignored = Vec::new();
        let mut update = None;

        for entity_id in entities.into_iter().collect::<BTreeSet<_>>() {
            if entity_id.is_empty() || !self.remember_entities() {
                ignored.push(entity_id);
                continue;
            }

            match self.entities.get(&entity_id).copied() {
                Some(ShardEntityState::Active | ShardEntityState::WaitingForRestart) => {
                    self.entities.remove(&entity_id);
                    self.message_buffers.remove(&entity_id);
                    removed.push(entity_id.clone());
                    if let Some(next_update) = self.remember.record_stop(entity_id) {
                        update.get_or_insert(next_update);
                    }
                }
                Some(ShardEntityState::Passivating | ShardEntityState::RememberingStop) | None => {
                    ignored.push(entity_id);
                }
            }
        }

        MovedRememberedEntitiesPlan {
            removed,
            ignored,
            update,
        }
    }

    /// Begins coordinator-directed shard handoff.
    ///
    /// Ordinary handoff starts one entity stopper when running children remain,
    /// or acknowledges immediately when the shard is empty. Coordinated shutdown
    /// emits stop messages and an immediate acknowledgement. A duplicate request
    /// cannot replace the in-flight handoff.
    pub fn handoff(&mut self, stop_message: M) -> ShardHandOffPlan<M> {
        if self.handoff_in_progress {
            return ShardHandOffPlan::AlreadyInProgress {
                shard: self.shard_id.clone(),
            };
        }

        let entities = self.active_entity_ids();
        if self.preparing_for_shutdown {
            self.handoff_in_progress = true;
            return ShardHandOffPlan::StopImmediately {
                stopped: ShardStopped {
                    shard_id: self.shard_id.clone(),
                },
                shard: self.shard_id.clone(),
                entities,
                stop_message,
            };
        }

        if entities.is_empty() {
            ShardHandOffPlan::ReplyShardStopped {
                stopped: ShardStopped {
                    shard_id: self.shard_id.clone(),
                },
                shard: self.shard_id.clone(),
            }
        } else {
            self.handoff_in_progress = true;
            ShardHandOffPlan::StartEntityStopper {
                shard: self.shard_id.clone(),
                entities,
                stop_message,
            }
        }
    }

    /// Clears the entity-stopper handoff flag.
    ///
    /// Returns whether a handoff was active, allowing duplicate or stale stopper
    /// termination notifications to be rejected by the actor boundary.
    pub fn handoff_stopper_terminated(&mut self) -> bool {
        let was_in_progress = self.handoff_in_progress;
        self.handoff_in_progress = false;
        was_in_progress
    }

    fn buffer_message(&mut self, entity_id: &EntityId, message: M) -> Result<(), M> {
        if self.total_buffered_count() >= self.buffer_capacity {
            return Err(message);
        }
        self.message_buffers
            .entry(entity_id.clone())
            .or_default()
            .push_back(message);
        Ok(())
    }

    fn buffer_new_remembered_entity(
        &mut self,
        entity_id: EntityId,
        message: M,
    ) -> ShardDeliverPlan<M> {
        match self.buffer_message(&entity_id, message) {
            Ok(()) => match self.remember.record_start(entity_id.clone()) {
                Some(update) => ShardDeliverPlan::RememberUpdate { update },
                None => ShardDeliverPlan::Buffered { entity_id },
            },
            Err(message) => ShardDeliverPlan::Dropped {
                entity_id: Some(entity_id),
                reason: ShardDropReason::BufferFull,
                message,
            },
        }
    }

    fn start_entity(&mut self, entity_id: EntityId, message: M) -> ShardDeliverPlan<M> {
        self.entities
            .insert(entity_id.clone(), ShardEntityState::Active);
        ShardDeliverPlan::StartEntity {
            delivery: EntityDelivery::new(entity_id, message),
        }
    }

    fn drain_buffer(&mut self, entity_id: &EntityId) -> Vec<M> {
        self.message_buffers
            .remove(entity_id)
            .map(VecDeque::into_iter)
            .map(Iterator::collect)
            .unwrap_or_default()
    }
}
