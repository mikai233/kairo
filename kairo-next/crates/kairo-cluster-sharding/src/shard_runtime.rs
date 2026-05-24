use std::collections::{BTreeMap, VecDeque};

use crate::{EntityId, ShardId, ShardStopped, ShardingEnvelope};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntityDelivery<M> {
    entity_id: EntityId,
    message: M,
}

impl<M> EntityDelivery<M> {
    pub fn new(entity_id: impl Into<EntityId>, message: M) -> Self {
        Self {
            entity_id: entity_id.into(),
            message,
        }
    }

    pub fn entity_id(&self) -> &str {
        &self.entity_id
    }

    pub fn message(&self) -> &M {
        &self.message
    }

    pub fn into_parts(self) -> (EntityId, M) {
        (self.entity_id, self.message)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardEntityState {
    Active,
    Passivating,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShardDeliverPlan<M> {
    StartEntity {
        delivery: EntityDelivery<M>,
    },
    Deliver {
        delivery: EntityDelivery<M>,
    },
    Buffered {
        entity_id: EntityId,
    },
    Dropped {
        entity_id: Option<EntityId>,
        reason: ShardDropReason,
        message: M,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardDropReason {
    EmptyEntityId,
    BufferFull,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PassivatePlan<M> {
    SendStop {
        entity_id: EntityId,
        stop_message: M,
    },
    Ignored {
        entity_id: EntityId,
        reason: PassivateIgnoreReason,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PassivateIgnoreReason {
    UnknownEntity,
    AlreadyPassivating,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntityTerminatedPlan<M> {
    Removed { entity_id: EntityId },
    Restart { buffered: Vec<EntityDelivery<M>> },
    IgnoredUnknown { entity_id: EntityId },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShardHandOffPlan<M> {
    StartEntityStopper {
        shard: ShardId,
        entities: Vec<EntityId>,
        stop_message: M,
    },
    StopImmediately {
        shard: ShardId,
        entities: Vec<EntityId>,
        stop_message: M,
        stopped: ShardStopped,
    },
    ReplyShardStopped {
        shard: ShardId,
        stopped: ShardStopped,
    },
    AlreadyInProgress {
        shard: ShardId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardRuntime<M> {
    shard_id: ShardId,
    buffer_capacity: usize,
    entities: BTreeMap<EntityId, ShardEntityState>,
    message_buffers: BTreeMap<EntityId, VecDeque<M>>,
    preparing_for_shutdown: bool,
    handoff_in_progress: bool,
}

impl<M> ShardRuntime<M> {
    pub fn new(shard_id: impl Into<ShardId>, buffer_capacity: usize) -> Self {
        Self {
            shard_id: shard_id.into(),
            buffer_capacity,
            entities: BTreeMap::new(),
            message_buffers: BTreeMap::new(),
            preparing_for_shutdown: false,
            handoff_in_progress: false,
        }
    }

    pub fn shard_id(&self) -> &ShardId {
        &self.shard_id
    }

    pub fn entity_state(&self, entity_id: &EntityId) -> Option<ShardEntityState> {
        self.entities.get(entity_id).copied()
    }

    pub fn active_entity_ids(&self) -> Vec<EntityId> {
        self.entities.keys().cloned().collect()
    }

    pub fn entity_count(&self) -> usize {
        self.entities.len()
    }

    pub fn buffered_count(&self, entity_id: &EntityId) -> usize {
        self.message_buffers.get(entity_id).map_or(0, VecDeque::len)
    }

    pub fn total_buffered_count(&self) -> usize {
        self.message_buffers.values().map(VecDeque::len).sum()
    }

    pub fn handoff_in_progress(&self) -> bool {
        self.handoff_in_progress
    }

    pub fn set_preparing_for_shutdown(&mut self, preparing: bool) {
        self.preparing_for_shutdown = preparing;
    }

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
            Some(ShardEntityState::Passivating) => match self.buffer_message(&entity_id, message) {
                Ok(()) => ShardDeliverPlan::Buffered { entity_id },
                Err(message) => ShardDeliverPlan::Dropped {
                    entity_id: Some(entity_id),
                    reason: ShardDropReason::BufferFull,
                    message,
                },
            },
            None => {
                self.entities
                    .insert(entity_id.clone(), ShardEntityState::Active);
                ShardDeliverPlan::StartEntity {
                    delivery: EntityDelivery::new(entity_id, message),
                }
            }
        }
    }

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
            Some(ShardEntityState::Passivating) => PassivatePlan::Ignored {
                entity_id,
                reason: PassivateIgnoreReason::AlreadyPassivating,
            },
            None => PassivatePlan::Ignored {
                entity_id,
                reason: PassivateIgnoreReason::UnknownEntity,
            },
        }
    }

    pub fn entity_terminated(&mut self, entity_id: impl Into<EntityId>) -> EntityTerminatedPlan<M> {
        let entity_id = entity_id.into();
        match self.entities.remove(&entity_id) {
            Some(ShardEntityState::Active) => {
                self.message_buffers.remove(&entity_id);
                EntityTerminatedPlan::Removed { entity_id }
            }
            Some(ShardEntityState::Passivating) => {
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
            None => EntityTerminatedPlan::IgnoredUnknown { entity_id },
        }
    }

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

    fn drain_buffer(&mut self, entity_id: &EntityId) -> Vec<M> {
        self.message_buffers
            .remove(entity_id)
            .map(VecDeque::into_iter)
            .map(Iterator::collect)
            .unwrap_or_default()
    }
}
