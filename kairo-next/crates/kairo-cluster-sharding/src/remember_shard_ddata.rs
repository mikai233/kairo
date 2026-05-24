use std::collections::{BTreeMap, BTreeSet};

use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, Context, Props, SendError};
use kairo_distributed_data::{
    GetResponse, ORSet, ORSetDelta, ReadConsistency, ReplicaId, ReplicatorActorMsg, ReplicatorKey,
    UpdateResponse, WriteConsistency,
};

use crate::{
    EntityId, REMEMBER_ENTITY_SHARD_KEY_COUNT, RememberShardUpdate, RememberShardUpdateDone,
    RememberedEntities, ShardId, ShardingError, remember_entity_key_index,
    remember_entity_shard_key,
};

pub struct RememberShardDDataStoreActor {
    type_name: String,
    shard_id: ShardId,
    replica_id: ReplicaId,
    replicator: ActorRef<ReplicatorActorMsg<ORSet<String>>>,
    read_consistency: ReadConsistency,
    write_consistency: WriteConsistency,
    load_pending: BTreeSet<usize>,
    entities_by_key: BTreeMap<usize, BTreeSet<EntityId>>,
    waiting_gets: Vec<ActorRef<Result<RememberedEntities, ShardingError>>>,
    pending_updates: BTreeMap<u64, PendingShardUpdate>,
    next_update_id: u64,
}

impl RememberShardDDataStoreActor {
    pub fn new(
        type_name: impl Into<String>,
        shard_id: impl Into<ShardId>,
        replica_id: impl Into<ReplicaId>,
        replicator: ActorRef<ReplicatorActorMsg<ORSet<String>>>,
    ) -> Self {
        Self::with_consistency(
            type_name,
            shard_id,
            replica_id,
            replicator,
            ReadConsistency::local(),
            WriteConsistency::local(),
        )
    }

    pub fn with_consistency(
        type_name: impl Into<String>,
        shard_id: impl Into<ShardId>,
        replica_id: impl Into<ReplicaId>,
        replicator: ActorRef<ReplicatorActorMsg<ORSet<String>>>,
        read_consistency: ReadConsistency,
        write_consistency: WriteConsistency,
    ) -> Self {
        Self {
            type_name: type_name.into(),
            shard_id: shard_id.into(),
            replica_id: replica_id.into(),
            replicator,
            read_consistency,
            write_consistency,
            load_pending: (0..REMEMBER_ENTITY_SHARD_KEY_COUNT).collect(),
            entities_by_key: BTreeMap::new(),
            waiting_gets: Vec::new(),
            pending_updates: BTreeMap::new(),
            next_update_id: 0,
        }
    }

    pub fn props(
        type_name: impl Into<String>,
        shard_id: impl Into<ShardId>,
        replica_id: impl Into<ReplicaId>,
        replicator: ActorRef<ReplicatorActorMsg<ORSet<String>>>,
    ) -> Props<Self> {
        let type_name = type_name.into();
        let shard_id = shard_id.into();
        let replica_id = replica_id.into();
        Props::new(move || Self::new(type_name, shard_id, replica_id, replicator))
    }

    pub fn props_with_consistency(
        type_name: impl Into<String>,
        shard_id: impl Into<ShardId>,
        replica_id: impl Into<ReplicaId>,
        replicator: ActorRef<ReplicatorActorMsg<ORSet<String>>>,
        read_consistency: ReadConsistency,
        write_consistency: WriteConsistency,
    ) -> Props<Self> {
        let type_name = type_name.into();
        let shard_id = shard_id.into();
        let replica_id = replica_id.into();
        Props::new(move || {
            Self::with_consistency(
                type_name,
                shard_id,
                replica_id,
                replicator,
                read_consistency,
                write_consistency,
            )
        })
    }

    pub fn type_name(&self) -> &str {
        &self.type_name
    }

    pub fn shard_id(&self) -> &ShardId {
        &self.shard_id
    }
}

pub enum RememberShardDDataStoreMsg {
    GetEntities {
        reply_to: ActorRef<Result<RememberedEntities, ShardingError>>,
    },
    Update {
        update: RememberShardUpdate,
        reply_to: ActorRef<Result<RememberShardUpdateDone, ShardingError>>,
    },
    GetState {
        reply_to: ActorRef<RememberShardDDataStoreSnapshot>,
    },
    #[doc(hidden)]
    ReplicatorGet {
        index: usize,
        response: GetResponse<ORSet<String>>,
    },
    #[doc(hidden)]
    ReplicatorUpdate {
        update_id: u64,
        index: usize,
        response: UpdateResponse<ORSetDelta<String>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RememberShardDDataStoreSnapshot {
    pub type_name: String,
    pub shard_id: ShardId,
    pub loaded: bool,
    pub pending_load_keys: BTreeSet<usize>,
    pub pending_updates: usize,
    pub entities_by_key: BTreeMap<usize, BTreeSet<EntityId>>,
}

#[derive(Clone)]
struct PendingShardUpdate {
    update: RememberShardUpdate,
    remaining_indexes: BTreeSet<usize>,
    reply_to: ActorRef<Result<RememberShardUpdateDone, ShardingError>>,
}

impl Actor for RememberShardDDataStoreActor {
    type Msg = RememberShardDDataStoreMsg;

    fn started(&mut self, ctx: &mut Context<Self::Msg>) -> ActorResult {
        self.load_all_entities(ctx)
    }

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            RememberShardDDataStoreMsg::GetEntities { reply_to } => {
                self.get_entities(reply_to)?;
            }
            RememberShardDDataStoreMsg::Update { update, reply_to } => {
                self.update(ctx, update, reply_to)?;
            }
            RememberShardDDataStoreMsg::GetState { reply_to } => {
                reply_to
                    .tell(self.snapshot())
                    .map_err(send_error_to_actor_error)?;
            }
            RememberShardDDataStoreMsg::ReplicatorGet { index, response } => {
                self.apply_get_response(index, response)?;
            }
            RememberShardDDataStoreMsg::ReplicatorUpdate {
                update_id,
                index,
                response,
            } => {
                self.apply_update_response(update_id, index, response)?;
            }
        }
        Ok(())
    }
}

impl RememberShardDDataStoreActor {
    fn load_all_entities(&self, ctx: &Context<RememberShardDDataStoreMsg>) -> ActorResult {
        for index in 0..REMEMBER_ENTITY_SHARD_KEY_COUNT {
            let adapter = ctx.message_adapter(move |response| {
                RememberShardDDataStoreMsg::ReplicatorGet { index, response }
            })?;
            self.replicator
                .tell(ReplicatorActorMsg::Get {
                    key: self.key(index).map_err(sharding_error_to_actor_error)?,
                    consistency: self.read_consistency.clone(),
                    reply_to: adapter,
                })
                .map_err(send_error_to_actor_error)?;
        }
        Ok(())
    }

    fn get_entities(
        &mut self,
        reply_to: ActorRef<Result<RememberedEntities, ShardingError>>,
    ) -> ActorResult {
        if self.loaded() {
            reply_to
                .tell(Ok(RememberedEntities {
                    entities: self.remembered_entities(),
                }))
                .map_err(send_error_to_actor_error)?;
        } else {
            self.waiting_gets.push(reply_to);
        }
        Ok(())
    }

    fn update(
        &mut self,
        ctx: &Context<RememberShardDDataStoreMsg>,
        update: RememberShardUpdate,
        reply_to: ActorRef<Result<RememberShardUpdateDone, ShardingError>>,
    ) -> ActorResult {
        if !self.loaded() {
            reply_to
                .tell(Err(ShardingError::RememberStoreUpdateFailed {
                    key: self.shard_key_prefix(),
                    reason: "initial entity load has not completed".to_string(),
                }))
                .map_err(send_error_to_actor_error)?;
            return Ok(());
        }

        if !self.pending_updates.is_empty() {
            reply_to
                .tell(Err(ShardingError::RememberStoreUpdateFailed {
                    key: self.shard_key_prefix(),
                    reason: "previous remember-entity update is still in progress".to_string(),
                }))
                .map_err(send_error_to_actor_error)?;
            return Ok(());
        }

        let grouped = group_update_by_key(&update);
        if grouped.is_empty() {
            reply_to
                .tell(Ok(RememberShardUpdateDone {
                    started: update.started().clone(),
                    stopped: update.stopped().clone(),
                }))
                .map_err(send_error_to_actor_error)?;
            return Ok(());
        }

        let update_id = self.next_update_id;
        self.next_update_id = self.next_update_id.wrapping_add(1);
        let remaining_indexes = grouped.keys().copied().collect();
        self.pending_updates.insert(
            update_id,
            PendingShardUpdate {
                update: update.clone(),
                remaining_indexes,
                reply_to,
            },
        );

        for (index, partial_update) in grouped {
            self.send_update(ctx, update_id, index, partial_update)?;
        }
        Ok(())
    }

    fn send_update(
        &self,
        ctx: &Context<RememberShardDDataStoreMsg>,
        update_id: u64,
        index: usize,
        update: RememberShardUpdate,
    ) -> ActorResult {
        let adapter =
            ctx.message_adapter(
                move |response| RememberShardDDataStoreMsg::ReplicatorUpdate {
                    update_id,
                    index,
                    response,
                },
            )?;
        let replica_id = self.replica_id.clone();
        self.replicator
            .tell(ReplicatorActorMsg::Update {
                key: self.key(index).map_err(sharding_error_to_actor_error)?,
                initial: ORSet::new(),
                consistency: self.write_consistency.clone(),
                modify: Box::new(move |set| {
                    let mut next = set;
                    for entity in update.stopped() {
                        next = next.remove(replica_id.clone(), entity);
                    }
                    for entity in update.started() {
                        next = next.add(replica_id.clone(), entity.clone());
                    }
                    Ok(next)
                }),
                reply_to: adapter,
            })
            .map_err(send_error_to_actor_error)?;
        Ok(())
    }

    fn apply_get_response(
        &mut self,
        index: usize,
        response: GetResponse<ORSet<String>>,
    ) -> ActorResult {
        match response {
            GetResponse::Success { data, .. } => {
                self.entities_by_key.insert(index, data.elements());
                self.load_pending.remove(&index);
                self.drain_waiting_gets_if_loaded()?;
            }
            GetResponse::NotFound { .. } => {
                self.entities_by_key.insert(index, BTreeSet::new());
                self.load_pending.remove(&index);
                self.drain_waiting_gets_if_loaded()?;
            }
            GetResponse::Failure { key, reason } => {
                self.load_pending.remove(&index);
                let error = ShardingError::RememberStoreReadFailed {
                    key: key.as_str().to_string(),
                    reason,
                };
                self.drain_waiting_gets(Err(error))?;
            }
        }
        Ok(())
    }

    fn apply_update_response(
        &mut self,
        update_id: u64,
        index: usize,
        response: UpdateResponse<ORSetDelta<String>>,
    ) -> ActorResult {
        let Some(pending) = self.pending_updates.get_mut(&update_id) else {
            return Ok(());
        };

        match response {
            UpdateResponse::Success(_) => {
                pending.remaining_indexes.remove(&index);
                if pending.remaining_indexes.is_empty() {
                    let pending = self
                        .pending_updates
                        .remove(&update_id)
                        .expect("pending update exists");
                    self.apply_loaded_update(&pending.update);
                    pending
                        .reply_to
                        .tell(Ok(RememberShardUpdateDone {
                            started: pending.update.started().clone(),
                            stopped: pending.update.stopped().clone(),
                        }))
                        .map_err(send_error_to_actor_error)?;
                }
            }
            UpdateResponse::Timeout { key } => {
                let pending = self
                    .pending_updates
                    .remove(&update_id)
                    .expect("pending update exists");
                pending
                    .reply_to
                    .tell(Err(ShardingError::RememberStoreUpdateFailed {
                        key: key.as_str().to_string(),
                        reason: "timed out while updating remembered entities".to_string(),
                    }))
                    .map_err(send_error_to_actor_error)?;
            }
            UpdateResponse::ModifyFailure { key, reason } => {
                let pending = self
                    .pending_updates
                    .remove(&update_id)
                    .expect("pending update exists");
                pending
                    .reply_to
                    .tell(Err(ShardingError::RememberStoreUpdateFailed {
                        key: key.as_str().to_string(),
                        reason,
                    }))
                    .map_err(send_error_to_actor_error)?;
            }
        }
        Ok(())
    }

    fn drain_waiting_gets_if_loaded(&mut self) -> ActorResult {
        if self.loaded() {
            self.drain_waiting_gets(Ok(RememberedEntities {
                entities: self.remembered_entities(),
            }))?;
        }
        Ok(())
    }

    fn drain_waiting_gets(
        &mut self,
        response: Result<RememberedEntities, ShardingError>,
    ) -> ActorResult {
        let waiting = std::mem::take(&mut self.waiting_gets);
        for reply_to in waiting {
            reply_to
                .tell(response.clone())
                .map_err(send_error_to_actor_error)?;
        }
        Ok(())
    }

    fn apply_loaded_update(&mut self, update: &RememberShardUpdate) {
        for entity in update.stopped() {
            let index = remember_entity_key_index(entity);
            self.entities_by_key
                .entry(index)
                .or_default()
                .remove(entity);
        }
        for entity in update.started() {
            let index = remember_entity_key_index(entity);
            self.entities_by_key
                .entry(index)
                .or_default()
                .insert(entity.clone());
        }
    }

    fn loaded(&self) -> bool {
        self.load_pending.is_empty()
    }

    fn remembered_entities(&self) -> BTreeSet<EntityId> {
        self.entities_by_key
            .values()
            .flat_map(|entities| entities.iter().cloned())
            .collect()
    }

    fn snapshot(&self) -> RememberShardDDataStoreSnapshot {
        RememberShardDDataStoreSnapshot {
            type_name: self.type_name.clone(),
            shard_id: self.shard_id.clone(),
            loaded: self.loaded(),
            pending_load_keys: self.load_pending.clone(),
            pending_updates: self.pending_updates.len(),
            entities_by_key: (0..REMEMBER_ENTITY_SHARD_KEY_COUNT)
                .map(|index| {
                    (
                        index,
                        self.entities_by_key
                            .get(&index)
                            .cloned()
                            .unwrap_or_default(),
                    )
                })
                .collect(),
        }
    }

    fn key(&self, index: usize) -> Result<ReplicatorKey, ShardingError> {
        remember_entity_shard_replicator_key(&self.type_name, &self.shard_id, index)
    }

    fn shard_key_prefix(&self) -> String {
        format!("shard-{}-{}", self.type_name, self.shard_id)
    }
}

pub fn remember_entity_shard_replicator_key(
    type_name: &str,
    shard_id: &str,
    index: usize,
) -> Result<ReplicatorKey, ShardingError> {
    remember_entity_shard_key(type_name, shard_id, index).map(ReplicatorKey::new)
}

fn group_update_by_key(update: &RememberShardUpdate) -> BTreeMap<usize, RememberShardUpdate> {
    let mut started: BTreeMap<usize, BTreeSet<EntityId>> = BTreeMap::new();
    let mut stopped: BTreeMap<usize, BTreeSet<EntityId>> = BTreeMap::new();
    for entity in update.started() {
        started
            .entry(remember_entity_key_index(entity))
            .or_default()
            .insert(entity.clone());
    }
    for entity in update.stopped() {
        stopped
            .entry(remember_entity_key_index(entity))
            .or_default()
            .insert(entity.clone());
    }

    started
        .keys()
        .chain(stopped.keys())
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|index| {
            (
                index,
                RememberShardUpdate::new(
                    started.remove(&index).unwrap_or_default(),
                    stopped.remove(&index).unwrap_or_default(),
                ),
            )
        })
        .collect()
}

fn send_error_to_actor_error<M>(error: SendError<M>) -> ActorError {
    ActorError::Message(error.to_string())
}

fn sharding_error_to_actor_error(error: ShardingError) -> ActorError {
    ActorError::Message(error.to_string())
}
