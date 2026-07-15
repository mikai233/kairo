#![deny(missing_docs)]

//! Distributed-data coordinator stores for remembered shard identifiers.
//!
//! Both implementations expose one typed local actor protocol. The GSet store
//! matches Pekko's additive coordinator key directly; the ORSet store uses add
//! operations only so it can share Kairo's typed `ORSet<String>` replicator with
//! shard-level entity stores. Only CRDT state crosses nodes.

use std::collections::BTreeSet;

use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, Context, Props, SendError};
use kairo_distributed_data::{
    GSet, GetResponse, ORSet, ORSetDelta, ReadConsistency, ReplicaId, ReplicatorActorMsg,
    ReplicatorKey, UpdateResponse, WriteConsistency,
};

use crate::{RememberCoordinatorUpdateDone, RememberedShards, ShardId, ShardingError};

/// Coordinator remember store backed by an additive distributed [`GSet`].
pub struct RememberCoordinatorDDataStoreActor {
    type_name: String,
    replicator: ActorRef<ReplicatorActorMsg<GSet<String>>>,
    read_consistency: ReadConsistency,
    write_consistency: WriteConsistency,
}

/// Coordinator remember store backed by add-only use of a distributed [`ORSet`].
///
/// This variant allows coordinator and shard remember stores to share one typed
/// ORSet distributed-data extension without changing additive coordinator
/// semantics.
pub struct RememberCoordinatorORSetDDataStoreActor {
    type_name: String,
    replica_id: ReplicaId,
    replicator: ActorRef<ReplicatorActorMsg<ORSet<String>>>,
    read_consistency: ReadConsistency,
    write_consistency: WriteConsistency,
}

impl RememberCoordinatorDDataStoreActor {
    /// Creates a GSet store with local read and write consistency.
    pub fn new(
        type_name: impl Into<String>,
        replicator: ActorRef<ReplicatorActorMsg<GSet<String>>>,
    ) -> Self {
        Self::with_consistency(
            type_name,
            replicator,
            ReadConsistency::local(),
            WriteConsistency::local(),
        )
    }

    /// Creates a GSet store with explicit read and write consistency.
    pub fn with_consistency(
        type_name: impl Into<String>,
        replicator: ActorRef<ReplicatorActorMsg<GSet<String>>>,
        read_consistency: ReadConsistency,
        write_consistency: WriteConsistency,
    ) -> Self {
        Self {
            type_name: type_name.into(),
            replicator,
            read_consistency,
            write_consistency,
        }
    }

    /// Creates repeatable GSet store properties with local consistency.
    pub fn props(
        type_name: impl Into<String>,
        replicator: ActorRef<ReplicatorActorMsg<GSet<String>>>,
    ) -> Props<Self> {
        let type_name = type_name.into();
        Props::new(move || Self::new(type_name, replicator))
    }

    /// Creates repeatable GSet store properties with explicit consistency.
    pub fn props_with_consistency(
        type_name: impl Into<String>,
        replicator: ActorRef<ReplicatorActorMsg<GSet<String>>>,
        read_consistency: ReadConsistency,
        write_consistency: WriteConsistency,
    ) -> Props<Self> {
        let type_name = type_name.into();
        Props::new(move || {
            Self::with_consistency(type_name, replicator, read_consistency, write_consistency)
        })
    }

    /// Returns the cluster-wide entity type name owning the shard set.
    pub fn type_name(&self) -> &str {
        &self.type_name
    }

    /// Returns the stable distributed-data key for the coordinator shard set.
    pub fn key(&self) -> ReplicatorKey {
        remember_coordinator_shards_key(&self.type_name)
    }
}

impl RememberCoordinatorORSetDDataStoreActor {
    /// Creates an ORSet store with local read and write consistency.
    ///
    /// `replica_id` supplies the causal identity for ORSet add operations.
    pub fn new(
        type_name: impl Into<String>,
        replica_id: impl Into<ReplicaId>,
        replicator: ActorRef<ReplicatorActorMsg<ORSet<String>>>,
    ) -> Self {
        Self::with_consistency(
            type_name,
            replica_id,
            replicator,
            ReadConsistency::local(),
            WriteConsistency::local(),
        )
    }

    /// Creates an ORSet store with explicit consistency and causal replica id.
    pub fn with_consistency(
        type_name: impl Into<String>,
        replica_id: impl Into<ReplicaId>,
        replicator: ActorRef<ReplicatorActorMsg<ORSet<String>>>,
        read_consistency: ReadConsistency,
        write_consistency: WriteConsistency,
    ) -> Self {
        Self {
            type_name: type_name.into(),
            replica_id: replica_id.into(),
            replicator,
            read_consistency,
            write_consistency,
        }
    }

    /// Creates repeatable ORSet store properties with local consistency.
    pub fn props(
        type_name: impl Into<String>,
        replica_id: impl Into<ReplicaId>,
        replicator: ActorRef<ReplicatorActorMsg<ORSet<String>>>,
    ) -> Props<Self> {
        let type_name = type_name.into();
        let replica_id = replica_id.into();
        Props::new(move || Self::new(type_name, replica_id, replicator))
    }

    /// Creates repeatable ORSet store properties with explicit consistency.
    pub fn props_with_consistency(
        type_name: impl Into<String>,
        replica_id: impl Into<ReplicaId>,
        replicator: ActorRef<ReplicatorActorMsg<ORSet<String>>>,
        read_consistency: ReadConsistency,
        write_consistency: WriteConsistency,
    ) -> Props<Self> {
        let type_name = type_name.into();
        let replica_id = replica_id.into();
        Props::new(move || {
            Self::with_consistency(
                type_name,
                replica_id,
                replicator,
                read_consistency,
                write_consistency,
            )
        })
    }

    /// Returns the cluster-wide entity type name owning the shard set.
    pub fn type_name(&self) -> &str {
        &self.type_name
    }

    /// Returns the stable distributed-data key for the coordinator shard set.
    pub fn key(&self) -> ReplicatorKey {
        remember_coordinator_shards_key(&self.type_name)
    }
}

/// Local request/reply protocol shared by both coordinator ddata stores.
pub enum RememberCoordinatorDDataStoreMsg {
    /// Adds one shard identifier to the coordinator's monotonic set.
    AddShard {
        /// Shard identifier to record.
        shard: ShardId,
        /// Actor that receives completion or the concrete store failure.
        reply_to: ActorRef<Result<RememberCoordinatorUpdateDone, ShardingError>>,
    },
    /// Reads every remembered shard identifier.
    GetShards {
        /// Actor that receives the set or the concrete store failure.
        reply_to: ActorRef<Result<RememberedShards, ShardingError>>,
    },
    /// Requests a diagnostic snapshot of key and consistency configuration.
    GetState {
        /// Actor that receives the diagnostic snapshot.
        reply_to: ActorRef<RememberCoordinatorDDataStoreSnapshot>,
    },
    #[doc(hidden)]
    ReplicatorGet {
        response: GetResponse<GSet<String>>,
        reply_to: ActorRef<Result<RememberedShards, ShardingError>>,
    },
    #[doc(hidden)]
    ReplicatorUpdate {
        shard: ShardId,
        response: UpdateResponse<GSet<String>>,
        reply_to: ActorRef<Result<RememberCoordinatorUpdateDone, ShardingError>>,
    },
    #[doc(hidden)]
    ORSetReplicatorGet {
        response: GetResponse<ORSet<String>>,
        reply_to: ActorRef<Result<RememberedShards, ShardingError>>,
    },
    #[doc(hidden)]
    ORSetReplicatorUpdate {
        shard: ShardId,
        response: UpdateResponse<ORSetDelta<String>>,
        reply_to: ActorRef<Result<RememberCoordinatorUpdateDone, ShardingError>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Diagnostic configuration snapshot for a coordinator ddata store.
pub struct RememberCoordinatorDDataStoreSnapshot {
    /// Cluster-wide entity type name.
    pub type_name: String,
    /// Stable distributed-data key containing shard identifiers.
    pub key: String,
    /// Consistency used for shard-set reads.
    pub read_consistency: ReadConsistency,
    /// Consistency used for shard-id additions.
    pub write_consistency: WriteConsistency,
}

impl Actor for RememberCoordinatorDDataStoreActor {
    type Msg = RememberCoordinatorDDataStoreMsg;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            RememberCoordinatorDDataStoreMsg::AddShard { shard, reply_to } => {
                self.add_shard(ctx, shard, reply_to)?;
            }
            RememberCoordinatorDDataStoreMsg::GetShards { reply_to } => {
                self.get_shards(ctx, reply_to)?;
            }
            RememberCoordinatorDDataStoreMsg::GetState { reply_to } => {
                reply_to
                    .tell(self.snapshot())
                    .map_err(send_error_to_actor_error)?;
            }
            RememberCoordinatorDDataStoreMsg::ReplicatorGet { response, reply_to } => {
                reply_to
                    .tell(map_get_response(response))
                    .map_err(send_error_to_actor_error)?;
            }
            RememberCoordinatorDDataStoreMsg::ReplicatorUpdate {
                shard,
                response,
                reply_to,
            } => {
                reply_to
                    .tell(map_update_response(shard, response))
                    .map_err(send_error_to_actor_error)?;
            }
            RememberCoordinatorDDataStoreMsg::ORSetReplicatorGet { .. }
            | RememberCoordinatorDDataStoreMsg::ORSetReplicatorUpdate { .. } => {
                return Err(ActorError::Message(
                    "ORSet coordinator response sent to GSet store".to_string(),
                ));
            }
        }
        Ok(())
    }
}

impl Actor for RememberCoordinatorORSetDDataStoreActor {
    type Msg = RememberCoordinatorDDataStoreMsg;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            RememberCoordinatorDDataStoreMsg::AddShard { shard, reply_to } => {
                self.add_shard(ctx, shard, reply_to)?;
            }
            RememberCoordinatorDDataStoreMsg::GetShards { reply_to } => {
                self.get_shards(ctx, reply_to)?;
            }
            RememberCoordinatorDDataStoreMsg::GetState { reply_to } => {
                reply_to
                    .tell(self.snapshot())
                    .map_err(send_error_to_actor_error)?;
            }
            RememberCoordinatorDDataStoreMsg::ORSetReplicatorGet { response, reply_to } => {
                reply_to
                    .tell(map_orset_get_response(response))
                    .map_err(send_error_to_actor_error)?;
            }
            RememberCoordinatorDDataStoreMsg::ORSetReplicatorUpdate {
                shard,
                response,
                reply_to,
            } => {
                reply_to
                    .tell(map_orset_update_response(shard, response))
                    .map_err(send_error_to_actor_error)?;
            }
            RememberCoordinatorDDataStoreMsg::ReplicatorGet { .. }
            | RememberCoordinatorDDataStoreMsg::ReplicatorUpdate { .. } => {
                return Err(ActorError::Message(
                    "GSet coordinator response sent to ORSet store".to_string(),
                ));
            }
        }
        Ok(())
    }
}

impl RememberCoordinatorORSetDDataStoreActor {
    fn get_shards(
        &self,
        ctx: &Context<RememberCoordinatorDDataStoreMsg>,
        reply_to: ActorRef<Result<RememberedShards, ShardingError>>,
    ) -> ActorResult {
        let adapter = ctx.message_adapter({
            let reply_to = reply_to.clone();
            move |response| RememberCoordinatorDDataStoreMsg::ORSetReplicatorGet {
                response,
                reply_to: reply_to.clone(),
            }
        })?;
        self.replicator
            .tell(ReplicatorActorMsg::Get {
                key: self.key(),
                consistency: self.read_consistency.clone(),
                reply_to: adapter,
            })
            .map_err(send_error_to_actor_error)?;
        Ok(())
    }

    fn add_shard(
        &self,
        ctx: &Context<RememberCoordinatorDDataStoreMsg>,
        shard: ShardId,
        reply_to: ActorRef<Result<RememberCoordinatorUpdateDone, ShardingError>>,
    ) -> ActorResult {
        let adapter = ctx.message_adapter({
            let shard = shard.clone();
            let reply_to = reply_to.clone();
            move |response| RememberCoordinatorDDataStoreMsg::ORSetReplicatorUpdate {
                shard: shard.clone(),
                response,
                reply_to: reply_to.clone(),
            }
        })?;
        let replica_id = self.replica_id.clone();
        self.replicator
            .tell(ReplicatorActorMsg::Update {
                key: self.key(),
                initial: ORSet::new(),
                consistency: self.write_consistency.clone(),
                modify: Box::new(move |set| Ok(set.add(replica_id, shard))),
                reply_to: adapter,
            })
            .map_err(send_error_to_actor_error)?;
        Ok(())
    }

    fn snapshot(&self) -> RememberCoordinatorDDataStoreSnapshot {
        RememberCoordinatorDDataStoreSnapshot {
            type_name: self.type_name.clone(),
            key: self.key().as_str().to_string(),
            read_consistency: self.read_consistency.clone(),
            write_consistency: self.write_consistency.clone(),
        }
    }
}

impl RememberCoordinatorDDataStoreActor {
    fn get_shards(
        &self,
        ctx: &Context<RememberCoordinatorDDataStoreMsg>,
        reply_to: ActorRef<Result<RememberedShards, ShardingError>>,
    ) -> ActorResult {
        let adapter = ctx.message_adapter({
            let reply_to = reply_to.clone();
            move |response| RememberCoordinatorDDataStoreMsg::ReplicatorGet {
                response,
                reply_to: reply_to.clone(),
            }
        })?;
        self.replicator
            .tell(ReplicatorActorMsg::Get {
                key: self.key(),
                consistency: self.read_consistency.clone(),
                reply_to: adapter,
            })
            .map_err(send_error_to_actor_error)?;
        Ok(())
    }

    fn add_shard(
        &self,
        ctx: &Context<RememberCoordinatorDDataStoreMsg>,
        shard: ShardId,
        reply_to: ActorRef<Result<RememberCoordinatorUpdateDone, ShardingError>>,
    ) -> ActorResult {
        let adapter = ctx.message_adapter({
            let shard = shard.clone();
            let reply_to = reply_to.clone();
            move |response| RememberCoordinatorDDataStoreMsg::ReplicatorUpdate {
                shard: shard.clone(),
                response,
                reply_to: reply_to.clone(),
            }
        })?;
        self.replicator
            .tell(ReplicatorActorMsg::Update {
                key: self.key(),
                initial: GSet::new(),
                consistency: self.write_consistency.clone(),
                modify: Box::new(move |set| Ok(set.add(shard))),
                reply_to: adapter,
            })
            .map_err(send_error_to_actor_error)?;
        Ok(())
    }

    fn snapshot(&self) -> RememberCoordinatorDDataStoreSnapshot {
        RememberCoordinatorDDataStoreSnapshot {
            type_name: self.type_name.clone(),
            key: self.key().as_str().to_string(),
            read_consistency: self.read_consistency.clone(),
            write_consistency: self.write_consistency.clone(),
        }
    }
}

/// Derives the Pekko-compatible coordinator shard-set key for `type_name`.
pub fn remember_coordinator_shards_key(type_name: &str) -> ReplicatorKey {
    ReplicatorKey::new(format!("shard-{type_name}-all"))
}

fn map_get_response(
    response: GetResponse<GSet<String>>,
) -> Result<RememberedShards, ShardingError> {
    match response {
        GetResponse::Success { data, .. } => Ok(RememberedShards {
            shards: data.elements().clone(),
        }),
        GetResponse::NotFound { .. } => Ok(RememberedShards {
            shards: BTreeSet::new(),
        }),
        GetResponse::Failure { key, reason } => Err(ShardingError::RememberStoreReadFailed {
            key: key.as_str().to_string(),
            reason,
        }),
    }
}

fn map_update_response(
    shard: ShardId,
    response: UpdateResponse<GSet<String>>,
) -> Result<RememberCoordinatorUpdateDone, ShardingError> {
    match response {
        UpdateResponse::Success(_) => Ok(RememberCoordinatorUpdateDone { shard }),
        UpdateResponse::Timeout { key } => Err(ShardingError::RememberStoreUpdateFailed {
            key: key.as_str().to_string(),
            reason: format!("timed out while adding shard {shard}"),
        }),
        UpdateResponse::ModifyFailure { key, reason } | UpdateResponse::Failure { key, reason } => {
            Err(ShardingError::RememberStoreUpdateFailed {
                key: key.as_str().to_string(),
                reason,
            })
        }
    }
}

fn map_orset_get_response(
    response: GetResponse<ORSet<String>>,
) -> Result<RememberedShards, ShardingError> {
    match response {
        GetResponse::Success { data, .. } => Ok(RememberedShards {
            shards: data.elements(),
        }),
        GetResponse::NotFound { .. } => Ok(RememberedShards {
            shards: BTreeSet::new(),
        }),
        GetResponse::Failure { key, reason } => Err(ShardingError::RememberStoreReadFailed {
            key: key.as_str().to_string(),
            reason,
        }),
    }
}

fn map_orset_update_response(
    shard: ShardId,
    response: UpdateResponse<ORSetDelta<String>>,
) -> Result<RememberCoordinatorUpdateDone, ShardingError> {
    match response {
        UpdateResponse::Success(_) => Ok(RememberCoordinatorUpdateDone { shard }),
        UpdateResponse::Timeout { key } => Err(ShardingError::RememberStoreUpdateFailed {
            key: key.as_str().to_string(),
            reason: format!("timed out while adding shard {shard}"),
        }),
        UpdateResponse::ModifyFailure { key, reason } | UpdateResponse::Failure { key, reason } => {
            Err(ShardingError::RememberStoreUpdateFailed {
                key: key.as_str().to_string(),
                reason,
            })
        }
    }
}

fn send_error_to_actor_error<M>(error: SendError<M>) -> ActorError {
    ActorError::Message(error.to_string())
}
