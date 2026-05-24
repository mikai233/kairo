//! Cluster sharding API surface and protocols.

mod allocation;
mod bootstrap;
mod codec;
mod coordinator;
mod coordinator_actor;
mod coordinator_handoff;
mod coordinator_runtime;
mod coordinator_store;
mod entity_ref;
mod entity_type;
mod envelope;
mod errors;
mod handoff_transport;
mod handoff_worker;
mod hashing;
mod protocol;
mod region_actor;
mod region_home_requests;
mod region_protocol;
mod region_registration;
mod region_runtime;
mod region_shards;
mod region_transport;
mod remember;
mod remember_actor;
mod remember_ddata;
mod remember_shard_ddata;
mod shard_actor;
mod shard_loading;
mod shard_remember;
mod shard_runtime;
mod shard_store;

pub use allocation::{LeastShardAllocationStrategy, ShardAllocationStrategy, ShardAllocations};
pub use bootstrap::ShardCoordinatorBootstrap;
pub use codec::{
    BEGIN_HANDOFF_ACK_SERIALIZER_ID, BEGIN_HANDOFF_SERIALIZER_ID, BeginHandOffAckCodec,
    BeginHandOffCodec, GET_SHARD_HOME_SERIALIZER_ID, GetShardHomeCodec, HANDOFF_SERIALIZER_ID,
    HOST_SHARD_SERIALIZER_ID, HandOffCodec, HostShardCodec, REGISTER_ACK_SERIALIZER_ID,
    REGISTER_SERIALIZER_ID, RegisterAckCodec, RegisterCodec, SHARD_HOME_SERIALIZER_ID,
    SHARD_STARTED_SERIALIZER_ID, SHARD_STOPPED_SERIALIZER_ID, ShardHomeCodec, ShardStartedCodec,
    ShardStoppedCodec, register_sharding_protocol_codecs,
};
pub use coordinator::{CoordinatorEvent, CoordinatorState};
pub use coordinator_actor::{CoordinatorStateSnapshot, ShardCoordinatorActor, ShardCoordinatorMsg};
pub use coordinator_handoff::CoordinatorHandoff;
pub use coordinator_runtime::{
    CoordinatorRuntime, GetShardHomeIgnoreReason, GetShardHomePlan, RebalanceCompletionPlan,
    RebalancePlan, RebalanceSkipReason, ShardRebalancePlan,
};
pub use entity_ref::EntityRef;
pub use entity_type::EntityTypeKey;
pub use envelope::ShardingEnvelope;
pub use errors::ShardingError;
pub use handoff_transport::{
    HandoffDeliveryFailure, HandoffDeliveryReport, HandoffDeliveryTarget, HandoffRegionTarget,
    HandoffTransport,
};
pub use handoff_worker::{
    HandoffWorkerActor, HandoffWorkerDone, HandoffWorkerMsg, HandoffWorkerPhase,
    HandoffWorkerSnapshot,
};
pub use hashing::{DEFAULT_SHARD_COUNT, default_shard_id_for, shard_id_for, stable_hash_entity_id};
pub use protocol::{
    BeginHandOff, BeginHandOffAck, GetShardHome, HandOff, HostShard, Register, RegisterAck,
    ShardHome, ShardStarted, ShardStopped,
};
pub use region_actor::ShardRegionActor;
pub use region_protocol::{
    RegionBufferedReplayPlan, RegionLocalHandOffCompletionFailure,
    RegionLocalHandOffCompletionPlan, RegionLocalHandOffPlan, RegionLocalRoutePlan, ShardRegionMsg,
    ShardRegionSnapshot,
};
pub use region_registration::{RegionRegistrationConfig, RegionRegistrationStatus};
pub use region_runtime::{
    BeginHandOffPlan, HandOffPlan, HostShardPlan, RegionDropReason, RegionRoutePlan, ShardHomePlan,
    ShardRegionRuntime, ShardStartedPlan,
};
pub use region_transport::{RegionRouteDelivery, RegionRouteTarget, RegionRouteTransport};
pub use remember::{
    REMEMBER_ENTITY_SHARD_KEY_COUNT, RememberCoordinatorStoreState, RememberCoordinatorUpdateDone,
    RememberShardStoreState, RememberShardUpdate, RememberShardUpdateDone, RememberedShards,
    remember_entity_key_index, remember_entity_key_index_for, remember_entity_shard_key,
};
pub use remember_actor::{
    RememberCoordinatorStoreActor, RememberCoordinatorStoreMsg, RememberCoordinatorStoreSnapshot,
    RememberShardStoreActor, RememberShardStoreMsg, RememberShardStoreSnapshot, RememberedEntities,
};
pub use remember_ddata::{
    RememberCoordinatorDDataStoreActor, RememberCoordinatorDDataStoreMsg,
    RememberCoordinatorDDataStoreSnapshot, remember_coordinator_shards_key,
};
pub use remember_shard_ddata::{
    RememberShardDDataStoreActor, RememberShardDDataStoreMsg, RememberShardDDataStoreSnapshot,
    remember_entity_shard_replicator_key,
};
pub use shard_actor::{ShardActor, ShardMsg, ShardSnapshot};
pub use shard_runtime::{
    EntityDelivery, EntityTerminatedPlan, PassivateIgnoreReason, PassivatePlan,
    RememberUpdateDonePlan, RememberedEntitiesPlan, ShardDeliverPlan, ShardDropReason,
    ShardEntityState, ShardHandOffPlan, ShardRuntime,
};

pub type EntityId = String;
pub type RegionId = String;
pub type ShardId = String;

#[cfg(test)]
mod tests;
