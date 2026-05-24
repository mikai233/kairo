//! Cluster sharding API surface and protocols.

mod allocation;
mod codec;
mod coordinator;
mod coordinator_actor;
mod coordinator_runtime;
mod entity_ref;
mod entity_type;
mod envelope;
mod errors;
mod handoff_transport;
mod hashing;
mod protocol;
mod region_actor;
mod region_runtime;
mod remember;
mod remember_actor;
mod remember_ddata;
mod shard_actor;
mod shard_runtime;

pub use allocation::{LeastShardAllocationStrategy, ShardAllocationStrategy, ShardAllocations};
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
pub use hashing::{DEFAULT_SHARD_COUNT, default_shard_id_for, shard_id_for, stable_hash_entity_id};
pub use protocol::{
    BeginHandOff, BeginHandOffAck, GetShardHome, HandOff, HostShard, Register, RegisterAck,
    ShardHome, ShardStarted, ShardStopped,
};
pub use region_actor::{ShardRegionActor, ShardRegionMsg, ShardRegionSnapshot};
pub use region_runtime::{
    BeginHandOffPlan, HandOffPlan, HostShardPlan, RegionDropReason, RegionRoutePlan, ShardHomePlan,
    ShardRegionRuntime, ShardStartedPlan,
};
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
pub use shard_actor::{ShardActor, ShardMsg, ShardSnapshot};
pub use shard_runtime::{
    EntityDelivery, EntityTerminatedPlan, PassivateIgnoreReason, PassivatePlan, ShardDeliverPlan,
    ShardDropReason, ShardEntityState, ShardHandOffPlan, ShardRuntime,
};

pub type EntityId = String;
pub type RegionId = String;
pub type ShardId = String;

#[cfg(test)]
mod tests;
