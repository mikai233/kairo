//! Cluster sharding API surface and protocols.

mod allocation;
mod codec;
mod coordinator;
mod entity_ref;
mod entity_type;
mod envelope;
mod errors;
mod hashing;
mod protocol;

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
pub use entity_ref::EntityRef;
pub use entity_type::EntityTypeKey;
pub use envelope::ShardingEnvelope;
pub use errors::ShardingError;
pub use hashing::{DEFAULT_SHARD_COUNT, default_shard_id_for, shard_id_for, stable_hash_entity_id};
pub use protocol::{
    BeginHandOff, BeginHandOffAck, GetShardHome, HandOff, HostShard, Register, RegisterAck,
    ShardHome, ShardStarted, ShardStopped,
};

pub type EntityId = String;
pub type RegionId = String;
pub type ShardId = String;

#[cfg(test)]
mod tests;
