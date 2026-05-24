//! Cluster sharding API surface and protocols.

mod entity_ref;
mod entity_type;
mod envelope;
mod errors;
mod hashing;
mod protocol;

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
pub type ShardId = String;

#[cfg(test)]
mod tests;
