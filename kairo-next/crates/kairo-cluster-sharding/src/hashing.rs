#![deny(missing_docs)]

use crate::{ShardId, ShardingError};

/// Default number of shards used by the typed envelope and entity-ref path.
pub const DEFAULT_SHARD_COUNT: u64 = 100;

const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;

/// Computes Kairo's fixed 64-bit FNV-1a hash over the entity id's UTF-8 bytes.
///
/// This function is a cross-node routing contract. It deliberately avoids
/// Rust's randomized `DefaultHasher`, compiler details, and platform-dependent
/// state so every node derives the same shard.
pub fn stable_hash_entity_id(entity_id: &str) -> u64 {
    entity_id
        .as_bytes()
        .iter()
        .fold(FNV_OFFSET_BASIS, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(FNV_PRIME)
        })
}

/// Derives a decimal shard identifier as `stable_hash % shard_count`.
///
/// Returns [`ShardingError::InvalidShardCount`] when `shard_count` is zero.
pub fn shard_id_for(
    entity_id: impl AsRef<str>,
    shard_count: u64,
) -> Result<ShardId, ShardingError> {
    if shard_count == 0 {
        return Err(ShardingError::InvalidShardCount);
    }
    Ok((stable_hash_entity_id(entity_id.as_ref()) % shard_count).to_string())
}

/// Derives a shard identifier with [`DEFAULT_SHARD_COUNT`].
pub fn default_shard_id_for(entity_id: impl AsRef<str>) -> ShardId {
    shard_id_for(entity_id, DEFAULT_SHARD_COUNT)
        .expect("DEFAULT_SHARD_COUNT must be greater than zero")
}
