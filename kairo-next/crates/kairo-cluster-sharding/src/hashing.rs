use crate::{ShardId, ShardingError};

pub const DEFAULT_SHARD_COUNT: u64 = 100;

const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;

pub fn stable_hash_entity_id(entity_id: &str) -> u64 {
    entity_id
        .as_bytes()
        .iter()
        .fold(FNV_OFFSET_BASIS, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(FNV_PRIME)
        })
}

pub fn shard_id_for(
    entity_id: impl AsRef<str>,
    shard_count: u64,
) -> Result<ShardId, ShardingError> {
    if shard_count == 0 {
        return Err(ShardingError::InvalidShardCount);
    }
    Ok((stable_hash_entity_id(entity_id.as_ref()) % shard_count).to_string())
}

pub fn default_shard_id_for(entity_id: impl AsRef<str>) -> ShardId {
    shard_id_for(entity_id, DEFAULT_SHARD_COUNT)
        .expect("DEFAULT_SHARD_COUNT must be greater than zero")
}
