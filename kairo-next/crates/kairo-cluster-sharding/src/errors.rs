use std::fmt::{self, Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShardingError {
    InvalidShardCount,
    InvalidRebalanceLimit,
    NoShardRegions,
    UnknownRegion(String),
    RegionAlreadyRegistered(String),
    UnknownRegionProxy(String),
    RegionProxyAlreadyRegistered(String),
    UnknownShard(String),
    ShardAlreadyAllocated(String),
    InvalidRememberEntityKeyCount,
    InvalidRememberEntityKeyIndex {
        index: usize,
        key_count: usize,
    },
    RememberStoreReadFailed {
        key: String,
        reason: String,
    },
    RememberStoreUpdateFailed {
        key: String,
        reason: String,
    },
    InconsistentShardHome {
        shard: String,
        current_region: String,
        new_region: String,
    },
}

impl Display for ShardingError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidShardCount => f.write_str("shard count must be greater than zero"),
            Self::InvalidRebalanceLimit => {
                f.write_str("rebalance limits must be finite and greater than zero")
            }
            Self::NoShardRegions => f.write_str("no shard regions are available"),
            Self::UnknownRegion(region) => write!(f, "unknown shard region {region}"),
            Self::RegionAlreadyRegistered(region) => {
                write!(f, "shard region {region} is already registered")
            }
            Self::UnknownRegionProxy(proxy) => write!(f, "unknown shard region proxy {proxy}"),
            Self::RegionProxyAlreadyRegistered(proxy) => {
                write!(f, "shard region proxy {proxy} is already registered")
            }
            Self::UnknownShard(shard) => write!(f, "unknown shard {shard}"),
            Self::ShardAlreadyAllocated(shard) => {
                write!(f, "shard {shard} is already allocated")
            }
            Self::InvalidRememberEntityKeyCount => {
                f.write_str("remember entity key count must be greater than zero")
            }
            Self::InvalidRememberEntityKeyIndex { index, key_count } => write!(
                f,
                "remember entity key index {index} is outside 0..{key_count}"
            ),
            Self::RememberStoreReadFailed { key, reason } => {
                write!(
                    f,
                    "remember entity store read failed for key {key}: {reason}"
                )
            }
            Self::RememberStoreUpdateFailed { key, reason } => {
                write!(
                    f,
                    "remember entity store update failed for key {key}: {reason}"
                )
            }
            Self::InconsistentShardHome {
                shard,
                current_region,
                new_region,
            } => write!(
                f,
                "shard {shard} home changed unexpectedly from {current_region} to {new_region}"
            ),
        }
    }
}

impl std::error::Error for ShardingError {}
