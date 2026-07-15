#![deny(missing_docs)]

//! Shared failures for sharding state transitions, allocation, routing, and
//! remember-entity storage.
//!
//! These errors describe local typed API and actor-operation failures. They are
//! not a remote serialization contract; wire adapters translate their own
//! failures at the remoting boundary.

use std::fmt::{self, Display, Formatter};

/// Failure produced while validating or applying a cluster-sharding operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShardingError {
    /// A stable shard calculation was requested with zero shards.
    InvalidShardCount,
    /// A rebalance limit was zero, negative, infinite, or not a number.
    InvalidRebalanceLimit,
    /// Allocation was requested while no shard region was available.
    NoShardRegions,
    /// An operation targeted a shard region absent from coordinator state.
    UnknownRegion(String),
    /// Coordinator state already contains the named shard region.
    RegionAlreadyRegistered(String),
    /// An operation targeted a region proxy absent from coordinator state.
    UnknownRegionProxy(String),
    /// Coordinator state already contains the named region proxy.
    RegionProxyAlreadyRegistered(String),
    /// An operation targeted a shard absent from coordinator state.
    UnknownShard(String),
    /// Coordinator state already has a home for the named shard.
    ShardAlreadyAllocated(String),
    /// Remember-entity partitioning was requested with zero storage keys.
    InvalidRememberEntityKeyCount,
    /// A remember-entity storage-key index is outside the configured range.
    InvalidRememberEntityKeyIndex {
        /// Rejected zero-based key index.
        index: usize,
        /// Number of configured storage keys.
        key_count: usize,
    },
    /// A remember-entity or remember-shard store read failed.
    RememberStoreReadFailed {
        /// Distributed-data or logical store key that was read.
        key: String,
        /// Store-provided failure detail.
        reason: String,
    },
    /// A remember-entity or remember-shard store update failed.
    RememberStoreUpdateFailed {
        /// Distributed-data or logical store key that was updated.
        key: String,
        /// Store-provided failure detail.
        reason: String,
    },
    /// A region learned a different home for a shard it already routes.
    InconsistentShardHome {
        /// Shard whose ownership conflicted.
        shard: String,
        /// Region currently recorded as the shard home.
        current_region: String,
        /// Conflicting region from the new shard-home observation.
        new_region: String,
    },
    /// A shard-home observation arrived after two-phase handoff began.
    ShardHomeDuringHandOff {
        /// Shard currently being handed off.
        shard: String,
        /// Region named by the rejected shard-home observation.
        region: String,
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
            Self::ShardHomeDuringHandOff { shard, region } => {
                write!(f, "shard {shard} home {region} arrived during handoff")
            }
        }
    }
}

impl std::error::Error for ShardingError {}
