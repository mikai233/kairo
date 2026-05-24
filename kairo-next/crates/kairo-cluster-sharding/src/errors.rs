use std::fmt::{self, Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShardingError {
    InvalidShardCount,
    InvalidRebalanceLimit,
    NoShardRegions,
    UnknownRegion(String),
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
        }
    }
}

impl std::error::Error for ShardingError {}
