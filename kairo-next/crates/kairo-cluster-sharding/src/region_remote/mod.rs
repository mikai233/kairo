#![deny(missing_docs)]

mod control;
mod error;
mod inbound;
mod outbound;

use kairo_cluster::UniqueAddress;
use kairo_serialization::ActorRefWireData;

pub use self::control::{
    ShardRegionRemoteControlCommand, ShardRegionRemoteControlInbound,
    ShardRegionRemoteControlOutbound, ShardRegionRemoteControlReplyTarget,
};
pub use self::error::ShardRegionRemoteError;
pub use self::inbound::ShardRegionRemoteInbound;
pub use self::outbound::ShardRegionRemoteOutbound;

/// Default stable remote actor path of a shard region endpoint.
pub const DEFAULT_SHARD_REGION_REMOTE_PATH: &str = "/system/sharding/region";

fn recipient_for_node(
    node: &UniqueAddress,
    recipient_path: &str,
) -> Result<ActorRefWireData, ShardRegionRemoteError> {
    if !recipient_path.starts_with('/') {
        return Err(ShardRegionRemoteError::InvalidRecipientPath(
            recipient_path.to_string(),
        ));
    }
    if node.address.host().is_none() {
        return Err(ShardRegionRemoteError::MissingRemoteHost {
            node: node.ordering_key(),
        });
    }
    Ok(ActorRefWireData::new(format!(
        "{}{}",
        node.address, recipient_path
    ))?)
}

fn validate_recipient(
    node: &UniqueAddress,
    recipient_path: &str,
    actual: &ActorRefWireData,
) -> Result<(), ShardRegionRemoteError> {
    let expected = recipient_for_node(node, recipient_path)?;
    if actual != &expected {
        return Err(ShardRegionRemoteError::WrongRecipient {
            expected: expected.path().to_string(),
            actual: actual.path().to_string(),
        });
    }
    Ok(())
}
