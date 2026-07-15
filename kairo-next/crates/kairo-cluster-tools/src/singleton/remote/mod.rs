mod error;
mod inbound;
mod local_inbound;
mod outbound;

use kairo_cluster::UniqueAddress;
use kairo_serialization::ActorRefWireData;

pub use self::error::SingletonManagerRemoteError;
pub use self::inbound::SingletonManagerRemoteInbound;
pub use self::local_inbound::LocalSingletonManagerRemoteInbound;
pub use self::outbound::SingletonManagerRemoteOutbound;

pub const DEFAULT_SINGLETON_MANAGER_REMOTE_PATH: &str = "/system/singleton/manager";

fn recipient_for_node(
    node: &UniqueAddress,
    recipient_path: &str,
) -> Result<ActorRefWireData, SingletonManagerRemoteError> {
    if !recipient_path.starts_with('/') {
        return Err(SingletonManagerRemoteError::InvalidRecipientPath(
            recipient_path.to_string(),
        ));
    }
    if node.address.host().is_none() {
        return Err(SingletonManagerRemoteError::MissingRemoteHost {
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
) -> Result<(), SingletonManagerRemoteError> {
    let expected = recipient_for_node(node, recipient_path)?;
    if actual != &expected {
        return Err(SingletonManagerRemoteError::WrongRecipient {
            expected: expected.path().to_string(),
            actual: actual.path().to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests;
