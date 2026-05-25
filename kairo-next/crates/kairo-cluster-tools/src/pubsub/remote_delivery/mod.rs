mod error;
mod inbound;
mod outbound;

use kairo_cluster::UniqueAddress;
use kairo_serialization::ActorRefWireData;

use super::remote::DEFAULT_PUBSUB_REMOTE_PATH;

pub use self::error::PubSubRemoteDeliveryError;
pub use self::inbound::PubSubRemoteDeliveryInbound;
pub use self::outbound::PubSubRemoteDeliveryOutbound;

fn recipient_for_node(
    node: &UniqueAddress,
    recipient_path: &str,
) -> Result<ActorRefWireData, PubSubRemoteDeliveryError> {
    if !recipient_path.starts_with('/') {
        return Err(PubSubRemoteDeliveryError::InvalidRecipientPath(
            recipient_path.to_string(),
        ));
    }
    if node.address.host().is_none() {
        return Err(PubSubRemoteDeliveryError::MissingRemoteHost {
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
) -> Result<(), PubSubRemoteDeliveryError> {
    let expected = recipient_for_node(node, recipient_path)?;
    if actual != &expected {
        return Err(PubSubRemoteDeliveryError::WrongRecipient {
            expected: expected.path().to_string(),
            actual: actual.path().to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests;
