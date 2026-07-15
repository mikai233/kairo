#![deny(missing_docs)]

use kairo_serialization::ActorRefWireData;

use crate::UniqueAddress;

use super::ClusterHeartbeatRemoteError;

/// Default remote actor path for cluster heartbeat requests.
pub const DEFAULT_CLUSTER_HEARTBEAT_RECEIVER_PATH: &str = "/system/cluster/heartbeatReceiver";
/// Default remote actor path for cluster heartbeat responses.
pub const DEFAULT_CLUSTER_HEARTBEAT_SENDER_PATH: &str = "/system/cluster/heartbeatSender";

pub(super) fn recipient_for_node(
    node: &UniqueAddress,
    recipient_path: &str,
) -> Result<ActorRefWireData, ClusterHeartbeatRemoteError> {
    if !recipient_path.starts_with('/') {
        return Err(ClusterHeartbeatRemoteError::InvalidRecipientPath(
            recipient_path.to_string(),
        ));
    }
    if node.address.host().is_none() {
        return Err(ClusterHeartbeatRemoteError::MissingRemoteHost {
            node: node.ordering_key(),
        });
    }
    Ok(ActorRefWireData::new(format!(
        "{}{}",
        node.address, recipient_path
    ))?)
}

pub(super) fn validate_recipient(
    node: &UniqueAddress,
    recipient_path: &str,
    actual: &ActorRefWireData,
) -> Result<(), ClusterHeartbeatRemoteError> {
    let expected = recipient_for_node(node, recipient_path)?;
    if actual != &expected {
        return Err(ClusterHeartbeatRemoteError::WrongRecipient {
            expected: expected.path().to_string(),
            actual: actual.path().to_string(),
        });
    }
    Ok(())
}
