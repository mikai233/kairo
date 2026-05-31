use std::fmt::{self, Display, Formatter};

use kairo_cluster::UniqueAddress;
use kairo_serialization::{ActorRefWireData, SerializationError};

pub const DEFAULT_SHARD_COORDINATOR_REMOTE_PATH: &str = "/system/sharding/coordinator";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardCoordinatorRemoteTarget {
    node: UniqueAddress,
    recipient: ActorRefWireData,
}

impl ShardCoordinatorRemoteTarget {
    pub fn new(
        node: UniqueAddress,
        recipient: ActorRefWireData,
    ) -> Result<Self, ShardCoordinatorRemoteTargetError> {
        if recipient.host().is_none() {
            return Err(ShardCoordinatorRemoteTargetError::MissingRemoteHost {
                node: node.ordering_key(),
            });
        }
        Ok(Self { node, recipient })
    }

    pub fn for_node(
        node: UniqueAddress,
        recipient_path: impl AsRef<str>,
    ) -> Result<Self, ShardCoordinatorRemoteTargetError> {
        let recipient = coordinator_recipient_for_node(&node, recipient_path.as_ref())?;
        Self::new(node, recipient)
    }

    pub fn node(&self) -> &UniqueAddress {
        &self.node
    }

    pub fn recipient(&self) -> &ActorRefWireData {
        &self.recipient
    }
}

#[derive(Debug)]
pub enum ShardCoordinatorRemoteTargetError {
    InvalidRecipientPath(String),
    MissingRemoteHost { node: String },
    Serialization(SerializationError),
}

impl Display for ShardCoordinatorRemoteTargetError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRecipientPath(path) => {
                write!(f, "invalid shard-coordinator recipient path `{path}`")
            }
            Self::MissingRemoteHost { node } => {
                write!(
                    f,
                    "shard-coordinator remote target `{node}` has no remote host"
                )
            }
            Self::Serialization(error) => {
                write!(f, "shard-coordinator remote target is invalid: {error}")
            }
        }
    }
}

impl std::error::Error for ShardCoordinatorRemoteTargetError {}

impl From<SerializationError> for ShardCoordinatorRemoteTargetError {
    fn from(error: SerializationError) -> Self {
        Self::Serialization(error)
    }
}

pub fn coordinator_recipient_for_node(
    node: &UniqueAddress,
    recipient_path: &str,
) -> Result<ActorRefWireData, ShardCoordinatorRemoteTargetError> {
    if !recipient_path.starts_with('/') {
        return Err(ShardCoordinatorRemoteTargetError::InvalidRecipientPath(
            recipient_path.to_string(),
        ));
    }
    if node.address.host().is_none() {
        return Err(ShardCoordinatorRemoteTargetError::MissingRemoteHost {
            node: node.ordering_key(),
        });
    }
    Ok(ActorRefWireData::new(format!(
        "{}{}",
        node.address, recipient_path
    ))?)
}

#[cfg(test)]
mod tests {
    use kairo_actor::Address;

    use super::*;

    #[test]
    fn coordinator_remote_target_builds_stable_wire_recipient_for_node() {
        let node = UniqueAddress::new(
            Address::new(
                "kairo",
                "sharding",
                Some("127.0.0.1".to_string()),
                Some(2552),
            ),
            17,
        );

        let target = ShardCoordinatorRemoteTarget::for_node(
            node.clone(),
            DEFAULT_SHARD_COORDINATOR_REMOTE_PATH,
        )
        .unwrap();

        assert_eq!(target.node(), &node);
        assert_eq!(
            target.recipient().path(),
            "kairo://sharding@127.0.0.1:2552/system/sharding/coordinator"
        );
        assert_eq!(target.recipient().system(), "sharding");
        assert_eq!(target.recipient().host(), Some("127.0.0.1"));
    }

    #[test]
    fn coordinator_remote_target_rejects_local_only_nodes() {
        let node = UniqueAddress::new(Address::local("sharding"), 18);

        let error =
            ShardCoordinatorRemoteTarget::for_node(node, DEFAULT_SHARD_COORDINATOR_REMOTE_PATH)
                .unwrap_err();

        assert!(matches!(
            error,
            ShardCoordinatorRemoteTargetError::MissingRemoteHost { .. }
        ));
    }
}
