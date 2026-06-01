use kairo_cluster::UniqueAddress;
use kairo_serialization::ActorRefWireData;

use super::super::{ShardRegionRemoteError, recipient_for_node};

#[derive(Clone)]
pub(super) enum ShardRegionRemoteControlTarget {
    Node {
        node: UniqueAddress,
        recipient_path: String,
    },
    Recipient(ActorRefWireData),
}

impl ShardRegionRemoteControlTarget {
    pub(super) fn node(node: UniqueAddress, recipient_path: String) -> Self {
        Self::Node {
            node,
            recipient_path,
        }
    }

    pub(super) fn recipient(recipient: ActorRefWireData) -> Self {
        Self::Recipient(recipient)
    }

    pub(super) fn resolve_recipient(&self) -> Result<ActorRefWireData, ShardRegionRemoteError> {
        match self {
            Self::Node {
                node,
                recipient_path,
            } => recipient_for_node(node, recipient_path),
            Self::Recipient(recipient) => Ok(recipient.clone()),
        }
    }

    pub(super) fn key(&self) -> String {
        match self {
            Self::Node { node, .. } => node.ordering_key(),
            Self::Recipient(recipient) => recipient.path().to_string(),
        }
    }

    pub(super) fn set_recipient_path(&mut self, path: String) {
        if let Self::Node { recipient_path, .. } = self {
            *recipient_path = path;
        }
    }
}
