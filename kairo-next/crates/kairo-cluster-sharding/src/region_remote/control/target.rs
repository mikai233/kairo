#![deny(missing_docs)]

use kairo_cluster::UniqueAddress;
use kairo_serialization::ActorRefWireData;

use super::super::{ShardRegionRemoteError, recipient_for_node};

/// Internal node-derived or explicit recipient form of a region control target.
#[derive(Clone)]
pub(super) enum ShardRegionRemoteControlTarget {
    /// Target resolved from cluster node address plus an actor path.
    Node {
        /// Remote cluster node.
        node: UniqueAddress,
        /// Absolute shard-region actor path.
        recipient_path: String,
    },
    /// Already resolved stable actor-ref recipient.
    Recipient(ActorRefWireData),
}

impl ShardRegionRemoteControlTarget {
    /// Creates a node-derived target.
    pub(super) fn node(node: UniqueAddress, recipient_path: String) -> Self {
        Self::Node {
            node,
            recipient_path,
        }
    }

    /// Creates an explicit wire-recipient target.
    pub(super) fn recipient(recipient: ActorRefWireData) -> Self {
        Self::Recipient(recipient)
    }

    /// Resolves the target into a stable actor-ref wire value.
    pub(super) fn resolve_recipient(&self) -> Result<ActorRefWireData, ShardRegionRemoteError> {
        match self {
            Self::Node {
                node,
                recipient_path,
            } => recipient_for_node(node, recipient_path),
            Self::Recipient(recipient) => Ok(recipient.clone()),
        }
    }

    /// Returns a deterministic identity for delivery errors.
    pub(super) fn key(&self) -> String {
        match self {
            Self::Node { node, .. } => node.ordering_key(),
            Self::Recipient(recipient) => recipient.path().to_string(),
        }
    }

    /// Replaces the actor path of a node-derived target.
    pub(super) fn set_recipient_path(&mut self, path: String) {
        if let Self::Node { recipient_path, .. } = self {
            *recipient_path = path;
        }
    }
}
