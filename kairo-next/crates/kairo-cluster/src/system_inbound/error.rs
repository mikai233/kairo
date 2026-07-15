#![deny(missing_docs)]

use std::fmt::{self, Display, Formatter};

use crate::{
    ClusterGossipWireError, ClusterHeartbeatRemoteError, ClusterMembershipWireError,
    ClusterSeedJoinWireError,
};

#[derive(Debug)]
/// Failure produced while validating or dispatching an inbound cluster control envelope.
pub enum ClusterSystemInboundError {
    /// Heartbeat request or response handling failed.
    Heartbeat(ClusterHeartbeatRemoteError),
    /// Gossip status handling failed.
    Gossip(ClusterGossipWireError),
    /// A configured canonical cluster recipient path was not absolute.
    InvalidRecipientPath(String),
    /// Membership command or gossip-envelope handling failed.
    Membership(ClusterMembershipWireError),
    /// Seed-contact request or response handling failed.
    SeedJoin(ClusterSeedJoinWireError),
    /// The router recognized the manifest but its protocol handler was not installed.
    MissingHandler(&'static str),
    /// The local node identity cannot form a canonical remote recipient.
    MissingRemoteHost {
        /// Stable diagnostic identity of the local node.
        node: String,
    },
    /// The registered codec could not decode the control message.
    Serialization(kairo_serialization::SerializationError),
    /// The envelope manifest is not part of the cluster control protocol.
    UnsupportedManifest(String),
    /// The envelope targets a different node or system actor path.
    WrongRecipient {
        /// Canonical recipient path required by the manifest.
        expected: String,
        /// Recipient path carried by the envelope.
        actual: String,
    },
}

impl Display for ClusterSystemInboundError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Heartbeat(error) => write!(f, "{error}"),
            Self::Gossip(error) => write!(f, "{error}"),
            Self::InvalidRecipientPath(path) => {
                write!(f, "cluster recipient path `{path}` must start with `/`")
            }
            Self::Membership(error) => write!(f, "{error}"),
            Self::SeedJoin(error) => write!(f, "{error}"),
            Self::MissingHandler(handler) => {
                write!(f, "cluster system inbound has no {handler} handler")
            }
            Self::MissingRemoteHost { node } => {
                write!(f, "cluster self node {node} has no remote host")
            }
            Self::Serialization(error) => write!(f, "{error}"),
            Self::UnsupportedManifest(manifest) => {
                write!(f, "unsupported cluster system manifest `{manifest}`")
            }
            Self::WrongRecipient { expected, actual } => {
                write!(
                    f,
                    "cluster system envelope addressed to {actual}, expected {expected}"
                )
            }
        }
    }
}

impl std::error::Error for ClusterSystemInboundError {}

impl From<ClusterHeartbeatRemoteError> for ClusterSystemInboundError {
    fn from(error: ClusterHeartbeatRemoteError) -> Self {
        Self::Heartbeat(error)
    }
}

impl From<ClusterMembershipWireError> for ClusterSystemInboundError {
    fn from(error: ClusterMembershipWireError) -> Self {
        Self::Membership(error)
    }
}

impl From<ClusterGossipWireError> for ClusterSystemInboundError {
    fn from(error: ClusterGossipWireError) -> Self {
        Self::Gossip(error)
    }
}

impl From<ClusterSeedJoinWireError> for ClusterSystemInboundError {
    fn from(error: ClusterSeedJoinWireError) -> Self {
        Self::SeedJoin(error)
    }
}

impl From<kairo_serialization::SerializationError> for ClusterSystemInboundError {
    fn from(error: kairo_serialization::SerializationError) -> Self {
        Self::Serialization(error)
    }
}
