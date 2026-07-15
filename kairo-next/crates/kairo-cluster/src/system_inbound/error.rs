use std::fmt::{self, Display, Formatter};

use crate::{ClusterHeartbeatRemoteError, ClusterMembershipWireError, ClusterSeedJoinWireError};

#[derive(Debug)]
pub enum ClusterSystemInboundError {
    Heartbeat(ClusterHeartbeatRemoteError),
    InvalidRecipientPath(String),
    Membership(ClusterMembershipWireError),
    SeedJoin(ClusterSeedJoinWireError),
    MissingHandler(&'static str),
    MissingRemoteHost { node: String },
    Serialization(kairo_serialization::SerializationError),
    UnsupportedManifest(String),
    WrongRecipient { expected: String, actual: String },
}

impl Display for ClusterSystemInboundError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Heartbeat(error) => write!(f, "{error}"),
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
