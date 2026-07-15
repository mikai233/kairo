#![deny(missing_docs)]

use std::fmt::{self, Display, Formatter};

use kairo_serialization::SerializationError;

#[derive(Debug)]
/// Failure while routing or decoding remote cluster heartbeat traffic.
pub enum ClusterHeartbeatRemoteError {
    /// A configured heartbeat actor path was not absolute.
    InvalidRecipientPath(String),
    /// The target node has a local-only address.
    MissingRemoteHost {
        /// Stable display key for the rejected node incarnation.
        node: String,
    },
    /// A heartbeat request omitted the sender route required for its response.
    MissingSender,
    /// Registry serialization or actor-reference encoding failed.
    Serialization(SerializationError),
    /// A remote outbound or local actor recipient rejected delivery.
    Send {
        /// Remote recipient or local actor path.
        target: String,
        /// Error reported by the delivery boundary.
        reason: String,
    },
    /// The payload manifest was not valid for this heartbeat endpoint.
    UnsupportedManifest(String),
    /// The envelope was addressed to a different heartbeat endpoint.
    WrongRecipient {
        /// Canonical actor path expected by the endpoint.
        expected: String,
        /// Canonical actor path carried by the envelope.
        actual: String,
    },
}

impl Display for ClusterHeartbeatRemoteError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRecipientPath(path) => {
                write!(
                    f,
                    "cluster heartbeat remote path `{path}` must start with `/`"
                )
            }
            Self::MissingRemoteHost { node } => {
                write!(f, "cluster heartbeat target {node} has no remote host")
            }
            Self::MissingSender => {
                write!(
                    f,
                    "cluster heartbeat request is missing remote sender metadata"
                )
            }
            Self::Serialization(error) => write!(f, "{error}"),
            Self::Send { target, reason } => {
                write!(
                    f,
                    "cluster heartbeat remote send to {target} failed: {reason}"
                )
            }
            Self::UnsupportedManifest(manifest) => {
                write!(f, "unsupported cluster heartbeat manifest `{manifest}`")
            }
            Self::WrongRecipient { expected, actual } => {
                write!(
                    f,
                    "cluster heartbeat envelope was addressed to {actual}, expected {expected}"
                )
            }
        }
    }
}

impl std::error::Error for ClusterHeartbeatRemoteError {}

impl From<SerializationError> for ClusterHeartbeatRemoteError {
    fn from(error: SerializationError) -> Self {
        Self::Serialization(error)
    }
}
