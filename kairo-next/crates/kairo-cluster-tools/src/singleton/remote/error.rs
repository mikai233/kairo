use std::fmt::{self, Display, Formatter};

use kairo_serialization::SerializationError;

/// Failure while constructing, sending, validating, or decoding singleton traffic.
#[derive(Debug)]
pub enum SingletonManagerRemoteError {
    /// The configured system recipient path is not absolute.
    InvalidRecipientPath(String),
    /// A target node has no host and therefore cannot be addressed remotely.
    MissingRemoteHost {
        /// Deterministic target member identity.
        node: String,
    },
    /// Stable message serialization or deserialization failed.
    Serialization(SerializationError),
    /// Delivery to a remote transport or local manager mailbox failed.
    Send {
        /// Remote member or local actor path that rejected delivery.
        target: String,
        /// Underlying delivery failure.
        reason: String,
    },
    /// A local-only manager effect was passed to the remote adapter.
    UnsupportedEffect(&'static str),
    /// The inbound message manifest is not part of the handover protocol.
    UnsupportedManifest(String),
    /// An envelope was not addressed to this node's canonical manager path.
    WrongRecipient {
        /// Canonical recipient path for the local node.
        expected: String,
        /// Recipient path carried by the rejected envelope.
        actual: String,
    },
}

impl Display for SingletonManagerRemoteError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRecipientPath(path) => {
                write!(
                    f,
                    "singleton manager remote path `{path}` must start with `/`"
                )
            }
            Self::MissingRemoteHost { node } => {
                write!(f, "singleton manager target {node} has no remote host")
            }
            Self::Serialization(error) => write!(f, "{error}"),
            Self::Send { target, reason } => {
                write!(
                    f,
                    "singleton manager remote send to {target} failed: {reason}"
                )
            }
            Self::UnsupportedEffect(effect) => {
                write!(f, "singleton manager effect `{effect}` is local-only")
            }
            Self::UnsupportedManifest(manifest) => {
                write!(f, "unsupported singleton manager manifest `{manifest}`")
            }
            Self::WrongRecipient { expected, actual } => {
                write!(
                    f,
                    "singleton manager envelope was addressed to {actual}, expected {expected}"
                )
            }
        }
    }
}

impl std::error::Error for SingletonManagerRemoteError {}

impl From<SerializationError> for SingletonManagerRemoteError {
    fn from(error: SerializationError) -> Self {
        Self::Serialization(error)
    }
}
