#![deny(missing_docs)]

use std::fmt::{self, Display, Formatter};

use kairo_serialization::SerializationError;

/// Failure at a remote shard-region routing or control boundary.
#[derive(Debug)]
pub enum ShardRegionRemoteError {
    /// Stable message or actor-ref serialization failed.
    Serialization(SerializationError),
    /// A configured region actor path was not absolute.
    InvalidRecipientPath(String),
    /// A target cluster node did not contain a remote host.
    MissingRemoteHost {
        /// Deterministic identity of the node that cannot be addressed remotely.
        node: String,
    },
    /// A control command that requires a reply omitted sender metadata.
    MissingSender(String),
    /// An inbound envelope targeted a different region endpoint.
    WrongRecipient {
        /// Configured region recipient path.
        expected: String,
        /// Recipient path carried by the envelope.
        actual: String,
    },
    /// A local typed region message has no stable remote representation here.
    UnsupportedLocalMessage(&'static str),
    /// An inbound manifest is not supported by the selected region bridge.
    UnsupportedManifest(String),
    /// Delivery to a local mailbox or outbound transport failed.
    Send {
        /// Local actor path or remote target identity.
        target: String,
        /// Delivery rejection reason.
        reason: String,
    },
}

impl Display for ShardRegionRemoteError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Serialization(error) => {
                write!(f, "sharding remote serialization failed: {error}")
            }
            Self::InvalidRecipientPath(path) => {
                write!(f, "invalid shard-region recipient path `{path}`")
            }
            Self::MissingRemoteHost { node } => {
                write!(f, "shard-region remote target `{node}` has no remote host")
            }
            Self::MissingSender(message) => {
                write!(f, "shard-region remote `{message}` envelope has no sender")
            }
            Self::WrongRecipient { expected, actual } => {
                write!(
                    f,
                    "shard-region remote envelope was addressed to {actual}, expected {expected}"
                )
            }
            Self::UnsupportedLocalMessage(message) => {
                write!(
                    f,
                    "shard-region remote outbound does not support local message `{message}`"
                )
            }
            Self::UnsupportedManifest(manifest) => {
                write!(f, "unsupported shard-region remote manifest `{manifest}`")
            }
            Self::Send { target, reason } => {
                write!(
                    f,
                    "shard-region remote delivery to `{target}` failed: {reason}"
                )
            }
        }
    }
}

impl std::error::Error for ShardRegionRemoteError {}

impl From<SerializationError> for ShardRegionRemoteError {
    fn from(error: SerializationError) -> Self {
        Self::Serialization(error)
    }
}
