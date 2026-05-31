use std::fmt::{self, Display, Formatter};

use kairo_serialization::SerializationError;

#[derive(Debug)]
pub enum ShardRegionRemoteError {
    Serialization(SerializationError),
    InvalidRecipientPath(String),
    MissingRemoteHost { node: String },
    MissingSender(String),
    WrongRecipient { expected: String, actual: String },
    UnsupportedLocalMessage(&'static str),
    UnsupportedManifest(String),
    Send { target: String, reason: String },
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
