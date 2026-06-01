use std::fmt::{self, Display, Formatter};

use kairo_serialization::SerializationError;

#[derive(Debug)]
pub enum ClusterHeartbeatRemoteError {
    InvalidRecipientPath(String),
    MissingRemoteHost { node: String },
    MissingSender,
    Serialization(SerializationError),
    Send { target: String, reason: String },
    UnsupportedManifest(String),
    WrongRecipient { expected: String, actual: String },
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
