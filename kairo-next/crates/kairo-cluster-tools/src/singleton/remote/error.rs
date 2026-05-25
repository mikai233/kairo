use std::fmt::{self, Display, Formatter};

use kairo_serialization::SerializationError;

#[derive(Debug)]
pub enum SingletonManagerRemoteError {
    InvalidRecipientPath(String),
    MissingRemoteHost { node: String },
    Serialization(SerializationError),
    Send { target: String, reason: String },
    UnsupportedEffect(&'static str),
    UnsupportedManifest(String),
    WrongRecipient { expected: String, actual: String },
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
