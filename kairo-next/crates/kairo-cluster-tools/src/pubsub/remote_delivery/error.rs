use std::fmt::{self, Display, Formatter};

use kairo_serialization::SerializationError;

#[derive(Debug)]
pub enum PubSubRemoteDeliveryError {
    InvalidRecipientPath(String),
    MissingRemoteHost { node: String },
    Serialization(SerializationError),
    Send { target: String, reason: String },
    UnsupportedManifest(String),
    UnsupportedLocalMessage(&'static str),
    WrongRecipient { expected: String, actual: String },
}

impl Display for PubSubRemoteDeliveryError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRecipientPath(path) => {
                write!(
                    f,
                    "pubsub remote delivery path `{path}` must start with `/`"
                )
            }
            Self::MissingRemoteHost { node } => {
                write!(f, "pubsub remote delivery target {node} has no remote host")
            }
            Self::Serialization(error) => write!(f, "{error}"),
            Self::Send { target, reason } => {
                write!(f, "pubsub remote delivery to {target} failed: {reason}")
            }
            Self::UnsupportedManifest(manifest) => {
                write!(f, "unsupported pubsub delivery manifest `{manifest}`")
            }
            Self::UnsupportedLocalMessage(message) => {
                write!(
                    f,
                    "pubsub remote delivery only supports publish messages, got `{message}`"
                )
            }
            Self::WrongRecipient { expected, actual } => {
                write!(
                    f,
                    "pubsub delivery envelope was addressed to {actual}, expected {expected}"
                )
            }
        }
    }
}

impl std::error::Error for PubSubRemoteDeliveryError {}

impl From<SerializationError> for PubSubRemoteDeliveryError {
    fn from(error: SerializationError) -> Self {
        Self::Serialization(error)
    }
}
