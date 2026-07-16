use std::fmt::{self, Display, Formatter};

use kairo_serialization::SerializationError;

/// Failure while encoding, validating, or dispatching remote pubsub delivery.
#[derive(Debug)]
pub enum PubSubRemoteDeliveryError {
    /// The configured recipient path was not an absolute actor path.
    InvalidRecipientPath(String),
    /// The destination is local-only and cannot identify a remote association.
    MissingRemoteHost {
        /// Ordering key of the rejected member incarnation.
        node: String,
    },
    /// Business-message, envelope, or actor-reference serialization failed.
    Serialization(SerializationError),
    /// A remote outbound or local mediator rejected the delivery.
    Send {
        /// Exact remote member key or local actor path that rejected delivery.
        target: String,
        /// Recipient- or transport-provided rejection reason.
        reason: String,
    },
    /// The inbound stable message was not a publish or path envelope.
    UnsupportedManifest(String),
    /// An actor-local command that must not cross the remote boundary was used.
    UnsupportedLocalMessage(&'static str),
    /// The remote envelope was addressed to a different actor path.
    WrongRecipient {
        /// Canonical recipient path for this inbound adapter.
        expected: String,
        /// Recipient path carried by the remote envelope.
        actual: String,
    },
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
