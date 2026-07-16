use std::fmt::{self, Display, Formatter};

use kairo_serialization::SerializationError;

#[derive(Debug)]
/// Failure while encoding, delivering, or validating a distributed-data remote envelope.
pub enum ReplicatorRemoteEnvelopeError {
    /// Stable remote-message serialization failed.
    Serialization(SerializationError),
    /// The configured outbound recipient rejected delivery.
    Send(String),
    /// An inbound envelope targeted a different actor reference.
    WrongRecipient {
        /// Actor reference owned by the inbound adapter.
        expected: String,
        /// Actor reference carried by the rejected envelope.
        actual: String,
    },
}

impl Display for ReplicatorRemoteEnvelopeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Serialization(error) => {
                write!(
                    f,
                    "replicator remote envelope serialization failed: {error}"
                )
            }
            Self::Send(reason) => {
                write!(f, "replicator remote envelope delivery failed: {reason}")
            }
            Self::WrongRecipient { expected, actual } => {
                write!(
                    f,
                    "replicator remote envelope was addressed to {actual}, expected {expected}"
                )
            }
        }
    }
}

impl std::error::Error for ReplicatorRemoteEnvelopeError {}

impl From<SerializationError> for ReplicatorRemoteEnvelopeError {
    fn from(error: SerializationError) -> Self {
        Self::Serialization(error)
    }
}
