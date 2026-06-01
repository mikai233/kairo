use std::fmt::{self, Display, Formatter};

use kairo_serialization::SerializationError;

#[derive(Debug)]
pub enum ReplicatorRemoteEnvelopeError {
    Serialization(SerializationError),
    Send(String),
    WrongRecipient { expected: String, actual: String },
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
