use std::fmt::{self, Display, Formatter};

use kairo_serialization::SerializationError;

#[derive(Debug)]
pub enum ReplicatorGossipError {
    Serialization(SerializationError),
    InvalidChunk { chunk: u32, total_chunks: u32 },
    ZeroMaxEntries,
}

impl Display for ReplicatorGossipError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Serialization(error) => {
                write!(f, "replicator gossip serialization failed: {error}")
            }
            Self::InvalidChunk {
                chunk,
                total_chunks,
            } => write!(
                f,
                "invalid replicator gossip chunk {chunk} for {total_chunks} total chunks"
            ),
            Self::ZeroMaxEntries => {
                write!(f, "replicator gossip max entries must be greater than zero")
            }
        }
    }
}

impl std::error::Error for ReplicatorGossipError {}

impl From<SerializationError> for ReplicatorGossipError {
    fn from(error: SerializationError) -> Self {
        Self::Serialization(error)
    }
}
