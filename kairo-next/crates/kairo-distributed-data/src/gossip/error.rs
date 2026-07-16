use std::fmt::{self, Display, Formatter};

use kairo_serialization::SerializationError;

#[derive(Debug)]
/// A failure while hashing, planning, encoding, or applying full-state gossip.
pub enum ReplicatorGossipError {
    /// A CRDT envelope could not be encoded or decoded.
    Serialization(SerializationError),
    /// A chunk index was outside the declared non-empty chunk range.
    InvalidChunk {
        /// The zero-based chunk index supplied by the caller or peer.
        chunk: u32,
        /// The declared number of chunks.
        total_chunks: u32,
    },
    /// A status response was requested with a zero entry limit.
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
