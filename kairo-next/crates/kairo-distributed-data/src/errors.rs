#![deny(missing_docs)]

//! Errors shared by CRDT arithmetic and consistency planning.

use std::fmt::{self, Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq)]
/// Failure produced by a CRDT value operation.
pub enum CrdtError {
    /// An unsigned per-replica counter or aggregate exceeded its numeric range.
    CounterOverflow,
    /// A signed counter result cannot be represented by the public value type.
    CounterValueOutOfRange,
}

impl Display for CrdtError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::CounterOverflow => f.write_str("counter value overflowed"),
            Self::CounterValueOutOfRange => {
                f.write_str("counter value is outside the supported signed range")
            }
        }
    }
}

impl std::error::Error for CrdtError {}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Invalid replica-count input for a non-local consistency level.
pub enum ConsistencyError {
    /// Fewer than two replicas were requested for distributed consistency.
    ReplicaCountTooSmall {
        /// Invalid requested replica count.
        requested: usize,
    },
}

impl Display for ConsistencyError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReplicaCountTooSmall { requested } => write!(
                f,
                "replica count {requested} is too small; use local consistency for one replica"
            ),
        }
    }
}

impl std::error::Error for ConsistencyError {}
