#![deny(missing_docs)]

//! Stable identity of a distributed-data replica incarnation.

use kairo_cluster::UniqueAddress;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
/// Stable identity of one distributed-data replica incarnation.
///
/// Production connectors derive this value from a cluster [`UniqueAddress`],
/// including its UID. Constructing an arbitrary id is useful for local tests
/// and explicit transports but does not create cluster membership.
pub struct ReplicaId(String);

impl ReplicaId {
    /// Creates a replica identity from its stable string representation.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the stable string representation.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&UniqueAddress> for ReplicaId {
    fn from(value: &UniqueAddress) -> Self {
        Self(value.ordering_key())
    }
}

impl From<UniqueAddress> for ReplicaId {
    fn from(value: UniqueAddress) -> Self {
        Self(value.ordering_key())
    }
}
