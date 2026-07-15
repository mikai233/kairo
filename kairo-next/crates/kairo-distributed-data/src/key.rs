#![deny(missing_docs)]

//! Stable string keys within a typed replicator namespace.

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
/// String key identifying one CRDT value within a typed replicator namespace.
///
/// Equality and ordering use the complete string. Separate registered CRDT
/// families may reuse a key string because each family owns a distinct typed
/// replicator actor and stable remote path.
pub struct ReplicatorKey(String);

impl ReplicatorKey {
    /// Creates a replicator key from its complete string identifier.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the complete key identifier.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
