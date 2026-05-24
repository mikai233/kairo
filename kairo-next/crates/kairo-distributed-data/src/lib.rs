//! CRDT-based replicated data for Kairo clusters.

mod protocol;

pub use protocol::{ReplicatorChanged, ReplicatorGet, ReplicatorSubscribe, ReplicatorUpdate};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReplicatorKey(String);

impl ReplicatorKey {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}
