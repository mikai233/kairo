#![deny(missing_docs)]

use std::marker::PhantomData;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
/// Typed logical key that identifies one sharded entity protocol.
///
/// The name must be unique for the entity type across the cluster. The message
/// parameter exists only at compile time, keeping regions for unrelated
/// business protocols distinct without relying on Rust type names on the wire.
pub struct EntityTypeKey<M> {
    name: String,
    _message: PhantomData<fn(M)>,
}

impl<M> EntityTypeKey<M> {
    /// Creates a typed entity key with a cluster-wide logical `name`.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            _message: PhantomData,
        }
    }

    /// Returns the stable logical entity-type name.
    pub fn name(&self) -> &str {
        &self.name
    }
}
