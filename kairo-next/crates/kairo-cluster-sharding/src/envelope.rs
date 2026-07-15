#![deny(missing_docs)]

use crate::EntityId;

#[derive(Debug, Clone, PartialEq, Eq)]
/// Default typed routing envelope accepted by a sharding region.
///
/// The entity identifier remains routing metadata outside `M`, so entity actors
/// receive only their business protocol.
pub struct ShardingEnvelope<M> {
    entity_id: EntityId,
    message: M,
}

impl<M> ShardingEnvelope<M> {
    /// Wraps `message` with the logical entity identifier used for routing.
    pub fn new(entity_id: impl Into<EntityId>, message: M) -> Self {
        Self {
            entity_id: entity_id.into(),
            message,
        }
    }

    /// Returns the logical entity identifier.
    pub fn entity_id(&self) -> &str {
        &self.entity_id
    }

    /// Borrows the business message.
    pub fn message(&self) -> &M {
        &self.message
    }

    /// Consumes the envelope and returns only the business message.
    pub fn into_message(self) -> M {
        self.message
    }

    /// Consumes the envelope and returns its routing metadata and message.
    pub fn into_parts(self) -> (EntityId, M) {
        (self.entity_id, self.message)
    }
}
