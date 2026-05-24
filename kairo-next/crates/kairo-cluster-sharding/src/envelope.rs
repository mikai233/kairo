use crate::EntityId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardingEnvelope<M> {
    entity_id: EntityId,
    message: M,
}

impl<M> ShardingEnvelope<M> {
    pub fn new(entity_id: impl Into<EntityId>, message: M) -> Self {
        Self {
            entity_id: entity_id.into(),
            message,
        }
    }

    pub fn entity_id(&self) -> &str {
        &self.entity_id
    }

    pub fn message(&self) -> &M {
        &self.message
    }

    pub fn into_message(self) -> M {
        self.message
    }

    pub fn into_parts(self) -> (EntityId, M) {
        (self.entity_id, self.message)
    }
}
