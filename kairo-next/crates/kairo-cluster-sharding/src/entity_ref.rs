use kairo_actor::{ActorRef, SendError};

use crate::{EntityId, ShardingEnvelope};

#[derive(Debug, Clone)]
pub struct EntityRef<M> {
    entity_id: EntityId,
    region: ActorRef<ShardingEnvelope<M>>,
}

impl<M: Send + 'static> EntityRef<M> {
    pub fn new(entity_id: impl Into<EntityId>, region: ActorRef<ShardingEnvelope<M>>) -> Self {
        Self {
            entity_id: entity_id.into(),
            region,
        }
    }

    pub fn entity_id(&self) -> &str {
        &self.entity_id
    }

    pub fn tell(&self, message: M) -> Result<(), SendError<ShardingEnvelope<M>>> {
        self.region
            .tell(ShardingEnvelope::new(self.entity_id.clone(), message))
    }
}
