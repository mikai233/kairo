#![deny(missing_docs)]

use kairo_actor::{ActorRef, SendError};

use crate::{EntityId, ShardingEnvelope};

#[derive(Debug, Clone)]
/// Typed, location-transparent reference to one logical sharded entity.
///
/// An entity ref is intentionally not an actor ref: passivation, handoff, and
/// rebalancing may stop or move the current actor incarnation while the logical
/// entity remains addressable. Sending wraps the business message in a
/// [`ShardingEnvelope`] and uses the region's at-most-once delivery boundary.
pub struct EntityRef<M> {
    entity_id: EntityId,
    region: ActorRef<ShardingEnvelope<M>>,
}

impl<M: Send + 'static> EntityRef<M> {
    /// Binds `entity_id` to the typed sharding `region`.
    pub fn new(entity_id: impl Into<EntityId>, region: ActorRef<ShardingEnvelope<M>>) -> Self {
        Self {
            entity_id: entity_id.into(),
            region,
        }
    }

    /// Returns the stable business identifier of the logical entity.
    pub fn entity_id(&self) -> &str {
        &self.entity_id
    }

    /// Sends one business message through the region to this logical entity.
    pub fn tell(&self, message: M) -> Result<(), SendError<ShardingEnvelope<M>>> {
        self.region
            .tell(ShardingEnvelope::new(self.entity_id.clone(), message))
    }
}
