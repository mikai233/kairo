//! Cluster sharding API surface and protocols.

use std::marker::PhantomData;

use kairo_actor::{ActorRef, SendError};

pub type EntityId = String;
pub type ShardId = String;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EntityTypeKey<M> {
    name: String,
    _message: PhantomData<fn(M)>,
}

impl<M> EntityTypeKey<M> {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            _message: PhantomData,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug, Clone)]
pub struct EntityRef<M> {
    entity_id: EntityId,
    region: ActorRef<M>,
}

impl<M: Send + 'static> EntityRef<M> {
    pub fn entity_id(&self) -> &str {
        &self.entity_id
    }

    pub fn tell(&self, message: M) -> Result<(), SendError<M>> {
        self.region.tell(message)
    }
}
