#![deny(missing_docs)]

use std::sync::Arc;

use kairo_actor::{Actor, ActorError, ActorRef, Context, Props};

use crate::{EntityId, ShardMsg};

type SpawnEntity<M> =
    dyn Fn(&Context<ShardMsg<M>>, &EntityId) -> Result<ActorRef<M>, ActorError> + Send + Sync;

/// Cloneable factory used by a shard actor to spawn typed entity actors on demand.
///
/// The factory receives the business entity id for each new incarnation. Kairo
/// encodes the id's UTF-8 bytes into a collision-free actor child name, keeping
/// arbitrary business identifiers out of actor-path syntax.
pub struct EntityActorFactory<M>
where
    M: Send + 'static,
{
    spawn: Arc<SpawnEntity<M>>,
}

impl<M> Clone for EntityActorFactory<M>
where
    M: Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            spawn: Arc::clone(&self.spawn),
        }
    }
}

impl<M> EntityActorFactory<M>
where
    M: Send + 'static,
{
    /// Creates a factory from an entity-id-aware actor constructor.
    pub fn new<A, F>(factory: F) -> Self
    where
        A: Actor<Msg = M>,
        F: Fn(EntityId) -> A + Send + Sync + 'static,
    {
        let factory = Arc::new(factory);
        Self {
            spawn: Arc::new(move |ctx, entity_id| {
                let entity_id = entity_id.clone();
                let name = entity_actor_name(&entity_id);
                let factory = Arc::clone(&factory);
                ctx.spawn(name, Props::new(move || factory(entity_id)))
            }),
        }
    }

    pub(crate) fn spawn(
        &self,
        ctx: &Context<ShardMsg<M>>,
        entity_id: &EntityId,
    ) -> Result<ActorRef<M>, ActorError> {
        (self.spawn)(ctx, entity_id)
    }
}

fn entity_actor_name(entity_id: &str) -> String {
    let mut name = String::from("entity-");
    for byte in entity_id.as_bytes() {
        name.push(hex_digit(byte >> 4));
        name.push(hex_digit(byte & 0x0f));
    }
    name
}

fn hex_digit(value: u8) -> char {
    match value {
        0..=9 => (b'0' + value) as char,
        10..=15 => (b'a' + value - 10) as char,
        _ => unreachable!("hex digit must be in 0..=15"),
    }
}
