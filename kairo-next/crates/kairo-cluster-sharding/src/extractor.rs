#![deny(missing_docs)]

use std::marker::PhantomData;

use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, Context, Props};

use crate::{
    EntityId, RegionLocalRoutePlan, ShardDeliverPlan, ShardId, ShardRegionMsg, ShardingEnvelope,
    ShardingError, shard_id_for,
};

/// Optional adapter from an application-specific input protocol to sharding metadata.
///
/// Returning `None` ignores the input. Returning an extracted message may
/// provide an explicit shard identifier or let the router derive one with
/// Kairo's stable hash. This adapter is not required when callers can use
/// [`ShardingEnvelope`] or [`crate::EntityRef`] directly.
pub trait EntityMessageExtractor<In, M>: Send + 'static
where
    In: Send + 'static,
    M: Send + 'static,
{
    /// Extracts routing metadata and the entity business message from one input.
    fn extract(&mut self, message: In) -> Option<ExtractedEntityMessage<M>>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Business message paired with entity routing metadata produced by an extractor.
pub struct ExtractedEntityMessage<M> {
    entity_id: EntityId,
    shard_id: Option<ShardId>,
    message: M,
}

impl<M> ExtractedEntityMessage<M> {
    /// Creates a message whose shard is derived from the stable entity-id hash.
    pub fn new(entity_id: impl Into<EntityId>, message: M) -> Self {
        Self {
            entity_id: entity_id.into(),
            shard_id: None,
            message,
        }
    }

    /// Creates a message with an application-supplied shard identifier.
    ///
    /// Explicit shard identifiers bypass the router's stable hash calculation.
    pub fn with_shard_id(
        entity_id: impl Into<EntityId>,
        shard_id: impl Into<ShardId>,
        message: M,
    ) -> Self {
        Self {
            entity_id: entity_id.into(),
            shard_id: Some(shard_id.into()),
            message,
        }
    }

    /// Returns the extracted logical entity identifier.
    pub fn entity_id(&self) -> &str {
        &self.entity_id
    }

    /// Returns the explicit shard identifier, if one was supplied.
    pub fn shard_id(&self) -> Option<&str> {
        self.shard_id.as_deref()
    }

    /// Borrows the extracted business message.
    pub fn message(&self) -> &M {
        &self.message
    }

    /// Consumes the value and returns the entity id, optional shard id, and message.
    pub fn into_parts(self) -> (EntityId, Option<ShardId>, M) {
        (self.entity_id, self.shard_id, self.message)
    }
}

/// Actor adapter that applies an [`EntityMessageExtractor`] and routes matches to a region.
///
/// Messages without an explicit shard use [`shard_id_for`]. Inputs rejected by
/// the extractor are deliberately dropped, matching an extractor's filtering
/// role rather than manufacturing a routing failure.
pub struct EntityMessageExtractorRouter<In, M, E>
where
    In: Send + 'static,
    M: Send + 'static,
    E: EntityMessageExtractor<In, M>,
{
    region: ActorRef<ShardRegionMsg<M>>,
    shard_count: u64,
    extractor: E,
    route_reply_to: Option<ActorRef<RegionLocalRoutePlan<M>>>,
    delivery_reply_to: Option<ActorRef<ShardDeliverPlan<M>>>,
    _input: PhantomData<fn(In)>,
}

impl<In, M, E> EntityMessageExtractorRouter<In, M, E>
where
    In: Send + 'static,
    M: Send + 'static,
    E: EntityMessageExtractor<In, M>,
{
    /// Creates an extractor router for `region` and the configured shard count.
    pub fn new(region: ActorRef<ShardRegionMsg<M>>, shard_count: u64, extractor: E) -> Self {
        Self {
            region,
            shard_count,
            extractor,
            route_reply_to: None,
            delivery_reply_to: None,
            _input: PhantomData,
        }
    }

    /// Creates actor properties for an extractor router.
    pub fn props(
        region: ActorRef<ShardRegionMsg<M>>,
        shard_count: u64,
        extractor: E,
    ) -> Props<Self> {
        Props::new(move || Self::new(region, shard_count, extractor))
    }

    /// Returns the typed region command target.
    pub fn region(&self) -> &ActorRef<ShardRegionMsg<M>> {
        &self.region
    }

    /// Returns the shard count used when an extractor omits a shard identifier.
    pub fn shard_count(&self) -> u64 {
        self.shard_count
    }
}

impl<In, M, E> Actor for EntityMessageExtractorRouter<In, M, E>
where
    In: Send + 'static,
    M: Send + 'static,
    E: EntityMessageExtractor<In, M>,
{
    type Msg = In;

    fn started(&mut self, ctx: &mut Context<Self::Msg>) -> ActorResult {
        let route_reply_to = ctx.spawn(
            "route-plan-sink",
            PlanSink::<RegionLocalRoutePlan<M>>::props(),
        )?;
        let delivery_reply_to = ctx.spawn(
            "delivery-plan-sink",
            PlanSink::<ShardDeliverPlan<M>>::props(),
        )?;
        self.route_reply_to = Some(route_reply_to);
        self.delivery_reply_to = Some(delivery_reply_to);
        Ok(())
    }

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        let Some(extracted) = self.extractor.extract(msg) else {
            return Ok(());
        };
        let (entity_id, shard_id, message) = extracted.into_parts();
        let shard = match shard_id {
            Some(shard_id) => shard_id,
            None => shard_id_for(&entity_id, self.shard_count).map_err(actor_error)?,
        };
        let route_reply_to = self.route_reply_to.clone().ok_or_else(|| {
            ActorError::Message("entity message extractor router is not started".into())
        })?;
        let delivery_reply_to = self.delivery_reply_to.clone().ok_or_else(|| {
            ActorError::Message(
                "entity message extractor router delivery sink is not started".into(),
            )
        })?;
        self.region
            .tell(ShardRegionMsg::RouteToLocalShard {
                shard,
                message: ShardingEnvelope::new(entity_id, message),
                route_reply_to,
                delivery_reply_to,
            })
            .map_err(|error| ActorError::Message(error.reason().to_string()))
    }
}

fn actor_error(error: ShardingError) -> ActorError {
    ActorError::Message(error.to_string())
}

struct PlanSink<M> {
    _message: PhantomData<fn(M)>,
}

impl<M> PlanSink<M> {
    fn props() -> Props<Self>
    where
        M: Send + 'static,
    {
        Props::new(|| Self {
            _message: PhantomData,
        })
    }
}

impl<M> Actor for PlanSink<M>
where
    M: Send + 'static,
{
    type Msg = M;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, _msg: Self::Msg) -> ActorResult {
        Ok(())
    }
}
