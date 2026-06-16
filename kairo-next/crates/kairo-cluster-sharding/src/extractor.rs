use std::marker::PhantomData;

use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, Context, Props};

use crate::{
    EntityId, RegionLocalRoutePlan, ShardDeliverPlan, ShardId, ShardRegionMsg, ShardingEnvelope,
    ShardingError, shard_id_for,
};

pub trait EntityMessageExtractor<In, M>: Send + 'static
where
    In: Send + 'static,
    M: Send + 'static,
{
    fn extract(&mut self, message: In) -> Option<ExtractedEntityMessage<M>>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtractedEntityMessage<M> {
    entity_id: EntityId,
    shard_id: Option<ShardId>,
    message: M,
}

impl<M> ExtractedEntityMessage<M> {
    pub fn new(entity_id: impl Into<EntityId>, message: M) -> Self {
        Self {
            entity_id: entity_id.into(),
            shard_id: None,
            message,
        }
    }

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

    pub fn entity_id(&self) -> &str {
        &self.entity_id
    }

    pub fn shard_id(&self) -> Option<&str> {
        self.shard_id.as_deref()
    }

    pub fn message(&self) -> &M {
        &self.message
    }

    pub fn into_parts(self) -> (EntityId, Option<ShardId>, M) {
        (self.entity_id, self.shard_id, self.message)
    }
}

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

    pub fn props(
        region: ActorRef<ShardRegionMsg<M>>,
        shard_count: u64,
        extractor: E,
    ) -> Props<Self> {
        Props::new(move || Self::new(region, shard_count, extractor))
    }

    pub fn region(&self) -> &ActorRef<ShardRegionMsg<M>> {
        &self.region
    }

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
