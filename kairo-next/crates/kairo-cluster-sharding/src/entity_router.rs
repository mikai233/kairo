#![deny(missing_docs)]

use std::marker::PhantomData;

use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, Context, Props};

use crate::{
    RegionLocalRoutePlan, ShardDeliverPlan, ShardRegionMsg, ShardingEnvelope, ShardingError,
    shard_id_for,
};

/// Actor that maps typed [`ShardingEnvelope`] values onto local region commands.
///
/// The router derives every shard with Kairo's stable hash and forwards the
/// unchanged envelope. Internal plan replies are intentionally consumed because
/// this public boundary exposes fire-and-forget entity messaging.
pub struct ShardingEnvelopeRouter<M>
where
    M: Send + 'static,
{
    region: ActorRef<ShardRegionMsg<M>>,
    shard_count: u64,
    route_reply_to: Option<ActorRef<RegionLocalRoutePlan<M>>>,
    delivery_reply_to: Option<ActorRef<ShardDeliverPlan<M>>>,
}

impl<M> ShardingEnvelopeRouter<M>
where
    M: Send + 'static,
{
    /// Creates an envelope router for `region` and `shard_count`.
    pub fn new(region: ActorRef<ShardRegionMsg<M>>, shard_count: u64) -> Self {
        Self {
            region,
            shard_count,
            route_reply_to: None,
            delivery_reply_to: None,
        }
    }

    /// Creates actor properties for an envelope router.
    pub fn props(region: ActorRef<ShardRegionMsg<M>>, shard_count: u64) -> Props<Self> {
        Props::new(move || Self::new(region, shard_count))
    }

    /// Returns the typed region command target.
    pub fn region(&self) -> &ActorRef<ShardRegionMsg<M>> {
        &self.region
    }

    /// Returns the shard count used for stable entity-id routing.
    pub fn shard_count(&self) -> u64 {
        self.shard_count
    }
}

impl<M> Actor for ShardingEnvelopeRouter<M>
where
    M: Send + 'static,
{
    type Msg = ShardingEnvelope<M>;

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
        let shard = shard_id_for(msg.entity_id(), self.shard_count).map_err(actor_error)?;
        let route_reply_to = self
            .route_reply_to
            .clone()
            .ok_or_else(|| ActorError::Message("sharding envelope router is not started".into()))?;
        let delivery_reply_to = self.delivery_reply_to.clone().ok_or_else(|| {
            ActorError::Message("sharding envelope router delivery sink is not started".into())
        })?;
        self.region
            .tell(ShardRegionMsg::RouteToLocalShard {
                shard,
                message: msg,
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
