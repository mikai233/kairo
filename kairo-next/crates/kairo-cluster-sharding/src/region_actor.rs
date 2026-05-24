use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, Context, Props};

use crate::region_shards::LocalShardSpawner;
use crate::{
    BeginHandOffPlan, EntityId, HandOffPlan, HostShardPlan, RegionId, RegionRoutePlan,
    ShardDeliverPlan, ShardHomePlan, ShardId, ShardMsg, ShardRegionRuntime, ShardStartedPlan,
    ShardingEnvelope, ShardingError,
};

pub struct ShardRegionActor<M> {
    runtime: ShardRegionRuntime<M>,
    local_shard_spawner: Option<LocalShardSpawner>,
    local_shards: BTreeMap<ShardId, ActorRef<ShardMsg<M>>>,
}

impl<M> ShardRegionActor<M> {
    pub fn new(self_region: impl Into<RegionId>, buffer_capacity: usize) -> Self {
        Self {
            runtime: ShardRegionRuntime::new(self_region, buffer_capacity),
            local_shard_spawner: None,
            local_shards: BTreeMap::new(),
        }
    }

    pub fn new_with_local_shards(
        self_region: impl Into<RegionId>,
        region_buffer_capacity: usize,
        shard_buffer_capacity: usize,
    ) -> Self {
        Self {
            runtime: ShardRegionRuntime::new(self_region, region_buffer_capacity),
            local_shard_spawner: Some(LocalShardSpawner::plain(shard_buffer_capacity)),
            local_shards: BTreeMap::new(),
        }
    }

    pub fn new_with_local_remember_store_shards(
        self_region: impl Into<RegionId>,
        type_name: impl Into<String>,
        region_buffer_capacity: usize,
        shard_buffer_capacity: usize,
        remembered_entities_by_shard: BTreeMap<ShardId, BTreeSet<EntityId>>,
        timeout: Duration,
    ) -> Self {
        Self {
            runtime: ShardRegionRuntime::new(self_region, region_buffer_capacity),
            local_shard_spawner: Some(LocalShardSpawner::with_local_remember_stores(
                type_name,
                shard_buffer_capacity,
                remembered_entities_by_shard,
                timeout,
            )),
            local_shards: BTreeMap::new(),
        }
    }

    pub fn props(self_region: impl Into<RegionId>, buffer_capacity: usize) -> Props<Self>
    where
        M: Send + 'static,
    {
        let self_region = self_region.into();
        Props::new(move || Self::new(self_region, buffer_capacity))
    }

    pub fn props_with_local_shards(
        self_region: impl Into<RegionId>,
        region_buffer_capacity: usize,
        shard_buffer_capacity: usize,
    ) -> Props<Self>
    where
        M: Send + 'static,
    {
        let self_region = self_region.into();
        Props::new(move || {
            Self::new_with_local_shards(self_region, region_buffer_capacity, shard_buffer_capacity)
        })
    }

    pub fn props_with_local_remember_store_shards(
        self_region: impl Into<RegionId>,
        type_name: impl Into<String>,
        region_buffer_capacity: usize,
        shard_buffer_capacity: usize,
        remembered_entities_by_shard: BTreeMap<ShardId, BTreeSet<EntityId>>,
        timeout: Duration,
    ) -> Props<Self>
    where
        M: Send + 'static,
    {
        let self_region = self_region.into();
        let type_name = type_name.into();
        Props::new(move || {
            Self::new_with_local_remember_store_shards(
                self_region,
                type_name,
                region_buffer_capacity,
                shard_buffer_capacity,
                remembered_entities_by_shard,
                timeout,
            )
        })
    }

    pub fn runtime(&self) -> &ShardRegionRuntime<M> {
        &self.runtime
    }
}

pub enum ShardRegionMsg<M> {
    Route {
        shard: ShardId,
        message: ShardingEnvelope<M>,
        reply_to: ActorRef<RegionRoutePlan<M>>,
    },
    RouteToLocalShard {
        shard: ShardId,
        message: ShardingEnvelope<M>,
        route_reply_to: ActorRef<RegionLocalRoutePlan<M>>,
        delivery_reply_to: ActorRef<ShardDeliverPlan<M>>,
    },
    HostShard {
        shard: ShardId,
        reply_to: ActorRef<HostShardPlan<M>>,
    },
    RecordShardHome {
        shard: ShardId,
        region: RegionId,
        reply_to: ActorRef<Result<ShardHomePlan<M>, ShardingError>>,
    },
    MarkShardStarted {
        shard: ShardId,
        reply_to: ActorRef<ShardStartedPlan<M>>,
    },
    BeginHandOff {
        shard: ShardId,
        reply_to: ActorRef<BeginHandOffPlan>,
    },
    HandOff {
        shard: ShardId,
        reply_to: ActorRef<HandOffPlan>,
    },
    MarkShardStopped {
        shard: ShardId,
        reply_to: Option<ActorRef<ShardRegionSnapshot>>,
    },
    SetGracefulShutdown {
        in_progress: bool,
    },
    SetPreparingForShutdown {
        preparing: bool,
    },
    GetState {
        reply_to: ActorRef<ShardRegionSnapshot>,
    },
    GetLocalShard {
        shard: ShardId,
        reply_to: ActorRef<Option<ActorRef<ShardMsg<M>>>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegionLocalRoutePlan<M> {
    DeliveredToLocalShard {
        shard: ShardId,
    },
    MissingLocalShard {
        shard: ShardId,
        message: ShardingEnvelope<M>,
    },
    Forward {
        shard: ShardId,
        region: RegionId,
        message: ShardingEnvelope<M>,
    },
    Buffered {
        shard: ShardId,
        request: Option<crate::GetShardHome>,
    },
    Dropped {
        shard: Option<ShardId>,
        reason: crate::RegionDropReason,
        message: ShardingEnvelope<M>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardRegionSnapshot {
    pub self_region: RegionId,
    pub local_shards: BTreeSet<ShardId>,
    pub starting_shards: BTreeSet<ShardId>,
    pub handing_off_shards: BTreeSet<ShardId>,
    pub total_buffered: usize,
}

impl<M> Actor for ShardRegionActor<M>
where
    M: Send + 'static,
{
    type Msg = ShardRegionMsg<M>;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            ShardRegionMsg::Route {
                shard,
                message,
                reply_to,
            } => {
                let plan = self.runtime.route(shard, message);
                let _ = reply_to.tell(plan);
            }
            ShardRegionMsg::RouteToLocalShard {
                shard,
                message,
                route_reply_to,
                delivery_reply_to,
            } => {
                let plan = self.runtime.route(shard, message);
                let local_plan = self.dispatch_local_route_plan(plan, delivery_reply_to)?;
                let _ = route_reply_to.tell(local_plan);
            }
            ShardRegionMsg::HostShard { shard, reply_to } => {
                let plan = self.runtime.host_shard(shard);
                let plan = self.maybe_start_local_shard_from_host_plan(ctx, plan)?;
                let _ = reply_to.tell(plan);
            }
            ShardRegionMsg::RecordShardHome {
                shard,
                region,
                reply_to,
            } => {
                let plan = self.runtime.record_shard_home(shard, region);
                let plan = match plan {
                    Ok(plan) => Ok(self.maybe_start_local_shard_from_home_plan(ctx, plan)?),
                    Err(error) => Err(error),
                };
                let _ = reply_to.tell(plan);
            }
            ShardRegionMsg::MarkShardStarted { shard, reply_to } => {
                let plan = self.runtime.mark_shard_started(shard);
                let _ = reply_to.tell(plan);
            }
            ShardRegionMsg::BeginHandOff { shard, reply_to } => {
                let plan = self.runtime.begin_handoff(shard);
                let _ = reply_to.tell(plan);
            }
            ShardRegionMsg::HandOff { shard, reply_to } => {
                let plan = self.runtime.handoff(shard);
                let _ = reply_to.tell(plan);
            }
            ShardRegionMsg::MarkShardStopped { shard, reply_to } => {
                self.runtime.mark_shard_stopped(&shard);
                self.local_shards.remove(&shard);
                reply_optional(reply_to, ShardRegionSnapshot::from(&self.runtime));
            }
            ShardRegionMsg::SetGracefulShutdown { in_progress } => {
                self.runtime.set_graceful_shutdown_in_progress(in_progress);
            }
            ShardRegionMsg::SetPreparingForShutdown { preparing } => {
                self.runtime.set_preparing_for_shutdown(preparing);
            }
            ShardRegionMsg::GetState { reply_to } => {
                let _ = reply_to.tell(ShardRegionSnapshot::from(&self.runtime));
            }
            ShardRegionMsg::GetLocalShard { shard, reply_to } => {
                let _ = reply_to.tell(self.local_shards.get(&shard).cloned());
            }
        }
        Ok(())
    }
}

impl<M> ShardRegionActor<M>
where
    M: Send + 'static,
{
    fn maybe_start_local_shard_from_host_plan(
        &mut self,
        ctx: &Context<ShardRegionMsg<M>>,
        plan: HostShardPlan<M>,
    ) -> Result<HostShardPlan<M>, kairo_actor::ActorError> {
        let HostShardPlan::StartLocalShard { shard, command } = plan else {
            return Ok(plan);
        };
        if self.local_shard_spawner.is_none() {
            return Ok(HostShardPlan::StartLocalShard { shard, command });
        }

        self.spawn_local_shard(ctx, &shard)?;
        let started = self.runtime.mark_shard_started(shard.clone());
        Ok(HostShardPlan::AlreadyStarted {
            shard,
            started: started.started,
            buffered: started.buffered,
        })
    }

    fn dispatch_local_route_plan(
        &self,
        plan: RegionRoutePlan<M>,
        delivery_reply_to: ActorRef<ShardDeliverPlan<M>>,
    ) -> Result<RegionLocalRoutePlan<M>, ActorError> {
        match plan {
            RegionRoutePlan::DeliverLocal { shard, message } => {
                let Some(shard_ref) = self.local_shards.get(&shard) else {
                    return Ok(RegionLocalRoutePlan::MissingLocalShard { shard, message });
                };
                shard_ref
                    .tell(ShardMsg::Deliver {
                        message,
                        reply_to: delivery_reply_to,
                    })
                    .map_err(|error| ActorError::Message(error.reason().to_string()))?;
                Ok(RegionLocalRoutePlan::DeliveredToLocalShard { shard })
            }
            RegionRoutePlan::Forward {
                shard,
                region,
                message,
            } => Ok(RegionLocalRoutePlan::Forward {
                shard,
                region,
                message,
            }),
            RegionRoutePlan::Buffered { shard, request } => {
                Ok(RegionLocalRoutePlan::Buffered { shard, request })
            }
            RegionRoutePlan::Dropped {
                shard,
                reason,
                message,
            } => Ok(RegionLocalRoutePlan::Dropped {
                shard,
                reason,
                message,
            }),
        }
    }

    fn maybe_start_local_shard_from_home_plan(
        &mut self,
        ctx: &Context<ShardRegionMsg<M>>,
        plan: ShardHomePlan<M>,
    ) -> Result<ShardHomePlan<M>, kairo_actor::ActorError> {
        let ShardHomePlan::StartLocalShard { shard, command } = plan else {
            return Ok(plan);
        };
        if self.local_shard_spawner.is_none() {
            return Ok(ShardHomePlan::StartLocalShard { shard, command });
        }

        self.spawn_local_shard(ctx, &shard)?;
        let started = self.runtime.mark_shard_started(shard.clone());
        Ok(ShardHomePlan::DeliverLocal {
            shard,
            buffered: started.buffered,
        })
    }

    fn spawn_local_shard(
        &mut self,
        ctx: &Context<ShardRegionMsg<M>>,
        shard: &ShardId,
    ) -> Result<(), kairo_actor::ActorError> {
        if self.local_shards.contains_key(shard) {
            return Ok(());
        }
        let Some(spawner) = &self.local_shard_spawner else {
            return Ok(());
        };
        let shard_ref = spawner.spawn(ctx, shard)?;
        self.local_shards.insert(shard.clone(), shard_ref);
        Ok(())
    }
}

impl<M> From<&ShardRegionRuntime<M>> for ShardRegionSnapshot {
    fn from(runtime: &ShardRegionRuntime<M>) -> Self {
        Self {
            self_region: runtime.self_region().clone(),
            local_shards: runtime.local_shards().clone(),
            starting_shards: runtime.starting_shards().clone(),
            handing_off_shards: runtime.handing_off_shards().clone(),
            total_buffered: runtime.total_buffered_count(),
        }
    }
}

fn reply_optional<M>(reply_to: Option<ActorRef<M>>, message: M)
where
    M: Send + 'static,
{
    if let Some(reply_to) = reply_to {
        let _ = reply_to.tell(message);
    }
}
