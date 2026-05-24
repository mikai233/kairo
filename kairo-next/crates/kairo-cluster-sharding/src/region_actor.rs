use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, Context, Props};

use crate::region_protocol::{
    RegionBufferedReplayPlan, RegionLocalHandOffCompletionFailure,
    RegionLocalHandOffCompletionPlan, RegionLocalHandOffPlan, RegionLocalRoutePlan, ShardRegionMsg,
    ShardRegionSnapshot,
};
use crate::region_shards::LocalShardSpawner;
use crate::{
    EntityId, HostShardPlan, RegionId, RegionRoutePlan, ShardDeliverPlan, ShardHandOffPlan,
    ShardHomePlan, ShardId, ShardMsg, ShardRegionRuntime, ShardingEnvelope,
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
            ShardRegionMsg::HostShardAndReplayBuffered {
                shard,
                reply_to,
                delivery_reply_to,
            } => {
                let plan = self.host_shard_and_replay_buffered(ctx, shard, delivery_reply_to)?;
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
            ShardRegionMsg::HandOffToLocalShard {
                shard,
                stop_message,
                region_reply_to,
                shard_reply_to,
            } => {
                let plan = self.runtime.handoff(shard);
                let local_plan =
                    self.dispatch_local_handoff_plan(plan, stop_message, shard_reply_to)?;
                let _ = region_reply_to.tell(local_plan);
            }
            ShardRegionMsg::CompleteLocalShardHandOff {
                shard,
                timeout,
                reply_to,
            } => {
                self.complete_local_shard_handoff(ctx, shard, timeout, reply_to)?;
            }
            ShardRegionMsg::LocalShardHandOffStopperResult {
                shard,
                result,
                reply_to,
            } => {
                let plan = self.apply_local_shard_handoff_stopper_result(ctx, shard, result)?;
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

    fn host_shard_and_replay_buffered(
        &mut self,
        ctx: &Context<ShardRegionMsg<M>>,
        shard: ShardId,
        delivery_reply_to: ActorRef<ShardDeliverPlan<M>>,
    ) -> Result<RegionBufferedReplayPlan, ActorError> {
        if self.local_shard_spawner.is_none() {
            return Ok(RegionBufferedReplayPlan::MissingLocalShardSpawner { shard });
        }

        match self.runtime.host_shard(shard) {
            HostShardPlan::IgnoredGracefulShutdown { shard } => {
                Ok(RegionBufferedReplayPlan::IgnoredGracefulShutdown { shard })
            }
            HostShardPlan::AlreadyStarted {
                shard,
                started,
                buffered,
            } => {
                let replayed =
                    self.replay_buffered_to_local_shard(&shard, buffered, delivery_reply_to)?;
                Ok(RegionBufferedReplayPlan::Replayed {
                    shard,
                    started,
                    replayed,
                })
            }
            HostShardPlan::StartLocalShard { shard, command: _ } => {
                self.spawn_local_shard(ctx, &shard)?;
                let started = self.runtime.mark_shard_started(shard.clone());
                let replayed = self.replay_buffered_to_local_shard(
                    &shard,
                    started.buffered,
                    delivery_reply_to,
                )?;
                Ok(RegionBufferedReplayPlan::Replayed {
                    shard,
                    started: started.started,
                    replayed,
                })
            }
        }
    }

    fn replay_buffered_to_local_shard(
        &self,
        shard: &ShardId,
        buffered: Vec<ShardingEnvelope<M>>,
        delivery_reply_to: ActorRef<ShardDeliverPlan<M>>,
    ) -> Result<usize, ActorError> {
        let Some(shard_ref) = self.local_shards.get(shard) else {
            return Err(ActorError::Message(format!(
                "local shard `{shard}` is not available for buffered replay"
            )));
        };
        let replayed = buffered.len();
        for message in buffered {
            shard_ref
                .tell(ShardMsg::Deliver {
                    message,
                    reply_to: delivery_reply_to.clone(),
                })
                .map_err(|error| ActorError::Message(error.reason().to_string()))?;
        }
        Ok(replayed)
    }

    fn dispatch_local_handoff_plan(
        &self,
        plan: crate::HandOffPlan,
        stop_message: M,
        shard_reply_to: ActorRef<ShardHandOffPlan<M>>,
    ) -> Result<RegionLocalHandOffPlan, ActorError> {
        match plan {
            crate::HandOffPlan::ForwardToLocalShard {
                shard,
                command,
                dropped_buffered,
            } => {
                let Some(shard_ref) = self.local_shards.get(&shard) else {
                    return Ok(RegionLocalHandOffPlan::MissingLocalShard {
                        shard,
                        command,
                        dropped_buffered,
                    });
                };
                shard_ref
                    .tell(ShardMsg::HandOff {
                        stop_message,
                        reply_to: shard_reply_to,
                    })
                    .map_err(|error| ActorError::Message(error.reason().to_string()))?;
                Ok(RegionLocalHandOffPlan::ForwardedToLocalShard {
                    shard,
                    command,
                    dropped_buffered,
                })
            }
            crate::HandOffPlan::ReplyShardStopped {
                shard,
                stopped,
                dropped_buffered,
            } => Ok(RegionLocalHandOffPlan::ReplyShardStopped {
                shard,
                stopped,
                dropped_buffered,
            }),
        }
    }

    fn complete_local_shard_handoff(
        &self,
        ctx: &Context<ShardRegionMsg<M>>,
        shard: ShardId,
        timeout: Duration,
        reply_to: ActorRef<RegionLocalHandOffCompletionPlan>,
    ) -> Result<(), ActorError> {
        let Some(shard_ref) = self.local_shards.get(&shard).cloned() else {
            let _ = reply_to.tell(RegionLocalHandOffCompletionPlan::Failed {
                shard,
                reason: RegionLocalHandOffCompletionFailure::MissingLocalShard,
            });
            return Ok(());
        };

        ctx.ask(
            shard_ref,
            timeout,
            |reply_to| ShardMsg::HandOffStopperTerminated { reply_to },
            move |result| ShardRegionMsg::LocalShardHandOffStopperResult {
                shard,
                result,
                reply_to,
            },
        )
    }

    fn apply_local_shard_handoff_stopper_result(
        &mut self,
        ctx: &mut Context<ShardRegionMsg<M>>,
        shard: ShardId,
        result: kairo_actor::AskResult<bool>,
    ) -> Result<RegionLocalHandOffCompletionPlan, ActorError> {
        match result {
            Ok(true) => {
                if let Some(shard_ref) = self.local_shards.get(&shard).cloned() {
                    ctx.stop(shard_ref)?;
                }
                self.runtime.mark_shard_stopped(&shard);
                self.local_shards.remove(&shard);
                Ok(RegionLocalHandOffCompletionPlan::Completed {
                    stopped: crate::ShardStopped {
                        shard_id: shard.clone(),
                    },
                    shard,
                })
            }
            Ok(false) => Ok(RegionLocalHandOffCompletionPlan::Failed {
                shard,
                reason: RegionLocalHandOffCompletionFailure::StopperNotInProgress,
            }),
            Err(kairo_actor::AskError::Timeout { timeout }) => {
                Ok(RegionLocalHandOffCompletionPlan::Failed {
                    shard,
                    reason: RegionLocalHandOffCompletionFailure::StopperTimeout { timeout },
                })
            }
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

fn reply_optional<M>(reply_to: Option<ActorRef<M>>, message: M)
where
    M: Send + 'static,
{
    if let Some(reply_to) = reply_to {
        let _ = reply_to.tell(message);
    }
}
