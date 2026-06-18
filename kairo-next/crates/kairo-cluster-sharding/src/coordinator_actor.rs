use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::Duration;

use kairo_actor::{
    Actor, ActorError, ActorPath, ActorRef, ActorResult, AskError, Context, Props, Signal,
};
use kairo_serialization::ActorRefWireData;

use crate::coordinator_handoff::CoordinatorHandoff;
use crate::coordinator_store::{CoordinatorRememberStore, LocalCoordinatorRememberStoreProvider};
use crate::{
    BeginHandOffAck, CoordinatorEvent, CoordinatorRemoteRegions, CoordinatorRemoteReplyTarget,
    CoordinatorRuntime, CoordinatorState, GetShardHomePlan, HandoffRegionTarget, HandoffTransport,
    HandoffWorkerDone, LeastShardAllocationStrategy, RebalanceCompletionPlan, RebalancePlan,
    RegionId, RegionShutdownPlan, RememberCoordinatorStoreMsg, RememberCoordinatorStoreState,
    RememberCoordinatorUpdateDone, RememberedShards, ShardAllocationStrategy, ShardId,
    ShardStarted, ShardStopped, ShardingError, remote_region_id,
};

pub const REBALANCE_TIMER_KEY: &str = "sharding-coordinator-rebalance";

mod construction;
mod remember_store;

pub struct ShardCoordinatorActor<M = ()>
where
    M: Send + 'static,
{
    runtime: CoordinatorRuntime,
    strategy: Box<dyn ShardAllocationStrategy + Send>,
    rebalance_interval: Option<Duration>,
    remember_store: Option<CoordinatorRememberStore>,
    local_remember_store_provider: Option<LocalCoordinatorRememberStoreProvider>,
    waiting_for_remember_store_load: bool,
    handoff: Option<CoordinatorHandoff<M>>,
    remote_regions: CoordinatorRemoteRegions,
    region_watch_by_path: HashMap<ActorPath, RegionId>,
}

#[derive(Clone)]
pub enum ShardCoordinatorMsg<M = ()>
where
    M: Send + 'static,
{
    ApplyEvent {
        event: CoordinatorEvent,
        reply_to: Option<ActorRef<Result<CoordinatorStateSnapshot, ShardingError>>>,
    },
    SetAllRegionsRegistered {
        all_registered: bool,
    },
    SetPreparingForShutdown {
        preparing: bool,
    },
    MarkGracefulShutdown {
        region: RegionId,
    },
    UnmarkGracefulShutdown {
        region: RegionId,
    },
    MarkRegionTerminating {
        region: RegionId,
    },
    UnmarkRegionTerminating {
        region: RegionId,
    },
    GracefulShutdownReq {
        region: RegionId,
        reply_to: Option<ActorRef<RegionShutdownPlan>>,
    },
    RequestShardHome {
        requester: RegionId,
        shard: ShardId,
        reply_to: ActorRef<Result<GetShardHomePlan, ShardingError>>,
    },
    RegisterLocalRegion {
        target: HandoffRegionTarget<M>,
        reply_to: ActorRef<Result<CoordinatorStateSnapshot, ShardingError>>,
    },
    RegisterRemoteRegion {
        region: ActorRefWireData,
        target: Option<HandoffRegionTarget<M>>,
        reply: CoordinatorRemoteReplyTarget,
    },
    RemoteGracefulShutdownReq {
        region: ActorRefWireData,
    },
    RegionStopped {
        region: RegionId,
    },
    RemoteRegionStopped {
        region: ActorRefWireData,
    },
    RequestRemoteShardHome {
        requester: ActorRefWireData,
        shard: ShardId,
        reply: CoordinatorRemoteReplyTarget,
    },
    PlanRebalance {
        reply_to: ActorRef<Result<RebalancePlan, ShardingError>>,
    },
    CompleteRebalance {
        shard: ShardId,
        ok: bool,
        reply_to: ActorRef<Result<RebalanceCompletionPlan, ShardingError>>,
    },
    HandoffWorkerDone(HandoffWorkerDone),
    HostShardObserved {
        shard: ShardId,
    },
    RemoteHostShardObserved {
        region: ActorRefWireData,
        started: ShardStarted,
    },
    RemoteBeginHandOffAck {
        region: ActorRefWireData,
        ack: BeginHandOffAck,
    },
    RemoteShardStopped {
        region: ActorRefWireData,
        stopped: ShardStopped,
    },
    RebalanceTick {
        reply_to: Option<ActorRef<Result<RebalancePlan, ShardingError>>>,
    },
    RememberStoreLoadResult {
        result: Result<RememberedShards, AskError>,
    },
    RememberStoreUpdateResult {
        result: Result<RememberCoordinatorUpdateDone, AskError>,
    },
    StartRebalanceTimer {
        initial_delay: Duration,
        interval: Duration,
    },
    StopRebalanceTimer,
    GetState {
        reply_to: ActorRef<CoordinatorStateSnapshot>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoordinatorStateSnapshot {
    pub allocations: BTreeMap<RegionId, Vec<ShardId>>,
    pub proxies: BTreeSet<RegionId>,
    pub unallocated_shards: BTreeSet<ShardId>,
    pub rebalance_in_progress: BTreeMap<ShardId, Vec<RegionId>>,
    pub remember_entities: bool,
}

impl<M> Actor for ShardCoordinatorActor<M>
where
    M: Clone + Send + 'static,
{
    type Msg = ShardCoordinatorMsg<M>;

    fn started(&mut self, ctx: &mut Context<Self::Msg>) -> ActorResult {
        self.spawn_local_remember_store_if_needed(ctx)?;
        self.request_remember_store_load(ctx)?;
        if let Some(interval) = self.rebalance_interval {
            start_rebalance_timer(ctx, interval, interval);
        }
        Ok(())
    }

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        if self.waiting_for_remember_store_load {
            return self.receive_loading(ctx, msg);
        }

        match msg {
            ShardCoordinatorMsg::ApplyEvent { event, reply_to } => {
                let result = self
                    .runtime
                    .apply_event(event)
                    .map(|()| CoordinatorStateSnapshot::from(&self.runtime));
                reply_optional(reply_to, result);
            }
            ShardCoordinatorMsg::SetAllRegionsRegistered { all_registered } => {
                self.runtime.set_all_regions_registered(all_registered);
            }
            ShardCoordinatorMsg::SetPreparingForShutdown { preparing } => {
                self.runtime.set_preparing_for_shutdown(preparing);
                if preparing {
                    ctx.cancel_timer(REBALANCE_TIMER_KEY);
                } else if let Some(interval) = self.rebalance_interval {
                    start_rebalance_timer(ctx, interval, interval);
                }
            }
            ShardCoordinatorMsg::MarkGracefulShutdown { region } => {
                self.runtime.mark_graceful_shutdown(region);
            }
            ShardCoordinatorMsg::UnmarkGracefulShutdown { region } => {
                self.runtime.unmark_graceful_shutdown(&region);
            }
            ShardCoordinatorMsg::MarkRegionTerminating { region } => {
                self.runtime.mark_region_terminating(region);
            }
            ShardCoordinatorMsg::UnmarkRegionTerminating { region } => {
                self.runtime.unmark_region_terminating(&region);
            }
            ShardCoordinatorMsg::GracefulShutdownReq { region, reply_to } => {
                let plan = self.runtime.plan_region_shutdown(region);
                self.spawn_shutdown_workers(ctx, &plan)?;
                reply_optional(reply_to, plan);
            }
            ShardCoordinatorMsg::RequestShardHome {
                requester,
                shard,
                reply_to,
            } => {
                let result =
                    self.runtime
                        .request_shard_home(requester, shard, self.strategy.as_ref());
                self.persist_allocated_shard(ctx, &result)?;
                if let (Some(handoff), Ok(plan)) = (&self.handoff, &result) {
                    handoff.dispatch_host_shard(ctx, plan)?;
                }
                let _ = reply_to.tell(result);
            }
            ShardCoordinatorMsg::RegisterLocalRegion { target, reply_to } => {
                let result = self.register_local_region(ctx, target)?;
                let _ = reply_to.tell(result);
            }
            ShardCoordinatorMsg::RegisterRemoteRegion {
                region,
                target,
                reply,
            } => {
                self.register_remote_region(ctx, region, target, reply)?;
            }
            ShardCoordinatorMsg::RemoteGracefulShutdownReq { region } => {
                self.apply_remote_graceful_shutdown(ctx, region)?;
            }
            ShardCoordinatorMsg::RegionStopped { region } => {
                self.apply_region_stopped(region)?;
            }
            ShardCoordinatorMsg::RemoteRegionStopped { region } => {
                let region_id = self.remote_regions.register(region);
                self.apply_region_stopped(region_id)?;
            }
            ShardCoordinatorMsg::RequestRemoteShardHome {
                requester,
                shard,
                reply,
            } => {
                self.request_remote_shard_home(ctx, requester, shard, reply)?;
            }
            ShardCoordinatorMsg::PlanRebalance { reply_to } => {
                let result = self.runtime.plan_rebalance(self.strategy.as_ref());
                self.spawn_handoff_workers(ctx, &result)?;
                let _ = reply_to.tell(result);
            }
            ShardCoordinatorMsg::CompleteRebalance {
                shard,
                ok,
                reply_to,
            } => {
                let result = self.runtime.complete_rebalance(shard, ok);
                let _ = reply_to.tell(result);
            }
            ShardCoordinatorMsg::HandoffWorkerDone(done) => {
                self.apply_handoff_worker_done(ctx, done)?
            }
            ShardCoordinatorMsg::HostShardObserved { shard: _ } => {}
            ShardCoordinatorMsg::RemoteHostShardObserved { region, started: _ } => {
                self.remote_regions.register(region);
            }
            ShardCoordinatorMsg::RemoteBeginHandOffAck { region, ack } => {
                self.apply_remote_begin_handoff_ack(region, ack)?;
            }
            ShardCoordinatorMsg::RemoteShardStopped { region, stopped } => {
                self.apply_remote_shard_stopped(region, stopped)?;
            }
            ShardCoordinatorMsg::RebalanceTick { reply_to } => {
                let result = self.runtime.plan_rebalance(self.strategy.as_ref());
                self.spawn_handoff_workers(ctx, &result)?;
                reply_optional(reply_to, result);
            }
            ShardCoordinatorMsg::RememberStoreLoadResult { result } => {
                self.apply_remember_store_load(ctx, result)?;
            }
            ShardCoordinatorMsg::RememberStoreUpdateResult { result } => {
                if result.is_err() {
                    return self.stop_for_remember_store_failure(ctx);
                }
            }
            ShardCoordinatorMsg::StartRebalanceTimer {
                initial_delay,
                interval,
            } => {
                self.rebalance_interval = Some(interval);
                start_rebalance_timer(ctx, initial_delay, interval);
            }
            ShardCoordinatorMsg::StopRebalanceTimer => {
                self.rebalance_interval = None;
                ctx.cancel_timer(REBALANCE_TIMER_KEY);
            }
            ShardCoordinatorMsg::GetState { reply_to } => {
                let _ = reply_to.tell(CoordinatorStateSnapshot::from(&self.runtime));
            }
        }
        Ok(())
    }

    fn signal(&mut self, _ctx: &mut Context<Self::Msg>, signal: Signal) -> ActorResult {
        match signal {
            Signal::Terminated(actor) | Signal::ChildFailed { actor, .. } => {
                if self.apply_watched_region_terminated(actor.path())? {
                    Ok(())
                } else {
                    Err(ActorError::DeathPact {
                        actor: actor.path().to_string(),
                    })
                }
            }
            Signal::PreRestart | Signal::PostStop => Ok(()),
        }
    }
}

impl<M> ShardCoordinatorActor<M>
where
    M: Clone + Send + 'static,
{
    fn spawn_handoff_workers(
        &mut self,
        ctx: &Context<ShardCoordinatorMsg<M>>,
        result: &Result<RebalancePlan, ShardingError>,
    ) -> ActorResult {
        let (Some(handoff), Ok(RebalancePlan::Started { shards })) = (&mut self.handoff, result)
        else {
            return Ok(());
        };

        handoff.spawn_workers(ctx, shards)?;
        Ok(())
    }

    fn spawn_shutdown_workers(
        &mut self,
        ctx: &Context<ShardCoordinatorMsg<M>>,
        plan: &RegionShutdownPlan,
    ) -> ActorResult {
        let (Some(handoff), RegionShutdownPlan::Started { shards, .. }) = (&mut self.handoff, plan)
        else {
            return Ok(());
        };

        handoff.spawn_workers(ctx, shards)?;
        Ok(())
    }

    fn register_local_region(
        &mut self,
        ctx: &mut Context<ShardCoordinatorMsg<M>>,
        target: HandoffRegionTarget<M>,
    ) -> Result<Result<CoordinatorStateSnapshot, ShardingError>, ActorError> {
        let region = target.region().clone();
        self.watch_local_region(ctx, &target)?;
        if !self.runtime.state().allocations().contains_region(&region)
            && let Err(error) = self
                .runtime
                .apply_event(CoordinatorEvent::ShardRegionRegistered { region })
        {
            return Ok(Err(error));
        }
        if let Some(handoff) = &mut self.handoff {
            handoff.register_region_target(target);
        }
        self.allocate_remembered_shard_homes(ctx)?;
        Ok(Ok(CoordinatorStateSnapshot::from(&self.runtime)))
    }

    fn watch_local_region(
        &mut self,
        ctx: &mut Context<ShardCoordinatorMsg<M>>,
        target: &HandoffRegionTarget<M>,
    ) -> ActorResult {
        let Some(region_ref) = target.watch_ref() else {
            return Ok(());
        };
        if region_ref.path() == ctx.myself().path() {
            return Ok(());
        }
        if self.region_watch_by_path.get(region_ref.path()) == Some(target.region()) {
            return Ok(());
        }

        ctx.watch(region_ref)?;
        self.region_watch_by_path
            .insert(region_ref.path().clone(), target.region().clone());
        Ok(())
    }

    fn apply_watched_region_terminated(&mut self, path: &ActorPath) -> Result<bool, ActorError> {
        let Some(region) = self.region_watch_by_path.remove(path) else {
            return Ok(false);
        };

        self.apply_region_stopped(region)?;
        Ok(true)
    }

    fn register_remote_region(
        &mut self,
        ctx: &Context<ShardCoordinatorMsg<M>>,
        region: ActorRefWireData,
        target: Option<HandoffRegionTarget<M>>,
        reply: CoordinatorRemoteReplyTarget,
    ) -> ActorResult {
        let region_id = self.remote_regions.register(region.clone());
        if !self
            .runtime
            .state()
            .allocations()
            .contains_region(&region_id)
        {
            self.runtime
                .apply_event(CoordinatorEvent::ShardRegionRegistered { region: region_id })
                .map_err(|error| ActorError::Message(error.to_string()))?;
        }
        if let (Some(handoff), Some(target)) = (&mut self.handoff, target) {
            handoff.register_region_target(target);
        }
        reply
            .send_register_ack(region)
            .map_err(|error| ActorError::Message(error.to_string()))?;
        self.allocate_remembered_shard_homes(ctx)
    }

    fn apply_remote_graceful_shutdown(
        &mut self,
        ctx: &Context<ShardCoordinatorMsg<M>>,
        region: ActorRefWireData,
    ) -> ActorResult {
        let region_id = self.remote_regions.register(region);
        let plan = self.runtime.plan_region_shutdown(region_id);
        self.spawn_shutdown_workers(ctx, &plan)
    }

    fn apply_region_stopped(&mut self, region: RegionId) -> ActorResult {
        self.runtime.unmark_graceful_shutdown(&region);
        self.runtime.unmark_region_terminating(&region);
        if let Some(handoff) = &mut self.handoff {
            handoff.remove_region_target(&region);
        }
        if !self.runtime.state().allocations().contains_region(&region) {
            return Ok(());
        }
        self.runtime
            .apply_event(CoordinatorEvent::ShardRegionTerminated { region })
            .map_err(|error| ActorError::Message(error.to_string()))
    }

    fn request_remote_shard_home(
        &mut self,
        ctx: &Context<ShardCoordinatorMsg<M>>,
        requester: ActorRefWireData,
        shard: ShardId,
        reply: CoordinatorRemoteReplyTarget,
    ) -> ActorResult {
        let requester_id = remote_region_id(&requester);
        let result = self
            .runtime
            .request_shard_home(requester_id, shard, self.strategy.as_ref());
        self.persist_allocated_shard(ctx, &result)?;
        if let (Some(handoff), Ok(plan)) = (&self.handoff, &result) {
            handoff.dispatch_host_shard(ctx, plan)?;
        }
        let plan = result.map_err(|error| ActorError::Message(error.to_string()))?;
        self.reply_remote_shard_home(requester, reply, plan)
    }

    fn reply_remote_shard_home(
        &self,
        requester: ActorRefWireData,
        reply: CoordinatorRemoteReplyTarget,
        plan: GetShardHomePlan,
    ) -> ActorResult {
        let (shard, region) = match plan {
            GetShardHomePlan::Reply { shard, region } => (shard, region),
            GetShardHomePlan::Allocated {
                event: CoordinatorEvent::ShardHomeAllocated { shard, region },
                ..
            } => (shard, region),
            GetShardHomePlan::Allocated { .. }
            | GetShardHomePlan::Deferred { .. }
            | GetShardHomePlan::Ignored { .. } => return Ok(()),
        };
        let Some(home_region) = self.remote_regions.wire_ref(&region).cloned() else {
            return Ok(());
        };
        reply
            .send_shard_home(shard, requester, home_region)
            .map_err(|error| ActorError::Message(error.to_string()))
    }

    fn apply_handoff_worker_done(
        &mut self,
        ctx: &Context<ShardCoordinatorMsg<M>>,
        done: HandoffWorkerDone,
    ) -> ActorResult {
        if let Some(handoff) = &mut self.handoff {
            handoff.remove_worker(&done.shard);
        }
        let completion = self
            .runtime
            .complete_rebalance(done.shard, done.ok)
            .map_err(|error| ActorError::Message(error.to_string()))?;
        self.reallocate_completed_rebalance(ctx, completion)
    }

    fn apply_remote_begin_handoff_ack(
        &mut self,
        region: ActorRefWireData,
        ack: BeginHandOffAck,
    ) -> ActorResult {
        let region_id = self.remote_regions.register(region);
        if let Some(handoff) = &self.handoff {
            handoff.forward_remote_begin_handoff_ack(region_id, ack)?;
        }
        Ok(())
    }

    fn apply_remote_shard_stopped(
        &mut self,
        region: ActorRefWireData,
        stopped: ShardStopped,
    ) -> ActorResult {
        let region_id = self.remote_regions.register(region);
        if let Some(handoff) = &self.handoff {
            handoff.forward_remote_shard_stopped(region_id, stopped)?;
        }
        Ok(())
    }

    fn reallocate_completed_rebalance(
        &mut self,
        ctx: &Context<ShardCoordinatorMsg<M>>,
        completion: RebalanceCompletionPlan,
    ) -> ActorResult {
        let RebalanceCompletionPlan::Deallocated {
            retry_get_shard_home,
            pending_requesters,
            ..
        } = completion
        else {
            return Ok(());
        };

        let requester = pending_requesters
            .into_iter()
            .next()
            .unwrap_or_else(|| "coordinator".to_string());
        let result = self.runtime.request_shard_home(
            requester,
            retry_get_shard_home.shard_id,
            self.strategy.as_ref(),
        );
        self.persist_allocated_shard(ctx, &result)?;
        let plan = result.map_err(|error| ActorError::Message(error.to_string()))?;
        if let Some(handoff) = &self.handoff {
            handoff.dispatch_host_shard(ctx, &plan)?;
        }
        Ok(())
    }
}

impl From<&CoordinatorRuntime> for CoordinatorStateSnapshot {
    fn from(runtime: &CoordinatorRuntime) -> Self {
        let mut snapshot = Self::from(runtime.state());
        snapshot.rebalance_in_progress = runtime
            .rebalance_in_progress()
            .iter()
            .map(|(shard, requesters)| (shard.clone(), requesters.iter().cloned().collect()))
            .collect();
        snapshot
    }
}

impl From<&CoordinatorState> for CoordinatorStateSnapshot {
    fn from(state: &CoordinatorState) -> Self {
        let allocations = state
            .allocations()
            .regions()
            .map(|region| {
                let shards = state
                    .allocations()
                    .shards_for(region)
                    .map(|shards| shards.to_vec())
                    .unwrap_or_default();
                (region.clone(), shards)
            })
            .collect();

        Self {
            allocations,
            proxies: state.proxies().clone(),
            unallocated_shards: state.unallocated_shards().clone(),
            rebalance_in_progress: BTreeMap::new(),
            remember_entities: state.remember_entities(),
        }
    }
}

fn start_rebalance_timer<M>(
    ctx: &mut Context<ShardCoordinatorMsg<M>>,
    initial_delay: Duration,
    interval: Duration,
) where
    M: Clone + Send + 'static,
{
    ctx.start_timer_with_fixed_delay(
        REBALANCE_TIMER_KEY,
        initial_delay,
        interval,
        ShardCoordinatorMsg::RebalanceTick { reply_to: None },
    );
}

fn reply_optional<M>(reply_to: Option<ActorRef<M>>, message: M)
where
    M: Send + 'static,
{
    if let Some(reply_to) = reply_to {
        let _ = reply_to.tell(message);
    }
}
