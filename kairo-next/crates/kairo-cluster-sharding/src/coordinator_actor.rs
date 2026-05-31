use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, AskError, Context, Props};
use kairo_serialization::ActorRefWireData;

use crate::coordinator_handoff::CoordinatorHandoff;
use crate::coordinator_store::{CoordinatorRememberStore, LocalCoordinatorRememberStoreProvider};
use crate::{
    CoordinatorEvent, CoordinatorRemoteRegions, CoordinatorRemoteReplyTarget, CoordinatorRuntime,
    CoordinatorState, GetShardHomePlan, HandoffRegionTarget, HandoffTransport, HandoffWorkerDone,
    LeastShardAllocationStrategy, RebalanceCompletionPlan, RebalancePlan, RegionId,
    RememberCoordinatorStoreMsg, RememberCoordinatorStoreState, RememberCoordinatorUpdateDone,
    RememberedShards, ShardAllocationStrategy, ShardId, ShardingError, remote_region_id,
};

pub const REBALANCE_TIMER_KEY: &str = "sharding-coordinator-rebalance";

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
}

impl ShardCoordinatorActor<()> {
    pub fn new(
        state: CoordinatorState,
        strategy: impl ShardAllocationStrategy + Send + 'static,
    ) -> Self {
        Self {
            runtime: CoordinatorRuntime::new(state),
            strategy: Box::new(strategy),
            rebalance_interval: None,
            remember_store: None,
            local_remember_store_provider: None,
            waiting_for_remember_store_load: false,
            handoff: None,
            remote_regions: CoordinatorRemoteRegions::new(),
        }
    }

    pub fn with_rebalance_interval(
        state: CoordinatorState,
        strategy: impl ShardAllocationStrategy + Send + 'static,
        interval: Duration,
    ) -> Self {
        Self {
            runtime: CoordinatorRuntime::new(state),
            strategy: Box::new(strategy),
            rebalance_interval: Some(interval),
            remember_store: None,
            local_remember_store_provider: None,
            waiting_for_remember_store_load: false,
            handoff: None,
            remote_regions: CoordinatorRemoteRegions::new(),
        }
    }

    pub fn with_remember_store(
        state: CoordinatorState,
        strategy: impl ShardAllocationStrategy + Send + 'static,
        remember_store: ActorRef<RememberCoordinatorStoreMsg>,
        timeout: Duration,
    ) -> Self {
        Self {
            runtime: CoordinatorRuntime::new(state.with_remember_entities(true)),
            strategy: Box::new(strategy),
            rebalance_interval: None,
            remember_store: Some(CoordinatorRememberStore::new(remember_store, timeout)),
            local_remember_store_provider: None,
            waiting_for_remember_store_load: true,
            handoff: None,
            remote_regions: CoordinatorRemoteRegions::new(),
        }
    }

    pub fn with_local_remember_store(
        state: CoordinatorState,
        strategy: impl ShardAllocationStrategy + Send + 'static,
        store_state: RememberCoordinatorStoreState,
        timeout: Duration,
    ) -> Self {
        Self {
            runtime: CoordinatorRuntime::new(state.with_remember_entities(true)),
            strategy: Box::new(strategy),
            rebalance_interval: None,
            remember_store: None,
            local_remember_store_provider: Some(LocalCoordinatorRememberStoreProvider::new(
                store_state,
                timeout,
            )),
            waiting_for_remember_store_load: true,
            handoff: None,
            remote_regions: CoordinatorRemoteRegions::new(),
        }
    }

    pub fn with_least_shard_strategy(state: CoordinatorState) -> Self {
        Self::new(state, LeastShardAllocationStrategy::default())
    }

    pub fn props(
        state: CoordinatorState,
        strategy: impl ShardAllocationStrategy + Send + 'static,
    ) -> Props<Self> {
        Props::new(move || Self::new(state, strategy))
    }

    pub fn props_with_rebalance_interval(
        state: CoordinatorState,
        strategy: impl ShardAllocationStrategy + Send + 'static,
        interval: Duration,
    ) -> Props<Self> {
        Props::new(move || Self::with_rebalance_interval(state, strategy, interval))
    }

    pub fn props_with_least_shard_strategy(state: CoordinatorState) -> Props<Self> {
        Props::new(move || Self::with_least_shard_strategy(state))
    }

    pub fn props_with_remember_store(
        state: CoordinatorState,
        strategy: impl ShardAllocationStrategy + Send + 'static,
        remember_store: ActorRef<RememberCoordinatorStoreMsg>,
        timeout: Duration,
        stash_capacity: usize,
    ) -> Props<Self> {
        Props::new(move || Self::with_remember_store(state, strategy, remember_store, timeout))
            .with_stash_capacity(stash_capacity)
    }

    pub fn props_with_local_remember_store(
        state: CoordinatorState,
        strategy: impl ShardAllocationStrategy + Send + 'static,
        store_state: RememberCoordinatorStoreState,
        timeout: Duration,
        stash_capacity: usize,
    ) -> Props<Self> {
        Props::new(move || Self::with_local_remember_store(state, strategy, store_state, timeout))
            .with_stash_capacity(stash_capacity)
    }
}

impl<M> ShardCoordinatorActor<M>
where
    M: Clone + Send + 'static,
{
    pub fn with_handoff(
        state: CoordinatorState,
        strategy: impl ShardAllocationStrategy + Send + 'static,
        stop_message: M,
        handoff_timeout: Duration,
        transport: HandoffTransport<M>,
    ) -> Self {
        Self {
            runtime: CoordinatorRuntime::new(state),
            strategy: Box::new(strategy),
            rebalance_interval: None,
            remember_store: None,
            local_remember_store_provider: None,
            waiting_for_remember_store_load: false,
            handoff: Some(CoordinatorHandoff::new(
                stop_message,
                handoff_timeout,
                transport,
            )),
            remote_regions: CoordinatorRemoteRegions::new(),
        }
    }

    pub fn props_with_handoff(
        state: CoordinatorState,
        strategy: impl ShardAllocationStrategy + Send + 'static,
        stop_message: M,
        handoff_timeout: Duration,
        transport: HandoffTransport<M>,
    ) -> Props<Self> {
        Props::new(move || {
            Self::with_handoff(state, strategy, stop_message, handoff_timeout, transport)
        })
    }

    pub fn runtime(&self) -> &CoordinatorRuntime {
        &self.runtime
    }
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
        reply: CoordinatorRemoteReplyTarget,
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
            ShardCoordinatorMsg::RequestShardHome {
                requester,
                shard,
                reply_to,
            } => {
                let result =
                    self.runtime
                        .request_shard_home(requester, shard, self.strategy.as_ref());
                self.persist_allocated_shard(ctx, &result)?;
                let _ = reply_to.tell(result);
            }
            ShardCoordinatorMsg::RegisterLocalRegion { target, reply_to } => {
                let result = self.register_local_region(ctx, target)?;
                let _ = reply_to.tell(result);
            }
            ShardCoordinatorMsg::RegisterRemoteRegion { region, reply } => {
                self.register_remote_region(ctx, region, reply)?;
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
}

impl<M> ShardCoordinatorActor<M>
where
    M: Clone + Send + 'static,
{
    fn receive_loading(
        &mut self,
        ctx: &mut Context<ShardCoordinatorMsg<M>>,
        msg: ShardCoordinatorMsg<M>,
    ) -> ActorResult {
        match msg {
            ShardCoordinatorMsg::RememberStoreLoadResult { result } => {
                self.apply_remember_store_load(ctx, result)?;
                ctx.unstash_all()
            }
            other => ctx.stash(other),
        }
    }

    fn spawn_local_remember_store_if_needed(
        &mut self,
        ctx: &Context<ShardCoordinatorMsg<M>>,
    ) -> ActorResult {
        if self.remember_store.is_some() {
            return Ok(());
        }
        let Some(provider) = &mut self.local_remember_store_provider else {
            return Ok(());
        };
        self.remember_store = Some(provider.spawn(ctx)?);
        Ok(())
    }

    fn request_remember_store_load(&self, ctx: &Context<ShardCoordinatorMsg<M>>) -> ActorResult {
        if let Some(store) = &self.remember_store {
            store.load(ctx)?;
        }
        Ok(())
    }

    fn apply_remember_store_load(
        &mut self,
        ctx: &mut Context<ShardCoordinatorMsg<M>>,
        result: Result<RememberedShards, AskError>,
    ) -> ActorResult {
        let remembered = match result {
            Ok(remembered) => remembered,
            Err(_) => return self.stop_for_remember_store_failure(ctx),
        };
        self.runtime.merge_remembered_shards(remembered.shards);
        self.waiting_for_remember_store_load = false;
        self.allocate_remembered_shard_homes(ctx)?;
        Ok(())
    }

    fn persist_allocated_shard(
        &self,
        ctx: &Context<ShardCoordinatorMsg<M>>,
        result: &Result<GetShardHomePlan, ShardingError>,
    ) -> ActorResult {
        let Ok(plan) = result else {
            return Ok(());
        };
        self.persist_allocated_shard_plan(ctx, plan)
    }

    fn persist_allocated_shard_plan(
        &self,
        ctx: &Context<ShardCoordinatorMsg<M>>,
        plan: &GetShardHomePlan,
    ) -> ActorResult {
        let GetShardHomePlan::Allocated {
            event: CoordinatorEvent::ShardHomeAllocated { shard, region: _ },
            host_region: _,
            host_shard: _,
        } = plan
        else {
            return Ok(());
        };

        if let Some(store) = &self.remember_store {
            store.add_shard(ctx, shard.clone())?;
        }
        Ok(())
    }

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

    fn register_local_region(
        &mut self,
        ctx: &Context<ShardCoordinatorMsg<M>>,
        target: HandoffRegionTarget<M>,
    ) -> Result<Result<CoordinatorStateSnapshot, ShardingError>, ActorError> {
        let region = target.region().clone();
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

    fn register_remote_region(
        &mut self,
        ctx: &Context<ShardCoordinatorMsg<M>>,
        region: ActorRefWireData,
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
        reply
            .send_register_ack(region)
            .map_err(|error| ActorError::Message(error.to_string()))?;
        self.allocate_remembered_shard_homes(ctx)
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

    fn allocate_remembered_shard_homes(
        &mut self,
        ctx: &Context<ShardCoordinatorMsg<M>>,
    ) -> ActorResult {
        let plans = self
            .runtime
            .allocate_remembered_shard_homes("coordinator", self.strategy.as_ref())
            .map_err(|error| ActorError::Message(error.to_string()))?;
        for plan in &plans {
            self.persist_allocated_shard_plan(ctx, plan)?;
            if let Some(handoff) = &self.handoff {
                handoff.dispatch_host_shard(ctx, plan)?;
            }
        }
        Ok(())
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

    fn stop_for_remember_store_failure(
        &mut self,
        ctx: &mut Context<ShardCoordinatorMsg<M>>,
    ) -> ActorResult {
        ctx.stop(ctx.myself())
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
