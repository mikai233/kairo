use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use kairo_actor::{Actor, ActorRef, ActorResult, Context, Props};

use crate::{
    CoordinatorEvent, CoordinatorRuntime, CoordinatorState, GetShardHomePlan,
    LeastShardAllocationStrategy, RebalanceCompletionPlan, RebalancePlan, RegionId,
    ShardAllocationStrategy, ShardId, ShardingError,
};

pub const REBALANCE_TIMER_KEY: &str = "sharding-coordinator-rebalance";

pub struct ShardCoordinatorActor {
    runtime: CoordinatorRuntime,
    strategy: Box<dyn ShardAllocationStrategy + Send>,
    rebalance_interval: Option<Duration>,
}

impl ShardCoordinatorActor {
    pub fn new(
        state: CoordinatorState,
        strategy: impl ShardAllocationStrategy + Send + 'static,
    ) -> Self {
        Self {
            runtime: CoordinatorRuntime::new(state),
            strategy: Box::new(strategy),
            rebalance_interval: None,
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

    pub fn runtime(&self) -> &CoordinatorRuntime {
        &self.runtime
    }
}

#[derive(Clone)]
pub enum ShardCoordinatorMsg {
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
    PlanRebalance {
        reply_to: ActorRef<Result<RebalancePlan, ShardingError>>,
    },
    CompleteRebalance {
        shard: ShardId,
        ok: bool,
        reply_to: ActorRef<Result<RebalanceCompletionPlan, ShardingError>>,
    },
    RebalanceTick {
        reply_to: Option<ActorRef<Result<RebalancePlan, ShardingError>>>,
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

impl Actor for ShardCoordinatorActor {
    type Msg = ShardCoordinatorMsg;

    fn started(&mut self, ctx: &mut Context<Self::Msg>) -> ActorResult {
        if let Some(interval) = self.rebalance_interval {
            start_rebalance_timer(ctx, interval, interval);
        }
        Ok(())
    }

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
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
                let _ = reply_to.tell(result);
            }
            ShardCoordinatorMsg::PlanRebalance { reply_to } => {
                let result = self.runtime.plan_rebalance(self.strategy.as_ref());
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
            ShardCoordinatorMsg::RebalanceTick { reply_to } => {
                let result = self.runtime.plan_rebalance(self.strategy.as_ref());
                reply_optional(reply_to, result);
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

fn start_rebalance_timer(
    ctx: &mut Context<ShardCoordinatorMsg>,
    initial_delay: Duration,
    interval: Duration,
) {
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
