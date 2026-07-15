#![deny(missing_docs)]
//! Typed shard-coordinator actor, mailbox protocol, and diagnostic snapshot.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::Duration;

use kairo_actor::{Actor, ActorError, ActorPath, ActorRef, ActorResult, Context, Props, Signal};
use kairo_serialization::ActorRefWireData;

use crate::coordinator_handoff::CoordinatorHandoff;
use crate::coordinator_store::{
    CoordinatorRememberStore, CoordinatorRememberStoreError, LocalCoordinatorRememberStoreProvider,
};
use crate::{
    BeginHandOffAck, CoordinatorEvent, CoordinatorRemoteRegions, CoordinatorRemoteReplyError,
    CoordinatorRemoteReplyTarget, CoordinatorRuntime, CoordinatorState, GetShardHomePlan,
    HandoffRegionTarget, HandoffTransport, HandoffWorkerDone, LeastShardAllocationStrategy,
    RebalanceCompletionPlan, RebalancePlan, RegionId, RegionShutdownPlan,
    RememberCoordinatorDDataStoreMsg, RememberCoordinatorStoreMsg, RememberCoordinatorStoreState,
    RememberCoordinatorUpdateDone, RememberedShards, ShardAllocationStrategy, ShardId,
    ShardStarted, ShardStopped, ShardingError, remote_region_id,
};

/// Actor-timer key used for the coordinator's periodic rebalance cadence.
pub const REBALANCE_TIMER_KEY: &str = "sharding-coordinator-rebalance";

mod construction;
mod remember_store;

/// Mailbox-owning coordinator for region registration, shard allocation, and handoff.
///
/// The actor serializes mutations to [`CoordinatorRuntime`], starts handoff
/// workers when configured, and optionally loads and updates a remember-shard
/// store. Remote wire messages are validated by dedicated adapters before they
/// are converted into [`ShardCoordinatorMsg`] variants.
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
/// Local mailbox protocol for [`ShardCoordinatorActor`].
///
/// This enum is not a stable wire contract. Variants prefixed with `Remote`
/// are trusted re-entry points used after the stable coordinator wire protocol
/// has been decoded and its sender and recipient identities validated.
pub enum ShardCoordinatorMsg<M = ()>
where
    M: Send + 'static,
{
    /// Applies one already validated coordinator domain event.
    ApplyEvent {
        /// Event to apply to the coordinator state machine.
        event: CoordinatorEvent,
        /// Optional observer for the resulting state or validation failure.
        reply_to: Option<ActorRef<Result<CoordinatorStateSnapshot, ShardingError>>>,
    },
    /// Changes the startup guard that defers allocation until regions register.
    SetAllRegionsRegistered {
        /// Whether every expected region has registered.
        all_registered: bool,
    },
    /// Enables or clears coordinated-shutdown preparation.
    SetPreparingForShutdown {
        /// Whether shutdown preparation is active.
        preparing: bool,
    },
    /// Excludes a region from new allocations while graceful shutdown runs.
    MarkGracefulShutdown {
        /// Draining region.
        region: RegionId,
    },
    /// Makes a previously draining region eligible again.
    UnmarkGracefulShutdown {
        /// Region whose graceful-shutdown marker is cleared.
        region: RegionId,
    },
    /// Excludes a region whose termination is already being processed.
    MarkRegionTerminating {
        /// Terminating region.
        region: RegionId,
    },
    /// Clears a region's termination-in-progress marker.
    UnmarkRegionTerminating {
        /// Region whose termination marker is cleared.
        region: RegionId,
    },
    /// Excludes a currently unavailable region from allocation and rebalance.
    MarkRegionUnavailable {
        /// Unavailable region.
        region: RegionId,
    },
    /// Returns a recovered region to allocation and rebalance eligibility.
    UnmarkRegionUnavailable {
        /// Recovered region.
        region: RegionId,
    },
    /// Plans graceful shutdown for a local region and starts its handoff workers.
    GracefulShutdownReq {
        /// Region requesting graceful shutdown.
        region: RegionId,
        /// Optional observer for the shutdown plan.
        reply_to: Option<ActorRef<RegionShutdownPlan>>,
    },
    /// Resolves or allocates the home of a shard for a local requester.
    RequestShardHome {
        /// Region asking for the shard home.
        requester: RegionId,
        /// Shard to resolve.
        shard: ShardId,
        /// Recipient for the reply, allocation, deferral, or typed failure.
        reply_to: ActorRef<Result<GetShardHomePlan, ShardingError>>,
    },
    /// Registers a local region, watches it, and allocates remembered shards.
    RegisterLocalRegion {
        /// Local handoff and host-shard delivery target.
        target: HandoffRegionTarget<M>,
        /// Recipient for the post-registration snapshot or failure.
        reply_to: ActorRef<Result<CoordinatorStateSnapshot, ShardingError>>,
    },
    /// Re-enters a validated remote region registration command.
    RegisterRemoteRegion {
        /// Stable remote region actor identity.
        region: ActorRefWireData,
        /// Optional transport target used for handoff and shard hosting.
        target: Option<HandoffRegionTarget<M>>,
        /// Stable remote reply route for the registration acknowledgement.
        reply: CoordinatorRemoteReplyTarget,
    },
    /// Re-enters a decoded graceful-shutdown request from a remote region.
    RemoteGracefulShutdownReq {
        /// Stable remote region actor identity.
        region: ActorRefWireData,
    },
    /// Removes a stopped local region and reallocates remembered shards.
    RegionStopped {
        /// Stopped region.
        region: RegionId,
    },
    /// Re-enters a decoded stopped-region notification from a remote region.
    RemoteRegionStopped {
        /// Stable stopped-region actor identity.
        region: ActorRefWireData,
    },
    /// Resolves or allocates a shard home for a validated remote requester.
    RequestRemoteShardHome {
        /// Stable actor identity of the requesting region.
        requester: ActorRefWireData,
        /// Shard to resolve.
        shard: ShardId,
        /// Stable remote route for a shard-home reply.
        reply: CoordinatorRemoteReplyTarget,
    },
    /// Computes a rebalance plan and starts configured handoff workers.
    PlanRebalance {
        /// Recipient for the plan or typed failure.
        reply_to: ActorRef<Result<RebalancePlan, ShardingError>>,
    },
    /// Completes an explicitly driven rebalance attempt.
    CompleteRebalance {
        /// Shard whose handoff completed or timed out.
        shard: ShardId,
        /// Whether every handoff participant acknowledged completion.
        ok: bool,
        /// Recipient for the deallocation or cleanup result.
        reply_to: ActorRef<Result<RebalanceCompletionPlan, ShardingError>>,
    },
    /// Returns a spawned handoff worker's terminal result to the coordinator.
    HandoffWorkerDone(HandoffWorkerDone),
    /// Observes best-effort local `HostShard` delivery.
    HostShardObserved {
        /// Shard whose host command was delivered.
        shard: ShardId,
    },
    /// Re-enters a decoded remote `ShardStarted` acknowledgement.
    RemoteHostShardObserved {
        /// Stable actor identity of the hosting region.
        region: ActorRefWireData,
        /// Acknowledgement emitted by the region.
        started: ShardStarted,
    },
    /// Re-enters a decoded remote begin-handoff acknowledgement.
    RemoteBeginHandOffAck {
        /// Stable actor identity of the acknowledging region.
        region: ActorRefWireData,
        /// Phase-one handoff acknowledgement.
        ack: BeginHandOffAck,
    },
    /// Re-enters a decoded remote shard-stopped acknowledgement.
    RemoteShardStopped {
        /// Stable actor identity of the acknowledging region.
        region: ActorRefWireData,
        /// Phase-two handoff acknowledgement.
        stopped: ShardStopped,
    },
    /// Runs one periodic rebalance turn.
    RebalanceTick {
        /// Optional observer for the resulting plan.
        reply_to: Option<ActorRef<Result<RebalancePlan, ShardingError>>>,
    },
    /// Returns the initial remember-store load to the actor mailbox.
    RememberStoreLoadResult {
        /// Loaded shards or the preserved ask/store failure.
        result: Result<RememberedShards, CoordinatorRememberStoreError>,
    },
    /// Returns one remember-store update to the actor mailbox.
    RememberStoreUpdateResult {
        /// Persisted shard acknowledgement or the preserved ask/store failure.
        result: Result<RememberCoordinatorUpdateDone, CoordinatorRememberStoreError>,
    },
    /// Starts or replaces the fixed-delay rebalance timer.
    StartRebalanceTimer {
        /// Delay before the first rebalance turn.
        initial_delay: Duration,
        /// Delay between later rebalance turns.
        interval: Duration,
    },
    /// Cancels periodic rebalancing and clears its configured interval.
    StopRebalanceTimer,
    /// Stops the coordinator, including while its remember store is loading.
    Terminate,
    /// Requests a diagnostic state snapshot.
    GetState {
        /// Recipient for the current snapshot.
        reply_to: ActorRef<CoordinatorStateSnapshot>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Diagnostic snapshot of coordinator allocation and lifecycle state.
pub struct CoordinatorStateSnapshot {
    /// Whether every currently known shard region has completed registration.
    pub all_regions_registered: bool,
    /// Shards currently assigned to each registered region.
    pub allocations: BTreeMap<RegionId, Vec<ShardId>>,
    /// Registered proxy identifiers retained by coordinator state.
    pub proxies: BTreeSet<RegionId>,
    /// Remembered shards that do not currently have a region home.
    pub unallocated_shards: BTreeSet<ShardId>,
    /// Shards in handoff and the regions waiting for their home.
    pub rebalance_in_progress: BTreeMap<ShardId, Vec<RegionId>>,
    /// Registered regions excluded because cluster reachability is unavailable.
    pub unavailable_regions: BTreeSet<RegionId>,
    /// Whether coordinator state retains deallocated shards for later recovery.
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
            ShardCoordinatorMsg::MarkRegionUnavailable { region } => {
                self.runtime.mark_region_unavailable(region);
            }
            ShardCoordinatorMsg::UnmarkRegionUnavailable { region } => {
                self.runtime.unmark_region_unavailable(&region);
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
                self.apply_region_stopped(ctx, region)?;
            }
            ShardCoordinatorMsg::RemoteRegionStopped { region } => {
                let region_id = self.remote_regions.register(region);
                self.apply_region_stopped(ctx, region_id)?;
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
            ShardCoordinatorMsg::Terminate => ctx.stop(ctx.myself())?,
            ShardCoordinatorMsg::GetState { reply_to } => {
                let _ = reply_to.tell(CoordinatorStateSnapshot::from(&self.runtime));
            }
        }
        Ok(())
    }

    fn signal(&mut self, ctx: &mut Context<Self::Msg>, signal: Signal) -> ActorResult {
        match signal {
            Signal::Terminated(actor) | Signal::ChildFailed { actor, .. } => {
                if self.apply_watched_region_terminated(ctx, actor.path())? {
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

    fn apply_watched_region_terminated(
        &mut self,
        ctx: &Context<ShardCoordinatorMsg<M>>,
        path: &ActorPath,
    ) -> Result<bool, ActorError> {
        let Some(region) = self.region_watch_by_path.remove(path) else {
            return Ok(false);
        };

        self.apply_region_stopped(ctx, region)?;
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
        tolerate_remote_reply_send(reply.send_register_ack(region))?;
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

    fn apply_region_stopped(
        &mut self,
        ctx: &Context<ShardCoordinatorMsg<M>>,
        region: RegionId,
    ) -> ActorResult {
        self.runtime.unmark_graceful_shutdown(&region);
        self.runtime.unmark_region_terminating(&region);
        self.runtime.unmark_region_unavailable(&region);
        if let Some(handoff) = &mut self.handoff {
            handoff.forward_region_terminated(&region)?;
            handoff.remove_region_target(&region);
        }
        self.remote_regions.remove(&region);
        if !self.runtime.state().allocations().contains_region(&region) {
            return Ok(());
        }
        self.runtime
            .apply_event(CoordinatorEvent::ShardRegionTerminated { region })
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
        let Some(home_region) = self
            .remote_regions
            .wire_ref(&region)
            .cloned()
            .or_else(|| ActorRefWireData::new(region).ok())
        else {
            return Ok(());
        };
        tolerate_remote_reply_send(reply.send_shard_home(shard, requester, home_region))
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
        match completion {
            RebalanceCompletionPlan::Deallocated {
                retry_get_shard_home,
                pending_requesters,
                ..
            } => {
                let requesters = if pending_requesters.is_empty() {
                    vec!["coordinator".to_string()]
                } else {
                    pending_requesters
                };
                self.retry_completed_rebalance_homes(ctx, retry_get_shard_home.shard_id, requesters)
            }
            RebalanceCompletionPlan::Cleared {
                shard,
                pending_requesters,
            } => {
                if pending_requesters.is_empty() {
                    return Ok(());
                }
                self.retry_completed_rebalance_homes(ctx, shard, pending_requesters)
            }
            RebalanceCompletionPlan::TimedOut {
                shard,
                pending_requesters,
            } => {
                if pending_requesters.is_empty() {
                    return Ok(());
                }
                self.retry_completed_rebalance_homes(ctx, shard, pending_requesters)
            }
        }
    }

    fn retry_completed_rebalance_homes(
        &mut self,
        ctx: &Context<ShardCoordinatorMsg<M>>,
        shard: ShardId,
        pending_requesters: Vec<RegionId>,
    ) -> ActorResult {
        for requester in pending_requesters {
            let result =
                self.runtime
                    .request_shard_home(requester, shard.clone(), self.strategy.as_ref());
            self.persist_allocated_shard(ctx, &result)?;
            let plan = result.map_err(|error| ActorError::Message(error.to_string()))?;
            if let Some(handoff) = &self.handoff {
                handoff.dispatch_host_shard(ctx, &plan)?;
            }
        }
        Ok(())
    }
}

fn tolerate_remote_reply_send(result: Result<(), CoordinatorRemoteReplyError>) -> ActorResult {
    match result {
        Ok(()) | Err(CoordinatorRemoteReplyError::Send { .. }) => Ok(()),
        Err(error @ CoordinatorRemoteReplyError::Serialization(_)) => {
            Err(ActorError::Message(error.to_string()))
        }
    }
}

impl From<&CoordinatorRuntime> for CoordinatorStateSnapshot {
    fn from(runtime: &CoordinatorRuntime) -> Self {
        let mut snapshot = Self::from(runtime.state());
        snapshot.all_regions_registered = runtime.all_regions_registered();
        snapshot.rebalance_in_progress = runtime
            .rebalance_in_progress()
            .iter()
            .map(|(shard, requesters)| (shard.clone(), requesters.iter().cloned().collect()))
            .collect();
        snapshot.unavailable_regions = runtime.unavailable_regions().clone();
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
            all_regions_registered: true,
            allocations,
            proxies: state.proxies().clone(),
            unallocated_shards: state.unallocated_shards().clone(),
            rebalance_in_progress: BTreeMap::new(),
            unavailable_regions: BTreeSet::new(),
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

#[cfg(test)]
mod remote_reply_tests {
    use kairo_serialization::SerializationError;

    use super::*;

    #[test]
    fn coordinator_survives_transient_remote_reply_send_failure() {
        assert!(
            tolerate_remote_reply_send(Err(CoordinatorRemoteReplyError::Send {
                target: "peer".to_string(),
                reason: "route unavailable".to_string(),
            }))
            .is_ok()
        );
        assert!(
            tolerate_remote_reply_send(Err(CoordinatorRemoteReplyError::Serialization(
                SerializationError::Message("invalid reply".to_string()),
            )))
            .is_err()
        );
    }
}
