use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, Context, Props, Signal};

use crate::region_home_requests::RegionHomeRequests;
use crate::region_protocol::{
    RegionBufferedReplayPlan, RegionLocalHandOffCompletionFailure,
    RegionLocalHandOffCompletionPlan, RegionLocalHandOffPlan, RegionLocalRoutePlan, ShardRegionMsg,
    ShardRegionSnapshot,
};
use crate::region_registration::{
    RegionRegistration, RegionRegistrationConfig, RegionRegistrationStatus,
};
use crate::region_remote_handoff::{
    RegionRemoteHandOff, RegionRemoteHandOffAction, RegionRemoteShardHandOffAction,
    plan_remote_handoff, plan_remote_shard_handoff,
};
use crate::region_shards::LocalShardSpawner;
use crate::{
    CoordinatorEvent, CoordinatorStateSnapshot, EntityActorFactory, EntityId, GetShardHome,
    GetShardHomePlan, HandoffRegionTarget, HostShardPlan, RegionCoordinatorDiscovery,
    RegionCoordinatorDiscoveryConfig, RegionCoordinatorDiscoveryPlan, RegionId,
    RegionRemoteCoordinator, RegionRemoteCoordinatorTransport, RegionRemoteRegistrationPlan,
    RegionRouteDelivery, RegionRoutePlan, RegionRouteTransport, RememberShardStoreMsg,
    ShardCoordinatorMsg, ShardCoordinatorRemoteHome, ShardCoordinatorRemoteRegistrationAck,
    ShardDeliverPlan, ShardHandOffPlan, ShardHomePlan, ShardId, ShardMsg, ShardRegionRuntime,
    ShardingEnvelope, ShardingError, shard_home_plan_from_remote,
};

mod construction;
mod coordinator_flow;
mod local_routing;
mod remote_lifecycle;

pub struct ShardRegionActor<M>
where
    M: Send + 'static,
{
    runtime: ShardRegionRuntime<M>,
    local_shard_spawner: Option<LocalShardSpawner<M>>,
    local_shards: BTreeMap<ShardId, ActorRef<ShardMsg<M>>>,
    registration: Option<RegionRegistration<M>>,
    remote_coordinator: RegionRemoteCoordinator,
    remote_coordinator_transport: Option<RegionRemoteCoordinatorTransport>,
    remote_handoff: Option<RegionRemoteHandOff<M>>,
    coordinator_discovery: Option<RegionCoordinatorDiscovery<M>>,
    home_requests: RegionHomeRequests<M>,
    route_transport: Option<RegionRouteTransport<M>>,
    pending_local_restarts: BTreeMap<ShardId, u64>,
    suppressed_local_restarts: BTreeMap<ShardId, u64>,
    local_restart_generations: BTreeMap<ShardId, u64>,
}

impl<M> Actor for ShardRegionActor<M>
where
    M: Send + 'static,
{
    type Msg = ShardRegionMsg<M>;

    fn started(&mut self, ctx: &mut Context<Self::Msg>) -> ActorResult {
        self.register_with_coordinator(ctx)
    }

    fn stopped(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        self.send_region_stopped_to_coordinator()
    }

    fn signal(&mut self, ctx: &mut Context<Self::Msg>, signal: Signal) -> ActorResult {
        match signal {
            Signal::Terminated(actor) | Signal::ChildFailed { actor, .. } => {
                if self.apply_local_shard_terminated(ctx, actor.path())? {
                    Ok(())
                } else {
                    Err(ActorError::DeathPact {
                        actor: actor.path().to_string(),
                    })
                }
            }
            Signal::PreRestart | Signal::PostStop => self.stopped(ctx),
        }
    }

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
                let request = shard_home_request(&plan);
                if let Some(shard) = buffered_shard(&plan) {
                    self.home_requests
                        .remember_delivery(shard.clone(), delivery_reply_to.clone());
                }
                self.dispatch_local_route_plan(plan, route_reply_to, delivery_reply_to)?;
                if let Some(request) = request {
                    self.request_shard_home_from_coordinator(ctx, request)?;
                }
            }
            ShardRegionMsg::HostShard { shard, reply_to } => {
                let plan = self.runtime.host_shard(shard);
                let plan = self.maybe_start_local_shard_from_host_plan(ctx, plan)?;
                let plan = self.replay_buffered_from_host_plan(plan)?;
                if matches!(plan, HostShardPlan::IgnoredGracefulShutdown { .. }) {
                    self.send_graceful_shutdown_to_coordinator()?;
                }
                let _ = reply_to.tell(plan);
            }
            ShardRegionMsg::HostShardAndReplayBuffered {
                shard,
                reply_to,
                delivery_reply_to,
            } => {
                let plan = self.host_shard_and_replay_buffered(ctx, shard, delivery_reply_to)?;
                if matches!(
                    plan,
                    RegionBufferedReplayPlan::IgnoredGracefulShutdown { .. }
                ) {
                    self.send_graceful_shutdown_to_coordinator()?;
                }
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
                self.suppress_pending_local_restart(&shard);
                let plan = self.runtime.begin_handoff(shard);
                let _ = reply_to.tell(plan);
            }
            ShardRegionMsg::HandOff { shard, reply_to } => {
                self.suppress_pending_local_restart(&shard);
                let plan = self.runtime.handoff(shard);
                let _ = reply_to.tell(plan);
            }
            ShardRegionMsg::RemoteHostShard { shard, reply } => {
                self.apply_remote_host_shard(ctx, shard, reply)?;
            }
            ShardRegionMsg::RemoteBeginHandOff { shard, reply } => {
                self.apply_remote_begin_handoff(shard, reply)?;
            }
            ShardRegionMsg::RemoteHandOff { shard, reply } => {
                self.apply_remote_handoff(ctx, shard, reply)?;
            }
            ShardRegionMsg::RemoteLocalShardHandOffObserved {
                plan,
                timeout,
                reply,
            } => {
                self.apply_remote_local_shard_handoff_observed(ctx, plan, timeout, reply)?;
            }
            ShardRegionMsg::RemoteLocalShardHandOffStopperResult {
                shard,
                result,
                reply,
            } => {
                self.apply_remote_local_shard_handoff_stopper_result(ctx, shard, result, reply)?;
            }
            ShardRegionMsg::GracefulShutdown { reply_to } => {
                self.apply_graceful_shutdown(ctx)?;
                reply_optional(reply_to, self.snapshot());
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
                self.try_complete_graceful_shutdown(ctx)?;
            }
            ShardRegionMsg::CoordinatorRegistrationResult { result } => {
                self.apply_registration_result(result, ctx)?;
            }
            ShardRegionMsg::RemoteCoordinatorRegistrationAck { ack } => {
                self.apply_remote_registration_ack(ctx, ack)?;
            }
            ShardRegionMsg::RetryCoordinatorRegistration => {
                self.register_with_coordinator(ctx)?;
            }
            ShardRegionMsg::CoordinatorShardHomeResult {
                requested_shard,
                result,
            } => {
                self.apply_coordinator_shard_home_result(ctx, requested_shard, result)?;
            }
            ShardRegionMsg::RemoteCoordinatorShardHome { home } => {
                self.apply_remote_coordinator_shard_home(ctx, home)?;
            }
            ShardRegionMsg::CoordinatorDiscoverySnapshot { state } => {
                let plan = self
                    .coordinator_discovery
                    .as_mut()
                    .map(|discovery| discovery.apply_snapshot(&state));
                if let Some(plan) = plan {
                    self.apply_coordinator_discovery_plan(ctx, plan)?;
                }
            }
            ShardRegionMsg::CoordinatorDiscoveryEvent { event } => {
                let plan = self
                    .coordinator_discovery
                    .as_mut()
                    .map(|discovery| discovery.apply_event(&event));
                if let Some(plan) = plan {
                    self.apply_coordinator_discovery_plan(ctx, plan)?;
                }
            }
            ShardRegionMsg::ForwardedBufferedRouteResult { result: _ } => {}
            ShardRegionMsg::MarkShardStopped { shard, reply_to } => {
                self.mark_local_shard_stopped(ctx, &shard);
                reply_optional(reply_to, self.snapshot());
                self.try_complete_graceful_shutdown(ctx)?;
            }
            ShardRegionMsg::MarkRegionStopped { region, reply_to } => {
                self.runtime.mark_region_stopped(&region);
                reply_optional(reply_to, self.snapshot());
            }
            ShardRegionMsg::RestartLocalShard { shard, generation } => {
                self.restart_local_shard(ctx, shard, generation)?;
            }
            ShardRegionMsg::SetGracefulShutdown { in_progress } => {
                self.runtime.set_graceful_shutdown_in_progress(in_progress);
            }
            ShardRegionMsg::SetPreparingForShutdown { preparing } => {
                self.runtime.set_preparing_for_shutdown(preparing);
            }
            ShardRegionMsg::GetState { reply_to } => {
                let _ = reply_to.tell(self.snapshot());
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
    fn apply_local_shard_terminated(
        &mut self,
        ctx: &mut Context<ShardRegionMsg<M>>,
        path: &kairo_actor::ActorPath,
    ) -> Result<bool, ActorError> {
        let shard = self
            .local_shards
            .iter()
            .find_map(|(shard, shard_ref)| (shard_ref.path() == path).then(|| shard.clone()));

        if let Some(shard) = shard {
            self.mark_local_shard_stopped(ctx, &shard);
            self.try_complete_graceful_shutdown(ctx)?;
            return Ok(true);
        }

        Ok(path.parent().as_ref() == Some(ctx.myself().path())
            && path.name().is_some_and(|name| name.starts_with("shard-")))
    }

    fn mark_local_shard_stopped(&mut self, ctx: &mut Context<ShardRegionMsg<M>>, shard: &ShardId) {
        let restart_backoff = self.remembered_shard_restart_backoff(shard);
        self.runtime.mark_shard_stopped(shard);
        self.local_shards.remove(shard);
        if let Some(backoff) = restart_backoff {
            let generation = self.schedule_pending_local_restart(shard);
            ctx.schedule_once_self(
                backoff,
                ShardRegionMsg::RestartLocalShard {
                    shard: shard.clone(),
                    generation,
                },
            );
        }
    }

    fn suppress_pending_local_restart(&mut self, shard: &ShardId) {
        if let Some(generation) = self.pending_local_restarts.get(shard).copied() {
            self.suppressed_local_restarts
                .insert(shard.clone(), generation);
        }
    }

    fn schedule_pending_local_restart(&mut self, shard: &ShardId) -> u64 {
        let generation = self
            .local_restart_generations
            .entry(shard.clone())
            .and_modify(|generation| *generation = generation.wrapping_add(1))
            .or_insert(1);
        let generation = *generation;
        self.pending_local_restarts
            .insert(shard.clone(), generation);
        self.suppressed_local_restarts.remove(shard);
        generation
    }

    fn remembered_shard_restart_backoff(&self, shard: &ShardId) -> Option<Duration> {
        if !self.local_shards.contains_key(shard) {
            return None;
        }
        if self.runtime.handing_off_shards().contains(shard)
            || self.runtime.graceful_shutdown_in_progress()
        {
            return None;
        }
        self.local_shard_spawner
            .as_ref()
            .and_then(LocalShardSpawner::failure_backoff)
    }
}

impl<M> ShardRegionActor<M>
where
    M: Send + 'static,
{
    fn snapshot(&self) -> ShardRegionSnapshot {
        let mut snapshot = ShardRegionSnapshot::from(&self.runtime);
        snapshot.registration_status = self
            .registration
            .as_ref()
            .map(RegionRegistration::status)
            .or_else(|| self.remote_coordinator.status())
            .unwrap_or(RegionRegistrationStatus::Disabled);
        snapshot
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

fn shard_home_request<M>(plan: &RegionRoutePlan<M>) -> Option<GetShardHome> {
    match plan {
        RegionRoutePlan::Buffered {
            request: Some(request),
            ..
        } => Some(request.clone()),
        _ => None,
    }
}

fn buffered_shard<M>(plan: &RegionRoutePlan<M>) -> Option<&ShardId> {
    match plan {
        RegionRoutePlan::Buffered { shard, .. } => Some(shard),
        _ => None,
    }
}
