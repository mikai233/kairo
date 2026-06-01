use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, Context, Props};

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
    RegionRouteDelivery, RegionRoutePlan, RegionRouteTransport, ShardCoordinatorMsg,
    ShardCoordinatorRemoteHome, ShardCoordinatorRemoteRegistrationAck, ShardDeliverPlan,
    ShardHandOffPlan, ShardHomePlan, ShardId, ShardMsg, ShardRegionRuntime, ShardingEnvelope,
    ShardingError, shard_home_plan_from_remote,
};

mod construction;
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
                self.runtime.mark_shard_stopped(&shard);
                self.local_shards.remove(&shard);
                reply_optional(reply_to, self.snapshot());
                self.try_complete_graceful_shutdown(ctx)?;
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
    fn register_with_coordinator(&mut self, ctx: &Context<ShardRegionMsg<M>>) -> ActorResult {
        if let Some(registration) = &mut self.registration {
            if registration.is_registered() {
                return Ok(());
            }

            let reply_to = ctx.message_adapter(|result| {
                ShardRegionMsg::CoordinatorRegistrationResult { result }
            })?;
            let target = HandoffRegionTarget::new(self.runtime.self_region().clone(), ctx.myself());
            let _ = registration
                .coordinator()
                .tell(ShardCoordinatorMsg::RegisterLocalRegion { target, reply_to });
            ctx.schedule_once_self(
                registration.retry_interval(),
                ShardRegionMsg::RetryCoordinatorRegistration,
            );
            return Ok(());
        }

        if self.remote_coordinator.is_registered() {
            return Ok(());
        }
        let Some(target) = self.remote_coordinator.target() else {
            return Ok(());
        };
        if let Some(transport) = &self.remote_coordinator_transport {
            let _ = transport.register(target);
        }
        if let Some(retry_interval) = self.remote_coordinator.retry_interval() {
            ctx.schedule_once_self(retry_interval, ShardRegionMsg::RetryCoordinatorRegistration);
        }
        Ok(())
    }

    fn apply_registration_result(
        &mut self,
        result: Result<CoordinatorStateSnapshot, ShardingError>,
        ctx: &Context<ShardRegionMsg<M>>,
    ) -> ActorResult {
        match result {
            Ok(_) => {
                if let Some(registration) = &mut self.registration {
                    registration.mark_registered();
                }
                self.request_pending_shard_homes_from_coordinator(ctx)?;
                Ok(())
            }
            Err(_) => {
                if let Some(registration) = &mut self.registration {
                    registration.mark_registering();
                }
                self.register_with_coordinator(ctx)
            }
        }
    }

    fn apply_coordinator_discovery_plan(
        &mut self,
        ctx: &Context<ShardRegionMsg<M>>,
        plan: RegionCoordinatorDiscoveryPlan<M>,
    ) -> ActorResult {
        if plan.registration_changed {
            self.registration = plan.registration.map(RegionRegistration::new);
            self.remote_coordinator
                .set_target(plan.remote_target, plan.remote_retry_interval);
        }
        self.register_with_coordinator(ctx)
    }

    fn apply_remote_registration_ack(
        &mut self,
        ctx: &Context<ShardRegionMsg<M>>,
        ack: ShardCoordinatorRemoteRegistrationAck,
    ) -> ActorResult {
        if matches!(
            self.remote_coordinator.apply_registration_ack(ack),
            RegionRemoteRegistrationPlan::Registered { .. }
        ) {
            self.request_pending_shard_homes_from_coordinator(ctx)?;
        }
        Ok(())
    }

    fn request_shard_home_from_coordinator(
        &self,
        ctx: &Context<ShardRegionMsg<M>>,
        request: GetShardHome,
    ) -> ActorResult {
        let Some(registration) = &self.registration else {
            let Some(target) = self.remote_coordinator.target() else {
                return Ok(());
            };
            if !self.remote_coordinator.is_registered() {
                return Ok(());
            }
            if let Some(transport) = &self.remote_coordinator_transport {
                let _ = transport.request_shard_home(target, request);
            }
            return Ok(());
        };
        if !registration.is_registered() {
            return Ok(());
        }

        let requested_shard = request.shard_id.clone();
        let reply_to =
            ctx.message_adapter(move |result| ShardRegionMsg::CoordinatorShardHomeResult {
                requested_shard: requested_shard.clone(),
                result,
            })?;
        let _ = registration
            .coordinator()
            .tell(ShardCoordinatorMsg::RequestShardHome {
                requester: self.runtime.self_region().clone(),
                shard: request.shard_id,
                reply_to,
            });
        Ok(())
    }

    fn request_pending_shard_homes_from_coordinator(
        &self,
        ctx: &Context<ShardRegionMsg<M>>,
    ) -> ActorResult {
        let pending = self
            .home_requests
            .pending_shards()
            .cloned()
            .collect::<Vec<_>>();
        for shard in pending {
            self.request_shard_home_from_coordinator(ctx, GetShardHome { shard_id: shard })?;
        }
        Ok(())
    }

    fn apply_coordinator_shard_home_result(
        &mut self,
        ctx: &Context<ShardRegionMsg<M>>,
        requested_shard: ShardId,
        result: Result<GetShardHomePlan, ShardingError>,
    ) -> ActorResult {
        let (shard, region) = match result {
            Ok(GetShardHomePlan::Reply { shard, region }) => (shard, region),
            Ok(GetShardHomePlan::Allocated {
                event: CoordinatorEvent::ShardHomeAllocated { shard, region },
                ..
            }) => (shard, region),
            Ok(GetShardHomePlan::Allocated { .. })
            | Ok(GetShardHomePlan::Deferred { .. })
            | Ok(GetShardHomePlan::Ignored { .. })
            | Err(_) => return Ok(()),
        };
        if shard != requested_shard {
            return Ok(());
        }
        let delivery_reply_to = self.home_requests.drain(&shard);

        let plan = match self.runtime.record_shard_home(shard, region) {
            Ok(plan) => plan,
            Err(_) => return Ok(()),
        };
        self.apply_coordinator_shard_home_plan(ctx, plan, delivery_reply_to)
    }

    fn apply_remote_coordinator_shard_home(
        &mut self,
        ctx: &Context<ShardRegionMsg<M>>,
        home: ShardCoordinatorRemoteHome,
    ) -> ActorResult {
        let plan = shard_home_plan_from_remote(home);
        let delivery_reply_to = self.home_requests.drain(&plan.shard);
        let plan = match self.runtime.record_shard_home(plan.shard, plan.region) {
            Ok(plan) => plan,
            Err(_) => return Ok(()),
        };
        self.apply_coordinator_shard_home_plan(ctx, plan, delivery_reply_to)
    }

    fn apply_coordinator_shard_home_plan(
        &mut self,
        ctx: &Context<ShardRegionMsg<M>>,
        plan: ShardHomePlan<M>,
        delivery_reply_to: Vec<ActorRef<ShardDeliverPlan<M>>>,
    ) -> ActorResult {
        let plan = self.maybe_start_local_shard_from_home_plan(ctx, plan)?;
        match plan {
            ShardHomePlan::DeliverLocal { shard, buffered } => {
                self.replay_buffered_to_local_shard_with_replies(
                    &shard,
                    buffered,
                    delivery_reply_to,
                )?;
            }
            ShardHomePlan::Forward {
                shard,
                region,
                buffered,
            } => {
                self.forward_buffered_to_region(ctx, shard, region, buffered, delivery_reply_to)?;
            }
            ShardHomePlan::StartLocalShard { .. } => {}
        }
        Ok(())
    }

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
