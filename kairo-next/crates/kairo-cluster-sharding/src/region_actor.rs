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

impl<M> ShardRegionActor<M>
where
    M: Send + 'static,
{
    pub fn new(self_region: impl Into<RegionId>, buffer_capacity: usize) -> Self {
        Self {
            runtime: ShardRegionRuntime::new(self_region, buffer_capacity),
            local_shard_spawner: None,
            local_shards: BTreeMap::new(),
            registration: None,
            remote_coordinator: RegionRemoteCoordinator::new(),
            remote_coordinator_transport: None,
            remote_handoff: None,
            coordinator_discovery: None,
            home_requests: RegionHomeRequests::new(),
            route_transport: None,
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
            registration: None,
            remote_coordinator: RegionRemoteCoordinator::new(),
            remote_coordinator_transport: None,
            remote_handoff: None,
            coordinator_discovery: None,
            home_requests: RegionHomeRequests::new(),
            route_transport: None,
        }
    }

    pub fn new_with_local_shards_and_registration(
        self_region: impl Into<RegionId>,
        region_buffer_capacity: usize,
        shard_buffer_capacity: usize,
        coordinator: ActorRef<ShardCoordinatorMsg<M>>,
        retry_interval: Duration,
    ) -> Self
    where
        M: Send + 'static,
    {
        Self {
            runtime: ShardRegionRuntime::new(self_region, region_buffer_capacity),
            local_shard_spawner: Some(LocalShardSpawner::plain(shard_buffer_capacity)),
            local_shards: BTreeMap::new(),
            registration: Some(RegionRegistration::new(RegionRegistrationConfig::new(
                coordinator,
                retry_interval,
            ))),
            remote_coordinator: RegionRemoteCoordinator::new(),
            remote_coordinator_transport: None,
            remote_handoff: None,
            coordinator_discovery: None,
            home_requests: RegionHomeRequests::new(),
            route_transport: None,
        }
    }

    pub fn new_with_local_entity_shards(
        self_region: impl Into<RegionId>,
        region_buffer_capacity: usize,
        shard_buffer_capacity: usize,
        entity_factory: EntityActorFactory<M>,
    ) -> Self
    where
        M: Clone,
    {
        Self {
            runtime: ShardRegionRuntime::new(self_region, region_buffer_capacity),
            local_shard_spawner: Some(LocalShardSpawner::entity_backed(
                shard_buffer_capacity,
                entity_factory,
            )),
            local_shards: BTreeMap::new(),
            registration: None,
            remote_coordinator: RegionRemoteCoordinator::new(),
            remote_coordinator_transport: None,
            remote_handoff: None,
            coordinator_discovery: None,
            home_requests: RegionHomeRequests::new(),
            route_transport: None,
        }
    }

    pub fn new_with_local_entity_shards_and_registration(
        self_region: impl Into<RegionId>,
        region_buffer_capacity: usize,
        shard_buffer_capacity: usize,
        entity_factory: EntityActorFactory<M>,
        coordinator: ActorRef<ShardCoordinatorMsg<M>>,
        retry_interval: Duration,
    ) -> Self
    where
        M: Clone,
    {
        Self {
            runtime: ShardRegionRuntime::new(self_region, region_buffer_capacity),
            local_shard_spawner: Some(LocalShardSpawner::entity_backed(
                shard_buffer_capacity,
                entity_factory,
            )),
            local_shards: BTreeMap::new(),
            registration: Some(RegionRegistration::new(RegionRegistrationConfig::new(
                coordinator,
                retry_interval,
            ))),
            remote_coordinator: RegionRemoteCoordinator::new(),
            remote_coordinator_transport: None,
            remote_handoff: None,
            coordinator_discovery: None,
            home_requests: RegionHomeRequests::new(),
            route_transport: None,
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
            registration: None,
            remote_coordinator: RegionRemoteCoordinator::new(),
            remote_coordinator_transport: None,
            remote_handoff: None,
            coordinator_discovery: None,
            home_requests: RegionHomeRequests::new(),
            route_transport: None,
        }
    }

    pub fn new_with_local_remember_store_shards_and_registration(
        self_region: impl Into<RegionId>,
        type_name: impl Into<String>,
        region_buffer_capacity: usize,
        shard_buffer_capacity: usize,
        remembered_entities_by_shard: BTreeMap<ShardId, BTreeSet<EntityId>>,
        timeout: Duration,
        registration: RegionRegistrationConfig<M>,
    ) -> Self
    where
        M: Send + 'static,
    {
        Self {
            runtime: ShardRegionRuntime::new(self_region, region_buffer_capacity),
            local_shard_spawner: Some(LocalShardSpawner::with_local_remember_stores(
                type_name,
                shard_buffer_capacity,
                remembered_entities_by_shard,
                timeout,
            )),
            local_shards: BTreeMap::new(),
            registration: Some(RegionRegistration::new(registration)),
            remote_coordinator: RegionRemoteCoordinator::new(),
            remote_coordinator_transport: None,
            remote_handoff: None,
            coordinator_discovery: None,
            home_requests: RegionHomeRequests::new(),
            route_transport: None,
        }
    }

    pub fn with_coordinator_discovery(
        mut self,
        discovery: RegionCoordinatorDiscoveryConfig<M>,
    ) -> Self {
        self.registration = None;
        self.remote_coordinator.set_target(None, None);
        self.coordinator_discovery = Some(RegionCoordinatorDiscovery::new(discovery));
        self
    }

    pub fn with_remote_coordinator_transport(
        mut self,
        transport: RegionRemoteCoordinatorTransport,
    ) -> Self {
        self.remote_coordinator_transport = Some(transport);
        self
    }

    pub fn with_region_route_transport(mut self, route_transport: RegionRouteTransport<M>) -> Self {
        self.route_transport = Some(route_transport);
        self
    }

    pub fn with_remote_handoff_stop_message_factory(
        mut self,
        stop_message: impl Fn() -> M + Send + Sync + 'static,
        timeout: Duration,
    ) -> Self {
        self.remote_handoff = Some(RegionRemoteHandOff::new(stop_message, timeout));
        self
    }

    pub fn with_remote_handoff_stop_message(mut self, stop_message: M, timeout: Duration) -> Self
    where
        M: Clone + Send + Sync + 'static,
    {
        self.remote_handoff = Some(RegionRemoteHandOff::from_message(stop_message, timeout));
        self
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

    pub fn props_with_local_shards_and_registration(
        self_region: impl Into<RegionId>,
        region_buffer_capacity: usize,
        shard_buffer_capacity: usize,
        coordinator: ActorRef<ShardCoordinatorMsg<M>>,
        retry_interval: Duration,
    ) -> Props<Self>
    where
        M: Send + 'static,
    {
        let self_region = self_region.into();
        Props::new(move || {
            Self::new_with_local_shards_and_registration(
                self_region,
                region_buffer_capacity,
                shard_buffer_capacity,
                coordinator.clone(),
                retry_interval,
            )
        })
    }

    pub fn props_with_local_shards_and_coordinator_discovery(
        self_region: impl Into<RegionId>,
        region_buffer_capacity: usize,
        shard_buffer_capacity: usize,
        discovery: RegionCoordinatorDiscoveryConfig<M>,
    ) -> Props<Self>
    where
        M: Send + 'static,
    {
        let self_region = self_region.into();
        Props::new(move || {
            Self::new_with_local_shards(self_region, region_buffer_capacity, shard_buffer_capacity)
                .with_coordinator_discovery(discovery.clone())
        })
    }

    pub fn props_with_local_entity_shards(
        self_region: impl Into<RegionId>,
        region_buffer_capacity: usize,
        shard_buffer_capacity: usize,
        entity_factory: EntityActorFactory<M>,
    ) -> Props<Self>
    where
        M: Clone + Send + 'static,
    {
        let self_region = self_region.into();
        Props::new(move || {
            Self::new_with_local_entity_shards(
                self_region,
                region_buffer_capacity,
                shard_buffer_capacity,
                entity_factory.clone(),
            )
        })
    }

    pub fn props_with_local_entity_shards_and_registration(
        self_region: impl Into<RegionId>,
        region_buffer_capacity: usize,
        shard_buffer_capacity: usize,
        entity_factory: EntityActorFactory<M>,
        coordinator: ActorRef<ShardCoordinatorMsg<M>>,
        retry_interval: Duration,
    ) -> Props<Self>
    where
        M: Clone + Send + 'static,
    {
        let self_region = self_region.into();
        Props::new(move || {
            Self::new_with_local_entity_shards_and_registration(
                self_region,
                region_buffer_capacity,
                shard_buffer_capacity,
                entity_factory.clone(),
                coordinator.clone(),
                retry_interval,
            )
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

    pub fn props_with_local_remember_store_shards_and_registration(
        self_region: impl Into<RegionId>,
        type_name: impl Into<String>,
        region_buffer_capacity: usize,
        shard_buffer_capacity: usize,
        remembered_entities_by_shard: BTreeMap<ShardId, BTreeSet<EntityId>>,
        timeout: Duration,
        registration: RegionRegistrationConfig<M>,
    ) -> Props<Self>
    where
        M: Send + 'static,
    {
        let self_region = self_region.into();
        let type_name = type_name.into();
        Props::new(move || {
            Self::new_with_local_remember_store_shards_and_registration(
                self_region,
                type_name.clone(),
                region_buffer_capacity,
                shard_buffer_capacity,
                remembered_entities_by_shard.clone(),
                timeout,
                registration.clone(),
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

    fn apply_remote_host_shard(
        &mut self,
        ctx: &Context<ShardRegionMsg<M>>,
        shard: ShardId,
        reply: crate::ShardRegionRemoteControlReplyTarget,
    ) -> ActorResult {
        let plan = self.runtime.host_shard(shard);
        let plan = self.maybe_start_local_shard_from_host_plan(ctx, plan)?;
        if let HostShardPlan::AlreadyStarted { started, .. } = plan {
            reply
                .send_shard_started(started.shard_id)
                .map_err(|error| ActorError::Message(error.to_string()))?;
        }
        Ok(())
    }

    fn apply_remote_begin_handoff(
        &mut self,
        shard: ShardId,
        reply: crate::ShardRegionRemoteControlReplyTarget,
    ) -> ActorResult {
        let plan = self.runtime.begin_handoff(shard);
        if let crate::BeginHandOffPlan::Ack { ack, .. } = plan {
            reply
                .send_begin_handoff_ack(ack.shard_id)
                .map_err(|error| ActorError::Message(error.to_string()))?;
        }
        Ok(())
    }

    fn apply_graceful_shutdown(&mut self, ctx: &mut Context<ShardRegionMsg<M>>) -> ActorResult {
        if self.runtime.preparing_for_shutdown() {
            ctx.stop(ctx.myself())?;
            return Ok(());
        }

        self.runtime.set_graceful_shutdown_in_progress(true);
        self.send_graceful_shutdown_to_coordinator()?;
        self.try_complete_graceful_shutdown(ctx)
    }

    fn send_graceful_shutdown_to_coordinator(&self) -> ActorResult {
        let Some(registration) = &self.registration else {
            let Some(target) = self.remote_coordinator.target() else {
                return Ok(());
            };
            if !self.remote_coordinator.is_registered() {
                return Ok(());
            }
            if let Some(transport) = &self.remote_coordinator_transport {
                transport
                    .graceful_shutdown(target)
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            return Ok(());
        };
        if !registration.is_registered() {
            return Ok(());
        }
        registration
            .coordinator()
            .tell(ShardCoordinatorMsg::GracefulShutdownReq {
                region: self.runtime.self_region().clone(),
                reply_to: None,
            })
            .map_err(|error| ActorError::Message(error.reason().to_string()))
    }

    fn send_region_stopped_to_coordinator(&self) -> ActorResult {
        if let Some(registration) = &self.registration {
            if registration.is_registered() {
                registration
                    .coordinator()
                    .tell(ShardCoordinatorMsg::RegionStopped {
                        region: self.runtime.self_region().clone(),
                    })
                    .map_err(|error| ActorError::Message(error.reason().to_string()))?;
            }
            return Ok(());
        }

        let Some(target) = self.remote_coordinator.target() else {
            return Ok(());
        };
        if !self.remote_coordinator.is_registered() {
            return Ok(());
        }
        if let Some(transport) = &self.remote_coordinator_transport {
            transport
                .region_stopped(target)
                .map_err(|error| ActorError::Message(error.to_string()))?;
        }
        Ok(())
    }

    fn try_complete_graceful_shutdown(
        &mut self,
        ctx: &mut Context<ShardRegionMsg<M>>,
    ) -> ActorResult {
        if self.runtime.graceful_shutdown_complete() {
            ctx.stop(ctx.myself())?;
        }
        Ok(())
    }

    fn apply_remote_handoff(
        &mut self,
        ctx: &mut Context<ShardRegionMsg<M>>,
        shard: ShardId,
        reply: crate::ShardRegionRemoteControlReplyTarget,
    ) -> ActorResult {
        let plan = self.runtime.handoff(shard);
        match plan_remote_handoff(plan, self.remote_handoff.as_ref()) {
            RegionRemoteHandOffAction::ReplyShardStopped { stopped, .. } => {
                reply
                    .send_shard_stopped(stopped.shard_id)
                    .map_err(|error| ActorError::Message(error.to_string()))?;
            }
            RegionRemoteHandOffAction::ForwardToLocalShard {
                shard,
                stop_message,
                timeout,
                ..
            } => {
                let Some(shard_ref) = self.local_shards.get(&shard) else {
                    self.complete_remote_handoff(ctx, shard, reply)?;
                    return Ok(());
                };
                let reply_to = ctx.message_adapter(move |plan| {
                    ShardRegionMsg::RemoteLocalShardHandOffObserved {
                        plan,
                        timeout,
                        reply: reply.clone(),
                    }
                })?;
                shard_ref
                    .tell(ShardMsg::HandOff {
                        stop_message,
                        reply_to,
                    })
                    .map_err(|error| ActorError::Message(error.reason().to_string()))?;
            }
            RegionRemoteHandOffAction::MissingStopMessage { .. } => {}
        }
        Ok(())
    }

    fn apply_remote_local_shard_handoff_observed(
        &mut self,
        ctx: &mut Context<ShardRegionMsg<M>>,
        plan: ShardHandOffPlan<M>,
        timeout: Duration,
        reply: crate::ShardRegionRemoteControlReplyTarget,
    ) -> ActorResult {
        match plan_remote_shard_handoff(plan, timeout) {
            RegionRemoteShardHandOffAction::Complete { shard, .. } => {
                self.complete_remote_handoff(ctx, shard, reply)?;
            }
            RegionRemoteShardHandOffAction::AskStopper { shard, timeout } => {
                self.ask_remote_handoff_stopper(ctx, shard, timeout, reply)?;
            }
            RegionRemoteShardHandOffAction::AlreadyInProgress { .. } => {}
        }
        Ok(())
    }

    fn ask_remote_handoff_stopper(
        &self,
        ctx: &Context<ShardRegionMsg<M>>,
        shard: ShardId,
        timeout: Duration,
        reply: crate::ShardRegionRemoteControlReplyTarget,
    ) -> ActorResult {
        let Some(shard_ref) = self.local_shards.get(&shard).cloned() else {
            return Ok(());
        };
        ctx.ask(
            shard_ref,
            timeout,
            |reply_to| ShardMsg::HandOffStopperTerminated { reply_to },
            move |result| ShardRegionMsg::RemoteLocalShardHandOffStopperResult {
                shard,
                result,
                reply,
            },
        )
    }

    fn apply_remote_local_shard_handoff_stopper_result(
        &mut self,
        ctx: &mut Context<ShardRegionMsg<M>>,
        shard: ShardId,
        result: kairo_actor::AskResult<bool>,
        reply: crate::ShardRegionRemoteControlReplyTarget,
    ) -> ActorResult {
        if matches!(result, Ok(true)) {
            self.complete_remote_handoff(ctx, shard, reply)?;
        }
        Ok(())
    }

    fn complete_remote_handoff(
        &mut self,
        ctx: &mut Context<ShardRegionMsg<M>>,
        shard: ShardId,
        reply: crate::ShardRegionRemoteControlReplyTarget,
    ) -> ActorResult {
        if let Some(shard_ref) = self.local_shards.get(&shard).cloned() {
            ctx.stop(shard_ref)?;
        }
        self.runtime.mark_shard_stopped(&shard);
        self.local_shards.remove(&shard);
        let result = reply
            .send_shard_stopped(shard)
            .map_err(|error| ActorError::Message(error.to_string()));
        self.try_complete_graceful_shutdown(ctx)?;
        result
    }

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

    fn dispatch_local_route_plan(
        &self,
        plan: RegionRoutePlan<M>,
        route_reply_to: ActorRef<RegionLocalRoutePlan<M>>,
        delivery_reply_to: ActorRef<ShardDeliverPlan<M>>,
    ) -> Result<(), ActorError> {
        let result = match plan {
            RegionRoutePlan::DeliverLocal { shard, message } => {
                if let Some(shard_ref) = self.local_shards.get(&shard) {
                    shard_ref
                        .tell(ShardMsg::Deliver {
                            message,
                            reply_to: delivery_reply_to,
                        })
                        .map_err(|error| ActorError::Message(error.reason().to_string()))?;
                    RegionLocalRoutePlan::DeliveredToLocalShard { shard }
                } else {
                    RegionLocalRoutePlan::MissingLocalShard { shard, message }
                }
            }
            RegionRoutePlan::Forward {
                shard,
                region,
                message,
            } => {
                let Some(route_transport) = &self.route_transport else {
                    let _ = route_reply_to.tell(RegionLocalRoutePlan::Forward {
                        shard,
                        region,
                        message,
                    });
                    return Ok(());
                };
                let delivery = route_transport.send_route_to(
                    &region,
                    shard,
                    message,
                    route_reply_to.clone(),
                    delivery_reply_to,
                );
                match delivery {
                    RegionRouteDelivery::Sent { .. } => return Ok(()),
                    failed => RegionLocalRoutePlan::ForwardedToRegion { delivery: failed },
                }
            }
            RegionRoutePlan::Buffered { shard, request } => {
                RegionLocalRoutePlan::Buffered { shard, request }
            }
            RegionRoutePlan::Dropped {
                shard,
                reason,
                message,
            } => RegionLocalRoutePlan::Dropped {
                shard,
                reason,
                message,
            },
        };
        let _ = route_reply_to.tell(result);
        Ok(())
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

    fn replay_buffered_to_local_shard_with_replies(
        &self,
        shard: &ShardId,
        buffered: Vec<ShardingEnvelope<M>>,
        delivery_reply_to: Vec<ActorRef<ShardDeliverPlan<M>>>,
    ) -> Result<usize, ActorError> {
        let Some(shard_ref) = self.local_shards.get(shard) else {
            return Err(ActorError::Message(format!(
                "local shard `{shard}` is not available for buffered replay"
            )));
        };
        if buffered.len() != delivery_reply_to.len() {
            return Err(ActorError::Message(format!(
                "local shard `{shard}` buffered replay has {} messages but {} delivery replies",
                buffered.len(),
                delivery_reply_to.len()
            )));
        }
        let replayed = buffered.len();
        for (message, reply_to) in buffered.into_iter().zip(delivery_reply_to) {
            shard_ref
                .tell(ShardMsg::Deliver { message, reply_to })
                .map_err(|error| ActorError::Message(error.reason().to_string()))?;
        }
        Ok(replayed)
    }

    fn forward_buffered_to_region(
        &self,
        ctx: &Context<ShardRegionMsg<M>>,
        shard: ShardId,
        region: RegionId,
        buffered: Vec<ShardingEnvelope<M>>,
        delivery_reply_to: Vec<ActorRef<ShardDeliverPlan<M>>>,
    ) -> Result<usize, ActorError> {
        let Some(route_transport) = &self.route_transport else {
            return Ok(0);
        };
        if buffered.len() != delivery_reply_to.len() {
            return Err(ActorError::Message(format!(
                "region `{region}` buffered forwarding has {} messages but {} delivery replies",
                buffered.len(),
                delivery_reply_to.len()
            )));
        }

        let mut forwarded = 0;
        for (message, delivery_reply_to) in buffered.into_iter().zip(delivery_reply_to) {
            let route_reply_to = ctx.message_adapter(|result| {
                ShardRegionMsg::ForwardedBufferedRouteResult { result }
            })?;
            match route_transport.send_route_to(
                &region,
                shard.clone(),
                message,
                route_reply_to,
                delivery_reply_to,
            ) {
                RegionRouteDelivery::Sent { .. } => forwarded += 1,
                RegionRouteDelivery::MissingTarget { .. }
                | RegionRouteDelivery::SendFailed { .. } => {}
            }
        }
        Ok(forwarded)
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
