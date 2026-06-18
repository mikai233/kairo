use super::*;

impl<M> ShardRegionActor<M>
where
    M: Send + 'static,
{
    pub(super) fn register_with_coordinator(
        &mut self,
        ctx: &Context<ShardRegionMsg<M>>,
    ) -> ActorResult {
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

    pub(super) fn apply_registration_result(
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

    pub(super) fn apply_coordinator_discovery_plan(
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

    pub(super) fn apply_remote_registration_ack(
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

    pub(super) fn request_shard_home_from_coordinator(
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

    pub(super) fn apply_coordinator_shard_home_result(
        &mut self,
        ctx: &mut Context<ShardRegionMsg<M>>,
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

        let plan = match self.runtime.record_shard_home(shard.clone(), region) {
            Ok(plan) => plan,
            Err(_) => return Ok(()),
        };
        self.watch_region_from_home_plan(ctx, &plan)?;
        let delivery_reply_to = self.home_requests.drain(&shard);
        self.apply_coordinator_shard_home_plan(ctx, plan, delivery_reply_to)
    }

    pub(super) fn apply_remote_coordinator_shard_home(
        &mut self,
        ctx: &mut Context<ShardRegionMsg<M>>,
        home: ShardCoordinatorRemoteHome,
    ) -> ActorResult {
        let plan = shard_home_plan_from_remote(home);
        let shard = plan.shard;
        let plan = match self.runtime.record_shard_home(shard.clone(), plan.region) {
            Ok(plan) => plan,
            Err(_) => return Ok(()),
        };
        self.watch_region_from_home_plan(ctx, &plan)?;
        let delivery_reply_to = self.home_requests.drain(&shard);
        self.apply_coordinator_shard_home_plan(ctx, plan, delivery_reply_to)
    }

    fn apply_coordinator_shard_home_plan(
        &mut self,
        ctx: &mut Context<ShardRegionMsg<M>>,
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
}
