use super::*;

impl<M> ShardRegionActor<M>
where
    M: Send + 'static,
{
    pub(super) fn apply_remote_host_shard(
        &mut self,
        ctx: &Context<ShardRegionMsg<M>>,
        shard: ShardId,
        reply: crate::ShardRegionRemoteControlReplyTarget,
    ) -> ActorResult {
        let plan = self.runtime.host_shard(shard);
        let plan = self.maybe_start_local_shard_from_host_plan(ctx, plan)?;
        let plan = self.replay_buffered_from_host_plan(plan)?;
        if let HostShardPlan::AlreadyStarted { started, .. } = plan {
            reply
                .send_shard_started(started.shard_id)
                .map_err(|error| ActorError::Message(error.to_string()))?;
        }
        Ok(())
    }

    pub(super) fn apply_remote_begin_handoff(
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

    pub(super) fn apply_graceful_shutdown(
        &mut self,
        ctx: &mut Context<ShardRegionMsg<M>>,
    ) -> ActorResult {
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

    pub(super) fn send_region_stopped_to_coordinator(&self) -> ActorResult {
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

    pub(super) fn try_complete_graceful_shutdown(
        &mut self,
        ctx: &mut Context<ShardRegionMsg<M>>,
    ) -> ActorResult {
        if self.runtime.graceful_shutdown_complete() {
            ctx.stop(ctx.myself())?;
        }
        Ok(())
    }

    pub(super) fn apply_remote_handoff(
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

    pub(super) fn apply_remote_local_shard_handoff_observed(
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

    pub(super) fn apply_remote_local_shard_handoff_stopper_result(
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
}
