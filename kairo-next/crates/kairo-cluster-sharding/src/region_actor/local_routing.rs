use super::*;

impl<M> ShardRegionActor<M>
where
    M: Send + 'static,
{
    pub(super) fn maybe_start_local_shard_from_host_plan(
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

    pub(super) fn replay_buffered_from_host_plan(
        &mut self,
        plan: HostShardPlan<M>,
    ) -> Result<HostShardPlan<M>, ActorError> {
        let HostShardPlan::AlreadyStarted {
            shard,
            started,
            buffered,
        } = plan
        else {
            return Ok(plan);
        };

        let delivery_reply_to = self.home_requests.drain(&shard);
        self.replay_buffered_to_local_shard_with_replies(&shard, buffered, delivery_reply_to)?;
        Ok(HostShardPlan::AlreadyStarted {
            shard,
            started,
            buffered: Vec::new(),
        })
    }

    pub(super) fn dispatch_local_route_plan(
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

    pub(super) fn host_shard_and_replay_buffered(
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

    pub(super) fn replay_buffered_to_local_shard_with_replies(
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

    pub(super) fn forward_buffered_to_region(
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

    pub(super) fn dispatch_local_handoff_plan(
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

    pub(super) fn complete_local_shard_handoff(
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

    pub(super) fn apply_local_shard_handoff_stopper_result(
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

    pub(super) fn maybe_start_local_shard_from_home_plan(
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
