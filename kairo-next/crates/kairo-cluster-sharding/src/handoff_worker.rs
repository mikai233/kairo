#![deny(missing_docs)]
//! Per-shard actor implementing the coordinator's two-phase handoff protocol.

use std::collections::BTreeSet;
use std::time::Duration;

use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, Context, Props};

use crate::{
    BeginHandOffPlan, HandoffTransport, RegionId, RegionLocalHandOffCompletionPlan,
    RegionLocalHandOffPlan, ShardHandOffPlan, ShardId, ShardRebalancePlan, ShardStopped,
};

/// Terminal result returned by a handoff worker to its coordinator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandoffWorkerDone {
    /// Shard whose handoff attempt ended.
    pub shard: ShardId,
    /// Whether the old owner stopped the shard or terminated during the stop phase.
    ///
    /// `false` denotes timeout, immediate delivery failure, or an inconsistent
    /// internal completion response.
    pub ok: bool,
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use kairo_actor::{Recipient, SendError};
    use kairo_testkit::ActorSystemTestKit;

    use crate::{HandoffRegionTarget, ShardStopped};

    use super::*;

    #[derive(Clone)]
    struct AcceptingRegion;

    impl Recipient<ShardRegionMsg<String>> for AcceptingRegion {
        fn tell(
            &self,
            message: ShardRegionMsg<String>,
        ) -> Result<(), SendError<ShardRegionMsg<String>>> {
            match message {
                ShardRegionMsg::HandOffToLocalShard { .. } => Ok(()),
                other => Err(SendError::new(other, "unexpected region message")),
            }
        }
    }

    #[derive(Clone)]
    struct AcceptingBeginAndHandOffRegion;

    impl Recipient<ShardRegionMsg<String>> for AcceptingBeginAndHandOffRegion {
        fn tell(
            &self,
            message: ShardRegionMsg<String>,
        ) -> Result<(), SendError<ShardRegionMsg<String>>> {
            match message {
                ShardRegionMsg::BeginHandOff { .. }
                | ShardRegionMsg::HandOffToLocalShard { .. } => Ok(()),
                other => Err(SendError::new(other, "unexpected region message")),
            }
        }
    }

    #[derive(Clone)]
    struct CompletingRegion;

    impl Recipient<ShardRegionMsg<String>> for CompletingRegion {
        fn tell(
            &self,
            message: ShardRegionMsg<String>,
        ) -> Result<(), SendError<ShardRegionMsg<String>>> {
            match message {
                ShardRegionMsg::HandOffToLocalShard { .. } => Ok(()),
                ShardRegionMsg::CompleteLocalShardHandOff {
                    shard, reply_to, ..
                } => {
                    let _ = reply_to.tell(RegionLocalHandOffCompletionPlan::Completed {
                        shard: shard.clone(),
                        stopped: ShardStopped { shard_id: shard },
                    });
                    Ok(())
                }
                other => Err(SendError::new(other, "unexpected region message")),
            }
        }
    }

    use crate::ShardRegionMsg;

    #[test]
    fn handoff_worker_completes_from_remote_shard_stopped() {
        let kit = ActorSystemTestKit::new("handoff-worker-remote-stopped").unwrap();
        let mut transport = HandoffTransport::new();
        transport.insert_target(HandoffRegionTarget::new("remote-region", AcceptingRegion));
        let worker = kit
            .system()
            .spawn(
                "worker",
                HandoffWorkerActor::props(
                    ShardRebalancePlan {
                        shard: "12".to_string(),
                        from_region: "remote-region".to_string(),
                        participants: Default::default(),
                        begin_handoff: crate::BeginHandOff {
                            shard_id: "12".to_string(),
                        },
                    },
                    "stop".to_string(),
                    Duration::from_secs(1),
                    transport,
                ),
            )
            .unwrap();
        let done = kit.create_probe::<HandoffWorkerDone>("done").unwrap();

        worker
            .tell(HandoffWorkerMsg::Start {
                reply_to: done.actor_ref(),
            })
            .unwrap();
        worker
            .tell(HandoffWorkerMsg::RemoteShardStopped {
                region: "remote-region".to_string(),
                stopped: ShardStopped {
                    shard_id: "12".to_string(),
                },
            })
            .unwrap();

        assert_eq!(
            done.expect_msg(Duration::from_millis(500)).unwrap(),
            HandoffWorkerDone {
                shard: "12".to_string(),
                ok: true,
            }
        );
        kit.shutdown(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn handoff_worker_finishes_from_local_shard_stop_immediately_plan() {
        let kit = ActorSystemTestKit::new("handoff-worker-local-stop-immediately").unwrap();
        let mut transport = HandoffTransport::new();
        transport.insert_target(HandoffRegionTarget::new("local-region", AcceptingRegion));
        let worker = kit
            .system()
            .spawn(
                "worker",
                HandoffWorkerActor::props(
                    ShardRebalancePlan {
                        shard: "12".to_string(),
                        from_region: "local-region".to_string(),
                        participants: Default::default(),
                        begin_handoff: crate::BeginHandOff {
                            shard_id: "12".to_string(),
                        },
                    },
                    "stop".to_string(),
                    Duration::from_secs(1),
                    transport,
                ),
            )
            .unwrap();
        let done = kit.create_probe::<HandoffWorkerDone>("done").unwrap();

        worker
            .tell(HandoffWorkerMsg::Start {
                reply_to: done.actor_ref(),
            })
            .unwrap();
        worker
            .tell(HandoffWorkerMsg::ShardHandOffObserved {
                plan: ShardHandOffPlan::StopImmediately {
                    shard: "12".to_string(),
                    entities: vec!["entity-1".to_string()],
                    stop_message: "stop".to_string(),
                    stopped: ShardStopped {
                        shard_id: "12".to_string(),
                    },
                },
            })
            .unwrap();

        assert_eq!(
            done.expect_msg(Duration::from_secs(2)).unwrap(),
            HandoffWorkerDone {
                shard: "12".to_string(),
                ok: true,
            }
        );
        kit.shutdown(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn handoff_worker_asks_local_region_completion_after_stopper_plan() {
        let kit = ActorSystemTestKit::new("handoff-worker-local-stopper").unwrap();
        let mut transport = HandoffTransport::new();
        transport.insert_target(HandoffRegionTarget::new("local-region", CompletingRegion));
        let worker = kit
            .system()
            .spawn(
                "worker",
                HandoffWorkerActor::props(
                    ShardRebalancePlan {
                        shard: "12".to_string(),
                        from_region: "local-region".to_string(),
                        participants: Default::default(),
                        begin_handoff: crate::BeginHandOff {
                            shard_id: "12".to_string(),
                        },
                    },
                    "stop".to_string(),
                    Duration::from_secs(1),
                    transport,
                ),
            )
            .unwrap();
        let done = kit.create_probe::<HandoffWorkerDone>("done").unwrap();

        worker
            .tell(HandoffWorkerMsg::Start {
                reply_to: done.actor_ref(),
            })
            .unwrap();
        worker
            .tell(HandoffWorkerMsg::ShardHandOffObserved {
                plan: ShardHandOffPlan::StartEntityStopper {
                    shard: "12".to_string(),
                    entities: vec!["entity-1".to_string()],
                    stop_message: "stop".to_string(),
                },
            })
            .unwrap();

        assert_eq!(
            done.expect_msg(Duration::from_millis(500)).unwrap(),
            HandoffWorkerDone {
                shard: "12".to_string(),
                ok: true,
            }
        );
        kit.shutdown(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn handoff_worker_treats_participant_termination_as_begin_ack() {
        let kit = ActorSystemTestKit::new("handoff-worker-participant-terminated").unwrap();
        let mut transport = HandoffTransport::new();
        transport.insert_target(HandoffRegionTarget::new(
            "owner-region",
            AcceptingBeginAndHandOffRegion,
        ));
        transport.insert_target(HandoffRegionTarget::new(
            "participant-region",
            AcceptingBeginAndHandOffRegion,
        ));
        let worker = kit
            .system()
            .spawn(
                "worker",
                HandoffWorkerActor::props(
                    ShardRebalancePlan {
                        shard: "12".to_string(),
                        from_region: "owner-region".to_string(),
                        participants: BTreeSet::from(["participant-region".to_string()]),
                        begin_handoff: crate::BeginHandOff {
                            shard_id: "12".to_string(),
                        },
                    },
                    "stop".to_string(),
                    Duration::from_secs(1),
                    transport,
                ),
            )
            .unwrap();
        let done = kit.create_probe::<HandoffWorkerDone>("done").unwrap();
        let state = kit.create_probe::<HandoffWorkerSnapshot>("state").unwrap();

        worker
            .tell(HandoffWorkerMsg::Start {
                reply_to: done.actor_ref(),
            })
            .unwrap();
        worker
            .tell(HandoffWorkerMsg::RegionTerminated {
                region: "participant-region".to_string(),
            })
            .unwrap();
        worker
            .tell(HandoffWorkerMsg::GetState {
                reply_to: state.actor_ref(),
            })
            .unwrap();

        let snapshot = state.expect_msg(Duration::from_millis(500)).unwrap();
        assert_eq!(snapshot.phase, HandoffWorkerPhase::AwaitingShardStopped);
        assert!(snapshot.remaining.is_empty());
        done.expect_no_msg(Duration::from_millis(50)).unwrap();
        kit.shutdown(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn handoff_worker_completes_when_owner_terminates_while_waiting_for_stop() {
        let kit = ActorSystemTestKit::new("handoff-worker-owner-terminated").unwrap();
        let mut transport = HandoffTransport::new();
        transport.insert_target(HandoffRegionTarget::new("owner-region", AcceptingRegion));
        let worker = kit
            .system()
            .spawn(
                "worker",
                HandoffWorkerActor::props(
                    ShardRebalancePlan {
                        shard: "12".to_string(),
                        from_region: "owner-region".to_string(),
                        participants: Default::default(),
                        begin_handoff: crate::BeginHandOff {
                            shard_id: "12".to_string(),
                        },
                    },
                    "stop".to_string(),
                    Duration::from_secs(1),
                    transport,
                ),
            )
            .unwrap();
        let done = kit.create_probe::<HandoffWorkerDone>("done").unwrap();

        worker
            .tell(HandoffWorkerMsg::Start {
                reply_to: done.actor_ref(),
            })
            .unwrap();
        worker
            .tell(HandoffWorkerMsg::RegionTerminated {
                region: "owner-region".to_string(),
            })
            .unwrap();

        assert_eq!(
            done.expect_msg(Duration::from_millis(500)).unwrap(),
            HandoffWorkerDone {
                shard: "12".to_string(),
                ok: true,
            }
        );
        kit.shutdown(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn handoff_worker_ignores_local_completion_before_stop_phase() {
        let kit = ActorSystemTestKit::new("handoff-worker-premature-local-completion").unwrap();
        let mut transport = HandoffTransport::new();
        transport.insert_target(HandoffRegionTarget::new("owner-region", AcceptingRegion));
        let worker = kit
            .system()
            .spawn(
                "worker",
                HandoffWorkerActor::props(
                    ShardRebalancePlan {
                        shard: "12".to_string(),
                        from_region: "owner-region".to_string(),
                        participants: Default::default(),
                        begin_handoff: crate::BeginHandOff {
                            shard_id: "12".to_string(),
                        },
                    },
                    "stop".to_string(),
                    Duration::from_secs(1),
                    transport,
                ),
            )
            .unwrap();
        let done = kit.create_probe::<HandoffWorkerDone>("done").unwrap();

        worker
            .tell(HandoffWorkerMsg::LocalHandOffCompleted {
                plan: RegionLocalHandOffCompletionPlan::Completed {
                    shard: "12".to_string(),
                    stopped: ShardStopped {
                        shard_id: "12".to_string(),
                    },
                },
            })
            .unwrap();
        worker
            .tell(HandoffWorkerMsg::Start {
                reply_to: done.actor_ref(),
            })
            .unwrap();
        worker
            .tell(HandoffWorkerMsg::RemoteShardStopped {
                region: "owner-region".to_string(),
                stopped: ShardStopped {
                    shard_id: "12".to_string(),
                },
            })
            .unwrap();

        assert_eq!(
            done.expect_msg(Duration::from_millis(500)).unwrap(),
            HandoffWorkerDone {
                shard: "12".to_string(),
                ok: true,
            }
        );
        kit.shutdown(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn handoff_worker_rejects_inconsistent_local_completion_identity() {
        let kit = ActorSystemTestKit::new("handoff-worker-mismatched-local-completion").unwrap();
        let mut transport = HandoffTransport::new();
        transport.insert_target(HandoffRegionTarget::new("owner-region", AcceptingRegion));
        let worker = kit
            .system()
            .spawn(
                "worker",
                HandoffWorkerActor::props(
                    ShardRebalancePlan {
                        shard: "12".to_string(),
                        from_region: "owner-region".to_string(),
                        participants: Default::default(),
                        begin_handoff: crate::BeginHandOff {
                            shard_id: "12".to_string(),
                        },
                    },
                    "stop".to_string(),
                    Duration::from_secs(1),
                    transport,
                ),
            )
            .unwrap();
        let done = kit.create_probe::<HandoffWorkerDone>("done").unwrap();

        worker
            .tell(HandoffWorkerMsg::Start {
                reply_to: done.actor_ref(),
            })
            .unwrap();
        worker
            .tell(HandoffWorkerMsg::LocalHandOffCompleted {
                plan: RegionLocalHandOffCompletionPlan::Completed {
                    shard: "12".to_string(),
                    stopped: ShardStopped {
                        shard_id: "other".to_string(),
                    },
                },
            })
            .unwrap();

        assert_eq!(
            done.expect_msg(Duration::from_millis(500)).unwrap(),
            HandoffWorkerDone {
                shard: "12".to_string(),
                ok: false,
            }
        );
        kit.shutdown(Duration::from_secs(1)).unwrap();
    }
}

/// Observable state of one handoff worker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandoffWorkerSnapshot {
    /// Shard being handed off.
    pub shard: ShardId,
    /// Current protocol phase.
    pub phase: HandoffWorkerPhase,
    /// Participant regions still expected to acknowledge begin-handoff.
    ///
    /// This set is empty outside [`HandoffWorkerPhase::AwaitingBeginAcks`].
    pub remaining: BTreeSet<RegionId>,
}

/// Phase of the two-stage handoff state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandoffWorkerPhase {
    /// The worker has not received its start command.
    Idle,
    /// Begin-handoff was sent and participant acknowledgements are outstanding.
    AwaitingBeginAcks,
    /// Every participant invalidated its shard-home view and the old owner is stopping the shard.
    AwaitingShardStopped,
    /// A terminal result was emitted and the actor is stopping.
    Done,
}

/// Internal actor protocol for one shard handoff worker.
pub enum HandoffWorkerMsg<M> {
    /// Starts the protocol unless it has already started.
    Start {
        /// Coordinator-owned recipient for the single terminal result.
        reply_to: ActorRef<HandoffWorkerDone>,
    },
    /// Records one participant's begin-handoff response.
    BeginHandOffAck {
        /// Region associated with the response route.
        region: RegionId,
        /// Region runtime decision, which must acknowledge this worker's shard.
        plan: BeginHandOffPlan,
    },
    /// Reports how the local owner region handled its handoff command.
    LocalHandOffForwarded {
        /// Region-level forwarding or immediate-completion decision.
        plan: RegionLocalHandOffPlan,
    },
    /// Reports how the owner's local shard began stopping its entities.
    ShardHandOffObserved {
        /// Shard-level immediate-stop or entity-stopper decision.
        plan: ShardHandOffPlan<M>,
    },
    /// Reports completion of a local entity-stopper-based handoff.
    LocalHandOffCompleted {
        /// Completion decision whose outer and nested shard IDs are validated.
        plan: RegionLocalHandOffCompletionPlan,
    },
    /// Reports a remote owner's stable wire-level shard-stopped acknowledgement.
    RemoteShardStopped {
        /// Remote region that sent the acknowledgement.
        region: RegionId,
        /// Acknowledgement whose shard ID must match the worker's shard.
        stopped: ShardStopped,
    },
    /// Reports termination of a participant or the old owner.
    RegionTerminated {
        /// Region observed as terminated.
        region: RegionId,
    },
    /// Fails whichever handoff phase is currently active.
    Timeout,
    /// Requests a diagnostic state snapshot.
    GetState {
        /// Recipient for the snapshot.
        reply_to: ActorRef<HandoffWorkerSnapshot>,
    },
}

/// Actor that coordinates one shard's begin-handoff and owner-stop phases.
///
/// The worker first waits for every participant to acknowledge cache
/// invalidation. It then tells the old owner to stop the shard and succeeds on
/// a matching shard-stopped response or owner termination. One timeout covers
/// the complete attempt.
pub struct HandoffWorkerActor<M>
where
    M: Send + 'static,
{
    plan: ShardRebalancePlan,
    stop_message: Option<M>,
    handoff_timeout: Duration,
    transport: HandoffTransport<M>,
    phase: HandoffWorkerPhase,
    remaining: BTreeSet<String>,
    reply_to: Option<ActorRef<HandoffWorkerDone>>,
}

impl<M> HandoffWorkerActor<M>
where
    M: Send + 'static,
{
    /// Creates an idle worker for one rebalance plan.
    pub fn new(
        plan: ShardRebalancePlan,
        stop_message: M,
        handoff_timeout: Duration,
        transport: HandoffTransport<M>,
    ) -> Self {
        Self {
            plan,
            stop_message: Some(stop_message),
            handoff_timeout,
            transport,
            phase: HandoffWorkerPhase::Idle,
            remaining: BTreeSet::new(),
            reply_to: None,
        }
    }

    /// Creates props that build an idle worker with the supplied plan and routes.
    pub fn props(
        plan: ShardRebalancePlan,
        stop_message: M,
        handoff_timeout: Duration,
        transport: HandoffTransport<M>,
    ) -> Props<Self>
    where
        M: Send + 'static,
    {
        Props::new(move || Self::new(plan, stop_message, handoff_timeout, transport))
    }
}

impl<M> Actor for HandoffWorkerActor<M>
where
    M: Send + 'static,
{
    type Msg = HandoffWorkerMsg<M>;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            HandoffWorkerMsg::Start { reply_to } => {
                self.start(ctx, reply_to)?;
            }
            HandoffWorkerMsg::BeginHandOffAck { region, plan } => {
                self.apply_begin_ack(ctx, region, plan)?;
            }
            HandoffWorkerMsg::LocalHandOffForwarded { plan } => {
                self.apply_local_handoff(ctx, plan)?;
            }
            HandoffWorkerMsg::ShardHandOffObserved { plan } => {
                self.apply_shard_handoff_observed(ctx, plan)?;
            }
            HandoffWorkerMsg::LocalHandOffCompleted { plan } => {
                self.apply_local_handoff_completion(ctx, plan)?;
            }
            HandoffWorkerMsg::RemoteShardStopped { region, stopped } => {
                self.apply_remote_shard_stopped(ctx, region, stopped)?;
            }
            HandoffWorkerMsg::RegionTerminated { region } => {
                self.apply_region_terminated(ctx, region)?;
            }
            HandoffWorkerMsg::Timeout => self.finish(ctx, false)?,
            HandoffWorkerMsg::GetState { reply_to } => {
                let _ = reply_to.tell(HandoffWorkerSnapshot {
                    shard: self.plan.shard.clone(),
                    phase: self.phase,
                    remaining: self.remaining.clone(),
                });
            }
        }
        Ok(())
    }
}

impl<M> HandoffWorkerActor<M>
where
    M: Send + 'static,
{
    fn start(
        &mut self,
        ctx: &mut Context<HandoffWorkerMsg<M>>,
        reply_to: ActorRef<HandoffWorkerDone>,
    ) -> Result<(), ActorError> {
        if self.phase != HandoffWorkerPhase::Idle {
            return Ok(());
        }

        self.reply_to = Some(reply_to);
        self.phase = HandoffWorkerPhase::AwaitingBeginAcks;
        self.remaining = self.plan.participants.clone();
        ctx.schedule_once_self(self.handoff_timeout, HandoffWorkerMsg::Timeout);

        if self.remaining.is_empty() {
            return self.send_local_handoff(ctx);
        }

        for region in self.plan.participants.clone() {
            let reply_region = region.clone();
            let reply_to = ctx.message_adapter(move |plan| HandoffWorkerMsg::BeginHandOffAck {
                region: reply_region.clone(),
                plan,
            })?;
            let report = self
                .transport
                .send_begin_handoff_to(&region, &self.plan.shard, reply_to);
            if !report.is_success() {
                return self.finish(ctx, false);
            }
        }
        Ok(())
    }

    fn apply_begin_ack(
        &mut self,
        ctx: &mut Context<HandoffWorkerMsg<M>>,
        region: String,
        plan: BeginHandOffPlan,
    ) -> Result<(), ActorError> {
        if self.phase != HandoffWorkerPhase::AwaitingBeginAcks {
            return Ok(());
        }

        let BeginHandOffPlan::Ack { shard, ack } = plan else {
            return self.finish(ctx, false);
        };
        if shard != self.plan.shard || ack.shard_id != self.plan.shard {
            return self.finish(ctx, false);
        }

        self.remaining.remove(&region);
        if self.remaining.is_empty() {
            self.send_local_handoff(ctx)?;
        }
        Ok(())
    }

    fn send_local_handoff(
        &mut self,
        ctx: &mut Context<HandoffWorkerMsg<M>>,
    ) -> Result<(), ActorError> {
        let Some(stop_message) = self.stop_message.take() else {
            return Ok(());
        };
        self.phase = HandoffWorkerPhase::AwaitingShardStopped;

        let handoff_reply_to =
            ctx.message_adapter(|plan| HandoffWorkerMsg::LocalHandOffForwarded { plan })?;
        let shard_reply_to =
            ctx.message_adapter(|plan| HandoffWorkerMsg::ShardHandOffObserved { plan })?;
        let report = self.transport.send_local_handoff_to(
            &self.plan.from_region,
            &self.plan.shard,
            stop_message,
            handoff_reply_to,
            shard_reply_to,
        );
        if !report.is_success() {
            return self.finish(ctx, false);
        }
        Ok(())
    }

    fn apply_local_handoff(
        &mut self,
        ctx: &mut Context<HandoffWorkerMsg<M>>,
        plan: RegionLocalHandOffPlan,
    ) -> Result<(), ActorError> {
        if self.phase != HandoffWorkerPhase::AwaitingShardStopped {
            return Ok(());
        }

        match plan {
            RegionLocalHandOffPlan::ForwardedToLocalShard { shard, .. }
                if shard == self.plan.shard =>
            {
                // The forwarded wrapper only confirms that the region sent the
                // handoff to the local shard. The shard handoff plan decides
                // whether completion is immediate or stopper-based.
            }
            RegionLocalHandOffPlan::ReplyShardStopped { shard, .. } if shard == self.plan.shard => {
                self.finish(ctx, true)?;
            }
            _ => self.finish(ctx, false)?,
        }
        Ok(())
    }

    fn apply_shard_handoff_observed(
        &mut self,
        ctx: &mut Context<HandoffWorkerMsg<M>>,
        plan: ShardHandOffPlan<M>,
    ) -> Result<(), ActorError> {
        if self.phase != HandoffWorkerPhase::AwaitingShardStopped {
            return Ok(());
        }

        match plan {
            ShardHandOffPlan::ReplyShardStopped { shard, .. }
            | ShardHandOffPlan::StopImmediately { shard, .. }
                if shard == self.plan.shard =>
            {
                self.finish(ctx, true)?;
            }
            ShardHandOffPlan::StartEntityStopper { shard, .. } if shard == self.plan.shard => {
                let reply_to =
                    ctx.message_adapter(|plan| HandoffWorkerMsg::LocalHandOffCompleted { plan })?;
                let report = self.transport.send_complete_local_handoff_to(
                    &self.plan.from_region,
                    &self.plan.shard,
                    self.handoff_timeout,
                    reply_to,
                );
                if !report.is_success() {
                    self.finish(ctx, false)?;
                }
            }
            ShardHandOffPlan::AlreadyInProgress { shard } if shard == self.plan.shard => {
                self.finish(ctx, false)?;
            }
            _ => self.finish(ctx, false)?,
        }
        Ok(())
    }

    fn apply_local_handoff_completion(
        &mut self,
        ctx: &mut Context<HandoffWorkerMsg<M>>,
        plan: RegionLocalHandOffCompletionPlan,
    ) -> Result<(), ActorError> {
        if self.phase != HandoffWorkerPhase::AwaitingShardStopped {
            return Ok(());
        }

        match plan {
            RegionLocalHandOffCompletionPlan::Completed { shard, stopped }
                if shard == self.plan.shard && stopped.shard_id == self.plan.shard =>
            {
                self.finish(ctx, true)
            }
            _ => self.finish(ctx, false),
        }
    }

    fn apply_remote_shard_stopped(
        &mut self,
        ctx: &mut Context<HandoffWorkerMsg<M>>,
        region: RegionId,
        stopped: ShardStopped,
    ) -> Result<(), ActorError> {
        if self.phase != HandoffWorkerPhase::AwaitingShardStopped {
            return Ok(());
        }
        if region == self.plan.from_region && stopped.shard_id == self.plan.shard {
            return self.finish(ctx, true);
        }
        self.finish(ctx, false)
    }

    fn apply_region_terminated(
        &mut self,
        ctx: &mut Context<HandoffWorkerMsg<M>>,
        region: RegionId,
    ) -> Result<(), ActorError> {
        match self.phase {
            HandoffWorkerPhase::AwaitingBeginAcks => {
                self.remaining.remove(&region);
                if self.remaining.is_empty() {
                    self.send_local_handoff(ctx)?;
                }
            }
            HandoffWorkerPhase::AwaitingShardStopped if region == self.plan.from_region => {
                self.finish(ctx, true)?;
            }
            _ => {}
        }
        Ok(())
    }

    fn finish(
        &mut self,
        ctx: &mut Context<HandoffWorkerMsg<M>>,
        ok: bool,
    ) -> Result<(), ActorError> {
        if self.phase == HandoffWorkerPhase::Done {
            return Ok(());
        }

        self.phase = HandoffWorkerPhase::Done;
        self.remaining.clear();
        if let Some(reply_to) = self.reply_to.take() {
            let _ = reply_to.tell(HandoffWorkerDone {
                shard: self.plan.shard.clone(),
                ok,
            });
        }
        ctx.stop(ctx.myself())
    }
}
