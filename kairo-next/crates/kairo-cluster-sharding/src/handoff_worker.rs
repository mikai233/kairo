use std::collections::BTreeSet;
use std::time::Duration;

use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, Context, Props};

use crate::{
    BeginHandOffPlan, HandoffTransport, RegionId, RegionLocalHandOffCompletionPlan,
    RegionLocalHandOffPlan, ShardHandOffPlan, ShardId, ShardRebalancePlan,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandoffWorkerDone {
    pub shard: ShardId,
    pub ok: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandoffWorkerSnapshot {
    pub shard: ShardId,
    pub phase: HandoffWorkerPhase,
    pub remaining: BTreeSet<RegionId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandoffWorkerPhase {
    Idle,
    AwaitingBeginAcks,
    AwaitingShardStopped,
    Done,
}

pub enum HandoffWorkerMsg<M> {
    Start {
        reply_to: ActorRef<HandoffWorkerDone>,
    },
    BeginHandOffAck {
        region: RegionId,
        plan: BeginHandOffPlan,
    },
    LocalHandOffForwarded {
        plan: RegionLocalHandOffPlan,
    },
    ShardHandOffObserved {
        plan: ShardHandOffPlan<M>,
    },
    LocalHandOffCompleted {
        plan: RegionLocalHandOffCompletionPlan,
    },
    Timeout,
    GetState {
        reply_to: ActorRef<HandoffWorkerSnapshot>,
    },
}

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
            HandoffWorkerMsg::ShardHandOffObserved { plan: _ } => {}
            HandoffWorkerMsg::LocalHandOffCompleted { plan } => {
                self.apply_local_handoff_completion(ctx, plan)?;
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
            RegionLocalHandOffPlan::ReplyShardStopped { shard, .. } if shard == self.plan.shard => {
                self.finish(ctx, true)?;
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
        match plan {
            RegionLocalHandOffCompletionPlan::Completed { shard, .. }
                if shard == self.plan.shard =>
            {
                self.finish(ctx, true)
            }
            _ => self.finish(ctx, false),
        }
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
