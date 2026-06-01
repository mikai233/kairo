use std::sync::Arc;
use std::time::Duration;

use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, Context};

use crate::{
    CrdtDataCodec, DeltaReplicatedData, ReadAggregationOutcome, ReadAggregationPlan,
    ReadAggregatorState, ReplicatorWireReply, WriteAggregationOutcome, WriteAggregationPlan,
    WriteAggregatorState, decode_read_result,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteAggregationActorEvent {
    RetryFullState { replica: crate::ReplicaId },
    Completed(WriteAggregationOutcome),
}

pub enum WriteAggregationActorMsg {
    Reply(ReplicatorWireReply),
    Timeout,
}

pub struct WriteAggregationActor {
    state: WriteAggregatorState,
    timeout: Option<Duration>,
    events: ActorRef<WriteAggregationActorEvent>,
}

impl WriteAggregationActor {
    pub fn new(plan: WriteAggregationPlan, events: ActorRef<WriteAggregationActorEvent>) -> Self {
        Self {
            state: plan.into_state(),
            timeout: None,
            events,
        }
    }

    pub fn with_timeout(
        plan: WriteAggregationPlan,
        timeout: Duration,
        events: ActorRef<WriteAggregationActorEvent>,
    ) -> Self {
        Self {
            state: plan.into_state(),
            timeout: Some(timeout),
            events,
        }
    }

    pub fn state(&self) -> &WriteAggregatorState {
        &self.state
    }
}

impl Actor for WriteAggregationActor {
    type Msg = WriteAggregationActorMsg;

    fn started(&mut self, ctx: &mut Context<Self::Msg>) -> ActorResult {
        if let Some(timeout) = self.timeout {
            ctx.schedule_once_self(timeout, WriteAggregationActorMsg::Timeout);
        }
        Ok(())
    }

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            WriteAggregationActorMsg::Reply(reply) => self.receive_reply(ctx, reply),
            WriteAggregationActorMsg::Timeout => {
                self.complete(ctx, self.state.timeout())?;
                Ok(())
            }
        }
    }
}

impl WriteAggregationActor {
    fn receive_reply(
        &mut self,
        ctx: &mut Context<WriteAggregationActorMsg>,
        reply: ReplicatorWireReply,
    ) -> ActorResult {
        match reply {
            ReplicatorWireReply::DeltaAck { from, .. }
            | ReplicatorWireReply::WriteAck { from, .. } => {
                let outcome = self.state.record_ack(&from);
                self.apply_outcome(ctx, outcome)
            }
            ReplicatorWireReply::WriteNack { from, .. } => {
                let outcome = self.state.record_nack(&from);
                self.apply_outcome(ctx, outcome)
            }
            ReplicatorWireReply::DeltaNack { from, .. } => tell_or_actor_error(
                &self.events,
                WriteAggregationActorEvent::RetryFullState { replica: from },
            ),
            ReplicatorWireReply::ReadResult { .. } => Ok(()),
        }
    }

    fn apply_outcome(
        &mut self,
        ctx: &mut Context<WriteAggregationActorMsg>,
        outcome: WriteAggregationOutcome,
    ) -> ActorResult {
        match outcome {
            WriteAggregationOutcome::InProgress => Ok(()),
            terminal => self.complete(ctx, terminal),
        }
    }

    fn complete(
        &self,
        ctx: &mut Context<WriteAggregationActorMsg>,
        outcome: WriteAggregationOutcome,
    ) -> ActorResult {
        tell_or_actor_error(&self.events, WriteAggregationActorEvent::Completed(outcome))?;
        ctx.stop(ctx.myself())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadAggregationActorEvent<D>
where
    D: DeltaReplicatedData + Send + 'static,
{
    Completed(ReadAggregationOutcome<D>),
    DecodeFailed {
        replica: crate::ReplicaId,
        reason: String,
    },
}

pub enum ReadAggregationActorMsg {
    Reply(ReplicatorWireReply),
    Timeout,
}

pub struct ReadAggregationActor<D>
where
    D: DeltaReplicatedData + Send + 'static,
{
    state: ReadAggregatorState<D>,
    codec: Arc<dyn CrdtDataCodec<D> + Send + Sync>,
    timeout: Option<Duration>,
    events: ActorRef<ReadAggregationActorEvent<D>>,
}

impl<D> ReadAggregationActor<D>
where
    D: DeltaReplicatedData + Send + 'static,
{
    pub fn new(
        plan: ReadAggregationPlan<D>,
        codec: Arc<dyn CrdtDataCodec<D> + Send + Sync>,
        events: ActorRef<ReadAggregationActorEvent<D>>,
    ) -> Self {
        Self {
            state: plan.into_state(),
            codec,
            timeout: None,
            events,
        }
    }

    pub fn with_timeout(
        plan: ReadAggregationPlan<D>,
        codec: Arc<dyn CrdtDataCodec<D> + Send + Sync>,
        timeout: Duration,
        events: ActorRef<ReadAggregationActorEvent<D>>,
    ) -> Self {
        Self {
            state: plan.into_state(),
            codec,
            timeout: Some(timeout),
            events,
        }
    }

    pub fn state(&self) -> &ReadAggregatorState<D> {
        &self.state
    }
}

impl<D> Actor for ReadAggregationActor<D>
where
    D: DeltaReplicatedData + Send + 'static,
{
    type Msg = ReadAggregationActorMsg;

    fn started(&mut self, ctx: &mut Context<Self::Msg>) -> ActorResult {
        if let Some(timeout) = self.timeout {
            ctx.schedule_once_self(timeout, ReadAggregationActorMsg::Timeout);
        }
        Ok(())
    }

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            ReadAggregationActorMsg::Reply(reply) => self.receive_reply(ctx, reply),
            ReadAggregationActorMsg::Timeout => {
                self.complete(ctx, self.state.timeout())?;
                Ok(())
            }
        }
    }
}

impl<D> ReadAggregationActor<D>
where
    D: DeltaReplicatedData + Send + 'static,
{
    fn receive_reply(
        &mut self,
        ctx: &mut Context<ReadAggregationActorMsg>,
        reply: ReplicatorWireReply,
    ) -> ActorResult {
        let ReplicatorWireReply::ReadResult { from, message } = reply else {
            return Ok(());
        };

        let envelope = match decode_read_result(&message, self.codec.as_ref()) {
            Ok(envelope) => envelope,
            Err(error) => {
                tell_or_actor_error(
                    &self.events,
                    ReadAggregationActorEvent::DecodeFailed {
                        replica: from,
                        reason: error.to_string(),
                    },
                )?;
                return Ok(());
            }
        };
        let outcome = self.state.record_read_from(&from, envelope);
        self.apply_outcome(ctx, outcome)
    }

    fn apply_outcome(
        &mut self,
        ctx: &mut Context<ReadAggregationActorMsg>,
        outcome: ReadAggregationOutcome<D>,
    ) -> ActorResult {
        match outcome {
            ReadAggregationOutcome::InProgress => Ok(()),
            terminal => self.complete(ctx, terminal),
        }
    }

    fn complete(
        &self,
        ctx: &mut Context<ReadAggregationActorMsg>,
        outcome: ReadAggregationOutcome<D>,
    ) -> ActorResult {
        tell_or_actor_error(&self.events, ReadAggregationActorEvent::Completed(outcome))?;
        ctx.stop(ctx.myself())
    }
}

fn tell_or_actor_error<M>(target: &ActorRef<M>, message: M) -> ActorResult
where
    M: Send + 'static,
{
    target
        .tell(message)
        .map_err(|error| ActorError::Message(error.reason().to_string()))
}

#[cfg(test)]
mod tests;
