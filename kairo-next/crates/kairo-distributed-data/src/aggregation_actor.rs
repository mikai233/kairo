#![deny(missing_docs)]
//! Typed actors that own remote read and write quorum completion.
//!
//! These actors consume addressed wire replies, update the pure aggregation
//! state machines, emit typed retry or completion events, and stop after a
//! terminal outcome. Optional deadlines are scheduled as actor self-messages.

use std::sync::Arc;
use std::time::Duration;

use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, Context};

use crate::{
    CrdtDataCodec, DeltaReplicatedData, ReadAggregationOutcome, ReadAggregationPlan,
    ReadAggregatorState, ReplicatorWireReply, WriteAggregationOutcome, WriteAggregationPlan,
    WriteAggregatorState, decode_read_result,
};

#[derive(Debug, Clone, PartialEq, Eq)]
/// Event emitted by a write aggregation actor.
pub enum WriteAggregationActorEvent {
    /// A causal delta gap requires retrying one replica with full state.
    RetryFullState {
        /// Replica that returned a delta NACK.
        replica: crate::ReplicaId,
    },
    /// The write aggregation reached a terminal outcome.
    Completed(WriteAggregationOutcome),
}

/// Input protocol for a write aggregation actor.
pub enum WriteAggregationActorMsg {
    /// One addressed replicator wire reply.
    Reply(ReplicatorWireReply),
    /// Deadline notification, normally scheduled by [`WriteAggregationActor::with_timeout`].
    Timeout,
}

/// Actor wrapper around [`WriteAggregatorState`].
///
/// ACK and NACK replies are counted once per known source. Delta NACKs emit a
/// full-state retry event without changing quorum state. Unrelated read replies
/// are ignored.
pub struct WriteAggregationActor {
    state: WriteAggregatorState,
    timeout: Option<Duration>,
    events: ActorRef<WriteAggregationActorEvent>,
}

impl WriteAggregationActor {
    /// Creates an actor without an automatic deadline.
    pub fn new(plan: WriteAggregationPlan, events: ActorRef<WriteAggregationActorEvent>) -> Self {
        Self {
            state: plan.into_state(),
            timeout: None,
            events,
        }
    }

    /// Creates an actor that schedules a timeout self-message after `timeout`.
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

    /// Returns the current pure write aggregation state.
    pub fn state(&self) -> &WriteAggregatorState {
        &self.state
    }
}

impl Actor for WriteAggregationActor {
    type Msg = WriteAggregationActorMsg;

    fn started(&mut self, ctx: &mut Context<Self::Msg>) -> ActorResult {
        match self.state.outcome() {
            WriteAggregationOutcome::InProgress => {
                if let Some(timeout) = self.timeout {
                    ctx.schedule_once_self(timeout, WriteAggregationActorMsg::Timeout);
                }
                Ok(())
            }
            terminal => self.complete(ctx, terminal),
        }
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
/// Event emitted by a read aggregation actor.
pub enum ReadAggregationActorEvent<D>
where
    D: DeltaReplicatedData + Send + 'static,
{
    /// The read aggregation reached a terminal outcome.
    Completed(ReadAggregationOutcome<D>),
    /// A result from one replica could not be decoded and was not counted.
    DecodeFailed {
        /// Replica that sent the invalid result.
        replica: crate::ReplicaId,
        /// Human-readable codec failure.
        reason: String,
    },
}

/// Input protocol for a read aggregation actor.
pub enum ReadAggregationActorMsg {
    /// One addressed replicator wire reply.
    Reply(ReplicatorWireReply),
    /// Deadline notification, normally scheduled by [`ReadAggregationActor::with_timeout`].
    Timeout,
}

/// Actor wrapper around [`ReadAggregatorState`].
///
/// Only decodable read results from distinct known sources advance the quorum.
/// Other wire reply kinds are ignored.
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
    /// Creates an actor without an automatic deadline.
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

    /// Creates an actor that schedules a timeout self-message after `timeout`.
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

    /// Returns the current pure read aggregation state.
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
        match self.state.outcome() {
            ReadAggregationOutcome::InProgress => {
                if let Some(timeout) = self.timeout {
                    ctx.schedule_once_self(timeout, ReadAggregationActorMsg::Timeout);
                }
                Ok(())
            }
            terminal => self.complete(ctx, terminal),
        }
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
