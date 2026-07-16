use kairo_actor::{Actor, ActorRef, ActorResult, Context};

use super::response::{read_aggregation_response, tell_or_actor_error};
use crate::{
    DeltaReplicatedData, GetResponse, ReadAggregationActorEvent, ReadAggregationOutcome, ReplicaId,
    ReplicatorKey,
};

#[derive(Debug, Clone, PartialEq, Eq)]
/// Non-terminal diagnostic emitted while adapting a read aggregation.
pub enum ReadAggregationOperationEvent {
    /// One remote result could not be decoded and was not counted.
    DecodeFailed {
        /// Key being read.
        key: ReplicatorKey,
        /// Replica that sent the undecodable result.
        replica: ReplicaId,
        /// Human-readable codec failure.
        reason: String,
    },
}

/// Input protocol for a read aggregation operation adapter.
pub enum ReadAggregationOperationMsg<D>
where
    D: DeltaReplicatedData + Send + 'static,
{
    /// Event emitted by the underlying read aggregation actor.
    Aggregation(ReadAggregationActorEvent<D>),
}

/// Maps a read aggregation actor's events into one typed client response.
///
/// Decode failures can be mirrored to an optional diagnostic target without
/// completing the operation. A terminal outcome produces exactly one
/// [`GetResponse`] and stops the adapter.
pub struct ReadAggregationOperation<D>
where
    D: DeltaReplicatedData + Send + 'static,
{
    key: ReplicatorKey,
    reply_to: ActorRef<GetResponse<D>>,
    events: Option<ActorRef<ReadAggregationOperationEvent>>,
}

impl<D> ReadAggregationOperation<D>
where
    D: DeltaReplicatedData + Send + 'static,
{
    /// Creates an adapter without diagnostic event publication.
    pub fn new(key: ReplicatorKey, reply_to: ActorRef<GetResponse<D>>) -> Self {
        Self {
            key,
            reply_to,
            events: None,
        }
    }

    /// Creates an adapter that mirrors decode failures to `events`.
    pub fn with_events(
        key: ReplicatorKey,
        reply_to: ActorRef<GetResponse<D>>,
        events: ActorRef<ReadAggregationOperationEvent>,
    ) -> Self {
        Self {
            key,
            reply_to,
            events: Some(events),
        }
    }
}

impl<D> Actor for ReadAggregationOperation<D>
where
    D: DeltaReplicatedData + Send + 'static,
{
    type Msg = ReadAggregationOperationMsg<D>;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            ReadAggregationOperationMsg::Aggregation(event) => self.receive_event(ctx, event),
        }
    }
}

impl<D> ReadAggregationOperation<D>
where
    D: DeltaReplicatedData + Send + 'static,
{
    fn receive_event(
        &mut self,
        ctx: &mut Context<ReadAggregationOperationMsg<D>>,
        event: ReadAggregationActorEvent<D>,
    ) -> ActorResult {
        match event {
            ReadAggregationActorEvent::DecodeFailed { replica, reason } => {
                if let Some(events) = &self.events {
                    tell_or_actor_error(
                        events,
                        ReadAggregationOperationEvent::DecodeFailed {
                            key: self.key.clone(),
                            replica,
                            reason,
                        },
                    )?;
                }
                Ok(())
            }
            ReadAggregationActorEvent::Completed(outcome) => {
                let response = self.response_for(outcome);
                tell_or_actor_error(&self.reply_to, response)?;
                ctx.stop(ctx.myself())
            }
        }
    }

    fn response_for(&self, outcome: ReadAggregationOutcome<D>) -> GetResponse<D> {
        read_aggregation_response(&self.key, outcome)
    }
}
