use kairo_actor::{Actor, ActorRef, ActorResult, Context};

use super::response::{tell_or_actor_error, write_aggregation_response};
use crate::{
    ReplicaId, ReplicatedDelta, ReplicatorKey, UpdateOutcome, UpdateResponse,
    WriteAggregationActorEvent, WriteAggregationOutcome,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteAggregationOperationEvent {
    RetryFullState {
        key: ReplicatorKey,
        replica: ReplicaId,
    },
}

pub enum WriteAggregationOperationMsg {
    Aggregation(WriteAggregationActorEvent),
}

pub struct WriteAggregationOperation<Delta>
where
    Delta: ReplicatedDelta + Send + 'static,
{
    key: ReplicatorKey,
    outcome: Option<UpdateOutcome<Delta>>,
    reply_to: ActorRef<UpdateResponse<Delta>>,
    events: Option<ActorRef<WriteAggregationOperationEvent>>,
}

impl<Delta> WriteAggregationOperation<Delta>
where
    Delta: ReplicatedDelta + Send + 'static,
{
    pub fn new(outcome: UpdateOutcome<Delta>, reply_to: ActorRef<UpdateResponse<Delta>>) -> Self {
        Self {
            key: outcome.key().clone(),
            outcome: Some(outcome),
            reply_to,
            events: None,
        }
    }

    pub fn with_events(
        outcome: UpdateOutcome<Delta>,
        reply_to: ActorRef<UpdateResponse<Delta>>,
        events: ActorRef<WriteAggregationOperationEvent>,
    ) -> Self {
        Self {
            key: outcome.key().clone(),
            outcome: Some(outcome),
            reply_to,
            events: Some(events),
        }
    }
}

impl<Delta> Actor for WriteAggregationOperation<Delta>
where
    Delta: ReplicatedDelta + Send + 'static,
{
    type Msg = WriteAggregationOperationMsg;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            WriteAggregationOperationMsg::Aggregation(event) => self.receive_event(ctx, event),
        }
    }
}

impl<Delta> WriteAggregationOperation<Delta>
where
    Delta: ReplicatedDelta + Send + 'static,
{
    fn receive_event(
        &mut self,
        ctx: &mut Context<WriteAggregationOperationMsg>,
        event: WriteAggregationActorEvent,
    ) -> ActorResult {
        match event {
            WriteAggregationActorEvent::RetryFullState { replica } => {
                if let Some(events) = &self.events {
                    tell_or_actor_error(
                        events,
                        WriteAggregationOperationEvent::RetryFullState {
                            key: self.key.clone(),
                            replica,
                        },
                    )?;
                }
                Ok(())
            }
            WriteAggregationActorEvent::Completed(outcome) => {
                let response = self.response_for(outcome);
                tell_or_actor_error(&self.reply_to, response)?;
                ctx.stop(ctx.myself())
            }
        }
    }

    fn response_for(&mut self, outcome: WriteAggregationOutcome) -> UpdateResponse<Delta> {
        write_aggregation_response(&self.key, &mut self.outcome, outcome)
    }
}
