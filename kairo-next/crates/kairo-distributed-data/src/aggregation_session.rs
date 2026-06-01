use std::sync::Arc;
use std::time::Duration;

use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, Context, Props};
use kairo_serialization::ActorRefWireData;

use crate::{
    AggregationTransport, AggregationTransportReport, CrdtDataCodec, DataEnvelope,
    DeltaReplicatedData, GetResponse, ReadAggregationActor, ReadAggregationActorEvent,
    ReadAggregationActorMsg, ReadAggregationOperationEvent, ReadAggregationOutcome,
    ReadAggregationPlan, ReplicaId, ReplicatedDelta, ReplicatorKey, UpdateOutcome, UpdateResponse,
    WriteAggregationActor, WriteAggregationActorEvent, WriteAggregationActorMsg,
    WriteAggregationOutcome, WriteAggregationPlan,
};

#[derive(Debug, Clone)]
pub enum WriteAggregationSessionEvent {
    Started {
        reply_to: ActorRef<WriteAggregationActorMsg>,
        report: AggregationTransportReport,
    },
    RetryFullState {
        key: ReplicatorKey,
        replica: ReplicaId,
        report: AggregationTransportReport,
    },
    Completed(WriteAggregationOutcome),
}

pub enum WriteAggregationSessionMsg {
    Aggregation(WriteAggregationActorEvent),
}

pub struct WriteAggregationSession<D, Codec>
where
    D: DeltaReplicatedData + Send + 'static,
    D::Delta: Send + 'static,
{
    plan: WriteAggregationPlan,
    envelope: DataEnvelope<D>,
    outcome: Option<UpdateOutcome<D::Delta>>,
    transport: AggregationTransport<Codec>,
    timeout: Duration,
    reply_to: ActorRef<UpdateResponse<D::Delta>>,
    events: Option<ActorRef<WriteAggregationSessionEvent>>,
    sender: Option<ActorRefWireData>,
}

impl<D, Codec> WriteAggregationSession<D, Codec>
where
    D: DeltaReplicatedData + Send + 'static,
    D::Delta: Send + 'static,
{
    pub fn new(
        plan: WriteAggregationPlan,
        envelope: DataEnvelope<D>,
        outcome: UpdateOutcome<D::Delta>,
        transport: AggregationTransport<Codec>,
        timeout: Duration,
        reply_to: ActorRef<UpdateResponse<D::Delta>>,
    ) -> Self {
        Self {
            plan,
            envelope,
            outcome: Some(outcome),
            transport,
            timeout,
            reply_to,
            events: None,
            sender: None,
        }
    }

    pub fn with_events(
        plan: WriteAggregationPlan,
        envelope: DataEnvelope<D>,
        outcome: UpdateOutcome<D::Delta>,
        transport: AggregationTransport<Codec>,
        timeout: Duration,
        reply_to: ActorRef<UpdateResponse<D::Delta>>,
        events: ActorRef<WriteAggregationSessionEvent>,
    ) -> Self {
        Self {
            plan,
            envelope,
            outcome: Some(outcome),
            transport,
            timeout,
            reply_to,
            events: Some(events),
            sender: None,
        }
    }
}

impl<D, Codec> Actor for WriteAggregationSession<D, Codec>
where
    D: DeltaReplicatedData + Send + 'static,
    D::Delta: ReplicatedDelta + Send + 'static,
    Codec: CrdtDataCodec<D> + Clone + Send + 'static,
{
    type Msg = WriteAggregationSessionMsg;

    fn started(&mut self, ctx: &mut Context<Self::Msg>) -> ActorResult {
        let events = ctx.message_adapter(WriteAggregationSessionMsg::Aggregation)?;
        let aggregator = ctx.spawn_anonymous(Props::new({
            let plan = self.plan.clone();
            let timeout = self.timeout;
            move || WriteAggregationActor::with_timeout(plan, timeout, events)
        }))?;
        let sender = actor_ref_wire_data(&aggregator)?;
        self.sender = Some(sender.clone());
        let report = self
            .transport
            .publish_write_with_sender(&self.plan, &self.envelope, &sender);
        self.emit(WriteAggregationSessionEvent::Started {
            reply_to: aggregator,
            report,
        })
    }

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            WriteAggregationSessionMsg::Aggregation(event) => self.receive_event(ctx, event),
        }
    }
}

impl<D, Codec> WriteAggregationSession<D, Codec>
where
    D: DeltaReplicatedData + Send + 'static,
    D::Delta: ReplicatedDelta + Send + 'static,
    Codec: CrdtDataCodec<D> + Clone + Send + 'static,
{
    fn receive_event(
        &mut self,
        ctx: &mut Context<WriteAggregationSessionMsg>,
        event: WriteAggregationActorEvent,
    ) -> ActorResult {
        match event {
            WriteAggregationActorEvent::RetryFullState { replica } => {
                let report = if let Some(sender) = &self.sender {
                    self.transport.publish_write_to_replicas_with_sender(
                        std::slice::from_ref(&replica),
                        &self.plan,
                        &self.envelope,
                        sender,
                    )
                } else {
                    self.transport.publish_write_to_replicas(
                        std::slice::from_ref(&replica),
                        &self.plan,
                        &self.envelope,
                    )
                };
                self.emit(WriteAggregationSessionEvent::RetryFullState {
                    key: self.plan.state().key().clone(),
                    replica,
                    report,
                })
            }
            WriteAggregationActorEvent::Completed(outcome) => {
                self.emit(WriteAggregationSessionEvent::Completed(outcome.clone()))?;
                let response = crate::aggregation_operation::write_aggregation_response(
                    self.plan.state().key(),
                    &mut self.outcome,
                    outcome,
                );
                tell_or_actor_error(&self.reply_to, response)?;
                ctx.stop(ctx.myself())
            }
        }
    }

    fn emit(&self, event: WriteAggregationSessionEvent) -> ActorResult {
        if let Some(events) = &self.events {
            tell_or_actor_error(events, event)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum ReadAggregationSessionEvent {
    Started {
        reply_to: ActorRef<ReadAggregationActorMsg>,
        report: AggregationTransportReport,
    },
    DecodeFailed(ReadAggregationOperationEvent),
    Completed(ReadAggregationSessionOutcome),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadAggregationSessionOutcome {
    InProgress,
    Success,
    NotFound,
    Failure { required: usize, received: usize },
}

impl<D> From<&ReadAggregationOutcome<D>> for ReadAggregationSessionOutcome {
    fn from(value: &ReadAggregationOutcome<D>) -> Self {
        match value {
            ReadAggregationOutcome::InProgress => Self::InProgress,
            ReadAggregationOutcome::Success { .. } => Self::Success,
            ReadAggregationOutcome::NotFound => Self::NotFound,
            ReadAggregationOutcome::Failure { required, received } => Self::Failure {
                required: *required,
                received: *received,
            },
        }
    }
}

pub enum ReadAggregationSessionMsg<D>
where
    D: DeltaReplicatedData + Send + 'static,
{
    Aggregation(ReadAggregationActorEvent<D>),
}

pub struct ReadAggregationSession<D, Codec>
where
    D: DeltaReplicatedData + Send + 'static,
{
    key: ReplicatorKey,
    plan: ReadAggregationPlan<D>,
    data_codec: Arc<dyn CrdtDataCodec<D> + Send + Sync>,
    transport: AggregationTransport<Codec>,
    timeout: Duration,
    reply_to: ActorRef<GetResponse<D>>,
    events: Option<ActorRef<ReadAggregationSessionEvent>>,
    sender: Option<ActorRefWireData>,
}

impl<D, Codec> ReadAggregationSession<D, Codec>
where
    D: DeltaReplicatedData + Send + 'static,
{
    pub fn new(
        plan: ReadAggregationPlan<D>,
        data_codec: Arc<dyn CrdtDataCodec<D> + Send + Sync>,
        transport: AggregationTransport<Codec>,
        timeout: Duration,
        reply_to: ActorRef<GetResponse<D>>,
    ) -> Self {
        Self {
            key: plan.state().key().clone(),
            plan,
            data_codec,
            transport,
            timeout,
            reply_to,
            events: None,
            sender: None,
        }
    }

    pub fn with_events(
        plan: ReadAggregationPlan<D>,
        data_codec: Arc<dyn CrdtDataCodec<D> + Send + Sync>,
        transport: AggregationTransport<Codec>,
        timeout: Duration,
        reply_to: ActorRef<GetResponse<D>>,
        events: ActorRef<ReadAggregationSessionEvent>,
    ) -> Self {
        Self {
            key: plan.state().key().clone(),
            plan,
            data_codec,
            transport,
            timeout,
            reply_to,
            events: Some(events),
            sender: None,
        }
    }
}

impl<D, Codec> Actor for ReadAggregationSession<D, Codec>
where
    D: DeltaReplicatedData + Send + 'static,
    Codec: Clone + Send + 'static,
{
    type Msg = ReadAggregationSessionMsg<D>;

    fn started(&mut self, ctx: &mut Context<Self::Msg>) -> ActorResult {
        let events = ctx.message_adapter(ReadAggregationSessionMsg::Aggregation)?;
        let aggregator = ctx.spawn_anonymous(Props::new({
            let plan = self.plan.clone();
            let codec = Arc::clone(&self.data_codec);
            let timeout = self.timeout;
            move || ReadAggregationActor::with_timeout(plan, codec, timeout, events)
        }))?;
        let sender = actor_ref_wire_data(&aggregator)?;
        self.sender = Some(sender.clone());
        let report = self.transport.publish_read_with_sender(&self.plan, &sender);
        self.emit(ReadAggregationSessionEvent::Started {
            reply_to: aggregator,
            report,
        })
    }

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            ReadAggregationSessionMsg::Aggregation(event) => self.receive_event(ctx, event),
        }
    }
}

impl<D, Codec> ReadAggregationSession<D, Codec>
where
    D: DeltaReplicatedData + Send + 'static,
    Codec: Clone + Send + 'static,
{
    fn receive_event(
        &mut self,
        ctx: &mut Context<ReadAggregationSessionMsg<D>>,
        event: ReadAggregationActorEvent<D>,
    ) -> ActorResult {
        match event {
            ReadAggregationActorEvent::DecodeFailed { replica, reason } => {
                self.emit(ReadAggregationSessionEvent::DecodeFailed(
                    ReadAggregationOperationEvent::DecodeFailed {
                        key: self.key.clone(),
                        replica,
                        reason,
                    },
                ))
            }
            ReadAggregationActorEvent::Completed(outcome) => {
                self.emit(ReadAggregationSessionEvent::Completed(
                    ReadAggregationSessionOutcome::from(&outcome),
                ))?;
                let response =
                    crate::aggregation_operation::read_aggregation_response(&self.key, outcome);
                tell_or_actor_error(&self.reply_to, response)?;
                ctx.stop(ctx.myself())
            }
        }
    }

    fn emit(&self, event: ReadAggregationSessionEvent) -> ActorResult {
        if let Some(events) = &self.events {
            tell_or_actor_error(events, event)?;
        }
        Ok(())
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

fn actor_ref_wire_data<M>(actor: &ActorRef<M>) -> Result<ActorRefWireData, ActorError>
where
    M: Send + 'static,
{
    ActorRefWireData::new(actor.path().to_string()).map_err(|error| {
        ActorError::Message(format!(
            "failed to encode aggregation reply actor ref {}: {error}",
            actor.path()
        ))
    })
}

#[cfg(test)]
mod tests;
