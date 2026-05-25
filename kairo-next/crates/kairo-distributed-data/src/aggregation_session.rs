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
mod tests {
    use std::collections::BTreeSet;
    use std::sync::mpsc::{self, Receiver};

    use kairo_actor::{ActorRef, ActorSystem, Recipient};

    use super::*;
    use crate::{
        DataEnvelope, GCounter, GCounterCodec, ReadAggregatorState, ReadConsistency,
        ReplicatorRead, ReplicatorWireReply, ReplicatorWrite, ReplicatorWriteAck,
        WriteAggregatorState, WriteConsistency, encode_read_result,
    };

    struct Forward<M> {
        tx: mpsc::Sender<M>,
    }

    impl<M> Actor for Forward<M>
    where
        M: Send + 'static,
    {
        type Msg = M;

        fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
            self.tx
                .send(msg)
                .map_err(|error| ActorError::Message(error.to_string()))
        }
    }

    fn probe<M>(system: &ActorSystem, name: &str) -> (ActorRef<M>, Receiver<M>)
    where
        M: Send + 'static,
    {
        let (tx, rx) = mpsc::channel();
        let actor = system
            .spawn(name, Props::new(move || Forward { tx }))
            .unwrap();
        (actor, rx)
    }

    #[derive(Clone)]
    struct SendToActor<M> {
        target: ActorRef<M>,
    }

    impl<M> Recipient<M> for SendToActor<M>
    where
        M: Send + 'static,
    {
        fn tell(&self, message: M) -> Result<(), kairo_actor::SendError<M>> {
            self.target.tell(message)
        }
    }

    fn replica(id: &str) -> ReplicaId {
        ReplicaId::new(id)
    }

    fn counter(replica_id: &str, value: u128) -> GCounter {
        GCounter::new()
            .increment(replica(replica_id), value)
            .unwrap()
            .reset_delta()
    }

    fn aggregation_target(
        replica_id: &str,
        write_ref: ActorRef<ReplicatorWrite>,
        read_ref: ActorRef<ReplicatorRead>,
    ) -> crate::AggregationTarget {
        crate::AggregationTarget::new(
            replica(replica_id),
            SendToActor { target: write_ref },
            SendToActor { target: read_ref },
        )
    }

    #[test]
    fn write_session_spawns_aggregator_sends_primary_and_replies_on_success() {
        let system = ActorSystem::builder("ddata-write-aggregation-session")
            .build()
            .unwrap();
        let (write_ref, write_rx) = probe::<ReplicatorWrite>(&system, "writes");
        let (read_ref, _read_rx) = probe::<ReplicatorRead>(&system, "reads");
        let (reply_to, replies) = probe::<UpdateResponse<GCounter>>(&system, "replies");
        let (events_ref, events) = probe::<WriteAggregationSessionEvent>(&system, "events");
        let key = ReplicatorKey::new("counter");
        let remote = replica("a");
        let state = WriteAggregatorState::new(
            key.clone(),
            &WriteConsistency::to(2, Duration::from_secs(1)).unwrap(),
            vec![remote.clone()],
        )
        .unwrap();
        let plan =
            WriteAggregationPlan::new(state.clone(), state.select_replicas(&BTreeSet::new()));
        let envelope = DataEnvelope::new(counter("local", 7));
        let outcome = UpdateOutcome::new(key.clone(), true, Some(counter("local", 7)));
        let mut transport = AggregationTransport::new(replica("local"), GCounterCodec);
        transport.insert_target(aggregation_target("a", write_ref, read_ref));

        system
            .spawn(
                "write-session",
                Props::new({
                    let events_ref = events_ref.clone();
                    let reply_to = reply_to.clone();
                    let plan = plan.clone();
                    let envelope = envelope.clone();
                    let transport = transport.clone();
                    move || {
                        WriteAggregationSession::with_events(
                            plan,
                            envelope,
                            outcome,
                            transport,
                            Duration::from_secs(5),
                            reply_to,
                            events_ref,
                        )
                    }
                }),
            )
            .unwrap();

        let started = events.recv_timeout(Duration::from_secs(1)).unwrap();
        let reply_actor = match started {
            WriteAggregationSessionEvent::Started { reply_to, report } => {
                assert_eq!(report.sent_to(), std::slice::from_ref(&remote));
                reply_to
            }
            other => panic!("expected session start, got {other:?}"),
        };
        assert_eq!(
            write_rx.recv_timeout(Duration::from_secs(1)).unwrap().key,
            key.as_str()
        );

        reply_actor
            .tell(WriteAggregationActorMsg::Reply(
                ReplicatorWireReply::WriteAck {
                    from: remote,
                    message: ReplicatorWriteAck,
                },
            ))
            .unwrap();
        assert!(matches!(
            replies.recv_timeout(Duration::from_secs(1)).unwrap(),
            UpdateResponse::Success(success) if success.key() == &key
        ));
        system.terminate(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn write_session_retries_full_state_on_delta_nack() {
        let system = ActorSystem::builder("ddata-write-aggregation-session-retry")
            .build()
            .unwrap();
        let (write_ref, write_rx) = probe::<ReplicatorWrite>(&system, "writes");
        let (read_ref, _read_rx) = probe::<ReplicatorRead>(&system, "reads");
        let (reply_to, _replies) = probe::<UpdateResponse<GCounter>>(&system, "replies");
        let (events_ref, events) = probe::<WriteAggregationSessionEvent>(&system, "events");
        let key = ReplicatorKey::new("counter");
        let remote = replica("a");
        let state = WriteAggregatorState::new(
            key.clone(),
            &WriteConsistency::to(2, Duration::from_secs(1)).unwrap(),
            vec![remote.clone()],
        )
        .unwrap();
        let plan =
            WriteAggregationPlan::new(state.clone(), state.select_replicas(&BTreeSet::new()));
        let envelope = DataEnvelope::new(counter("local", 3));
        let outcome = UpdateOutcome::new(key.clone(), true, Some(counter("local", 3)));
        let mut transport = AggregationTransport::new(replica("local"), GCounterCodec);
        transport.insert_target(aggregation_target("a", write_ref, read_ref));

        system
            .spawn(
                "write-session",
                Props::new({
                    let events_ref = events_ref.clone();
                    let reply_to = reply_to.clone();
                    let transport = transport.clone();
                    move || {
                        WriteAggregationSession::with_events(
                            plan,
                            envelope,
                            outcome,
                            transport,
                            Duration::from_secs(5),
                            reply_to,
                            events_ref,
                        )
                    }
                }),
            )
            .unwrap();

        let reply_actor = match events.recv_timeout(Duration::from_secs(1)).unwrap() {
            WriteAggregationSessionEvent::Started { reply_to, .. } => reply_to,
            other => panic!("expected session start, got {other:?}"),
        };
        write_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        reply_actor
            .tell(WriteAggregationActorMsg::Reply(
                ReplicatorWireReply::DeltaNack {
                    from: remote.clone(),
                    message: crate::ReplicatorDeltaNack,
                },
            ))
            .unwrap();

        match events.recv_timeout(Duration::from_secs(1)).unwrap() {
            WriteAggregationSessionEvent::RetryFullState {
                key: retry_key,
                replica,
                report,
            } => {
                assert_eq!(retry_key, key);
                assert_eq!(replica, remote);
                assert_eq!(report.sent_to(), &[replica]);
            }
            other => panic!("expected retry event, got {other:?}"),
        }
        assert_eq!(
            write_rx.recv_timeout(Duration::from_secs(1)).unwrap().key,
            key.as_str()
        );
        system.terminate(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn read_session_spawns_aggregator_sends_primary_and_maps_result() {
        let system = ActorSystem::builder("ddata-read-aggregation-session")
            .build()
            .unwrap();
        let (write_ref, _write_rx) = probe::<ReplicatorWrite>(&system, "writes");
        let (read_ref, read_rx) = probe::<ReplicatorRead>(&system, "reads");
        let (reply_to, replies) = probe::<GetResponse<GCounter>>(&system, "replies");
        let (events_ref, events) = probe::<ReadAggregationSessionEvent>(&system, "events");
        let key = ReplicatorKey::new("counter");
        let remote = replica("a");
        let state = ReadAggregatorState::new(
            key.clone(),
            &ReadConsistency::from(2, Duration::from_secs(1)).unwrap(),
            vec![remote.clone()],
            None,
        )
        .unwrap();
        let plan = ReadAggregationPlan::new(state.clone(), state.select_replicas(&BTreeSet::new()));
        let mut transport = AggregationTransport::new(replica("local"), GCounterCodec);
        transport.insert_target(aggregation_target("a", write_ref, read_ref));

        system
            .spawn(
                "read-session",
                Props::new({
                    let events_ref = events_ref.clone();
                    let reply_to = reply_to.clone();
                    let transport = transport.clone();
                    move || {
                        ReadAggregationSession::with_events(
                            plan,
                            Arc::new(GCounterCodec),
                            transport,
                            Duration::from_secs(5),
                            reply_to,
                            events_ref,
                        )
                    }
                }),
            )
            .unwrap();

        let reply_actor = match events.recv_timeout(Duration::from_secs(1)).unwrap() {
            ReadAggregationSessionEvent::Started { reply_to, report } => {
                assert_eq!(report.sent_to(), std::slice::from_ref(&remote));
                reply_to
            }
            other => panic!("expected session start, got {other:?}"),
        };
        assert_eq!(
            read_rx.recv_timeout(Duration::from_secs(1)).unwrap().key,
            key.as_str()
        );

        reply_actor
            .tell(ReadAggregationActorMsg::Reply(
                ReplicatorWireReply::ReadResult {
                    from: remote,
                    message: encode_read_result(
                        Some(&DataEnvelope::new(counter("a", 9))),
                        &GCounterCodec,
                    )
                    .unwrap(),
                },
            ))
            .unwrap();
        match replies.recv_timeout(Duration::from_secs(1)).unwrap() {
            GetResponse::Success {
                key: success_key,
                data,
            } => {
                assert_eq!(success_key, key);
                assert_eq!(data.value().unwrap(), 9);
            }
            other => panic!("expected get success, got {other:?}"),
        }
        system.terminate(Duration::from_secs(1)).unwrap();
    }
}
