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
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use std::sync::mpsc::{self, Receiver};
    use std::time::Duration;

    use kairo_actor::{ActorRef, ActorSystem, Props};

    use super::*;
    use crate::{
        DataEnvelope, GCounter, GCounterCodec, ReadConsistency, ReplicaId, ReplicatorKey,
        ReplicatorReadResult, ReplicatorWireReply, ReplicatorWriteAck, ReplicatorWriteNack,
        WriteConsistency, encode_read_result,
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
                .map_err(|error| kairo_actor::ActorError::Message(error.to_string()))
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

    fn replica(id: &str) -> ReplicaId {
        ReplicaId::new(id)
    }

    fn counter(replica_id: &str, value: u128) -> GCounter {
        GCounter::new()
            .increment(replica(replica_id), value)
            .unwrap()
            .reset_delta()
    }

    fn write_plan(
        key: &ReplicatorKey,
        consistency: &WriteConsistency,
        remote_nodes: Vec<ReplicaId>,
    ) -> WriteAggregationPlan {
        let state = WriteAggregatorState::new(key.clone(), consistency, remote_nodes).unwrap();
        WriteAggregationPlan::new(state.clone(), state.select_replicas(&BTreeSet::new()))
    }

    fn read_plan(
        key: &ReplicatorKey,
        consistency: &ReadConsistency,
        remote_nodes: Vec<ReplicaId>,
        local_value: Option<DataEnvelope<GCounter>>,
    ) -> ReadAggregationPlan<GCounter> {
        let state =
            ReadAggregatorState::new(key.clone(), consistency, remote_nodes, local_value).unwrap();
        ReadAggregationPlan::new(state.clone(), state.select_replicas(&BTreeSet::new()))
    }

    #[test]
    fn write_aggregation_actor_tracks_retries_acks_and_completion() {
        let system = ActorSystem::builder("ddata-write-aggregation-actor")
            .build()
            .unwrap();
        let (events, event_rx) = probe::<WriteAggregationActorEvent>(&system, "events");
        let key = ReplicatorKey::new("counter");
        let actor = system
            .spawn(
                "write-aggregator",
                Props::new({
                    let events = events.clone();
                    move || {
                        WriteAggregationActor::new(
                            write_plan(
                                &key,
                                &WriteConsistency::to(3, Duration::from_secs(1)).unwrap(),
                                vec![replica("a"), replica("b")],
                            ),
                            events,
                        )
                    }
                }),
            )
            .unwrap();

        actor
            .tell(WriteAggregationActorMsg::Reply(
                ReplicatorWireReply::DeltaNack {
                    from: replica("a"),
                    message: crate::ReplicatorDeltaNack,
                },
            ))
            .unwrap();
        assert_eq!(
            event_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            WriteAggregationActorEvent::RetryFullState {
                replica: replica("a")
            }
        );

        actor
            .tell(WriteAggregationActorMsg::Reply(
                ReplicatorWireReply::DeltaAck {
                    from: replica("a"),
                    message: crate::ReplicatorDeltaAck,
                },
            ))
            .unwrap();
        assert!(event_rx.recv_timeout(Duration::from_millis(50)).is_err());
        actor
            .tell(WriteAggregationActorMsg::Reply(
                ReplicatorWireReply::WriteAck {
                    from: replica("b"),
                    message: ReplicatorWriteAck,
                },
            ))
            .unwrap();
        assert_eq!(
            event_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            WriteAggregationActorEvent::Completed(WriteAggregationOutcome::Success)
        );
        system.terminate(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn write_aggregation_actor_reports_nack_failure_and_timeout() {
        let system = ActorSystem::builder("ddata-write-aggregation-fail")
            .build()
            .unwrap();
        let (events, event_rx) = probe::<WriteAggregationActorEvent>(&system, "events");
        let key = ReplicatorKey::new("counter");
        let actor = system
            .spawn(
                "write-aggregator-fail",
                Props::new({
                    let events = events.clone();
                    let key = key.clone();
                    move || {
                        WriteAggregationActor::new(
                            write_plan(
                                &key,
                                &WriteConsistency::to(3, Duration::from_secs(1)).unwrap(),
                                vec![replica("a"), replica("b")],
                            ),
                            events,
                        )
                    }
                }),
            )
            .unwrap();

        actor
            .tell(WriteAggregationActorMsg::Reply(
                ReplicatorWireReply::WriteNack {
                    from: replica("a"),
                    message: ReplicatorWriteNack,
                },
            ))
            .unwrap();
        assert_eq!(
            event_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            WriteAggregationActorEvent::Completed(WriteAggregationOutcome::Failed {
                required: 2,
                available: 1
            })
        );

        let actor = system
            .spawn(
                "write-aggregator-timeout",
                Props::new({
                    let events = events.clone();
                    move || {
                        WriteAggregationActor::new(
                            write_plan(
                                &ReplicatorKey::new("timeout"),
                                &WriteConsistency::majority(Duration::from_secs(1)),
                                vec![replica("a"), replica("b")],
                            ),
                            events,
                        )
                    }
                }),
            )
            .unwrap();
        actor.tell(WriteAggregationActorMsg::Timeout).unwrap();
        assert_eq!(
            event_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            WriteAggregationActorEvent::Completed(WriteAggregationOutcome::Timeout {
                required: 1,
                acknowledged: 0
            })
        );
        system.terminate(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn read_aggregation_actor_merges_source_replies_once() {
        let system = ActorSystem::builder("ddata-read-aggregation-actor")
            .build()
            .unwrap();
        let (events, event_rx) = probe::<ReadAggregationActorEvent<GCounter>>(&system, "events");
        let key = ReplicatorKey::new("counter");
        let actor = system
            .spawn(
                "read-aggregator",
                Props::new({
                    let events = events.clone();
                    let key = key.clone();
                    move || {
                        ReadAggregationActor::new(
                            read_plan(
                                &key,
                                &ReadConsistency::from(3, Duration::from_secs(1)).unwrap(),
                                vec![replica("a"), replica("b")],
                                Some(DataEnvelope::new(counter("local", 1))),
                            ),
                            Arc::new(GCounterCodec),
                            events,
                        )
                    }
                }),
            )
            .unwrap();

        actor
            .tell(ReadAggregationActorMsg::Reply(
                ReplicatorWireReply::ReadResult {
                    from: replica("a"),
                    message: encode_read_result(
                        Some(&DataEnvelope::new(counter("a", 2))),
                        &GCounterCodec,
                    )
                    .unwrap(),
                },
            ))
            .unwrap();
        assert!(event_rx.recv_timeout(Duration::from_millis(50)).is_err());

        actor
            .tell(ReadAggregationActorMsg::Reply(
                ReplicatorWireReply::ReadResult {
                    from: replica("a"),
                    message: encode_read_result(
                        Some(&DataEnvelope::new(counter("duplicate", 100))),
                        &GCounterCodec,
                    )
                    .unwrap(),
                },
            ))
            .unwrap();
        assert!(event_rx.recv_timeout(Duration::from_millis(50)).is_err());

        actor
            .tell(ReadAggregationActorMsg::Reply(
                ReplicatorWireReply::ReadResult {
                    from: replica("b"),
                    message: encode_read_result(
                        Some(&DataEnvelope::new(counter("b", 3))),
                        &GCounterCodec,
                    )
                    .unwrap(),
                },
            ))
            .unwrap();

        match event_rx.recv_timeout(Duration::from_secs(1)).unwrap() {
            ReadAggregationActorEvent::Completed(ReadAggregationOutcome::Success { envelope }) => {
                assert_eq!(envelope.data().value().unwrap(), 6);
            }
            other => panic!("expected read success, got {other:?}"),
        }
        system.terminate(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn read_aggregation_actor_reports_not_found_timeout_and_decode_failures() {
        let system = ActorSystem::builder("ddata-read-aggregation-fail")
            .build()
            .unwrap();
        let (events, event_rx) = probe::<ReadAggregationActorEvent<GCounter>>(&system, "events");

        let actor = system
            .spawn(
                "read-not-found",
                Props::new({
                    let events = events.clone();
                    move || {
                        ReadAggregationActor::new(
                            read_plan(
                                &ReplicatorKey::new("missing"),
                                &ReadConsistency::from(2, Duration::from_secs(1)).unwrap(),
                                vec![replica("a")],
                                None,
                            ),
                            Arc::new(GCounterCodec),
                            events,
                        )
                    }
                }),
            )
            .unwrap();
        actor
            .tell(ReadAggregationActorMsg::Reply(
                ReplicatorWireReply::ReadResult {
                    from: replica("a"),
                    message: ReplicatorReadResult { envelope: None },
                },
            ))
            .unwrap();
        assert_eq!(
            event_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            ReadAggregationActorEvent::Completed(ReadAggregationOutcome::NotFound)
        );

        let actor = system
            .spawn(
                "read-timeout",
                Props::new({
                    let events = events.clone();
                    move || {
                        ReadAggregationActor::new(
                            read_plan(
                                &ReplicatorKey::new("timeout"),
                                &ReadConsistency::from(3, Duration::from_secs(1)).unwrap(),
                                vec![replica("a"), replica("b")],
                                None,
                            ),
                            Arc::new(GCounterCodec),
                            events,
                        )
                    }
                }),
            )
            .unwrap();
        actor.tell(ReadAggregationActorMsg::Timeout).unwrap();
        assert_eq!(
            event_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            ReadAggregationActorEvent::Completed(ReadAggregationOutcome::Failure {
                required: 2,
                received: 0
            })
        );

        let actor = system
            .spawn(
                "read-decode-fail",
                Props::new({
                    let events = events.clone();
                    move || {
                        ReadAggregationActor::new(
                            read_plan(
                                &ReplicatorKey::new("decode"),
                                &ReadConsistency::from(2, Duration::from_secs(1)).unwrap(),
                                vec![replica("a")],
                                None,
                            ),
                            Arc::new(GCounterCodec),
                            events,
                        )
                    }
                }),
            )
            .unwrap();
        actor
            .tell(ReadAggregationActorMsg::Reply(
                ReplicatorWireReply::ReadResult {
                    from: replica("a"),
                    message: ReplicatorReadResult {
                        envelope: Some(crate::ReplicatorDataEnvelope {
                            crdt_manifest: crate::GSET_STRING_MANIFEST.to_string(),
                            crdt_version: crate::CRDT_CODEC_VERSION,
                            payload: bytes::Bytes::new(),
                        }),
                    },
                },
            ))
            .unwrap();
        assert!(matches!(
            event_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            ReadAggregationActorEvent::DecodeFailed { replica: failed_replica, reason }
                if failed_replica == replica("a") && reason.contains("expected CRDT manifest")
        ));
        system.terminate(Duration::from_secs(1)).unwrap();
    }
}
