use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, Context};

use crate::{
    DeltaReplicatedData, GetResponse, ReadAggregationActorEvent, ReadAggregationOutcome, ReplicaId,
    ReplicatedDelta, ReplicatorKey, UpdateOutcome, UpdateResponse, WriteAggregationActorEvent,
    WriteAggregationOutcome,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadAggregationOperationEvent {
    DecodeFailed {
        key: ReplicatorKey,
        replica: ReplicaId,
        reason: String,
    },
}

pub enum ReadAggregationOperationMsg<D>
where
    D: DeltaReplicatedData + Send + 'static,
{
    Aggregation(ReadAggregationActorEvent<D>),
}

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
    pub fn new(key: ReplicatorKey, reply_to: ActorRef<GetResponse<D>>) -> Self {
        Self {
            key,
            reply_to,
            events: None,
        }
    }

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

pub(crate) fn write_aggregation_response<Delta>(
    key: &ReplicatorKey,
    outcome: &mut Option<UpdateOutcome<Delta>>,
    aggregation: WriteAggregationOutcome,
) -> UpdateResponse<Delta>
where
    Delta: ReplicatedDelta + Send + 'static,
{
    match aggregation {
        WriteAggregationOutcome::InProgress => UpdateResponse::Failure {
            key: key.clone(),
            reason: "write aggregation completed with non-terminal state".to_string(),
        },
        WriteAggregationOutcome::Success => {
            if let Some(outcome) = outcome.take() {
                UpdateResponse::Success(outcome)
            } else {
                UpdateResponse::Failure {
                    key: key.clone(),
                    reason: "write aggregation success was reported more than once".to_string(),
                }
            }
        }
        WriteAggregationOutcome::Failed {
            required,
            available,
        } => UpdateResponse::Failure {
            key: key.clone(),
            reason: format!(
                "write quorum failed: required {required} remote acknowledgements, \
                 only {available} replicas remain available"
            ),
        },
        WriteAggregationOutcome::Timeout { .. } => UpdateResponse::Timeout { key: key.clone() },
    }
}

pub(crate) fn read_aggregation_response<D>(
    key: &ReplicatorKey,
    outcome: ReadAggregationOutcome<D>,
) -> GetResponse<D>
where
    D: DeltaReplicatedData + Send + 'static,
{
    match outcome {
        ReadAggregationOutcome::InProgress => GetResponse::Failure {
            key: key.clone(),
            reason: "read aggregation completed with non-terminal state".to_string(),
        },
        ReadAggregationOutcome::Success { envelope } => GetResponse::Success {
            key: key.clone(),
            data: envelope.into_data(),
        },
        ReadAggregationOutcome::NotFound => GetResponse::NotFound { key: key.clone() },
        ReadAggregationOutcome::Failure { required, received } => GetResponse::Failure {
            key: key.clone(),
            reason: format!(
                "read quorum failed: required {required} remote replies, \
                 received {received}"
            ),
        },
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
    use std::sync::mpsc::{self, Receiver};
    use std::time::Duration;

    use kairo_actor::{ActorSystem, Props};

    use super::*;
    use crate::{DataEnvelope, GCounter, ReplicaId};

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

    fn replica(id: &str) -> ReplicaId {
        ReplicaId::new(id)
    }

    fn counter(replica_id: &str, value: u128) -> GCounter {
        GCounter::new()
            .increment(replica(replica_id), value)
            .unwrap()
            .reset_delta()
    }

    #[test]
    fn write_operation_maps_success_failure_timeout_and_retry_effects() {
        let system = ActorSystem::builder("ddata-write-aggregation-operation")
            .build()
            .unwrap();
        let (reply_to, replies) = probe::<UpdateResponse<GCounter>>(&system, "replies");
        let (event_to, events) = probe::<WriteAggregationOperationEvent>(&system, "events");
        let key = ReplicatorKey::new("counter");
        let outcome = UpdateOutcome::new(key.clone(), true, Some(GCounter::default()));
        let actor = system
            .spawn(
                "operation",
                Props::new({
                    let reply_to = reply_to.clone();
                    let event_to = event_to.clone();
                    let outcome = outcome.clone();
                    move || WriteAggregationOperation::with_events(outcome, reply_to, event_to)
                }),
            )
            .unwrap();

        actor
            .tell(WriteAggregationOperationMsg::Aggregation(
                WriteAggregationActorEvent::RetryFullState {
                    replica: replica("a"),
                },
            ))
            .unwrap();
        assert_eq!(
            events.recv_timeout(Duration::from_secs(1)).unwrap(),
            WriteAggregationOperationEvent::RetryFullState {
                key: key.clone(),
                replica: replica("a"),
            }
        );

        actor
            .tell(WriteAggregationOperationMsg::Aggregation(
                WriteAggregationActorEvent::Completed(WriteAggregationOutcome::Success),
            ))
            .unwrap();
        assert!(matches!(
            replies.recv_timeout(Duration::from_secs(1)).unwrap(),
            UpdateResponse::Success(success) if success.key() == &key
        ));

        let failed = system
            .spawn(
                "operation-failed",
                Props::new({
                    let reply_to = reply_to.clone();
                    let outcome = outcome.clone();
                    move || WriteAggregationOperation::new(outcome, reply_to)
                }),
            )
            .unwrap();
        failed
            .tell(WriteAggregationOperationMsg::Aggregation(
                WriteAggregationActorEvent::Completed(WriteAggregationOutcome::Failed {
                    required: 2,
                    available: 1,
                }),
            ))
            .unwrap();
        assert!(matches!(
            replies.recv_timeout(Duration::from_secs(1)).unwrap(),
            UpdateResponse::Failure { key: failed_key, reason }
                if failed_key == key && reason.contains("required 2")
        ));

        let timed_out = system
            .spawn(
                "operation-timeout",
                Props::new({
                    let reply_to = reply_to.clone();
                    let outcome = outcome.clone();
                    move || WriteAggregationOperation::new(outcome, reply_to)
                }),
            )
            .unwrap();
        timed_out
            .tell(WriteAggregationOperationMsg::Aggregation(
                WriteAggregationActorEvent::Completed(WriteAggregationOutcome::Timeout {
                    required: 2,
                    acknowledged: 1,
                }),
            ))
            .unwrap();
        assert_eq!(
            replies.recv_timeout(Duration::from_secs(1)).unwrap(),
            UpdateResponse::Timeout { key: key.clone() }
        );
        system.terminate(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn read_operation_maps_completed_results_and_decode_diagnostics() {
        let system = ActorSystem::builder("ddata-read-aggregation-operation")
            .build()
            .unwrap();
        let (reply_to, replies) = probe::<GetResponse<GCounter>>(&system, "replies");
        let (event_to, events) = probe::<ReadAggregationOperationEvent>(&system, "events");
        let key = ReplicatorKey::new("counter");
        let actor = system
            .spawn(
                "operation",
                Props::new({
                    let reply_to = reply_to.clone();
                    let event_to = event_to.clone();
                    let key = key.clone();
                    move || ReadAggregationOperation::with_events(key, reply_to, event_to)
                }),
            )
            .unwrap();

        actor
            .tell(ReadAggregationOperationMsg::Aggregation(
                ReadAggregationActorEvent::DecodeFailed {
                    replica: replica("a"),
                    reason: "bad manifest".to_string(),
                },
            ))
            .unwrap();
        assert_eq!(
            events.recv_timeout(Duration::from_secs(1)).unwrap(),
            ReadAggregationOperationEvent::DecodeFailed {
                key: key.clone(),
                replica: replica("a"),
                reason: "bad manifest".to_string(),
            }
        );
        assert!(replies.recv_timeout(Duration::from_millis(50)).is_err());

        actor
            .tell(ReadAggregationOperationMsg::Aggregation(
                ReadAggregationActorEvent::Completed(ReadAggregationOutcome::Success {
                    envelope: DataEnvelope::new(counter("a", 7)),
                }),
            ))
            .unwrap();
        match replies.recv_timeout(Duration::from_secs(1)).unwrap() {
            GetResponse::Success {
                key: success_key,
                data,
            } => {
                assert_eq!(success_key, key);
                assert_eq!(data.value().unwrap(), 7);
            }
            other => panic!("expected read success, got {other:?}"),
        }

        let failed = system
            .spawn(
                "operation-failed",
                Props::new({
                    let reply_to = reply_to.clone();
                    let key = key.clone();
                    move || ReadAggregationOperation::new(key, reply_to)
                }),
            )
            .unwrap();
        failed
            .tell(ReadAggregationOperationMsg::Aggregation(
                ReadAggregationActorEvent::Completed(ReadAggregationOutcome::Failure {
                    required: 2,
                    received: 1,
                }),
            ))
            .unwrap();
        assert!(matches!(
            replies.recv_timeout(Duration::from_secs(1)).unwrap(),
            GetResponse::Failure { key: failed_key, reason }
                if failed_key == key && reason.contains("required 2")
        ));
        system.terminate(Duration::from_secs(1)).unwrap();
    }
}
