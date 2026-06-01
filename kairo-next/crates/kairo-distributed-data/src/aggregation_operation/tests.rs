use std::sync::mpsc::{self, Receiver};
use std::time::Duration;

use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, ActorSystem, Context, Props};

use super::*;
use crate::{
    DataEnvelope, DeltaReplicatedData, GCounter, GetResponse, ReadAggregationActorEvent,
    ReadAggregationOutcome, ReplicaId, ReplicatorKey, UpdateOutcome, UpdateResponse,
    WriteAggregationActorEvent, WriteAggregationOutcome,
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
