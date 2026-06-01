use std::collections::BTreeSet;
use std::sync::{
    Arc,
    mpsc::{self, Receiver},
};
use std::time::Duration;

use kairo_actor::{
    Actor, ActorError, ActorRef, ActorResult, ActorSystem, Context, Props, Recipient,
};

use super::*;
use crate::{
    DataEnvelope, GCounter, GCounterCodec, ReadAggregatorState, ReadConsistency, ReplicatorRead,
    ReplicatorWireReply, ReplicatorWrite, ReplicatorWriteAck, WriteAggregatorState,
    WriteConsistency, decode_data_envelope, encode_read_result,
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
    let plan = WriteAggregationPlan::new(state.clone(), state.select_replicas(&BTreeSet::new()));
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
    let plan = WriteAggregationPlan::new(state.clone(), state.select_replicas(&BTreeSet::new()));
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
    let retry_write = write_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(retry_write.key, key.as_str());
    assert_eq!(retry_write.from, Some(replica("local")));
    assert_eq!(
        decode_data_envelope::<GCounter, _>(&retry_write.envelope, &GCounterCodec)
            .unwrap()
            .data()
            .value()
            .unwrap(),
        3
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
