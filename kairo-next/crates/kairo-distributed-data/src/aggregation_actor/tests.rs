use std::collections::BTreeSet;
use std::sync::{
    Arc,
    mpsc::{self, Receiver},
};
use std::time::Duration;

use kairo_actor::{Actor, ActorRef, ActorResult, ActorSystem, Context, Props};

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
                        pruning: Vec::new(),
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
