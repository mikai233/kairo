use std::collections::BTreeSet;
use std::marker::PhantomData;
use std::sync::{
    Arc,
    mpsc::{self, Receiver},
};
use std::time::Duration;

use kairo_actor::{
    Actor, ActorError, ActorRef, ActorResult, ActorSystem, Context, Props, Recipient,
};
use kairo_remote::RemoteSettings;
use kairo_serialization::ActorRefWireData;

use super::*;
use crate::{
    DataEnvelope, GCounter, GCounterCodec, ReadAggregatorState, ReadConsistency, ReplicatorRead,
    ReplicatorWireReply, ReplicatorWrite, ReplicatorWriteAck, SenderAwareRecipient,
    WriteAggregatorState, WriteConsistency, decode_data_envelope, encode_read_result,
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

struct CaptureSender<M> {
    tx: mpsc::Sender<ActorRefWireData>,
    _message: PhantomData<fn(M)>,
}

impl<M> CaptureSender<M> {
    fn new(tx: mpsc::Sender<ActorRefWireData>) -> Self {
        Self {
            tx,
            _message: PhantomData,
        }
    }
}

impl<M> SenderAwareRecipient<M> for CaptureSender<M>
where
    M: Send + 'static,
{
    fn tell_with_sender(
        &self,
        _message: M,
        sender: &ActorRefWireData,
    ) -> Result<(), kairo_actor::SendError<M>> {
        self.tx
            .send(sender.clone())
            .map_err(|error| kairo_actor::SendError::new(_message, error.to_string()))
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

fn sender_aware_aggregation_target(
    replica_id: &str,
    write_ref: ActorRef<ReplicatorWrite>,
    read_ref: ActorRef<ReplicatorRead>,
    write_sender_tx: mpsc::Sender<ActorRefWireData>,
    read_sender_tx: mpsc::Sender<ActorRefWireData>,
) -> crate::AggregationTarget {
    crate::AggregationTarget::new_sender_aware(
        replica(replica_id),
        SendToActor { target: write_ref },
        SendToActor { target: read_ref },
        CaptureSender::<ReplicatorWrite>::new(write_sender_tx),
        CaptureSender::<ReplicatorRead>::new(read_sender_tx),
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

#[test]
fn read_session_waits_for_read_repair_before_success() {
    let system = ActorSystem::builder("ddata-read-aggregation-repair")
        .build()
        .unwrap();
    let (write_ref, _write_rx) = probe::<ReplicatorWrite>(&system, "writes");
    let (read_ref, read_rx) = probe::<ReplicatorRead>(&system, "reads");
    let (reply_to, replies) = probe::<GetResponse<GCounter>>(&system, "replies");
    let (events_ref, events) = probe::<ReadAggregationSessionEvent>(&system, "events");
    let (repair_ref, repairs) = probe::<ReadRepairRequest<GCounter>>(&system, "repairs");
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

    let session = system
        .spawn(
            "read-session",
            Props::new({
                let events_ref = events_ref.clone();
                let reply_to = reply_to.clone();
                let repair_ref = repair_ref.clone();
                move || {
                    ReadAggregationSession::with_events(
                        plan,
                        Arc::new(GCounterCodec),
                        transport,
                        Duration::from_secs(5),
                        reply_to,
                        events_ref,
                    )
                    .with_read_repair(repair_ref)
                }
            }),
        )
        .unwrap();

    let reply_actor = match events.recv_timeout(Duration::from_secs(1)).unwrap() {
        ReadAggregationSessionEvent::Started { reply_to, .. } => reply_to,
        other => panic!("expected session start, got {other:?}"),
    };
    read_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    session
        .tell(ReadAggregationSessionMsg::ReadRepairApplied)
        .unwrap();
    assert!(replies.recv_timeout(Duration::from_millis(50)).is_err());
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

    let repair = repairs.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(repair.key, key);
    assert_eq!(repair.envelope.data().value().unwrap(), 9);
    assert!(replies.recv_timeout(Duration::from_millis(50)).is_err());
    assert!(events.recv_timeout(Duration::from_millis(50)).is_err());

    repair.reply_to.tell(()).unwrap();
    assert!(matches!(
        events.recv_timeout(Duration::from_secs(1)).unwrap(),
        ReadAggregationSessionEvent::Completed(ReadAggregationSessionOutcome::Success)
    ));
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

#[test]
fn write_session_can_publish_canonical_sender_ref() {
    let system = ActorSystem::builder("ddata-write-aggregation-canonical")
        .build()
        .unwrap();
    let (write_ref, _write_rx) = probe::<ReplicatorWrite>(&system, "writes");
    let (read_ref, _read_rx) = probe::<ReplicatorRead>(&system, "reads");
    let (reply_to, _replies) = probe::<UpdateResponse<GCounter>>(&system, "replies");
    let (events_ref, events) = probe::<WriteAggregationSessionEvent>(&system, "events");
    let (write_sender_tx, write_sender_rx) = mpsc::channel();
    let (read_sender_tx, _read_sender_rx) = mpsc::channel();
    let key = ReplicatorKey::new("counter");
    let remote = replica("a");
    let state = WriteAggregatorState::new(
        key.clone(),
        &WriteConsistency::to(2, Duration::from_secs(1)).unwrap(),
        vec![remote],
    )
    .unwrap();
    let plan = WriteAggregationPlan::new(state.clone(), state.select_replicas(&BTreeSet::new()));
    let envelope = DataEnvelope::new(counter("local", 5));
    let outcome = UpdateOutcome::new(key, true, Some(counter("local", 5)));
    let mut transport = AggregationTransport::new(replica("local"), GCounterCodec);
    transport.insert_target(sender_aware_aggregation_target(
        "a",
        write_ref,
        read_ref,
        write_sender_tx,
        read_sender_tx,
    ));

    system
        .spawn(
            "write-session",
            Props::new({
                let events_ref = events_ref.clone();
                let reply_to = reply_to.clone();
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
                    .with_sender_remote_settings(RemoteSettings::new("127.0.0.1", 25520))
                }
            }),
        )
        .unwrap();

    events.recv_timeout(Duration::from_secs(1)).unwrap();
    let sender = write_sender_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    assert_eq!(sender.protocol(), "kairo");
    assert_eq!(sender.system(), "ddata-write-aggregation-canonical");
    assert_eq!(sender.host(), Some("127.0.0.1"));
    assert_eq!(sender.port(), Some(25520));
    assert!(sender.path().starts_with(
        "kairo://ddata-write-aggregation-canonical@127.0.0.1:25520/user/write-session#"
    ));
    assert!(sender.path().contains("/$anon-"));
    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn read_session_can_publish_canonical_sender_ref() {
    let system = ActorSystem::builder("ddata-read-aggregation-canonical")
        .build()
        .unwrap();
    let (write_ref, _write_rx) = probe::<ReplicatorWrite>(&system, "writes");
    let (read_ref, _read_rx) = probe::<ReplicatorRead>(&system, "reads");
    let (reply_to, _replies) = probe::<GetResponse<GCounter>>(&system, "replies");
    let (events_ref, events) = probe::<ReadAggregationSessionEvent>(&system, "events");
    let (write_sender_tx, _write_sender_rx) = mpsc::channel();
    let (read_sender_tx, read_sender_rx) = mpsc::channel();
    let remote = replica("a");
    let state = ReadAggregatorState::new(
        ReplicatorKey::new("counter"),
        &ReadConsistency::from(2, Duration::from_secs(1)).unwrap(),
        vec![remote],
        None,
    )
    .unwrap();
    let plan = ReadAggregationPlan::new(state.clone(), state.select_replicas(&BTreeSet::new()));
    let mut transport = AggregationTransport::new(replica("local"), GCounterCodec);
    transport.insert_target(sender_aware_aggregation_target(
        "a",
        write_ref,
        read_ref,
        write_sender_tx,
        read_sender_tx,
    ));

    system
        .spawn(
            "read-session",
            Props::new({
                let events_ref = events_ref.clone();
                let reply_to = reply_to.clone();
                move || {
                    ReadAggregationSession::with_events(
                        plan,
                        Arc::new(GCounterCodec),
                        transport,
                        Duration::from_secs(5),
                        reply_to,
                        events_ref,
                    )
                    .with_sender_remote_settings(RemoteSettings::new("127.0.0.1", 25521))
                }
            }),
        )
        .unwrap();

    events.recv_timeout(Duration::from_secs(1)).unwrap();
    let sender = read_sender_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(sender.protocol(), "kairo");
    assert_eq!(sender.system(), "ddata-read-aggregation-canonical");
    assert_eq!(sender.host(), Some("127.0.0.1"));
    assert_eq!(sender.port(), Some(25521));
    assert!(sender.path().starts_with(
        "kairo://ddata-read-aggregation-canonical@127.0.0.1:25521/user/read-session#"
    ));
    assert!(sender.path().contains("/$anon-"));
    system.terminate(Duration::from_secs(1)).unwrap();
}
