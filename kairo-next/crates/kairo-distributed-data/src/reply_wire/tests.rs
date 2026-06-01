use std::sync::{
    Arc,
    mpsc::{self, Receiver},
};
use std::time::Duration;

use kairo_actor::{Actor, ActorResult, ActorSystem, Context, Props};
use kairo_serialization::Manifest;

use super::*;
use crate::{
    DataEnvelope, DeltaPropagationLog, DeltaReplicatedData, GCounter, GCounterCodec,
    REPLICATOR_DELTA_ACK_SERIALIZER_ID, REPLICATOR_READ_RESULT_SERIALIZER_ID,
    REPLICATOR_WRITE_ACK_SERIALIZER_ID, ReplicatorKey, register_ddata_protocol_codecs,
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

fn probe<M>(system: &ActorSystem, name: &str) -> (kairo_actor::ActorRef<M>, Receiver<M>)
where
    M: Send + 'static,
{
    let (tx, rx) = mpsc::channel();
    let actor = system
        .spawn(name, Props::new(move || Forward { tx }))
        .unwrap();
    (actor, rx)
}

fn registry() -> Arc<Registry> {
    let mut registry = Registry::new();
    register_ddata_protocol_codecs(&mut registry).unwrap();
    Arc::new(registry)
}

fn replica(id: &str) -> ReplicaId {
    ReplicaId::new(id)
}

fn counter(replica_id: &str, value: u128) -> GCounter {
    GCounter::new()
        .increment(replica(replica_id), value)
        .unwrap()
}

fn delta_report(reply: bool) -> DeltaPropagationReceiveReport {
    let key = ReplicatorKey::new("counter");
    let mut log = DeltaPropagationLog::new([replica("local")]);
    log.record_delta(key, Some(counter("remote", 3)));
    let propagation = log
        .collect_propagations()
        .remove(&replica("local"))
        .unwrap();
    let propagation =
        crate::encode_delta_propagation(replica("remote"), reply, &propagation, &GCounterCodec)
            .unwrap();

    let mut state = crate::ReplicatorState::<GCounter>::new();
    let mut tracker = crate::DeltaReceiveTracker::new();
    tracker.apply_propagation(&mut state, &propagation, &GCounterCodec)
}

#[test]
fn reply_outbound_serializes_delta_ack_only_when_reply_was_requested() {
    let system = ActorSystem::builder("ddata-reply-wire-delta-out")
        .build()
        .unwrap();
    let registry = registry();
    let (outbound_ref, outbound_rx) = probe::<ReplicatorSerializedReply>(&system, "wire-out");
    let outbound =
        ReplicatorReplyWireOutbound::new(replica("local"), registry.clone(), outbound_ref);
    let report = delta_report(false);

    assert!(!outbound.send_delta_report(&report).unwrap());
    assert!(outbound_rx.recv_timeout(Duration::from_millis(50)).is_err());

    let report = delta_report(true);
    assert!(outbound.send_delta_report(&report).unwrap());
    let envelope = outbound_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(envelope.from, replica("local"));
    assert_eq!(envelope.target, replica("remote"));
    assert_eq!(
        envelope.message.serializer_id,
        REPLICATOR_DELTA_ACK_SERIALIZER_ID
    );
    assert_eq!(
        registry
            .deserialize::<ReplicatorDeltaAck>(envelope.message)
            .unwrap(),
        ReplicatorDeltaAck
    );
    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn reply_outbound_serializes_write_and_read_results_to_original_sender() {
    let system = ActorSystem::builder("ddata-reply-wire-results-out")
        .build()
        .unwrap();
    let registry = registry();
    let (outbound_ref, outbound_rx) = probe::<ReplicatorSerializedReply>(&system, "wire-out");
    let outbound =
        ReplicatorReplyWireOutbound::new(replica("local"), registry.clone(), outbound_ref);
    let key = ReplicatorKey::new("counter");

    let write_result = DirectWriteResult::Ack {
        key: key.clone(),
        from: Some(replica("remote")),
        changed: true,
        message: ReplicatorWriteAck,
    };
    outbound.send_write_result(&write_result).unwrap();
    let write_envelope = outbound_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(write_envelope.target, replica("remote"));
    assert_eq!(
        write_envelope.message.serializer_id,
        REPLICATOR_WRITE_ACK_SERIALIZER_ID
    );

    let read_message = crate::encode_read_result(
        Some(&DataEnvelope::new(counter("local", 5).reset_delta())),
        &GCounterCodec,
    )
    .unwrap();
    let read_result = DirectReadResult::new(key, Some(replica("remote")), read_message.clone());
    outbound.send_read_result(&read_result).unwrap();
    let read_envelope = outbound_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(read_envelope.from, replica("local"));
    assert_eq!(read_envelope.target, replica("remote"));
    assert_eq!(
        read_envelope.message.serializer_id,
        REPLICATOR_READ_RESULT_SERIALIZER_ID
    );
    assert_eq!(
        registry
            .deserialize::<ReplicatorReadResult>(read_envelope.message)
            .unwrap(),
        read_message
    );
    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn reply_inbound_decodes_replies_with_source_replica() {
    let registry = registry();
    let inbound = ReplicatorReplyWireInbound::new(replica("local"), registry.clone());
    let reply = inbound
        .receive(ReplicatorSerializedReply::new(
            replica("remote"),
            replica("local"),
            registry.serialize(&ReplicatorWriteAck).unwrap(),
        ))
        .unwrap();
    assert!(matches!(
        reply,
        ReplicatorWireReply::WriteAck { from, message: ReplicatorWriteAck }
            if from == replica("remote")
    ));

    let reply = inbound
        .receive_message(
            replica("remote"),
            registry
                .serialize(&ReplicatorReadResult { envelope: None })
                .unwrap(),
        )
        .unwrap();
    assert_eq!(reply.from(), &replica("remote"));
    assert_eq!(reply.manifest(), ReplicatorReadResult::MANIFEST);
}

#[test]
fn reply_wire_rejects_missing_targets_wrong_targets_and_unknown_manifests() {
    let system = ActorSystem::builder("ddata-reply-wire-reject")
        .build()
        .unwrap();
    let registry = registry();
    let (outbound_ref, _outbound_rx) = probe::<ReplicatorSerializedReply>(&system, "wire-out");
    let outbound =
        ReplicatorReplyWireOutbound::new(replica("local"), registry.clone(), outbound_ref);
    let missing = outbound
        .send_write_result(&DirectWriteResult::Ack {
            key: ReplicatorKey::new("counter"),
            from: None,
            changed: false,
            message: ReplicatorWriteAck,
        })
        .expect_err("missing target should fail");
    assert!(matches!(
        missing,
        ReplicatorReplyWireError::MissingReplyTarget(_)
    ));

    let inbound = ReplicatorReplyWireInbound::new(replica("local"), registry);
    let wrong_target = inbound
        .receive(ReplicatorSerializedReply::new(
            replica("remote"),
            replica("other"),
            SerializedMessage::new(
                REPLICATOR_WRITE_ACK_SERIALIZER_ID,
                Manifest::new(ReplicatorWriteAck::MANIFEST),
                ReplicatorWriteAck::VERSION,
                bytes::Bytes::new(),
            ),
        ))
        .expect_err("wrong target should fail");
    assert!(matches!(
        wrong_target,
        ReplicatorReplyWireError::WrongTarget { .. }
    ));

    let unknown = inbound
        .receive_message(
            replica("remote"),
            SerializedMessage::new(
                9_999,
                Manifest::new("kairo.ddata.unknown-reply"),
                1,
                bytes::Bytes::new(),
            ),
        )
        .expect_err("unknown manifest should fail");
    assert!(matches!(
        unknown,
        ReplicatorReplyWireError::UnsupportedManifest(_)
    ));
    system.terminate(Duration::from_secs(1)).unwrap();
}
