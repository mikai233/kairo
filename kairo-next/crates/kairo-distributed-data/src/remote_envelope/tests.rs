use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver};
use std::time::Duration;

use kairo_actor::{Actor, ActorRef, ActorResult, ActorSystem, Context, Props, Recipient};
use kairo_serialization::{
    ActorRefWireData, Manifest, RemoteEnvelope, RemoteMessage, SerializedMessage,
};

use super::*;
use crate::{
    AggregationTarget, AggregationTransport, CrdtDataCodec, DataEnvelope, DeltaReplicatedData,
    GCounter, GCounterCodec, REPLICATOR_READ_RESULT_SERIALIZER_ID, REPLICATOR_READ_SERIALIZER_ID,
    REPLICATOR_WRITE_ACK_SERIALIZER_ID, REPLICATOR_WRITE_SERIALIZER_ID, ReadAggregationPlan,
    ReadAggregatorState, ReadConsistency, ReplicaId, ReplicatorDataEnvelope, ReplicatorKey,
    WriteAggregationPlan, WriteAggregatorState, WriteConsistency, register_ddata_protocol_codecs,
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

fn registry() -> Arc<kairo_serialization::Registry> {
    let mut registry = kairo_serialization::Registry::new();
    register_ddata_protocol_codecs(&mut registry).unwrap();
    Arc::new(registry)
}

fn replica(id: &str) -> ReplicaId {
    ReplicaId::new(id)
}

fn actor_ref(path: &str) -> ActorRefWireData {
    ActorRefWireData::new(path).unwrap()
}

fn target() -> ReplicatorRemoteTarget {
    ReplicatorRemoteTarget::new(
        replica("remote"),
        actor_ref("kairo://remote@127.0.0.1:25521/system/ddata#1"),
    )
}

fn sender() -> ActorRefWireData {
    actor_ref("kairo://local@127.0.0.1:25520/system/ddata-agg-1#7")
}

fn counter(replica_id: &str, value: u128) -> GCounter {
    GCounter::new()
        .increment(replica(replica_id), value)
        .unwrap()
        .reset_delta()
}

#[test]
fn remote_outbound_wraps_replicator_requests_with_sender_actor_ref() {
    let system = ActorSystem::builder("ddata-remote-envelope-out")
        .build()
        .unwrap();
    let registry = registry();
    let (outbound_ref, outbound_rx) = probe::<ReplicatorRemoteEnvelope>(&system, "remote-out");
    let outbound = ReplicatorRemoteEnvelopeOutbound::new(
        target(),
        Some(sender()),
        registry.clone(),
        outbound_ref,
    );
    let key = ReplicatorKey::new("counter");
    let write = crate::encode_write(
        &key,
        Some(replica("local")),
        &DataEnvelope::new(counter("local", 5)),
        &GCounterCodec,
    )
    .unwrap();

    outbound.tell(write.clone()).unwrap();
    let sent = outbound_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(sent.target, replica("remote"));
    assert_eq!(sent.envelope.recipient, target().recipient().clone());
    assert_eq!(sent.envelope.sender, Some(sender()));
    assert_eq!(
        sent.envelope.message.serializer_id,
        REPLICATOR_WRITE_SERIALIZER_ID
    );
    assert_eq!(
        registry
            .deserialize::<crate::ReplicatorWrite>(sent.envelope.message)
            .unwrap(),
        write
    );

    let read = crate::encode_read(&key, Some(replica("local")));
    outbound.tell(read.clone()).unwrap();
    let sent = outbound_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(
        sent.envelope.message.serializer_id,
        REPLICATOR_READ_SERIALIZER_ID
    );
    assert_eq!(
        registry
            .deserialize::<crate::ReplicatorRead>(sent.envelope.message)
            .unwrap(),
        read
    );
    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn remote_outbound_wraps_replies_to_aggregator_actor_ref() {
    let system = ActorSystem::builder("ddata-remote-envelope-reply")
        .build()
        .unwrap();
    let registry = registry();
    let (outbound_ref, outbound_rx) = probe::<ReplicatorRemoteEnvelope>(&system, "remote-out");
    let reply_target = ReplicatorRemoteTarget::new(replica("local"), sender());
    let outbound =
        ReplicatorRemoteEnvelopeOutbound::new(reply_target, None, registry.clone(), outbound_ref);

    outbound.tell(crate::ReplicatorWriteAck).unwrap();
    let sent = outbound_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(sent.target, replica("local"));
    assert_eq!(sent.envelope.recipient, sender());
    assert_eq!(sent.envelope.sender, None);
    assert_eq!(
        sent.envelope.message.serializer_id,
        REPLICATOR_WRITE_ACK_SERIALIZER_ID
    );

    let read_result = crate::ReplicatorReadResult {
        envelope: Some(ReplicatorDataEnvelope::new(
            GCounterCodec.serialize(&counter("remote", 9)).unwrap(),
        )),
    };
    outbound.tell(read_result.clone()).unwrap();
    let sent = outbound_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(
        sent.envelope.message.serializer_id,
        REPLICATOR_READ_RESULT_SERIALIZER_ID
    );
    assert_eq!(
        registry
            .deserialize::<crate::ReplicatorReadResult>(sent.envelope.message)
            .unwrap(),
        read_result
    );
    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn aggregation_remote_target_preserves_session_sender_actor_ref() {
    let system = ActorSystem::builder("ddata-aggregation-remote-envelope")
        .build()
        .unwrap();
    let registry = registry();
    let (outbound_ref, outbound_rx) = probe::<ReplicatorRemoteEnvelope>(&system, "remote-out");
    let outbound =
        ReplicatorRemoteEnvelopeOutbound::new(target(), None, registry.clone(), outbound_ref);
    let mut transport = AggregationTransport::new(replica("local"), GCounterCodec);
    transport.insert_target(AggregationTarget::remote_envelope(
        replica("remote"),
        outbound.clone(),
        outbound,
    ));
    let key = ReplicatorKey::new("counter");
    let write_state = WriteAggregatorState::new(
        key.clone(),
        &WriteConsistency::to(2, Duration::from_secs(1)).unwrap(),
        vec![replica("remote")],
    )
    .unwrap();
    let write_plan = WriteAggregationPlan::new(
        write_state.clone(),
        write_state.select_replicas(&BTreeSet::new()),
    );

    let report = transport.publish_write_with_sender(
        &write_plan,
        &DataEnvelope::new(counter("local", 5)),
        &sender(),
    );
    assert_eq!(report.sent_to(), &[replica("remote")]);
    let sent = outbound_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(sent.envelope.sender, Some(sender()));
    assert_eq!(
        sent.envelope.message.serializer_id,
        REPLICATOR_WRITE_SERIALIZER_ID
    );

    let read_state = ReadAggregatorState::<GCounter>::new(
        key,
        &ReadConsistency::from(2, Duration::from_secs(1)).unwrap(),
        vec![replica("remote")],
        None,
    )
    .unwrap();
    let read_plan = ReadAggregationPlan::new(
        read_state.clone(),
        read_state.select_replicas(&BTreeSet::new()),
    );
    let report = transport.publish_read_with_sender(&read_plan, &sender());
    assert_eq!(report.sent_to(), &[replica("remote")]);
    let sent = outbound_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(sent.envelope.sender, Some(sender()));
    assert_eq!(
        sent.envelope.message.serializer_id,
        REPLICATOR_READ_SERIALIZER_ID
    );
    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn remote_inbound_validates_recipient_and_preserves_sender() {
    let registry = registry();
    let recipient = target().recipient().clone();
    let inbound = ReplicatorRemoteEnvelopeInbound::new(recipient.clone());
    let read = crate::encode_read(&ReplicatorKey::new("counter"), Some(replica("local")));
    let envelope = RemoteEnvelope::new(
        recipient,
        Some(sender()),
        registry.serialize(&read).unwrap(),
    );

    let inbound_message = inbound.receive(envelope).unwrap();
    assert_eq!(inbound_message.sender, Some(sender()));
    assert_eq!(
        registry
            .deserialize::<crate::ReplicatorRead>(inbound_message.message)
            .unwrap(),
        read
    );

    let wrong = inbound
        .receive(RemoteEnvelope::new(
            actor_ref("kairo://remote@127.0.0.1:25521/system/other#2"),
            None,
            SerializedMessage::new(
                REPLICATOR_READ_SERIALIZER_ID,
                Manifest::new(crate::ReplicatorRead::MANIFEST),
                crate::ReplicatorRead::VERSION,
                bytes::Bytes::new(),
            ),
        ))
        .expect_err("wrong recipient should fail");
    assert!(matches!(
        wrong,
        ReplicatorRemoteEnvelopeError::WrongRecipient { .. }
    ));
}
