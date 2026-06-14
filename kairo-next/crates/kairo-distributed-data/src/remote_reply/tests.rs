use std::sync::{
    Arc,
    mpsc::{self, Receiver},
};
use std::time::Duration;

use bytes::Bytes;
use kairo_actor::{Actor, ActorRef, ActorResult, Context, Props};
use kairo_remote::RemoteSettings;
use kairo_serialization::{Manifest, RemoteEnvelope, SerializedMessage};

use super::*;
use crate::{
    DataEnvelope, DeltaReplicatedData, GCounter, GCounterCodec, REPLICATOR_DELTA_ACK_SERIALIZER_ID,
    REPLICATOR_READ_RESULT_SERIALIZER_ID, REPLICATOR_WRITE_ACK_SERIALIZER_ID, ReplicatorKey,
    ReplicatorRead, ReplicatorRemoteTarget, register_ddata_protocol_codecs,
};

struct WriteReplyProbe {
    tx: mpsc::Sender<ReplicatorWireReply>,
}

impl Actor for WriteReplyProbe {
    type Msg = WriteAggregationActorMsg;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        if let WriteAggregationActorMsg::Reply(reply) = msg {
            self.tx
                .send(reply)
                .map_err(|error| kairo_actor::ActorError::Message(error.to_string()))?;
        }
        Ok(())
    }
}

struct ReadReplyProbe {
    tx: mpsc::Sender<ReplicatorWireReply>,
}

impl Actor for ReadReplyProbe {
    type Msg = ReadAggregationActorMsg;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        if let ReadAggregationActorMsg::Reply(reply) = msg {
            self.tx
                .send(reply)
                .map_err(|error| kairo_actor::ActorError::Message(error.to_string()))?;
        }
        Ok(())
    }
}

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
        .reset_delta()
}

fn actor_ref<M>(actor: &ActorRef<M>) -> ActorRefWireData
where
    M: Send + 'static,
{
    ActorRefWireData::new(actor.path().to_string()).unwrap()
}

fn wire_ref(path: &str) -> ActorRefWireData {
    ActorRefWireData::new(path).unwrap()
}

fn reply_target() -> ReplicatorRemoteTarget {
    ReplicatorRemoteTarget::new(
        replica("remote"),
        wire_ref("kairo://remote@127.0.0.1:25520/user/agg#9"),
    )
}

fn write_probe(
    system: &ActorSystem,
) -> (
    ActorRef<WriteAggregationActorMsg>,
    Receiver<ReplicatorWireReply>,
) {
    let (tx, rx) = mpsc::channel();
    let actor = system
        .spawn("write-agg", Props::new(move || WriteReplyProbe { tx }))
        .unwrap();
    (actor, rx)
}

fn read_probe(
    system: &ActorSystem,
) -> (
    ActorRef<ReadAggregationActorMsg>,
    Receiver<ReplicatorWireReply>,
) {
    let (tx, rx) = mpsc::channel();
    let actor = system
        .spawn("read-agg", Props::new(move || ReadReplyProbe { tx }))
        .unwrap();
    (actor, rx)
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

#[test]
fn remote_reply_outbound_sends_direct_results_to_original_sender_ref() {
    let system = ActorSystem::builder("ddata-remote-reply-out")
        .build()
        .unwrap();
    let registry = registry();
    let (outbound_ref, outbound_rx) = probe::<ReplicatorRemoteEnvelope>(&system, "remote-out");
    let sender = wire_ref("kairo://local@127.0.0.1:25521/system/ddata#1");
    let outbound = ReplicatorRemoteReplyOutbound::new(
        reply_target(),
        Some(sender.clone()),
        registry.clone(),
        outbound_ref,
    );

    outbound
        .send_write_result(&DirectWriteResult::Ack {
            key: ReplicatorKey::new("counter"),
            from: Some(replica("remote")),
            changed: true,
            message: ReplicatorWriteAck,
        })
        .unwrap();
    let write = outbound_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(write.target, replica("remote"));
    assert_eq!(write.envelope.recipient, reply_target().recipient().clone());
    assert_eq!(write.envelope.sender, Some(sender.clone()));
    assert_eq!(
        write.envelope.message.serializer_id,
        REPLICATOR_WRITE_ACK_SERIALIZER_ID
    );

    let read_result = DirectReadResult::new(
        ReplicatorKey::new("counter"),
        Some(replica("remote")),
        crate::encode_read_result(
            Some(&DataEnvelope::new(counter("local", 4))),
            &GCounterCodec,
        )
        .unwrap(),
    );
    outbound.send_read_result(&read_result).unwrap();
    let read = outbound_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(read.envelope.recipient, reply_target().recipient().clone());
    assert_eq!(read.envelope.sender, Some(sender));
    assert_eq!(
        read.envelope.message.serializer_id,
        REPLICATOR_READ_RESULT_SERIALIZER_ID
    );
    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn remote_reply_outbound_sends_delta_ack_only_when_requested() {
    let system = ActorSystem::builder("ddata-remote-reply-delta-out")
        .build()
        .unwrap();
    let registry = registry();
    let (outbound_ref, outbound_rx) = probe::<ReplicatorRemoteEnvelope>(&system, "remote-out");
    let outbound = ReplicatorRemoteReplyOutbound::new(reply_target(), None, registry, outbound_ref);
    let mut log = crate::DeltaPropagationLog::new([replica("local")]);
    log.record_delta(ReplicatorKey::new("counter"), Some(counter("remote", 3)));
    let propagation = log
        .collect_propagations()
        .remove(&replica("local"))
        .unwrap();
    let message =
        crate::encode_delta_propagation(replica("remote"), false, &propagation, &GCounterCodec)
            .unwrap();
    let mut state = crate::ReplicatorState::<GCounter>::new();
    let mut tracker = crate::DeltaReceiveTracker::new();
    let report = tracker.apply_propagation(&mut state, &message, &GCounterCodec);

    assert!(!outbound.send_delta_report(&report).unwrap());
    assert!(outbound_rx.recv_timeout(Duration::from_millis(50)).is_err());

    let message =
        crate::encode_delta_propagation(replica("remote"), true, &propagation, &GCounterCodec)
            .unwrap();
    let report = tracker.apply_propagation(&mut state, &message, &GCounterCodec);
    assert!(outbound.send_delta_report(&report).unwrap());
    let ack = outbound_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(ack.target, replica("remote"));
    assert_eq!(ack.envelope.recipient, reply_target().recipient().clone());
    assert_eq!(
        ack.envelope.message.serializer_id,
        REPLICATOR_DELTA_ACK_SERIALIZER_ID
    );
    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn remote_reply_inbound_delivers_write_replies_to_addressed_aggregator() {
    let system = ActorSystem::builder("ddata-remote-reply-write")
        .build()
        .unwrap();
    let registry = registry();
    let inbound = ReplicatorRemoteReplyInbound::new(system.clone(), registry.clone());
    let (recipient, replies) = write_probe(&system);

    inbound
        .receive_from(
            replica("remote"),
            RemoteEnvelope::new(
                actor_ref(&recipient),
                None,
                registry.serialize(&ReplicatorWriteAck).unwrap(),
            ),
        )
        .unwrap();

    assert!(matches!(
        replies.recv_timeout(Duration::from_secs(1)).unwrap(),
        ReplicatorWireReply::WriteAck { from, message: ReplicatorWriteAck }
            if from == replica("remote")
    ));
    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn remote_reply_inbound_maps_owned_canonical_recipient_to_local_aggregator() {
    let system = ActorSystem::builder("ddata-remote-reply-canonical")
        .build()
        .unwrap();
    let registry = registry();
    let inbound = ReplicatorRemoteReplyInbound::with_remote_settings(
        system.clone(),
        RemoteSettings::new("127.0.0.1", 25520),
        registry.clone(),
    );
    let (recipient, replies) = write_probe(&system);
    let canonical_recipient = wire_ref(&recipient.path().as_str().replacen(
        "kairo://ddata-remote-reply-canonical",
        "kairo://ddata-remote-reply-canonical@127.0.0.1:25520",
        1,
    ));

    inbound
        .receive_from(
            replica("remote"),
            RemoteEnvelope::new(
                canonical_recipient,
                None,
                registry.serialize(&ReplicatorWriteAck).unwrap(),
            ),
        )
        .unwrap();

    assert!(matches!(
        replies.recv_timeout(Duration::from_secs(1)).unwrap(),
        ReplicatorWireReply::WriteAck { from, message: ReplicatorWriteAck }
            if from == replica("remote")
    ));
    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn remote_reply_inbound_delivers_read_results_to_addressed_aggregator() {
    let system = ActorSystem::builder("ddata-remote-reply-read")
        .build()
        .unwrap();
    let registry = registry();
    let inbound = ReplicatorRemoteReplyInbound::new(system.clone(), registry.clone());
    let (recipient, replies) = read_probe(&system);

    inbound
        .receive_message(
            replica("remote"),
            actor_ref(&recipient),
            registry
                .serialize(&ReplicatorReadResult { envelope: None })
                .unwrap(),
        )
        .unwrap();

    assert!(matches!(
        replies.recv_timeout(Duration::from_secs(1)).unwrap(),
        ReplicatorWireReply::ReadResult {
            from,
            message: ReplicatorReadResult { envelope: None },
        } if from == replica("remote")
    ));
    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn remote_reply_inbound_reports_unknown_manifest_and_missing_actor() {
    let system = ActorSystem::builder("ddata-remote-reply-errors")
        .build()
        .unwrap();
    let registry = registry();
    let inbound = ReplicatorRemoteReplyInbound::new(system.clone(), registry.clone());
    let missing =
        ActorRefWireData::new("kairo://ddata-remote-reply-errors/user/missing#9").unwrap();

    let delivery_error = inbound
        .receive_message(
            replica("remote"),
            missing.clone(),
            registry.serialize(&ReplicatorWriteAck).unwrap(),
        )
        .expect_err("missing local aggregation actor should fail");
    assert!(matches!(
        delivery_error,
        ReplicatorRemoteReplyError::Send { .. }
    ));
    assert!(
        system
            .dead_letters()
            .wait_for_len(1, Duration::from_secs(1))
    );
    assert_eq!(
        system.dead_letters().records()[0].recipient().as_str(),
        missing.path()
    );

    let unsupported = inbound
        .receive_message(
            replica("remote"),
            missing,
            SerializedMessage::new(
                REPLICATOR_READ_RESULT_SERIALIZER_ID,
                Manifest::new(ReplicatorRead::MANIFEST),
                ReplicatorRead::VERSION,
                Bytes::new(),
            ),
        )
        .expect_err("request manifest is not a reply manifest");
    assert!(matches!(
        unsupported,
        ReplicatorRemoteReplyError::UnsupportedManifest(_)
    ));
    system.terminate(Duration::from_secs(1)).unwrap();
}
