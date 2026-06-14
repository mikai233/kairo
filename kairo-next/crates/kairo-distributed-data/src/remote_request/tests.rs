use std::sync::{
    Arc,
    mpsc::{self, Receiver},
};
use std::time::Duration;

use kairo_actor::{ActorSystem, Props};
use kairo_serialization::{ActorRefWireData, Manifest, RemoteEnvelope, SerializedMessage};

use super::*;
use crate::{
    DataEnvelope, DeltaPropagationLog, DeltaReplicatedData, GCounter, GCounterCodec, GetResponse,
    REPLICATOR_DELTA_ACK_SERIALIZER_ID, REPLICATOR_READ_RESULT_SERIALIZER_ID,
    REPLICATOR_WRITE_ACK_SERIALIZER_ID, ReadConsistency, ReplicatorActor, ReplicatorKey,
    ReplicatorReadResult, register_ddata_protocol_codecs,
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

fn delta_counter(replica_id: &str, value: u128) -> GCounter {
    GCounter::new()
        .increment(replica(replica_id), value)
        .unwrap()
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

fn wire_codecs() -> ReplicatorWireCodecs<GCounter> {
    ReplicatorWireCodecs::new(Arc::new(GCounterCodec), Arc::new(GCounterCodec))
}

#[test]
fn remote_request_inbound_applies_write_and_replies_to_sender_ref() {
    let system = ActorSystem::builder("ddata-remote-request-write")
        .build()
        .unwrap();
    let registry = registry();
    let replicator = system
        .spawn("replicator", Props::new(ReplicatorActor::<GCounter>::new))
        .unwrap();
    let (outbound_ref, outbound_rx) = probe::<ReplicatorRemoteEnvelope>(&system, "remote-out");
    let local_ref = actor_ref(&replicator);
    let remote_sender = wire_ref("kairo://remote@127.0.0.1:25520/user/write-agg#4");
    let inbound = ReplicatorRemoteRequestInbound::new(
        system.clone(),
        local_ref.clone(),
        Some(local_ref.clone()),
        registry.clone(),
        replicator.clone(),
        wire_codecs(),
        outbound_ref,
    );
    let key = ReplicatorKey::new("counter");
    let write = crate::encode_write(
        &key,
        Some(replica("remote")),
        &DataEnvelope::new(counter("remote", 12)),
        &GCounterCodec,
    )
    .unwrap();

    inbound
        .receive_from(
            replica("remote"),
            RemoteEnvelope::new(
                local_ref.clone(),
                Some(remote_sender.clone()),
                registry.serialize(&write).unwrap(),
            ),
        )
        .unwrap();

    let reply = outbound_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(reply.target, replica("remote"));
    assert_eq!(reply.envelope.recipient, remote_sender);
    assert_eq!(reply.envelope.sender, Some(local_ref.clone()));
    assert_eq!(
        reply.envelope.message.serializer_id,
        REPLICATOR_WRITE_ACK_SERIALIZER_ID
    );

    let (get_ref, get_rx) = probe::<GetResponse<GCounter>>(&system, "get");
    replicator
        .tell(ReplicatorActorMsg::Get {
            key,
            consistency: ReadConsistency::local(),
            reply_to: get_ref,
        })
        .unwrap();
    assert_eq!(
        get_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .data()
            .unwrap()
            .value()
            .unwrap(),
        12
    );
    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn remote_request_inbound_applies_delta_and_replies_to_sender_ref_when_requested() {
    let system = ActorSystem::builder("ddata-remote-request-delta")
        .build()
        .unwrap();
    let registry = registry();
    let replicator = system
        .spawn("replicator", Props::new(ReplicatorActor::<GCounter>::new))
        .unwrap();
    let (outbound_ref, outbound_rx) = probe::<ReplicatorRemoteEnvelope>(&system, "remote-out");
    let local_ref = actor_ref(&replicator);
    let remote_sender = wire_ref("kairo://remote@127.0.0.1:25520/user/delta-ack#6");
    let inbound = ReplicatorRemoteRequestInbound::new(
        system.clone(),
        local_ref.clone(),
        Some(local_ref.clone()),
        registry.clone(),
        replicator.clone(),
        wire_codecs(),
        outbound_ref,
    );
    let key = ReplicatorKey::new("counter");
    let mut log = DeltaPropagationLog::new([replica("local")]);
    log.record_delta(key.clone(), Some(delta_counter("remote", 3)));
    let propagation = log.collect_propagations().into_values().next().unwrap();
    let wire =
        crate::encode_delta_propagation(replica("remote"), true, &propagation, &GCounterCodec)
            .unwrap();

    inbound
        .receive_from(
            replica("remote"),
            RemoteEnvelope::new(
                local_ref.clone(),
                Some(remote_sender.clone()),
                registry.serialize(&wire).unwrap(),
            ),
        )
        .unwrap();

    let reply = outbound_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(reply.target, replica("remote"));
    assert_eq!(reply.envelope.recipient, remote_sender);
    assert_eq!(reply.envelope.sender, Some(local_ref.clone()));
    assert_eq!(
        reply.envelope.message.serializer_id,
        REPLICATOR_DELTA_ACK_SERIALIZER_ID
    );

    let (get_ref, get_rx) = probe::<GetResponse<GCounter>>(&system, "get-delta");
    replicator
        .tell(ReplicatorActorMsg::Get {
            key,
            consistency: ReadConsistency::local(),
            reply_to: get_ref,
        })
        .unwrap();
    assert_eq!(
        get_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .data()
            .unwrap()
            .value()
            .unwrap(),
        3
    );
    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn remote_request_inbound_serves_read_and_replies_to_sender_ref() {
    let system = ActorSystem::builder("ddata-remote-request-read")
        .build()
        .unwrap();
    let registry = registry();
    let replicator = system
        .spawn("replicator", Props::new(ReplicatorActor::<GCounter>::new))
        .unwrap();
    let (outbound_ref, outbound_rx) = probe::<ReplicatorRemoteEnvelope>(&system, "remote-out");
    let local_ref = actor_ref(&replicator);
    let remote_sender = wire_ref("kairo://remote@127.0.0.1:25520/user/read-agg#5");
    let inbound = ReplicatorRemoteRequestInbound::new(
        system.clone(),
        local_ref.clone(),
        Some(local_ref.clone()),
        registry.clone(),
        replicator.clone(),
        wire_codecs(),
        outbound_ref,
    );
    let key = ReplicatorKey::new("counter");
    replicator
        .tell(ReplicatorActorMsg::WriteFull {
            key: key.clone(),
            envelope: DataEnvelope::new(counter("local", 7)),
        })
        .unwrap();
    let read = crate::encode_read(&key, Some(replica("remote")));

    inbound
        .receive_from(
            replica("remote"),
            RemoteEnvelope::new(
                local_ref.clone(),
                Some(remote_sender.clone()),
                registry.serialize(&read).unwrap(),
            ),
        )
        .unwrap();

    let reply = outbound_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(reply.target, replica("remote"));
    assert_eq!(reply.envelope.recipient, remote_sender);
    assert_eq!(reply.envelope.sender, Some(local_ref));
    assert_eq!(
        reply.envelope.message.serializer_id,
        REPLICATOR_READ_RESULT_SERIALIZER_ID
    );
    assert!(
        registry
            .deserialize::<ReplicatorReadResult>(reply.envelope.message)
            .unwrap()
            .envelope
            .is_some()
    );
    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn remote_request_inbound_rejects_missing_sender_and_unknown_manifest() {
    let system = ActorSystem::builder("ddata-remote-request-errors")
        .build()
        .unwrap();
    let registry = registry();
    let replicator = system
        .spawn("replicator", Props::new(ReplicatorActor::<GCounter>::new))
        .unwrap();
    let (outbound_ref, _outbound_rx) = probe::<ReplicatorRemoteEnvelope>(&system, "remote-out");
    let local_ref = actor_ref(&replicator);
    let inbound = ReplicatorRemoteRequestInbound::new(
        system.clone(),
        local_ref.clone(),
        Some(local_ref.clone()),
        registry.clone(),
        replicator.clone(),
        wire_codecs(),
        outbound_ref,
    );

    let missing_sender = inbound
        .receive_from(
            replica("remote"),
            RemoteEnvelope::new(
                local_ref,
                None,
                registry
                    .serialize(&crate::encode_read(
                        &ReplicatorKey::new("counter"),
                        Some(replica("remote")),
                    ))
                    .unwrap(),
            ),
        )
        .expect_err("direct read without sender cannot be replied to");
    assert!(matches!(
        missing_sender,
        ReplicatorRemoteRequestError::MissingSender(_)
    ));

    let unknown = inbound
        .receive_from(
            replica("remote"),
            RemoteEnvelope::new(
                inbound.recipient().clone(),
                Some(wire_ref("kairo://remote/user/agg#1")),
                SerializedMessage::new(
                    REPLICATOR_READ_RESULT_SERIALIZER_ID,
                    Manifest::new(ReplicatorReadResult::MANIFEST),
                    ReplicatorReadResult::VERSION,
                    bytes::Bytes::new(),
                ),
            ),
        )
        .expect_err("reply manifest is not a request manifest");
    assert!(matches!(
        unknown,
        ReplicatorRemoteRequestError::UnsupportedManifest(_)
    ));
    system.terminate(Duration::from_secs(1)).unwrap();
}
