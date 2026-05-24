use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use kairo_actor::{ActorRef, Recipient, SendError};
use kairo_serialization::{Registry, RemoteMessage, SerializationError, SerializedMessage};

use crate::{
    CrdtDataCodec, DeltaPropagationReceiveReport, DeltaReplicatedData, DirectReadResult,
    DirectWriteResult, ReplicaId, ReplicatorActorMsg, ReplicatorDeltaAck, ReplicatorDeltaNack,
    ReplicatorDeltaPropagation, ReplicatorRead, ReplicatorReadResult, ReplicatorWrite,
    ReplicatorWriteAck, ReplicatorWriteNack,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorSerializedMessage {
    pub target: ReplicaId,
    pub message: SerializedMessage,
}

impl ReplicatorSerializedMessage {
    pub fn new(target: ReplicaId, message: SerializedMessage) -> Self {
        Self { target, message }
    }
}

#[derive(Debug)]
pub enum ReplicatorWireError {
    Serialization(SerializationError),
    Send(String),
    UnsupportedManifest(String),
    WrongTarget {
        expected: ReplicaId,
        actual: ReplicaId,
    },
}

impl Display for ReplicatorWireError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Serialization(error) => write!(f, "replicator serialization failed: {error}"),
            Self::Send(reason) => write!(f, "replicator delivery failed: {reason}"),
            Self::UnsupportedManifest(manifest) => {
                write!(f, "unsupported replicator manifest `{manifest}`")
            }
            Self::WrongTarget { expected, actual } => {
                write!(
                    f,
                    "replicator message was addressed to {}, expected {}",
                    actual.as_str(),
                    expected.as_str()
                )
            }
        }
    }
}

impl std::error::Error for ReplicatorWireError {}

impl From<SerializationError> for ReplicatorWireError {
    fn from(error: SerializationError) -> Self {
        Self::Serialization(error)
    }
}

#[derive(Clone)]
pub struct ReplicatorWireOutbound {
    target: ReplicaId,
    registry: Arc<Registry>,
    outbound: Arc<dyn Recipient<ReplicatorSerializedMessage> + Send + Sync>,
}

impl ReplicatorWireOutbound {
    pub fn new(
        target: ReplicaId,
        registry: Arc<Registry>,
        outbound: impl Recipient<ReplicatorSerializedMessage> + Send + Sync + 'static,
    ) -> Self {
        Self {
            target,
            registry,
            outbound: Arc::new(outbound),
        }
    }

    pub fn from_arc(
        target: ReplicaId,
        registry: Arc<Registry>,
        outbound: Arc<dyn Recipient<ReplicatorSerializedMessage> + Send + Sync>,
    ) -> Self {
        Self {
            target,
            registry,
            outbound,
        }
    }

    pub fn target(&self) -> &ReplicaId {
        &self.target
    }

    pub fn send<M>(&self, message: &M) -> Result<(), ReplicatorWireError>
    where
        M: RemoteMessage,
    {
        let serialized = self.registry.serialize(message)?;
        self.outbound
            .tell(ReplicatorSerializedMessage::new(
                self.target.clone(),
                serialized,
            ))
            .map_err(|error| ReplicatorWireError::Send(error.reason().to_string()))
    }
}

impl Recipient<ReplicatorDeltaPropagation> for ReplicatorWireOutbound {
    fn tell(
        &self,
        message: ReplicatorDeltaPropagation,
    ) -> Result<(), SendError<ReplicatorDeltaPropagation>> {
        self.send(&message)
            .map_err(|error| SendError::new(message, error.to_string()))
    }
}

impl Recipient<ReplicatorWrite> for ReplicatorWireOutbound {
    fn tell(&self, message: ReplicatorWrite) -> Result<(), SendError<ReplicatorWrite>> {
        self.send(&message)
            .map_err(|error| SendError::new(message, error.to_string()))
    }
}

impl Recipient<ReplicatorRead> for ReplicatorWireOutbound {
    fn tell(&self, message: ReplicatorRead) -> Result<(), SendError<ReplicatorRead>> {
        self.send(&message)
            .map_err(|error| SendError::new(message, error.to_string()))
    }
}

#[derive(Clone)]
pub struct ReplicatorWireCodecs<D>
where
    D: DeltaReplicatedData + Send + 'static,
    D::Delta: Send + 'static,
{
    delta_codec: Arc<dyn CrdtDataCodec<D::Delta> + Send + Sync>,
    data_codec: Arc<dyn CrdtDataCodec<D> + Send + Sync>,
}

impl<D> ReplicatorWireCodecs<D>
where
    D: DeltaReplicatedData + Send + 'static,
    D::Delta: Send + 'static,
{
    pub fn new(
        delta_codec: Arc<dyn CrdtDataCodec<D::Delta> + Send + Sync>,
        data_codec: Arc<dyn CrdtDataCodec<D> + Send + Sync>,
    ) -> Self {
        Self {
            delta_codec,
            data_codec,
        }
    }
}

#[derive(Clone)]
pub struct ReplicatorWireReplies {
    delta_reply_to: ActorRef<DeltaPropagationReceiveReport>,
    write_reply_to: ActorRef<DirectWriteResult>,
    read_reply_to: ActorRef<Result<DirectReadResult, String>>,
}

impl ReplicatorWireReplies {
    pub fn new(
        delta_reply_to: ActorRef<DeltaPropagationReceiveReport>,
        write_reply_to: ActorRef<DirectWriteResult>,
        read_reply_to: ActorRef<Result<DirectReadResult, String>>,
    ) -> Self {
        Self {
            delta_reply_to,
            write_reply_to,
            read_reply_to,
        }
    }
}

#[derive(Clone)]
pub struct ReplicatorWireInbound<D>
where
    D: DeltaReplicatedData + Send + 'static,
    D::Delta: Send + 'static,
{
    self_replica: ReplicaId,
    registry: Arc<Registry>,
    replicator: ActorRef<ReplicatorActorMsg<D>>,
    codecs: ReplicatorWireCodecs<D>,
    replies: ReplicatorWireReplies,
}

impl<D> ReplicatorWireInbound<D>
where
    D: DeltaReplicatedData + Send + 'static,
    D::Delta: Send + 'static,
{
    pub fn new(
        self_replica: ReplicaId,
        registry: Arc<Registry>,
        replicator: ActorRef<ReplicatorActorMsg<D>>,
        codecs: ReplicatorWireCodecs<D>,
        replies: ReplicatorWireReplies,
    ) -> Self {
        Self {
            self_replica,
            registry,
            replicator,
            codecs,
            replies,
        }
    }

    pub fn receive(
        &self,
        envelope: ReplicatorSerializedMessage,
    ) -> Result<(), ReplicatorWireError> {
        if envelope.target != self.self_replica {
            return Err(ReplicatorWireError::WrongTarget {
                expected: self.self_replica.clone(),
                actual: envelope.target,
            });
        }
        self.receive_message(envelope.message)
    }

    pub fn receive_message(&self, message: SerializedMessage) -> Result<(), ReplicatorWireError> {
        match message.manifest.as_str() {
            ReplicatorDeltaPropagation::MANIFEST => {
                let propagation = self
                    .registry
                    .deserialize::<ReplicatorDeltaPropagation>(message)?;
                self.replicator
                    .tell(ReplicatorActorMsg::ApplyDeltaPropagation {
                        propagation,
                        codec: Arc::clone(&self.codecs.delta_codec),
                        reply_to: self.replies.delta_reply_to.clone(),
                    })
                    .map_err(|error| ReplicatorWireError::Send(error.reason().to_string()))
            }
            ReplicatorWrite::MANIFEST => {
                let write = self.registry.deserialize::<ReplicatorWrite>(message)?;
                self.replicator
                    .tell(ReplicatorActorMsg::ApplyWrite {
                        write,
                        codec: Arc::clone(&self.codecs.data_codec),
                        reply_to: self.replies.write_reply_to.clone(),
                    })
                    .map_err(|error| ReplicatorWireError::Send(error.reason().to_string()))
            }
            ReplicatorRead::MANIFEST => {
                let read = self.registry.deserialize::<ReplicatorRead>(message)?;
                self.replicator
                    .tell(ReplicatorActorMsg::ServeRead {
                        read,
                        codec: Arc::clone(&self.codecs.data_codec),
                        reply_to: self.replies.read_reply_to.clone(),
                    })
                    .map_err(|error| ReplicatorWireError::Send(error.reason().to_string()))
            }
            ReplicatorDeltaAck::MANIFEST
            | ReplicatorDeltaNack::MANIFEST
            | ReplicatorWriteAck::MANIFEST
            | ReplicatorWriteNack::MANIFEST
            | ReplicatorReadResult::MANIFEST => Ok(()),
            manifest => Err(ReplicatorWireError::UnsupportedManifest(
                manifest.to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::mpsc::{self, Receiver};
    use std::time::Duration;

    use kairo_actor::{Actor, ActorResult, ActorSystem, Context, Props, Recipient};
    use kairo_serialization::{Manifest, RemoteMessage};

    use super::*;
    use crate::{
        DataEnvelope, DeltaPropagationLog, DeltaReceiveReply, DeltaReplicatedData, GCounter,
        GCounterCodec, GetResponse, REPLICATOR_DELTA_PROPAGATION_SERIALIZER_ID,
        REPLICATOR_READ_SERIALIZER_ID, REPLICATOR_WRITE_SERIALIZER_ID, ReadConsistency,
        ReplicatorActor, ReplicatorKey, ReplicatorReadResult, register_ddata_protocol_codecs,
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

    fn delta_propagation(
        from: ReplicaId,
        key: &ReplicatorKey,
        value: u128,
    ) -> ReplicatorDeltaPropagation {
        let mut log = DeltaPropagationLog::new([replica("local")]);
        log.record_delta(key.clone(), Some(counter(from.as_str(), value)));
        let propagation = log
            .collect_propagations()
            .remove(&replica("local"))
            .unwrap();
        crate::encode_delta_propagation(from, true, &propagation, &GCounterCodec).unwrap()
    }

    fn wire_codecs() -> ReplicatorWireCodecs<GCounter> {
        ReplicatorWireCodecs::new(Arc::new(GCounterCodec), Arc::new(GCounterCodec))
    }

    #[test]
    fn wire_outbound_serializes_delta_write_and_read_for_target_replica() {
        let system = ActorSystem::builder("ddata-wire-outbound").build().unwrap();
        let registry = registry();
        let (outbound_ref, outbound_rx) = probe::<ReplicatorSerializedMessage>(&system, "wire-out");
        let target = replica("remote");
        let outbound = ReplicatorWireOutbound::new(target.clone(), registry.clone(), outbound_ref);
        let key = ReplicatorKey::new("counter");

        let delta = delta_propagation(replica("local"), &key, 3);
        outbound.tell(delta.clone()).unwrap();
        let delta_envelope = outbound_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(delta_envelope.target, target);
        assert_eq!(
            delta_envelope.message.serializer_id,
            REPLICATOR_DELTA_PROPAGATION_SERIALIZER_ID
        );
        assert_eq!(
            registry
                .deserialize::<ReplicatorDeltaPropagation>(delta_envelope.message)
                .unwrap(),
            delta
        );

        let write = crate::encode_write(
            &key,
            Some(replica("local")),
            &DataEnvelope::new(counter("local", 5).reset_delta()),
            &GCounterCodec,
        )
        .unwrap();
        outbound.tell(write.clone()).unwrap();
        let write_envelope = outbound_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(
            write_envelope.message.serializer_id,
            REPLICATOR_WRITE_SERIALIZER_ID
        );
        assert_eq!(
            registry
                .deserialize::<ReplicatorWrite>(write_envelope.message)
                .unwrap(),
            write
        );

        let read = crate::encode_read(&key, Some(replica("local")));
        outbound.tell(read.clone()).unwrap();
        let read_envelope = outbound_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(
            read_envelope.message.serializer_id,
            REPLICATOR_READ_SERIALIZER_ID
        );
        assert_eq!(
            registry
                .deserialize::<ReplicatorRead>(read_envelope.message)
                .unwrap(),
            read
        );
        system.terminate(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn wire_inbound_applies_delta_propagation_to_replicator_actor() {
        let system = ActorSystem::builder("ddata-wire-delta-in").build().unwrap();
        let registry = registry();
        let replicator = system
            .spawn("replicator", Props::new(ReplicatorActor::<GCounter>::new))
            .unwrap();
        let (delta_reply, delta_rx) =
            probe::<DeltaPropagationReceiveReport>(&system, "delta-report");
        let (write_reply, _write_rx) = probe::<DirectWriteResult>(&system, "write-report");
        let (read_reply, _read_rx) =
            probe::<Result<DirectReadResult, String>>(&system, "read-report");
        let key = ReplicatorKey::new("counter");
        let inbound = ReplicatorWireInbound::new(
            replica("local"),
            registry.clone(),
            replicator.clone(),
            wire_codecs(),
            ReplicatorWireReplies::new(delta_reply, write_reply, read_reply),
        );

        let propagation = delta_propagation(replica("remote"), &key, 7);
        inbound
            .receive(ReplicatorSerializedMessage::new(
                replica("local"),
                registry.serialize(&propagation).unwrap(),
            ))
            .unwrap();

        let report = delta_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(report.from(), &replica("remote"));
        assert!(report.is_success());
        assert!(matches!(report.reply(), Some(DeltaReceiveReply::Ack(_))));

        let (get_ref, get_rx) = probe::<GetResponse<GCounter>>(&system, "get");
        replicator
            .tell(ReplicatorActorMsg::Get {
                key: key.clone(),
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
            7
        );
        system.terminate(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn wire_inbound_applies_write_and_serves_read() {
        let system = ActorSystem::builder("ddata-wire-write-read-in")
            .build()
            .unwrap();
        let registry = registry();
        let replicator = system
            .spawn("replicator", Props::new(ReplicatorActor::<GCounter>::new))
            .unwrap();
        let (delta_reply, _delta_rx) =
            probe::<DeltaPropagationReceiveReport>(&system, "delta-report");
        let (write_reply, write_rx) = probe::<DirectWriteResult>(&system, "write-report");
        let (read_reply, read_rx) =
            probe::<Result<DirectReadResult, String>>(&system, "read-report");
        let key = ReplicatorKey::new("counter");
        let inbound = ReplicatorWireInbound::new(
            replica("local"),
            registry.clone(),
            replicator,
            wire_codecs(),
            ReplicatorWireReplies::new(delta_reply, write_reply, read_reply),
        );
        let write = crate::encode_write(
            &key,
            Some(replica("remote")),
            &DataEnvelope::new(counter("remote", 11).reset_delta()),
            &GCounterCodec,
        )
        .unwrap();

        inbound
            .receive(ReplicatorSerializedMessage::new(
                replica("local"),
                registry.serialize(&write).unwrap(),
            ))
            .unwrap();
        assert!(matches!(
            write_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            DirectWriteResult::Ack { .. }
        ));

        let read = crate::encode_read(&key, Some(replica("remote")));
        inbound
            .receive(ReplicatorSerializedMessage::new(
                replica("local"),
                registry.serialize(&read).unwrap(),
            ))
            .unwrap();
        let read_result = read_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .unwrap();
        assert_eq!(
            read_result
                .message()
                .envelope
                .as_ref()
                .unwrap()
                .crdt_manifest,
            crate::GCOUNTER_MANIFEST
        );
        system.terminate(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn wire_inbound_rejects_wrong_target_and_unknown_manifest() {
        let system = ActorSystem::builder("ddata-wire-reject").build().unwrap();
        let registry = registry();
        let replicator = system
            .spawn("replicator", Props::new(ReplicatorActor::<GCounter>::new))
            .unwrap();
        let (delta_reply, _delta_rx) =
            probe::<DeltaPropagationReceiveReport>(&system, "delta-report");
        let (write_reply, _write_rx) = probe::<DirectWriteResult>(&system, "write-report");
        let (read_reply, _read_rx) =
            probe::<Result<DirectReadResult, String>>(&system, "read-report");
        let inbound = ReplicatorWireInbound::new(
            replica("local"),
            registry.clone(),
            replicator,
            wire_codecs(),
            ReplicatorWireReplies::new(delta_reply, write_reply, read_reply),
        );

        let wrong_target = inbound
            .receive(ReplicatorSerializedMessage::new(
                replica("other"),
                SerializedMessage::new(
                    REPLICATOR_DELTA_PROPAGATION_SERIALIZER_ID,
                    Manifest::new(ReplicatorDeltaPropagation::MANIFEST),
                    ReplicatorDeltaPropagation::VERSION,
                    bytes::Bytes::new(),
                ),
            ))
            .expect_err("wrong target should fail");
        assert!(matches!(
            wrong_target,
            ReplicatorWireError::WrongTarget { .. }
        ));

        let unknown = inbound
            .receive_message(SerializedMessage::new(
                9_999,
                Manifest::new("kairo.ddata.unknown"),
                1,
                bytes::Bytes::new(),
            ))
            .expect_err("unknown manifest should fail");
        assert!(matches!(
            unknown,
            ReplicatorWireError::UnsupportedManifest(_)
        ));
        let ignored_reply = registry
            .serialize(&ReplicatorReadResult { envelope: None })
            .unwrap();
        inbound.receive_message(ignored_reply).unwrap();
        system.terminate(Duration::from_secs(1)).unwrap();
    }
}
