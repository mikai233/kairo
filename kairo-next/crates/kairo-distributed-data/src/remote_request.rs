use std::fmt::{self, Display, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use kairo_actor::{
    Actor, ActorError, ActorRef, ActorResult, ActorSystem, Context, Props, Recipient,
};
use kairo_serialization::{
    ActorRefWireData, Registry, RemoteEnvelope, RemoteMessage, SerializationError,
};

use crate::{
    DeltaPropagationReceiveReport, DeltaReplicatedData, DirectReadResult, DirectWriteResult,
    ReplicaId, ReplicatorActorMsg, ReplicatorDeltaPropagation, ReplicatorGossip,
    ReplicatorGossipStatus, ReplicatorRead, ReplicatorRemoteEnvelope,
    ReplicatorRemoteEnvelopeError, ReplicatorRemoteEnvelopeInbound, ReplicatorRemoteReplyOutbound,
    ReplicatorRemoteTarget, ReplicatorWireCodecs, ReplicatorWrite,
};

#[derive(Debug)]
pub enum ReplicatorRemoteRequestError {
    Envelope(ReplicatorRemoteEnvelopeError),
    Serialization(SerializationError),
    MissingSender(&'static str),
    Spawn(String),
    Send(String),
    UnsupportedManifest(String),
}

impl Display for ReplicatorRemoteRequestError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Envelope(error) => write!(f, "{error}"),
            Self::Serialization(error) => write!(f, "replicator request decode failed: {error}"),
            Self::MissingSender(manifest) => {
                write!(f, "remote replicator request `{manifest}` has no sender")
            }
            Self::Spawn(reason) => {
                write!(f, "remote replicator reply actor spawn failed: {reason}")
            }
            Self::Send(reason) => write!(f, "remote replicator request delivery failed: {reason}"),
            Self::UnsupportedManifest(manifest) => {
                write!(
                    f,
                    "unsupported remote replicator request manifest `{manifest}`"
                )
            }
        }
    }
}

impl std::error::Error for ReplicatorRemoteRequestError {}

impl From<ReplicatorRemoteEnvelopeError> for ReplicatorRemoteRequestError {
    fn from(error: ReplicatorRemoteEnvelopeError) -> Self {
        Self::Envelope(error)
    }
}

impl From<SerializationError> for ReplicatorRemoteRequestError {
    fn from(error: SerializationError) -> Self {
        Self::Serialization(error)
    }
}

pub struct ReplicatorRemoteRequestInbound<D>
where
    D: DeltaReplicatedData + Send + 'static,
    D::Delta: Send + 'static,
{
    system: ActorSystem,
    envelope: ReplicatorRemoteEnvelopeInbound,
    local_sender: Option<ActorRefWireData>,
    registry: Arc<Registry>,
    replicator: ActorRef<ReplicatorActorMsg<D>>,
    codecs: ReplicatorWireCodecs<D>,
    outbound: Arc<dyn Recipient<ReplicatorRemoteEnvelope> + Send + Sync>,
    next_reply_actor: AtomicU64,
}

impl<D> ReplicatorRemoteRequestInbound<D>
where
    D: DeltaReplicatedData + Send + 'static,
    D::Delta: Send + 'static,
{
    pub fn new(
        system: ActorSystem,
        recipient: ActorRefWireData,
        local_sender: Option<ActorRefWireData>,
        registry: Arc<Registry>,
        replicator: ActorRef<ReplicatorActorMsg<D>>,
        codecs: ReplicatorWireCodecs<D>,
        outbound: impl Recipient<ReplicatorRemoteEnvelope> + Send + Sync + 'static,
    ) -> Self {
        Self {
            system,
            envelope: ReplicatorRemoteEnvelopeInbound::new(recipient),
            local_sender,
            registry,
            replicator,
            codecs,
            outbound: Arc::new(outbound),
            next_reply_actor: AtomicU64::new(0),
        }
    }

    pub fn from_arc(
        system: ActorSystem,
        recipient: ActorRefWireData,
        local_sender: Option<ActorRefWireData>,
        registry: Arc<Registry>,
        replicator: ActorRef<ReplicatorActorMsg<D>>,
        codecs: ReplicatorWireCodecs<D>,
        outbound: Arc<dyn Recipient<ReplicatorRemoteEnvelope> + Send + Sync>,
    ) -> Self {
        Self {
            system,
            envelope: ReplicatorRemoteEnvelopeInbound::new(recipient),
            local_sender,
            registry,
            replicator,
            codecs,
            outbound,
            next_reply_actor: AtomicU64::new(0),
        }
    }

    pub fn recipient(&self) -> &ActorRefWireData {
        self.envelope.recipient()
    }

    pub fn receive_from(
        &self,
        from: ReplicaId,
        envelope: RemoteEnvelope,
    ) -> Result<(), ReplicatorRemoteRequestError> {
        let inbound = self.envelope.receive(envelope)?;
        match inbound.message.manifest.as_str() {
            ReplicatorDeltaPropagation::MANIFEST => {
                let propagation = self
                    .registry
                    .deserialize::<ReplicatorDeltaPropagation>(inbound.message)?;
                let reply_to = if propagation.reply {
                    let sender =
                        inbound
                            .sender
                            .ok_or(ReplicatorRemoteRequestError::MissingSender(
                                ReplicatorDeltaPropagation::MANIFEST,
                            ))?;
                    self.spawn_delta_reply(from, Some(sender))?
                } else {
                    self.spawn_delta_reply(from, None)?
                };
                self.replicator
                    .tell(ReplicatorActorMsg::ApplyDeltaPropagation {
                        propagation,
                        codec: self.codecs.delta_codec(),
                        reply_to,
                    })
                    .map_err(|error| ReplicatorRemoteRequestError::Send(error.reason().to_string()))
            }
            ReplicatorWrite::MANIFEST => {
                let sender = inbound
                    .sender
                    .ok_or(ReplicatorRemoteRequestError::MissingSender(
                        ReplicatorWrite::MANIFEST,
                    ))?;
                let write = self
                    .registry
                    .deserialize::<ReplicatorWrite>(inbound.message)?;
                let reply_to = self.spawn_write_reply(from, sender)?;
                self.replicator
                    .tell(ReplicatorActorMsg::ApplyWrite {
                        write,
                        codec: self.codecs.data_codec(),
                        reply_to,
                    })
                    .map_err(|error| ReplicatorRemoteRequestError::Send(error.reason().to_string()))
            }
            ReplicatorRead::MANIFEST => {
                let sender = inbound
                    .sender
                    .ok_or(ReplicatorRemoteRequestError::MissingSender(
                        ReplicatorRead::MANIFEST,
                    ))?;
                let read = self
                    .registry
                    .deserialize::<ReplicatorRead>(inbound.message)?;
                let reply_to = self.spawn_read_reply(from, sender)?;
                self.replicator
                    .tell(ReplicatorActorMsg::ServeRead {
                        read,
                        codec: self.codecs.data_codec(),
                        reply_to,
                    })
                    .map_err(|error| ReplicatorRemoteRequestError::Send(error.reason().to_string()))
            }
            ReplicatorGossipStatus::MANIFEST => {
                let status = self
                    .registry
                    .deserialize::<ReplicatorGossipStatus>(inbound.message)?;
                self.replicator
                    .tell(ReplicatorActorMsg::ReceiveGossipStatus {
                        from,
                        status,
                        codec: self.codecs.data_codec(),
                        reply_to: None,
                    })
                    .map_err(|error| ReplicatorRemoteRequestError::Send(error.reason().to_string()))
            }
            ReplicatorGossip::MANIFEST => {
                let gossip = self
                    .registry
                    .deserialize::<ReplicatorGossip>(inbound.message)?;
                self.replicator
                    .tell(ReplicatorActorMsg::ReceiveGossip {
                        from,
                        gossip,
                        codec: self.codecs.data_codec(),
                        reply_to: None,
                    })
                    .map_err(|error| ReplicatorRemoteRequestError::Send(error.reason().to_string()))
            }
            manifest => Err(ReplicatorRemoteRequestError::UnsupportedManifest(
                manifest.to_string(),
            )),
        }
    }

    fn spawn_delta_reply(
        &self,
        target: ReplicaId,
        recipient: Option<ActorRefWireData>,
    ) -> Result<ActorRef<DeltaPropagationReceiveReport>, ReplicatorRemoteRequestError> {
        let outbound = recipient.map(|recipient| self.reply_outbound(target, recipient));
        self.system
            .spawn(
                self.next_reply_name("delta"),
                Props::new(move || RemoteDeltaReplyActor { outbound }),
            )
            .map_err(|error| ReplicatorRemoteRequestError::Spawn(error.to_string()))
    }

    fn spawn_write_reply(
        &self,
        target: ReplicaId,
        recipient: ActorRefWireData,
    ) -> Result<ActorRef<DirectWriteResult>, ReplicatorRemoteRequestError> {
        let outbound = self.reply_outbound(target, recipient);
        self.system
            .spawn(
                self.next_reply_name("write"),
                Props::new(move || RemoteWriteReplyActor { outbound }),
            )
            .map_err(|error| ReplicatorRemoteRequestError::Spawn(error.to_string()))
    }

    fn spawn_read_reply(
        &self,
        target: ReplicaId,
        recipient: ActorRefWireData,
    ) -> Result<ActorRef<Result<DirectReadResult, String>>, ReplicatorRemoteRequestError> {
        let outbound = self.reply_outbound(target, recipient);
        self.system
            .spawn(
                self.next_reply_name("read"),
                Props::new(move || RemoteReadReplyActor { outbound }),
            )
            .map_err(|error| ReplicatorRemoteRequestError::Spawn(error.to_string()))
    }

    fn reply_outbound(
        &self,
        target: ReplicaId,
        recipient: ActorRefWireData,
    ) -> ReplicatorRemoteReplyOutbound {
        ReplicatorRemoteReplyOutbound::from_arc(
            ReplicatorRemoteTarget::new(target, recipient),
            self.local_sender.clone(),
            Arc::clone(&self.registry),
            Arc::clone(&self.outbound),
        )
    }

    fn next_reply_name(&self, kind: &str) -> String {
        let id = self.next_reply_actor.fetch_add(1, Ordering::Relaxed);
        format!("ddata-remote-{kind}-reply-{id}")
    }
}

struct RemoteDeltaReplyActor {
    outbound: Option<ReplicatorRemoteReplyOutbound>,
}

impl Actor for RemoteDeltaReplyActor {
    type Msg = DeltaPropagationReceiveReport;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        if let Some(outbound) = &self.outbound {
            outbound
                .send_delta_report(&msg)
                .map_err(|error| ActorError::Message(error.to_string()))?;
        }
        ctx.stop(ctx.myself())
    }
}

struct RemoteWriteReplyActor {
    outbound: ReplicatorRemoteReplyOutbound,
}

impl Actor for RemoteWriteReplyActor {
    type Msg = DirectWriteResult;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        self.outbound
            .send_write_result(&msg)
            .map_err(|error| ActorError::Message(error.to_string()))?;
        ctx.stop(ctx.myself())
    }
}

struct RemoteReadReplyActor {
    outbound: ReplicatorRemoteReplyOutbound,
}

impl Actor for RemoteReadReplyActor {
    type Msg = Result<DirectReadResult, String>;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            Ok(result) => self
                .outbound
                .send_read_result(&result)
                .map_err(|error| ActorError::Message(error.to_string()))?,
            Err(reason) => return Err(ActorError::Message(reason)),
        }
        ctx.stop(ctx.myself())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc::{self, Receiver};
    use std::time::Duration;

    use kairo_actor::{ActorSystem, Props};
    use kairo_serialization::{ActorRefWireData, Manifest, RemoteEnvelope, SerializedMessage};

    use super::*;
    use crate::{
        DataEnvelope, DeltaReplicatedData, GCounter, GCounterCodec, GetResponse,
        REPLICATOR_READ_RESULT_SERIALIZER_ID, REPLICATOR_WRITE_ACK_SERIALIZER_ID, ReadConsistency,
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
}
