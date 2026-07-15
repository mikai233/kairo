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
    UnknownRecipient(String),
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
            Self::UnknownRecipient(path) => {
                write!(
                    f,
                    "no replicated-data family owns remote recipient `{path}`"
                )
            }
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
    reply_actor_prefix: String,
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
            reply_actor_prefix: "ddata-remote".to_string(),
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
            reply_actor_prefix: "ddata-remote".to_string(),
        }
    }

    pub fn recipient(&self) -> &ActorRefWireData {
        self.envelope.recipient()
    }

    pub fn with_reply_actor_prefix(mut self, value: impl Into<String>) -> Self {
        self.reply_actor_prefix = value.into();
        self
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
        format!("{}-{kind}-reply-{id}", self.reply_actor_prefix)
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
mod tests;
