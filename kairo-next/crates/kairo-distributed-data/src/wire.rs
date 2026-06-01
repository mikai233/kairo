use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use kairo_actor::{ActorRef, Recipient, SendError};
use kairo_serialization::{Registry, RemoteMessage, SerializationError, SerializedMessage};

use crate::{
    CrdtDataCodec, DeltaPropagationReceiveReport, DeltaReplicatedData, DirectReadResult,
    DirectWriteResult, ReplicaId, ReplicatorActorMsg, ReplicatorDeltaPropagation, ReplicatorRead,
    ReplicatorWrite,
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

    pub fn delta_codec(&self) -> Arc<dyn CrdtDataCodec<D::Delta> + Send + Sync> {
        Arc::clone(&self.delta_codec)
    }

    pub fn data_codec(&self) -> Arc<dyn CrdtDataCodec<D> + Send + Sync> {
        Arc::clone(&self.data_codec)
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
            manifest => Err(ReplicatorWireError::UnsupportedManifest(
                manifest.to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests;
