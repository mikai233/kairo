#![deny(missing_docs)]

//! Transport-neutral wire adapters for direct replicator requests.
//!
//! These types bridge registered stable [`RemoteMessage`] codecs to the typed
//! replicator mailbox. They are a lower-level transport boundary, not a
//! replacement for Kairo's typed distributed-data API.

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
/// A serialized replicator request paired with its logical destination.
pub struct ReplicatorSerializedMessage {
    /// The replica that must receive the request.
    pub target: ReplicaId,
    /// The stable serializer metadata and payload.
    pub message: SerializedMessage,
}

impl ReplicatorSerializedMessage {
    /// Creates a request envelope for `target`.
    pub fn new(target: ReplicaId, message: SerializedMessage) -> Self {
        Self { target, message }
    }
}

#[derive(Debug)]
/// A failure while encoding, routing, or dispatching a replicator request.
pub enum ReplicatorWireError {
    /// Stable-message serialization or deserialization failed.
    Serialization(SerializationError),
    /// The transport or typed replicator mailbox rejected delivery.
    Send(String),
    /// The inbound request manifest is not supported by this adapter.
    UnsupportedManifest(String),
    /// The envelope was addressed to a different logical replica.
    WrongTarget {
        /// The local replica expected by the inbound adapter.
        expected: ReplicaId,
        /// The target carried by the received envelope.
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
/// Serializes typed direct requests for one logical replica.
pub struct ReplicatorWireOutbound {
    target: ReplicaId,
    registry: Arc<Registry>,
    outbound: Arc<dyn Recipient<ReplicatorSerializedMessage> + Send + Sync>,
}

impl ReplicatorWireOutbound {
    /// Creates an outbound adapter from an owned transport recipient.
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

    /// Creates an outbound adapter from a shared transport recipient.
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

    /// Returns the logical destination attached to every request.
    pub fn target(&self) -> &ReplicaId {
        &self.target
    }

    /// Serializes and sends a registered stable remote message.
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
/// The delta and full-state codecs for one typed CRDT family.
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
    /// Creates the paired codecs used for delta and full-state operations.
    pub fn new(
        delta_codec: Arc<dyn CrdtDataCodec<D::Delta> + Send + Sync>,
        data_codec: Arc<dyn CrdtDataCodec<D> + Send + Sync>,
    ) -> Self {
        Self {
            delta_codec,
            data_codec,
        }
    }

    /// Returns the codec for this family's delta type.
    pub fn delta_codec(&self) -> Arc<dyn CrdtDataCodec<D::Delta> + Send + Sync> {
        Arc::clone(&self.delta_codec)
    }

    /// Returns the codec for this family's full replicated-data type.
    pub fn data_codec(&self) -> Arc<dyn CrdtDataCodec<D> + Send + Sync> {
        Arc::clone(&self.data_codec)
    }
}

#[derive(Clone)]
/// Mailbox recipients for direct-request outcomes.
///
/// Supplying actor refs keeps delta, write, and read completion inside actor
/// turns even when the request arrived through an external transport.
pub struct ReplicatorWireReplies {
    delta_reply_to: ActorRef<DeltaPropagationReceiveReport>,
    write_reply_to: ActorRef<DirectWriteResult>,
    read_reply_to: ActorRef<Result<DirectReadResult, String>>,
}

impl ReplicatorWireReplies {
    /// Creates the reply-recipient set used by an inbound adapter.
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
/// Validates and dispatches serialized direct requests to a typed replicator.
///
/// The adapter accepts only the delta-propagation, direct-write, and
/// direct-read manifests. [`Self::receive`] additionally requires an exact
/// logical target match before any deserialization or mailbox delivery.
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
    /// Creates an inbound adapter for one local replica and CRDT family.
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

    /// Validates the envelope target and dispatches its serialized request.
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

    /// Decodes and dispatches a request after target validation.
    ///
    /// Callers that have not independently established the logical recipient
    /// should use [`Self::receive`] instead.
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
