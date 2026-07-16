#![deny(missing_docs)]

use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use kairo_actor::{ActorSystem, Recipient, SendError};
use kairo_remote::{CanonicalLocalAddress, RemoteSettings};
use kairo_serialization::{
    ActorRefWireData, Registry, RemoteEnvelope, RemoteMessage, SerializationError,
    SerializedMessage,
};

use crate::{
    DeltaPropagationReceiveReport, DeltaReceiveReply, DirectReadResult, DirectWriteResult,
    ReadAggregationActorMsg, ReplicaId, ReplicatorDeltaAck, ReplicatorDeltaNack,
    ReplicatorReadResult, ReplicatorRemoteEnvelope, ReplicatorRemoteEnvelopeError,
    ReplicatorRemoteTarget, ReplicatorWireReply, ReplicatorWriteAck, ReplicatorWriteNack,
    WriteAggregationActorMsg,
};

#[derive(Debug)]
/// Failure while decoding or delivering a remote replicator reply.
pub enum ReplicatorRemoteReplyError {
    /// Stable-message deserialization failed.
    Serialization(SerializationError),
    /// The addressed local aggregation actor rejected the decoded reply.
    Send {
        /// Canonical local recipient path used for delivery.
        recipient: String,
        /// Actor delivery failure diagnostic.
        reason: String,
    },
    /// The serialized message manifest is not a supported replicator reply.
    UnsupportedManifest(String),
}

impl Display for ReplicatorRemoteReplyError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Serialization(error) => write!(f, "replicator reply decode failed: {error}"),
            Self::Send { recipient, reason } => {
                write!(
                    f,
                    "replicator reply delivery to `{recipient}` failed: {reason}"
                )
            }
            Self::UnsupportedManifest(manifest) => {
                write!(
                    f,
                    "unsupported remote replicator reply manifest `{manifest}`"
                )
            }
        }
    }
}

impl std::error::Error for ReplicatorRemoteReplyError {}

impl From<SerializationError> for ReplicatorRemoteReplyError {
    fn from(error: SerializationError) -> Self {
        Self::Serialization(error)
    }
}

#[derive(Clone)]
/// Converts local direct-operation results into remote reply envelopes.
///
/// The target actor reference is normally copied from the originating request sender. The
/// optional sender identifies the local replicator for diagnostics and later protocol extension.
pub struct ReplicatorRemoteReplyOutbound {
    target: ReplicatorRemoteTarget,
    sender: Option<ActorRefWireData>,
    registry: Arc<Registry>,
    outbound: Arc<dyn Recipient<ReplicatorRemoteEnvelope> + Send + Sync>,
}

impl ReplicatorRemoteReplyOutbound {
    /// Creates a reply adapter from a concrete envelope recipient.
    pub fn new(
        target: ReplicatorRemoteTarget,
        sender: Option<ActorRefWireData>,
        registry: Arc<Registry>,
        outbound: impl Recipient<ReplicatorRemoteEnvelope> + Send + Sync + 'static,
    ) -> Self {
        Self {
            target,
            sender,
            registry,
            outbound: Arc::new(outbound),
        }
    }

    /// Creates a reply adapter from a shared type-erased envelope recipient.
    pub fn from_arc(
        target: ReplicatorRemoteTarget,
        sender: Option<ActorRefWireData>,
        registry: Arc<Registry>,
        outbound: Arc<dyn Recipient<ReplicatorRemoteEnvelope> + Send + Sync>,
    ) -> Self {
        Self {
            target,
            sender,
            registry,
            outbound,
        }
    }

    /// Returns the destination replica and aggregation actor reference.
    pub fn target(&self) -> &ReplicatorRemoteTarget {
        &self.target
    }

    /// Returns the sender actor reference attached to replies, if configured.
    pub fn sender(&self) -> Option<&ActorRefWireData> {
        self.sender.as_ref()
    }

    /// Sends the report's ACK or NACK when one was requested.
    ///
    /// Returns `true` when a reply was present and delivered, or `false` for a one-way delta.
    pub fn send_delta_report(
        &self,
        report: &DeltaPropagationReceiveReport,
    ) -> Result<bool, ReplicatorRemoteEnvelopeError> {
        match report.reply() {
            Some(DeltaReceiveReply::Ack(message)) => {
                self.send_remote_message(&message)?;
                Ok(true)
            }
            Some(DeltaReceiveReply::Nack(message)) => {
                self.send_remote_message(&message)?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Sends a direct write ACK or NACK to the originating aggregation actor.
    pub fn send_write_result(
        &self,
        result: &DirectWriteResult,
    ) -> Result<(), ReplicatorRemoteEnvelopeError> {
        match result {
            DirectWriteResult::Ack { message, .. } => self.send_remote_message(message),
            DirectWriteResult::Nack { message, .. } => self.send_remote_message(message),
        }
    }

    /// Sends a direct read result, including successful absence, to the originating aggregator.
    pub fn send_read_result(
        &self,
        result: &DirectReadResult,
    ) -> Result<(), ReplicatorRemoteEnvelopeError> {
        self.send_remote_message(result.message())
    }

    fn send_remote_message<M>(&self, message: &M) -> Result<(), ReplicatorRemoteEnvelopeError>
    where
        M: RemoteMessage,
    {
        let serialized = self.registry.serialize(message)?;
        let envelope = RemoteEnvelope::new(
            self.target.recipient().clone(),
            self.sender.clone(),
            serialized,
        );
        self.outbound
            .tell(ReplicatorRemoteEnvelope::new(
                self.target.replica().clone(),
                envelope,
            ))
            .map_err(|error| ReplicatorRemoteEnvelopeError::Send(error.reason().to_string()))
    }
}

impl Recipient<DeltaPropagationReceiveReport> for ReplicatorRemoteReplyOutbound {
    fn tell(
        &self,
        message: DeltaPropagationReceiveReport,
    ) -> Result<(), SendError<DeltaPropagationReceiveReport>> {
        self.send_delta_report(&message)
            .map(|_| ())
            .map_err(|error| SendError::new(message, error.to_string()))
    }
}

impl Recipient<DirectWriteResult> for ReplicatorRemoteReplyOutbound {
    fn tell(&self, message: DirectWriteResult) -> Result<(), SendError<DirectWriteResult>> {
        self.send_write_result(&message)
            .map_err(|error| SendError::new(message, error.to_string()))
    }
}

impl Recipient<Result<DirectReadResult, String>> for ReplicatorRemoteReplyOutbound {
    fn tell(
        &self,
        message: Result<DirectReadResult, String>,
    ) -> Result<(), SendError<Result<DirectReadResult, String>>> {
        match message {
            Ok(result) => self
                .send_read_result(&result)
                .map_err(|error| SendError::new(Ok(result), error.to_string())),
            Err(reason) => Err(SendError::new(Err(reason.clone()), reason)),
        }
    }
}

#[derive(Clone)]
/// Decodes remote replicator replies and delivers them to addressed local aggregation actors.
///
/// With canonical remote settings, owned remote actor references are reduced to local paths
/// before resolution. Unknown manifests are rejected rather than delivered through a fallback.
pub struct ReplicatorRemoteReplyInbound {
    system: ActorSystem,
    canonical_address: Option<CanonicalLocalAddress>,
    registry: Arc<Registry>,
}

impl ReplicatorRemoteReplyInbound {
    /// Creates an inbound reply adapter for local-path actor references.
    pub fn new(system: ActorSystem, registry: Arc<Registry>) -> Self {
        Self {
            system,
            canonical_address: None,
            registry,
        }
    }

    /// Creates an inbound adapter that recognizes the ActorSystem's canonical remote address.
    pub fn with_remote_settings(
        system: ActorSystem,
        settings: RemoteSettings,
        registry: Arc<Registry>,
    ) -> Self {
        Self {
            canonical_address: Some(CanonicalLocalAddress::from_system_settings(
                &system, settings,
            )),
            system,
            registry,
        }
    }

    /// Returns the ActorSystem used to resolve aggregation actors.
    pub fn system(&self) -> &ActorSystem {
        &self.system
    }

    /// Dispatches a complete envelope using the supplied source replica identity.
    pub fn receive_from(
        &self,
        from: ReplicaId,
        envelope: RemoteEnvelope,
    ) -> Result<(), ReplicatorRemoteReplyError> {
        self.receive_message(from, envelope.recipient, envelope.message)
    }

    /// Decodes and delivers a serialized reply to its exact read or write aggregator.
    pub fn receive_message(
        &self,
        from: ReplicaId,
        recipient: ActorRefWireData,
        message: SerializedMessage,
    ) -> Result<(), ReplicatorRemoteReplyError> {
        match message.manifest.as_str() {
            ReplicatorDeltaAck::MANIFEST => {
                let message = self.registry.deserialize::<ReplicatorDeltaAck>(message)?;
                self.deliver_write(recipient, ReplicatorWireReply::DeltaAck { from, message })
            }
            ReplicatorDeltaNack::MANIFEST => {
                let message = self.registry.deserialize::<ReplicatorDeltaNack>(message)?;
                self.deliver_write(recipient, ReplicatorWireReply::DeltaNack { from, message })
            }
            ReplicatorWriteAck::MANIFEST => {
                let message = self.registry.deserialize::<ReplicatorWriteAck>(message)?;
                self.deliver_write(recipient, ReplicatorWireReply::WriteAck { from, message })
            }
            ReplicatorWriteNack::MANIFEST => {
                let message = self.registry.deserialize::<ReplicatorWriteNack>(message)?;
                self.deliver_write(recipient, ReplicatorWireReply::WriteNack { from, message })
            }
            ReplicatorReadResult::MANIFEST => {
                let message = self.registry.deserialize::<ReplicatorReadResult>(message)?;
                self.deliver_read(recipient, ReplicatorWireReply::ReadResult { from, message })
            }
            manifest => Err(ReplicatorRemoteReplyError::UnsupportedManifest(
                manifest.to_string(),
            )),
        }
    }

    fn deliver_write(
        &self,
        recipient: ActorRefWireData,
        reply: ReplicatorWireReply,
    ) -> Result<(), ReplicatorRemoteReplyError> {
        let recipient_path = self.local_recipient_path(&recipient);
        let actor = self
            .system
            .resolve_local_or_missing::<WriteAggregationActorMsg>(&recipient_path);
        actor
            .tell(WriteAggregationActorMsg::Reply(reply))
            .map_err(|error| ReplicatorRemoteReplyError::Send {
                recipient: recipient_path,
                reason: error.reason().to_string(),
            })
    }

    fn deliver_read(
        &self,
        recipient: ActorRefWireData,
        reply: ReplicatorWireReply,
    ) -> Result<(), ReplicatorRemoteReplyError> {
        let recipient_path = self.local_recipient_path(&recipient);
        let actor = self
            .system
            .resolve_local_or_missing::<ReadAggregationActorMsg>(&recipient_path);
        actor
            .tell(ReadAggregationActorMsg::Reply(reply))
            .map_err(|error| ReplicatorRemoteReplyError::Send {
                recipient: recipient_path,
                reason: error.reason().to_string(),
            })
    }

    fn local_recipient_path(&self, recipient: &ActorRefWireData) -> String {
        self.canonical_address
            .as_ref()
            .and_then(|canonical| canonical.local_recipient_path(recipient))
            .unwrap_or_else(|| recipient.path().to_string())
    }
}

#[cfg(test)]
mod tests;
