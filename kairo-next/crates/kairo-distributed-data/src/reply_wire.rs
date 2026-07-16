#![deny(missing_docs)]

//! Transport-neutral wire adapters for direct replicator replies.
//!
//! Replies retain their logical source and destination independently from the
//! stable serialized payload. Decoding produces a typed, source-attributed
//! outcome for the aggregation layer rather than exposing wire metadata as the
//! primary user API.

use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use kairo_actor::{Recipient, SendError};
use kairo_serialization::{Registry, RemoteMessage, SerializationError, SerializedMessage};

use crate::{
    DeltaPropagationReceiveReport, DeltaReceiveReply, DirectReadResult, DirectWriteResult,
    ReplicaId, ReplicatorDeltaAck, ReplicatorDeltaNack, ReplicatorReadResult, ReplicatorWriteAck,
    ReplicatorWriteNack,
};

#[derive(Debug, Clone, PartialEq, Eq)]
/// A serialized replicator reply with logical source and destination replicas.
pub struct ReplicatorSerializedReply {
    /// The replica that produced the reply.
    pub from: ReplicaId,
    /// The replica that must receive the reply.
    pub target: ReplicaId,
    /// The stable serializer metadata and payload.
    pub message: SerializedMessage,
}

impl ReplicatorSerializedReply {
    /// Creates a serialized reply envelope.
    pub fn new(from: ReplicaId, target: ReplicaId, message: SerializedMessage) -> Self {
        Self {
            from,
            target,
            message,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// A decoded direct-replicator reply attributed to its source replica.
pub enum ReplicatorWireReply {
    /// A delta propagation was accepted.
    DeltaAck {
        /// The replica that accepted the propagation.
        from: ReplicaId,
        /// The decoded acknowledgement.
        message: ReplicatorDeltaAck,
    },
    /// A delta propagation was rejected and may require full-state retry.
    DeltaNack {
        /// The replica that rejected the propagation.
        from: ReplicaId,
        /// The decoded negative acknowledgement.
        message: ReplicatorDeltaNack,
    },
    /// A direct write was accepted.
    WriteAck {
        /// The replica that accepted the write.
        from: ReplicaId,
        /// The decoded acknowledgement.
        message: ReplicatorWriteAck,
    },
    /// A direct write was rejected.
    WriteNack {
        /// The replica that rejected the write.
        from: ReplicaId,
        /// The decoded negative acknowledgement.
        message: ReplicatorWriteNack,
    },
    /// A direct read completed, including successful absence when applicable.
    ReadResult {
        /// The replica that served the read.
        from: ReplicaId,
        /// The decoded read result.
        message: ReplicatorReadResult,
    },
}

impl ReplicatorWireReply {
    /// Returns the logical replica that produced this reply.
    pub fn from(&self) -> &ReplicaId {
        match self {
            Self::DeltaAck { from, .. }
            | Self::DeltaNack { from, .. }
            | Self::WriteAck { from, .. }
            | Self::WriteNack { from, .. }
            | Self::ReadResult { from, .. } => from,
        }
    }

    /// Returns the stable manifest for this reply variant.
    pub fn manifest(&self) -> &'static str {
        match self {
            Self::DeltaAck { .. } => ReplicatorDeltaAck::MANIFEST,
            Self::DeltaNack { .. } => ReplicatorDeltaNack::MANIFEST,
            Self::WriteAck { .. } => ReplicatorWriteAck::MANIFEST,
            Self::WriteNack { .. } => ReplicatorWriteNack::MANIFEST,
            Self::ReadResult { .. } => ReplicatorReadResult::MANIFEST,
        }
    }
}

#[derive(Debug)]
/// A failure while encoding, routing, or decoding a replicator reply.
pub enum ReplicatorReplyWireError {
    /// Stable-message serialization or deserialization failed.
    Serialization(SerializationError),
    /// The transport rejected delivery.
    Send(String),
    /// A reply-producing result did not retain its originating replica.
    MissingReplyTarget(&'static str),
    /// The inbound reply manifest is not supported by this adapter.
    UnsupportedManifest(String),
    /// The envelope was addressed to a different logical replica.
    WrongTarget {
        /// The local replica expected by the inbound adapter.
        expected: ReplicaId,
        /// The target carried by the received envelope.
        actual: ReplicaId,
    },
}

impl Display for ReplicatorReplyWireError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Serialization(error) => {
                write!(f, "replicator reply serialization failed: {error}")
            }
            Self::Send(reason) => write!(f, "replicator reply delivery failed: {reason}"),
            Self::MissingReplyTarget(message) => {
                write!(f, "replicator reply `{message}` has no target replica")
            }
            Self::UnsupportedManifest(manifest) => {
                write!(f, "unsupported replicator reply manifest `{manifest}`")
            }
            Self::WrongTarget { expected, actual } => {
                write!(
                    f,
                    "replicator reply was addressed to {}, expected {}",
                    actual.as_str(),
                    expected.as_str()
                )
            }
        }
    }
}

impl std::error::Error for ReplicatorReplyWireError {}

impl From<SerializationError> for ReplicatorReplyWireError {
    fn from(error: SerializationError) -> Self {
        Self::Serialization(error)
    }
}

#[derive(Clone)]
/// Serializes actor-owned replicator outcomes back to their originating replica.
pub struct ReplicatorReplyWireOutbound {
    self_replica: ReplicaId,
    registry: Arc<Registry>,
    outbound: Arc<dyn Recipient<ReplicatorSerializedReply> + Send + Sync>,
}

impl ReplicatorReplyWireOutbound {
    /// Creates an outbound reply adapter from an owned transport recipient.
    pub fn new(
        self_replica: ReplicaId,
        registry: Arc<Registry>,
        outbound: impl Recipient<ReplicatorSerializedReply> + Send + Sync + 'static,
    ) -> Self {
        Self {
            self_replica,
            registry,
            outbound: Arc::new(outbound),
        }
    }

    /// Creates an outbound reply adapter from a shared transport recipient.
    pub fn from_arc(
        self_replica: ReplicaId,
        registry: Arc<Registry>,
        outbound: Arc<dyn Recipient<ReplicatorSerializedReply> + Send + Sync>,
    ) -> Self {
        Self {
            self_replica,
            registry,
            outbound,
        }
    }

    /// Returns the logical replica recorded as the source of every reply.
    pub fn self_replica(&self) -> &ReplicaId {
        &self.self_replica
    }

    /// Sends an ACK or NACK when the propagation requested a reply.
    ///
    /// Returns `false` without sending when the receive report represents a
    /// one-way propagation.
    pub fn send_delta_report(
        &self,
        report: &DeltaPropagationReceiveReport,
    ) -> Result<bool, ReplicatorReplyWireError> {
        match report.reply() {
            Some(DeltaReceiveReply::Ack(message)) => {
                self.send_remote_message(report.from(), &message)?;
                Ok(true)
            }
            Some(DeltaReceiveReply::Nack(message)) => {
                self.send_remote_message(report.from(), &message)?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Sends a direct-write outcome to its retained source replica.
    pub fn send_write_result(
        &self,
        result: &DirectWriteResult,
    ) -> Result<(), ReplicatorReplyWireError> {
        let target = result.from().ok_or({
            ReplicatorReplyWireError::MissingReplyTarget(match result {
                DirectWriteResult::Ack { .. } => ReplicatorWriteAck::MANIFEST,
                DirectWriteResult::Nack { .. } => ReplicatorWriteNack::MANIFEST,
            })
        })?;

        match result {
            DirectWriteResult::Ack { message, .. } => self.send_remote_message(target, message),
            DirectWriteResult::Nack { message, .. } => self.send_remote_message(target, message),
        }
    }

    /// Sends a direct-read result to its retained source replica.
    pub fn send_read_result(
        &self,
        result: &DirectReadResult,
    ) -> Result<(), ReplicatorReplyWireError> {
        let target = result
            .from()
            .ok_or(ReplicatorReplyWireError::MissingReplyTarget(
                ReplicatorReadResult::MANIFEST,
            ))?;
        self.send_remote_message(target, result.message())
    }

    fn send_remote_message<M>(
        &self,
        target: &ReplicaId,
        message: &M,
    ) -> Result<(), ReplicatorReplyWireError>
    where
        M: RemoteMessage,
    {
        let serialized = self.registry.serialize(message)?;
        self.outbound
            .tell(ReplicatorSerializedReply::new(
                self.self_replica.clone(),
                target.clone(),
                serialized,
            ))
            .map_err(|error| ReplicatorReplyWireError::Send(error.reason().to_string()))
    }
}

impl Recipient<DeltaPropagationReceiveReport> for ReplicatorReplyWireOutbound {
    fn tell(
        &self,
        message: DeltaPropagationReceiveReport,
    ) -> Result<(), SendError<DeltaPropagationReceiveReport>> {
        self.send_delta_report(&message)
            .map(|_| ())
            .map_err(|error| SendError::new(message, error.to_string()))
    }
}

impl Recipient<DirectWriteResult> for ReplicatorReplyWireOutbound {
    fn tell(&self, message: DirectWriteResult) -> Result<(), SendError<DirectWriteResult>> {
        self.send_write_result(&message)
            .map_err(|error| SendError::new(message, error.to_string()))
    }
}

impl Recipient<Result<DirectReadResult, String>> for ReplicatorReplyWireOutbound {
    fn tell(
        &self,
        message: Result<DirectReadResult, String>,
    ) -> Result<(), SendError<Result<DirectReadResult, String>>> {
        match message {
            Ok(result) => match self.send_read_result(&result) {
                Ok(()) => Ok(()),
                Err(error) => Err(SendError::new(Ok(result), error.to_string())),
            },
            Err(reason) => Err(SendError::new(Err(reason.clone()), reason)),
        }
    }
}

#[derive(Clone)]
/// Validates and decodes serialized replies for one local replica.
///
/// The adapter accepts only delta ACK/NACK, write ACK/NACK, and read-result
/// manifests and retains the envelope's source in the typed result.
pub struct ReplicatorReplyWireInbound {
    self_replica: ReplicaId,
    registry: Arc<Registry>,
}

impl ReplicatorReplyWireInbound {
    /// Creates an inbound reply adapter for `self_replica`.
    pub fn new(self_replica: ReplicaId, registry: Arc<Registry>) -> Self {
        Self {
            self_replica,
            registry,
        }
    }

    /// Returns the only logical destination accepted by this adapter.
    pub fn self_replica(&self) -> &ReplicaId {
        &self.self_replica
    }

    /// Validates the envelope target and decodes its reply.
    pub fn receive(
        &self,
        envelope: ReplicatorSerializedReply,
    ) -> Result<ReplicatorWireReply, ReplicatorReplyWireError> {
        if envelope.target != self.self_replica {
            return Err(ReplicatorReplyWireError::WrongTarget {
                expected: self.self_replica.clone(),
                actual: envelope.target,
            });
        }
        self.receive_message(envelope.from, envelope.message)
    }

    /// Decodes a reply whose source and destination were already established.
    ///
    /// Callers that have not independently validated the destination should
    /// use [`Self::receive`] instead.
    pub fn receive_message(
        &self,
        from: ReplicaId,
        message: SerializedMessage,
    ) -> Result<ReplicatorWireReply, ReplicatorReplyWireError> {
        match message.manifest.as_str() {
            ReplicatorDeltaAck::MANIFEST => {
                let message = self.registry.deserialize::<ReplicatorDeltaAck>(message)?;
                Ok(ReplicatorWireReply::DeltaAck { from, message })
            }
            ReplicatorDeltaNack::MANIFEST => {
                let message = self.registry.deserialize::<ReplicatorDeltaNack>(message)?;
                Ok(ReplicatorWireReply::DeltaNack { from, message })
            }
            ReplicatorWriteAck::MANIFEST => {
                let message = self.registry.deserialize::<ReplicatorWriteAck>(message)?;
                Ok(ReplicatorWireReply::WriteAck { from, message })
            }
            ReplicatorWriteNack::MANIFEST => {
                let message = self.registry.deserialize::<ReplicatorWriteNack>(message)?;
                Ok(ReplicatorWireReply::WriteNack { from, message })
            }
            ReplicatorReadResult::MANIFEST => {
                let message = self.registry.deserialize::<ReplicatorReadResult>(message)?;
                Ok(ReplicatorWireReply::ReadResult { from, message })
            }
            manifest => Err(ReplicatorReplyWireError::UnsupportedManifest(
                manifest.to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests;
