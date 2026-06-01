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
pub struct ReplicatorSerializedReply {
    pub from: ReplicaId,
    pub target: ReplicaId,
    pub message: SerializedMessage,
}

impl ReplicatorSerializedReply {
    pub fn new(from: ReplicaId, target: ReplicaId, message: SerializedMessage) -> Self {
        Self {
            from,
            target,
            message,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicatorWireReply {
    DeltaAck {
        from: ReplicaId,
        message: ReplicatorDeltaAck,
    },
    DeltaNack {
        from: ReplicaId,
        message: ReplicatorDeltaNack,
    },
    WriteAck {
        from: ReplicaId,
        message: ReplicatorWriteAck,
    },
    WriteNack {
        from: ReplicaId,
        message: ReplicatorWriteNack,
    },
    ReadResult {
        from: ReplicaId,
        message: ReplicatorReadResult,
    },
}

impl ReplicatorWireReply {
    pub fn from(&self) -> &ReplicaId {
        match self {
            Self::DeltaAck { from, .. }
            | Self::DeltaNack { from, .. }
            | Self::WriteAck { from, .. }
            | Self::WriteNack { from, .. }
            | Self::ReadResult { from, .. } => from,
        }
    }

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
pub enum ReplicatorReplyWireError {
    Serialization(SerializationError),
    Send(String),
    MissingReplyTarget(&'static str),
    UnsupportedManifest(String),
    WrongTarget {
        expected: ReplicaId,
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
pub struct ReplicatorReplyWireOutbound {
    self_replica: ReplicaId,
    registry: Arc<Registry>,
    outbound: Arc<dyn Recipient<ReplicatorSerializedReply> + Send + Sync>,
}

impl ReplicatorReplyWireOutbound {
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

    pub fn self_replica(&self) -> &ReplicaId {
        &self.self_replica
    }

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
pub struct ReplicatorReplyWireInbound {
    self_replica: ReplicaId,
    registry: Arc<Registry>,
}

impl ReplicatorReplyWireInbound {
    pub fn new(self_replica: ReplicaId, registry: Arc<Registry>) -> Self {
        Self {
            self_replica,
            registry,
        }
    }

    pub fn self_replica(&self) -> &ReplicaId {
        &self.self_replica
    }

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
