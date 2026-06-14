use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use kairo_actor::{ActorSystem, Recipient, SendError};
use kairo_remote::RemoteSettings;
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
pub enum ReplicatorRemoteReplyError {
    Serialization(SerializationError),
    Send { recipient: String, reason: String },
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
pub struct ReplicatorRemoteReplyOutbound {
    target: ReplicatorRemoteTarget,
    sender: Option<ActorRefWireData>,
    registry: Arc<Registry>,
    outbound: Arc<dyn Recipient<ReplicatorRemoteEnvelope> + Send + Sync>,
}

impl ReplicatorRemoteReplyOutbound {
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

    pub fn target(&self) -> &ReplicatorRemoteTarget {
        &self.target
    }

    pub fn sender(&self) -> Option<&ActorRefWireData> {
        self.sender.as_ref()
    }

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

    pub fn send_write_result(
        &self,
        result: &DirectWriteResult,
    ) -> Result<(), ReplicatorRemoteEnvelopeError> {
        match result {
            DirectWriteResult::Ack { message, .. } => self.send_remote_message(message),
            DirectWriteResult::Nack { message, .. } => self.send_remote_message(message),
        }
    }

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
pub struct ReplicatorRemoteReplyInbound {
    system: ActorSystem,
    canonical_address: Option<ReplicatorCanonicalAddress>,
    registry: Arc<Registry>,
}

impl ReplicatorRemoteReplyInbound {
    pub fn new(system: ActorSystem, registry: Arc<Registry>) -> Self {
        Self {
            system,
            canonical_address: None,
            registry,
        }
    }

    pub fn with_remote_settings(
        system: ActorSystem,
        settings: RemoteSettings,
        registry: Arc<Registry>,
    ) -> Self {
        Self {
            canonical_address: Some(ReplicatorCanonicalAddress::new(&system, settings)),
            system,
            registry,
        }
    }

    pub fn system(&self) -> &ActorSystem {
        &self.system
    }

    pub fn receive_from(
        &self,
        from: ReplicaId,
        envelope: RemoteEnvelope,
    ) -> Result<(), ReplicatorRemoteReplyError> {
        self.receive_message(from, envelope.recipient, envelope.message)
    }

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

#[derive(Clone)]
struct ReplicatorCanonicalAddress {
    protocol: String,
    system: String,
    host: String,
    port: u16,
}

impl ReplicatorCanonicalAddress {
    fn new(system: &ActorSystem, settings: RemoteSettings) -> Self {
        Self {
            protocol: system.address().protocol().to_string(),
            system: system.name().to_string(),
            host: settings.canonical_hostname,
            port: settings.canonical_port,
        }
    }

    fn local_recipient_path(&self, recipient: &ActorRefWireData) -> Option<String> {
        (recipient.protocol() == self.protocol
            && recipient.system() == self.system
            && recipient.host() == Some(self.host.as_str())
            && recipient.port() == Some(self.port))
        .then(|| {
            recipient.path().replacen(
                &format!(
                    "{}://{}@{}:{}",
                    self.protocol, self.system, self.host, self.port
                ),
                &format!("{}://{}", self.protocol, self.system),
                1,
            )
        })
    }
}

#[cfg(test)]
mod tests;
