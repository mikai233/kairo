use kairo_serialization::{ActorRefWireData, RemoteEnvelope};

use super::{ReplicatorRemoteEnvelopeError, ReplicatorRemoteInboundMessage};

#[derive(Clone)]
pub struct ReplicatorRemoteEnvelopeInbound {
    recipient: ActorRefWireData,
}

impl ReplicatorRemoteEnvelopeInbound {
    pub fn new(recipient: ActorRefWireData) -> Self {
        Self { recipient }
    }

    pub fn recipient(&self) -> &ActorRefWireData {
        &self.recipient
    }

    pub fn receive(
        &self,
        envelope: RemoteEnvelope,
    ) -> Result<ReplicatorRemoteInboundMessage, ReplicatorRemoteEnvelopeError> {
        if envelope.recipient != self.recipient {
            return Err(ReplicatorRemoteEnvelopeError::WrongRecipient {
                expected: self.recipient.path().to_string(),
                actual: envelope.recipient.path().to_string(),
            });
        }
        Ok(ReplicatorRemoteInboundMessage::new(
            envelope.sender,
            envelope.message,
        ))
    }
}
