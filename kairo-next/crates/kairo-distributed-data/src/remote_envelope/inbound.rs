use kairo_serialization::{ActorRefWireData, RemoteEnvelope};

use super::{ReplicatorRemoteEnvelopeError, ReplicatorRemoteInboundMessage};

#[derive(Clone)]
/// Validates one distributed-data recipient and exposes its serialized inbound payload.
///
/// Sender metadata is preserved so request handlers can return direct read, write, and optional
/// delta acknowledgements to the originating aggregation actor.
pub struct ReplicatorRemoteEnvelopeInbound {
    recipient: ActorRefWireData,
}

impl ReplicatorRemoteEnvelopeInbound {
    /// Creates an inbound adapter for one exact remote actor reference.
    pub fn new(recipient: ActorRefWireData) -> Self {
        Self { recipient }
    }

    /// Returns the exact actor reference accepted by this adapter.
    pub fn recipient(&self) -> &ActorRefWireData {
        &self.recipient
    }

    /// Validates the recipient and returns the preserved sender plus serialized message.
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
