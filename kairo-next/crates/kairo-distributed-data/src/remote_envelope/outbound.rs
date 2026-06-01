use std::sync::Arc;

use kairo_actor::{Recipient, SendError};
use kairo_serialization::{ActorRefWireData, Registry, RemoteEnvelope, RemoteMessage};

use super::{ReplicatorRemoteEnvelope, ReplicatorRemoteEnvelopeError, ReplicatorRemoteTarget};
use crate::{
    ReplicatorDeltaPropagation, ReplicatorGossip, ReplicatorGossipStatus, ReplicatorRead,
    ReplicatorReadResult, ReplicatorWrite, ReplicatorWriteAck, ReplicatorWriteNack,
};

#[derive(Clone)]
pub struct ReplicatorRemoteEnvelopeOutbound {
    target: ReplicatorRemoteTarget,
    sender: Option<ActorRefWireData>,
    registry: Arc<Registry>,
    outbound: Arc<dyn Recipient<ReplicatorRemoteEnvelope> + Send + Sync>,
}

impl ReplicatorRemoteEnvelopeOutbound {
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

    pub fn with_sender(mut self, sender: Option<ActorRefWireData>) -> Self {
        self.sender = sender;
        self
    }

    pub fn send<M>(&self, message: &M) -> Result<(), ReplicatorRemoteEnvelopeError>
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

impl Recipient<ReplicatorDeltaPropagation> for ReplicatorRemoteEnvelopeOutbound {
    fn tell(
        &self,
        message: ReplicatorDeltaPropagation,
    ) -> Result<(), SendError<ReplicatorDeltaPropagation>> {
        self.send(&message)
            .map_err(|error| SendError::new(message, error.to_string()))
    }
}

impl Recipient<ReplicatorWrite> for ReplicatorRemoteEnvelopeOutbound {
    fn tell(&self, message: ReplicatorWrite) -> Result<(), SendError<ReplicatorWrite>> {
        self.send(&message)
            .map_err(|error| SendError::new(message, error.to_string()))
    }
}

impl Recipient<ReplicatorRead> for ReplicatorRemoteEnvelopeOutbound {
    fn tell(&self, message: ReplicatorRead) -> Result<(), SendError<ReplicatorRead>> {
        self.send(&message)
            .map_err(|error| SendError::new(message, error.to_string()))
    }
}

impl Recipient<ReplicatorGossipStatus> for ReplicatorRemoteEnvelopeOutbound {
    fn tell(
        &self,
        message: ReplicatorGossipStatus,
    ) -> Result<(), SendError<ReplicatorGossipStatus>> {
        self.send(&message)
            .map_err(|error| SendError::new(message, error.to_string()))
    }
}

impl Recipient<ReplicatorGossip> for ReplicatorRemoteEnvelopeOutbound {
    fn tell(&self, message: ReplicatorGossip) -> Result<(), SendError<ReplicatorGossip>> {
        self.send(&message)
            .map_err(|error| SendError::new(message, error.to_string()))
    }
}

impl Recipient<ReplicatorWriteAck> for ReplicatorRemoteEnvelopeOutbound {
    fn tell(&self, message: ReplicatorWriteAck) -> Result<(), SendError<ReplicatorWriteAck>> {
        self.send(&message)
            .map_err(|error| SendError::new(message, error.to_string()))
    }
}

impl Recipient<ReplicatorWriteNack> for ReplicatorRemoteEnvelopeOutbound {
    fn tell(&self, message: ReplicatorWriteNack) -> Result<(), SendError<ReplicatorWriteNack>> {
        self.send(&message)
            .map_err(|error| SendError::new(message, error.to_string()))
    }
}

impl Recipient<ReplicatorReadResult> for ReplicatorRemoteEnvelopeOutbound {
    fn tell(&self, message: ReplicatorReadResult) -> Result<(), SendError<ReplicatorReadResult>> {
        self.send(&message)
            .map_err(|error| SendError::new(message, error.to_string()))
    }
}
