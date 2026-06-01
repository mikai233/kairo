use kairo_serialization::{ActorRefWireData, RemoteEnvelope, SerializedMessage};

use crate::ReplicaId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorRemoteTarget {
    replica: ReplicaId,
    recipient: ActorRefWireData,
}

impl ReplicatorRemoteTarget {
    pub fn new(replica: ReplicaId, recipient: ActorRefWireData) -> Self {
        Self { replica, recipient }
    }

    pub fn replica(&self) -> &ReplicaId {
        &self.replica
    }

    pub fn recipient(&self) -> &ActorRefWireData {
        &self.recipient
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorRemoteEnvelope {
    pub target: ReplicaId,
    pub envelope: RemoteEnvelope,
}

impl ReplicatorRemoteEnvelope {
    pub fn new(target: ReplicaId, envelope: RemoteEnvelope) -> Self {
        Self { target, envelope }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorRemoteInboundMessage {
    pub sender: Option<ActorRefWireData>,
    pub message: SerializedMessage,
}

impl ReplicatorRemoteInboundMessage {
    pub fn new(sender: Option<ActorRefWireData>, message: SerializedMessage) -> Self {
        Self { sender, message }
    }
}
