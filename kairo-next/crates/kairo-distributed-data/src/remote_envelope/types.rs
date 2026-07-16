use kairo_serialization::{ActorRefWireData, RemoteEnvelope, SerializedMessage};

use crate::ReplicaId;

#[derive(Debug, Clone, PartialEq, Eq)]
/// Destination replica identity paired with its canonical replicator actor reference.
pub struct ReplicatorRemoteTarget {
    replica: ReplicaId,
    recipient: ActorRefWireData,
}

impl ReplicatorRemoteTarget {
    /// Creates a target for one exact replica and remote actor reference.
    pub fn new(replica: ReplicaId, recipient: ActorRefWireData) -> Self {
        Self { replica, recipient }
    }

    /// Returns the logical distributed-data replica identity.
    pub fn replica(&self) -> &ReplicaId {
        &self.replica
    }

    /// Returns the canonical remote actor reference receiving the message.
    pub fn recipient(&self) -> &ActorRefWireData {
        &self.recipient
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Transport-neutral envelope carrying a serialized remote message to one replica.
pub struct ReplicatorRemoteEnvelope {
    /// Logical destination replica used for route selection and diagnostics.
    pub target: ReplicaId,
    /// Stable serialization envelope addressed to the remote replicator or reply actor.
    pub envelope: RemoteEnvelope,
}

impl ReplicatorRemoteEnvelope {
    /// Creates a replica-targeted remote envelope.
    pub fn new(target: ReplicaId, envelope: RemoteEnvelope) -> Self {
        Self { target, envelope }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Recipient-validated inbound message with preserved reply metadata.
pub struct ReplicatorRemoteInboundMessage {
    /// Canonical sender actor reference, when the protocol expects or permits a reply.
    pub sender: Option<ActorRefWireData>,
    /// Stable serialized replicator message awaiting manifest dispatch.
    pub message: SerializedMessage,
}

impl ReplicatorRemoteInboundMessage {
    /// Creates an inbound message from preserved sender metadata and serialized payload.
    pub fn new(sender: Option<ActorRefWireData>, message: SerializedMessage) -> Self {
        Self { sender, message }
    }
}
