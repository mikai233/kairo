#![deny(missing_docs)]

use std::sync::Arc;

use kairo_remote::RemoteOutbound;
use kairo_serialization::{ActorRefWireData, Registry, RemoteEnvelope, RemoteMessage};

use crate::{Heartbeat, HeartbeatRsp, UniqueAddress};

use super::ClusterHeartbeatRemoteError;
use super::paths::{DEFAULT_CLUSTER_HEARTBEAT_RECEIVER_PATH, validate_recipient};

#[derive(Clone)]
/// Validates remote heartbeat requests and replies to their advertised sender.
pub struct HeartbeatRemoteReceiverInbound {
    self_node: UniqueAddress,
    registry: Arc<Registry>,
    sender: Option<ActorRefWireData>,
    recipient_path: String,
    outbound: Arc<dyn RemoteOutbound>,
}

impl HeartbeatRemoteReceiverInbound {
    /// Creates a receiver endpoint backed by an owned remoting outbound.
    pub fn new(
        self_node: UniqueAddress,
        registry: Arc<Registry>,
        outbound: impl RemoteOutbound + 'static,
    ) -> Self {
        Self::from_arc(self_node, registry, Arc::new(outbound))
    }

    /// Creates a receiver endpoint backed by a shared remoting outbound.
    pub fn from_arc(
        self_node: UniqueAddress,
        registry: Arc<Registry>,
        outbound: Arc<dyn RemoteOutbound>,
    ) -> Self {
        Self {
            self_node,
            registry,
            sender: None,
            recipient_path: DEFAULT_CLUSTER_HEARTBEAT_RECEIVER_PATH.to_string(),
            outbound,
        }
    }

    /// Sets the optional actor identity advertised as the response sender.
    pub fn with_sender(mut self, sender: Option<ActorRefWireData>) -> Self {
        self.sender = sender;
        self
    }

    /// Overrides the absolute local heartbeat receiver path.
    pub fn with_recipient_path(mut self, path: impl Into<String>) -> Self {
        self.recipient_path = path.into();
        self
    }

    /// Validates and decodes a probe, then echoes its sequence and creation time
    /// to the envelope's sender route.
    pub fn receive(&self, envelope: RemoteEnvelope) -> Result<(), ClusterHeartbeatRemoteError> {
        validate_recipient(&self.self_node, &self.recipient_path, &envelope.recipient)?;
        if envelope.message.manifest.as_str() != Heartbeat::MANIFEST {
            return Err(ClusterHeartbeatRemoteError::UnsupportedManifest(
                envelope.message.manifest.as_str().to_string(),
            ));
        }
        let response_recipient = envelope
            .sender
            .clone()
            .ok_or(ClusterHeartbeatRemoteError::MissingSender)?;
        let heartbeat = self.registry.deserialize::<Heartbeat>(envelope.message)?;
        let response = HeartbeatRsp {
            from: self.self_node.clone(),
            sequence_nr: heartbeat.sequence_nr,
            creation_time_nanos: heartbeat.creation_time_nanos,
        };
        let target = response_recipient.path().to_string();
        let envelope = RemoteEnvelope::new(
            response_recipient,
            self.sender.clone(),
            self.registry.serialize(&response)?,
        );
        self.outbound
            .send(envelope)
            .map_err(|error| ClusterHeartbeatRemoteError::Send {
                target,
                reason: error.to_string(),
            })
    }
}
