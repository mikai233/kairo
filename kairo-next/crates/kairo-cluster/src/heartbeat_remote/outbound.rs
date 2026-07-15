#![deny(missing_docs)]

use std::sync::Arc;

use kairo_actor::{Actor, ActorError, ActorResult, Context};
use kairo_remote::RemoteOutbound;
use kairo_serialization::{ActorRefWireData, Registry, RemoteEnvelope};

use crate::{Heartbeat, HeartbeatReceiverMsg, UniqueAddress};

use super::ClusterHeartbeatRemoteError;
use super::paths::{DEFAULT_CLUSTER_HEARTBEAT_RECEIVER_PATH, recipient_for_node};

#[derive(Clone)]
/// Actor route that sends heartbeat probes to one remote node incarnation.
///
/// Transient remote send failures are consumed by the actor so failure
/// detection, rather than route termination, observes a missed response.
pub struct HeartbeatRemoteReceiverOutbound {
    target: UniqueAddress,
    registry: Arc<Registry>,
    sender: ActorRefWireData,
    recipient_path: String,
    outbound: Arc<dyn RemoteOutbound>,
}

impl HeartbeatRemoteReceiverOutbound {
    /// Creates a route backed by an owned remoting outbound.
    pub fn new(
        target: UniqueAddress,
        registry: Arc<Registry>,
        sender: ActorRefWireData,
        outbound: impl RemoteOutbound + 'static,
    ) -> Self {
        Self::from_arc(target, registry, sender, Arc::new(outbound))
    }

    /// Creates a route backed by a shared remoting outbound.
    pub fn from_arc(
        target: UniqueAddress,
        registry: Arc<Registry>,
        sender: ActorRefWireData,
        outbound: Arc<dyn RemoteOutbound>,
    ) -> Self {
        Self {
            target,
            registry,
            sender,
            recipient_path: DEFAULT_CLUSTER_HEARTBEAT_RECEIVER_PATH.to_string(),
            outbound,
        }
    }

    /// Overrides the absolute remote heartbeat receiver path.
    pub fn with_recipient_path(mut self, path: impl Into<String>) -> Self {
        self.recipient_path = path.into();
        self
    }

    /// Returns the remote node incarnation targeted by this route.
    pub fn target(&self) -> &UniqueAddress {
        &self.target
    }

    /// Returns the actor identity advertised for heartbeat responses.
    pub fn sender(&self) -> &ActorRefWireData {
        &self.sender
    }

    /// Resolves and validates the remote recipient actor reference.
    pub fn recipient_for_target(&self) -> Result<ActorRefWireData, ClusterHeartbeatRemoteError> {
        recipient_for_node(&self.target, &self.recipient_path)
    }

    /// Serializes and sends one heartbeat probe.
    pub fn send_heartbeat(&self, heartbeat: Heartbeat) -> Result<(), ClusterHeartbeatRemoteError> {
        let recipient = self.recipient_for_target()?;
        let target = self.target.ordering_key();
        let message = self.registry.serialize(&heartbeat)?;
        let envelope = RemoteEnvelope::new(recipient, Some(self.sender.clone()), message);
        self.outbound
            .send(envelope)
            .map_err(|error| ClusterHeartbeatRemoteError::Send {
                target,
                reason: error.to_string(),
            })
    }
}

impl Actor for HeartbeatRemoteReceiverOutbound {
    type Msg = HeartbeatReceiverMsg;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            HeartbeatReceiverMsg::Heartbeat { heartbeat, .. } => {
                match self.send_heartbeat(heartbeat) {
                    Ok(()) | Err(ClusterHeartbeatRemoteError::Send { .. }) => {}
                    Err(error) => return Err(ActorError::Message(error.to_string())),
                }
            }
        }
        Ok(())
    }
}
