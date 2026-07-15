use std::sync::Arc;

use kairo_actor::{Actor, ActorError, ActorResult, Context};
use kairo_remote::RemoteOutbound;
use kairo_serialization::{ActorRefWireData, Registry, RemoteEnvelope};

use crate::{Heartbeat, HeartbeatReceiverMsg, UniqueAddress};

use super::ClusterHeartbeatRemoteError;
use super::paths::{DEFAULT_CLUSTER_HEARTBEAT_RECEIVER_PATH, recipient_for_node};

#[derive(Clone)]
pub struct HeartbeatRemoteReceiverOutbound {
    target: UniqueAddress,
    registry: Arc<Registry>,
    sender: ActorRefWireData,
    recipient_path: String,
    outbound: Arc<dyn RemoteOutbound>,
}

impl HeartbeatRemoteReceiverOutbound {
    pub fn new(
        target: UniqueAddress,
        registry: Arc<Registry>,
        sender: ActorRefWireData,
        outbound: impl RemoteOutbound + 'static,
    ) -> Self {
        Self::from_arc(target, registry, sender, Arc::new(outbound))
    }

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

    pub fn with_recipient_path(mut self, path: impl Into<String>) -> Self {
        self.recipient_path = path.into();
        self
    }

    pub fn target(&self) -> &UniqueAddress {
        &self.target
    }

    pub fn sender(&self) -> &ActorRefWireData {
        &self.sender
    }

    pub fn recipient_for_target(&self) -> Result<ActorRefWireData, ClusterHeartbeatRemoteError> {
        recipient_for_node(&self.target, &self.recipient_path)
    }

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
