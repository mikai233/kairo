#![deny(missing_docs)]

use std::sync::Arc;

use kairo_actor::ActorRef;
use kairo_serialization::{ActorRefWireData, Registry, RemoteEnvelope, RemoteMessage};

use crate::{HeartbeatRsp, HeartbeatSenderMsg};

use super::ClusterHeartbeatRemoteError;

#[derive(Clone)]
/// Validates remote heartbeat responses and forwards them to the local sender.
pub struct HeartbeatRemoteResponseInbound {
    recipient: ActorRefWireData,
    registry: Arc<Registry>,
    sender: ActorRef<HeartbeatSenderMsg>,
}

impl HeartbeatRemoteResponseInbound {
    /// Creates a response endpoint for one exact serialized actor recipient.
    pub fn new(
        recipient: ActorRefWireData,
        registry: Arc<Registry>,
        sender: ActorRef<HeartbeatSenderMsg>,
    ) -> Self {
        Self {
            recipient,
            registry,
            sender,
        }
    }

    /// Validates and decodes a response before delivering it to the sender
    /// actor.
    pub fn receive(&self, envelope: RemoteEnvelope) -> Result<(), ClusterHeartbeatRemoteError> {
        if envelope.recipient != self.recipient {
            return Err(ClusterHeartbeatRemoteError::WrongRecipient {
                expected: self.recipient.path().to_string(),
                actual: envelope.recipient.path().to_string(),
            });
        }
        if envelope.message.manifest.as_str() != HeartbeatRsp::MANIFEST {
            return Err(ClusterHeartbeatRemoteError::UnsupportedManifest(
                envelope.message.manifest.as_str().to_string(),
            ));
        }
        let response = self
            .registry
            .deserialize::<HeartbeatRsp>(envelope.message)?;
        self.sender
            .tell(HeartbeatSenderMsg::HeartbeatResponse(response))
            .map_err(|error| ClusterHeartbeatRemoteError::Send {
                target: self.sender.path().to_string(),
                reason: error.reason().to_string(),
            })
    }
}
