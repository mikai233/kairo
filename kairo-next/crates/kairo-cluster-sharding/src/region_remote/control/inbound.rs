use std::sync::Arc;

use kairo_actor::Recipient;
use kairo_serialization::{
    ActorRefWireData, Registry, RemoteEnvelope, RemoteMessage, SerializedMessage,
};

use crate::{BeginHandOff, HandOff, HostShard};

use super::super::ShardRegionRemoteError;
use super::reply::ShardRegionRemoteControlReplyTarget;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShardRegionRemoteControlCommand {
    HostShard {
        shard: String,
        reply: ShardRegionRemoteControlReplyTarget,
    },
    BeginHandOff {
        shard: String,
        reply: ShardRegionRemoteControlReplyTarget,
    },
    HandOff {
        shard: String,
        reply: ShardRegionRemoteControlReplyTarget,
    },
}

#[derive(Clone)]
pub struct ShardRegionRemoteControlInbound {
    region: ActorRefWireData,
    registry: Arc<Registry>,
    outbound: Arc<dyn Recipient<RemoteEnvelope> + Send + Sync>,
}

impl ShardRegionRemoteControlInbound {
    pub fn new(
        region: ActorRefWireData,
        registry: Arc<Registry>,
        outbound: impl Recipient<RemoteEnvelope> + Send + Sync + 'static,
    ) -> Self {
        Self::from_arc(region, registry, Arc::new(outbound))
    }

    pub fn from_arc(
        region: ActorRefWireData,
        registry: Arc<Registry>,
        outbound: Arc<dyn Recipient<RemoteEnvelope> + Send + Sync>,
    ) -> Self {
        Self {
            region,
            registry,
            outbound,
        }
    }

    pub fn region(&self) -> &ActorRefWireData {
        &self.region
    }

    pub fn receive(
        &self,
        envelope: RemoteEnvelope,
    ) -> Result<ShardRegionRemoteControlCommand, ShardRegionRemoteError> {
        if envelope.recipient != self.region {
            return Err(ShardRegionRemoteError::WrongRecipient {
                expected: self.region.path().to_string(),
                actual: envelope.recipient.path().to_string(),
            });
        }
        let coordinator = envelope
            .sender
            .ok_or(ShardRegionRemoteError::MissingSender(
                envelope.message.manifest.as_str().to_string(),
            ))?;
        let reply = ShardRegionRemoteControlReplyTarget::from_arc(
            self.region.clone(),
            coordinator,
            self.registry.clone(),
            self.outbound.clone(),
        );
        self.receive_message(envelope.message, reply)
    }

    fn receive_message(
        &self,
        message: SerializedMessage,
        reply: ShardRegionRemoteControlReplyTarget,
    ) -> Result<ShardRegionRemoteControlCommand, ShardRegionRemoteError> {
        match message.manifest.as_str() {
            HostShard::MANIFEST => {
                let command = self.registry.deserialize::<HostShard>(message)?;
                Ok(ShardRegionRemoteControlCommand::HostShard {
                    shard: command.shard_id,
                    reply,
                })
            }
            BeginHandOff::MANIFEST => {
                let command = self.registry.deserialize::<BeginHandOff>(message)?;
                Ok(ShardRegionRemoteControlCommand::BeginHandOff {
                    shard: command.shard_id,
                    reply,
                })
            }
            HandOff::MANIFEST => {
                let command = self.registry.deserialize::<HandOff>(message)?;
                Ok(ShardRegionRemoteControlCommand::HandOff {
                    shard: command.shard_id,
                    reply,
                })
            }
            manifest => Err(ShardRegionRemoteError::UnsupportedManifest(
                manifest.to_string(),
            )),
        }
    }
}
