#![deny(missing_docs)]

use std::sync::Arc;

use kairo_actor::Recipient;
use kairo_serialization::{
    ActorRefWireData, Registry, RemoteEnvelope, RemoteMessage, SerializedMessage,
};

use crate::{BeginHandOff, HandOff, HostShard};

use super::super::ShardRegionRemoteError;
use super::reply::ShardRegionRemoteControlReplyTarget;

/// Typed local effect decoded from a stable coordinator-to-region command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShardRegionRemoteControlCommand {
    /// Start hosting a shard and acknowledge with `ShardStarted`.
    HostShard {
        /// Stable shard identifier to host.
        shard: String,
        /// Reply target bound to the commanding coordinator sender.
        reply: ShardRegionRemoteControlReplyTarget,
    },
    /// Invalidate a shard home before handoff and acknowledge the phase.
    BeginHandOff {
        /// Stable shard identifier entering handoff.
        shard: String,
        /// Reply target bound to the commanding coordinator sender.
        reply: ShardRegionRemoteControlReplyTarget,
    },
    /// Stop an owned shard and acknowledge after all entities terminate.
    HandOff {
        /// Stable shard identifier to stop.
        shard: String,
        /// Reply target bound to the commanding coordinator sender.
        reply: ShardRegionRemoteControlReplyTarget,
    },
}

/// Validates and decodes stable coordinator-to-region control envelopes.
///
/// Every command must target the configured region and carry coordinator
/// sender metadata. The resulting reply target preserves those identities so
/// acknowledgements return to the coordinator from the region endpoint.
#[derive(Clone)]
pub struct ShardRegionRemoteControlInbound {
    region: ActorRefWireData,
    registry: Arc<Registry>,
    outbound: Arc<dyn Recipient<RemoteEnvelope> + Send + Sync>,
}

impl ShardRegionRemoteControlInbound {
    /// Creates a control decoder from a concrete outbound reply recipient.
    pub fn new(
        region: ActorRefWireData,
        registry: Arc<Registry>,
        outbound: impl Recipient<RemoteEnvelope> + Send + Sync + 'static,
    ) -> Self {
        Self::from_arc(region, registry, Arc::new(outbound))
    }

    /// Creates a control decoder from a shared type-erased reply recipient.
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

    /// Returns the only accepted region wire recipient.
    pub fn region(&self) -> &ActorRefWireData {
        &self.region
    }

    /// Validates and decodes one stable region control envelope.
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
