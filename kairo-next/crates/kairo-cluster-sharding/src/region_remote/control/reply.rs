#![deny(missing_docs)]

use std::sync::Arc;

use kairo_actor::Recipient;
use kairo_serialization::{ActorRefWireData, Registry, RemoteEnvelope, RemoteMessage};

use crate::{BeginHandOffAck, ShardStarted, ShardStopped};

use super::super::ShardRegionRemoteError;

/// Region-to-coordinator reply bridge bound to one received control command.
///
/// Replies use the region as envelope sender and the command's coordinator
/// sender as recipient, preserving the actor reply direction across the stable
/// wire boundary.
#[derive(Clone)]
pub struct ShardRegionRemoteControlReplyTarget {
    region: ActorRefWireData,
    coordinator: ActorRefWireData,
    registry: Arc<Registry>,
    outbound: Arc<dyn Recipient<RemoteEnvelope> + Send + Sync>,
}

impl ShardRegionRemoteControlReplyTarget {
    /// Creates a reply target from a concrete outbound recipient.
    pub fn new(
        region: ActorRefWireData,
        coordinator: ActorRefWireData,
        registry: Arc<Registry>,
        outbound: impl Recipient<RemoteEnvelope> + Send + Sync + 'static,
    ) -> Self {
        Self::from_arc(region, coordinator, registry, Arc::new(outbound))
    }

    /// Creates a reply target from a shared type-erased outbound recipient.
    pub fn from_arc(
        region: ActorRefWireData,
        coordinator: ActorRefWireData,
        registry: Arc<Registry>,
        outbound: Arc<dyn Recipient<RemoteEnvelope> + Send + Sync>,
    ) -> Self {
        Self {
            region,
            coordinator,
            registry,
            outbound,
        }
    }

    /// Returns the stable region sender identity.
    pub fn region(&self) -> &ActorRefWireData {
        &self.region
    }

    /// Returns the stable coordinator reply recipient.
    pub fn coordinator(&self) -> &ActorRefWireData {
        &self.coordinator
    }

    /// Reports that the region started hosting `shard`.
    pub fn send_shard_started(&self, shard: String) -> Result<(), ShardRegionRemoteError> {
        self.send_to_coordinator(&ShardStarted { shard_id: shard })
    }

    /// Acknowledges cache invalidation for the first handoff phase.
    pub fn send_begin_handoff_ack(&self, shard: String) -> Result<(), ShardRegionRemoteError> {
        self.send_to_coordinator(&BeginHandOffAck { shard_id: shard })
    }

    /// Reports that a handed-off shard and all its entities have stopped.
    pub fn send_shard_stopped(&self, shard: String) -> Result<(), ShardRegionRemoteError> {
        self.send_to_coordinator(&ShardStopped { shard_id: shard })
    }

    fn send_to_coordinator<M>(&self, message: &M) -> Result<(), ShardRegionRemoteError>
    where
        M: RemoteMessage,
    {
        let serialized = self.registry.serialize(message)?;
        self.outbound
            .tell(RemoteEnvelope::new(
                self.coordinator.clone(),
                Some(self.region.clone()),
                serialized,
            ))
            .map_err(|error| ShardRegionRemoteError::Send {
                target: self.coordinator.path().to_string(),
                reason: error.reason().to_string(),
            })
    }
}

impl std::fmt::Debug for ShardRegionRemoteControlReplyTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardRegionRemoteControlReplyTarget")
            .field("region", &self.region)
            .field("coordinator", &self.coordinator)
            .finish_non_exhaustive()
    }
}

impl PartialEq for ShardRegionRemoteControlReplyTarget {
    fn eq(&self, other: &Self) -> bool {
        self.region == other.region && self.coordinator == other.coordinator
    }
}

impl Eq for ShardRegionRemoteControlReplyTarget {}
