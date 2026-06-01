use std::sync::Arc;

use kairo_actor::Recipient;
use kairo_serialization::{ActorRefWireData, Registry, RemoteEnvelope, RemoteMessage};

use crate::{BeginHandOffAck, ShardStarted, ShardStopped};

use super::super::ShardRegionRemoteError;

#[derive(Clone)]
pub struct ShardRegionRemoteControlReplyTarget {
    region: ActorRefWireData,
    coordinator: ActorRefWireData,
    registry: Arc<Registry>,
    outbound: Arc<dyn Recipient<RemoteEnvelope> + Send + Sync>,
}

impl ShardRegionRemoteControlReplyTarget {
    pub fn new(
        region: ActorRefWireData,
        coordinator: ActorRefWireData,
        registry: Arc<Registry>,
        outbound: impl Recipient<RemoteEnvelope> + Send + Sync + 'static,
    ) -> Self {
        Self::from_arc(region, coordinator, registry, Arc::new(outbound))
    }

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

    pub fn region(&self) -> &ActorRefWireData {
        &self.region
    }

    pub fn coordinator(&self) -> &ActorRefWireData {
        &self.coordinator
    }

    pub fn send_shard_started(&self, shard: String) -> Result<(), ShardRegionRemoteError> {
        self.send_to_coordinator(&ShardStarted { shard_id: shard })
    }

    pub fn send_begin_handoff_ack(&self, shard: String) -> Result<(), ShardRegionRemoteError> {
        self.send_to_coordinator(&BeginHandOffAck { shard_id: shard })
    }

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
