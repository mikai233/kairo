#![deny(missing_docs)]

use std::marker::PhantomData;
use std::sync::Arc;

use kairo_actor::ActorRef;
use kairo_cluster::UniqueAddress;
use kairo_serialization::{Registry, RemoteEnvelope, RemoteMessage, SerializedMessage};

use crate::{
    RegionLocalRoutePlan, RoutedShardEnvelope, ShardDeliverPlan, ShardRegionMsg, ShardingEnvelope,
};

use super::{DEFAULT_SHARD_REGION_REMOTE_PATH, ShardRegionRemoteError, validate_recipient};

/// Decodes serialized remote entity traffic into a typed local shard region.
///
/// The outer [`Self::receive`] entry point verifies the stable region
/// recipient. The nested business message is then decoded with its registered
/// `M` codec and re-enters the local region as a [`ShardingEnvelope`]; entity
/// and shard ids remain outside the business protocol.
#[derive(Clone)]
pub struct ShardRegionRemoteInbound<M>
where
    M: Send + 'static,
{
    self_node: UniqueAddress,
    registry: Arc<Registry>,
    recipient_path: String,
    region: ActorRef<ShardRegionMsg<M>>,
    route_reply_to: ActorRef<RegionLocalRoutePlan<M>>,
    delivery_reply_to: ActorRef<ShardDeliverPlan<M>>,
    _message: PhantomData<fn(M)>,
}

impl<M> ShardRegionRemoteInbound<M>
where
    M: Send + 'static,
{
    /// Creates an inbound bridge at [`DEFAULT_SHARD_REGION_REMOTE_PATH`].
    pub fn new(
        self_node: UniqueAddress,
        registry: Arc<Registry>,
        region: ActorRef<ShardRegionMsg<M>>,
        route_reply_to: ActorRef<RegionLocalRoutePlan<M>>,
        delivery_reply_to: ActorRef<ShardDeliverPlan<M>>,
    ) -> Self {
        Self {
            self_node,
            registry,
            recipient_path: DEFAULT_SHARD_REGION_REMOTE_PATH.to_string(),
            region,
            route_reply_to,
            delivery_reply_to,
            _message: PhantomData,
        }
    }

    /// Overrides the absolute region recipient path.
    pub fn with_recipient_path(mut self, path: impl Into<String>) -> Self {
        self.recipient_path = path.into();
        self
    }

    /// Validates the envelope recipient, then decodes and delivers its message.
    pub fn receive(&self, envelope: RemoteEnvelope) -> Result<(), ShardRegionRemoteError>
    where
        M: RemoteMessage,
    {
        validate_recipient(&self.self_node, &self.recipient_path, &envelope.recipient)?;
        self.receive_message(envelope.message)
    }

    /// Decodes and delivers an already-unwrapped serialized route message.
    ///
    /// This entry point does not validate an envelope recipient. Use it only
    /// after an outer system router has established the target endpoint.
    pub fn receive_message(&self, message: SerializedMessage) -> Result<(), ShardRegionRemoteError>
    where
        M: RemoteMessage,
    {
        match message.manifest.as_str() {
            RoutedShardEnvelope::MANIFEST => {
                let routed = self.registry.deserialize::<RoutedShardEnvelope>(message)?;
                let business = self.registry.deserialize::<M>(routed.message)?;
                self.region
                    .tell(ShardRegionMsg::RouteToLocalShard {
                        shard: routed.shard_id,
                        message: ShardingEnvelope::new(routed.entity_id, business),
                        route_reply_to: self.route_reply_to.clone(),
                        delivery_reply_to: self.delivery_reply_to.clone(),
                    })
                    .map_err(|error| ShardRegionRemoteError::Send {
                        target: self.region.path().to_string(),
                        reason: error.reason().to_string(),
                    })
            }
            manifest => Err(ShardRegionRemoteError::UnsupportedManifest(
                manifest.to_string(),
            )),
        }
    }
}
