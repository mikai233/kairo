use std::marker::PhantomData;
use std::sync::Arc;

use kairo_actor::ActorRef;
use kairo_cluster::UniqueAddress;
use kairo_serialization::{Registry, RemoteEnvelope, RemoteMessage, SerializedMessage};

use crate::{
    RegionLocalRoutePlan, RoutedShardEnvelope, ShardDeliverPlan, ShardRegionMsg, ShardingEnvelope,
};

use super::{DEFAULT_SHARD_REGION_REMOTE_PATH, ShardRegionRemoteError, validate_recipient};

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

    pub fn with_recipient_path(mut self, path: impl Into<String>) -> Self {
        self.recipient_path = path.into();
        self
    }

    pub fn receive(&self, envelope: RemoteEnvelope) -> Result<(), ShardRegionRemoteError>
    where
        M: RemoteMessage,
    {
        validate_recipient(&self.self_node, &self.recipient_path, &envelope.recipient)?;
        self.receive_message(envelope.message)
    }

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
