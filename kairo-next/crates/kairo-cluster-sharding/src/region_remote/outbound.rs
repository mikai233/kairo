use std::sync::Arc;

use kairo_actor::{Recipient, SendError};
use kairo_cluster::UniqueAddress;
use kairo_serialization::{ActorRefWireData, Registry, RemoteEnvelope, RemoteMessage};

use crate::{RegionId, RegionRouteTarget, RoutedShardEnvelope, ShardRegionMsg, ShardingEnvelope};

use super::{DEFAULT_SHARD_REGION_REMOTE_PATH, ShardRegionRemoteError, recipient_for_node};

#[derive(Clone)]
pub struct ShardRegionRemoteOutbound<M> {
    target: UniqueAddress,
    registry: Arc<Registry>,
    recipient_path: String,
    sender: Option<ActorRefWireData>,
    outbound: Arc<dyn Recipient<RemoteEnvelope> + Send + Sync>,
    _message: std::marker::PhantomData<fn(M)>,
}

impl<M> ShardRegionRemoteOutbound<M> {
    pub fn new(
        target: UniqueAddress,
        registry: Arc<Registry>,
        outbound: impl Recipient<RemoteEnvelope> + Send + Sync + 'static,
    ) -> Self {
        Self::from_arc(target, registry, Arc::new(outbound))
    }

    pub fn from_arc(
        target: UniqueAddress,
        registry: Arc<Registry>,
        outbound: Arc<dyn Recipient<RemoteEnvelope> + Send + Sync>,
    ) -> Self {
        Self {
            target,
            registry,
            recipient_path: DEFAULT_SHARD_REGION_REMOTE_PATH.to_string(),
            sender: None,
            outbound,
            _message: std::marker::PhantomData,
        }
    }

    pub fn with_recipient_path(mut self, path: impl Into<String>) -> Self {
        self.recipient_path = path.into();
        self
    }

    pub fn with_sender(mut self, sender: Option<ActorRefWireData>) -> Self {
        self.sender = sender;
        self
    }

    pub fn recipient_for_target(&self) -> Result<ActorRefWireData, ShardRegionRemoteError> {
        recipient_for_node(&self.target, &self.recipient_path)
    }

    pub fn into_region_route_target(self, region: impl Into<RegionId>) -> RegionRouteTarget<M>
    where
        M: RemoteMessage + Send + 'static,
    {
        RegionRouteTarget::new(region, self)
    }
}

impl<M> ShardRegionRemoteOutbound<M>
where
    M: RemoteMessage,
{
    fn routed_envelope(
        &self,
        shard: &str,
        message: &ShardingEnvelope<M>,
    ) -> Result<RoutedShardEnvelope, ShardRegionRemoteError> {
        Ok(RoutedShardEnvelope {
            shard_id: shard.to_string(),
            entity_id: message.entity_id().to_string(),
            message: self.registry.serialize(message.message())?,
        })
    }

    fn send_routed(&self, routed: RoutedShardEnvelope) -> Result<(), ShardRegionRemoteError> {
        let recipient = self.recipient_for_target()?;
        let message = self.registry.serialize(&routed)?;
        let envelope = RemoteEnvelope::new(recipient, self.sender.clone(), message);
        self.outbound
            .tell(envelope)
            .map_err(|error| ShardRegionRemoteError::Send {
                target: self.target.ordering_key(),
                reason: error.reason().to_string(),
            })
    }
}

impl<M> Recipient<ShardRegionMsg<M>> for ShardRegionRemoteOutbound<M>
where
    M: RemoteMessage + Send + 'static,
{
    fn tell(&self, message: ShardRegionMsg<M>) -> Result<(), SendError<ShardRegionMsg<M>>> {
        match message {
            ShardRegionMsg::RouteToLocalShard {
                shard,
                message,
                route_reply_to,
                delivery_reply_to,
            } => match self.routed_envelope(&shard, &message) {
                Ok(routed) => match self.send_routed(routed) {
                    Ok(()) => Ok(()),
                    Err(error) => Err(SendError::new(
                        ShardRegionMsg::RouteToLocalShard {
                            shard,
                            message,
                            route_reply_to,
                            delivery_reply_to,
                        },
                        error.to_string(),
                    )),
                },
                Err(error) => Err(SendError::new(
                    ShardRegionMsg::RouteToLocalShard {
                        shard,
                        message,
                        route_reply_to,
                        delivery_reply_to,
                    },
                    error.to_string(),
                )),
            },
            other => Err(SendError::new(
                other,
                ShardRegionRemoteError::UnsupportedLocalMessage("non-route-to-local-shard")
                    .to_string(),
            )),
        }
    }
}
