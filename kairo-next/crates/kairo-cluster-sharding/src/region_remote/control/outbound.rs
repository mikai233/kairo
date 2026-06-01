use std::sync::Arc;

use kairo_actor::{Recipient, SendError};
use kairo_cluster::UniqueAddress;
use kairo_serialization::{ActorRefWireData, Registry, RemoteEnvelope, RemoteMessage};

use crate::{BeginHandOff, HandOff, HandoffRegionTarget, HostShard, RegionId, ShardRegionMsg};

use super::super::{DEFAULT_SHARD_REGION_REMOTE_PATH, ShardRegionRemoteError};
use super::target::ShardRegionRemoteControlTarget;

#[derive(Clone)]
pub struct ShardRegionRemoteControlOutbound<M>
where
    M: Send + 'static,
{
    target: ShardRegionRemoteControlTarget,
    registry: Arc<Registry>,
    sender: Option<ActorRefWireData>,
    outbound: Arc<dyn Recipient<RemoteEnvelope> + Send + Sync>,
    _message: std::marker::PhantomData<fn(M)>,
}

impl<M> ShardRegionRemoteControlOutbound<M>
where
    M: Send + 'static,
{
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
            target: ShardRegionRemoteControlTarget::node(
                target,
                DEFAULT_SHARD_REGION_REMOTE_PATH.to_string(),
            ),
            registry,
            sender: None,
            outbound,
            _message: std::marker::PhantomData,
        }
    }

    pub fn for_recipient(
        recipient: ActorRefWireData,
        registry: Arc<Registry>,
        outbound: impl Recipient<RemoteEnvelope> + Send + Sync + 'static,
    ) -> Self {
        Self::for_recipient_arc(recipient, registry, Arc::new(outbound))
    }

    pub fn for_recipient_arc(
        recipient: ActorRefWireData,
        registry: Arc<Registry>,
        outbound: Arc<dyn Recipient<RemoteEnvelope> + Send + Sync>,
    ) -> Self {
        Self {
            target: ShardRegionRemoteControlTarget::recipient(recipient),
            registry,
            sender: None,
            outbound,
            _message: std::marker::PhantomData,
        }
    }

    pub fn with_recipient_path(mut self, path: impl Into<String>) -> Self {
        self.target.set_recipient_path(path.into());
        self
    }

    pub fn with_sender(mut self, sender: Option<ActorRefWireData>) -> Self {
        self.sender = sender;
        self
    }

    pub fn recipient_for_target(&self) -> Result<ActorRefWireData, ShardRegionRemoteError> {
        self.target.resolve_recipient()
    }

    pub fn into_handoff_region_target(self, region: impl Into<RegionId>) -> HandoffRegionTarget<M> {
        HandoffRegionTarget::new(region, self)
    }

    fn send_control<C>(&self, command: &C) -> Result<(), ShardRegionRemoteError>
    where
        C: RemoteMessage,
    {
        let recipient = self.recipient_for_target()?;
        let message = self.registry.serialize(command)?;
        let envelope = RemoteEnvelope::new(recipient, self.sender.clone(), message);
        self.outbound
            .tell(envelope)
            .map_err(|error| ShardRegionRemoteError::Send {
                target: self.target.key(),
                reason: error.reason().to_string(),
            })
    }
}

impl<M> Recipient<ShardRegionMsg<M>> for ShardRegionRemoteControlOutbound<M>
where
    M: Send + 'static,
{
    fn tell(&self, message: ShardRegionMsg<M>) -> Result<(), SendError<ShardRegionMsg<M>>> {
        match message {
            ShardRegionMsg::HostShard { shard, reply_to } => self
                .send_control(&HostShard {
                    shard_id: shard.clone(),
                })
                .map_err(|error| {
                    SendError::new(
                        ShardRegionMsg::HostShard { shard, reply_to },
                        error.to_string(),
                    )
                }),
            ShardRegionMsg::BeginHandOff { shard, reply_to } => self
                .send_control(&BeginHandOff {
                    shard_id: shard.clone(),
                })
                .map_err(|error| {
                    SendError::new(
                        ShardRegionMsg::BeginHandOff { shard, reply_to },
                        error.to_string(),
                    )
                }),
            ShardRegionMsg::HandOff { shard, reply_to } => self
                .send_control(&HandOff {
                    shard_id: shard.clone(),
                })
                .map_err(|error| {
                    SendError::new(
                        ShardRegionMsg::HandOff { shard, reply_to },
                        error.to_string(),
                    )
                }),
            ShardRegionMsg::HandOffToLocalShard {
                shard,
                stop_message,
                region_reply_to,
                shard_reply_to,
            } => self
                .send_control(&HandOff {
                    shard_id: shard.clone(),
                })
                .map_err(|error| {
                    SendError::new(
                        ShardRegionMsg::HandOffToLocalShard {
                            shard,
                            stop_message,
                            region_reply_to,
                            shard_reply_to,
                        },
                        error.to_string(),
                    )
                }),
            other => Err(SendError::new(
                other,
                ShardRegionRemoteError::UnsupportedLocalMessage("non-region-control").to_string(),
            )),
        }
    }
}
