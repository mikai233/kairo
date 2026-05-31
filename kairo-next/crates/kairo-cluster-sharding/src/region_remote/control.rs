use std::sync::Arc;

use kairo_actor::{Recipient, SendError};
use kairo_cluster::UniqueAddress;
use kairo_serialization::{ActorRefWireData, Registry, RemoteEnvelope, RemoteMessage};

use crate::{BeginHandOff, HandOff, HandoffRegionTarget, HostShard, RegionId, ShardRegionMsg};

use super::{DEFAULT_SHARD_REGION_REMOTE_PATH, ShardRegionRemoteError, recipient_for_node};

#[derive(Clone)]
pub struct ShardRegionRemoteControlOutbound<M>
where
    M: Send + 'static,
{
    target: UniqueAddress,
    registry: Arc<Registry>,
    recipient_path: String,
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
                target: self.target.ordering_key(),
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::mpsc::{self, Receiver};

    use std::time::Duration;

    use kairo_actor::{Recipient, SendError};
    use kairo_cluster::UniqueAddress;
    use kairo_serialization::{ActorRefWireData, Registry, RemoteEnvelope, RemoteMessage};
    use kairo_testkit::ActorSystemTestKit;

    use crate::{
        BEGIN_HANDOFF_SERIALIZER_ID, BeginHandOff, HANDOFF_SERIALIZER_ID, HOST_SHARD_SERIALIZER_ID,
        HandOff, HandOffPlan, HostShard, HostShardPlan, register_sharding_protocol_codecs,
    };

    use super::*;

    struct CollectingRecipient<M> {
        tx: mpsc::Sender<M>,
    }

    impl<M> Recipient<M> for CollectingRecipient<M>
    where
        M: Send + 'static,
    {
        fn tell(&self, message: M) -> Result<(), SendError<M>> {
            self.tx
                .send(message)
                .map_err(|error| SendError::new(error.0, "collector closed"))
        }
    }

    fn collector<M>() -> (CollectingRecipient<M>, Receiver<M>)
    where
        M: Send + 'static,
    {
        let (tx, rx) = mpsc::channel();
        (CollectingRecipient { tx }, rx)
    }

    #[test]
    fn remote_control_outbound_sends_stable_region_control_envelopes() {
        let kit = ActorSystemTestKit::new("sharding-remote-control-outbound").unwrap();
        let registry = registry();
        let (outbound, rx) = collector::<RemoteEnvelope>();
        let host_reply = kit.create_probe::<HostShardPlan<String>>("host").unwrap();
        let begin_reply = kit
            .create_probe::<crate::BeginHandOffPlan>("begin")
            .unwrap();
        let handoff_reply = kit.create_probe::<HandOffPlan>("handoff").unwrap();
        let target = ShardRegionRemoteControlOutbound::<String>::new(
            target_node(),
            registry.clone(),
            outbound,
        )
        .with_sender(Some(coordinator()));

        target
            .tell(ShardRegionMsg::HostShard {
                shard: "12".to_string(),
                reply_to: host_reply.actor_ref(),
            })
            .unwrap();
        target
            .tell(ShardRegionMsg::BeginHandOff {
                shard: "12".to_string(),
                reply_to: begin_reply.actor_ref(),
            })
            .unwrap();
        target
            .tell(ShardRegionMsg::HandOff {
                shard: "12".to_string(),
                reply_to: handoff_reply.actor_ref(),
            })
            .unwrap();

        let host = rx.recv().unwrap();
        assert_eq!(host.recipient, region());
        assert_eq!(host.sender, Some(coordinator()));
        assert_eq!(host.message.serializer_id, HOST_SHARD_SERIALIZER_ID);
        assert_eq!(host.message.manifest.as_str(), HostShard::MANIFEST);

        let begin = rx.recv().unwrap();
        assert_eq!(begin.recipient, region());
        assert_eq!(begin.sender, Some(coordinator()));
        assert_eq!(begin.message.serializer_id, BEGIN_HANDOFF_SERIALIZER_ID);
        assert_eq!(begin.message.manifest.as_str(), BeginHandOff::MANIFEST);

        let handoff = rx.recv().unwrap();
        assert_eq!(handoff.recipient, region());
        assert_eq!(handoff.sender, Some(coordinator()));
        assert_eq!(handoff.message.serializer_id, HANDOFF_SERIALIZER_ID);
        assert_eq!(handoff.message.manifest.as_str(), HandOff::MANIFEST);
        kit.shutdown(Duration::from_secs(1)).unwrap();
    }

    fn registry() -> Arc<Registry> {
        let mut registry = Registry::new();
        register_sharding_protocol_codecs(&mut registry).unwrap();
        Arc::new(registry)
    }

    fn target_node() -> UniqueAddress {
        UniqueAddress::new(
            kairo_actor::Address::new("kairo", "remote", Some("127.0.0.1".to_string()), Some(2552)),
            2,
        )
    }

    fn coordinator() -> ActorRefWireData {
        ActorRefWireData::new("kairo://local@127.0.0.1:2551/system/sharding/coordinator").unwrap()
    }

    fn region() -> ActorRefWireData {
        ActorRefWireData::new("kairo://remote@127.0.0.1:2552/system/sharding/region").unwrap()
    }
}
