use std::sync::Arc;

use kairo_actor::{Recipient, SendError};
use kairo_cluster::UniqueAddress;
use kairo_serialization::{
    ActorRefWireData, Registry, RemoteEnvelope, RemoteMessage, SerializedMessage,
};

use crate::{
    BeginHandOff, BeginHandOffAck, HandOff, HandoffRegionTarget, HostShard, RegionId,
    ShardRegionMsg, ShardStarted, ShardStopped,
};

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
        HandOff, HandOffPlan, HostShard, HostShardPlan, SHARD_STARTED_SERIALIZER_ID, ShardStarted,
        register_sharding_protocol_codecs,
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

    #[test]
    fn remote_control_inbound_decodes_command_and_sends_stable_reply() {
        let registry = registry();
        let (outbound, rx) = collector::<RemoteEnvelope>();
        let inbound = ShardRegionRemoteControlInbound::new(region(), registry.clone(), outbound);

        let decoded = inbound
            .receive(RemoteEnvelope::new(
                region(),
                Some(coordinator()),
                registry
                    .serialize(&HostShard {
                        shard_id: "12".to_string(),
                    })
                    .unwrap(),
            ))
            .unwrap();

        let ShardRegionRemoteControlCommand::HostShard { shard, reply } = decoded else {
            panic!("expected HostShard command");
        };
        assert_eq!(shard, "12");
        assert_eq!(reply.region(), &region());
        assert_eq!(reply.coordinator(), &coordinator());

        reply.send_shard_started(shard).unwrap();
        let envelope = rx.recv().unwrap();
        assert_eq!(envelope.recipient, coordinator());
        assert_eq!(envelope.sender, Some(region()));
        assert_eq!(envelope.message.serializer_id, SHARD_STARTED_SERIALIZER_ID);
        assert_eq!(envelope.message.manifest.as_str(), ShardStarted::MANIFEST);
    }

    #[test]
    fn remote_control_inbound_rejects_wrong_recipient_sender_or_manifest() {
        let registry = registry();
        let (outbound, _rx) = collector::<RemoteEnvelope>();
        let inbound = ShardRegionRemoteControlInbound::new(region(), registry.clone(), outbound);

        let wrong_recipient = RemoteEnvelope::new(
            ActorRefWireData::new("kairo://remote@127.0.0.1:2552/user/not-region").unwrap(),
            Some(coordinator()),
            registry
                .serialize(&HostShard {
                    shard_id: "12".to_string(),
                })
                .unwrap(),
        );
        assert!(matches!(
            inbound.receive(wrong_recipient).unwrap_err(),
            ShardRegionRemoteError::WrongRecipient { .. }
        ));

        let missing_sender = RemoteEnvelope::new(
            region(),
            None,
            registry
                .serialize(&BeginHandOff {
                    shard_id: "12".to_string(),
                })
                .unwrap(),
        );
        assert!(matches!(
            inbound.receive(missing_sender).unwrap_err(),
            ShardRegionRemoteError::MissingSender(_)
        ));

        let wrong_manifest = RemoteEnvelope::new(
            region(),
            Some(coordinator()),
            registry
                .serialize(&ShardStarted {
                    shard_id: "12".to_string(),
                })
                .unwrap(),
        );
        assert!(matches!(
            inbound.receive(wrong_manifest).unwrap_err(),
            ShardRegionRemoteError::UnsupportedManifest(_)
        ));
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
