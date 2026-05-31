use std::fmt::{self, Display, Formatter};
use std::marker::PhantomData;
use std::sync::Arc;

use kairo_actor::{ActorRef, Recipient};
use kairo_serialization::{
    ActorRefWireData, Registry, RemoteEnvelope, RemoteMessage, SerializationError,
};

use crate::{
    BeginHandOffAck, CoordinatorRemoteReplyTarget, GetShardHome, Register, ShardCoordinatorMsg,
    ShardStarted, ShardStopped,
};

#[derive(Debug)]
pub enum ShardCoordinatorSystemInboundError {
    MissingSender(&'static str),
    Serialization(SerializationError),
    Send { target: String, reason: String },
    UnsupportedManifest(String),
    WrongRecipient { expected: String, actual: String },
}

impl Display for ShardCoordinatorSystemInboundError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingSender(message) => {
                write!(
                    f,
                    "shard-coordinator system inbound `{message}` envelope has no sender"
                )
            }
            Self::Serialization(error) => {
                write!(f, "shard-coordinator system inbound codec failed: {error}")
            }
            Self::Send { target, reason } => {
                write!(
                    f,
                    "shard-coordinator system inbound delivery to `{target}` failed: {reason}"
                )
            }
            Self::UnsupportedManifest(manifest) => {
                write!(
                    f,
                    "unsupported shard-coordinator system manifest `{manifest}`"
                )
            }
            Self::WrongRecipient { expected, actual } => {
                write!(
                    f,
                    "shard-coordinator system inbound envelope was addressed to {actual}, expected {expected}"
                )
            }
        }
    }
}

impl std::error::Error for ShardCoordinatorSystemInboundError {}

impl From<SerializationError> for ShardCoordinatorSystemInboundError {
    fn from(error: SerializationError) -> Self {
        Self::Serialization(error)
    }
}

#[derive(Clone)]
pub struct ShardCoordinatorSystemInbound<M = ()>
where
    M: Send + 'static,
{
    coordinator: ActorRef<ShardCoordinatorMsg<M>>,
    coordinator_wire: ActorRefWireData,
    registry: Arc<Registry>,
    outbound: Arc<dyn Recipient<RemoteEnvelope> + Send + Sync>,
    _message: PhantomData<fn(M)>,
}

impl<M> ShardCoordinatorSystemInbound<M>
where
    M: Send + 'static,
{
    pub fn new(
        coordinator: ActorRef<ShardCoordinatorMsg<M>>,
        coordinator_wire: ActorRefWireData,
        registry: Arc<Registry>,
        outbound: impl Recipient<RemoteEnvelope> + Send + Sync + 'static,
    ) -> Self {
        Self::from_arc(coordinator, coordinator_wire, registry, Arc::new(outbound))
    }

    pub fn from_arc(
        coordinator: ActorRef<ShardCoordinatorMsg<M>>,
        coordinator_wire: ActorRefWireData,
        registry: Arc<Registry>,
        outbound: Arc<dyn Recipient<RemoteEnvelope> + Send + Sync>,
    ) -> Self {
        Self {
            coordinator,
            coordinator_wire,
            registry,
            outbound,
            _message: PhantomData,
        }
    }

    pub fn coordinator_wire(&self) -> &ActorRefWireData {
        &self.coordinator_wire
    }

    pub fn receive(
        &self,
        envelope: RemoteEnvelope,
    ) -> Result<(), ShardCoordinatorSystemInboundError> {
        if envelope.recipient != self.coordinator_wire {
            return Err(ShardCoordinatorSystemInboundError::WrongRecipient {
                expected: self.coordinator_wire.path().to_string(),
                actual: envelope.recipient.path().to_string(),
            });
        }

        match envelope.message.manifest.as_str() {
            Register::MANIFEST => self.receive_register(envelope),
            GetShardHome::MANIFEST => self.receive_get_shard_home(envelope),
            ShardStarted::MANIFEST => self.receive_shard_started(envelope),
            BeginHandOffAck::MANIFEST => self.receive_begin_handoff_ack(envelope),
            ShardStopped::MANIFEST => self.receive_shard_stopped(envelope),
            manifest => Err(ShardCoordinatorSystemInboundError::UnsupportedManifest(
                manifest.to_string(),
            )),
        }
    }

    fn receive_register(
        &self,
        envelope: RemoteEnvelope,
    ) -> Result<(), ShardCoordinatorSystemInboundError> {
        let register = self.registry.deserialize::<Register>(envelope.message)?;
        self.tell_coordinator(ShardCoordinatorMsg::RegisterRemoteRegion {
            region: register.region,
            reply: self.reply_target(),
        })
    }

    fn receive_get_shard_home(
        &self,
        envelope: RemoteEnvelope,
    ) -> Result<(), ShardCoordinatorSystemInboundError> {
        let requester =
            envelope
                .sender
                .ok_or(ShardCoordinatorSystemInboundError::MissingSender(
                    GetShardHome::MANIFEST,
                ))?;
        let request = self
            .registry
            .deserialize::<GetShardHome>(envelope.message)?;
        self.tell_coordinator(ShardCoordinatorMsg::RequestRemoteShardHome {
            requester,
            shard: request.shard_id,
            reply: self.reply_target(),
        })
    }

    fn receive_shard_started(
        &self,
        envelope: RemoteEnvelope,
    ) -> Result<(), ShardCoordinatorSystemInboundError> {
        let region = envelope
            .sender
            .ok_or(ShardCoordinatorSystemInboundError::MissingSender(
                ShardStarted::MANIFEST,
            ))?;
        let started = self
            .registry
            .deserialize::<ShardStarted>(envelope.message)?;
        self.tell_coordinator(ShardCoordinatorMsg::RemoteHostShardObserved { region, started })
    }

    fn receive_begin_handoff_ack(
        &self,
        envelope: RemoteEnvelope,
    ) -> Result<(), ShardCoordinatorSystemInboundError> {
        let region = envelope
            .sender
            .ok_or(ShardCoordinatorSystemInboundError::MissingSender(
                BeginHandOffAck::MANIFEST,
            ))?;
        let ack = self
            .registry
            .deserialize::<BeginHandOffAck>(envelope.message)?;
        self.tell_coordinator(ShardCoordinatorMsg::RemoteBeginHandOffAck { region, ack })
    }

    fn receive_shard_stopped(
        &self,
        envelope: RemoteEnvelope,
    ) -> Result<(), ShardCoordinatorSystemInboundError> {
        let region = envelope
            .sender
            .ok_or(ShardCoordinatorSystemInboundError::MissingSender(
                ShardStopped::MANIFEST,
            ))?;
        let stopped = self
            .registry
            .deserialize::<ShardStopped>(envelope.message)?;
        self.tell_coordinator(ShardCoordinatorMsg::RemoteShardStopped { region, stopped })
    }

    fn reply_target(&self) -> CoordinatorRemoteReplyTarget {
        CoordinatorRemoteReplyTarget::from_arc(
            self.coordinator_wire.clone(),
            self.registry.clone(),
            self.outbound.clone(),
        )
    }

    fn tell_coordinator(
        &self,
        msg: ShardCoordinatorMsg<M>,
    ) -> Result<(), ShardCoordinatorSystemInboundError> {
        self.coordinator
            .tell(msg)
            .map_err(|error| ShardCoordinatorSystemInboundError::Send {
                target: self.coordinator.path().to_string(),
                reason: error.reason().to_string(),
            })
    }
}

pub fn is_shard_coordinator_system_manifest(manifest: &str) -> bool {
    matches!(
        manifest,
        Register::MANIFEST
            | GetShardHome::MANIFEST
            | ShardStarted::MANIFEST
            | BeginHandOffAck::MANIFEST
            | ShardStopped::MANIFEST
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use kairo_serialization::{ActorRefWireData, Manifest, Registry, SerializedMessage};
    use kairo_testkit::ActorSystemTestKit;

    use crate::{
        CoordinatorState, LeastShardAllocationStrategy, RegisterAck, ShardCoordinatorActor,
        ShardHome, register_sharding_protocol_codecs,
    };

    use super::*;

    #[test]
    fn coordinator_system_inbound_routes_register_and_get_shard_home() {
        let kit = ActorSystemTestKit::new("sharding-coordinator-system-inbound").unwrap();
        let registry = registry();
        let outbound = kit
            .create_probe::<RemoteEnvelope>("remote-outbound")
            .unwrap();
        let coordinator = kit
            .system()
            .spawn(
                "coordinator",
                ShardCoordinatorActor::props(
                    CoordinatorState::new(),
                    LeastShardAllocationStrategy::default(),
                ),
            )
            .unwrap();
        let inbound = ShardCoordinatorSystemInbound::<()>::new(
            coordinator,
            coordinator_wire(),
            registry.clone(),
            outbound.actor_ref(),
        );

        inbound
            .receive(RemoteEnvelope::new(
                coordinator_wire(),
                Some(region_wire()),
                registry
                    .serialize(&Register {
                        region: region_wire(),
                    })
                    .unwrap(),
            ))
            .unwrap();

        let ack = outbound.expect_msg(Duration::from_millis(500)).unwrap();
        assert_eq!(ack.recipient, region_wire());
        assert_eq!(ack.sender, Some(coordinator_wire()));
        assert_eq!(ack.message.manifest.as_str(), RegisterAck::MANIFEST);

        inbound
            .receive(RemoteEnvelope::new(
                coordinator_wire(),
                Some(region_wire()),
                registry
                    .serialize(&GetShardHome {
                        shard_id: "12".to_string(),
                    })
                    .unwrap(),
            ))
            .unwrap();

        let home = outbound.expect_msg(Duration::from_millis(500)).unwrap();
        assert_eq!(home.recipient, region_wire());
        assert_eq!(home.sender, Some(coordinator_wire()));
        assert_eq!(home.message.manifest.as_str(), ShardHome::MANIFEST);
        assert_eq!(
            registry.deserialize::<ShardHome>(home.message).unwrap(),
            ShardHome {
                shard_id: "12".to_string(),
                region: region_wire(),
            }
        );
        kit.shutdown(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn coordinator_system_inbound_routes_region_control_replies() {
        let kit = ActorSystemTestKit::new("sharding-coordinator-system-control-replies").unwrap();
        let registry = registry();
        let outbound = kit
            .create_probe::<RemoteEnvelope>("remote-outbound")
            .unwrap();
        let coordinator = kit
            .create_probe::<ShardCoordinatorMsg<()>>("coordinator")
            .unwrap();
        let inbound = ShardCoordinatorSystemInbound::<()>::new(
            coordinator.actor_ref(),
            coordinator_wire(),
            registry.clone(),
            outbound.actor_ref(),
        );

        inbound
            .receive(RemoteEnvelope::new(
                coordinator_wire(),
                Some(region_wire()),
                registry
                    .serialize(&ShardStarted {
                        shard_id: "12".to_string(),
                    })
                    .unwrap(),
            ))
            .unwrap();
        match coordinator.expect_msg(Duration::from_millis(500)).unwrap() {
            ShardCoordinatorMsg::RemoteHostShardObserved { region, started } => {
                assert_eq!(region, region_wire());
                assert_eq!(started.shard_id, "12");
            }
            _ => panic!("expected remote host-shard observation"),
        }

        inbound
            .receive(RemoteEnvelope::new(
                coordinator_wire(),
                Some(region_wire()),
                registry
                    .serialize(&BeginHandOffAck {
                        shard_id: "12".to_string(),
                    })
                    .unwrap(),
            ))
            .unwrap();
        match coordinator.expect_msg(Duration::from_millis(500)).unwrap() {
            ShardCoordinatorMsg::RemoteBeginHandOffAck { region, ack } => {
                assert_eq!(region, region_wire());
                assert_eq!(ack.shard_id, "12");
            }
            _ => panic!("expected remote begin-handoff ack"),
        }

        inbound
            .receive(RemoteEnvelope::new(
                coordinator_wire(),
                Some(region_wire()),
                registry
                    .serialize(&ShardStopped {
                        shard_id: "12".to_string(),
                    })
                    .unwrap(),
            ))
            .unwrap();
        match coordinator.expect_msg(Duration::from_millis(500)).unwrap() {
            ShardCoordinatorMsg::RemoteShardStopped { region, stopped } => {
                assert_eq!(region, region_wire());
                assert_eq!(stopped.shard_id, "12");
            }
            _ => panic!("expected remote shard-stopped"),
        }
        kit.shutdown(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn coordinator_system_inbound_rejects_wrong_recipient_sender_or_manifest() {
        let kit = ActorSystemTestKit::new("sharding-coordinator-system-inbound-errors").unwrap();
        let registry = registry();
        let outbound = kit
            .create_probe::<RemoteEnvelope>("remote-outbound")
            .unwrap();
        let coordinator = kit
            .system()
            .spawn(
                "coordinator",
                ShardCoordinatorActor::props(
                    CoordinatorState::new(),
                    LeastShardAllocationStrategy::default(),
                ),
            )
            .unwrap();
        let inbound = ShardCoordinatorSystemInbound::<()>::new(
            coordinator,
            coordinator_wire(),
            registry.clone(),
            outbound.actor_ref(),
        );

        let wrong_recipient = RemoteEnvelope::new(
            ActorRefWireData::new("kairo://remote@127.0.0.1:2552/user/not-coordinator").unwrap(),
            Some(region_wire()),
            registry
                .serialize(&Register {
                    region: region_wire(),
                })
                .unwrap(),
        );
        assert!(matches!(
            inbound.receive(wrong_recipient).unwrap_err(),
            ShardCoordinatorSystemInboundError::WrongRecipient { .. }
        ));

        let missing_sender = RemoteEnvelope::new(
            coordinator_wire(),
            None,
            registry
                .serialize(&GetShardHome {
                    shard_id: "12".to_string(),
                })
                .unwrap(),
        );
        assert!(matches!(
            inbound.receive(missing_sender).unwrap_err(),
            ShardCoordinatorSystemInboundError::MissingSender(_)
        ));

        let wrong_manifest = RemoteEnvelope::new(
            coordinator_wire(),
            Some(region_wire()),
            SerializedMessage {
                serializer_id: 4_000,
                manifest: Manifest::new("kairo.sharding.unsupported-coordinator"),
                version: 1,
                payload: bytes::Bytes::new(),
            },
        );
        assert!(matches!(
            inbound.receive(wrong_manifest).unwrap_err(),
            ShardCoordinatorSystemInboundError::UnsupportedManifest(_)
        ));
        kit.shutdown(Duration::from_secs(1)).unwrap();
    }

    fn registry() -> Arc<Registry> {
        let mut registry = Registry::new();
        register_sharding_protocol_codecs(&mut registry).unwrap();
        Arc::new(registry)
    }

    fn coordinator_wire() -> ActorRefWireData {
        ActorRefWireData::new("kairo://remote@127.0.0.1:2552/system/sharding/coordinator").unwrap()
    }

    fn region_wire() -> ActorRefWireData {
        ActorRefWireData::new("kairo://remote@127.0.0.1:2552/system/sharding/region").unwrap()
    }
}
