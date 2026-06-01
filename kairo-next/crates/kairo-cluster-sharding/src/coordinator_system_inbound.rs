use std::fmt::{self, Display, Formatter};
use std::marker::PhantomData;
use std::sync::Arc;

use kairo_actor::{ActorRef, Recipient};
use kairo_serialization::{
    ActorRefWireData, Registry, RemoteEnvelope, RemoteMessage, SerializationError,
};

use crate::{
    BeginHandOffAck, CoordinatorRemoteReplyTarget, GetShardHome, GracefulShutdownReq,
    RegionStopped, Register, ShardCoordinatorMsg, ShardRegionRemoteControlOutbound, ShardStarted,
    ShardStopped, remote_region_id,
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
            GracefulShutdownReq::MANIFEST => self.receive_graceful_shutdown(envelope),
            RegionStopped::MANIFEST => self.receive_region_stopped(envelope),
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
        let target = ShardRegionRemoteControlOutbound::<M>::for_recipient_arc(
            register.region.clone(),
            self.registry.clone(),
            self.outbound.clone(),
        )
        .with_sender(Some(self.coordinator_wire.clone()))
        .into_handoff_region_target(remote_region_id(&register.region));
        self.tell_coordinator(ShardCoordinatorMsg::RegisterRemoteRegion {
            region: register.region,
            target: Some(target),
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

    fn receive_graceful_shutdown(
        &self,
        envelope: RemoteEnvelope,
    ) -> Result<(), ShardCoordinatorSystemInboundError> {
        let request = self
            .registry
            .deserialize::<GracefulShutdownReq>(envelope.message)?;
        self.tell_coordinator(ShardCoordinatorMsg::RemoteGracefulShutdownReq {
            region: request.region,
        })
    }

    fn receive_region_stopped(
        &self,
        envelope: RemoteEnvelope,
    ) -> Result<(), ShardCoordinatorSystemInboundError> {
        let stopped = self
            .registry
            .deserialize::<RegionStopped>(envelope.message)?;
        self.tell_coordinator(ShardCoordinatorMsg::RemoteRegionStopped {
            region: stopped.region,
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
            | GracefulShutdownReq::MANIFEST
            | RegionStopped::MANIFEST
            | ShardStarted::MANIFEST
            | BeginHandOffAck::MANIFEST
            | ShardStopped::MANIFEST
    )
}

#[cfg(test)]
mod tests;
