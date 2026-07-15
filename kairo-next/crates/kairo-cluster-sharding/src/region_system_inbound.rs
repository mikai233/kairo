#![deny(missing_docs)]

use std::fmt::{self, Display, Formatter};
use std::marker::PhantomData;

use kairo_actor::ActorRef;
use kairo_serialization::{RemoteEnvelope, RemoteMessage};

use crate::{
    BeginHandOff, HandOff, HostShard, RegisterAck, RoutedShardEnvelope,
    ShardCoordinatorRemoteHomeError, ShardCoordinatorRemoteHomeInbound,
    ShardCoordinatorRemoteRegistrationError, ShardCoordinatorRemoteRegistrationInbound, ShardHome,
    ShardRegionMsg, ShardRegionRemoteControlCommand, ShardRegionRemoteControlInbound,
    ShardRegionRemoteError, ShardRegionRemoteInbound,
};

/// Failure while dispatching a remote system envelope into a shard region.
#[derive(Debug)]
pub enum ShardRegionSystemInboundError {
    /// No decoder was configured for the envelope's protocol family.
    MissingHandler(&'static str),
    /// Remote entity-route validation, decoding, or delivery failed.
    RegionRemote(ShardRegionRemoteError),
    /// Coordinator registration-reply decoding failed.
    Registration(ShardCoordinatorRemoteRegistrationError),
    /// Coordinator shard-home reply decoding failed.
    ShardHome(ShardCoordinatorRemoteHomeError),
    /// Delivery to the local typed region mailbox failed.
    Send {
        /// Local region actor path.
        target: String,
        /// Mailbox rejection reason.
        reason: String,
    },
    /// The envelope manifest is not a region system protocol message.
    UnsupportedManifest(String),
}

impl Display for ShardRegionSystemInboundError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingHandler(handler) => {
                write!(f, "shard-region system inbound has no {handler} handler")
            }
            Self::RegionRemote(error) => write!(f, "{error}"),
            Self::Registration(error) => write!(f, "{error}"),
            Self::ShardHome(error) => write!(f, "{error}"),
            Self::Send { target, reason } => {
                write!(
                    f,
                    "shard-region system inbound delivery to `{target}` failed: {reason}"
                )
            }
            Self::UnsupportedManifest(manifest) => {
                write!(f, "unsupported shard-region system manifest `{manifest}`")
            }
        }
    }
}

impl std::error::Error for ShardRegionSystemInboundError {}

impl From<ShardRegionRemoteError> for ShardRegionSystemInboundError {
    fn from(error: ShardRegionRemoteError) -> Self {
        Self::RegionRemote(error)
    }
}

impl From<ShardCoordinatorRemoteRegistrationError> for ShardRegionSystemInboundError {
    fn from(error: ShardCoordinatorRemoteRegistrationError) -> Self {
        Self::Registration(error)
    }
}

impl From<ShardCoordinatorRemoteHomeError> for ShardRegionSystemInboundError {
    fn from(error: ShardCoordinatorRemoteHomeError) -> Self {
        Self::ShardHome(error)
    }
}

/// Manifest dispatcher for stable remote traffic addressed to one shard region.
///
/// Each protocol family has an explicit optional decoder. Those decoders own
/// recipient, sender, and payload validation; this dispatcher converts their
/// outputs into typed local [`ShardRegionMsg`] values and reports missing
/// composition instead of silently dropping traffic.
#[derive(Clone)]
pub struct ShardRegionSystemInbound<M>
where
    M: Send + 'static,
{
    region: ActorRef<ShardRegionMsg<M>>,
    routes: Option<ShardRegionRemoteInbound<M>>,
    registration: Option<ShardCoordinatorRemoteRegistrationInbound>,
    shard_home: Option<ShardCoordinatorRemoteHomeInbound>,
    control: Option<ShardRegionRemoteControlInbound>,
    _message: PhantomData<fn(M)>,
}

impl<M> ShardRegionSystemInbound<M>
where
    M: Send + 'static,
{
    /// Creates a dispatcher with no remote protocol handlers installed.
    pub fn new(region: ActorRef<ShardRegionMsg<M>>) -> Self {
        Self {
            region,
            routes: None,
            registration: None,
            shard_home: None,
            control: None,
            _message: PhantomData,
        }
    }

    /// Installs the serialized entity-routing decoder.
    pub fn with_routes(mut self, inbound: ShardRegionRemoteInbound<M>) -> Self {
        self.routes = Some(inbound);
        self
    }

    /// Installs the coordinator registration-reply decoder.
    pub fn with_registration(mut self, inbound: ShardCoordinatorRemoteRegistrationInbound) -> Self {
        self.registration = Some(inbound);
        self
    }

    /// Installs the coordinator shard-home reply decoder.
    pub fn with_shard_home(mut self, inbound: ShardCoordinatorRemoteHomeInbound) -> Self {
        self.shard_home = Some(inbound);
        self
    }

    /// Installs the coordinator-to-region control decoder.
    pub fn with_control(mut self, inbound: ShardRegionRemoteControlInbound) -> Self {
        self.control = Some(inbound);
        self
    }

    /// Dispatches one stable envelope into the typed local region mailbox.
    pub fn receive(&self, envelope: RemoteEnvelope) -> Result<(), ShardRegionSystemInboundError>
    where
        M: RemoteMessage,
    {
        match envelope.message.manifest.as_str() {
            RoutedShardEnvelope::MANIFEST => self
                .routes
                .as_ref()
                .ok_or(ShardRegionSystemInboundError::MissingHandler(
                    "region route",
                ))?
                .receive(envelope)
                .map_err(Into::into),
            RegisterAck::MANIFEST => {
                let ack = self
                    .registration
                    .as_ref()
                    .ok_or(ShardRegionSystemInboundError::MissingHandler(
                        "coordinator registration",
                    ))?
                    .receive(envelope)?;
                self.region
                    .tell(ShardRegionMsg::RemoteCoordinatorRegistrationAck { ack })
                    .map_err(|error| ShardRegionSystemInboundError::Send {
                        target: self.region.path().to_string(),
                        reason: error.reason().to_string(),
                    })
            }
            ShardHome::MANIFEST => {
                let home = self
                    .shard_home
                    .as_ref()
                    .ok_or(ShardRegionSystemInboundError::MissingHandler(
                        "coordinator shard-home",
                    ))?
                    .receive(envelope)?;
                self.region
                    .tell(ShardRegionMsg::RemoteCoordinatorShardHome { home })
                    .map_err(|error| ShardRegionSystemInboundError::Send {
                        target: self.region.path().to_string(),
                        reason: error.reason().to_string(),
                    })
            }
            HostShard::MANIFEST | BeginHandOff::MANIFEST | HandOff::MANIFEST => {
                let command = self
                    .control
                    .as_ref()
                    .ok_or(ShardRegionSystemInboundError::MissingHandler(
                        "region control",
                    ))?
                    .receive(envelope)?;
                self.tell_region_control(command)
            }
            manifest => Err(ShardRegionSystemInboundError::UnsupportedManifest(
                manifest.to_string(),
            )),
        }
    }

    fn tell_region_control(
        &self,
        command: ShardRegionRemoteControlCommand,
    ) -> Result<(), ShardRegionSystemInboundError> {
        let message = match command {
            ShardRegionRemoteControlCommand::HostShard { shard, reply } => {
                ShardRegionMsg::RemoteHostShard { shard, reply }
            }
            ShardRegionRemoteControlCommand::BeginHandOff { shard, reply } => {
                ShardRegionMsg::RemoteBeginHandOff { shard, reply }
            }
            ShardRegionRemoteControlCommand::HandOff { shard, reply } => {
                ShardRegionMsg::RemoteHandOff { shard, reply }
            }
        };
        self.region
            .tell(message)
            .map_err(|error| ShardRegionSystemInboundError::Send {
                target: self.region.path().to_string(),
                reason: error.reason().to_string(),
            })
    }
}

/// Returns whether `manifest` belongs to the region system endpoint.
pub fn is_shard_region_system_manifest(manifest: &str) -> bool {
    matches!(
        manifest,
        RoutedShardEnvelope::MANIFEST
            | RegisterAck::MANIFEST
            | ShardHome::MANIFEST
            | HostShard::MANIFEST
            | BeginHandOff::MANIFEST
            | HandOff::MANIFEST
    )
}

#[cfg(test)]
mod tests;
