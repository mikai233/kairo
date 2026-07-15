#![deny(missing_docs)]

use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use kairo_actor::{Recipient, SendError};
use kairo_serialization::{
    ActorRefWireData, Registry, RemoteEnvelope, RemoteMessage, SerializationError,
};

use crate::{GetShardHome, ShardCoordinatorRemoteTarget, ShardHome, ShardId};

/// Failure while sending or decoding the remote shard-home protocol.
#[derive(Debug)]
pub enum ShardCoordinatorRemoteHomeError {
    /// Stable message serialization or deserialization failed.
    Serialization(SerializationError),
    /// The outbound transport rejected a shard-home request envelope.
    Send {
        /// Stable coordinator recipient path.
        target: String,
        /// Transport rejection reason.
        reason: String,
    },
    /// A shard-home reply targeted a different region.
    WrongRecipient {
        /// Configured region recipient path.
        expected: String,
        /// Recipient path carried by the envelope.
        actual: String,
    },
    /// The inbound envelope did not carry [`ShardHome::MANIFEST`].
    UnsupportedManifest(String),
}

impl Display for ShardCoordinatorRemoteHomeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Serialization(error) => {
                write!(
                    f,
                    "shard-coordinator remote shard-home codec failed: {error}"
                )
            }
            Self::Send { target, reason } => {
                write!(
                    f,
                    "shard-coordinator remote shard-home send to `{target}` failed: {reason}"
                )
            }
            Self::WrongRecipient { expected, actual } => {
                write!(
                    f,
                    "shard-coordinator remote shard-home reply was addressed to {actual}, expected {expected}"
                )
            }
            Self::UnsupportedManifest(manifest) => {
                write!(
                    f,
                    "unsupported shard-coordinator remote shard-home manifest `{manifest}`"
                )
            }
        }
    }
}

impl std::error::Error for ShardCoordinatorRemoteHomeError {}

impl From<SerializationError> for ShardCoordinatorRemoteHomeError {
    fn from(error: SerializationError) -> Self {
        Self::Serialization(error)
    }
}

/// Outbound bridge for stable remote shard-home requests.
///
/// The region wire ref is the default envelope sender so the coordinator can
/// route [`ShardHome`] replies back to the requesting region. Retry and
/// buffered-delivery policy remain owned by the region actor.
#[derive(Clone)]
pub struct ShardCoordinatorRemoteHomeOutbound {
    target: ShardCoordinatorRemoteTarget,
    region: ActorRefWireData,
    sender: Option<ActorRefWireData>,
    registry: Arc<Registry>,
    outbound: Arc<dyn Recipient<RemoteEnvelope> + Send + Sync>,
}

impl ShardCoordinatorRemoteHomeOutbound {
    /// Creates a bridge from a concrete outbound envelope recipient.
    pub fn new(
        target: ShardCoordinatorRemoteTarget,
        region: ActorRefWireData,
        registry: Arc<Registry>,
        outbound: impl Recipient<RemoteEnvelope> + Send + Sync + 'static,
    ) -> Self {
        Self::from_arc(target, region, registry, Arc::new(outbound))
    }

    /// Creates a bridge from a shared type-erased outbound recipient.
    pub fn from_arc(
        target: ShardCoordinatorRemoteTarget,
        region: ActorRefWireData,
        registry: Arc<Registry>,
        outbound: Arc<dyn Recipient<RemoteEnvelope> + Send + Sync>,
    ) -> Self {
        let sender = Some(region.clone());
        Self {
            target,
            region,
            sender,
            registry,
            outbound,
        }
    }

    /// Returns the selected remote coordinator target.
    pub fn target(&self) -> &ShardCoordinatorRemoteTarget {
        &self.target
    }

    /// Returns the requesting region's stable wire identity.
    pub fn region(&self) -> &ActorRefWireData {
        &self.region
    }

    /// Returns the envelope sender metadata used for requests.
    pub fn sender(&self) -> Option<&ActorRefWireData> {
        self.sender.as_ref()
    }

    /// Overrides the default region sender metadata.
    pub fn with_sender(mut self, sender: Option<ActorRefWireData>) -> Self {
        self.sender = sender;
        self
    }

    /// Builds, serializes, and enqueues a lookup for `shard_id`.
    pub fn request_shard_home(
        &self,
        shard_id: impl Into<ShardId>,
    ) -> Result<(), ShardCoordinatorRemoteHomeError> {
        self.send_request(GetShardHome {
            shard_id: shard_id.into(),
        })
    }

    /// Serializes and enqueues an existing shard-home request.
    pub fn send_request(
        &self,
        request: GetShardHome,
    ) -> Result<(), ShardCoordinatorRemoteHomeError> {
        let message = self.registry.serialize(&request)?;
        let envelope = RemoteEnvelope::new(
            self.target.recipient().clone(),
            self.sender.clone(),
            message,
        );
        self.outbound
            .tell(envelope)
            .map_err(|error| ShardCoordinatorRemoteHomeError::Send {
                target: self.target.recipient().path().to_string(),
                reason: error.reason().to_string(),
            })
    }
}

impl Recipient<GetShardHome> for ShardCoordinatorRemoteHomeOutbound {
    fn tell(&self, message: GetShardHome) -> Result<(), SendError<GetShardHome>> {
        self.send_request(message.clone())
            .map_err(|error| SendError::new(message, error.to_string()))
    }
}

/// Decoded remote shard-home reply and its envelope sender.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardCoordinatorRemoteHome {
    /// Coordinator sender metadata carried by the remote envelope.
    pub sender: Option<ActorRefWireData>,
    /// Stable shard identifier whose home was resolved.
    pub shard_id: ShardId,
    /// Stable wire ref of the region that owns the shard.
    pub region: ActorRefWireData,
}

/// Inbound bridge for shard-home replies addressed to one region endpoint.
#[derive(Clone)]
pub struct ShardCoordinatorRemoteHomeInbound {
    region: ActorRefWireData,
    registry: Arc<Registry>,
}

impl ShardCoordinatorRemoteHomeInbound {
    /// Creates a decoder for replies addressed to `region`.
    pub fn new(region: ActorRefWireData, registry: Arc<Registry>) -> Self {
        Self { region, registry }
    }

    /// Returns the only accepted region recipient.
    pub fn region(&self) -> &ActorRefWireData {
        &self.region
    }

    /// Validates and decodes one stable [`ShardHome`] envelope.
    pub fn receive(
        &self,
        envelope: RemoteEnvelope,
    ) -> Result<ShardCoordinatorRemoteHome, ShardCoordinatorRemoteHomeError> {
        if envelope.recipient != self.region {
            return Err(ShardCoordinatorRemoteHomeError::WrongRecipient {
                expected: self.region.path().to_string(),
                actual: envelope.recipient.path().to_string(),
            });
        }
        match envelope.message.manifest.as_str() {
            ShardHome::MANIFEST => {
                let home = self.registry.deserialize::<ShardHome>(envelope.message)?;
                Ok(ShardCoordinatorRemoteHome {
                    sender: envelope.sender,
                    shard_id: home.shard_id,
                    region: home.region,
                })
            }
            manifest => Err(ShardCoordinatorRemoteHomeError::UnsupportedManifest(
                manifest.to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::mpsc::{self, Receiver};

    use kairo_actor::{Recipient, SendError};
    use kairo_cluster::UniqueAddress;
    use kairo_serialization::{Manifest, RemoteMessage, SerializedMessage};

    use crate::{
        DEFAULT_SHARD_COORDINATOR_REMOTE_PATH, GET_SHARD_HOME_SERIALIZER_ID,
        SHARD_HOME_SERIALIZER_ID, ShardCoordinatorRemoteTarget, register_sharding_protocol_codecs,
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
    fn remote_home_outbound_sends_stable_get_shard_home_envelope() {
        let registry = registry();
        let (outbound, rx) = collector::<RemoteEnvelope>();
        let bridge =
            ShardCoordinatorRemoteHomeOutbound::new(target(), region(), registry.clone(), outbound);

        bridge.request_shard_home("12").unwrap();

        let envelope = rx.recv().unwrap();
        assert_eq!(envelope.recipient, target().recipient().clone());
        assert_eq!(envelope.sender, Some(region()));
        assert_eq!(envelope.message.serializer_id, GET_SHARD_HOME_SERIALIZER_ID);
        assert_eq!(envelope.message.manifest.as_str(), GetShardHome::MANIFEST);
        assert_eq!(
            registry
                .deserialize::<GetShardHome>(envelope.message)
                .unwrap(),
            GetShardHome {
                shard_id: "12".to_string()
            }
        );
    }

    #[test]
    fn remote_home_inbound_decodes_stable_shard_home_reply() {
        let registry = registry();
        let inbound = ShardCoordinatorRemoteHomeInbound::new(region(), registry.clone());
        let home = ShardHome {
            shard_id: "12".to_string(),
            region: remote_region(),
        };
        let message = registry.serialize(&home).unwrap();
        let envelope = RemoteEnvelope::new(region(), Some(target().recipient().clone()), message);

        let decoded = inbound.receive(envelope).unwrap();

        assert_eq!(decoded.sender, Some(target().recipient().clone()));
        assert_eq!(decoded.shard_id, "12");
        assert_eq!(decoded.region, remote_region());
    }

    #[test]
    fn remote_home_inbound_rejects_wrong_recipient_or_manifest() {
        let registry = registry();
        let inbound = ShardCoordinatorRemoteHomeInbound::new(region(), registry.clone());
        let wrong_recipient = RemoteEnvelope::new(
            actor_ref("kairo://local@127.0.0.1:2551/user/not-region"),
            None,
            registry
                .serialize(&ShardHome {
                    shard_id: "12".to_string(),
                    region: remote_region(),
                })
                .unwrap(),
        );

        assert!(matches!(
            inbound.receive(wrong_recipient).unwrap_err(),
            ShardCoordinatorRemoteHomeError::WrongRecipient { .. }
        ));

        let wrong_manifest = RemoteEnvelope::new(
            region(),
            None,
            SerializedMessage {
                serializer_id: SHARD_HOME_SERIALIZER_ID,
                manifest: Manifest::new("kairo.sharding.unsupported-home"),
                version: 1,
                payload: bytes::Bytes::new(),
            },
        );
        assert!(matches!(
            inbound.receive(wrong_manifest).unwrap_err(),
            ShardCoordinatorRemoteHomeError::UnsupportedManifest(_)
        ));
    }

    fn registry() -> Arc<Registry> {
        let mut registry = Registry::new();
        register_sharding_protocol_codecs(&mut registry).unwrap();
        Arc::new(registry)
    }

    fn target() -> ShardCoordinatorRemoteTarget {
        ShardCoordinatorRemoteTarget::for_node(
            UniqueAddress::new(
                kairo_actor::Address::new(
                    "kairo",
                    "remote",
                    Some("127.0.0.1".to_string()),
                    Some(2552),
                ),
                2,
            ),
            DEFAULT_SHARD_COORDINATOR_REMOTE_PATH,
        )
        .unwrap()
    }

    fn region() -> ActorRefWireData {
        actor_ref("kairo://local@127.0.0.1:2551/system/sharding/region")
    }

    fn remote_region() -> ActorRefWireData {
        actor_ref("kairo://remote@127.0.0.1:2552/system/sharding/region")
    }

    fn actor_ref(path: &str) -> ActorRefWireData {
        ActorRefWireData::new(path).unwrap()
    }
}
