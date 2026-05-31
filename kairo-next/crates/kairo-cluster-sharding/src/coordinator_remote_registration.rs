use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use kairo_actor::{Recipient, SendError};
use kairo_serialization::{
    ActorRefWireData, Registry, RemoteEnvelope, RemoteMessage, SerializationError,
};

use crate::{Register, RegisterAck, ShardCoordinatorRemoteTarget};

#[derive(Debug)]
pub enum ShardCoordinatorRemoteRegistrationError {
    Serialization(SerializationError),
    Send { target: String, reason: String },
    WrongRecipient { expected: String, actual: String },
    UnsupportedManifest(String),
}

impl Display for ShardCoordinatorRemoteRegistrationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Serialization(error) => {
                write!(
                    f,
                    "shard-coordinator remote registration serialization failed: {error}"
                )
            }
            Self::Send { target, reason } => {
                write!(
                    f,
                    "shard-coordinator remote registration send to `{target}` failed: {reason}"
                )
            }
            Self::WrongRecipient { expected, actual } => {
                write!(
                    f,
                    "shard-coordinator remote registration reply was addressed to {actual}, expected {expected}"
                )
            }
            Self::UnsupportedManifest(manifest) => {
                write!(
                    f,
                    "unsupported shard-coordinator remote registration manifest `{manifest}`"
                )
            }
        }
    }
}

impl std::error::Error for ShardCoordinatorRemoteRegistrationError {}

impl From<SerializationError> for ShardCoordinatorRemoteRegistrationError {
    fn from(error: SerializationError) -> Self {
        Self::Serialization(error)
    }
}

#[derive(Clone)]
pub struct ShardCoordinatorRemoteRegistrationOutbound {
    target: ShardCoordinatorRemoteTarget,
    region: ActorRefWireData,
    sender: Option<ActorRefWireData>,
    registry: Arc<Registry>,
    outbound: Arc<dyn Recipient<RemoteEnvelope> + Send + Sync>,
}

impl ShardCoordinatorRemoteRegistrationOutbound {
    pub fn new(
        target: ShardCoordinatorRemoteTarget,
        region: ActorRefWireData,
        registry: Arc<Registry>,
        outbound: impl Recipient<RemoteEnvelope> + Send + Sync + 'static,
    ) -> Self {
        Self::from_arc(target, region, registry, Arc::new(outbound))
    }

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

    pub fn target(&self) -> &ShardCoordinatorRemoteTarget {
        &self.target
    }

    pub fn region(&self) -> &ActorRefWireData {
        &self.region
    }

    pub fn sender(&self) -> Option<&ActorRefWireData> {
        self.sender.as_ref()
    }

    pub fn with_sender(mut self, sender: Option<ActorRefWireData>) -> Self {
        self.sender = sender;
        self
    }

    pub fn register(&self) -> Result<(), ShardCoordinatorRemoteRegistrationError> {
        let register = Register {
            region: self.region.clone(),
        };
        let message = self.registry.serialize(&register)?;
        let envelope = RemoteEnvelope::new(
            self.target.recipient().clone(),
            self.sender.clone(),
            message,
        );
        self.outbound.tell(envelope).map_err(|error| {
            ShardCoordinatorRemoteRegistrationError::Send {
                target: self.target.recipient().path().to_string(),
                reason: error.reason().to_string(),
            }
        })
    }
}

impl Recipient<ShardCoordinatorRemoteTarget> for ShardCoordinatorRemoteRegistrationOutbound {
    fn tell(
        &self,
        message: ShardCoordinatorRemoteTarget,
    ) -> Result<(), SendError<ShardCoordinatorRemoteTarget>> {
        if message != self.target {
            return Err(SendError::new(
                message,
                "remote registration target does not match outbound target",
            ));
        }
        self.register()
            .map_err(|error| SendError::new(message, error.to_string()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardCoordinatorRemoteRegistrationAck {
    pub sender: Option<ActorRefWireData>,
    pub coordinator: ActorRefWireData,
}

#[derive(Clone)]
pub struct ShardCoordinatorRemoteRegistrationInbound {
    region: ActorRefWireData,
    registry: Arc<Registry>,
}

impl ShardCoordinatorRemoteRegistrationInbound {
    pub fn new(region: ActorRefWireData, registry: Arc<Registry>) -> Self {
        Self { region, registry }
    }

    pub fn region(&self) -> &ActorRefWireData {
        &self.region
    }

    pub fn receive(
        &self,
        envelope: RemoteEnvelope,
    ) -> Result<ShardCoordinatorRemoteRegistrationAck, ShardCoordinatorRemoteRegistrationError>
    {
        if envelope.recipient != self.region {
            return Err(ShardCoordinatorRemoteRegistrationError::WrongRecipient {
                expected: self.region.path().to_string(),
                actual: envelope.recipient.path().to_string(),
            });
        }
        match envelope.message.manifest.as_str() {
            RegisterAck::MANIFEST => {
                let ack = self.registry.deserialize::<RegisterAck>(envelope.message)?;
                Ok(ShardCoordinatorRemoteRegistrationAck {
                    sender: envelope.sender,
                    coordinator: ack.coordinator,
                })
            }
            manifest => Err(
                ShardCoordinatorRemoteRegistrationError::UnsupportedManifest(manifest.to_string()),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc::{self, Receiver};
    use std::time::Duration;

    use kairo_actor::{Actor, ActorRef, ActorResult, ActorSystem, Context, Props};
    use kairo_cluster::UniqueAddress;
    use kairo_serialization::{Manifest, RemoteMessage, SerializedMessage};

    use crate::{
        DEFAULT_SHARD_COORDINATOR_REMOTE_PATH, REGISTER_ACK_SERIALIZER_ID, REGISTER_SERIALIZER_ID,
        ShardCoordinatorRemoteTarget, register_sharding_protocol_codecs,
    };

    use super::*;

    struct Forward<M> {
        tx: mpsc::Sender<M>,
    }

    impl<M> Actor for Forward<M>
    where
        M: Send + 'static,
    {
        type Msg = M;

        fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
            self.tx
                .send(msg)
                .map_err(|error| kairo_actor::ActorError::Message(error.to_string()))
        }
    }

    fn probe<M>(system: &ActorSystem, name: &str) -> (ActorRef<M>, Receiver<M>)
    where
        M: Send + 'static,
    {
        let (tx, rx) = mpsc::channel();
        let actor = system
            .spawn(name, Props::new(move || Forward { tx }))
            .unwrap();
        (actor, rx)
    }

    #[test]
    fn remote_registration_outbound_sends_stable_register_envelope() {
        let system = ActorSystem::builder("sharding-remote-register-out")
            .build()
            .unwrap();
        let registry = registry();
        let (outbound_ref, outbound_rx) = probe::<RemoteEnvelope>(&system, "remote-out");
        let outbound = ShardCoordinatorRemoteRegistrationOutbound::new(
            target(),
            region(),
            registry.clone(),
            outbound_ref,
        );

        outbound.register().unwrap();

        let envelope = outbound_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(envelope.recipient, target().recipient().clone());
        assert_eq!(envelope.sender, Some(region()));
        assert_eq!(envelope.message.serializer_id, REGISTER_SERIALIZER_ID);
        assert_eq!(envelope.message.manifest.as_str(), Register::MANIFEST);
        assert_eq!(
            registry.deserialize::<Register>(envelope.message).unwrap(),
            Register { region: region() }
        );
        system.terminate(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn remote_registration_inbound_decodes_stable_register_ack() {
        let registry = registry();
        let inbound = ShardCoordinatorRemoteRegistrationInbound::new(region(), registry.clone());
        let ack = RegisterAck {
            coordinator: target().recipient().clone(),
        };
        let message = registry.serialize(&ack).unwrap();
        let envelope = RemoteEnvelope::new(region(), Some(target().recipient().clone()), message);

        let decoded = inbound.receive(envelope).unwrap();

        assert_eq!(decoded.sender, Some(target().recipient().clone()));
        assert_eq!(decoded.coordinator, target().recipient().clone());
    }

    #[test]
    fn remote_registration_inbound_rejects_wrong_recipient_or_manifest() {
        let registry = registry();
        let inbound = ShardCoordinatorRemoteRegistrationInbound::new(region(), registry.clone());
        let wrong_recipient = RemoteEnvelope::new(
            actor_ref("kairo://local@127.0.0.1:2551/user/not-region"),
            None,
            registry
                .serialize(&RegisterAck {
                    coordinator: region(),
                })
                .unwrap(),
        );

        assert!(matches!(
            inbound.receive(wrong_recipient).unwrap_err(),
            ShardCoordinatorRemoteRegistrationError::WrongRecipient { .. }
        ));

        let wrong_manifest = RemoteEnvelope::new(
            region(),
            None,
            SerializedMessage {
                serializer_id: REGISTER_ACK_SERIALIZER_ID,
                manifest: Manifest::new("kairo.sharding.unsupported"),
                version: 1,
                payload: bytes::Bytes::new(),
            },
        );
        assert!(matches!(
            inbound.receive(wrong_manifest).unwrap_err(),
            ShardCoordinatorRemoteRegistrationError::UnsupportedManifest(_)
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

    fn actor_ref(path: &str) -> ActorRefWireData {
        ActorRefWireData::new(path).unwrap()
    }
}
