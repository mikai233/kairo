#![deny(missing_docs)]

use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, Address, Context, Recipient};
use kairo_remote::RemoteOutbound;
use kairo_serialization::{ActorRefWireData, Registry, RemoteEnvelope, RemoteMessage};

use crate::{
    ApplicationVersion, ClusterSeedJoinEffect, ClusterSeedJoinProcessMsg,
    DEFAULT_CLUSTER_MEMBERSHIP_REMOTE_PATH, InitJoin, InitJoinAck, InitJoinNack, Join,
    UniqueAddress,
};

#[derive(Debug, Clone, PartialEq, Eq)]
/// Decoded initial contact paired with its validated remote origin.
pub struct ClusterInitJoinRequest {
    /// Canonical address derived from the remote daemon sender path.
    pub origin: Address,
    /// Initial contact payload from the joining node.
    pub message: InitJoin,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Terminal notification that a seed reported incompatible configuration.
pub struct ClusterSeedJoinIncompatible {
    /// Seed address that rejected the joining configuration.
    pub target: Address,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Wire reply produced by the initial-contact responder.
pub enum ClusterInitJoinResponse {
    /// The seed is initialized and can receive a membership join request.
    Ack(InitJoinAck),
    /// The seed is not currently initialized or joinable.
    Nack(InitJoinNack),
}

#[derive(Debug)]
/// Failure while encoding, validating, or delivering seed-join traffic.
pub enum ClusterSeedJoinWireError {
    /// The claimed sender was not the cluster daemon path at its address.
    InvalidSenderPath(String),
    /// An address without both a remote host and port was used for seed traffic.
    MissingRemoteHost {
        /// Rejected address formatted for diagnostics.
        address: String,
    },
    /// An inbound seed message omitted its remote sender identity.
    MissingSender,
    /// A remote outbound or local actor recipient rejected delivery.
    Send {
        /// Remote address or local handler description.
        target: String,
        /// Error reported by the delivery boundary.
        reason: String,
    },
    /// The registry or actor-reference wire format rejected serialization.
    Serialization(kairo_serialization::SerializationError),
    /// The inbound manifest is not an initial seed-contact message.
    UnsupportedManifest(String),
}

impl Display for ClusterSeedJoinWireError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSenderPath(path) => {
                write!(
                    f,
                    "cluster seed message has invalid daemon sender path `{path}`"
                )
            }
            Self::MissingRemoteHost { address } => {
                write!(f, "cluster seed target {address} has no remote host")
            }
            Self::MissingSender => write!(f, "cluster seed message has no remote sender"),
            Self::Send { target, reason } => {
                write!(f, "cluster seed delivery to {target} failed: {reason}")
            }
            Self::Serialization(error) => write!(f, "{error}"),
            Self::UnsupportedManifest(manifest) => {
                write!(f, "unsupported cluster seed manifest `{manifest}`")
            }
        }
    }
}

impl std::error::Error for ClusterSeedJoinWireError {}

impl From<kairo_serialization::SerializationError> for ClusterSeedJoinWireError {
    fn from(error: kairo_serialization::SerializationError) -> Self {
        Self::Serialization(error)
    }
}

#[derive(Clone)]
/// Executes seed-join effects across local membership and remote daemon boundaries.
///
/// Contact, join, ack, and nack messages use registry serialization and carry
/// the local cluster daemon as sender. Self-join and incompatibility effects
/// remain typed local actor deliveries.
pub struct ClusterSeedJoinWireOutbound {
    self_node: UniqueAddress,
    roles: Vec<String>,
    app_version: ApplicationVersion,
    registry: Arc<Registry>,
    outbound: Arc<dyn RemoteOutbound>,
    membership: ActorRef<crate::ClusterMembershipMsg>,
    incompatible: Arc<dyn Recipient<ClusterSeedJoinIncompatible> + Send + Sync>,
}

impl ClusterSeedJoinWireOutbound {
    /// Creates a seed-join effect executor for the local node incarnation.
    pub fn new(
        self_node: UniqueAddress,
        roles: Vec<String>,
        registry: Arc<Registry>,
        outbound: Arc<dyn RemoteOutbound>,
        membership: ActorRef<crate::ClusterMembershipMsg>,
        incompatible: impl Recipient<ClusterSeedJoinIncompatible> + Send + Sync + 'static,
    ) -> Self {
        Self {
            self_node,
            roles,
            app_version: ApplicationVersion::default(),
            registry,
            outbound,
            membership,
            incompatible: Arc::new(incompatible),
        }
    }

    /// Sets the application version advertised by outgoing join requests.
    pub fn with_app_version(mut self, app_version: ApplicationVersion) -> Self {
        self.app_version = app_version;
        self
    }

    /// Executes one effect emitted by [`crate::ClusterSeedJoinState`].
    pub fn send_effect(
        &self,
        effect: ClusterSeedJoinEffect,
    ) -> Result<(), ClusterSeedJoinWireError> {
        match effect {
            ClusterSeedJoinEffect::Contact { target, message } => {
                self.send_remote(&target, &message)
            }
            ClusterSeedJoinEffect::Join { target } => self.send_remote(
                &target,
                &Join {
                    node: self.self_node.clone(),
                    roles: self.roles.clone(),
                    app_version: self.app_version.clone(),
                },
            ),
            ClusterSeedJoinEffect::JoinSelf => self
                .membership
                .tell(crate::ClusterMembershipMsg::JoinSelf)
                .map_err(|error| ClusterSeedJoinWireError::Send {
                    target: self.self_node.ordering_key(),
                    reason: error.reason().to_string(),
                }),
            ClusterSeedJoinEffect::RejectIncompatible { target } => self
                .incompatible
                .tell(ClusterSeedJoinIncompatible {
                    target: target.clone(),
                })
                .map_err(|error| ClusterSeedJoinWireError::Send {
                    target: target.to_string(),
                    reason: error.reason().to_string(),
                }),
        }
    }

    /// Sends an initial-contact acknowledgement or nack to `target`.
    pub fn send_init_join_response(
        &self,
        target: &Address,
        response: ClusterInitJoinResponse,
    ) -> Result<(), ClusterSeedJoinWireError> {
        match response {
            ClusterInitJoinResponse::Ack(message) => self.send_remote(target, &message),
            ClusterInitJoinResponse::Nack(message) => self.send_remote(target, &message),
        }
    }

    fn send_remote<M>(&self, target: &Address, message: &M) -> Result<(), ClusterSeedJoinWireError>
    where
        M: RemoteMessage,
    {
        require_remote(target)?;
        require_remote(&self.self_node.address)?;
        let recipient = daemon_wire(target)?;
        let sender = daemon_wire(&self.self_node.address)?;
        let envelope =
            RemoteEnvelope::new(recipient, Some(sender), self.registry.serialize(message)?);
        self.outbound
            .send(envelope)
            .map_err(|error| ClusterSeedJoinWireError::Send {
                target: target.to_string(),
                reason: error.to_string(),
            })
    }
}

/// Actor wrapper for [`ClusterSeedJoinWireOutbound`].
pub struct ClusterSeedJoinWireOutboundActor {
    outbound: ClusterSeedJoinWireOutbound,
}

impl ClusterSeedJoinWireOutboundActor {
    /// Creates an actor around `outbound`.
    pub fn new(outbound: ClusterSeedJoinWireOutbound) -> Self {
        Self { outbound }
    }
}

impl Actor for ClusterSeedJoinWireOutboundActor {
    type Msg = ClusterSeedJoinEffect;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        self.outbound
            .send_effect(msg)
            .map_err(|error| ActorError::Message(error.to_string()))
    }
}

#[derive(Clone)]
/// Validates and dispatches inbound initial seed-contact envelopes.
///
/// The remote sender must identify the membership daemon at a canonical remote
/// address. Requests go to the responder; acknowledgements and nacks go to the
/// active seed-join process with the validated origin attached.
pub struct ClusterSeedJoinWireInbound {
    registry: Arc<Registry>,
    process: ActorRef<ClusterSeedJoinProcessMsg>,
    init_join: Arc<dyn Recipient<ClusterInitJoinRequest> + Send + Sync>,
}

impl ClusterSeedJoinWireInbound {
    /// Creates an inbound adapter for one seed-join process and request handler.
    pub fn new(
        registry: Arc<Registry>,
        process: ActorRef<ClusterSeedJoinProcessMsg>,
        init_join: impl Recipient<ClusterInitJoinRequest> + Send + Sync + 'static,
    ) -> Self {
        Self {
            registry,
            process,
            init_join: Arc::new(init_join),
        }
    }

    /// Validates, decodes, and dispatches one remote seed-contact envelope.
    pub fn receive(&self, envelope: RemoteEnvelope) -> Result<(), ClusterSeedJoinWireError> {
        let origin = sender_address(envelope.sender.as_ref())?;
        match envelope.message.manifest.as_str() {
            InitJoin::MANIFEST => {
                let message = self.registry.deserialize::<InitJoin>(envelope.message)?;
                self.init_join
                    .tell(ClusterInitJoinRequest { origin, message })
                    .map_err(|error| ClusterSeedJoinWireError::Send {
                        target: "local init-join handler".to_string(),
                        reason: error.reason().to_string(),
                    })
            }
            InitJoinAck::MANIFEST => {
                let message = self.registry.deserialize::<InitJoinAck>(envelope.message)?;
                self.process
                    .tell(ClusterSeedJoinProcessMsg::Ack { origin, message })
                    .map_err(|error| ClusterSeedJoinWireError::Send {
                        target: "local seed-join process".to_string(),
                        reason: error.reason().to_string(),
                    })
            }
            InitJoinNack::MANIFEST => {
                let message = self
                    .registry
                    .deserialize::<InitJoinNack>(envelope.message)?;
                self.process
                    .tell(ClusterSeedJoinProcessMsg::Nack { origin, message })
                    .map_err(|error| ClusterSeedJoinWireError::Send {
                        target: "local seed-join process".to_string(),
                        reason: error.reason().to_string(),
                    })
            }
            manifest => Err(ClusterSeedJoinWireError::UnsupportedManifest(
                manifest.to_string(),
            )),
        }
    }
}

fn require_remote(address: &Address) -> Result<(), ClusterSeedJoinWireError> {
    if address.host().is_none() || address.port().is_none() {
        return Err(ClusterSeedJoinWireError::MissingRemoteHost {
            address: address.to_string(),
        });
    }
    Ok(())
}

fn daemon_wire(address: &Address) -> Result<ActorRefWireData, ClusterSeedJoinWireError> {
    Ok(ActorRefWireData::new(format!(
        "{address}{DEFAULT_CLUSTER_MEMBERSHIP_REMOTE_PATH}"
    ))?)
}

fn sender_address(sender: Option<&ActorRefWireData>) -> Result<Address, ClusterSeedJoinWireError> {
    let sender = sender.ok_or(ClusterSeedJoinWireError::MissingSender)?;
    let address = Address::new(
        sender.protocol(),
        sender.system(),
        sender.host().map(str::to_string),
        sender.port(),
    );
    require_remote(&address)?;
    if daemon_wire(&address)?.path() != sender.path() {
        return Err(ClusterSeedJoinWireError::InvalidSenderPath(
            sender.path().to_string(),
        ));
    }
    Ok(address)
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::time::Duration;

    use bytes::Bytes;
    use kairo_remote::Result as RemoteResult;
    use kairo_testkit::ActorSystemTestKit;

    use super::*;
    use crate::{
        ClusterConfigCheck, ClusterMembershipMsg, INIT_JOIN_SERIALIZER_ID, JOIN_SERIALIZER_ID,
        register_cluster_control_codecs,
    };

    #[derive(Default)]
    struct CollectingOutbound {
        envelopes: Mutex<Vec<RemoteEnvelope>>,
    }

    impl CollectingOutbound {
        fn envelopes(&self) -> Vec<RemoteEnvelope> {
            self.envelopes.lock().unwrap().clone()
        }
    }

    impl RemoteOutbound for CollectingOutbound {
        fn send(&self, envelope: RemoteEnvelope) -> RemoteResult<()> {
            self.envelopes.lock().unwrap().push(envelope);
            Ok(())
        }
    }

    fn registry() -> Arc<Registry> {
        let mut registry = Registry::new();
        register_cluster_control_codecs(&mut registry).unwrap();
        Arc::new(registry)
    }

    fn address(system: &str, port: u16) -> Address {
        Address::new("kairo", system, Some("127.0.0.1".to_string()), Some(port))
    }

    #[test]
    fn effect_outbound_routes_remote_and_local_seed_actions() {
        let kit = ActorSystemTestKit::new("seed-wire-effects").unwrap();
        let membership = kit
            .create_probe::<ClusterMembershipMsg>("membership")
            .unwrap();
        let incompatible = kit
            .create_probe::<ClusterSeedJoinIncompatible>("incompatible")
            .unwrap();
        let collected = Arc::new(CollectingOutbound::default());
        let self_node = UniqueAddress::new(address("joining", 2551), 11);
        let seed = address("seed", 2552);
        let app_version = crate::ApplicationVersion::new("5.1.0").unwrap();
        let registry = registry();
        let outbound = ClusterSeedJoinWireOutbound::new(
            self_node.clone(),
            vec!["backend".to_string()],
            registry.clone(),
            collected.clone(),
            membership.actor_ref(),
            incompatible.actor_ref(),
        )
        .with_app_version(app_version.clone());

        outbound
            .send_effect(ClusterSeedJoinEffect::Contact {
                target: seed.clone(),
                message: InitJoin {
                    joining_config_digest: Bytes::from_static(b"digest"),
                },
            })
            .unwrap();
        outbound
            .send_effect(ClusterSeedJoinEffect::Join {
                target: seed.clone(),
            })
            .unwrap();
        outbound
            .send_effect(ClusterSeedJoinEffect::JoinSelf)
            .unwrap();
        assert!(matches!(
            membership.expect_msg(Duration::from_secs(1)).unwrap(),
            ClusterMembershipMsg::JoinSelf
        ));
        outbound
            .send_effect(ClusterSeedJoinEffect::RejectIncompatible {
                target: seed.clone(),
            })
            .unwrap();
        assert_eq!(
            incompatible.expect_msg(Duration::from_secs(1)).unwrap(),
            ClusterSeedJoinIncompatible {
                target: seed.clone()
            }
        );

        let envelopes = collected.envelopes();
        assert_eq!(envelopes.len(), 2);
        assert_eq!(envelopes[0].message.serializer_id, INIT_JOIN_SERIALIZER_ID);
        assert_eq!(envelopes[1].message.serializer_id, JOIN_SERIALIZER_ID);
        assert_eq!(
            envelopes[0].recipient.path(),
            format!("{seed}{DEFAULT_CLUSTER_MEMBERSHIP_REMOTE_PATH}")
        );
        assert_eq!(
            envelopes[0].sender.as_ref().unwrap().path(),
            format!(
                "{}{DEFAULT_CLUSTER_MEMBERSHIP_REMOTE_PATH}",
                self_node.address
            )
        );
        assert_eq!(
            registry
                .deserialize::<InitJoin>(envelopes[0].message.clone())
                .unwrap()
                .joining_config_digest,
            Bytes::from_static(b"digest")
        );
        assert_eq!(
            registry
                .deserialize::<Join>(envelopes[1].message.clone())
                .unwrap(),
            Join {
                node: self_node,
                roles: vec!["backend".to_string()],
                app_version,
            }
        );
        kit.shutdown(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn inbound_uses_validated_sender_address_for_seed_messages() {
        let kit = ActorSystemTestKit::new("seed-wire-inbound").unwrap();
        let process = kit
            .create_probe::<ClusterSeedJoinProcessMsg>("process")
            .unwrap();
        let init_join = kit
            .create_probe::<ClusterInitJoinRequest>("init-join")
            .unwrap();
        let registry = registry();
        let inbound = ClusterSeedJoinWireInbound::new(
            registry.clone(),
            process.actor_ref(),
            init_join.actor_ref(),
        );
        let origin = address("seed", 2552);
        let recipient = daemon_wire(&address("joining", 2551)).unwrap();
        let sender = Some(daemon_wire(&origin).unwrap());

        inbound
            .receive(RemoteEnvelope::new(
                recipient.clone(),
                sender.clone(),
                registry
                    .serialize(&InitJoin {
                        joining_config_digest: Bytes::from_static(b"digest"),
                    })
                    .unwrap(),
            ))
            .unwrap();
        assert_eq!(
            init_join.expect_msg(Duration::from_secs(1)).unwrap(),
            ClusterInitJoinRequest {
                origin: origin.clone(),
                message: InitJoin {
                    joining_config_digest: Bytes::from_static(b"digest")
                }
            }
        );

        inbound
            .receive(RemoteEnvelope::new(
                recipient,
                sender,
                registry
                    .serialize(&InitJoinAck {
                        address: address("canonical", 2553),
                        config_check: ClusterConfigCheck::Compatible,
                    })
                    .unwrap(),
            ))
            .unwrap();
        assert!(matches!(
            process.expect_msg(Duration::from_secs(1)).unwrap(),
            ClusterSeedJoinProcessMsg::Ack { origin: actual, message }
                if actual == origin && message.address == address("canonical", 2553)
        ));
        kit.shutdown(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn inbound_rejects_missing_or_non_daemon_sender() {
        let kit = ActorSystemTestKit::new("seed-wire-invalid-sender").unwrap();
        let process = kit
            .create_probe::<ClusterSeedJoinProcessMsg>("process")
            .unwrap();
        let init_join = kit
            .create_probe::<ClusterInitJoinRequest>("init-join")
            .unwrap();
        let registry = registry();
        let inbound = ClusterSeedJoinWireInbound::new(
            registry.clone(),
            process.actor_ref(),
            init_join.actor_ref(),
        );
        let message = registry
            .serialize(&InitJoinNack {
                address: address("seed", 2552),
            })
            .unwrap();
        let recipient = daemon_wire(&address("joining", 2551)).unwrap();

        assert!(matches!(
            inbound.receive(RemoteEnvelope::new(
                recipient.clone(),
                None,
                message.clone()
            )),
            Err(ClusterSeedJoinWireError::MissingSender)
        ));
        let wrong_sender =
            ActorRefWireData::new(format!("{}/user/not-daemon", address("seed", 2552))).unwrap();
        assert!(matches!(
            inbound.receive(RemoteEnvelope::new(recipient, Some(wrong_sender), message)),
            Err(ClusterSeedJoinWireError::InvalidSenderPath(_))
        ));
        kit.shutdown(Duration::from_secs(1)).unwrap();
    }
}
