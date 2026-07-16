#![deny(missing_docs)]

use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use kairo_actor::{ActorRef, Recipient, SendError};
use kairo_cluster::UniqueAddress;
use kairo_serialization::{Registry, RemoteMessage, SerializationError, SerializedMessage};

use crate::{PubSubDelta, PubSubStatus};

use super::gossip::PubSubGossipMsg;

/// Serialized pubsub gossip payload paired with its exact destination member.
///
/// This intermediate value keeps registry serialization separate from the
/// remoting adapter that constructs the canonical `RemoteEnvelope`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubSubSerializedGossip {
    /// Exact cluster-member incarnation that should receive the payload.
    pub target: UniqueAddress,
    /// Stable status or delta wire message.
    pub message: SerializedMessage,
}

impl PubSubSerializedGossip {
    /// Creates a serialized gossip payload for one exact destination.
    pub fn new(target: UniqueAddress, message: SerializedMessage) -> Self {
        Self { target, message }
    }
}

/// Failure while encoding, validating, or delivering pubsub gossip.
#[derive(Debug)]
pub enum PubSubGossipWireError {
    /// Stable message serialization or deserialization failed.
    Serialization(SerializationError),
    /// The local gossip actor rejected a decoded message.
    Send(String),
    /// The inbound serialized message did not use a status or delta manifest.
    UnsupportedManifest(String),
    /// The outer gossip destination did not match this member incarnation.
    WrongTarget {
        /// Ordering key of the receiving member incarnation.
        expected: String,
        /// Ordering key carried by the outer gossip value.
        actual: String,
    },
}

impl Display for PubSubGossipWireError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Serialization(error) => write!(f, "pubsub gossip serialization failed: {error}"),
            Self::Send(reason) => write!(f, "pubsub gossip delivery failed: {reason}"),
            Self::UnsupportedManifest(manifest) => {
                write!(f, "unsupported pubsub gossip manifest `{manifest}`")
            }
            Self::WrongTarget { expected, actual } => {
                write!(
                    f,
                    "pubsub gossip message was addressed to {}, expected {}",
                    actual, expected
                )
            }
        }
    }
}

impl std::error::Error for PubSubGossipWireError {}

impl From<SerializationError> for PubSubGossipWireError {
    fn from(error: SerializationError) -> Self {
        Self::Serialization(error)
    }
}

/// Outbound adapter from actor-local gossip commands to serialized payloads.
///
/// Only status and delta commands cross this boundary. Peer management,
/// registry mutation, ticks, and snapshots remain local actor messages.
#[derive(Clone)]
pub struct PubSubGossipWireOutbound {
    target: UniqueAddress,
    registry: Arc<Registry>,
    outbound: Arc<dyn Recipient<PubSubSerializedGossip> + Send + Sync>,
}

impl PubSubGossipWireOutbound {
    /// Creates an adapter for one exact destination and concrete payload sink.
    pub fn new(
        target: UniqueAddress,
        registry: Arc<Registry>,
        outbound: impl Recipient<PubSubSerializedGossip> + Send + Sync + 'static,
    ) -> Self {
        Self {
            target,
            registry,
            outbound: Arc::new(outbound),
        }
    }

    /// Creates an adapter for one exact destination and shared payload sink.
    pub fn from_arc(
        target: UniqueAddress,
        registry: Arc<Registry>,
        outbound: Arc<dyn Recipient<PubSubSerializedGossip> + Send + Sync>,
    ) -> Self {
        Self {
            target,
            registry,
            outbound,
        }
    }

    /// Returns the exact destination member incarnation.
    pub fn target(&self) -> &UniqueAddress {
        &self.target
    }
}

impl Recipient<PubSubGossipMsg> for PubSubGossipWireOutbound {
    fn tell(&self, message: PubSubGossipMsg) -> Result<(), SendError<PubSubGossipMsg>> {
        match message {
            PubSubGossipMsg::Status {
                from,
                versions,
                reply,
            } => {
                let wire = PubSubStatus {
                    from: from.clone(),
                    versions: versions.clone(),
                    reply,
                };
                let serialized = self.registry.serialize(&wire).map_err(|error| {
                    SendError::new(
                        PubSubGossipMsg::Status {
                            from: from.clone(),
                            versions: versions.clone(),
                            reply,
                        },
                        error.to_string(),
                    )
                })?;
                self.outbound
                    .tell(PubSubSerializedGossip::new(self.target.clone(), serialized))
                    .map_err(|error| {
                        SendError::new(
                            PubSubGossipMsg::Status {
                                from,
                                versions,
                                reply,
                            },
                            error.reason().to_string(),
                        )
                    })
            }
            PubSubGossipMsg::Delta { from, delta } => {
                let wire = PubSubDelta {
                    from: from.clone(),
                    delta: delta.clone(),
                };
                let serialized = self.registry.serialize(&wire).map_err(|error| {
                    SendError::new(
                        PubSubGossipMsg::Delta {
                            from: from.clone(),
                            delta: delta.clone(),
                        },
                        error.to_string(),
                    )
                })?;
                self.outbound
                    .tell(PubSubSerializedGossip::new(self.target.clone(), serialized))
                    .map_err(|error| {
                        SendError::new(
                            PubSubGossipMsg::Delta { from, delta },
                            error.reason().to_string(),
                        )
                    })
            }
            other => Err(SendError::new(
                other,
                "pubsub gossip wire outbound only supports status and delta messages",
            )),
        }
    }
}

/// Inbound adapter from serialized status/delta payloads to a gossip actor.
///
/// The adapter validates exact destination identity before decoding the stable
/// manifest and preserves actor-mailbox ordering on delivery.
#[derive(Clone)]
pub struct PubSubGossipWireInbound {
    self_node: UniqueAddress,
    registry: Arc<Registry>,
    gossip: ActorRef<PubSubGossipMsg>,
}

impl PubSubGossipWireInbound {
    /// Creates an inbound adapter for this member and gossip actor.
    pub fn new(
        self_node: UniqueAddress,
        registry: Arc<Registry>,
        gossip: ActorRef<PubSubGossipMsg>,
    ) -> Self {
        Self {
            self_node,
            registry,
            gossip,
        }
    }

    /// Validates the outer destination, decodes the payload, and tells gossip.
    pub fn receive(&self, envelope: PubSubSerializedGossip) -> Result<(), PubSubGossipWireError> {
        if envelope.target != self.self_node {
            return Err(PubSubGossipWireError::WrongTarget {
                expected: self.self_node.ordering_key(),
                actual: envelope.target.ordering_key(),
            });
        }
        self.receive_message(envelope.message)
    }

    /// Decodes a payload and tells gossip without outer-destination validation.
    ///
    /// Use this only when a path-indexed remoting router has already validated
    /// the canonical recipient. Direct callers should prefer [`Self::receive`].
    pub fn receive_message(&self, message: SerializedMessage) -> Result<(), PubSubGossipWireError> {
        match message.manifest.as_str() {
            PubSubStatus::MANIFEST => {
                let status = self.registry.deserialize::<PubSubStatus>(message)?;
                self.gossip
                    .tell(PubSubGossipMsg::Status {
                        from: status.from,
                        versions: status.versions,
                        reply: status.reply,
                    })
                    .map_err(|error| PubSubGossipWireError::Send(error.reason().to_string()))
            }
            PubSubDelta::MANIFEST => {
                let delta = self.registry.deserialize::<PubSubDelta>(message)?;
                self.gossip
                    .tell(PubSubGossipMsg::Delta {
                        from: delta.from,
                        delta: delta.delta,
                    })
                    .map_err(|error| PubSubGossipWireError::Send(error.reason().to_string()))
            }
            manifest => Err(PubSubGossipWireError::UnsupportedManifest(
                manifest.to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use kairo_actor::{Address, Props};
    use kairo_serialization::{Manifest, RemoteMessage};
    use kairo_testkit::ActorSystemTestKit;

    use super::*;
    use crate::{
        PUBSUB_DELTA_SERIALIZER_ID, PUBSUB_STATUS_SERIALIZER_ID, PubSubGossipActor,
        PubSubGossipPeer, PubSubRegistryState, TopicName, register_cluster_tools_protocol_codecs,
    };

    fn registry() -> Arc<Registry> {
        let mut registry = Registry::new();
        register_cluster_tools_protocol_codecs(&mut registry).unwrap();
        Arc::new(registry)
    }

    fn node(system: &str, uid: u64) -> UniqueAddress {
        UniqueAddress::new(
            Address::new("kairo", system, Some("127.0.0.1".to_string()), Some(25520)),
            uid,
        )
    }

    #[test]
    fn wire_outbound_serializes_status_and_delta_for_target_node() {
        let kit = ActorSystemTestKit::new("pubsub-wire-outbound").unwrap();
        let registry = registry();
        let node_a = node("a", 1);
        let node_b = node("b", 2);
        let outbound_probe = kit
            .create_probe::<PubSubSerializedGossip>("wire-out")
            .unwrap();
        let outbound = PubSubGossipWireOutbound::new(
            node_b.clone(),
            registry.clone(),
            outbound_probe.actor_ref(),
        );

        outbound
            .tell(PubSubGossipMsg::Status {
                from: node_a.clone(),
                versions: BTreeMap::from([(node_a.ordering_key(), 7)]),
                reply: true,
            })
            .unwrap();
        let status_envelope = outbound_probe
            .expect_msg(Duration::from_millis(500))
            .unwrap();
        assert_eq!(status_envelope.target, node_b);
        assert_eq!(
            status_envelope.message.serializer_id,
            PUBSUB_STATUS_SERIALIZER_ID
        );
        let status = registry
            .deserialize::<PubSubStatus>(status_envelope.message)
            .unwrap();
        assert_eq!(status.from, node_a);
        assert_eq!(status.versions.values().copied().collect::<Vec<_>>(), [7]);
        assert!(status.reply);

        let mut state = PubSubRegistryState::new(node("a", 1));
        state.register_local_topic(TopicName::new("orders"));
        let delta = state.collect_delta(&BTreeMap::new(), 10);
        outbound
            .tell(PubSubGossipMsg::Delta {
                from: node("a", 1),
                delta: delta.clone(),
            })
            .unwrap();
        let delta_envelope = outbound_probe
            .expect_msg(Duration::from_millis(500))
            .unwrap();
        assert_eq!(
            delta_envelope.message.serializer_id,
            PUBSUB_DELTA_SERIALIZER_ID
        );
        assert_eq!(
            registry
                .deserialize::<PubSubDelta>(delta_envelope.message)
                .unwrap()
                .delta,
            delta
        );
        kit.shutdown(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn wire_inbound_delivers_status_to_gossip_actor() {
        let kit = ActorSystemTestKit::new("pubsub-wire-status-in").unwrap();
        let registry = registry();
        let node_a = node("a", 1);
        let node_b = node("b", 2);
        let peer_b = kit.create_probe::<PubSubGossipMsg>("peer-b").unwrap();
        let gossip_node = node_a.clone();
        let gossip = kit
            .system()
            .spawn(
                "gossip",
                Props::new(move || PubSubGossipActor::new(gossip_node)),
            )
            .unwrap();
        gossip
            .tell(PubSubGossipMsg::AddPeer {
                peer: PubSubGossipPeer::new(node_b.clone(), peer_b.actor_ref()),
            })
            .unwrap();
        gossip
            .tell(PubSubGossipMsg::RegisterTopic {
                topic: TopicName::new("orders"),
            })
            .unwrap();

        let inbound = PubSubGossipWireInbound::new(node_a.clone(), registry.clone(), gossip);
        let status = PubSubStatus {
            from: node_b,
            versions: BTreeMap::new(),
            reply: false,
        };
        inbound
            .receive(PubSubSerializedGossip::new(
                node_a,
                registry.serialize(&status).unwrap(),
            ))
            .unwrap();

        match peer_b.expect_msg(Duration::from_millis(500)).unwrap() {
            PubSubGossipMsg::Delta { from, delta } => {
                assert_eq!(from, node("a", 1));
                assert_eq!(delta.buckets.len(), 1);
            }
            _ => panic!("expected status to produce a delta reply"),
        }
        kit.shutdown(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn wire_inbound_delivers_delta_to_gossip_actor() {
        let kit = ActorSystemTestKit::new("pubsub-wire-delta-in").unwrap();
        let registry = registry();
        let node_a = node("a", 1);
        let node_b = node("b", 2);
        let peer_b = kit.create_probe::<PubSubGossipMsg>("peer-b").unwrap();
        let registry_probe = kit.create_probe::<PubSubRegistryState>("registry").unwrap();
        let gossip_node = node_a.clone();
        let gossip = kit
            .system()
            .spawn(
                "gossip",
                Props::new(move || PubSubGossipActor::new(gossip_node)),
            )
            .unwrap();
        let jobs = TopicName::new("jobs");
        let mut remote_registry = PubSubRegistryState::new(node_b.clone());
        remote_registry.register_local_group(jobs.clone(), "workers");
        gossip
            .tell(PubSubGossipMsg::AddPeer {
                peer: PubSubGossipPeer::new(node_b.clone(), peer_b.actor_ref()),
            })
            .unwrap();

        let inbound =
            PubSubGossipWireInbound::new(node_a.clone(), registry.clone(), gossip.clone());
        let delta = PubSubDelta {
            from: node_b.clone(),
            delta: remote_registry.collect_delta(&BTreeMap::new(), 10),
        };
        inbound
            .receive(PubSubSerializedGossip::new(
                node_a,
                registry.serialize(&delta).unwrap(),
            ))
            .unwrap();
        gossip
            .tell(PubSubGossipMsg::GetRegistry {
                reply_to: registry_probe.actor_ref(),
            })
            .unwrap();

        let registry_state = registry_probe
            .expect_msg(Duration::from_millis(500))
            .unwrap();
        assert_eq!(
            registry_state.one_per_group_targets(&jobs).get("workers"),
            Some(&node_b)
        );
        kit.shutdown(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn wire_inbound_rejects_wrong_target_and_unknown_manifest() {
        let kit = ActorSystemTestKit::new("pubsub-wire-reject").unwrap();
        let registry = registry();
        let node_a = node("a", 1);
        let gossip_node = node_a.clone();
        let gossip = kit
            .system()
            .spawn(
                "gossip",
                Props::new(move || PubSubGossipActor::new(gossip_node)),
            )
            .unwrap();
        let inbound = PubSubGossipWireInbound::new(node_a.clone(), registry, gossip);
        let wrong_target = inbound
            .receive(PubSubSerializedGossip::new(
                node("other", 99),
                SerializedMessage::new(
                    PUBSUB_STATUS_SERIALIZER_ID,
                    Manifest::new(PubSubStatus::MANIFEST),
                    PubSubStatus::VERSION,
                    bytes::Bytes::new(),
                ),
            ))
            .expect_err("wrong target should fail");
        assert!(matches!(
            wrong_target,
            PubSubGossipWireError::WrongTarget { .. }
        ));

        let unknown = inbound
            .receive_message(SerializedMessage::new(
                9_999,
                Manifest::new("kairo.cluster-tools.pubsub.unknown"),
                1,
                bytes::Bytes::new(),
            ))
            .expect_err("unknown manifest should fail");
        assert!(matches!(
            unknown,
            PubSubGossipWireError::UnsupportedManifest(_)
        ));
        kit.shutdown(Duration::from_secs(1)).unwrap();
    }
}
