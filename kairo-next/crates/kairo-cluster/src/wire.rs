use std::collections::BTreeMap;
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, Context, Recipient};
use kairo_serialization::{Registry, RemoteMessage, SerializationError, SerializedMessage};

use crate::{ClusterMembershipMsg, GossipEnvelope, Join, UniqueAddress, Welcome};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterSerializedMembership {
    pub target: UniqueAddress,
    pub message: SerializedMessage,
}

impl ClusterSerializedMembership {
    pub fn new(target: UniqueAddress, message: SerializedMessage) -> Self {
        Self { target, message }
    }
}

#[derive(Debug)]
pub enum ClusterMembershipWireError {
    Serialization(SerializationError),
    Send(String),
    UnsupportedMessage(&'static str),
    UnsupportedManifest(String),
    WrongTarget { expected: String, actual: String },
}

impl Display for ClusterMembershipWireError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Serialization(error) => {
                write!(f, "cluster membership serialization failed: {error}")
            }
            Self::Send(reason) => write!(f, "cluster membership delivery failed: {reason}"),
            Self::UnsupportedMessage(message) => {
                write!(f, "unsupported cluster membership wire message `{message}`")
            }
            Self::UnsupportedManifest(manifest) => {
                write!(f, "unsupported cluster membership manifest `{manifest}`")
            }
            Self::WrongTarget { expected, actual } => {
                write!(
                    f,
                    "cluster membership message was addressed to {}, expected {}",
                    actual, expected
                )
            }
        }
    }
}

impl std::error::Error for ClusterMembershipWireError {}

impl From<SerializationError> for ClusterMembershipWireError {
    fn from(error: SerializationError) -> Self {
        Self::Serialization(error)
    }
}

#[derive(Clone)]
pub struct ClusterMembershipWireOutbound {
    target: UniqueAddress,
    registry: Arc<Registry>,
    outbound: Arc<dyn Recipient<ClusterSerializedMembership> + Send + Sync>,
}

impl ClusterMembershipWireOutbound {
    pub fn new(
        target: UniqueAddress,
        registry: Arc<Registry>,
        outbound: impl Recipient<ClusterSerializedMembership> + Send + Sync + 'static,
    ) -> Self {
        Self {
            target,
            registry,
            outbound: Arc::new(outbound),
        }
    }

    pub fn from_arc(
        target: UniqueAddress,
        registry: Arc<Registry>,
        outbound: Arc<dyn Recipient<ClusterSerializedMembership> + Send + Sync>,
    ) -> Self {
        Self {
            target,
            registry,
            outbound,
        }
    }

    pub fn target(&self) -> &UniqueAddress {
        &self.target
    }

    fn send_remote_message<M>(&self, message: &M) -> Result<(), ClusterMembershipWireError>
    where
        M: RemoteMessage,
    {
        let serialized = self.registry.serialize(message)?;
        self.outbound
            .tell(ClusterSerializedMembership::new(
                self.target.clone(),
                serialized,
            ))
            .map_err(|error| ClusterMembershipWireError::Send(error.reason().to_string()))
    }

    pub fn send_membership(
        &self,
        message: ClusterMembershipMsg,
    ) -> Result<(), ClusterMembershipWireError> {
        match message {
            ClusterMembershipMsg::Join { join, .. } => self.send_remote_message(&join),
            ClusterMembershipMsg::Welcome(welcome) => self.send_remote_message(welcome.as_ref()),
            ClusterMembershipMsg::Gossip { envelope, .. } => {
                self.send_remote_message(envelope.as_ref())
            }
            ClusterMembershipMsg::JoinSelf => {
                Err(ClusterMembershipWireError::UnsupportedMessage("join-self"))
            }
            ClusterMembershipMsg::MarkUnreachable { .. } => Err(
                ClusterMembershipWireError::UnsupportedMessage("mark-unreachable"),
            ),
            ClusterMembershipMsg::MarkReachable { .. } => Err(
                ClusterMembershipWireError::UnsupportedMessage("mark-reachable"),
            ),
            ClusterMembershipMsg::Down { .. } => {
                Err(ClusterMembershipWireError::UnsupportedMessage("down"))
            }
            ClusterMembershipMsg::ApplyDowningDecision(_) => Err(
                ClusterMembershipWireError::UnsupportedMessage("apply-downing-decision"),
            ),
            ClusterMembershipMsg::RegisterDowningProvider { .. } => Err(
                ClusterMembershipWireError::UnsupportedMessage("register-downing-provider"),
            ),
            ClusterMembershipMsg::LeaderActionsTick => Err(
                ClusterMembershipWireError::UnsupportedMessage("leader-actions-tick"),
            ),
            ClusterMembershipMsg::SendCurrentGossip { .. } => Err(
                ClusterMembershipWireError::UnsupportedMessage("send-current-gossip"),
            ),
            ClusterMembershipMsg::SendCurrentState { .. } => Err(
                ClusterMembershipWireError::UnsupportedMessage("send-current-state"),
            ),
        }
    }
}

pub struct ClusterMembershipWireOutboundActor {
    outbound: ClusterMembershipWireOutbound,
}

impl ClusterMembershipWireOutboundActor {
    pub fn new(outbound: ClusterMembershipWireOutbound) -> Self {
        Self { outbound }
    }

    pub fn outbound(&self) -> &ClusterMembershipWireOutbound {
        &self.outbound
    }
}

impl Actor for ClusterMembershipWireOutboundActor {
    type Msg = ClusterMembershipMsg;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        self.outbound
            .send_membership(msg)
            .map_err(|error| ActorError::Message(error.to_string()))
    }
}

#[derive(Clone)]
pub struct ClusterMembershipWireInbound {
    self_node: UniqueAddress,
    registry: Arc<Registry>,
    membership: ActorRef<ClusterMembershipMsg>,
    reply_routes: BTreeMap<String, ActorRef<ClusterMembershipMsg>>,
}

impl ClusterMembershipWireInbound {
    pub fn new(
        self_node: UniqueAddress,
        registry: Arc<Registry>,
        membership: ActorRef<ClusterMembershipMsg>,
    ) -> Self {
        Self {
            self_node,
            registry,
            membership,
            reply_routes: BTreeMap::new(),
        }
    }

    pub fn with_reply_route(
        mut self,
        node: UniqueAddress,
        route: ActorRef<ClusterMembershipMsg>,
    ) -> Self {
        self.reply_routes.insert(node.ordering_key(), route);
        self
    }

    pub fn set_reply_route(
        &mut self,
        node: UniqueAddress,
        route: ActorRef<ClusterMembershipMsg>,
    ) -> Option<ActorRef<ClusterMembershipMsg>> {
        self.reply_routes.insert(node.ordering_key(), route)
    }

    pub fn remove_reply_route(
        &mut self,
        node: &UniqueAddress,
    ) -> Option<ActorRef<ClusterMembershipMsg>> {
        self.reply_routes.remove(&node.ordering_key())
    }

    pub fn receive(
        &self,
        envelope: ClusterSerializedMembership,
    ) -> Result<(), ClusterMembershipWireError> {
        if envelope.target != self.self_node {
            return Err(ClusterMembershipWireError::WrongTarget {
                expected: self.self_node.ordering_key(),
                actual: envelope.target.ordering_key(),
            });
        }
        self.receive_message(envelope.message)
    }

    pub fn receive_message(
        &self,
        message: SerializedMessage,
    ) -> Result<(), ClusterMembershipWireError> {
        match message.manifest.as_str() {
            Join::MANIFEST => {
                let join = self.registry.deserialize::<Join>(message)?;
                let reply_to = self.reply_route(&join.node);
                self.membership
                    .tell(ClusterMembershipMsg::Join { join, reply_to })
                    .map_err(|error| ClusterMembershipWireError::Send(error.reason().to_string()))
            }
            Welcome::MANIFEST => {
                let welcome = self.registry.deserialize::<Welcome>(message)?;
                self.membership
                    .tell(ClusterMembershipMsg::Welcome(Box::new(welcome)))
                    .map_err(|error| ClusterMembershipWireError::Send(error.reason().to_string()))
            }
            GossipEnvelope::MANIFEST => {
                let envelope = self.registry.deserialize::<GossipEnvelope>(message)?;
                let reply_to = self.reply_route(&envelope.from);
                self.membership
                    .tell(ClusterMembershipMsg::Gossip {
                        envelope: Box::new(envelope),
                        reply_to,
                    })
                    .map_err(|error| ClusterMembershipWireError::Send(error.reason().to_string()))
            }
            manifest => Err(ClusterMembershipWireError::UnsupportedManifest(
                manifest.to_string(),
            )),
        }
    }

    fn reply_route(&self, node: &UniqueAddress) -> Option<ActorRef<ClusterMembershipMsg>> {
        self.reply_routes.get(&node.ordering_key()).cloned()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use kairo_actor::{Address, Props};
    use kairo_serialization::{Manifest, RemoteMessage};
    use kairo_testkit::ActorSystemTestKit;

    use super::*;
    use crate::{
        ClusterEvent, ClusterEventPublisher, ClusterEventPublisherMsg,
        GOSSIP_ENVELOPE_SERIALIZER_ID, Gossip, JOIN_SERIALIZER_ID, Member, MemberStatus,
        SubscriptionInitialState, WELCOME_SERIALIZER_ID, register_cluster_protocol_codecs,
    };

    fn registry() -> Arc<Registry> {
        let mut registry = Registry::new();
        register_cluster_protocol_codecs(&mut registry).unwrap();
        Arc::new(registry)
    }

    fn node(system: &str, uid: u64) -> UniqueAddress {
        UniqueAddress::new(Address::local(system), uid)
    }

    fn member(unique_address: UniqueAddress, status: MemberStatus) -> Member {
        Member::new(unique_address, Vec::new()).with_status(status)
    }

    fn spawn_membership(
        kit: &ActorSystemTestKit,
        self_node: UniqueAddress,
        name: &str,
    ) -> ActorRef<ClusterMembershipMsg> {
        let publisher = kit
            .system()
            .spawn(
                format!("{name}-publisher"),
                Props::new({
                    let self_node = self_node.clone();
                    move || ClusterEventPublisher::new(self_node.clone())
                }),
            )
            .unwrap();
        let events = kit
            .create_probe::<ClusterEvent>(format!("{name}-events"))
            .unwrap();
        publisher
            .tell(ClusterEventPublisherMsg::Subscribe {
                subscriber: events.actor_ref(),
                initial_state: SubscriptionInitialState::None,
            })
            .unwrap();
        kit.system()
            .spawn(
                name,
                Props::new(move || {
                    crate::ClusterMembership::new(self_node.clone(), Vec::new(), publisher.clone())
                }),
            )
            .unwrap()
    }

    #[test]
    fn wire_outbound_serializes_join_welcome_and_gossip_for_target_node() {
        let kit = ActorSystemTestKit::new("cluster-wire-outbound").unwrap();
        let registry = registry();
        let node_a = node("a", 1);
        let node_b = node("b", 2);
        let outbound_probe = kit
            .create_probe::<ClusterSerializedMembership>("wire-out")
            .unwrap();
        let outbound = ClusterMembershipWireOutbound::new(
            node_b.clone(),
            registry.clone(),
            outbound_probe.actor_ref(),
        );

        outbound
            .send_membership(ClusterMembershipMsg::Join {
                join: Join {
                    node: node_a.clone(),
                    roles: vec!["backend".to_string()],
                },
                reply_to: None,
            })
            .unwrap();
        let join_envelope = outbound_probe
            .expect_msg(Duration::from_millis(500))
            .unwrap();
        assert_eq!(join_envelope.target, node_b);
        assert_eq!(join_envelope.message.serializer_id, JOIN_SERIALIZER_ID);
        assert_eq!(
            registry
                .deserialize::<Join>(join_envelope.message)
                .unwrap()
                .node,
            node_a
        );

        let gossip = Gossip::from_members([member(node("a", 1), MemberStatus::Up)]);
        outbound
            .send_membership(ClusterMembershipMsg::Welcome(Box::new(Welcome {
                from: node("a", 1),
                gossip: gossip.clone(),
            })))
            .unwrap();
        let welcome_envelope = outbound_probe
            .expect_msg(Duration::from_millis(500))
            .unwrap();
        assert_eq!(
            welcome_envelope.message.serializer_id,
            WELCOME_SERIALIZER_ID
        );
        assert_eq!(
            registry
                .deserialize::<Welcome>(welcome_envelope.message)
                .unwrap()
                .gossip,
            gossip
        );

        outbound
            .send_membership(ClusterMembershipMsg::Gossip {
                envelope: Box::new(GossipEnvelope {
                    from: node("a", 1),
                    to: node("b", 2),
                    sequence_nr: 7,
                    gossip: Gossip::from_members([member(node("b", 2), MemberStatus::Joining)]),
                }),
                reply_to: None,
            })
            .unwrap();
        let gossip_envelope = outbound_probe
            .expect_msg(Duration::from_millis(500))
            .unwrap();
        assert_eq!(
            gossip_envelope.message.serializer_id,
            GOSSIP_ENVELOPE_SERIALIZER_ID
        );
        assert_eq!(
            registry
                .deserialize::<GossipEnvelope>(gossip_envelope.message)
                .unwrap()
                .sequence_nr,
            7
        );
        kit.shutdown(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn wire_inbound_delivers_join_and_routes_welcome_reply() {
        let kit = ActorSystemTestKit::new("cluster-wire-join-in").unwrap();
        let registry = registry();
        let seed = node("seed", 1);
        let joining = node("joining", 2);
        let membership = spawn_membership(&kit, seed.clone(), "membership");
        let outbound_probe = kit
            .create_probe::<ClusterSerializedMembership>("wire-out")
            .unwrap();
        let outbound = ClusterMembershipWireOutbound::new(
            joining.clone(),
            registry.clone(),
            outbound_probe.actor_ref(),
        );
        let outbound_actor = kit
            .system()
            .spawn(
                "wire-outbound",
                Props::new(move || ClusterMembershipWireOutboundActor::new(outbound.clone())),
            )
            .unwrap();
        membership.tell(ClusterMembershipMsg::JoinSelf).unwrap();
        let inbound = ClusterMembershipWireInbound::new(seed.clone(), registry.clone(), membership)
            .with_reply_route(joining.clone(), outbound_actor);

        inbound
            .receive(ClusterSerializedMembership::new(
                seed.clone(),
                registry
                    .serialize(&Join {
                        node: joining.clone(),
                        roles: vec!["backend".to_string()],
                    })
                    .unwrap(),
            ))
            .unwrap();

        let welcome_envelope = outbound_probe.expect_msg(Duration::from_secs(1)).unwrap();
        assert_eq!(welcome_envelope.target, joining.clone());
        assert_eq!(
            welcome_envelope.message.serializer_id,
            WELCOME_SERIALIZER_ID
        );
        let welcome = registry
            .deserialize::<Welcome>(welcome_envelope.message)
            .unwrap();
        assert_eq!(welcome.from, seed);
        assert_eq!(
            welcome.gossip.member(&joining).map(|member| member.status),
            Some(MemberStatus::Joining)
        );
        kit.shutdown(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn wire_inbound_delivers_welcome_to_membership_actor() {
        let kit = ActorSystemTestKit::new("cluster-wire-welcome-in").unwrap();
        let registry = registry();
        let seed = node("seed", 1);
        let joining = node("joining", 2);
        let membership = spawn_membership(&kit, joining.clone(), "membership");
        let gossip_probe = kit.create_probe::<Gossip>("gossip").unwrap();
        let gossip = Gossip::from_members([
            member(seed.clone(), MemberStatus::Up),
            member(joining.clone(), MemberStatus::Joining),
        ]);
        let inbound = ClusterMembershipWireInbound::new(
            joining.clone(),
            registry.clone(),
            membership.clone(),
        );

        inbound
            .receive(ClusterSerializedMembership::new(
                joining.clone(),
                registry.serialize(&Welcome { from: seed, gossip }).unwrap(),
            ))
            .unwrap();
        membership
            .tell(ClusterMembershipMsg::SendCurrentGossip {
                reply_to: gossip_probe.actor_ref(),
            })
            .unwrap();

        let current = gossip_probe.expect_msg(Duration::from_secs(1)).unwrap();
        assert!(current.has_member(&joining));
        assert!(current.seen_by().contains(&joining));
        kit.shutdown(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn wire_inbound_rejects_wrong_target_and_unknown_manifest() {
        let kit = ActorSystemTestKit::new("cluster-wire-reject").unwrap();
        let registry = registry();
        let self_node = node("self", 1);
        let membership = spawn_membership(&kit, self_node.clone(), "membership");
        let inbound = ClusterMembershipWireInbound::new(self_node, registry, membership);

        let wrong_target = inbound
            .receive(ClusterSerializedMembership::new(
                node("other", 99),
                SerializedMessage::new(
                    JOIN_SERIALIZER_ID,
                    Manifest::new(Join::MANIFEST),
                    Join::VERSION,
                    bytes::Bytes::new(),
                ),
            ))
            .expect_err("wrong target should fail");
        assert!(matches!(
            wrong_target,
            ClusterMembershipWireError::WrongTarget { .. }
        ));

        let unknown = inbound
            .receive_message(SerializedMessage::new(
                9_999,
                Manifest::new("kairo.cluster.unknown"),
                1,
                bytes::Bytes::new(),
            ))
            .expect_err("unknown manifest should fail");
        assert!(matches!(
            unknown,
            ClusterMembershipWireError::UnsupportedManifest(_)
        ));
        kit.shutdown(Duration::from_secs(1)).unwrap();
    }
}
