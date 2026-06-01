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
mod tests;
