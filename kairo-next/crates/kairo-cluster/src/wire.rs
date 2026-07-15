#![deny(missing_docs)]

use std::collections::BTreeMap;
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, Context, Recipient};
use kairo_serialization::{Registry, RemoteMessage, SerializationError, SerializedMessage};

use crate::{
    ClusterMembershipMsg, ClusterSeedJoinProcessMsg, Down, ExitingConfirmed, GossipEnvelope, Join,
    Leave, UniqueAddress, Welcome,
};

#[derive(Debug, Clone, PartialEq, Eq)]
/// Serialized cluster membership message paired with its target incarnation.
///
/// Keeping the [`UniqueAddress`] beside the payload lets the receiving node
/// reject traffic intended for an earlier incarnation at the same address.
pub struct ClusterSerializedMembership {
    /// Unique node incarnation for which the message is intended.
    pub target: UniqueAddress,
    /// Registry-produced manifest, version, serializer identifier, and payload.
    pub message: SerializedMessage,
}

impl ClusterSerializedMembership {
    /// Pairs `message` with its intended target incarnation.
    pub fn new(target: UniqueAddress, message: SerializedMessage) -> Self {
        Self { target, message }
    }
}

#[derive(Debug)]
/// Failure while serializing, routing, or decoding cluster membership traffic.
pub enum ClusterMembershipWireError {
    /// The registry rejected message serialization or deserialization.
    Serialization(SerializationError),
    /// An actor recipient rejected delivery.
    Send(String),
    /// A local-only membership command was passed to the remote outbound.
    UnsupportedMessage(&'static str),
    /// The inbound payload manifest is not part of the membership protocol.
    UnsupportedManifest(String),
    /// The serialized envelope names a different node incarnation.
    WrongTarget {
        /// Stable display key for the local node incarnation.
        expected: String,
        /// Stable display key carried by the rejected envelope.
        actual: String,
    },
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
/// Serializes remotely meaningful membership commands for one target node.
///
/// Commands that only mutate local actor state are rejected with
/// [`ClusterMembershipWireError::UnsupportedMessage`].
pub struct ClusterMembershipWireOutbound {
    target: UniqueAddress,
    registry: Arc<Registry>,
    outbound: Arc<dyn Recipient<ClusterSerializedMembership> + Send + Sync>,
}

impl ClusterMembershipWireOutbound {
    /// Creates an outbound route backed by an owned serialized-message recipient.
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

    /// Creates an outbound route backed by a shared serialized-message recipient.
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

    /// Returns the unique node incarnation targeted by this route.
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

    /// Serializes and sends one remotely meaningful membership command.
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
            ClusterMembershipMsg::Leave { address } => self.send_remote_message(&Leave { address }),
            ClusterMembershipMsg::DownAddress { address } => {
                self.send_remote_message(&Down { address })
            }
            ClusterMembershipMsg::ExitingConfirmed { node } => {
                self.send_remote_message(&ExitingConfirmed { node })
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
            ClusterMembershipMsg::RegisterInitJoinResponder { .. } => Err(
                ClusterMembershipWireError::UnsupportedMessage("register-init-join-responder"),
            ),
            ClusterMembershipMsg::RegisterGossipProcess { .. } => Err(
                ClusterMembershipWireError::UnsupportedMessage("register-gossip-process"),
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

/// Actor wrapper for a [`ClusterMembershipWireOutbound`] route.
///
/// Transient delivery failures are consumed so cached reply routes stay alive;
/// seed join and gossip own retry policy. Serialization and unsupported-command
/// errors still fail actor processing.
pub struct ClusterMembershipWireOutboundActor {
    outbound: ClusterMembershipWireOutbound,
}

impl ClusterMembershipWireOutboundActor {
    /// Creates an actor around `outbound`.
    pub fn new(outbound: ClusterMembershipWireOutbound) -> Self {
        Self { outbound }
    }

    /// Returns the route used by this actor.
    pub fn outbound(&self) -> &ClusterMembershipWireOutbound {
        &self.outbound
    }
}

impl Actor for ClusterMembershipWireOutboundActor {
    type Msg = ClusterMembershipMsg;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match self.outbound.send_membership(msg) {
            // Membership delivery is retried by seed join and gossip. A transient
            // association failure must not permanently stop a cached reply route.
            Ok(()) | Err(ClusterMembershipWireError::Send(_)) => Ok(()),
            Err(error) => Err(ActorError::Message(error.to_string())),
        }
    }
}

#[derive(Clone)]
/// Decodes membership protocol payloads and delivers typed actor commands.
///
/// Join and gossip messages may need a route back to their sending node. Such
/// routes can be installed eagerly or created lazily with
/// [`Self::with_reply_route_factory`].
pub struct ClusterMembershipWireInbound {
    self_node: UniqueAddress,
    registry: Arc<Registry>,
    membership: ActorRef<ClusterMembershipMsg>,
    seed_join_process: Option<ActorRef<ClusterSeedJoinProcessMsg>>,
    reply_routes: BTreeMap<String, ActorRef<ClusterMembershipMsg>>,
    reply_route_factory: Option<Arc<ReplyRouteFactory>>,
}

type ReplyRouteFactory =
    dyn Fn(&UniqueAddress) -> Result<ActorRef<ClusterMembershipMsg>, String> + Send + Sync;

impl ClusterMembershipWireInbound {
    /// Creates an inbound adapter for `self_node` and its membership actor.
    pub fn new(
        self_node: UniqueAddress,
        registry: Arc<Registry>,
        membership: ActorRef<ClusterMembershipMsg>,
    ) -> Self {
        Self {
            self_node,
            registry,
            membership,
            seed_join_process: None,
            reply_routes: BTreeMap::new(),
            reply_route_factory: None,
        }
    }

    /// Adds the seed-join process notified when a [`Welcome`] arrives.
    pub fn with_seed_join_process(mut self, process: ActorRef<ClusterSeedJoinProcessMsg>) -> Self {
        self.seed_join_process = Some(process);
        self
    }

    /// Installs an eager reply route for one remote node incarnation.
    pub fn with_reply_route(
        mut self,
        node: UniqueAddress,
        route: ActorRef<ClusterMembershipMsg>,
    ) -> Self {
        self.reply_routes.insert(node.ordering_key(), route);
        self
    }

    /// Installs a factory used when no eager reply route exists for a sender.
    pub fn with_reply_route_factory<F>(mut self, factory: F) -> Self
    where
        F: Fn(&UniqueAddress) -> Result<ActorRef<ClusterMembershipMsg>, String>
            + Send
            + Sync
            + 'static,
    {
        self.reply_route_factory = Some(Arc::new(factory));
        self
    }

    /// Inserts or replaces an eager reply route, returning the previous route.
    pub fn set_reply_route(
        &mut self,
        node: UniqueAddress,
        route: ActorRef<ClusterMembershipMsg>,
    ) -> Option<ActorRef<ClusterMembershipMsg>> {
        self.reply_routes.insert(node.ordering_key(), route)
    }

    /// Removes and returns the eager reply route for `node`, if present.
    pub fn remove_reply_route(
        &mut self,
        node: &UniqueAddress,
    ) -> Option<ActorRef<ClusterMembershipMsg>> {
        self.reply_routes.remove(&node.ordering_key())
    }

    /// Validates the target incarnation, decodes the payload, and delivers it.
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

    /// Decodes and delivers a payload whose remote recipient was validated
    /// externally.
    ///
    /// System-inbound routing uses this entry point after it has checked the
    /// remote recipient path. Prefer [`Self::receive`] when a
    /// [`ClusterSerializedMembership`] target is available.
    pub fn receive_message(
        &self,
        message: SerializedMessage,
    ) -> Result<(), ClusterMembershipWireError> {
        match message.manifest.as_str() {
            Join::MANIFEST => {
                let join = self.registry.deserialize::<Join>(message)?;
                let reply_to = self.reply_route(&join.node)?;
                self.membership
                    .tell(ClusterMembershipMsg::Join { join, reply_to })
                    .map_err(|error| ClusterMembershipWireError::Send(error.reason().to_string()))
            }
            Welcome::MANIFEST => {
                let welcome = self.registry.deserialize::<Welcome>(message)?;
                self.membership
                    .tell(ClusterMembershipMsg::Welcome(Box::new(welcome.clone())))
                    .map_err(|error| {
                        ClusterMembershipWireError::Send(error.reason().to_string())
                    })?;
                if let Some(process) = &self.seed_join_process {
                    process
                        .tell(ClusterSeedJoinProcessMsg::Welcome {
                            from: welcome.from.address,
                        })
                        .map_err(|error| {
                            ClusterMembershipWireError::Send(error.reason().to_string())
                        })?;
                }
                Ok(())
            }
            GossipEnvelope::MANIFEST => {
                let envelope = self.registry.deserialize::<GossipEnvelope>(message)?;
                let reply_to = self.reply_route(&envelope.from)?;
                self.membership
                    .tell(ClusterMembershipMsg::Gossip {
                        envelope: Box::new(envelope),
                        reply_to,
                    })
                    .map_err(|error| ClusterMembershipWireError::Send(error.reason().to_string()))
            }
            Leave::MANIFEST => {
                let leave = self.registry.deserialize::<Leave>(message)?;
                self.membership
                    .tell(ClusterMembershipMsg::Leave {
                        address: leave.address,
                    })
                    .map_err(|error| ClusterMembershipWireError::Send(error.reason().to_string()))
            }
            Down::MANIFEST => {
                let down = self.registry.deserialize::<Down>(message)?;
                self.membership
                    .tell(ClusterMembershipMsg::DownAddress {
                        address: down.address,
                    })
                    .map_err(|error| ClusterMembershipWireError::Send(error.reason().to_string()))
            }
            ExitingConfirmed::MANIFEST => {
                let confirmation = self.registry.deserialize::<ExitingConfirmed>(message)?;
                self.membership
                    .tell(ClusterMembershipMsg::ExitingConfirmed {
                        node: confirmation.node,
                    })
                    .map_err(|error| ClusterMembershipWireError::Send(error.reason().to_string()))
            }
            manifest => Err(ClusterMembershipWireError::UnsupportedManifest(
                manifest.to_string(),
            )),
        }
    }

    fn reply_route(
        &self,
        node: &UniqueAddress,
    ) -> Result<Option<ActorRef<ClusterMembershipMsg>>, ClusterMembershipWireError> {
        if let Some(route) = self.reply_routes.get(&node.ordering_key()) {
            return Ok(Some(route.clone()));
        }
        match &self.reply_route_factory {
            Some(factory) => factory(node)
                .map(Some)
                .map_err(ClusterMembershipWireError::Send),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests;
