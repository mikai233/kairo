#![deny(missing_docs)]

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;
use std::time::Duration;

use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, Context};
use kairo_remote::RemoteOutbound;
use kairo_serialization::{Registry, RemoteMessage, SerializationError, SerializedMessage};

use crate::{
    ClusterMembershipMsg, ClusterMembershipRemoteEnvelopeError,
    ClusterMembershipRemoteEnvelopeOutbound, ClusterSerializedMembership, Gossip, GossipEnvelope,
    GossipStatus, ReachabilityStatus, UniqueAddress, VectorClockOrdering,
};

const GOSSIP_TIMER: &str = "cluster-gossip";

/// Scheduling policy for periodic gossip negotiation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClusterGossipProcessSettings {
    interval: Duration,
    automatic_ticks: bool,
}

impl ClusterGossipProcessSettings {
    /// Creates settings with automatic periodic ticks enabled.
    ///
    /// # Errors
    ///
    /// Returns [`ClusterGossipProcessSettingsError::ZeroInterval`] when
    /// `interval` is zero.
    pub fn new(interval: Duration) -> Result<Self, ClusterGossipProcessSettingsError> {
        if interval.is_zero() {
            return Err(ClusterGossipProcessSettingsError::ZeroInterval);
        }
        Ok(Self {
            interval,
            automatic_ticks: true,
        })
    }

    /// Returns the delay between periodic gossip rounds.
    pub fn interval(self) -> Duration {
        self.interval
    }

    /// Returns whether the process schedules its own periodic ticks.
    pub fn automatic_ticks(self) -> bool {
        self.automatic_ticks
    }

    /// Enables or disables actor-owned periodic tick scheduling.
    pub fn with_automatic_ticks(mut self, automatic_ticks: bool) -> Self {
        self.automatic_ticks = automatic_ticks;
        self
    }
}

impl Default for ClusterGossipProcessSettings {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(1),
            automatic_ticks: true,
        }
    }
}

/// Invalid gossip-process configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterGossipProcessSettingsError {
    /// The periodic gossip interval was zero.
    ZeroInterval,
}

impl Display for ClusterGossipProcessSettingsError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroInterval => write!(f, "cluster gossip interval must be greater than zero"),
        }
    }
}

impl Error for ClusterGossipProcessSettingsError {}

/// Outbound result of one gossip selection or status-negotiation step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterGossipAction {
    /// Send only causal version and seen digest metadata.
    SendStatus {
        /// Remote cluster member receiving the status.
        target: UniqueAddress,
        /// Local version and seen digest.
        status: GossipStatus,
    },
    /// Send a complete gossip snapshot.
    SendGossip {
        /// Remote cluster member receiving the snapshot.
        target: UniqueAddress,
        /// Addressed full-gossip envelope.
        envelope: Box<GossipEnvelope>,
    },
}

/// Deterministic target-selection and gossip-status negotiation state.
#[derive(Debug, Clone)]
pub struct ClusterGossipState {
    self_node: UniqueAddress,
    target_cursor: usize,
    sequence_nr: u64,
}

impl ClusterGossipState {
    /// Creates gossip state for one local unique address.
    pub fn new(self_node: UniqueAddress) -> Self {
        Self {
            self_node,
            target_cursor: 0,
            sequence_nr: 0,
        }
    }

    /// Selects the next reachable peer and action for a periodic round.
    ///
    /// Peers that have not seen the current view are preferred and receive full
    /// gossip. When every candidate has seen it, round-robin selection sends a
    /// compact status instead. Self and locally unreachable peers are excluded.
    pub fn initiate(&mut self, gossip: &Gossip) -> Option<ClusterGossipAction> {
        let mut valid: Vec<_> = gossip
            .members()
            .iter()
            .map(|member| &member.unique_address)
            .filter(|node| self.valid_target(gossip, node))
            .cloned()
            .collect();
        valid.sort_by_key(UniqueAddress::ordering_key);

        let different_view: Vec<_> = valid
            .iter()
            .filter(|node| !gossip.seen_by().contains(*node))
            .cloned()
            .collect();
        let candidates = if different_view.is_empty() {
            &valid
        } else {
            &different_view
        };
        if candidates.is_empty() {
            return None;
        }

        let target = candidates[self.target_cursor % candidates.len()].clone();
        self.target_cursor = self.target_cursor.wrapping_add(1);
        if gossip.seen_by().contains(&target) {
            Some(self.status_action(gossip, target))
        } else {
            Some(self.gossip_action(gossip, target))
        }
    }

    /// Negotiates one received gossip status against the local snapshot.
    ///
    /// A seen-digest mismatch, older remote version, or concurrent version sends
    /// full local gossip. A newer remote version receives local status so it can
    /// answer with its full view. Equal versions and seen digests need no reply.
    pub fn receive_status(
        &mut self,
        gossip: &Gossip,
        status: GossipStatus,
    ) -> Option<ClusterGossipAction> {
        let from = status.from.clone();
        if !self.valid_target(gossip, &from) {
            return None;
        }

        let local_digest = gossip.seen_digest();
        let seen_same = status.seen_digest.is_empty()
            || local_digest.is_empty()
            || status.seen_digest == local_digest;
        if !seen_same {
            return Some(self.gossip_action(gossip, from));
        }

        match status.version.compare(gossip.version()) {
            VectorClockOrdering::Same => None,
            VectorClockOrdering::After => Some(self.status_action(gossip, from)),
            VectorClockOrdering::Before | VectorClockOrdering::Concurrent => {
                Some(self.gossip_action(gossip, from))
            }
        }
    }

    fn valid_target(&self, gossip: &Gossip, node: &UniqueAddress) -> bool {
        node != &self.self_node
            && gossip.has_member(node)
            && gossip.reachability().status(&self.self_node, node) == ReachabilityStatus::Reachable
    }

    fn status_action(&self, gossip: &Gossip, target: UniqueAddress) -> ClusterGossipAction {
        ClusterGossipAction::SendStatus {
            target,
            status: GossipStatus {
                from: self.self_node.clone(),
                version: gossip.version().clone(),
                seen_digest: gossip.seen_digest(),
            },
        }
    }

    fn gossip_action(&mut self, gossip: &Gossip, target: UniqueAddress) -> ClusterGossipAction {
        self.sequence_nr = self.sequence_nr.wrapping_add(1);
        ClusterGossipAction::SendGossip {
            target: target.clone(),
            envelope: Box::new(GossipEnvelope {
                from: self.self_node.clone(),
                to: target,
                sequence_nr: self.sequence_nr,
                gossip: gossip.clone(),
            }),
        }
    }
}

/// Failure while encoding, routing, or delivering gossip protocol messages.
#[derive(Debug)]
pub enum ClusterGossipWireError {
    /// Remote membership envelope construction or delivery failed.
    Remote(ClusterMembershipRemoteEnvelopeError),
    /// The local gossip actor rejected a decoded message.
    Send(String),
    /// Gossip protocol serialization or deserialization failed.
    Serialization(SerializationError),
    /// An inbound message used a manifest not handled by this adapter.
    UnsupportedManifest(String),
}

impl Display for ClusterGossipWireError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Remote(error) => write!(f, "{error}"),
            Self::Send(reason) => write!(f, "cluster gossip delivery failed: {reason}"),
            Self::Serialization(error) => write!(f, "cluster gossip serialization failed: {error}"),
            Self::UnsupportedManifest(manifest) => {
                write!(f, "unsupported cluster gossip manifest `{manifest}`")
            }
        }
    }
}

impl Error for ClusterGossipWireError {}

impl From<ClusterMembershipRemoteEnvelopeError> for ClusterGossipWireError {
    fn from(error: ClusterMembershipRemoteEnvelopeError) -> Self {
        Self::Remote(error)
    }
}

impl From<SerializationError> for ClusterGossipWireError {
    fn from(error: SerializationError) -> Self {
        Self::Serialization(error)
    }
}

/// Serializes gossip actions and sends addressed membership envelopes remotely.
#[derive(Clone)]
pub struct ClusterGossipWireOutbound {
    registry: Arc<Registry>,
    remote: ClusterMembershipRemoteEnvelopeOutbound,
}

impl ClusterGossipWireOutbound {
    /// Creates an outbound adapter from a concrete remote transport.
    pub fn new(registry: Arc<Registry>, remote: impl RemoteOutbound + 'static) -> Self {
        Self::from_arc(registry, Arc::new(remote))
    }

    /// Creates an outbound adapter from a shared remote transport.
    pub fn from_arc(registry: Arc<Registry>, remote: Arc<dyn RemoteOutbound>) -> Self {
        Self {
            registry,
            remote: ClusterMembershipRemoteEnvelopeOutbound::from_arc(remote),
        }
    }

    /// Serializes and sends one selected gossip action.
    ///
    /// # Errors
    ///
    /// Returns an error for missing codecs, serialization failure, invalid
    /// addressing, or remote envelope delivery failure.
    pub fn send(&self, action: ClusterGossipAction) -> Result<(), ClusterGossipWireError> {
        let (target, message) = match action {
            ClusterGossipAction::SendStatus { target, status } => {
                (target, self.registry.serialize(&status)?)
            }
            ClusterGossipAction::SendGossip { target, envelope } => {
                (target, self.registry.serialize(envelope.as_ref())?)
            }
        };
        self.remote
            .send_serialized(ClusterSerializedMembership::new(target, message))?;
        Ok(())
    }
}

/// Actor protocol for periodic gossip selection and status negotiation.
#[derive(Debug, Clone)]
pub enum ClusterGossipProcessMsg {
    /// Start one periodic leader-action and gossip round.
    Tick,
    /// Current membership gossip returned by the membership actor.
    CurrentGossip(Gossip),
    /// Status metadata received from a remote peer.
    Status(GossipStatus),
}

/// Actor that drives best-effort periodic gossip exchange.
///
/// Transport send failures are retried by later rounds and do not terminate the
/// process; local serialization and protocol-composition failures remain fatal.
pub struct ClusterGossipProcess {
    state: ClusterGossipState,
    membership: ActorRef<ClusterMembershipMsg>,
    outbound: ClusterGossipWireOutbound,
    settings: ClusterGossipProcessSettings,
    current_gossip_reply: Option<ActorRef<Gossip>>,
    query_pending: bool,
    periodic_pending: bool,
    pending_statuses: Vec<GossipStatus>,
}

impl ClusterGossipProcess {
    /// Creates a gossip process from pure state, membership actor, and wire adapter.
    pub fn new(
        state: ClusterGossipState,
        membership: ActorRef<ClusterMembershipMsg>,
        outbound: ClusterGossipWireOutbound,
        settings: ClusterGossipProcessSettings,
    ) -> Self {
        Self {
            state,
            membership,
            outbound,
            settings,
            current_gossip_reply: None,
            query_pending: false,
            periodic_pending: false,
            pending_statuses: Vec::new(),
        }
    }

    fn request_gossip(&mut self) -> ActorResult {
        if self.query_pending {
            return Ok(());
        }
        let reply_to = self.current_gossip_reply.clone().ok_or_else(|| {
            ActorError::Message("cluster gossip reply adapter is not initialized".to_string())
        })?;
        self.membership
            .tell(ClusterMembershipMsg::SendCurrentGossip { reply_to })
            .map_err(|error| ActorError::Message(error.reason().to_string()))?;
        self.query_pending = true;
        Ok(())
    }

    fn send(&self, action: Option<ClusterGossipAction>) -> ActorResult {
        if let Some(action) = action {
            // Gossip is best-effort. Association or transport failure must not
            // terminate the process; a later round will retry convergence.
            match self.outbound.send(action) {
                Ok(())
                | Err(ClusterGossipWireError::Remote(
                    ClusterMembershipRemoteEnvelopeError::Send { .. },
                )) => {}
                Err(error) => return Err(ActorError::Message(error.to_string())),
            }
        }
        Ok(())
    }
}

impl Actor for ClusterGossipProcess {
    type Msg = ClusterGossipProcessMsg;

    fn started(&mut self, ctx: &mut Context<Self::Msg>) -> ActorResult {
        self.current_gossip_reply = Some(ctx.message_adapter(Self::Msg::CurrentGossip)?);
        if self.settings.automatic_ticks {
            ctx.start_timer_with_fixed_delay(
                GOSSIP_TIMER,
                self.settings.interval,
                self.settings.interval,
                ClusterGossipProcessMsg::Tick,
            );
        }
        Ok(())
    }

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            ClusterGossipProcessMsg::Tick => {
                let _ = self
                    .membership
                    .tell(ClusterMembershipMsg::LeaderActionsTick);
                self.periodic_pending = true;
                self.request_gossip()?;
            }
            ClusterGossipProcessMsg::Status(status) => {
                self.pending_statuses.push(status);
                self.request_gossip()?;
            }
            ClusterGossipProcessMsg::CurrentGossip(gossip) => {
                self.query_pending = false;
                for status in std::mem::take(&mut self.pending_statuses) {
                    let action = self.state.receive_status(&gossip, status);
                    self.send(action)?;
                }
                if std::mem::take(&mut self.periodic_pending) {
                    let action = self.state.initiate(&gossip);
                    self.send(action)?;
                }
            }
        }
        Ok(())
    }
}

/// Deserializes remote gossip status messages into the gossip process mailbox.
#[derive(Clone)]
pub struct ClusterGossipWireInbound {
    registry: Arc<Registry>,
    process: ActorRef<ClusterGossipProcessMsg>,
}

impl ClusterGossipWireInbound {
    /// Creates an inbound status adapter.
    pub fn new(registry: Arc<Registry>, process: ActorRef<ClusterGossipProcessMsg>) -> Self {
        Self { registry, process }
    }

    /// Deserializes and delivers one gossip status message.
    ///
    /// # Errors
    ///
    /// Returns an error for an unsupported manifest, missing or invalid codec,
    /// or a stopped gossip process.
    pub fn receive_message(
        &self,
        message: SerializedMessage,
    ) -> Result<(), ClusterGossipWireError> {
        if message.manifest.as_str() != GossipStatus::MANIFEST {
            return Err(ClusterGossipWireError::UnsupportedManifest(
                message.manifest.as_str().to_string(),
            ));
        }
        let status = self.registry.deserialize::<GossipStatus>(message)?;
        self.process
            .tell(ClusterGossipProcessMsg::Status(status))
            .map_err(|error| ClusterGossipWireError::Send(error.reason().to_string()))
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use kairo_actor::Address;

    use super::*;
    use crate::{Member, MemberStatus, VectorClock};

    fn node(name: &str, uid: u64) -> UniqueAddress {
        UniqueAddress::new(
            Address::new(
                "kairo",
                "gossip",
                Some(format!("{name}.example.test")),
                Some(2552),
            ),
            uid,
        )
    }

    fn member(node: UniqueAddress) -> Member {
        Member::new(node, vec![]).with_status(MemberStatus::Up)
    }

    #[test]
    fn periodic_round_sends_full_gossip_to_member_with_different_view() {
        let self_node = node("self", 1);
        let peer = node("peer", 2);
        let gossip = Gossip::from_members([member(self_node.clone()), member(peer.clone())])
            .seen(self_node.clone())
            .increment_version(&self_node);
        let mut state = ClusterGossipState::new(self_node.clone());

        let action = state.initiate(&gossip).unwrap();

        assert!(matches!(
            action,
            ClusterGossipAction::SendGossip { target, envelope }
                if target == peer && envelope.from == self_node && envelope.to == peer
        ));
    }

    #[test]
    fn periodic_round_sends_status_when_peer_has_seen_current_view() {
        let self_node = node("self", 1);
        let peer = node("peer", 2);
        let gossip = Gossip::from_members([member(self_node.clone()), member(peer.clone())])
            .seen(self_node.clone())
            .seen(peer.clone())
            .increment_version(&self_node);
        let mut state = ClusterGossipState::new(self_node.clone());

        let action = state.initiate(&gossip).unwrap();

        assert!(matches!(
            action,
            ClusterGossipAction::SendStatus { target, status }
                if target == peer && status.from == self_node
                    && status.seen_digest == gossip.seen_digest()
        ));
    }

    #[test]
    fn status_negotiation_matches_pekko_version_and_seen_rules() {
        let self_node = node("self", 1);
        let peer = node("peer", 2);
        let local = Gossip::from_members([member(self_node.clone()), member(peer.clone())])
            .seen(self_node.clone())
            .increment_version(&self_node);
        let mut state = ClusterGossipState::new(self_node.clone());

        let different_seen = GossipStatus {
            from: peer.clone(),
            version: local.version().clone(),
            seen_digest: Bytes::from_static(b"different"),
        };
        assert!(matches!(
            state.receive_status(&local, different_seen),
            Some(ClusterGossipAction::SendGossip { target, .. }) if target == peer
        ));

        let remote_newer = GossipStatus {
            from: peer.clone(),
            version: local.version().increment("peer"),
            seen_digest: local.seen_digest(),
        };
        assert!(matches!(
            state.receive_status(&local, remote_newer),
            Some(ClusterGossipAction::SendStatus { target, .. }) if target == peer
        ));

        for remote_version in [VectorClock::new(), VectorClock::new().increment("peer")] {
            let older_or_concurrent = GossipStatus {
                from: peer.clone(),
                version: remote_version,
                seen_digest: local.seen_digest(),
            };
            assert!(matches!(
                state.receive_status(&local, older_or_concurrent),
                Some(ClusterGossipAction::SendGossip { target, .. }) if target == peer
            ));
        }

        let same = GossipStatus {
            from: peer,
            version: local.version().clone(),
            seen_digest: local.seen_digest(),
        };
        assert_eq!(state.receive_status(&local, same), None);
    }

    #[test]
    fn status_from_unknown_or_locally_unreachable_member_is_ignored() {
        let self_node = node("self", 1);
        let peer = node("peer", 2);
        let unknown = node("unknown", 3);
        let local = Gossip::from_members([member(self_node.clone()), member(peer.clone())])
            .with_reachability(
                crate::Reachability::new().unreachable(self_node.clone(), peer.clone()),
            );
        let mut state = ClusterGossipState::new(self_node);
        let status = |from| GossipStatus {
            from,
            version: VectorClock::new(),
            seen_digest: Bytes::new(),
        };

        assert_eq!(state.receive_status(&local, status(peer)), None);
        assert_eq!(state.receive_status(&local, status(unknown)), None);
    }
}
