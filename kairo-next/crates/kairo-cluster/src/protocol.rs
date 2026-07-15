#![deny(missing_docs)]

use bytes::Bytes;
use kairo_actor::Address;
use kairo_serialization::RemoteMessage;

use crate::{Gossip, UniqueAddress, VectorClock};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Result of comparing a joining node's cluster configuration with a seed node.
pub enum ClusterConfigCheck {
    /// The seed node has no configured digest, so compatibility was not checked.
    Unchecked,
    /// The joining and seed nodes supplied the same configuration digest.
    Compatible,
    /// The joining and seed nodes supplied different configuration digests.
    Incompatible,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Initial seed-contact request sent before a node issues [`Join`].
pub struct InitJoin {
    /// Opaque digest of the joining node's compatibility-sensitive cluster configuration.
    pub joining_config_digest: Bytes,
}

impl RemoteMessage for InitJoin {
    const MANIFEST: &'static str = "kairo.cluster.init-join";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Positive seed-contact reply identifying a node that can receive [`Join`].
pub struct InitJoinAck {
    /// Canonical address of the seed node accepting the contact request.
    pub address: Address,
    /// Outcome of the seed node's configuration compatibility check.
    pub config_check: ClusterConfigCheck,
}

impl RemoteMessage for InitJoinAck {
    const MANIFEST: &'static str = "kairo.cluster.init-join-ack";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Negative seed-contact reply from a node that cannot currently accept [`Join`].
pub struct InitJoinNack {
    /// Canonical address of the seed node declining the contact request.
    pub address: Address,
}

impl RemoteMessage for InitJoinNack {
    const MANIFEST: &'static str = "kairo.cluster.init-join-nack";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Periodic liveness probe sent to a cluster heartbeat receiver.
pub struct Heartbeat {
    /// Unique incarnation of the node sending the probe.
    pub from: UniqueAddress,
    /// Sender-local sequence number copied into the response.
    pub sequence_nr: u64,
    /// Sender clock reading, in nanoseconds, copied into the response.
    pub creation_time_nanos: u64,
}

impl RemoteMessage for Heartbeat {
    const MANIFEST: &'static str = "kairo.cluster.heartbeat";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Reply to a [`Heartbeat`] used to update the sender's failure detector.
pub struct HeartbeatRsp {
    /// Unique incarnation of the node replying to the probe.
    pub from: UniqueAddress,
    /// Sequence number copied from the matching probe.
    pub sequence_nr: u64,
    /// Creation time copied from the matching probe.
    pub creation_time_nanos: u64,
}

impl RemoteMessage for HeartbeatRsp {
    const MANIFEST: &'static str = "kairo.cluster.heartbeat-rsp";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Request for a unique node incarnation to join cluster membership.
pub struct Join {
    /// Unique incarnation of the joining node.
    pub node: UniqueAddress,
    /// Roles advertised by the joining node.
    pub roles: Vec<String>,
}

impl RemoteMessage for Join {
    const MANIFEST: &'static str = "kairo.cluster.join";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Successful [`Join`] reply carrying the receiver's current membership state.
pub struct Welcome {
    /// Unique incarnation of the cluster node accepting the join.
    pub from: UniqueAddress,
    /// Membership state the joining node should adopt and merge.
    pub gossip: Gossip,
}

impl RemoteMessage for Welcome {
    const MANIFEST: &'static str = "kairo.cluster.welcome";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Full membership state exchanged during gossip convergence.
///
/// The destination is a [`UniqueAddress`] so a restarted node can reject an
/// envelope addressed to an earlier incarnation at the same canonical address.
pub struct GossipEnvelope {
    /// Unique incarnation that sent the envelope.
    pub from: UniqueAddress,
    /// Unique incarnation for which the envelope was intended.
    pub to: UniqueAddress,
    /// Sender-local sequence number for this full-gossip transmission.
    pub sequence_nr: u64,
    /// Full membership state offered to the destination.
    pub gossip: Gossip,
}

impl RemoteMessage for GossipEnvelope {
    const MANIFEST: &'static str = "kairo.cluster.gossip-envelope";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Compact causal summary used to negotiate whether full gossip is required.
pub struct GossipStatus {
    /// Unique incarnation advertising the status.
    pub from: UniqueAddress,
    /// Causal version of the sender's membership state.
    pub version: VectorClock,
    /// Stable digest of the nodes that have seen that version.
    pub seen_digest: Bytes,
}

impl RemoteMessage for GossipStatus {
    const MANIFEST: &'static str = "kairo.cluster.gossip-status";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Request for the member at a canonical address to leave gracefully.
pub struct Leave {
    /// Canonical address of the member that should leave.
    pub address: Address,
}

impl RemoteMessage for Leave {
    const MANIFEST: &'static str = "kairo.cluster.leave";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Request to mark the member at a canonical address as down.
pub struct Down {
    /// Canonical address of the member that should be downed.
    pub address: Address,
}

impl RemoteMessage for Down {
    const MANIFEST: &'static str = "kairo.cluster.down";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Confirmation that a leaving node incarnation has entered the exiting phase.
pub struct ExitingConfirmed {
    /// Unique incarnation that confirmed its exit.
    pub node: UniqueAddress,
}

impl RemoteMessage for ExitingConfirmed {
    const MANIFEST: &'static str = "kairo.cluster.exiting-confirmed";
    const VERSION: u16 = 1;
}

#[cfg(test)]
mod tests {
    use kairo_serialization::RemoteMessage;

    use super::*;

    #[test]
    fn cluster_system_manifests_are_stable() {
        assert_eq!(InitJoin::MANIFEST, "kairo.cluster.init-join");
        assert_eq!(InitJoinAck::MANIFEST, "kairo.cluster.init-join-ack");
        assert_eq!(InitJoinNack::MANIFEST, "kairo.cluster.init-join-nack");
        assert_eq!(Heartbeat::MANIFEST, "kairo.cluster.heartbeat");
        assert_eq!(HeartbeatRsp::MANIFEST, "kairo.cluster.heartbeat-rsp");
        assert_eq!(Join::MANIFEST, "kairo.cluster.join");
        assert_eq!(Welcome::MANIFEST, "kairo.cluster.welcome");
        assert_eq!(GossipEnvelope::MANIFEST, "kairo.cluster.gossip-envelope");
        assert_eq!(GossipStatus::MANIFEST, "kairo.cluster.gossip-status");
        assert_eq!(Leave::MANIFEST, "kairo.cluster.leave");
        assert_eq!(Down::MANIFEST, "kairo.cluster.down");
        assert_eq!(
            ExitingConfirmed::MANIFEST,
            "kairo.cluster.exiting-confirmed"
        );
        assert_eq!(Heartbeat::VERSION, 1);
        assert_eq!(HeartbeatRsp::VERSION, 1);
        assert_eq!(Join::VERSION, 1);
        assert!(!Heartbeat::MANIFEST.contains(std::any::type_name::<Heartbeat>()));
        assert!(!GossipEnvelope::MANIFEST.contains(std::any::type_name::<GossipEnvelope>()));
        assert!(!InitJoin::MANIFEST.contains(std::any::type_name::<InitJoin>()));
    }
}
