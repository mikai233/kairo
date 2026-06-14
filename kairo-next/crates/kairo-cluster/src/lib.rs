//! Gossip-based cluster membership and cluster events.
//!
//! Cluster membership state is owned by gossip. Nodes exchange versioned
//! [`Gossip`] views, merge concurrent membership facts with [`VectorClock`],
//! carry observer-owned [`Reachability`] records from local failure detector
//! observations, and derive convergence, leaders, downing decisions, and
//! cluster events from that state. Discovery and configured seeds may provide
//! contact addresses, but they are not an authoritative membership store.
//!
//! This mirrors Pekko's observable cluster model while keeping the Rust API
//! explicit: membership data is plain structs and enums, failure detector
//! observations are local inputs, and remoting only transports already
//! addressed cluster protocol messages. Kairo deliberately does not use etcd,
//! Kubernetes leases, or any other central source of cluster truth.
//! `ClusterEventPublisher::with_diagnostics` can attach a backend-neutral
//! observer for gossip state-change diagnostics without selecting a logging or
//! metrics dependency.
//!
//! ```
//! use kairo_actor::Address;
//! use kairo_cluster::{
//!     Gossip, Member, MemberStatus, Reachability, ReachabilityStatus, UniqueAddress,
//! };
//!
//! fn node(port: u16, uid: u64) -> UniqueAddress {
//!     UniqueAddress::new(
//!         Address::new(
//!             "kairo",
//!             "cluster-docs",
//!             Some("127.0.0.1".to_string()),
//!             Some(port),
//!         ),
//!         uid,
//!     )
//! }
//!
//! let node_a = node(2551, 1);
//! let node_b = node(2552, 2);
//!
//! let joining_a = Member::new(node_a.clone(), vec![]).with_status(MemberStatus::Joining);
//! let up_a = Member::new(node_a.clone(), vec![]).with_status(MemberStatus::Up);
//! let up_b = Member::new(node_b.clone(), vec![]).with_status(MemberStatus::Up);
//!
//! let local = Gossip::from_members([joining_a])
//!     .seen(node_a.clone())
//!     .increment_version(&node_a);
//! let remote = Gossip::from_members([up_a, up_b]).increment_version(&node_b);
//!
//! let merged = local.merge(&remote);
//! assert_eq!(merged.member(&node_a).unwrap().status, MemberStatus::Up);
//! assert!(merged.member(&node_b).is_some());
//! assert!(merged.seen_by().is_empty());
//!
//! let reachability = Reachability::new().unreachable(node_a.clone(), node_b.clone());
//! assert_eq!(
//!     reachability.status_of(&node_b),
//!     ReachabilityStatus::Unreachable,
//! );
//! ```

mod association_peers;
mod cluster;
mod codec;
mod convergence;
mod downing;
mod downing_provider;
mod event_publisher;
mod events;
mod failure_detector;
mod gossip;
mod heartbeat;
mod heartbeat_actor;
mod heartbeat_remote;
mod leader;
mod leader_actions;
mod member;
mod membership_actor;
mod protocol;
mod reachability;
mod remote;
mod remote_tcp;
mod system_inbound;
#[cfg(test)]
mod tcp_membership_downing;
mod tcp_peer_bootstrap;
mod tcp_peer_connector;
mod tcp_peer_reconnect;
mod tcp_peer_routes;
mod tcp_peer_runtime;
mod vector_clock;
mod wire;

pub use association_peers::{
    ClusterAssociationPeerChange, ClusterAssociationPeerError, ClusterAssociationPeerResult,
    ClusterAssociationPeerState, ClusterAssociationPeerTarget,
};
pub use cluster::{Cluster, ClusterError};
pub use codec::{
    GOSSIP_ENVELOPE_SERIALIZER_ID, GossipEnvelopeCodec, HEARTBEAT_RSP_SERIALIZER_ID,
    HEARTBEAT_SERIALIZER_ID, HeartbeatCodec, HeartbeatRspCodec, JOIN_SERIALIZER_ID, JoinCodec,
    WELCOME_SERIALIZER_ID, WelcomeCodec, register_cluster_control_codecs,
    register_cluster_protocol_codecs,
};
pub use convergence::{Convergence, ConvergenceBlocker};
pub use downing::{
    DowningDecision, DowningHook, DowningPlan, LeaseMajorityHook, LeaseMajorityLease,
    LeaseMajoritySettings, LeaseMajoritySettingsError, NoDowning, SplitBrainResolverHook,
    SplitBrainStrategy, StaticDowningHook,
};
pub use downing_provider::{DowningProviderActor, DowningProviderMsg, DowningProviderSnapshot};
pub use event_publisher::{
    ClusterDiagnostic, ClusterDiagnosticFilter, ClusterDiagnostics, ClusterEventPublisher,
    ClusterEventPublisherMsg, ClusterSubscriptionEvent, ClusterSubscriptionInitialState,
    CurrentClusterState, SubscriptionInitialState,
};
pub use events::{ClusterEvent, ClusterEvents, MemberEvent, ReachabilityEvent};
pub use failure_detector::{
    DeadlineFailureDetector, DeadlineFailureDetectorSettings, FailureDetectorError,
    FailureDetectorRegistry,
};
pub use gossip::Gossip;
pub use heartbeat::{HeartbeatError, HeartbeatNodeRing, HeartbeatSenderState};
pub use heartbeat_actor::{
    HeartbeatClock, HeartbeatReceiver, HeartbeatReceiverMsg, HeartbeatSender, HeartbeatSenderMsg,
    HeartbeatSenderSettings, HeartbeatSenderSnapshot, SystemHeartbeatClock,
};
pub use heartbeat_remote::{
    ClusterHeartbeatRemoteError, DEFAULT_CLUSTER_HEARTBEAT_RECEIVER_PATH,
    DEFAULT_CLUSTER_HEARTBEAT_SENDER_PATH, HeartbeatRemoteReceiverInbound,
    HeartbeatRemoteReceiverOutbound, HeartbeatRemoteResponseInbound,
};
pub use leader::LeaderSelection;
pub use leader_actions::{LeaderActionError, LeaderActionOutcome, LeaderActions};
pub use member::{Member, MemberStatus, UniqueAddress};
pub use membership_actor::{ClusterMembership, ClusterMembershipMsg};
pub use protocol::{GossipEnvelope, Heartbeat, HeartbeatRsp, Join, Welcome};
pub use reachability::{Reachability, ReachabilityRecord, ReachabilityStatus};
pub use remote::{
    ClusterMembershipRemoteEnvelopeError, ClusterMembershipRemoteEnvelopeOutbound,
    DEFAULT_CLUSTER_MEMBERSHIP_REMOTE_PATH,
};
pub use remote_tcp::{
    ClusterTcpAssociationRuntime, cluster_association_identity_for, cluster_lane_classifier,
};
pub use system_inbound::{
    ClusterSystemInbound, ClusterSystemInboundError, is_cluster_system_manifest,
};
pub use tcp_peer_bootstrap::{
    ClusterTcpPeerBootstrap, ClusterTcpPeerBootstrapError, ClusterTcpPeerBootstrapIdentity,
    ClusterTcpPeerBootstrapResult, ClusterTcpPeerBootstrapSettings,
};
pub use tcp_peer_connector::{
    ClusterTcpPeerConnector, ClusterTcpPeerConnectorMsg, ClusterTcpPeerConnectorSettings,
    ClusterTcpPeerConnectorSettingsError, ClusterTcpPeerConnectorSnapshot,
};
pub use tcp_peer_reconnect::{
    ClusterTcpPeerReconnectError, ClusterTcpPeerReconnectPending, ClusterTcpPeerReconnectReport,
    ClusterTcpPeerReconnectResult, ClusterTcpPeerReconnectSettings, ClusterTcpPeerReconnectState,
};
pub use tcp_peer_routes::{
    ClusterTcpPeerRouteError, ClusterTcpPeerRouteReport, ClusterTcpPeerRouteResult,
    ClusterTcpPeerRoutes,
};
pub use tcp_peer_runtime::{
    ClusterTcpPeerRuntime, ClusterTcpPeerRuntimeError, ClusterTcpPeerRuntimeResult,
    ClusterTcpPeerRuntimeShutdownReport,
};
pub use vector_clock::{VectorClock, VectorClockNode, VectorClockOrdering};
pub use wire::{
    ClusterMembershipWireError, ClusterMembershipWireInbound, ClusterMembershipWireOutbound,
    ClusterMembershipWireOutboundActor, ClusterSerializedMembership,
};
