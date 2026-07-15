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
mod daemon_bootstrap;
mod downing;
mod downing_provider;
mod event_publisher;
mod events;
mod extension;
mod failure_detector;
mod gossip;
mod gossip_process;
mod heartbeat;
mod heartbeat_actor;
mod heartbeat_connector;
mod heartbeat_remote;
mod init_join_responder;
mod leader;
mod leader_actions;
mod leave_coordinator;
mod member;
mod membership_actor;
mod protocol;
mod reachability;
mod remote;
mod remote_peer_connector;
mod remote_tcp;
mod seed_join;
mod seed_join_actor;
mod seed_join_wire;
mod shared_remote_runtime;
#[cfg(test)]
mod shared_remote_runtime_tests;
mod system_inbound;
#[cfg(test)]
mod tcp_membership_downing;
mod tcp_peer_bootstrap;
mod tcp_peer_connector;
mod tcp_peer_reconnect;
mod tcp_peer_routes;
mod tcp_peer_runtime;
#[cfg(test)]
mod test_support;
mod vector_clock;
mod wire;

pub use association_peers::{
    ClusterAssociationPeerChange, ClusterAssociationPeerError, ClusterAssociationPeerResult,
    ClusterAssociationPeerState, ClusterAssociationPeerTarget,
};
pub use cluster::{Cluster, ClusterError};
pub use codec::{
    DOWN_SERIALIZER_ID, DownCodec, EXITING_CONFIRMED_SERIALIZER_ID, ExitingConfirmedCodec,
    GOSSIP_ENVELOPE_SERIALIZER_ID, GOSSIP_STATUS_SERIALIZER_ID, GossipEnvelopeCodec,
    GossipStatusCodec, HEARTBEAT_RSP_SERIALIZER_ID, HEARTBEAT_SERIALIZER_ID, HeartbeatCodec,
    HeartbeatRspCodec, INIT_JOIN_ACK_SERIALIZER_ID, INIT_JOIN_NACK_SERIALIZER_ID,
    INIT_JOIN_SERIALIZER_ID, InitJoinAckCodec, InitJoinCodec, InitJoinNackCodec,
    JOIN_SERIALIZER_ID, JoinCodec, LEAVE_SERIALIZER_ID, LeaveCodec, WELCOME_SERIALIZER_ID,
    WelcomeCodec, register_cluster_control_codecs, register_cluster_protocol_codecs,
};
pub use convergence::{Convergence, ConvergenceBlocker};
pub use daemon_bootstrap::{
    ClusterDaemonBootstrapError, ClusterDaemonBootstrapSettings, ClusterDaemonHandle,
    ClusterDaemonRegistration, register_cluster_daemon,
};
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
pub use extension::ClusterExtension;
pub use failure_detector::{
    DeadlineFailureDetector, DeadlineFailureDetectorSettings, FailureDetectorError,
    FailureDetectorRegistry,
};
pub use gossip::Gossip;
pub use gossip_process::{
    ClusterGossipAction, ClusterGossipProcess, ClusterGossipProcessMsg,
    ClusterGossipProcessSettings, ClusterGossipProcessSettingsError, ClusterGossipState,
    ClusterGossipWireError, ClusterGossipWireInbound, ClusterGossipWireOutbound,
};
pub use heartbeat::{HeartbeatError, HeartbeatNodeRing, HeartbeatSenderState};
pub use heartbeat_actor::{
    HeartbeatClock, HeartbeatReceiver, HeartbeatReceiverMsg, HeartbeatSender, HeartbeatSenderMsg,
    HeartbeatSenderSettings, HeartbeatSenderSnapshot, SystemHeartbeatClock,
};
pub use heartbeat_connector::{ClusterHeartbeatConnector, ClusterHeartbeatConnectorMsg};
pub use heartbeat_remote::{
    ClusterHeartbeatRemoteError, DEFAULT_CLUSTER_HEARTBEAT_RECEIVER_PATH,
    DEFAULT_CLUSTER_HEARTBEAT_SENDER_PATH, HeartbeatRemoteReceiverInbound,
    HeartbeatRemoteReceiverOutbound, HeartbeatRemoteResponseInbound,
};
pub use init_join_responder::{
    ClusterInitJoinLifecycle, ClusterInitJoinResponder, ClusterInitJoinResponderMsg,
    ClusterInitJoinResponderPort, ClusterInitJoinResponderState,
};
pub use leader::LeaderSelection;
pub use leader_actions::{LeaderActionError, LeaderActionOutcome, LeaderActions};
pub use member::{Member, MemberStatus, UniqueAddress};
pub use membership_actor::{ClusterMembership, ClusterMembershipMsg};
pub use protocol::{
    ClusterConfigCheck, Down, ExitingConfirmed, GossipEnvelope, GossipStatus, Heartbeat,
    HeartbeatRsp, InitJoin, InitJoinAck, InitJoinNack, Join, Leave, Welcome,
};
pub use reachability::{Reachability, ReachabilityRecord, ReachabilityStatus};
pub use remote::{
    ClusterMembershipRemoteEnvelopeError, ClusterMembershipRemoteEnvelopeOutbound,
    DEFAULT_CLUSTER_MEMBERSHIP_REMOTE_PATH,
};
pub use remote_peer_connector::{
    ClusterRemotePeerConnector, ClusterRemotePeerConnectorMsg, ClusterRemotePeerConnectorSnapshot,
};
pub use remote_tcp::{
    ClusterTcpAssociationRuntime, cluster_association_identity_for, cluster_lane_classifier,
};
pub use seed_join::{
    ClusterSeedJoinEffect, ClusterSeedJoinError, ClusterSeedJoinPhase, ClusterSeedJoinState,
};
pub use seed_join_actor::{
    ClusterSeedJoinProcess, ClusterSeedJoinProcessMsg, ClusterSeedJoinProcessSettings,
    ClusterSeedJoinProcessSettingsError, ClusterSeedJoinProcessSnapshot,
};
pub use seed_join_wire::{
    ClusterInitJoinRequest, ClusterInitJoinResponse, ClusterSeedJoinIncompatible,
    ClusterSeedJoinWireError, ClusterSeedJoinWireInbound, ClusterSeedJoinWireOutbound,
    ClusterSeedJoinWireOutboundActor,
};
pub use shared_remote_runtime::{CLUSTER_SYSTEM_MANIFESTS, register_cluster_system_inbound};
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

#[cfg(test)]
mod source_guards {
    #[test]
    fn cluster_sources_do_not_introduce_authoritative_membership_store()
    -> Result<(), Box<dyn std::error::Error>> {
        let crate_src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let forbidden_terms = [
            concat!("et", "cd"),
            concat!("kuber", "netes"),
            "membership_store",
            "membershipstore",
            "centralmembershipstore",
        ];

        let mut files = Vec::new();
        collect_active_rs_files(&crate_src, &mut files)?;

        for file in files {
            let source = std::fs::read_to_string(&file)?.replace("\r\n", "\n");
            for (line_index, line) in source.lines().enumerate() {
                let trimmed = line.trim_start();
                if trimmed.starts_with("//") {
                    continue;
                }

                let normalized_line = line.to_ascii_lowercase();
                for term in forbidden_terms {
                    assert!(
                        !normalized_line.contains(term),
                        "{}:{} must keep cluster membership gossip-based; discovery may provide contacts but not membership truth",
                        file.display(),
                        line_index + 1
                    );
                }
            }
        }

        Ok(())
    }

    fn collect_active_rs_files(
        directory: &std::path::Path,
        files: &mut Vec<std::path::PathBuf>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        for entry in std::fs::read_dir(directory)? {
            let entry = entry?;
            let path = entry.path();
            let file_name = path.file_name().and_then(|name| name.to_str());
            if path.is_dir() {
                if file_name == Some("tests") {
                    continue;
                }
                collect_active_rs_files(&path, files)?;
            } else if path.extension().and_then(|extension| extension.to_str()) == Some("rs")
                && !file_name.is_some_and(|name| name == "lib.rs" || name.contains("test"))
            {
                files.push(path);
            }
        }

        Ok(())
    }
}
