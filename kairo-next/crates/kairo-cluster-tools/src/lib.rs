//! Higher-level cluster utilities built on top of `kairo-cluster`.
//!
//! `kairo-cluster-tools` contains features that depend on cluster membership
//! without becoming the membership authority. Singleton tracking consumes
//! `kairo-cluster` member events to find the oldest eligible node, manager
//! actors turn oldest-member changes into explicit handover/start/stop effects,
//! proxy actors buffer while the singleton is unknown, and pubsub actors keep
//! local topics plus versioned distributed registrations. TCP peer runtimes and
//! system inbound routers connect those tools to `kairo-remote` association
//! caches while cluster truth remains gossip plus local failure-detector
//! observations.
//!
//! Composed applications can call [`register_distributed_pubsub`] before the
//! shared remote runtime binds and activate the resulting registration after
//! the cluster extension. [`DistributedPubSubExtension<M>`] then exposes one
//! typed mediator whose peers and gossip routes follow authoritative cluster
//! snapshots and member events without a second listener.
//! [`register_cluster_singleton`] similarly installs one shared manifest
//! router, while [`ClusterSingleton::init`] returns a typed
//! [`ClusterSingletonRef`] for each logical name. Membership events select the
//! oldest eligible owner and drive graceful handover without guessing
//! UID-bearing remote actor paths.
//!
//! Remote cluster-tools messages use stable
//! [`RemoteMessage`](kairo_serialization::RemoteMessage) manifests, serializer
//! ids, and registered codecs. Singleton handover messages and distributed
//! pubsub status/delta/publish envelopes encode explicit `UniqueAddress`,
//! topic, group, bucket-version, tombstone, recipient, and payload fields
//! instead of Rust type names, enum discriminants, or memory layout.
//!
//! ```
//! use kairo_actor::Address;
//! use kairo_cluster::{Member, MemberStatus, UniqueAddress};
//! use kairo_cluster_tools::{SingletonOldestTracker, SingletonScope};
//!
//! fn member(port: u16, uid: u64, up_number: u64) -> Member {
//!     let address = Address::new(
//!         "kairo",
//!         "cluster",
//!         Some("127.0.0.1".to_string()),
//!         Some(port),
//!     );
//!     Member::new(UniqueAddress::new(address, uid), vec!["backend".to_string()])
//!         .with_status(MemberStatus::Up)
//!         .with_up_number(up_number)
//! }
//!
//! let oldest = member(25520, 1, 1);
//! let self_member = member(25521, 2, 2);
//! let self_node = self_member.unique_address.clone();
//!
//! let (_tracker, observation) = SingletonOldestTracker::from_members(
//!     self_node,
//!     SingletonScope::for_role("backend"),
//!     [oldest.clone(), self_member],
//! );
//!
//! assert_eq!(observation.oldest(), Some(&oldest.unique_address));
//! assert!(observation.safe_to_be_oldest());
//! ```
//!
//! The public API is intentionally split by responsibility: singleton oldest
//! tracking, manager runtime, local manager actor, proxy route table, topic
//! state, local pubsub, distributed pubsub registry, pubsub gossip, remote
//! envelope adapters, and TCP peer ownership all live in focused modules rather
//! than in the crate root.

mod cluster_singleton;
mod codec;
mod extension;
mod protocol;
mod pubsub;
mod remote_tcp;
mod shared_remote_runtime;
mod singleton;
mod system_inbound;
mod tcp_peer_bootstrap;
mod tcp_peer_connector;
mod tcp_peer_reconnect;
mod tcp_peer_routes;
mod tcp_peer_runtime;
#[cfg(test)]
mod test_support;
mod topic;

pub use cluster_singleton::{
    ClusterSingleton, ClusterSingletonBootstrapError, ClusterSingletonConnectorMsg,
    ClusterSingletonConnectorSnapshot, ClusterSingletonRef, ClusterSingletonRegistration,
    ClusterSingletonSettings, SINGLETON_MESSAGE_MANIFESTS, SINGLETON_SYSTEM_MANIFESTS, Singleton,
    register_cluster_singleton,
};
pub use codec::{
    PUBSUB_DELTA_SERIALIZER_ID, PUBSUB_PATH_SERIALIZER_ID, PUBSUB_PUBLISH_SERIALIZER_ID,
    PUBSUB_STATUS_SERIALIZER_ID, SINGLETON_HAND_OVER_DONE_SERIALIZER_ID,
    SINGLETON_HAND_OVER_IN_PROGRESS_SERIALIZER_ID, SINGLETON_HAND_OVER_TO_ME_SERIALIZER_ID,
    SINGLETON_MESSAGE_SERIALIZER_ID, SINGLETON_TAKE_OVER_FROM_ME_SERIALIZER_ID,
    register_cluster_tools_protocol_codecs,
};
pub use extension::{
    DistributedPubSubBootstrapError, DistributedPubSubConnectorMsg,
    DistributedPubSubConnectorSnapshot, DistributedPubSubExtension, DistributedPubSubHandle,
    DistributedPubSubRegistration, DistributedPubSubSettings, PUBSUB_SYSTEM_MANIFESTS,
    register_distributed_pubsub,
};
pub use protocol::{
    PubSubDelta, PubSubPathEnvelope, PubSubPublishEnvelope, PubSubStatus, SingletonHandOverDone,
    SingletonHandOverInProgress, SingletonHandOverToMe, SingletonMessageEnvelope,
    SingletonTakeOverFromMe,
};
pub use pubsub::{
    CurrentTopics, DEFAULT_PUBSUB_REMOTE_PATH, DistributedPubSubMediatorActor,
    DistributedPubSubMediatorMsg, DistributedPubSubPublishReport, DistributedPubSubSendReport,
    DistributedPubSubSnapshot, LocalPubSub, LocalPubSubActor, LocalPubSubMsg, PubSubBucket,
    PubSubDeliveryFailure, PubSubDeliveryPlan, PubSubDeliveryReport, PubSubDeliveryTarget,
    PubSubDeliveryTransport, PubSubGossipActor, PubSubGossipMsg, PubSubGossipPeer,
    PubSubGossipWireError, PubSubGossipWireInbound, PubSubGossipWireOutbound,
    PubSubPathDeliveryFailure, PubSubPathDeliveryMode, PubSubPathDeliveryPlan,
    PubSubPathDeliveryReport, PubSubPathDeliveryTarget, PubSubPathRegistration, PubSubPathReport,
    PubSubRegistryDelta, PubSubRegistryEntry, PubSubRegistryKey, PubSubRegistryState,
    PubSubRemoteDeliveryError, PubSubRemoteDeliveryInbound, PubSubRemoteDeliveryOutbound,
    PubSubRemoteEnvelopeError, PubSubRemoteEnvelopeOutbound, PubSubRemoteTarget,
    PubSubSerializedGossip, PubSubSubscribeAck, PubSubTopicReport,
};
pub use remote_tcp::{
    ClusterToolsTcpAssociationRuntime, cluster_tools_association_identity_for,
    cluster_tools_lane_classifier,
};
pub use shared_remote_runtime::{
    CLUSTER_TOOLS_SYSTEM_MANIFESTS, register_cluster_tools_system_inbound,
};
pub use singleton::{
    DEFAULT_SINGLETON_MANAGER_REMOTE_PATH, LocalSingletonManagerActor, LocalSingletonManagerMsg,
    LocalSingletonManagerRemoteInbound, LocalSingletonManagerSnapshot, SingletonManagerActor,
    SingletonManagerEffect, SingletonManagerMsg, SingletonManagerRemoteError,
    SingletonManagerRemoteInbound, SingletonManagerRemoteOutbound, SingletonManagerRuntime,
    SingletonManagerSettings, SingletonManagerSettingsError, SingletonManagerSnapshot,
    SingletonManagerState, SingletonOldestChange, SingletonOldestObservation,
    SingletonOldestTracker, SingletonProxyActor, SingletonProxyMsg, SingletonProxySettings,
    SingletonProxySettingsError, SingletonProxySnapshot, SingletonProxyTarget, SingletonScope,
};
pub use system_inbound::{
    ClusterToolsSystemInbound, ClusterToolsSystemInboundError, is_cluster_tools_system_manifest,
};
pub use tcp_peer_bootstrap::{
    ClusterToolsTcpPeerBootstrap, ClusterToolsTcpPeerBootstrapError,
    ClusterToolsTcpPeerBootstrapResult, ClusterToolsTcpPeerBootstrapSettings,
};
pub use tcp_peer_connector::{
    ClusterToolsTcpPeerConnector, ClusterToolsTcpPeerConnectorMsg,
    ClusterToolsTcpPeerConnectorSettings, ClusterToolsTcpPeerConnectorSettingsError,
    ClusterToolsTcpPeerConnectorSnapshot,
};
pub use tcp_peer_reconnect::{
    ClusterToolsTcpPeerReconnectError, ClusterToolsTcpPeerReconnectPending,
    ClusterToolsTcpPeerReconnectReport, ClusterToolsTcpPeerReconnectResult,
    ClusterToolsTcpPeerReconnectSettings, ClusterToolsTcpPeerReconnectState,
};
pub use tcp_peer_routes::{
    ClusterToolsTcpPeerRouteError, ClusterToolsTcpPeerRouteReport, ClusterToolsTcpPeerRouteResult,
    ClusterToolsTcpPeerRoutes,
};
pub use tcp_peer_runtime::{
    ClusterToolsTcpPeerRuntime, ClusterToolsTcpPeerRuntimeError, ClusterToolsTcpPeerRuntimeResult,
    ClusterToolsTcpPeerRuntimeShutdownReport,
};
pub use topic::{
    LocalTopic, TopicName, TopicPublishMode, TopicPublishReport, TopicSubscriptionChange,
};

#[cfg(test)]
mod tests;
