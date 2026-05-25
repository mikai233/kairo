//! Higher-level cluster utilities built on top of `kairo-cluster`.

mod codec;
mod protocol;
mod pubsub;
mod remote_tcp;
mod singleton;
mod system_inbound;
mod topic;

pub use codec::{
    PUBSUB_DELTA_SERIALIZER_ID, PUBSUB_PUBLISH_SERIALIZER_ID, PUBSUB_STATUS_SERIALIZER_ID,
    SINGLETON_HAND_OVER_DONE_SERIALIZER_ID, SINGLETON_HAND_OVER_IN_PROGRESS_SERIALIZER_ID,
    SINGLETON_HAND_OVER_TO_ME_SERIALIZER_ID, SINGLETON_TAKE_OVER_FROM_ME_SERIALIZER_ID,
    register_cluster_tools_protocol_codecs,
};
pub use protocol::{
    PubSubDelta, PubSubPublishEnvelope, PubSubStatus, SingletonHandOverDone,
    SingletonHandOverInProgress, SingletonHandOverToMe, SingletonTakeOverFromMe,
};
pub use pubsub::{
    CurrentTopics, DEFAULT_PUBSUB_REMOTE_PATH, DistributedPubSubMediatorActor,
    DistributedPubSubMediatorMsg, DistributedPubSubPublishReport, DistributedPubSubSnapshot,
    LocalPubSub, LocalPubSubActor, LocalPubSubMsg, PubSubBucket, PubSubDeliveryFailure,
    PubSubDeliveryPlan, PubSubDeliveryReport, PubSubDeliveryTarget, PubSubDeliveryTransport,
    PubSubGossipActor, PubSubGossipMsg, PubSubGossipPeer, PubSubGossipWireError,
    PubSubGossipWireInbound, PubSubGossipWireOutbound, PubSubRegistryDelta, PubSubRegistryEntry,
    PubSubRegistryKey, PubSubRegistryState, PubSubRemoteDeliveryError, PubSubRemoteDeliveryInbound,
    PubSubRemoteDeliveryOutbound, PubSubRemoteEnvelopeError, PubSubRemoteEnvelopeOutbound,
    PubSubRemoteTarget, PubSubSerializedGossip, PubSubSubscribeAck, PubSubTopicReport,
};
pub use remote_tcp::{
    ClusterToolsTcpAssociationRuntime, cluster_tools_association_identity_for,
    cluster_tools_lane_classifier,
};
pub use singleton::{
    DEFAULT_SINGLETON_MANAGER_REMOTE_PATH, LocalSingletonManagerActor, LocalSingletonManagerMsg,
    LocalSingletonManagerSnapshot, SingletonManagerActor, SingletonManagerEffect,
    SingletonManagerMsg, SingletonManagerRemoteError, SingletonManagerRemoteInbound,
    SingletonManagerRemoteOutbound, SingletonManagerRuntime, SingletonManagerSnapshot,
    SingletonManagerState, SingletonOldestChange, SingletonOldestObservation,
    SingletonOldestTracker, SingletonProxyActor, SingletonProxyMsg, SingletonProxySettings,
    SingletonProxySettingsError, SingletonProxySnapshot, SingletonProxyTarget, SingletonScope,
};
pub use system_inbound::{
    ClusterToolsSystemInbound, ClusterToolsSystemInboundError, is_cluster_tools_system_manifest,
};
pub use topic::{
    LocalTopic, TopicName, TopicPublishMode, TopicPublishReport, TopicSubscriptionChange,
};

#[cfg(test)]
mod tests;
