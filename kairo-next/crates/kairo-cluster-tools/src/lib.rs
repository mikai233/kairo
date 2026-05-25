//! Higher-level cluster utilities built on top of `kairo-cluster`.

mod codec;
mod protocol;
mod pubsub;
mod singleton;
mod topic;

pub use codec::{
    PUBSUB_DELTA_SERIALIZER_ID, PUBSUB_STATUS_SERIALIZER_ID, register_cluster_tools_protocol_codecs,
};
pub use protocol::{PubSubDelta, PubSubStatus};
pub use pubsub::{
    CurrentTopics, DEFAULT_PUBSUB_REMOTE_PATH, DistributedPubSubMediatorActor,
    DistributedPubSubMediatorMsg, DistributedPubSubPublishReport, DistributedPubSubSnapshot,
    LocalPubSub, LocalPubSubActor, LocalPubSubMsg, PubSubBucket, PubSubDeliveryFailure,
    PubSubDeliveryPlan, PubSubDeliveryReport, PubSubDeliveryTarget, PubSubDeliveryTransport,
    PubSubGossipActor, PubSubGossipMsg, PubSubGossipPeer, PubSubGossipWireError,
    PubSubGossipWireInbound, PubSubGossipWireOutbound, PubSubRegistryDelta, PubSubRegistryEntry,
    PubSubRegistryKey, PubSubRegistryState, PubSubRemoteEnvelopeError,
    PubSubRemoteEnvelopeOutbound, PubSubRemoteTarget, PubSubSerializedGossip, PubSubSubscribeAck,
    PubSubTopicReport,
};
pub use singleton::{
    LocalSingletonManagerActor, LocalSingletonManagerMsg, LocalSingletonManagerSnapshot,
    SingletonManagerActor, SingletonManagerEffect, SingletonManagerMsg, SingletonManagerRuntime,
    SingletonManagerSnapshot, SingletonManagerState, SingletonOldestChange,
    SingletonOldestObservation, SingletonOldestTracker, SingletonProxyActor, SingletonProxyMsg,
    SingletonProxySettings, SingletonProxySettingsError, SingletonProxySnapshot,
    SingletonProxyTarget, SingletonScope,
};
pub use topic::{
    LocalTopic, TopicName, TopicPublishMode, TopicPublishReport, TopicSubscriptionChange,
};

#[cfg(test)]
mod tests;
