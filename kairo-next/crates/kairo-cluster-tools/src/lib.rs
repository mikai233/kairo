//! Higher-level cluster utilities built on top of `kairo-cluster`.

mod pubsub;
mod singleton;
mod topic;

pub use pubsub::{
    CurrentTopics, DistributedPubSubMediatorActor, DistributedPubSubMediatorMsg,
    DistributedPubSubPublishReport, DistributedPubSubSnapshot, LocalPubSub, LocalPubSubActor,
    LocalPubSubMsg, PubSubBucket, PubSubDeliveryFailure, PubSubDeliveryPlan, PubSubDeliveryReport,
    PubSubDeliveryTarget, PubSubDeliveryTransport, PubSubGossipActor, PubSubGossipMsg,
    PubSubGossipPeer, PubSubRegistryDelta, PubSubRegistryEntry, PubSubRegistryKey,
    PubSubRegistryState, PubSubRemoteTarget, PubSubSubscribeAck, PubSubTopicReport,
};
pub use singleton::{
    LocalSingletonManagerActor, LocalSingletonManagerMsg, LocalSingletonManagerSnapshot,
    SingletonManagerActor, SingletonManagerEffect, SingletonManagerMsg, SingletonManagerRuntime,
    SingletonManagerSnapshot, SingletonManagerState, SingletonOldestChange,
    SingletonOldestObservation, SingletonOldestTracker, SingletonProxyActor, SingletonProxyMsg,
    SingletonProxySettings, SingletonProxySettingsError, SingletonProxySnapshot, SingletonScope,
};
pub use topic::{
    LocalTopic, TopicName, TopicPublishMode, TopicPublishReport, TopicSubscriptionChange,
};

#[cfg(test)]
mod tests;
