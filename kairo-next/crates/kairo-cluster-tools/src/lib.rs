//! Higher-level cluster utilities built on top of `kairo-cluster`.

mod pubsub;
mod singleton;
mod topic;

pub use pubsub::{
    CurrentTopics, LocalPubSub, LocalPubSubActor, LocalPubSubMsg, PubSubBucket,
    PubSubDeliveryFailure, PubSubDeliveryPlan, PubSubDeliveryReport, PubSubDeliveryTarget,
    PubSubDeliveryTransport, PubSubGossipActor, PubSubGossipMsg, PubSubGossipPeer,
    PubSubRegistryDelta, PubSubRegistryEntry, PubSubRegistryKey, PubSubRegistryState,
    PubSubRemoteTarget, PubSubSubscribeAck, PubSubTopicReport,
};
pub use singleton::{
    SingletonManagerActor, SingletonManagerEffect, SingletonManagerMsg, SingletonManagerRuntime,
    SingletonManagerSnapshot, SingletonManagerState, SingletonOldestChange,
    SingletonOldestObservation, SingletonOldestTracker, SingletonScope,
};
pub use topic::{
    LocalTopic, TopicName, TopicPublishMode, TopicPublishReport, TopicSubscriptionChange,
};

#[cfg(test)]
mod tests;
