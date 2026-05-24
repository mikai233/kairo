//! Higher-level cluster utilities built on top of `kairo-cluster`.

mod pubsub;
mod singleton;
mod topic;

pub use pubsub::{LocalPubSub, PubSubTopicReport};
pub use singleton::{
    SingletonManagerEffect, SingletonManagerRuntime, SingletonManagerState, SingletonOldestChange,
    SingletonOldestObservation, SingletonOldestTracker, SingletonScope,
};
pub use topic::{
    LocalTopic, TopicName, TopicPublishMode, TopicPublishReport, TopicSubscriptionChange,
};

#[cfg(test)]
mod tests;
