use std::collections::{BTreeMap, BTreeSet};

use kairo_actor::{ActorPath, ActorRef};

use crate::{LocalTopic, TopicName, TopicPublishMode, TopicPublishReport, TopicSubscriptionChange};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubSubTopicReport {
    pub topic: TopicName,
    pub report: TopicPublishReport,
}

#[derive(Debug, Clone)]
pub struct LocalPubSub<M> {
    topics: BTreeMap<TopicName, LocalTopic<M>>,
}

impl<M: Send + 'static> Default for LocalPubSub<M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<M: Send + 'static> LocalPubSub<M> {
    pub fn new() -> Self {
        Self {
            topics: BTreeMap::new(),
        }
    }

    pub fn topic_count(&self) -> usize {
        self.topics.len()
    }

    pub fn current_topics(&self) -> BTreeSet<TopicName> {
        self.topics.keys().cloned().collect()
    }

    pub fn topic(&self, topic: &TopicName) -> Option<&LocalTopic<M>> {
        self.topics.get(topic)
    }

    pub fn subscribe(
        &mut self,
        topic: TopicName,
        subscriber: ActorRef<M>,
    ) -> TopicSubscriptionChange {
        self.topic_mut(topic).subscribe(subscriber)
    }

    pub fn subscribe_group(
        &mut self,
        topic: TopicName,
        group: impl Into<String>,
        subscriber: ActorRef<M>,
    ) -> TopicSubscriptionChange {
        self.topic_mut(topic).subscribe_group(group, subscriber)
    }

    pub fn unsubscribe(&mut self, topic: &TopicName, subscriber: &ActorRef<M>) -> bool {
        let Some(topic_state) = self.topics.get_mut(topic) else {
            return false;
        };
        let removed = topic_state.unsubscribe(subscriber);
        self.remove_topic_if_empty(topic);
        removed
    }

    pub fn unsubscribe_group(
        &mut self,
        topic: &TopicName,
        group: &str,
        subscriber: &ActorRef<M>,
    ) -> bool {
        let Some(topic_state) = self.topics.get_mut(topic) else {
            return false;
        };
        let removed = topic_state.unsubscribe_group(group, subscriber);
        self.remove_topic_if_empty(topic);
        removed
    }

    pub fn remove_subscriber(&mut self, subscriber: &ActorRef<M>) -> Vec<TopicName> {
        self.remove_subscriber_path(subscriber.path())
    }

    pub fn remove_subscriber_path(&mut self, subscriber: &ActorPath) -> Vec<TopicName> {
        let mut changed_topics = Vec::new();
        for (topic, topic_state) in &mut self.topics {
            if topic_state.remove_subscriber_path(subscriber) {
                changed_topics.push(topic.clone());
            }
        }
        let empty_topics: Vec<_> = self
            .topics
            .iter()
            .filter(|(_, topic_state)| topic_state.is_empty())
            .map(|(topic, _)| topic.clone())
            .collect();
        for topic in empty_topics {
            self.topics.remove(&topic);
        }
        changed_topics
    }

    pub fn contains_subscriber_path(&self, subscriber: &ActorPath) -> bool {
        self.topics
            .values()
            .any(|topic_state| topic_state.contains_subscriber_path(subscriber))
    }

    pub fn publish(
        &mut self,
        topic: &TopicName,
        message: M,
        mode: TopicPublishMode,
    ) -> PubSubTopicReport
    where
        M: Clone,
    {
        let report = self
            .topics
            .get_mut(topic)
            .map(|topic_state| topic_state.publish(message, mode))
            .unwrap_or_else(TopicPublishReport::empty_for_no_subscribers);
        PubSubTopicReport {
            topic: topic.clone(),
            report,
        }
    }

    fn topic_mut(&mut self, topic: TopicName) -> &mut LocalTopic<M> {
        self.topics
            .entry(topic.clone())
            .or_insert_with(|| LocalTopic::new(topic))
    }

    fn remove_topic_if_empty(&mut self, topic: &TopicName) {
        if self.topics.get(topic).is_some_and(LocalTopic::is_empty) {
            self.topics.remove(topic);
        }
    }
}
