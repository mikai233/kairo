use std::collections::{BTreeMap, BTreeSet};

use kairo_actor::{ActorPath, ActorRef};

use crate::{LocalTopic, TopicName, TopicPublishMode, TopicPublishReport, TopicSubscriptionChange};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubSubTopicReport {
    pub topic: TopicName,
    pub report: TopicPublishReport,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubSubPathReport {
    pub path: String,
    pub report: TopicPublishReport,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubSubPathRegistration {
    pub path: String,
    pub changed: bool,
}

#[derive(Debug, Clone)]
pub struct LocalPubSub<M> {
    topics: BTreeMap<TopicName, LocalTopic<M>>,
    paths: BTreeMap<String, ActorRef<M>>,
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
            paths: BTreeMap::new(),
        }
    }

    pub fn topic_count(&self) -> usize {
        self.topics.len()
    }

    pub fn current_topics(&self) -> BTreeSet<TopicName> {
        self.topics.keys().cloned().collect()
    }

    pub fn current_paths(&self) -> BTreeSet<String> {
        self.paths.keys().cloned().collect()
    }

    pub fn topic_groups(&self) -> BTreeMap<TopicName, BTreeSet<String>> {
        self.topics
            .iter()
            .map(|(topic, topic_state)| (topic.clone(), topic_state.group_names()))
            .collect()
    }

    pub fn topic(&self, topic: &TopicName) -> Option<&LocalTopic<M>> {
        self.topics.get(topic)
    }

    pub fn path_actor(&self, path: &str) -> Option<&ActorRef<M>> {
        self.paths.get(path)
    }

    pub fn register_path(&mut self, actor: ActorRef<M>) -> PubSubPathRegistration {
        let path = path_key(actor.path());
        let changed = self
            .paths
            .get(&path)
            .is_none_or(|existing| existing.path() != actor.path());
        self.paths.insert(path.clone(), actor);
        PubSubPathRegistration { path, changed }
    }

    pub fn remove_path(&mut self, path: &str) -> PubSubPathRegistration {
        let changed = self.paths.remove(path).is_some();
        PubSubPathRegistration {
            path: path.to_string(),
            changed,
        }
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
        self.paths
            .retain(|_, registered| registered.path() != subscriber);
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
            || self
                .paths
                .values()
                .any(|registered| registered.path() == subscriber)
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
        self.remove_topic_if_empty(topic);
        PubSubTopicReport {
            topic: topic.clone(),
            report,
        }
    }

    pub fn send_path(&mut self, path: &str, message: M) -> PubSubPathReport
    where
        M: Clone,
    {
        let report = self.deliver_path(path, message);
        PubSubPathReport {
            path: path.to_string(),
            report,
        }
    }

    pub fn send_path_to_all(&mut self, path: &str, message: M) -> PubSubPathReport
    where
        M: Clone,
    {
        let report = self.deliver_path(path, message);
        PubSubPathReport {
            path: path.to_string(),
            report,
        }
    }

    pub fn publish_group(&mut self, topic: &TopicName, group: &str, message: M) -> PubSubTopicReport
    where
        M: Clone,
    {
        let report = self
            .topics
            .get_mut(topic)
            .map(|topic_state| topic_state.publish_group(group, message))
            .unwrap_or_else(TopicPublishReport::empty_for_no_subscribers);
        self.remove_topic_if_empty(topic);
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

    fn deliver_path(&mut self, path: &str, message: M) -> TopicPublishReport
    where
        M: Clone,
    {
        let mut report = TopicPublishReport::empty_for_no_subscribers();
        let Some(actor) = self.paths.get(path).cloned() else {
            return report;
        };
        let delivered = record_path_delivery(&mut report, &actor, message);
        if !delivered && actor.is_stopped() {
            self.paths.remove(path);
        }
        report
    }
}

fn record_path_delivery<M: Send + 'static>(
    report: &mut TopicPublishReport,
    actor: &ActorRef<M>,
    message: M,
) -> bool {
    match actor.tell(message) {
        Ok(()) => {
            report.delivered += 1;
            report.no_subscribers = false;
            true
        }
        Err(_) if actor.is_stopped() => false,
        Err(_) => {
            report.failed += 1;
            report.no_subscribers = false;
            true
        }
    }
}

fn path_key(path: &ActorPath) -> String {
    let logical = path
        .as_str()
        .split_once("://")
        .and_then(|(_, rest)| rest.split_once('/').map(|(_, path)| format!("/{path}")))
        .unwrap_or_else(|| path.as_str().to_string());
    logical
        .split('/')
        .map(|segment| segment.split_once('#').map_or(segment, |(name, _)| name))
        .collect::<Vec<_>>()
        .join("/")
}
