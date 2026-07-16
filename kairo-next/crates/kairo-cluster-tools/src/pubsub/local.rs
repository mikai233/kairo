#![deny(missing_docs)]

use std::collections::{BTreeMap, BTreeSet};

use kairo_actor::{ActorPath, ActorRef};

use crate::{LocalTopic, TopicName, TopicPublishMode, TopicPublishReport, TopicSubscriptionChange};

/// Topic identity paired with the immediate result of one publication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubSubTopicReport {
    /// Published topic.
    pub topic: TopicName,
    /// Local mailbox delivery counts.
    pub report: TopicPublishReport,
}

/// Logical actor path paired with the immediate result of one send.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubSubPathReport {
    /// Address-independent logical path used for lookup.
    pub path: String,
    /// Local mailbox delivery counts.
    pub report: TopicPublishReport,
}

/// Result of registering or removing a logical actor path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubSubPathRegistration {
    /// Address-independent logical path affected by the operation.
    pub path: String,
    /// Whether the stored path mapping changed.
    pub changed: bool,
}

/// Serialization-free local pubsub topic and logical-path registry.
///
/// Topic delivery uses typed local actor refs. Logical paths omit transport
/// address and actor-incarnation suffixes so a replacement registration can
/// take over the same application path without exposing remoting details.
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
    /// Creates an empty local pubsub registry.
    pub fn new() -> Self {
        Self {
            topics: BTreeMap::new(),
            paths: BTreeMap::new(),
        }
    }

    /// Returns the number of non-empty topics.
    pub fn topic_count(&self) -> usize {
        self.topics.len()
    }

    /// Returns current topic names in deterministic order.
    pub fn current_topics(&self) -> BTreeSet<TopicName> {
        self.topics.keys().cloned().collect()
    }

    /// Returns registered logical paths in deterministic order.
    pub fn current_paths(&self) -> BTreeSet<String> {
        self.paths.keys().cloned().collect()
    }

    /// Returns each topic's current group names in deterministic order.
    pub fn topic_groups(&self) -> BTreeMap<TopicName, BTreeSet<String>> {
        self.topics
            .iter()
            .map(|(topic, topic_state)| (topic.clone(), topic_state.group_names()))
            .collect()
    }

    /// Returns local state for `topic`, when it has subscribers.
    pub fn topic(&self, topic: &TopicName) -> Option<&LocalTopic<M>> {
        self.topics.get(topic)
    }

    /// Resolves a registered logical path to its current local actor ref.
    pub fn path_actor(&self, path: &str) -> Option<&ActorRef<M>> {
        self.paths.get(path)
    }

    /// Registers `actor` under its address-independent logical path.
    ///
    /// A new incarnation replaces an older mapping at the same logical path.
    pub fn register_path(&mut self, actor: ActorRef<M>) -> PubSubPathRegistration {
        let path = path_key(actor.path());
        let changed = self
            .paths
            .get(&path)
            .is_none_or(|existing| existing.path() != actor.path());
        self.paths.insert(path.clone(), actor);
        PubSubPathRegistration { path, changed }
    }

    /// Removes the actor registered at `path`, if any.
    pub fn remove_path(&mut self, path: &str) -> PubSubPathRegistration {
        let changed = self.paths.remove(path).is_some();
        PubSubPathRegistration {
            path: path.to_string(),
            changed,
        }
    }

    /// Adds a direct subscriber to `topic`.
    pub fn subscribe(
        &mut self,
        topic: TopicName,
        subscriber: ActorRef<M>,
    ) -> TopicSubscriptionChange {
        self.topic_mut(topic).subscribe(subscriber)
    }

    /// Adds a subscriber to a named group within `topic`.
    pub fn subscribe_group(
        &mut self,
        topic: TopicName,
        group: impl Into<String>,
        subscriber: ActorRef<M>,
    ) -> TopicSubscriptionChange {
        self.topic_mut(topic).subscribe_group(group, subscriber)
    }

    /// Removes a direct topic subscription and prunes an empty topic.
    pub fn unsubscribe(&mut self, topic: &TopicName, subscriber: &ActorRef<M>) -> bool {
        let Some(topic_state) = self.topics.get_mut(topic) else {
            return false;
        };
        let removed = topic_state.unsubscribe(subscriber);
        self.remove_topic_if_empty(topic);
        removed
    }

    /// Removes a grouped subscription and prunes empty group/topic state.
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

    /// Removes every topic and path registration for `subscriber`.
    ///
    /// The returned topic names are exactly those whose subscription state changed.
    pub fn remove_subscriber(&mut self, subscriber: &ActorRef<M>) -> Vec<TopicName> {
        self.remove_subscriber_path(subscriber.path())
    }

    /// Removes every topic and path registration matching `subscriber`.
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

    /// Returns whether a topic or logical-path registration matches `subscriber`.
    pub fn contains_subscriber_path(&self, subscriber: &ActorPath) -> bool {
        self.topics
            .values()
            .any(|topic_state| topic_state.contains_subscriber_path(subscriber))
            || self
                .paths
                .values()
                .any(|registered| registered.path() == subscriber)
    }

    /// Publishes to local topic subscribers using `mode`.
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

    /// Sends to the single local actor currently registered at `path`.
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

    /// Sends to the single local actor currently registered at `path`.
    ///
    /// Local state contains at most one actor per path, so this is equivalent
    /// to [`LocalPubSub::send_path`]. Distributed mediators apply the
    /// send-to-all fan-out before reaching this local boundary.
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

    /// Publishes to one live round-robin subscriber in `group`.
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
