use std::collections::{BTreeMap, BTreeSet};

use kairo_actor::{ActorPath, ActorRef};

use super::TopicName;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopicPublishMode {
    Broadcast,
    OnePerGroup,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicPublishReport {
    pub delivered: usize,
    pub failed: usize,
    pub no_subscribers: bool,
}

impl TopicPublishReport {
    pub fn empty_for_no_subscribers() -> Self {
        Self {
            delivered: 0,
            failed: 0,
            no_subscribers: true,
        }
    }

    fn record_success(&mut self) {
        self.no_subscribers = false;
        self.delivered += 1;
    }

    fn record_failure(&mut self) {
        self.no_subscribers = false;
        self.failed += 1;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicSubscriptionChange {
    pub inserted: bool,
}

#[derive(Debug, Clone)]
pub struct LocalTopic<M> {
    name: TopicName,
    subscribers: Vec<ActorRef<M>>,
    groups: BTreeMap<String, LocalTopicGroup<M>>,
}

impl<M: Send + 'static> LocalTopic<M> {
    pub fn new(name: TopicName) -> Self {
        Self {
            name,
            subscribers: Vec::new(),
            groups: BTreeMap::new(),
        }
    }

    pub fn name(&self) -> &TopicName {
        &self.name
    }

    pub fn subscriber_count(&self) -> usize {
        self.subscribers.len()
            + self
                .groups
                .values()
                .map(LocalTopicGroup::subscriber_count)
                .sum::<usize>()
    }

    pub fn is_empty(&self) -> bool {
        self.subscriber_count() == 0
    }

    pub fn group_count(&self) -> usize {
        self.groups.len()
    }

    pub fn group_names(&self) -> BTreeSet<String> {
        self.groups.keys().cloned().collect()
    }

    pub fn group_subscriber_count(&self, group: &str) -> usize {
        self.groups
            .get(group)
            .map(LocalTopicGroup::subscriber_count)
            .unwrap_or_default()
    }

    pub fn subscribe(&mut self, subscriber: ActorRef<M>) -> TopicSubscriptionChange {
        insert_unique(&mut self.subscribers, subscriber)
    }

    pub fn subscribe_group(
        &mut self,
        group: impl Into<String>,
        subscriber: ActorRef<M>,
    ) -> TopicSubscriptionChange {
        self.groups
            .entry(group.into())
            .or_default()
            .subscribe(subscriber)
    }

    pub fn unsubscribe(&mut self, subscriber: &ActorRef<M>) -> bool {
        remove_by_path(&mut self.subscribers, subscriber.path())
    }

    pub fn unsubscribe_group(&mut self, group: &str, subscriber: &ActorRef<M>) -> bool {
        let Some(group_state) = self.groups.get_mut(group) else {
            return false;
        };
        let removed = group_state.unsubscribe(subscriber);
        if group_state.subscribers.is_empty() {
            self.groups.remove(group);
        }
        removed
    }

    pub fn remove_subscriber(&mut self, subscriber: &ActorRef<M>) -> bool {
        self.remove_subscriber_path(subscriber.path())
    }

    pub fn remove_subscriber_path(&mut self, subscriber: &ActorPath) -> bool {
        let mut removed = remove_by_path(&mut self.subscribers, subscriber);
        let empty_groups: Vec<_> = self
            .groups
            .iter_mut()
            .filter_map(|(group_name, group)| {
                removed |= group.remove_subscriber_path(subscriber);
                group.subscribers.is_empty().then(|| group_name.clone())
            })
            .collect();
        for group_name in empty_groups {
            self.groups.remove(&group_name);
        }
        removed
    }

    pub fn contains_subscriber_path(&self, subscriber: &ActorPath) -> bool {
        self.subscribers
            .iter()
            .any(|existing| existing.path() == subscriber)
            || self
                .groups
                .values()
                .any(|group| group.contains_subscriber_path(subscriber))
    }

    pub fn publish(&mut self, message: M, mode: TopicPublishMode) -> TopicPublishReport
    where
        M: Clone,
    {
        match mode {
            TopicPublishMode::Broadcast => self.publish_broadcast(message),
            TopicPublishMode::OnePerGroup => self.publish_one_per_group(message),
        }
    }

    pub fn publish_group(&mut self, group: &str, message: M) -> TopicPublishReport
    where
        M: Clone,
    {
        let mut report = TopicPublishReport::empty_for_no_subscribers();
        if let Some(group_state) = self.groups.get_mut(group) {
            group_state.publish_one(message, &mut report);
            if group_state.subscribers.is_empty() {
                self.groups.remove(group);
            }
        }
        report
    }

    fn publish_broadcast(&mut self, message: M) -> TopicPublishReport
    where
        M: Clone,
    {
        let mut report = TopicPublishReport::empty_for_no_subscribers();
        self.subscribers
            .retain(|subscriber| record_delivery(&mut report, subscriber, message.clone()));
        let empty_groups: Vec<_> = self
            .groups
            .iter_mut()
            .filter_map(|(group_name, group)| {
                group
                    .subscribers
                    .retain(|subscriber| record_delivery(&mut report, subscriber, message.clone()));
                if group.subscribers.is_empty() {
                    Some(group_name.clone())
                } else {
                    None
                }
            })
            .collect();
        for group_name in empty_groups {
            self.groups.remove(&group_name);
        }
        report
    }

    fn publish_one_per_group(&mut self, message: M) -> TopicPublishReport
    where
        M: Clone,
    {
        let mut report = TopicPublishReport::empty_for_no_subscribers();
        for group in self.groups.values_mut() {
            group.publish_one(message.clone(), &mut report);
        }
        let empty_groups: Vec<_> = self
            .groups
            .iter()
            .filter(|(_, group)| group.subscribers.is_empty())
            .map(|(group_name, _)| group_name.clone())
            .collect();
        for group_name in empty_groups {
            self.groups.remove(&group_name);
        }
        report
    }
}

#[derive(Debug, Clone)]
struct LocalTopicGroup<M> {
    subscribers: Vec<ActorRef<M>>,
    next_index: usize,
}

impl<M> Default for LocalTopicGroup<M> {
    fn default() -> Self {
        Self {
            subscribers: Vec::new(),
            next_index: 0,
        }
    }
}

impl<M: Send + 'static> LocalTopicGroup<M> {
    fn subscriber_count(&self) -> usize {
        self.subscribers.len()
    }

    fn subscribe(&mut self, subscriber: ActorRef<M>) -> TopicSubscriptionChange {
        insert_unique(&mut self.subscribers, subscriber)
    }

    fn unsubscribe(&mut self, subscriber: &ActorRef<M>) -> bool {
        self.remove_subscriber_path(subscriber.path())
    }

    fn remove_subscriber_path(&mut self, subscriber: &ActorPath) -> bool {
        let removed = remove_by_path(&mut self.subscribers, subscriber);
        if !self.subscribers.is_empty() {
            self.next_index %= self.subscribers.len();
        } else {
            self.next_index = 0;
        }
        removed
    }

    fn contains_subscriber_path(&self, subscriber: &ActorPath) -> bool {
        self.subscribers
            .iter()
            .any(|existing| existing.path() == subscriber)
    }

    fn next_subscriber(&mut self) -> Option<ActorRef<M>> {
        if self.subscribers.is_empty() {
            return None;
        }
        let index = self.next_index % self.subscribers.len();
        self.next_index = (index + 1) % self.subscribers.len();
        Some(self.subscribers[index].clone())
    }

    fn publish_one(&mut self, message: M, report: &mut TopicPublishReport)
    where
        M: Clone,
    {
        let attempts = self.subscribers.len();
        for _ in 0..attempts {
            let Some(subscriber) = self.next_subscriber() else {
                return;
            };
            if record_delivery(report, &subscriber, message.clone()) {
                return;
            }
            self.remove_subscriber_path(subscriber.path());
        }
    }
}

fn insert_unique<M: Send + 'static>(
    subscribers: &mut Vec<ActorRef<M>>,
    subscriber: ActorRef<M>,
) -> TopicSubscriptionChange {
    let inserted = !subscribers
        .iter()
        .any(|existing| existing.path() == subscriber.path());
    if inserted {
        subscribers.push(subscriber);
    }
    TopicSubscriptionChange { inserted }
}

fn remove_by_path<M: Send + 'static>(subscribers: &mut Vec<ActorRef<M>>, path: &ActorPath) -> bool {
    let before = subscribers.len();
    subscribers.retain(|subscriber| subscriber.path() != path);
    subscribers.len() != before
}

fn record_delivery<M: Send + 'static>(
    report: &mut TopicPublishReport,
    subscriber: &ActorRef<M>,
    message: M,
) -> bool {
    match subscriber.tell(message) {
        Ok(()) => {
            report.record_success();
            true
        }
        Err(_) if subscriber.is_stopped() => false,
        Err(_) => {
            report.record_failure();
            true
        }
    }
}
