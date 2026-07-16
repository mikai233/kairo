#![deny(missing_docs)]

use std::collections::BTreeSet;

use kairo_actor::{Actor, ActorRef, ActorResult, Context, Signal};

use crate::{
    LocalPubSub, PubSubPathRegistration, PubSubPathReport, PubSubTopicReport, TopicName,
    TopicPublishMode,
};

/// Actor wrapper around serialization-free [`LocalPubSub`] state.
///
/// The actor watches subscribed and path-registered actors, removing every
/// matching registration when a watched incarnation terminates.
pub struct LocalPubSubActor<M> {
    state: LocalPubSub<M>,
}

impl<M> Default for LocalPubSubActor<M>
where
    M: Send + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<M> LocalPubSubActor<M>
where
    M: Send + 'static,
{
    /// Creates an actor with an empty local pubsub registry.
    pub fn new() -> Self {
        Self {
            state: LocalPubSub::new(),
        }
    }

    /// Returns the current registry state for diagnostics and focused tests.
    pub fn state(&self) -> &LocalPubSub<M> {
        &self.state
    }
}

/// Typed local pubsub actor protocol.
///
/// Application messages remain ordinary local Rust values and require neither
/// [`RemoteMessage`](kairo_serialization::RemoteMessage) nor a codec.
pub enum LocalPubSubMsg<M>
where
    M: Send + 'static,
{
    /// Adds a direct topic subscriber.
    Subscribe {
        /// Topic to update.
        topic: TopicName,
        /// Local actor that receives published `M` values.
        subscriber: ActorRef<M>,
        /// Optional recipient for the idempotent subscription result.
        reply_to: Option<ActorRef<PubSubSubscribeAck>>,
    },
    /// Adds a subscriber to a named topic group.
    SubscribeGroup {
        /// Topic to update.
        topic: TopicName,
        /// Group used for one-message-per-group selection.
        group: String,
        /// Local actor that receives selected `M` values.
        subscriber: ActorRef<M>,
        /// Optional recipient for the idempotent subscription result.
        reply_to: Option<ActorRef<PubSubSubscribeAck>>,
    },
    /// Removes a direct topic subscription.
    Unsubscribe {
        /// Topic to update.
        topic: TopicName,
        /// Subscriber actor path to remove.
        subscriber: ActorRef<M>,
        /// Optional recipient for the removal result.
        reply_to: Option<ActorRef<PubSubSubscribeAck>>,
    },
    /// Removes one grouped topic subscription.
    UnsubscribeGroup {
        /// Topic to update.
        topic: TopicName,
        /// Group containing the registration.
        group: String,
        /// Subscriber actor path to remove.
        subscriber: ActorRef<M>,
        /// Optional recipient for the removal result.
        reply_to: Option<ActorRef<PubSubSubscribeAck>>,
    },
    /// Registers a local actor under its address-independent logical path.
    Put {
        /// Actor to register.
        actor: ActorRef<M>,
        /// Optional recipient for the registration result.
        reply_to: Option<ActorRef<PubSubPathRegistration>>,
    },
    /// Removes a logical-path registration.
    RemovePath {
        /// Address-independent path to remove.
        path: String,
        /// Optional recipient for the removal result.
        reply_to: Option<ActorRef<PubSubPathRegistration>>,
    },
    /// Publishes a message to a topic using an explicit selection mode.
    Publish {
        /// Destination topic.
        topic: TopicName,
        /// Typed local message.
        message: M,
        /// Broadcast or one-per-group selection.
        mode: TopicPublishMode,
        /// Optional recipient for immediate delivery counts.
        reply_to: Option<ActorRef<PubSubTopicReport>>,
    },
    /// Publishes to one subscriber in a selected topic group.
    PublishGroup {
        /// Destination topic.
        topic: TopicName,
        /// Selected group.
        group: String,
        /// Typed local message.
        message: M,
        /// Optional recipient for immediate delivery counts.
        reply_to: Option<ActorRef<PubSubTopicReport>>,
    },
    /// Sends to the actor registered at a logical path.
    Send {
        /// Address-independent destination path.
        path: String,
        /// Typed local message.
        message: M,
        /// Optional recipient for immediate delivery counts.
        reply_to: Option<ActorRef<PubSubPathReport>>,
    },
    /// Applies local delivery for a distributed send-to-all operation.
    SendToAll {
        /// Address-independent destination path.
        path: String,
        /// Typed local message.
        message: M,
        /// Optional recipient for immediate delivery counts.
        reply_to: Option<ActorRef<PubSubPathReport>>,
    },
    /// Lists currently non-empty local topics.
    GetTopics {
        /// Recipient for the topic snapshot.
        reply_to: ActorRef<CurrentTopics>,
    },
    /// Removes an actor from every topic, group, and path registration.
    RemoveSubscriber {
        /// Actor path to remove.
        subscriber: ActorRef<M>,
        /// Optional recipient for the topic names whose state changed.
        reply_to: Option<ActorRef<Vec<TopicName>>>,
    },
}

/// Idempotent result of a subscribe or unsubscribe operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubSubSubscribeAck {
    /// Topic affected by the operation.
    pub topic: TopicName,
    /// Group affected by the operation, or `None` for a direct subscription.
    pub group: Option<String>,
    /// Whether registry state changed.
    pub changed: bool,
}

/// Snapshot of currently non-empty local topics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentTopics {
    /// Topic names in deterministic order.
    pub topics: BTreeSet<TopicName>,
}

impl<M> Actor for LocalPubSubActor<M>
where
    M: Clone + Send + 'static,
{
    type Msg = LocalPubSubMsg<M>;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            LocalPubSubMsg::Subscribe {
                topic,
                subscriber,
                reply_to,
            } => {
                ctx.watch(&subscriber)?;
                let change = self.state.subscribe(topic.clone(), subscriber);
                send_subscribe_ack(reply_to, topic, None, change.inserted);
            }
            LocalPubSubMsg::SubscribeGroup {
                topic,
                group,
                subscriber,
                reply_to,
            } => {
                ctx.watch(&subscriber)?;
                let change = self
                    .state
                    .subscribe_group(topic.clone(), group.clone(), subscriber);
                send_subscribe_ack(reply_to, topic, Some(group), change.inserted);
            }
            LocalPubSubMsg::Unsubscribe {
                topic,
                subscriber,
                reply_to,
            } => {
                let removed = self.state.unsubscribe(&topic, &subscriber);
                self.unwatch_if_unsubscribed(ctx, &subscriber);
                send_subscribe_ack(reply_to, topic, None, removed);
            }
            LocalPubSubMsg::UnsubscribeGroup {
                topic,
                group,
                subscriber,
                reply_to,
            } => {
                let removed = self.state.unsubscribe_group(&topic, &group, &subscriber);
                self.unwatch_if_unsubscribed(ctx, &subscriber);
                send_subscribe_ack(reply_to, topic, Some(group), removed);
            }
            LocalPubSubMsg::Put { actor, reply_to } => {
                ctx.watch(&actor)?;
                let registration = self.state.register_path(actor);
                send_path_registration(reply_to, registration);
            }
            LocalPubSubMsg::RemovePath { path, reply_to } => {
                let actor = self.state.path_actor(&path).cloned();
                let registration = self.state.remove_path(&path);
                if let Some(actor) = actor {
                    self.unwatch_if_unsubscribed(ctx, &actor);
                }
                send_path_registration(reply_to, registration);
            }
            LocalPubSubMsg::Publish {
                topic,
                message,
                mode,
                reply_to,
            } => {
                let report = self.state.publish(&topic, message, mode);
                if let Some(reply_to) = reply_to {
                    let _ = reply_to.tell(report);
                }
            }
            LocalPubSubMsg::PublishGroup {
                topic,
                group,
                message,
                reply_to,
            } => {
                let report = self.state.publish_group(&topic, &group, message);
                if let Some(reply_to) = reply_to {
                    let _ = reply_to.tell(report);
                }
            }
            LocalPubSubMsg::Send {
                path,
                message,
                reply_to,
            } => {
                let report = self.state.send_path(&path, message);
                if let Some(reply_to) = reply_to {
                    let _ = reply_to.tell(report);
                }
            }
            LocalPubSubMsg::SendToAll {
                path,
                message,
                reply_to,
            } => {
                let report = self.state.send_path_to_all(&path, message);
                if let Some(reply_to) = reply_to {
                    let _ = reply_to.tell(report);
                }
            }
            LocalPubSubMsg::GetTopics { reply_to } => {
                let _ = reply_to.tell(CurrentTopics {
                    topics: self.state.current_topics(),
                });
            }
            LocalPubSubMsg::RemoveSubscriber {
                subscriber,
                reply_to,
            } => {
                let changed = self.state.remove_subscriber(&subscriber);
                self.unwatch_if_unsubscribed(ctx, &subscriber);
                if let Some(reply_to) = reply_to {
                    let _ = reply_to.tell(changed);
                }
            }
        }
        Ok(())
    }

    fn signal(&mut self, _ctx: &mut Context<Self::Msg>, signal: Signal) -> ActorResult {
        if let Signal::Terminated(actor) = signal {
            self.state.remove_subscriber_path(actor.path());
        }
        Ok(())
    }
}

impl<M> LocalPubSubActor<M>
where
    M: Clone + Send + 'static,
{
    fn unwatch_if_unsubscribed(
        &self,
        ctx: &mut Context<LocalPubSubMsg<M>>,
        subscriber: &ActorRef<M>,
    ) {
        if !self.state.contains_subscriber_path(subscriber.path()) {
            ctx.unwatch(subscriber);
        }
    }
}

fn send_subscribe_ack(
    reply_to: Option<ActorRef<PubSubSubscribeAck>>,
    topic: TopicName,
    group: Option<String>,
    changed: bool,
) {
    if let Some(reply_to) = reply_to {
        let _ = reply_to.tell(PubSubSubscribeAck {
            topic,
            group,
            changed,
        });
    }
}

fn send_path_registration(
    reply_to: Option<ActorRef<PubSubPathRegistration>>,
    registration: PubSubPathRegistration,
) {
    if let Some(reply_to) = reply_to {
        let _ = reply_to.tell(registration);
    }
}
