use std::collections::BTreeSet;

use kairo_actor::{Actor, ActorRef, ActorResult, Context, Signal};

use crate::{LocalPubSub, PubSubTopicReport, TopicName, TopicPublishMode};

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
    pub fn new() -> Self {
        Self {
            state: LocalPubSub::new(),
        }
    }

    pub fn state(&self) -> &LocalPubSub<M> {
        &self.state
    }
}

pub enum LocalPubSubMsg<M>
where
    M: Send + 'static,
{
    Subscribe {
        topic: TopicName,
        subscriber: ActorRef<M>,
        reply_to: Option<ActorRef<PubSubSubscribeAck>>,
    },
    SubscribeGroup {
        topic: TopicName,
        group: String,
        subscriber: ActorRef<M>,
        reply_to: Option<ActorRef<PubSubSubscribeAck>>,
    },
    Unsubscribe {
        topic: TopicName,
        subscriber: ActorRef<M>,
        reply_to: Option<ActorRef<PubSubSubscribeAck>>,
    },
    UnsubscribeGroup {
        topic: TopicName,
        group: String,
        subscriber: ActorRef<M>,
        reply_to: Option<ActorRef<PubSubSubscribeAck>>,
    },
    Publish {
        topic: TopicName,
        message: M,
        mode: TopicPublishMode,
        reply_to: Option<ActorRef<PubSubTopicReport>>,
    },
    PublishGroup {
        topic: TopicName,
        group: String,
        message: M,
        reply_to: Option<ActorRef<PubSubTopicReport>>,
    },
    GetTopics {
        reply_to: ActorRef<CurrentTopics>,
    },
    RemoveSubscriber {
        subscriber: ActorRef<M>,
        reply_to: Option<ActorRef<Vec<TopicName>>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubSubSubscribeAck {
    pub topic: TopicName,
    pub group: Option<String>,
    pub changed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentTopics {
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
