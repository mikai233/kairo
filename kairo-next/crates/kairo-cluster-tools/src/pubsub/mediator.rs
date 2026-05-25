use std::collections::{BTreeMap, BTreeSet};

use kairo_actor::{Actor, ActorRef, ActorResult, Context, Recipient, SendError, Signal};
use kairo_cluster::{ClusterEvent, MemberEvent, UniqueAddress};

use crate::{
    CurrentTopics, LocalPubSub, LocalPubSubMsg, PubSubDeliveryPlan, PubSubDeliveryReport,
    PubSubDeliveryTransport, PubSubRegistryDelta, PubSubRegistryState, PubSubRemoteTarget,
    PubSubSubscribeAck, TopicName, TopicPublishMode,
};

pub struct DistributedPubSubMediatorActor<M>
where
    M: Send + 'static,
{
    local: LocalPubSub<M>,
    registry: PubSubRegistryState,
    delivery: PubSubDeliveryTransport<M>,
}

impl<M> DistributedPubSubMediatorActor<M>
where
    M: Send + 'static,
{
    pub fn new(self_node: UniqueAddress) -> Self {
        Self {
            local: LocalPubSub::new(),
            registry: PubSubRegistryState::new(self_node),
            delivery: PubSubDeliveryTransport::new(),
        }
    }

    pub fn registry(&self) -> &PubSubRegistryState {
        &self.registry
    }

    pub fn local(&self) -> &LocalPubSub<M> {
        &self.local
    }
}

pub enum DistributedPubSubMediatorMsg<M>
where
    M: Send + 'static,
{
    AddRemoteMediator {
        node: UniqueAddress,
        mediator: ActorRef<DistributedPubSubMediatorMsg<M>>,
    },
    AddRemoteTarget {
        target: PubSubRemoteTarget<M>,
    },
    RemoveRemoteMediator {
        node: UniqueAddress,
    },
    ApplyClusterEvent {
        event: ClusterEvent,
    },
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
        reply_to: Option<ActorRef<DistributedPubSubPublishReport>>,
    },
    LocalDelivery(LocalPubSubMsg<M>),
    MergeDelta {
        delta: PubSubRegistryDelta,
    },
    PruneTombstones {
        retained_version_gap: u64,
    },
    GetRegistry {
        reply_to: ActorRef<PubSubRegistryState>,
    },
    GetTopics {
        reply_to: ActorRef<CurrentTopics>,
    },
    GetState {
        reply_to: ActorRef<DistributedPubSubSnapshot>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DistributedPubSubPublishReport {
    pub topic: TopicName,
    pub mode: TopicPublishMode,
    pub plan: PubSubDeliveryPlan,
    pub delivery: PubSubDeliveryReport,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DistributedPubSubSnapshot {
    pub registry: PubSubRegistryState,
    pub current_topics: BTreeSet<TopicName>,
    pub remote_target_count: usize,
}

#[derive(Clone)]
struct MediatorLocalRecipient<M>
where
    M: Send + 'static,
{
    mediator: ActorRef<DistributedPubSubMediatorMsg<M>>,
}

impl<M> MediatorLocalRecipient<M>
where
    M: Send + 'static,
{
    fn new(mediator: ActorRef<DistributedPubSubMediatorMsg<M>>) -> Self {
        Self { mediator }
    }
}

impl<M> Recipient<LocalPubSubMsg<M>> for MediatorLocalRecipient<M>
where
    M: Send + 'static,
{
    fn tell(&self, message: LocalPubSubMsg<M>) -> Result<(), SendError<LocalPubSubMsg<M>>> {
        self.mediator
            .tell(DistributedPubSubMediatorMsg::LocalDelivery(message))
            .map_err(|error| {
                let reason = error.reason().to_string();
                match error.into_message() {
                    DistributedPubSubMediatorMsg::LocalDelivery(message) => {
                        SendError::new(message, reason)
                    }
                    _ => unreachable!("mediator local recipient only sends LocalDelivery"),
                }
            })
    }
}

impl<M> Actor for DistributedPubSubMediatorActor<M>
where
    M: Clone + Send + 'static,
{
    type Msg = DistributedPubSubMediatorMsg<M>;

    fn started(&mut self, ctx: &mut Context<Self::Msg>) -> ActorResult {
        self.delivery
            .set_local(MediatorLocalRecipient::new(ctx.myself().clone()));
        Ok(())
    }

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            DistributedPubSubMediatorMsg::AddRemoteMediator { node, mediator } => {
                self.delivery.insert_remote_target(PubSubRemoteTarget::new(
                    node,
                    MediatorLocalRecipient::new(mediator),
                ));
            }
            DistributedPubSubMediatorMsg::AddRemoteTarget { target } => {
                self.delivery.insert_remote_target(target);
            }
            DistributedPubSubMediatorMsg::RemoveRemoteMediator { node } => {
                self.remove_remote_node(&node);
            }
            DistributedPubSubMediatorMsg::ApplyClusterEvent { event } => {
                self.apply_cluster_event(ctx, event)?;
            }
            DistributedPubSubMediatorMsg::Subscribe {
                topic,
                subscriber,
                reply_to,
            } => self.subscribe(ctx, topic, subscriber, reply_to)?,
            DistributedPubSubMediatorMsg::SubscribeGroup {
                topic,
                group,
                subscriber,
                reply_to,
            } => self.subscribe_group(ctx, topic, group, subscriber, reply_to)?,
            DistributedPubSubMediatorMsg::Unsubscribe {
                topic,
                subscriber,
                reply_to,
            } => self.unsubscribe(ctx, topic, subscriber, reply_to),
            DistributedPubSubMediatorMsg::UnsubscribeGroup {
                topic,
                group,
                subscriber,
                reply_to,
            } => self.unsubscribe_group(ctx, topic, group, subscriber, reply_to),
            DistributedPubSubMediatorMsg::Publish {
                topic,
                message,
                mode,
                reply_to,
            } => self.publish(topic, message, mode, reply_to),
            DistributedPubSubMediatorMsg::LocalDelivery(delivery) => {
                self.local_delivery(ctx, delivery)?;
            }
            DistributedPubSubMediatorMsg::MergeDelta { delta } => {
                self.registry.merge_delta(delta);
            }
            DistributedPubSubMediatorMsg::PruneTombstones {
                retained_version_gap,
            } => self
                .registry
                .prune_tombstones_older_than(retained_version_gap),
            DistributedPubSubMediatorMsg::GetRegistry { reply_to } => {
                let _ = reply_to.tell(self.registry.clone());
            }
            DistributedPubSubMediatorMsg::GetTopics { reply_to } => {
                let _ = reply_to.tell(CurrentTopics {
                    topics: self.local.current_topics(),
                });
            }
            DistributedPubSubMediatorMsg::GetState { reply_to } => {
                let _ = reply_to.tell(DistributedPubSubSnapshot {
                    registry: self.registry.clone(),
                    current_topics: self.local.current_topics(),
                    remote_target_count: self.delivery.remote_target_count(),
                });
            }
        }
        Ok(())
    }

    fn signal(&mut self, _ctx: &mut Context<Self::Msg>, signal: Signal) -> ActorResult {
        if let Signal::Terminated(actor) = signal {
            let before = self.local.topic_groups();
            self.local.remove_subscriber_path(actor.path());
            self.sync_registry(before);
        }
        Ok(())
    }
}

impl<M> DistributedPubSubMediatorActor<M>
where
    M: Clone + Send + 'static,
{
    fn subscribe(
        &mut self,
        ctx: &mut Context<DistributedPubSubMediatorMsg<M>>,
        topic: TopicName,
        subscriber: ActorRef<M>,
        reply_to: Option<ActorRef<PubSubSubscribeAck>>,
    ) -> ActorResult {
        let before = self.local.topic_groups();
        ctx.watch(&subscriber)?;
        let change = self.local.subscribe(topic.clone(), subscriber);
        self.sync_registry(before);
        send_subscribe_ack(reply_to, topic, None, change.inserted);
        Ok(())
    }

    fn subscribe_group(
        &mut self,
        ctx: &mut Context<DistributedPubSubMediatorMsg<M>>,
        topic: TopicName,
        group: String,
        subscriber: ActorRef<M>,
        reply_to: Option<ActorRef<PubSubSubscribeAck>>,
    ) -> ActorResult {
        let before = self.local.topic_groups();
        ctx.watch(&subscriber)?;
        let change = self
            .local
            .subscribe_group(topic.clone(), group.clone(), subscriber);
        self.sync_registry(before);
        send_subscribe_ack(reply_to, topic, Some(group), change.inserted);
        Ok(())
    }

    fn unsubscribe(
        &mut self,
        ctx: &mut Context<DistributedPubSubMediatorMsg<M>>,
        topic: TopicName,
        subscriber: ActorRef<M>,
        reply_to: Option<ActorRef<PubSubSubscribeAck>>,
    ) {
        let before = self.local.topic_groups();
        let removed = self.local.unsubscribe(&topic, &subscriber);
        self.unwatch_if_unsubscribed(ctx, &subscriber);
        self.sync_registry(before);
        send_subscribe_ack(reply_to, topic, None, removed);
    }

    fn unsubscribe_group(
        &mut self,
        ctx: &mut Context<DistributedPubSubMediatorMsg<M>>,
        topic: TopicName,
        group: String,
        subscriber: ActorRef<M>,
        reply_to: Option<ActorRef<PubSubSubscribeAck>>,
    ) {
        let before = self.local.topic_groups();
        let removed = self.local.unsubscribe_group(&topic, &group, &subscriber);
        self.unwatch_if_unsubscribed(ctx, &subscriber);
        self.sync_registry(before);
        send_subscribe_ack(reply_to, topic, Some(group), removed);
    }

    fn publish(
        &mut self,
        topic: TopicName,
        message: M,
        mode: TopicPublishMode,
        reply_to: Option<ActorRef<DistributedPubSubPublishReport>>,
    ) {
        let plan = PubSubDeliveryPlan::for_registry(&self.registry, topic.clone(), mode);
        let delivery = self.delivery.publish(&plan, message);
        if let Some(reply_to) = reply_to {
            let _ = reply_to.tell(DistributedPubSubPublishReport {
                topic,
                mode,
                plan,
                delivery,
            });
        }
    }

    fn local_delivery(
        &mut self,
        ctx: &mut Context<DistributedPubSubMediatorMsg<M>>,
        delivery: LocalPubSubMsg<M>,
    ) -> ActorResult {
        match delivery {
            LocalPubSubMsg::Publish {
                topic,
                message,
                mode,
                reply_to,
            } => {
                let report = self.local.publish(&topic, message, mode);
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
                let report = self.local.publish_group(&topic, &group, message);
                if let Some(reply_to) = reply_to {
                    let _ = reply_to.tell(report);
                }
            }
            LocalPubSubMsg::Subscribe {
                topic,
                subscriber,
                reply_to,
            } => self.subscribe(ctx, topic, subscriber, reply_to)?,
            LocalPubSubMsg::SubscribeGroup {
                topic,
                group,
                subscriber,
                reply_to,
            } => self.subscribe_group(ctx, topic, group, subscriber, reply_to)?,
            LocalPubSubMsg::Unsubscribe {
                topic,
                subscriber,
                reply_to,
            } => self.unsubscribe(ctx, topic, subscriber, reply_to),
            LocalPubSubMsg::UnsubscribeGroup {
                topic,
                group,
                subscriber,
                reply_to,
            } => self.unsubscribe_group(ctx, topic, group, subscriber, reply_to),
            LocalPubSubMsg::GetTopics { reply_to } => {
                let _ = reply_to.tell(CurrentTopics {
                    topics: self.local.current_topics(),
                });
            }
            LocalPubSubMsg::RemoveSubscriber {
                subscriber,
                reply_to,
            } => {
                let before = self.local.topic_groups();
                let changed = self.local.remove_subscriber(&subscriber);
                self.unwatch_if_unsubscribed(ctx, &subscriber);
                self.sync_registry(before);
                if let Some(reply_to) = reply_to {
                    let _ = reply_to.tell(changed);
                }
            }
        }
        Ok(())
    }

    fn unwatch_if_unsubscribed(
        &self,
        ctx: &mut Context<DistributedPubSubMediatorMsg<M>>,
        subscriber: &ActorRef<M>,
    ) {
        if !self.local.contains_subscriber_path(subscriber.path()) {
            ctx.unwatch(subscriber);
        }
    }

    fn apply_cluster_event(
        &mut self,
        ctx: &mut Context<DistributedPubSubMediatorMsg<M>>,
        event: ClusterEvent,
    ) -> ActorResult {
        match event {
            ClusterEvent::Member(MemberEvent::Left(member))
            | ClusterEvent::Member(MemberEvent::Downed(member)) => {
                self.remove_remote_node(&member.unique_address);
            }
            ClusterEvent::Member(MemberEvent::Removed { member, .. }) => {
                if &member.unique_address == self.registry.self_node() {
                    ctx.stop(ctx.myself())?;
                } else {
                    self.remove_remote_node(&member.unique_address);
                }
            }
            ClusterEvent::Member(
                MemberEvent::Joined(_)
                | MemberEvent::WeaklyUp(_)
                | MemberEvent::Up(_)
                | MemberEvent::Exited(_),
            )
            | ClusterEvent::Reachability(_)
            | ClusterEvent::LeaderChanged { .. }
            | ClusterEvent::RoleLeaderChanged { .. }
            | ClusterEvent::SeenChanged { .. }
            | ClusterEvent::ReachabilityChanged { .. }
            | ClusterEvent::MemberTombstonesChanged { .. } => {}
        }
        Ok(())
    }

    fn remove_remote_node(&mut self, node: &UniqueAddress) {
        self.delivery.remove_remote_target(node);
        self.registry.remove_node(node);
    }

    fn sync_registry(&mut self, before: BTreeMap<TopicName, BTreeSet<String>>) {
        let after = self.local.topic_groups();

        for (topic, groups) in &before {
            for group in groups {
                if !after
                    .get(topic)
                    .is_some_and(|after_groups| after_groups.contains(group))
                {
                    self.registry
                        .unregister_local_group(topic.clone(), group.clone());
                }
            }
        }

        for topic in before.keys() {
            if !after.contains_key(topic) {
                self.registry.unregister_local_topic(topic.clone());
            }
        }

        for (topic, groups) in &after {
            for group in groups {
                if !before
                    .get(topic)
                    .is_some_and(|before_groups| before_groups.contains(group))
                {
                    self.registry
                        .register_local_group(topic.clone(), group.clone());
                }
            }
        }

        for (topic, groups) in &after {
            if groups.is_empty() && !before.contains_key(topic) {
                self.registry.register_local_topic(topic.clone());
            }
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
