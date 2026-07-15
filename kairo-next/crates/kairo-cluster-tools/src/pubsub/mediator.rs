use std::collections::{BTreeMap, BTreeSet};

use kairo_actor::{Actor, ActorRef, ActorResult, Context, Signal};
use kairo_cluster::{ClusterEvent, MemberEvent, UniqueAddress};

mod protocol;
mod recipient;

pub use protocol::{
    DistributedPubSubMediatorMsg, DistributedPubSubPublishReport, DistributedPubSubSendReport,
    DistributedPubSubSnapshot,
};

use crate::{
    CurrentTopics, LocalPubSub, LocalPubSubMsg, PubSubDeliveryPlan, PubSubDeliveryTransport,
    PubSubPathDeliveryMode, PubSubPathDeliveryPlan, PubSubPathRegistration, PubSubRegistryState,
    PubSubRemoteTarget, PubSubSubscribeAck, TopicName, TopicPublishMode,
};
use recipient::MediatorLocalRecipient;

pub struct DistributedPubSubMediatorActor<M>
where
    M: Send + 'static,
{
    local: LocalPubSub<M>,
    registry: PubSubRegistryState,
    delivery: PubSubDeliveryTransport<M>,
    gossip: Option<ActorRef<crate::PubSubGossipMsg>>,
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
            gossip: None,
        }
    }

    pub fn with_gossip(mut self, gossip: ActorRef<crate::PubSubGossipMsg>) -> Self {
        self.gossip = Some(gossip);
        self
    }

    pub fn registry(&self) -> &PubSubRegistryState {
        &self.registry
    }

    pub fn local(&self) -> &LocalPubSub<M> {
        &self.local
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
            DistributedPubSubMediatorMsg::Put { actor, reply_to } => {
                self.put(ctx, actor, reply_to)?
            }
            DistributedPubSubMediatorMsg::RemovePath { path, reply_to } => {
                self.remove_path(ctx, path, reply_to)
            }
            DistributedPubSubMediatorMsg::Publish {
                topic,
                message,
                mode,
                reply_to,
            } => self.publish(topic, message, mode, reply_to),
            DistributedPubSubMediatorMsg::Send {
                path,
                message,
                local_affinity,
                reply_to,
            } => self.send_path(path, message, local_affinity, reply_to),
            DistributedPubSubMediatorMsg::SendToAll {
                path,
                message,
                all_but_self,
                reply_to,
            } => self.send_path_to_all(path, message, all_but_self, reply_to),
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
            let before_topics = self.local.topic_groups();
            let before_paths = self.local.current_paths();
            self.local.remove_subscriber_path(actor.path());
            self.sync_registry(before_topics, before_paths);
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
        let before_topics = self.local.topic_groups();
        let before_paths = self.local.current_paths();
        ctx.watch(&subscriber)?;
        let change = self.local.subscribe(topic.clone(), subscriber);
        self.sync_registry(before_topics, before_paths);
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
        let before_topics = self.local.topic_groups();
        let before_paths = self.local.current_paths();
        ctx.watch(&subscriber)?;
        let change = self
            .local
            .subscribe_group(topic.clone(), group.clone(), subscriber);
        self.sync_registry(before_topics, before_paths);
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
        let before_topics = self.local.topic_groups();
        let before_paths = self.local.current_paths();
        let removed = self.local.unsubscribe(&topic, &subscriber);
        self.unwatch_if_unsubscribed(ctx, &subscriber);
        self.sync_registry(before_topics, before_paths);
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
        let before_topics = self.local.topic_groups();
        let before_paths = self.local.current_paths();
        let removed = self.local.unsubscribe_group(&topic, &group, &subscriber);
        self.unwatch_if_unsubscribed(ctx, &subscriber);
        self.sync_registry(before_topics, before_paths);
        send_subscribe_ack(reply_to, topic, Some(group), removed);
    }

    fn put(
        &mut self,
        ctx: &mut Context<DistributedPubSubMediatorMsg<M>>,
        actor: ActorRef<M>,
        reply_to: Option<ActorRef<PubSubPathRegistration>>,
    ) -> ActorResult {
        let before_topics = self.local.topic_groups();
        let before_paths = self.local.current_paths();
        ctx.watch(&actor)?;
        let registration = self.local.register_path(actor);
        self.sync_registry(before_topics, before_paths);
        send_path_registration(reply_to, registration);
        Ok(())
    }

    fn remove_path(
        &mut self,
        ctx: &mut Context<DistributedPubSubMediatorMsg<M>>,
        path: String,
        reply_to: Option<ActorRef<PubSubPathRegistration>>,
    ) {
        let before_topics = self.local.topic_groups();
        let before_paths = self.local.current_paths();
        let actor = self.local.path_actor(&path).cloned();
        let registration = self.local.remove_path(&path);
        if let Some(actor) = actor {
            self.unwatch_if_unsubscribed(ctx, &actor);
        }
        self.sync_registry(before_topics, before_paths);
        send_path_registration(reply_to, registration);
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

    fn send_path(
        &mut self,
        path: String,
        message: M,
        local_affinity: bool,
        reply_to: Option<ActorRef<DistributedPubSubSendReport>>,
    ) {
        let plan = PubSubPathDeliveryPlan::send(&self.registry, path.clone(), local_affinity);
        let delivery = self.delivery.send_path(&plan, message);
        if let Some(reply_to) = reply_to {
            let _ = reply_to.tell(DistributedPubSubSendReport {
                path,
                mode: PubSubPathDeliveryMode::One { local_affinity },
                plan,
                delivery,
            });
        }
    }

    fn send_path_to_all(
        &mut self,
        path: String,
        message: M,
        all_but_self: bool,
        reply_to: Option<ActorRef<DistributedPubSubSendReport>>,
    ) {
        let plan = PubSubPathDeliveryPlan::send_to_all(&self.registry, path.clone(), all_but_self);
        let delivery = self.delivery.send_path(&plan, message);
        if let Some(reply_to) = reply_to {
            let _ = reply_to.tell(DistributedPubSubSendReport {
                path,
                mode: PubSubPathDeliveryMode::All { all_but_self },
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
            LocalPubSubMsg::Put { actor, reply_to } => self.put(ctx, actor, reply_to)?,
            LocalPubSubMsg::RemovePath { path, reply_to } => self.remove_path(ctx, path, reply_to),
            LocalPubSubMsg::GetTopics { reply_to } => {
                let _ = reply_to.tell(CurrentTopics {
                    topics: self.local.current_topics(),
                });
            }
            LocalPubSubMsg::Send {
                path,
                message,
                reply_to,
            } => {
                let report = self.local.send_path(&path, message);
                if let Some(reply_to) = reply_to {
                    let _ = reply_to.tell(report);
                }
            }
            LocalPubSubMsg::SendToAll {
                path,
                message,
                reply_to,
            } => {
                let report = self.local.send_path_to_all(&path, message);
                if let Some(reply_to) = reply_to {
                    let _ = reply_to.tell(report);
                }
            }
            LocalPubSubMsg::RemoveSubscriber {
                subscriber,
                reply_to,
            } => {
                let before_topics = self.local.topic_groups();
                let before_paths = self.local.current_paths();
                let changed = self.local.remove_subscriber(&subscriber);
                self.unwatch_if_unsubscribed(ctx, &subscriber);
                self.sync_registry(before_topics, before_paths);
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
                if member.unique_address.address == self.registry.self_node().address {
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

    fn sync_registry(
        &mut self,
        before: BTreeMap<TopicName, BTreeSet<String>>,
        before_paths: BTreeSet<String>,
    ) {
        let after = self.local.topic_groups();
        let after_paths = self.local.current_paths();

        for (topic, groups) in &before {
            for group in groups {
                if !after
                    .get(topic)
                    .is_some_and(|after_groups| after_groups.contains(group))
                {
                    self.registry
                        .unregister_local_group(topic.clone(), group.clone());
                    self.tell_gossip(crate::PubSubGossipMsg::UnregisterGroup {
                        topic: topic.clone(),
                        group: group.clone(),
                    });
                }
            }
        }

        for topic in before.keys() {
            if !after.contains_key(topic) {
                self.registry.unregister_local_topic(topic.clone());
                self.tell_gossip(crate::PubSubGossipMsg::UnregisterTopic {
                    topic: topic.clone(),
                });
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
                    self.tell_gossip(crate::PubSubGossipMsg::RegisterGroup {
                        topic: topic.clone(),
                        group: group.clone(),
                    });
                }
            }
        }

        for (topic, groups) in &after {
            if groups.is_empty() && !before.contains_key(topic) {
                self.registry.register_local_topic(topic.clone());
                self.tell_gossip(crate::PubSubGossipMsg::RegisterTopic {
                    topic: topic.clone(),
                });
            }
        }

        for path in before_paths.difference(&after_paths) {
            self.registry.unregister_local_path(path.clone());
            self.tell_gossip(crate::PubSubGossipMsg::UnregisterPath { path: path.clone() });
        }
        for path in after_paths.difference(&before_paths) {
            self.registry.register_local_path(path.clone());
            self.tell_gossip(crate::PubSubGossipMsg::RegisterPath { path: path.clone() });
        }
    }

    fn tell_gossip(&self, message: crate::PubSubGossipMsg) {
        if let Some(gossip) = &self.gossip {
            let _ = gossip.tell(message);
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
