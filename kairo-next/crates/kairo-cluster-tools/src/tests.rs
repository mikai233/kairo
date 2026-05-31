use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use kairo_actor::{Actor, ActorRef, ActorResult, Address, Context, Props};
use kairo_cluster::{ClusterEvent, Member, MemberEvent, MemberStatus, UniqueAddress};
use kairo_remote::{RemoteActorRef, RemoteOutbound};
use kairo_serialization::{
    ActorRefWireData, MessageCodec, Registry, RemoteEnvelope, RemoteMessage, SerializationRegistry,
};
use kairo_testkit::ActorSystemTestKit;

use crate::{
    CurrentTopics, DistributedPubSubMediatorActor, DistributedPubSubMediatorMsg,
    DistributedPubSubPublishReport, DistributedPubSubSnapshot, LocalPubSub, LocalPubSubActor,
    LocalPubSubMsg, LocalSingletonManagerActor, LocalSingletonManagerMsg,
    LocalSingletonManagerSnapshot, LocalTopic, PubSubDeliveryFailure, PubSubDeliveryPlan,
    PubSubDeliveryTarget, PubSubDeliveryTransport, PubSubGossipActor, PubSubGossipMsg,
    PubSubGossipPeer, PubSubRegistryKey, PubSubRegistryState, PubSubRemoteTarget,
    PubSubSubscribeAck, PubSubTopicReport, SingletonManagerActor, SingletonManagerEffect,
    SingletonManagerMsg, SingletonManagerRuntime, SingletonManagerSnapshot, SingletonManagerState,
    SingletonOldestChange, SingletonOldestTracker, SingletonProxyActor, SingletonProxyMsg,
    SingletonProxySettings, SingletonProxySnapshot,
    SingletonProxyTarget as RemoteSingletonProxyTarget, SingletonScope, TopicName,
    TopicPublishMode,
};

mod local_pubsub;
mod local_singleton_manager;
mod local_topic;
mod pubsub_delivery;
mod pubsub_gossip;
mod pubsub_registry;
mod singleton_manager;
mod singleton_oldest;

#[test]
fn singleton_proxy_buffers_and_flushes_to_identified_singleton() {
    let kit = ActorSystemTestKit::new("singleton-proxy-flush").unwrap();
    let singleton = kit.create_probe::<String>("singleton").unwrap();
    let state = kit
        .create_probe::<SingletonProxySnapshot>("proxy-state")
        .unwrap();
    let proxy = kit
        .system()
        .spawn(
            "singleton-proxy",
            SingletonProxyActor::<String>::props(SingletonProxySettings::new(4).unwrap()),
        )
        .unwrap();

    proxy
        .tell(SingletonProxyMsg::Route("one".to_string()))
        .unwrap();
    proxy
        .tell(SingletonProxyMsg::Route("two".to_string()))
        .unwrap();
    singleton.expect_no_msg(Duration::from_millis(100)).unwrap();

    proxy
        .tell(SingletonProxyMsg::IdentifySingleton {
            singleton: singleton.actor_ref(),
        })
        .unwrap();
    singleton
        .expect_msg_eq("one".to_string(), Duration::from_millis(500))
        .unwrap();
    singleton
        .expect_msg_eq("two".to_string(), Duration::from_millis(500))
        .unwrap();

    proxy
        .tell(SingletonProxyMsg::Route("three".to_string()))
        .unwrap();
    singleton
        .expect_msg_eq("three".to_string(), Duration::from_millis(500))
        .unwrap();

    proxy
        .tell(SingletonProxyMsg::GetState {
            reply_to: state.actor_ref(),
        })
        .unwrap();
    let snapshot = state.expect_msg(Duration::from_millis(500)).unwrap();
    assert_eq!(snapshot.buffered_messages, 0);
    assert_eq!(snapshot.dropped_messages, 0);
    assert_eq!(
        snapshot.singleton_path.as_ref(),
        Some(singleton.actor_ref().path())
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn singleton_proxy_drops_oldest_message_when_buffer_is_full() {
    let kit = ActorSystemTestKit::new("singleton-proxy-overflow").unwrap();
    let singleton = kit.create_probe::<String>("singleton").unwrap();
    let state = kit
        .create_probe::<SingletonProxySnapshot>("proxy-state")
        .unwrap();
    let proxy = kit
        .system()
        .spawn(
            "singleton-proxy",
            SingletonProxyActor::<String>::props(SingletonProxySettings::new(2).unwrap()),
        )
        .unwrap();

    proxy
        .tell(SingletonProxyMsg::Route("one".to_string()))
        .unwrap();
    proxy
        .tell(SingletonProxyMsg::Route("two".to_string()))
        .unwrap();
    proxy
        .tell(SingletonProxyMsg::Route("three".to_string()))
        .unwrap();
    proxy
        .tell(SingletonProxyMsg::GetState {
            reply_to: state.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        state.expect_msg(Duration::from_millis(500)).unwrap(),
        SingletonProxySnapshot {
            current_oldest: None,
            registered_routes: 0,
            singleton_path: None,
            buffered_messages: 2,
            dropped_messages: 1,
        }
    );

    proxy
        .tell(SingletonProxyMsg::IdentifySingleton {
            singleton: singleton.actor_ref(),
        })
        .unwrap();
    singleton
        .expect_msg_eq("two".to_string(), Duration::from_millis(500))
        .unwrap();
    singleton
        .expect_msg_eq("three".to_string(), Duration::from_millis(500))
        .unwrap();
    singleton.expect_no_msg(Duration::from_millis(100)).unwrap();
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn singleton_proxy_identifies_registered_route_from_initial_observation() {
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let (_tracker, observation) = SingletonOldestTracker::from_members(
        node_b,
        SingletonScope::all(),
        [member(node_a.clone(), MemberStatus::Up, 1)],
    );
    let kit = ActorSystemTestKit::new("singleton-proxy-initial-oldest").unwrap();
    let singleton = kit.create_probe::<String>("singleton").unwrap();
    let state = kit
        .create_probe::<SingletonProxySnapshot>("proxy-state")
        .unwrap();
    let proxy = kit
        .system()
        .spawn(
            "singleton-proxy",
            SingletonProxyActor::<String>::props(SingletonProxySettings::new(4).unwrap()),
        )
        .unwrap();

    proxy
        .tell(SingletonProxyMsg::Route("before".to_string()))
        .unwrap();
    proxy
        .tell(SingletonProxyMsg::RegisterRoute {
            node: node_a.clone(),
            singleton: singleton.actor_ref(),
        })
        .unwrap();
    proxy
        .tell(SingletonProxyMsg::ApplyInitialObservation { observation })
        .unwrap();

    singleton
        .expect_msg_eq("before".to_string(), Duration::from_millis(500))
        .unwrap();
    proxy
        .tell(SingletonProxyMsg::Route("after".to_string()))
        .unwrap();
    singleton
        .expect_msg_eq("after".to_string(), Duration::from_millis(500))
        .unwrap();

    proxy
        .tell(SingletonProxyMsg::GetState {
            reply_to: state.actor_ref(),
        })
        .unwrap();
    let snapshot = state.expect_msg(Duration::from_millis(500)).unwrap();
    assert_eq!(snapshot.current_oldest, Some(node_a));
    assert_eq!(snapshot.registered_routes, 1);
    assert_eq!(
        snapshot.singleton_path.as_ref(),
        Some(singleton.actor_ref().path())
    );
    assert_eq!(snapshot.buffered_messages, 0);
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RemoteSingletonMsg {
    value: u8,
}

impl RemoteMessage for RemoteSingletonMsg {
    const MANIFEST: &'static str = "kairo.cluster-tools.test.RemoteSingletonMsg";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, Copy)]
struct RemoteSingletonMsgCodec;

impl MessageCodec<RemoteSingletonMsg> for RemoteSingletonMsgCodec {
    fn serializer_id(&self) -> u32 {
        73_001
    }

    fn encode(&self, message: &RemoteSingletonMsg) -> kairo_serialization::Result<Bytes> {
        Ok(Bytes::from(vec![message.value]))
    }

    fn decode(
        &self,
        payload: Bytes,
        _version: u16,
    ) -> kairo_serialization::Result<RemoteSingletonMsg> {
        Ok(RemoteSingletonMsg { value: payload[0] })
    }
}

#[derive(Default)]
struct CollectingRemoteOutbound {
    sent: Mutex<Vec<RemoteEnvelope>>,
    changed: Condvar,
}

impl CollectingRemoteOutbound {
    fn wait_for_len(&self, len: usize, timeout: Duration) -> Vec<RemoteEnvelope> {
        let deadline = Instant::now() + timeout;
        let mut sent = self.sent.lock().expect("remote outbound poisoned");
        while sent.len() < len {
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                break;
            };
            let (next_sent, wait) = self
                .changed
                .wait_timeout(sent, remaining)
                .expect("remote outbound poisoned");
            sent = next_sent;
            if wait.timed_out() {
                break;
            }
        }
        sent.clone()
    }
}

impl RemoteOutbound for CollectingRemoteOutbound {
    fn send(&self, envelope: RemoteEnvelope) -> kairo_remote::Result<()> {
        self.sent
            .lock()
            .expect("remote outbound poisoned")
            .push(envelope);
        self.changed.notify_all();
        Ok(())
    }
}

fn remote_singleton_registry() -> Arc<Registry> {
    let mut registry = Registry::new();
    registry
        .register::<RemoteSingletonMsg, _>(RemoteSingletonMsgCodec)
        .unwrap();
    Arc::new(registry)
}

#[test]
fn singleton_proxy_flushes_buffered_messages_to_remote_target() {
    let self_node = node("singleton-proxy-remote-local", 1);
    let remote_node = remote_node("singleton-proxy-remote", 2);
    let (_tracker, observation) = SingletonOldestTracker::from_members(
        self_node,
        SingletonScope::all(),
        [member(remote_node.clone(), MemberStatus::Up, 1)],
    );
    let kit = ActorSystemTestKit::new("singleton-proxy-remote").unwrap();
    let proxy = kit
        .system()
        .spawn(
            "singleton-proxy",
            SingletonProxyActor::<RemoteSingletonMsg>::props(
                SingletonProxySettings::new(4).unwrap(),
            ),
        )
        .unwrap();
    let outbound = Arc::new(CollectingRemoteOutbound::default());
    let remote_ref = RemoteActorRef::new(
        ActorRefWireData::new(format!("{}/user/singleton", remote_node.address)).unwrap(),
        remote_singleton_registry(),
        outbound.clone() as Arc<dyn RemoteOutbound>,
    );

    proxy
        .tell(SingletonProxyMsg::Route(RemoteSingletonMsg { value: 1 }))
        .unwrap();
    proxy
        .tell(SingletonProxyMsg::RegisterTarget {
            node: remote_node.clone(),
            singleton: RemoteSingletonProxyTarget::remote(remote_ref),
        })
        .unwrap();
    proxy
        .tell(SingletonProxyMsg::ApplyInitialObservation { observation })
        .unwrap();
    proxy
        .tell(SingletonProxyMsg::Route(RemoteSingletonMsg { value: 2 }))
        .unwrap();

    let sent = outbound.wait_for_len(2, Duration::from_secs(1));
    assert_eq!(sent.len(), 2);
    assert_eq!(
        sent[0].recipient.path(),
        "kairo://singleton-proxy-remote@singleton-proxy-remote.example.test:2552/user/singleton"
    );
    assert_eq!(
        sent[0].message.manifest.as_str(),
        RemoteSingletonMsg::MANIFEST
    );
    assert_eq!(sent[0].message.payload, Bytes::from_static(&[1]));
    assert_eq!(sent[1].message.payload, Bytes::from_static(&[2]));
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn singleton_proxy_reidentifies_when_oldest_route_changes() {
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let (_tracker, observation) = SingletonOldestTracker::from_members(
        node_a.clone(),
        SingletonScope::all(),
        [member(node_a.clone(), MemberStatus::Up, 1)],
    );
    let kit = ActorSystemTestKit::new("singleton-proxy-oldest-change").unwrap();
    let singleton_a = kit.create_probe::<String>("singleton-a").unwrap();
    let singleton_b = kit.create_probe::<String>("singleton-b").unwrap();
    let proxy = kit
        .system()
        .spawn(
            "singleton-proxy",
            SingletonProxyActor::<String>::props(SingletonProxySettings::new(4).unwrap()),
        )
        .unwrap();

    proxy
        .tell(SingletonProxyMsg::RegisterRoute {
            node: node_a.clone(),
            singleton: singleton_a.actor_ref(),
        })
        .unwrap();
    proxy
        .tell(SingletonProxyMsg::ApplyInitialObservation { observation })
        .unwrap();
    proxy
        .tell(SingletonProxyMsg::Route("one".to_string()))
        .unwrap();
    singleton_a
        .expect_msg_eq("one".to_string(), Duration::from_millis(500))
        .unwrap();

    proxy
        .tell(SingletonProxyMsg::ApplyOldestChange {
            change: SingletonOldestChange::OldestChanged(Some(node_b.clone())),
        })
        .unwrap();
    proxy
        .tell(SingletonProxyMsg::Route("two".to_string()))
        .unwrap();
    singleton_a
        .expect_no_msg(Duration::from_millis(100))
        .unwrap();
    singleton_b
        .expect_no_msg(Duration::from_millis(100))
        .unwrap();

    proxy
        .tell(SingletonProxyMsg::RegisterRoute {
            node: node_b,
            singleton: singleton_b.actor_ref(),
        })
        .unwrap();
    singleton_b
        .expect_msg_eq("two".to_string(), Duration::from_millis(500))
        .unwrap();
    proxy
        .tell(SingletonProxyMsg::Route("three".to_string()))
        .unwrap();
    singleton_b
        .expect_msg_eq("three".to_string(), Duration::from_millis(500))
        .unwrap();
    singleton_a
        .expect_no_msg(Duration::from_millis(100))
        .unwrap();
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[derive(Clone)]
enum SingletonProxyTargetMsg {
    Payload(&'static str),
    Stop,
}

struct SingletonProxyTarget {
    observed: ActorRef<&'static str>,
}

impl Actor for SingletonProxyTarget {
    type Msg = SingletonProxyTargetMsg;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            SingletonProxyTargetMsg::Payload(value) => {
                let _ = self.observed.tell(value);
            }
            SingletonProxyTargetMsg::Stop => ctx.stop(ctx.myself())?,
        }
        Ok(())
    }
}

#[test]
fn singleton_proxy_clears_current_singleton_on_termination_and_buffers_again() {
    let kit = ActorSystemTestKit::new("singleton-proxy-termination").unwrap();
    let observed = kit.create_probe::<&'static str>("observed").unwrap();
    let state = kit
        .create_probe::<SingletonProxySnapshot>("proxy-state")
        .unwrap();
    let proxy = kit
        .system()
        .spawn(
            "singleton-proxy",
            SingletonProxyActor::<SingletonProxyTargetMsg>::props(
                SingletonProxySettings::new(4).unwrap(),
            ),
        )
        .unwrap();
    let target_1 = kit
        .system()
        .spawn(
            "target-1",
            Props::new({
                let observed = observed.actor_ref();
                move || SingletonProxyTarget { observed }
            }),
        )
        .unwrap();

    proxy
        .tell(SingletonProxyMsg::IdentifySingleton {
            singleton: target_1.clone(),
        })
        .unwrap();
    proxy
        .tell(SingletonProxyMsg::Route(SingletonProxyTargetMsg::Payload(
            "first",
        )))
        .unwrap();
    observed
        .expect_msg_eq("first", Duration::from_millis(500))
        .unwrap();

    target_1.tell(SingletonProxyTargetMsg::Stop).unwrap();
    assert!(target_1.wait_for_stop(Duration::from_secs(1)));
    let mut cleared = None;
    for _ in 0..100 {
        proxy
            .tell(SingletonProxyMsg::GetState {
                reply_to: state.actor_ref(),
            })
            .unwrap();
        let snapshot = state.expect_msg(Duration::from_millis(500)).unwrap();
        if snapshot.singleton_path.is_none() {
            cleared = Some(snapshot);
            break;
        }
    }
    assert_eq!(
        cleared.expect("proxy should observe singleton termination"),
        SingletonProxySnapshot {
            current_oldest: None,
            registered_routes: 0,
            singleton_path: None,
            buffered_messages: 0,
            dropped_messages: 0,
        }
    );

    proxy
        .tell(SingletonProxyMsg::Route(SingletonProxyTargetMsg::Payload(
            "buffered",
        )))
        .unwrap();
    observed.expect_no_msg(Duration::from_millis(100)).unwrap();

    let target_2 = kit
        .system()
        .spawn(
            "target-2",
            Props::new({
                let observed = observed.actor_ref();
                move || SingletonProxyTarget { observed }
            }),
        )
        .unwrap();
    proxy
        .tell(SingletonProxyMsg::IdentifySingleton {
            singleton: target_2,
        })
        .unwrap();
    observed
        .expect_msg_eq("buffered", Duration::from_millis(500))
        .unwrap();
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn distributed_pubsub_mediator_registers_local_subscription_and_publishes() {
    let node_a = node("a", 1);
    let topic = TopicName::new("orders");
    let kit = ActorSystemTestKit::new("distributed-pubsub-local").unwrap();
    let subscriber = kit.create_probe::<String>("subscriber").unwrap();
    let ack_probe = kit.create_probe::<PubSubSubscribeAck>("acks").unwrap();
    let report_probe = kit
        .create_probe::<DistributedPubSubPublishReport>("reports")
        .unwrap();
    let state_probe = kit
        .create_probe::<DistributedPubSubSnapshot>("state")
        .unwrap();
    let mediator = kit
        .system()
        .spawn(
            "mediator",
            Props::new({
                let node_a = node_a.clone();
                move || DistributedPubSubMediatorActor::<String>::new(node_a)
            }),
        )
        .unwrap();

    mediator
        .tell(DistributedPubSubMediatorMsg::Subscribe {
            topic: topic.clone(),
            subscriber: subscriber.actor_ref(),
            reply_to: Some(ack_probe.actor_ref()),
        })
        .unwrap();
    assert_eq!(
        ack_probe.expect_msg(Duration::from_millis(500)).unwrap(),
        PubSubSubscribeAck {
            topic: topic.clone(),
            group: None,
            changed: true,
        }
    );

    mediator
        .tell(DistributedPubSubMediatorMsg::GetState {
            reply_to: state_probe.actor_ref(),
        })
        .unwrap();
    let snapshot = state_probe.expect_msg(Duration::from_millis(500)).unwrap();
    assert_eq!(snapshot.current_topics, BTreeSet::from([topic.clone()]));
    assert_eq!(
        snapshot.registry.broadcast_targets(&topic, true),
        vec![node_a]
    );

    mediator
        .tell(DistributedPubSubMediatorMsg::Publish {
            topic: topic.clone(),
            message: "created".to_string(),
            mode: TopicPublishMode::Broadcast,
            reply_to: Some(report_probe.actor_ref()),
        })
        .unwrap();
    let report = report_probe.expect_msg(Duration::from_millis(500)).unwrap();
    assert_eq!(report.plan.targets, vec![PubSubDeliveryTarget::LocalTopic]);
    assert!(report.delivery.is_success());
    subscriber
        .expect_msg_eq("created".to_string(), Duration::from_millis(500))
        .unwrap();
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn distributed_pubsub_mediator_routes_to_remote_mediator_from_merged_registry() {
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let topic = TopicName::new("orders");
    let kit = ActorSystemTestKit::new("distributed-pubsub-remote").unwrap();
    let subscriber_b = kit.create_probe::<String>("subscriber-b").unwrap();
    let registry_probe = kit.create_probe::<PubSubRegistryState>("registry").unwrap();
    let report_probe = kit
        .create_probe::<DistributedPubSubPublishReport>("reports")
        .unwrap();
    let mediator_a = kit
        .system()
        .spawn(
            "mediator-a",
            Props::new({
                let node_a = node_a.clone();
                move || DistributedPubSubMediatorActor::<String>::new(node_a)
            }),
        )
        .unwrap();
    let mediator_b = kit
        .system()
        .spawn(
            "mediator-b",
            Props::new({
                let node_b = node_b.clone();
                move || DistributedPubSubMediatorActor::<String>::new(node_b)
            }),
        )
        .unwrap();

    mediator_b
        .tell(DistributedPubSubMediatorMsg::Subscribe {
            topic: topic.clone(),
            subscriber: subscriber_b.actor_ref(),
            reply_to: None,
        })
        .unwrap();
    mediator_b
        .tell(DistributedPubSubMediatorMsg::GetRegistry {
            reply_to: registry_probe.actor_ref(),
        })
        .unwrap();
    let registry_b = registry_probe
        .expect_msg(Duration::from_millis(500))
        .unwrap();

    mediator_a
        .tell(DistributedPubSubMediatorMsg::AddRemoteMediator {
            node: node_b.clone(),
            mediator: mediator_b,
        })
        .unwrap();
    mediator_a
        .tell(DistributedPubSubMediatorMsg::MergeDelta {
            delta: registry_b.collect_delta(&BTreeMap::new(), 10),
        })
        .unwrap();
    mediator_a
        .tell(DistributedPubSubMediatorMsg::Publish {
            topic: topic.clone(),
            message: "created".to_string(),
            mode: TopicPublishMode::Broadcast,
            reply_to: Some(report_probe.actor_ref()),
        })
        .unwrap();

    let report = report_probe.expect_msg(Duration::from_millis(500)).unwrap();
    assert_eq!(
        report.plan.targets,
        vec![PubSubDeliveryTarget::RemoteTopic { node: node_b }]
    );
    assert!(report.delivery.is_success());
    subscriber_b
        .expect_msg_eq("created".to_string(), Duration::from_millis(500))
        .unwrap();
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn distributed_pubsub_mediator_removes_remote_route_on_cluster_member_left() {
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let topic = TopicName::new("orders");
    let kit = ActorSystemTestKit::new("distributed-pubsub-member-left").unwrap();
    let subscriber_b = kit.create_probe::<String>("subscriber-b").unwrap();
    let registry_probe = kit.create_probe::<PubSubRegistryState>("registry").unwrap();
    let report_probe = kit
        .create_probe::<DistributedPubSubPublishReport>("reports")
        .unwrap();
    let state_probe = kit
        .create_probe::<DistributedPubSubSnapshot>("state")
        .unwrap();
    let mediator_a = kit
        .system()
        .spawn(
            "mediator-a",
            Props::new({
                let node_a = node_a.clone();
                move || DistributedPubSubMediatorActor::<String>::new(node_a)
            }),
        )
        .unwrap();
    let mediator_b = kit
        .system()
        .spawn(
            "mediator-b",
            Props::new({
                let node_b = node_b.clone();
                move || DistributedPubSubMediatorActor::<String>::new(node_b)
            }),
        )
        .unwrap();

    mediator_b
        .tell(DistributedPubSubMediatorMsg::Subscribe {
            topic: topic.clone(),
            subscriber: subscriber_b.actor_ref(),
            reply_to: None,
        })
        .unwrap();
    mediator_b
        .tell(DistributedPubSubMediatorMsg::GetRegistry {
            reply_to: registry_probe.actor_ref(),
        })
        .unwrap();
    let registry_b = registry_probe
        .expect_msg(Duration::from_millis(500))
        .unwrap();
    mediator_a
        .tell(DistributedPubSubMediatorMsg::AddRemoteMediator {
            node: node_b.clone(),
            mediator: mediator_b,
        })
        .unwrap();
    mediator_a
        .tell(DistributedPubSubMediatorMsg::MergeDelta {
            delta: registry_b.collect_delta(&BTreeMap::new(), 10),
        })
        .unwrap();

    mediator_a
        .tell(DistributedPubSubMediatorMsg::ApplyClusterEvent {
            event: ClusterEvent::Member(MemberEvent::Left(member(
                node_b.clone(),
                MemberStatus::Leaving,
                2,
            ))),
        })
        .unwrap();
    mediator_a
        .tell(DistributedPubSubMediatorMsg::GetState {
            reply_to: state_probe.actor_ref(),
        })
        .unwrap();
    let snapshot = state_probe.expect_msg(Duration::from_millis(500)).unwrap();
    assert_eq!(snapshot.remote_target_count, 0);
    assert!(snapshot.registry.broadcast_targets(&topic, true).is_empty());

    mediator_a
        .tell(DistributedPubSubMediatorMsg::Publish {
            topic: topic.clone(),
            message: "after-left".to_string(),
            mode: TopicPublishMode::Broadcast,
            reply_to: Some(report_probe.actor_ref()),
        })
        .unwrap();
    let report = report_probe.expect_msg(Duration::from_millis(500)).unwrap();
    assert!(report.plan.is_empty());
    assert!(report.delivery.sent_to().is_empty());
    subscriber_b
        .expect_no_msg(Duration::from_millis(100))
        .unwrap();
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn distributed_pubsub_mediator_routes_one_message_per_group_across_nodes() {
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let topic = TopicName::new("jobs");
    let kit = ActorSystemTestKit::new("distributed-pubsub-one-per-group").unwrap();
    let local_blue = kit.create_probe::<String>("local-blue").unwrap();
    let remote_red = kit.create_probe::<String>("remote-red").unwrap();
    let registry_probe = kit.create_probe::<PubSubRegistryState>("registry").unwrap();
    let report_probe = kit
        .create_probe::<DistributedPubSubPublishReport>("reports")
        .unwrap();
    let mediator_a = kit
        .system()
        .spawn(
            "mediator-a",
            Props::new({
                let node_a = node_a.clone();
                move || DistributedPubSubMediatorActor::<String>::new(node_a)
            }),
        )
        .unwrap();
    let mediator_b = kit
        .system()
        .spawn(
            "mediator-b",
            Props::new({
                let node_b = node_b.clone();
                move || DistributedPubSubMediatorActor::<String>::new(node_b)
            }),
        )
        .unwrap();

    mediator_a
        .tell(DistributedPubSubMediatorMsg::SubscribeGroup {
            topic: topic.clone(),
            group: "blue".to_string(),
            subscriber: local_blue.actor_ref(),
            reply_to: None,
        })
        .unwrap();
    mediator_b
        .tell(DistributedPubSubMediatorMsg::SubscribeGroup {
            topic: topic.clone(),
            group: "red".to_string(),
            subscriber: remote_red.actor_ref(),
            reply_to: None,
        })
        .unwrap();
    mediator_b
        .tell(DistributedPubSubMediatorMsg::GetRegistry {
            reply_to: registry_probe.actor_ref(),
        })
        .unwrap();
    let registry_b = registry_probe
        .expect_msg(Duration::from_millis(500))
        .unwrap();
    mediator_a
        .tell(DistributedPubSubMediatorMsg::AddRemoteMediator {
            node: node_b.clone(),
            mediator: mediator_b,
        })
        .unwrap();
    mediator_a
        .tell(DistributedPubSubMediatorMsg::MergeDelta {
            delta: registry_b.collect_delta(&BTreeMap::new(), 10),
        })
        .unwrap();
    mediator_a
        .tell(DistributedPubSubMediatorMsg::Publish {
            topic: topic.clone(),
            message: "run".to_string(),
            mode: TopicPublishMode::OnePerGroup,
            reply_to: Some(report_probe.actor_ref()),
        })
        .unwrap();

    let report = report_probe.expect_msg(Duration::from_millis(500)).unwrap();
    assert_eq!(
        report.plan.targets,
        vec![
            PubSubDeliveryTarget::LocalGroup {
                group: "blue".to_string()
            },
            PubSubDeliveryTarget::RemoteGroup {
                group: "red".to_string(),
                node: node_b,
            },
        ]
    );
    assert!(report.delivery.is_success());
    local_blue
        .expect_msg_eq("run".to_string(), Duration::from_millis(500))
        .unwrap();
    remote_red
        .expect_msg_eq("run".to_string(), Duration::from_millis(500))
        .unwrap();
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

fn member(unique_address: UniqueAddress, status: MemberStatus, up_number: u64) -> Member {
    Member::new(unique_address, Vec::new())
        .with_status(status)
        .with_up_number(up_number)
}

fn member_with_roles(
    unique_address: UniqueAddress,
    status: MemberStatus,
    up_number: u64,
    roles: impl IntoIterator<Item = &'static str>,
) -> Member {
    Member::new(
        unique_address,
        roles.into_iter().map(String::from).collect(),
    )
    .with_status(status)
    .with_up_number(up_number)
}

fn node(system: &str, uid: u64) -> UniqueAddress {
    UniqueAddress::new(Address::local(system), uid)
}

fn remote_node(system: &str, uid: u64) -> UniqueAddress {
    UniqueAddress::new(
        Address::new(
            "kairo",
            system,
            Some(format!("{system}.example.test")),
            Some(2552),
        ),
        uid,
    )
}
