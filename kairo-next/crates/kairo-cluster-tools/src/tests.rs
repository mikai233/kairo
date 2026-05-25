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

#[test]
fn singleton_oldest_tracker_filters_by_role_and_initial_age() {
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let node_c = node("c", 3);

    let (_tracker, observation) = SingletonOldestTracker::from_members(
        node_c.clone(),
        SingletonScope::for_role("backend"),
        [
            member_with_roles(node_a.clone(), MemberStatus::Up, 1, ["backend"]),
            member_with_roles(node_b, MemberStatus::Up, 2, ["frontend"]),
            member_with_roles(node_c.clone(), MemberStatus::Up, 3, ["backend"]),
            Member::new(node("joining", 4), vec!["backend".to_string()])
                .with_status(MemberStatus::Joining),
        ],
    );

    assert_eq!(observation.oldest(), Some(&node_a));
    assert_eq!(
        observation.older_or_self(),
        &[node_a.clone(), node_c.clone()]
    );
    assert!(observation.safe_to_be_oldest());
}

#[test]
fn singleton_oldest_tracker_marks_takeover_unsafe_when_older_member_is_leaving() {
    let node_a = node("a", 1);
    let node_b = node("b", 2);

    let (_tracker, observation) = SingletonOldestTracker::from_members(
        node_b,
        SingletonScope::all(),
        [
            member(node_a.clone(), MemberStatus::Leaving, 1),
            member(node("b", 2), MemberStatus::Up, 2),
        ],
    );

    assert_eq!(observation.oldest(), Some(&node_a));
    assert!(!observation.safe_to_be_oldest());
}

#[test]
fn singleton_oldest_tracker_reports_oldest_change_for_member_up_and_removed() {
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let node_c = node("c", 3);

    let (mut tracker, _observation) = SingletonOldestTracker::from_members(
        node_c.clone(),
        SingletonScope::all(),
        [
            member(node_b.clone(), MemberStatus::Up, 2),
            member(node_c, MemberStatus::Up, 3),
        ],
    );

    assert_eq!(
        tracker.apply_cluster_event(&ClusterEvent::Member(MemberEvent::Up(member(
            node_a.clone(),
            MemberStatus::Up,
            1,
        )))),
        Some(SingletonOldestChange::OldestChanged(Some(node_a.clone())))
    );
    assert_eq!(tracker.current_oldest(), Some(&node_a));

    assert_eq!(
        tracker.apply_member_event(&MemberEvent::Removed {
            member: member(node_a.clone(), MemberStatus::Removed, 1),
            previous_status: MemberStatus::Up,
        }),
        Some(SingletonOldestChange::OldestChanged(Some(node_b.clone())))
    );
    assert_eq!(tracker.current_oldest(), Some(&node_b));
}

#[test]
fn singleton_oldest_tracker_ignores_self_exited_and_non_matching_role() {
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let node_c = node("c", 3);

    let (mut tracker, _observation) = SingletonOldestTracker::from_members(
        node_b.clone(),
        SingletonScope::for_role("backend"),
        [member_with_roles(
            node_b.clone(),
            MemberStatus::Up,
            2,
            ["backend"],
        )],
    );

    assert_eq!(
        tracker.apply_member_event(&MemberEvent::Up(member_with_roles(
            node_a,
            MemberStatus::Up,
            1,
            ["frontend"],
        ))),
        None
    );
    assert_eq!(tracker.current_oldest(), Some(&node_b));

    assert_eq!(
        tracker.apply_member_event(&MemberEvent::Exited(member_with_roles(
            node_b.clone(),
            MemberStatus::Exiting,
            2,
            ["backend"],
        ))),
        None
    );
    assert_eq!(tracker.current_oldest(), Some(&node_b));

    assert_eq!(
        tracker.apply_member_event(&MemberEvent::Up(member_with_roles(
            node_c.clone(),
            MemberStatus::Up,
            3,
            ["backend"],
        ))),
        None
    );
    assert_eq!(
        tracker
            .members_by_age()
            .iter()
            .map(|member| member.unique_address.clone())
            .collect::<Vec<_>>(),
        vec![node_b, node_c]
    );
}

#[test]
fn singleton_manager_starts_immediately_when_self_is_safe_oldest() {
    let node_a = node("a", 1);
    let (_tracker, observation) = SingletonOldestTracker::from_members(
        node_a.clone(),
        SingletonScope::all(),
        [member(node_a.clone(), MemberStatus::Up, 1)],
    );
    let mut manager = SingletonManagerRuntime::new(node_a);

    assert_eq!(
        manager.apply_initial_observation(observation),
        vec![SingletonManagerEffect::StartSingleton]
    );
    assert_eq!(
        manager.state(),
        &SingletonManagerState::Oldest {
            singleton_running: true,
        }
    );
}

#[test]
fn singleton_manager_requests_handover_before_becoming_oldest() {
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let (_tracker, observation) = SingletonOldestTracker::from_members(
        node_b.clone(),
        SingletonScope::all(),
        [
            member(node_a.clone(), MemberStatus::Up, 1),
            member(node_b.clone(), MemberStatus::Up, 2),
        ],
    );
    let mut manager = SingletonManagerRuntime::new(node_b.clone());
    assert!(manager.apply_initial_observation(observation).is_empty());

    assert_eq!(
        manager.apply_oldest_change(SingletonOldestChange::OldestChanged(Some(node_b.clone()))),
        vec![SingletonManagerEffect::SendHandOverToMe { to: node_a.clone() }]
    );
    assert_eq!(
        manager.state(),
        &SingletonManagerState::BecomingOldest {
            previous_oldest: vec![node_a.clone()],
            handover_started: false,
        }
    );

    assert!(manager.hand_over_in_progress(&node_a).is_empty());
    assert_eq!(
        manager.state(),
        &SingletonManagerState::BecomingOldest {
            previous_oldest: vec![node_a.clone()],
            handover_started: true,
        }
    );
    assert_eq!(
        manager.hand_over_done(&node_a),
        vec![SingletonManagerEffect::StartSingleton]
    );
    assert_eq!(
        manager.state(),
        &SingletonManagerState::Oldest {
            singleton_running: true,
        }
    );
}

#[test]
fn singleton_manager_starts_when_previous_oldest_is_removed() {
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let (_tracker, observation) = SingletonOldestTracker::from_members(
        node_b.clone(),
        SingletonScope::all(),
        [
            member(node_a.clone(), MemberStatus::Leaving, 1),
            member(node_b.clone(), MemberStatus::Up, 2),
        ],
    );
    let mut manager = SingletonManagerRuntime::new(node_b);
    assert!(manager.apply_initial_observation(observation).is_empty());

    assert!(manager.mark_removed(node_a).is_empty());
    assert_eq!(
        manager.apply_oldest_change(SingletonOldestChange::OldestChanged(Some(node("b", 2)))),
        vec![SingletonManagerEffect::StartSingleton]
    );
    assert_eq!(
        manager.state(),
        &SingletonManagerState::Oldest {
            singleton_running: true,
        }
    );
}

#[test]
fn singleton_manager_hands_over_when_oldest_changes_away() {
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let (_tracker, observation) = SingletonOldestTracker::from_members(
        node_a.clone(),
        SingletonScope::all(),
        [member(node_a.clone(), MemberStatus::Up, 1)],
    );
    let mut manager = SingletonManagerRuntime::new(node_a.clone());
    manager.apply_initial_observation(observation);

    assert_eq!(
        manager.apply_oldest_change(SingletonOldestChange::OldestChanged(Some(node_b.clone()))),
        vec![SingletonManagerEffect::SendTakeOverFromMe { to: node_b.clone() }]
    );
    assert_eq!(
        manager.hand_over_to_me(node_b.clone()),
        vec![
            SingletonManagerEffect::SendHandOverInProgress { to: node_b.clone() },
            SingletonManagerEffect::StopSingleton,
        ]
    );
    assert_eq!(
        manager.state(),
        &SingletonManagerState::HandingOver {
            singleton_running: true,
            handover_to: Some(node_b.clone()),
        }
    );

    assert_eq!(
        manager.singleton_terminated(),
        vec![SingletonManagerEffect::SendHandOverDone { to: node_b }]
    );
    assert_eq!(manager.state(), &SingletonManagerState::End);
}

#[test]
fn singleton_manager_actor_applies_initial_observation_in_mailbox_turn() {
    let node_a = node("singleton-actor-a", 1);
    let (_tracker, observation) = SingletonOldestTracker::from_members(
        node_a.clone(),
        SingletonScope::all(),
        [member(node_a.clone(), MemberStatus::Up, 1)],
    );
    let kit = ActorSystemTestKit::new("singleton-manager-actor-initial").unwrap();
    let manager = kit
        .system()
        .spawn(
            "singleton-manager",
            SingletonManagerActor::props(node_a.clone()),
        )
        .unwrap();
    let effects = kit
        .create_probe::<Vec<SingletonManagerEffect>>("effects")
        .unwrap();
    let state = kit
        .create_probe::<SingletonManagerSnapshot>("state")
        .unwrap();

    manager
        .tell(SingletonManagerMsg::ApplyInitialObservation {
            observation,
            reply_to: Some(effects.actor_ref()),
        })
        .unwrap();
    effects
        .expect_msg_eq(
            vec![SingletonManagerEffect::StartSingleton],
            Duration::from_millis(500),
        )
        .unwrap();

    manager
        .tell(SingletonManagerMsg::GetState {
            reply_to: state.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        state.expect_msg(Duration::from_millis(500)).unwrap(),
        SingletonManagerSnapshot {
            self_node: node_a,
            state: SingletonManagerState::Oldest {
                singleton_running: true,
            },
            removed_members: Vec::new(),
        }
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn singleton_manager_actor_runs_handover_protocol_messages_in_order() {
    let node_a = node("singleton-actor-a", 1);
    let node_b = node("singleton-actor-b", 2);
    let (_tracker, observation) = SingletonOldestTracker::from_members(
        node_b.clone(),
        SingletonScope::all(),
        [
            member(node_a.clone(), MemberStatus::Up, 1),
            member(node_b.clone(), MemberStatus::Up, 2),
        ],
    );
    let kit = ActorSystemTestKit::new("singleton-manager-actor-handover").unwrap();
    let manager = kit
        .system()
        .spawn(
            "singleton-manager",
            SingletonManagerActor::props(node_b.clone()),
        )
        .unwrap();
    let effects = kit
        .create_probe::<Vec<SingletonManagerEffect>>("effects")
        .unwrap();
    let state = kit
        .create_probe::<SingletonManagerSnapshot>("state")
        .unwrap();

    manager
        .tell(SingletonManagerMsg::ApplyInitialObservation {
            observation,
            reply_to: Some(effects.actor_ref()),
        })
        .unwrap();
    effects
        .expect_msg_eq(Vec::new(), Duration::from_millis(500))
        .unwrap();

    manager
        .tell(SingletonManagerMsg::ApplyOldestChange {
            change: SingletonOldestChange::OldestChanged(Some(node_b.clone())),
            reply_to: Some(effects.actor_ref()),
        })
        .unwrap();
    effects
        .expect_msg_eq(
            vec![SingletonManagerEffect::SendHandOverToMe { to: node_a.clone() }],
            Duration::from_millis(500),
        )
        .unwrap();

    manager
        .tell(SingletonManagerMsg::HandOverInProgress {
            from: node_a.clone(),
            reply_to: Some(effects.actor_ref()),
        })
        .unwrap();
    effects
        .expect_msg_eq(Vec::new(), Duration::from_millis(500))
        .unwrap();

    manager
        .tell(SingletonManagerMsg::HandOverDone {
            from: node_a,
            reply_to: Some(effects.actor_ref()),
        })
        .unwrap();
    effects
        .expect_msg_eq(
            vec![SingletonManagerEffect::StartSingleton],
            Duration::from_millis(500),
        )
        .unwrap();

    manager
        .tell(SingletonManagerMsg::GetState {
            reply_to: state.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        state.expect_msg(Duration::from_millis(500)).unwrap().state,
        SingletonManagerState::Oldest {
            singleton_running: true,
        }
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[derive(Clone)]
enum LocalSingletonProbeMsg {
    Stop,
    Ping(ActorRef<&'static str>),
}

struct LocalSingletonProbe {
    started: ActorRef<&'static str>,
    stopped: ActorRef<&'static str>,
}

impl Actor for LocalSingletonProbe {
    type Msg = LocalSingletonProbeMsg;

    fn started(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        let _ = self.started.tell("started");
        Ok(())
    }

    fn stopped(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        let _ = self.stopped.tell("stopped");
        Ok(())
    }

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            LocalSingletonProbeMsg::Stop => ctx.stop(ctx.myself())?,
            LocalSingletonProbeMsg::Ping(reply_to) => {
                let _ = reply_to.tell("pong");
            }
        }
        Ok(())
    }
}

#[test]
fn local_singleton_manager_spawns_child_when_oldest() {
    let node_a = node("local-singleton-a", 1);
    let (_tracker, observation) = SingletonOldestTracker::from_members(
        node_a.clone(),
        SingletonScope::all(),
        [member(node_a.clone(), MemberStatus::Up, 1)],
    );
    let kit = ActorSystemTestKit::new("local-singleton-start").unwrap();
    let started = kit.create_probe::<&'static str>("started").unwrap();
    let stopped = kit.create_probe::<&'static str>("stopped").unwrap();
    let effects = kit
        .create_probe::<Vec<SingletonManagerEffect>>("effects")
        .unwrap();
    let singleton_reply = kit
        .create_probe::<Option<ActorRef<LocalSingletonProbeMsg>>>("singleton-ref")
        .unwrap();
    let ping_reply = kit.create_probe::<&'static str>("ping").unwrap();
    let state = kit
        .create_probe::<LocalSingletonManagerSnapshot>("state")
        .unwrap();
    let manager = kit
        .system()
        .spawn(
            "local-singleton-manager",
            LocalSingletonManagerActor::<LocalSingletonProbe>::props(
                node_a.clone(),
                "singleton",
                {
                    let started = started.actor_ref();
                    let stopped = stopped.actor_ref();
                    move || {
                        let started = started.clone();
                        let stopped = stopped.clone();
                        Props::new(move || LocalSingletonProbe { started, stopped })
                    }
                },
                LocalSingletonProbeMsg::Stop,
            ),
        )
        .unwrap();

    manager
        .tell(LocalSingletonManagerMsg::ApplyInitialObservation {
            observation,
            reply_to: Some(effects.actor_ref()),
        })
        .unwrap();

    effects
        .expect_msg_eq(
            vec![SingletonManagerEffect::StartSingleton],
            Duration::from_millis(500),
        )
        .unwrap();
    started
        .expect_msg_eq("started", Duration::from_millis(500))
        .unwrap();

    manager
        .tell(LocalSingletonManagerMsg::GetSingleton {
            reply_to: singleton_reply.actor_ref(),
        })
        .unwrap();
    let singleton = singleton_reply
        .expect_msg(Duration::from_millis(500))
        .unwrap()
        .expect("singleton child should be available");
    singleton
        .tell(LocalSingletonProbeMsg::Ping(ping_reply.actor_ref()))
        .unwrap();
    ping_reply
        .expect_msg_eq("pong", Duration::from_millis(500))
        .unwrap();

    manager
        .tell(LocalSingletonManagerMsg::GetState {
            reply_to: state.actor_ref(),
        })
        .unwrap();
    let snapshot = state.expect_msg(Duration::from_millis(500)).unwrap();
    assert_eq!(
        snapshot.state,
        SingletonManagerState::Oldest {
            singleton_running: true,
        }
    );
    assert_eq!(snapshot.self_node, node_a);
    assert!(snapshot.singleton_path.is_some());
    stopped.expect_no_msg(Duration::from_millis(100)).unwrap();
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn local_singleton_manager_stops_child_before_handover_done() {
    let node_a = node("local-singleton-oldest", 1);
    let node_b = node("local-singleton-new", 2);
    let (_tracker, observation) = SingletonOldestTracker::from_members(
        node_a.clone(),
        SingletonScope::all(),
        [
            member(node_a.clone(), MemberStatus::Up, 1),
            member(node_b.clone(), MemberStatus::Up, 2),
        ],
    );
    let kit = ActorSystemTestKit::new("local-singleton-handover").unwrap();
    let started = kit.create_probe::<&'static str>("started").unwrap();
    let stopped = kit.create_probe::<&'static str>("stopped").unwrap();
    let effects = kit
        .create_probe::<Vec<SingletonManagerEffect>>("effects")
        .unwrap();
    let state = kit
        .create_probe::<LocalSingletonManagerSnapshot>("state")
        .unwrap();
    let manager = kit
        .system()
        .spawn(
            "local-singleton-manager",
            LocalSingletonManagerActor::<LocalSingletonProbe>::props(
                node_a,
                "singleton",
                {
                    let started = started.actor_ref();
                    let stopped = stopped.actor_ref();
                    move || {
                        let started = started.clone();
                        let stopped = stopped.clone();
                        Props::new(move || LocalSingletonProbe { started, stopped })
                    }
                },
                LocalSingletonProbeMsg::Stop,
            ),
        )
        .unwrap();

    manager
        .tell(LocalSingletonManagerMsg::ApplyInitialObservation {
            observation,
            reply_to: Some(effects.actor_ref()),
        })
        .unwrap();
    effects
        .expect_msg_eq(
            vec![SingletonManagerEffect::StartSingleton],
            Duration::from_millis(500),
        )
        .unwrap();
    started
        .expect_msg_eq("started", Duration::from_millis(500))
        .unwrap();

    manager
        .tell(LocalSingletonManagerMsg::ApplyOldestChange {
            change: SingletonOldestChange::OldestChanged(Some(node_b.clone())),
            reply_to: Some(effects.actor_ref()),
        })
        .unwrap();
    effects
        .expect_msg_eq(
            vec![SingletonManagerEffect::SendTakeOverFromMe { to: node_b.clone() }],
            Duration::from_millis(500),
        )
        .unwrap();

    manager
        .tell(LocalSingletonManagerMsg::HandOverToMe {
            from: node_b.clone(),
            reply_to: Some(effects.actor_ref()),
        })
        .unwrap();
    effects
        .expect_msg_eq(
            vec![
                SingletonManagerEffect::SendHandOverInProgress { to: node_b },
                SingletonManagerEffect::StopSingleton,
            ],
            Duration::from_millis(500),
        )
        .unwrap();
    stopped
        .expect_msg_eq("stopped", Duration::from_millis(500))
        .unwrap();

    let mut end_snapshot = None;
    for _ in 0..100 {
        manager
            .tell(LocalSingletonManagerMsg::GetState {
                reply_to: state.actor_ref(),
            })
            .unwrap();
        let snapshot = state.expect_msg(Duration::from_millis(500)).unwrap();
        if snapshot.state == SingletonManagerState::End {
            end_snapshot = Some(snapshot);
            break;
        }
    }
    let snapshot = end_snapshot.expect("singleton manager should finish handover");
    assert!(snapshot.singleton_path.is_none());
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

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
fn local_topic_broadcasts_to_direct_and_group_subscribers() {
    let kit = ActorSystemTestKit::new("topic-broadcast").unwrap();
    let direct = kit.create_probe::<String>("direct").unwrap();
    let grouped_a = kit.create_probe::<String>("grouped-a").unwrap();
    let grouped_b = kit.create_probe::<String>("grouped-b").unwrap();
    let mut topic = LocalTopic::new(TopicName::new("orders"));

    assert!(topic.subscribe(direct.actor_ref()).inserted);
    assert!(!topic.subscribe(direct.actor_ref()).inserted);
    assert!(
        topic
            .subscribe_group("workers", grouped_a.actor_ref())
            .inserted
    );
    assert!(
        topic
            .subscribe_group("workers", grouped_b.actor_ref())
            .inserted
    );

    let report = topic.publish("created".to_string(), TopicPublishMode::Broadcast);

    assert_eq!(report.delivered, 3);
    assert_eq!(report.failed, 0);
    assert!(!report.no_subscribers);
    direct
        .expect_msg_eq("created".to_string(), Duration::from_millis(200))
        .unwrap();
    grouped_a
        .expect_msg_eq("created".to_string(), Duration::from_millis(200))
        .unwrap();
    grouped_b
        .expect_msg_eq("created".to_string(), Duration::from_millis(200))
        .unwrap();
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn local_topic_one_per_group_uses_deterministic_group_routing() {
    let kit = ActorSystemTestKit::new("topic-one-per-group").unwrap();
    let direct = kit.create_probe::<String>("direct").unwrap();
    let red_a = kit.create_probe::<String>("red-a").unwrap();
    let red_b = kit.create_probe::<String>("red-b").unwrap();
    let blue = kit.create_probe::<String>("blue").unwrap();
    let mut topic = LocalTopic::new(TopicName::new("jobs"));

    topic.subscribe(direct.actor_ref());
    topic.subscribe_group("red", red_a.actor_ref());
    topic.subscribe_group("red", red_b.actor_ref());
    topic.subscribe_group("blue", blue.actor_ref());

    let first = topic.publish("first".to_string(), TopicPublishMode::OnePerGroup);
    assert_eq!(first.delivered, 2);
    red_a
        .expect_msg_eq("first".to_string(), Duration::from_millis(200))
        .unwrap();
    blue.expect_msg_eq("first".to_string(), Duration::from_millis(200))
        .unwrap();
    direct.expect_no_msg(Duration::from_millis(30)).unwrap();
    red_b.expect_no_msg(Duration::from_millis(30)).unwrap();

    let second = topic.publish("second".to_string(), TopicPublishMode::OnePerGroup);
    assert_eq!(second.delivered, 2);
    red_b
        .expect_msg_eq("second".to_string(), Duration::from_millis(200))
        .unwrap();
    blue.expect_msg_eq("second".to_string(), Duration::from_millis(200))
        .unwrap();
    red_a.expect_no_msg(Duration::from_millis(30)).unwrap();
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn local_topic_unsubscribe_and_remove_subscriber_updates_empty_state() {
    let kit = ActorSystemTestKit::new("topic-remove").unwrap();
    let direct = kit.create_probe::<String>("direct").unwrap();
    let grouped = kit.create_probe::<String>("grouped").unwrap();
    let mut topic = LocalTopic::new(TopicName::new("events"));

    topic.subscribe(direct.actor_ref());
    topic.subscribe_group("listeners", grouped.actor_ref());
    assert_eq!(topic.subscriber_count(), 2);
    assert_eq!(topic.group_count(), 1);

    assert!(topic.unsubscribe(&direct.actor_ref()));
    assert!(!topic.unsubscribe(&direct.actor_ref()));
    assert_eq!(topic.subscriber_count(), 1);

    assert!(topic.remove_subscriber(&grouped.actor_ref()));
    assert_eq!(topic.group_count(), 0);
    assert!(topic.is_empty());

    let report = topic.publish("ignored".to_string(), TopicPublishMode::Broadcast);
    assert_eq!(report.delivered, 0);
    assert!(report.no_subscribers);
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn local_pubsub_lists_topics_and_removes_empty_topics() {
    let kit = ActorSystemTestKit::new("pubsub-topics").unwrap();
    let direct = kit.create_probe::<String>("direct").unwrap();
    let grouped = kit.create_probe::<String>("grouped").unwrap();
    let orders = TopicName::new("orders");
    let jobs = TopicName::new("jobs");
    let mut pubsub = LocalPubSub::new();

    pubsub.subscribe(orders.clone(), direct.actor_ref());
    pubsub.subscribe_group(jobs.clone(), "workers", grouped.actor_ref());
    assert_eq!(
        pubsub.current_topics(),
        BTreeSet::from([jobs.clone(), orders.clone()])
    );

    assert!(pubsub.unsubscribe(&orders, &direct.actor_ref()));
    assert_eq!(pubsub.current_topics(), BTreeSet::from([jobs.clone()]));

    assert!(pubsub.unsubscribe_group(&jobs, "workers", &grouped.actor_ref()));
    assert!(pubsub.current_topics().is_empty());
    assert_eq!(pubsub.topic_count(), 0);
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn local_pubsub_routes_publish_to_named_topic_only() {
    let kit = ActorSystemTestKit::new("pubsub-route").unwrap();
    let orders_probe = kit.create_probe::<String>("orders-probe").unwrap();
    let jobs_probe = kit.create_probe::<String>("jobs-probe").unwrap();
    let orders = TopicName::new("orders");
    let jobs = TopicName::new("jobs");
    let mut pubsub = LocalPubSub::new();

    pubsub.subscribe(orders.clone(), orders_probe.actor_ref());
    pubsub.subscribe(jobs.clone(), jobs_probe.actor_ref());

    let report = pubsub.publish(&orders, "created".to_string(), TopicPublishMode::Broadcast);
    assert_eq!(report.topic, orders);
    assert_eq!(report.report.delivered, 1);
    assert!(!report.report.no_subscribers);
    orders_probe
        .expect_msg_eq("created".to_string(), Duration::from_millis(200))
        .unwrap();
    jobs_probe.expect_no_msg(Duration::from_millis(30)).unwrap();

    let missing = pubsub.publish(
        &TopicName::new("missing"),
        "lost".to_string(),
        TopicPublishMode::Broadcast,
    );
    assert_eq!(missing.report.delivered, 0);
    assert!(missing.report.no_subscribers);
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn local_pubsub_removes_subscriber_from_all_topics() {
    let kit = ActorSystemTestKit::new("pubsub-remove").unwrap();
    let shared = kit.create_probe::<String>("shared").unwrap();
    let other = kit.create_probe::<String>("other").unwrap();
    let orders = TopicName::new("orders");
    let jobs = TopicName::new("jobs");
    let mut pubsub = LocalPubSub::new();

    pubsub.subscribe(orders.clone(), shared.actor_ref());
    pubsub.subscribe_group(jobs.clone(), "workers", shared.actor_ref());
    pubsub.subscribe_group(jobs.clone(), "workers", other.actor_ref());

    assert_eq!(
        pubsub.remove_subscriber(&shared.actor_ref()),
        vec![jobs.clone(), orders.clone()]
    );
    assert_eq!(pubsub.current_topics(), BTreeSet::from([jobs.clone()]));
    assert_eq!(
        pubsub
            .topic(&jobs)
            .map(|topic| topic.group_subscriber_count("workers")),
        Some(1)
    );

    let report = pubsub.publish(&jobs, "work".to_string(), TopicPublishMode::OnePerGroup);
    assert_eq!(report.report.delivered, 1);
    other
        .expect_msg_eq("work".to_string(), Duration::from_millis(200))
        .unwrap();
    shared.expect_no_msg(Duration::from_millis(30)).unwrap();
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn local_pubsub_publishes_only_to_selected_group() {
    let kit = ActorSystemTestKit::new("pubsub-group-target").unwrap();
    let red_a = kit.create_probe::<String>("red-a").unwrap();
    let red_b = kit.create_probe::<String>("red-b").unwrap();
    let blue = kit.create_probe::<String>("blue").unwrap();
    let topic = TopicName::new("jobs");
    let mut pubsub = LocalPubSub::new();

    pubsub.subscribe_group(topic.clone(), "red", red_a.actor_ref());
    pubsub.subscribe_group(topic.clone(), "red", red_b.actor_ref());
    pubsub.subscribe_group(topic.clone(), "blue", blue.actor_ref());

    let first = pubsub.publish_group(&topic, "red", "one".to_string());
    assert_eq!(first.report.delivered, 1);
    assert!(!first.report.no_subscribers);
    red_a
        .expect_msg_eq("one".to_string(), Duration::from_millis(200))
        .unwrap();
    red_b.expect_no_msg(Duration::from_millis(30)).unwrap();
    blue.expect_no_msg(Duration::from_millis(30)).unwrap();

    let second = pubsub.publish_group(&topic, "red", "two".to_string());
    assert_eq!(second.report.delivered, 1);
    red_b
        .expect_msg_eq("two".to_string(), Duration::from_millis(200))
        .unwrap();

    let missing = pubsub.publish_group(&topic, "missing", "ignored".to_string());
    assert!(missing.report.no_subscribers);
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn local_pubsub_actor_subscribes_publishes_and_lists_topics() {
    let kit = ActorSystemTestKit::new("pubsub-actor").unwrap();
    let pubsub = kit
        .system()
        .spawn("pubsub", Props::new(LocalPubSubActor::<String>::new))
        .unwrap();
    let subscriber = kit.create_probe::<String>("subscriber").unwrap();
    let ack_probe = kit.create_probe::<PubSubSubscribeAck>("acks").unwrap();
    let report_probe = kit.create_probe::<PubSubTopicReport>("reports").unwrap();
    let topics_probe = kit.create_probe::<CurrentTopics>("topics").unwrap();
    let orders = TopicName::new("orders");

    pubsub
        .tell(LocalPubSubMsg::Subscribe {
            topic: orders.clone(),
            subscriber: subscriber.actor_ref(),
            reply_to: Some(ack_probe.actor_ref()),
        })
        .unwrap();
    assert_eq!(
        ack_probe.expect_msg(Duration::from_millis(200)).unwrap(),
        PubSubSubscribeAck {
            topic: orders.clone(),
            group: None,
            changed: true,
        }
    );

    pubsub
        .tell(LocalPubSubMsg::Publish {
            topic: orders.clone(),
            message: "created".to_string(),
            mode: TopicPublishMode::Broadcast,
            reply_to: Some(report_probe.actor_ref()),
        })
        .unwrap();
    subscriber
        .expect_msg_eq("created".to_string(), Duration::from_millis(200))
        .unwrap();
    assert_eq!(
        report_probe.expect_msg(Duration::from_millis(200)).unwrap(),
        PubSubTopicReport {
            topic: orders.clone(),
            report: crate::TopicPublishReport {
                delivered: 1,
                failed: 0,
                no_subscribers: false,
            },
        }
    );

    pubsub
        .tell(LocalPubSubMsg::GetTopics {
            reply_to: topics_probe.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        topics_probe.expect_msg(Duration::from_millis(200)).unwrap(),
        CurrentTopics {
            topics: BTreeSet::from([orders]),
        }
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn local_pubsub_actor_removes_terminated_subscribers() {
    let kit = ActorSystemTestKit::new("pubsub-actor-terminated").unwrap();
    let pubsub = kit
        .system()
        .spawn("pubsub", Props::new(LocalPubSubActor::<String>::new))
        .unwrap();
    let subscriber = kit.create_probe::<String>("subscriber").unwrap();
    let report_probe = kit.create_probe::<PubSubTopicReport>("reports").unwrap();
    let topic = TopicName::new("orders");

    pubsub
        .tell(LocalPubSubMsg::Subscribe {
            topic: topic.clone(),
            subscriber: subscriber.actor_ref(),
            reply_to: None,
        })
        .unwrap();
    kit.system().stop(&subscriber.actor_ref());
    assert!(
        subscriber
            .actor_ref()
            .wait_for_stop(Duration::from_millis(500))
    );

    pubsub
        .tell(LocalPubSubMsg::Publish {
            topic: topic.clone(),
            message: "ignored".to_string(),
            mode: TopicPublishMode::Broadcast,
            reply_to: Some(report_probe.actor_ref()),
        })
        .unwrap();
    assert_eq!(
        report_probe.expect_msg(Duration::from_millis(500)).unwrap(),
        PubSubTopicReport {
            topic,
            report: crate::TopicPublishReport {
                delivered: 0,
                failed: 0,
                no_subscribers: true,
            },
        }
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn pubsub_registry_collects_and_merges_versioned_topic_deltas() {
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let orders = TopicName::new("orders");
    let jobs = TopicName::new("jobs");
    let mut source = PubSubRegistryState::new(node_a.clone());
    let mut target = PubSubRegistryState::new(node_b.clone());

    source.register_local_topic(orders.clone());
    let initial_delta = source.collect_delta(&target.versions(), 10);
    source.unregister_local_topic(orders.clone());
    source.register_local_group(jobs.clone(), "workers");
    target.merge_delta(source.collect_delta(&target.versions(), 10));

    assert!(target.broadcast_targets(&orders, true).is_empty());
    assert_eq!(target.broadcast_targets(&jobs, false), vec![node_a.clone()]);
    assert_eq!(
        target.one_per_group_targets(&jobs).get("workers"),
        Some(&node_a)
    );

    target.merge_delta(initial_delta);
    assert!(target.broadcast_targets(&orders, true).is_empty());
}

#[test]
fn pubsub_registry_collect_delta_respects_peer_versions_and_entry_limit() {
    let node_a = node("a", 1);
    let topic = TopicName::new("jobs");
    let mut registry = PubSubRegistryState::new(node_a.clone());
    registry.register_local_group(topic.clone(), "red");
    registry.register_local_group(topic.clone(), "blue");

    let limited = registry.collect_delta(&BTreeMap::new(), 1);
    assert_eq!(limited.buckets.len(), 1);
    assert_eq!(limited.buckets[0].entries.len(), 1);

    let full = registry.collect_delta(&BTreeMap::new(), 10);
    let peer_versions = BTreeMap::from([(node_a.ordering_key(), full.buckets[0].version)]);
    assert!(
        registry
            .collect_delta(&peer_versions, 10)
            .buckets
            .is_empty()
    );
}

#[test]
fn pubsub_registry_plans_one_remote_target_per_group_deterministically() {
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let node_c = node("c", 3);
    let topic = TopicName::new("jobs");
    let mut node_a_registry = PubSubRegistryState::new(node_a.clone());
    let mut node_b_registry = PubSubRegistryState::new(node_b.clone());
    let mut merged = PubSubRegistryState::new(node_c);

    node_a_registry.register_local_group(topic.clone(), "workers");
    node_b_registry.register_local_group(topic.clone(), "workers");
    merged.merge_delta(node_b_registry.collect_delta(&BTreeMap::new(), 10));
    merged.merge_delta(node_a_registry.collect_delta(&BTreeMap::new(), 10));

    assert_eq!(
        merged.one_per_group_targets(&topic),
        BTreeMap::from([("workers".to_string(), node_a)])
    );
}

#[test]
fn pubsub_registry_prunes_old_tombstones_without_dropping_present_entries() {
    let node_a = node("a", 1);
    let orders = TopicName::new("orders");
    let jobs = TopicName::new("jobs");
    let mut registry = PubSubRegistryState::new(node_a);

    registry.register_local_topic(orders.clone());
    registry.unregister_local_topic(orders.clone());
    registry.register_local_topic(jobs.clone());
    registry.prune_tombstones_older_than(0);

    let bucket = registry.bucket(registry.self_node()).unwrap();
    assert!(
        !bucket
            .entries
            .contains_key(&PubSubRegistryKey::topic(orders))
    );
    assert!(bucket.entries.contains_key(&PubSubRegistryKey::topic(jobs)));
}

#[test]
fn pubsub_gossip_actor_sends_status_to_peers_on_tick() {
    let kit = ActorSystemTestKit::new("pubsub-gossip-tick").unwrap();
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let node_c = node("c", 3);
    let peer_b = kit.create_probe::<PubSubGossipMsg>("peer-b").unwrap();
    let peer_c = kit.create_probe::<PubSubGossipMsg>("peer-c").unwrap();
    let actor_node = node_a.clone();
    let gossip = kit
        .system()
        .spawn(
            "gossip",
            Props::new(move || PubSubGossipActor::new(actor_node)),
        )
        .unwrap();

    gossip
        .tell(PubSubGossipMsg::RegisterTopic {
            topic: TopicName::new("orders"),
        })
        .unwrap();
    gossip
        .tell(PubSubGossipMsg::AddPeer {
            peer: PubSubGossipPeer::new(node_b.clone(), peer_b.actor_ref()),
        })
        .unwrap();
    gossip
        .tell(PubSubGossipMsg::AddPeer {
            peer: PubSubGossipPeer::new(node_c, peer_c.actor_ref()),
        })
        .unwrap();

    gossip.tell(PubSubGossipMsg::GossipTick).unwrap();
    match peer_b.expect_msg(Duration::from_millis(500)).unwrap() {
        PubSubGossipMsg::Status {
            from,
            versions,
            reply,
        } => {
            assert_eq!(from, node_a);
            assert!(!reply);
            assert_eq!(versions.get(&node("a", 1).ordering_key()), Some(&1));
        }
        _ => panic!("expected status gossip"),
    }
    peer_c.expect_no_msg(Duration::from_millis(30)).unwrap();

    gossip.tell(PubSubGossipMsg::GossipTick).unwrap();
    match peer_c.expect_msg(Duration::from_millis(500)).unwrap() {
        PubSubGossipMsg::Status { reply, .. } => assert!(!reply),
        _ => panic!("expected status gossip"),
    }
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn pubsub_gossip_actor_replies_to_status_with_delta_and_status_when_needed() {
    let kit = ActorSystemTestKit::new("pubsub-gossip-status").unwrap();
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let peer_b = kit.create_probe::<PubSubGossipMsg>("peer-b").unwrap();
    let actor_node = node_a.clone();
    let gossip = kit
        .system()
        .spawn(
            "gossip",
            Props::new(move || PubSubGossipActor::new(actor_node)),
        )
        .unwrap();
    let orders = TopicName::new("orders");

    gossip
        .tell(PubSubGossipMsg::AddPeer {
            peer: PubSubGossipPeer::new(node_b.clone(), peer_b.actor_ref()),
        })
        .unwrap();
    gossip
        .tell(PubSubGossipMsg::RegisterTopic {
            topic: orders.clone(),
        })
        .unwrap();
    gossip
        .tell(PubSubGossipMsg::Status {
            from: node_b.clone(),
            versions: BTreeMap::new(),
            reply: false,
        })
        .unwrap();

    match peer_b.expect_msg(Duration::from_millis(500)).unwrap() {
        PubSubGossipMsg::Delta { from, delta } => {
            assert_eq!(from, node_a.clone());
            assert_eq!(delta.buckets.len(), 1);
            assert!(
                delta.buckets[0]
                    .entries
                    .contains_key(&PubSubRegistryKey::topic(orders))
            );
        }
        _ => panic!("expected delta reply"),
    }

    gossip
        .tell(PubSubGossipMsg::Status {
            from: node_b.clone(),
            versions: BTreeMap::from([(node_a.ordering_key(), 1), (node_b.ordering_key(), 1)]),
            reply: false,
        })
        .unwrap();
    match peer_b.expect_msg(Duration::from_millis(500)).unwrap() {
        PubSubGossipMsg::Status { from, reply, .. } => {
            assert_eq!(from, node("a", 1));
            assert!(reply);
        }
        _ => panic!("expected status reply"),
    }
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn pubsub_gossip_actor_merges_delta_from_known_peer() {
    let kit = ActorSystemTestKit::new("pubsub-gossip-delta").unwrap();
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let peer_b = kit.create_probe::<PubSubGossipMsg>("peer-b").unwrap();
    let registry_probe = kit.create_probe::<PubSubRegistryState>("registry").unwrap();
    let count_probe = kit.create_probe::<u64>("delta-count").unwrap();
    let actor_node = node_a;
    let gossip = kit
        .system()
        .spawn(
            "gossip",
            Props::new(move || PubSubGossipActor::new(actor_node)),
        )
        .unwrap();
    let jobs = TopicName::new("jobs");
    let mut remote_registry = PubSubRegistryState::new(node_b.clone());
    remote_registry.register_local_group(jobs.clone(), "workers");

    gossip
        .tell(PubSubGossipMsg::AddPeer {
            peer: PubSubGossipPeer::new(node_b.clone(), peer_b.actor_ref()),
        })
        .unwrap();
    gossip
        .tell(PubSubGossipMsg::Delta {
            from: node_b.clone(),
            delta: remote_registry.collect_delta(&BTreeMap::new(), 10),
        })
        .unwrap();
    gossip
        .tell(PubSubGossipMsg::GetRegistry {
            reply_to: registry_probe.actor_ref(),
        })
        .unwrap();
    gossip
        .tell(PubSubGossipMsg::GetDeltaCount {
            reply_to: count_probe.actor_ref(),
        })
        .unwrap();

    let registry = registry_probe
        .expect_msg(Duration::from_millis(500))
        .unwrap();
    assert_eq!(
        registry.one_per_group_targets(&jobs).get("workers"),
        Some(&node_b)
    );
    assert_eq!(
        count_probe.expect_msg(Duration::from_millis(500)).unwrap(),
        1
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn pubsub_gossip_actor_ignores_delta_from_unknown_peer_and_removes_left_peer() {
    let kit = ActorSystemTestKit::new("pubsub-gossip-unknown").unwrap();
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let peer_b = kit.create_probe::<PubSubGossipMsg>("peer-b").unwrap();
    let registry_probe = kit.create_probe::<PubSubRegistryState>("registry").unwrap();
    let peers_probe = kit.create_probe::<Vec<UniqueAddress>>("peers").unwrap();
    let actor_node = node_a.clone();
    let gossip = kit
        .system()
        .spawn(
            "gossip",
            Props::new(move || PubSubGossipActor::new(actor_node)),
        )
        .unwrap();
    let jobs = TopicName::new("jobs");
    let mut remote_registry = PubSubRegistryState::new(node_b.clone());
    remote_registry.register_local_topic(jobs.clone());
    let delta = remote_registry.collect_delta(&BTreeMap::new(), 10);

    gossip
        .tell(PubSubGossipMsg::Delta {
            from: node_b.clone(),
            delta: delta.clone(),
        })
        .unwrap();
    gossip
        .tell(PubSubGossipMsg::GetRegistry {
            reply_to: registry_probe.actor_ref(),
        })
        .unwrap();
    assert!(
        registry_probe
            .expect_msg(Duration::from_millis(500))
            .unwrap()
            .broadcast_targets(&jobs, true)
            .is_empty()
    );

    gossip
        .tell(PubSubGossipMsg::AddPeer {
            peer: PubSubGossipPeer::new(node_b.clone(), peer_b.actor_ref()),
        })
        .unwrap();
    gossip
        .tell(PubSubGossipMsg::Delta {
            from: node_b.clone(),
            delta,
        })
        .unwrap();
    gossip
        .tell(PubSubGossipMsg::RemovePeer {
            node: node_b.clone(),
        })
        .unwrap();
    gossip
        .tell(PubSubGossipMsg::GetRegistry {
            reply_to: registry_probe.actor_ref(),
        })
        .unwrap();
    gossip
        .tell(PubSubGossipMsg::GetPeers {
            reply_to: peers_probe.actor_ref(),
        })
        .unwrap();

    assert!(
        registry_probe
            .expect_msg(Duration::from_millis(500))
            .unwrap()
            .broadcast_targets(&jobs, true)
            .is_empty()
    );
    assert!(
        peers_probe
            .expect_msg(Duration::from_millis(500))
            .unwrap()
            .is_empty()
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn pubsub_delivery_plan_splits_broadcast_between_local_and_remote_nodes() {
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let topic = TopicName::new("orders");
    let mut local = PubSubRegistryState::new(node_a.clone());
    let mut remote = PubSubRegistryState::new(node_b.clone());

    local.register_local_topic(topic.clone());
    remote.register_local_topic(topic.clone());
    local.merge_delta(remote.collect_delta(&BTreeMap::new(), 10));

    let plan = PubSubDeliveryPlan::for_registry(&local, topic.clone(), TopicPublishMode::Broadcast);

    assert_eq!(plan.topic, topic);
    assert_eq!(plan.mode, TopicPublishMode::Broadcast);
    assert_eq!(
        plan.targets,
        vec![
            PubSubDeliveryTarget::LocalTopic,
            PubSubDeliveryTarget::RemoteTopic {
                node: node_b.clone(),
            },
        ]
    );
    assert!(plan.has_local_target());
    assert_eq!(plan.remote_nodes(), vec![node_b]);
}

#[test]
fn pubsub_delivery_plan_uses_one_target_per_group() {
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let node_c = node("c", 3);
    let topic = TopicName::new("jobs");
    let mut local = PubSubRegistryState::new(node_b.clone());
    let mut oldest_remote = PubSubRegistryState::new(node_a.clone());
    let mut other_remote = PubSubRegistryState::new(node_c.clone());

    local.register_local_group(topic.clone(), "red");
    oldest_remote.register_local_group(topic.clone(), "red");
    other_remote.register_local_group(topic.clone(), "blue");
    local.merge_delta(oldest_remote.collect_delta(&BTreeMap::new(), 10));
    local.merge_delta(other_remote.collect_delta(&BTreeMap::new(), 10));

    let plan =
        PubSubDeliveryPlan::for_registry(&local, topic.clone(), TopicPublishMode::OnePerGroup);

    assert_eq!(
        plan.targets,
        vec![
            PubSubDeliveryTarget::RemoteGroup {
                group: "blue".to_string(),
                node: node_c.clone(),
            },
            PubSubDeliveryTarget::RemoteGroup {
                group: "red".to_string(),
                node: node_a.clone(),
            },
        ]
    );
    assert!(!plan.has_local_target());
    assert_eq!(plan.remote_nodes(), vec![node_c, node_a]);
}

#[test]
fn pubsub_delivery_plan_reports_empty_when_registry_has_no_topic() {
    let local = PubSubRegistryState::new(node("a", 1));
    let plan = PubSubDeliveryPlan::for_registry(
        &local,
        TopicName::new("missing"),
        TopicPublishMode::Broadcast,
    );

    assert!(plan.is_empty());
    assert!(!plan.has_local_target());
    assert!(plan.remote_nodes().is_empty());
}

#[test]
fn pubsub_delivery_transport_sends_broadcast_to_local_and_remote_mediators() {
    let kit = ActorSystemTestKit::new("pubsub-delivery-broadcast").unwrap();
    let local_pubsub = kit
        .system()
        .spawn("pubsub-local", Props::new(LocalPubSubActor::<String>::new))
        .unwrap();
    let remote_pubsub = kit
        .system()
        .spawn("pubsub-remote", Props::new(LocalPubSubActor::<String>::new))
        .unwrap();
    let local_subscriber = kit.create_probe::<String>("local-sub").unwrap();
    let remote_subscriber = kit.create_probe::<String>("remote-sub").unwrap();
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let topic = TopicName::new("orders");
    let mut local_registry = PubSubRegistryState::new(node_a.clone());
    let mut remote_registry = PubSubRegistryState::new(node_b.clone());

    local_registry.register_local_topic(topic.clone());
    remote_registry.register_local_topic(topic.clone());
    local_registry.merge_delta(remote_registry.collect_delta(&BTreeMap::new(), 10));
    local_pubsub
        .tell(LocalPubSubMsg::Subscribe {
            topic: topic.clone(),
            subscriber: local_subscriber.actor_ref(),
            reply_to: None,
        })
        .unwrap();
    remote_pubsub
        .tell(LocalPubSubMsg::Subscribe {
            topic: topic.clone(),
            subscriber: remote_subscriber.actor_ref(),
            reply_to: None,
        })
        .unwrap();

    let plan =
        PubSubDeliveryPlan::for_registry(&local_registry, topic, TopicPublishMode::Broadcast);
    let mut transport = PubSubDeliveryTransport::new().with_local(local_pubsub);
    transport.insert_remote_target(PubSubRemoteTarget::new(node_b, remote_pubsub));
    let report = transport.publish(&plan, "created".to_string());

    assert_eq!(
        report.sent_to(),
        &[
            PubSubDeliveryTarget::LocalTopic,
            PubSubDeliveryTarget::RemoteTopic { node: node("b", 2) },
        ]
    );
    assert!(report.is_success());
    local_subscriber
        .expect_msg_eq("created".to_string(), Duration::from_millis(500))
        .unwrap();
    remote_subscriber
        .expect_msg_eq("created".to_string(), Duration::from_millis(500))
        .unwrap();
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn pubsub_delivery_transport_routes_one_per_group_to_selected_nodes_only() {
    let kit = ActorSystemTestKit::new("pubsub-delivery-groups").unwrap();
    let local_pubsub = kit
        .system()
        .spawn("pubsub-local", Props::new(LocalPubSubActor::<String>::new))
        .unwrap();
    let remote_a_pubsub = kit
        .system()
        .spawn(
            "pubsub-remote-a",
            Props::new(LocalPubSubActor::<String>::new),
        )
        .unwrap();
    let remote_c_pubsub = kit
        .system()
        .spawn(
            "pubsub-remote-c",
            Props::new(LocalPubSubActor::<String>::new),
        )
        .unwrap();
    let local_red = kit.create_probe::<String>("local-red").unwrap();
    let local_blue = kit.create_probe::<String>("local-blue").unwrap();
    let remote_red = kit.create_probe::<String>("remote-red").unwrap();
    let remote_blue = kit.create_probe::<String>("remote-blue").unwrap();
    let node_a = node("a", 1);
    let node_b = node("b", 2);
    let node_c = node("c", 3);
    let topic = TopicName::new("jobs");
    let mut local_registry = PubSubRegistryState::new(node_b.clone());
    let mut remote_a_registry = PubSubRegistryState::new(node_a.clone());
    let mut remote_c_registry = PubSubRegistryState::new(node_c.clone());

    local_registry.register_local_group(topic.clone(), "red");
    local_registry.register_local_group(topic.clone(), "blue");
    remote_a_registry.register_local_group(topic.clone(), "red");
    remote_c_registry.register_local_group(topic.clone(), "blue");
    local_registry.merge_delta(remote_a_registry.collect_delta(&BTreeMap::new(), 10));
    local_registry.merge_delta(remote_c_registry.collect_delta(&BTreeMap::new(), 10));

    local_pubsub
        .tell(LocalPubSubMsg::SubscribeGroup {
            topic: topic.clone(),
            group: "red".to_string(),
            subscriber: local_red.actor_ref(),
            reply_to: None,
        })
        .unwrap();
    local_pubsub
        .tell(LocalPubSubMsg::SubscribeGroup {
            topic: topic.clone(),
            group: "blue".to_string(),
            subscriber: local_blue.actor_ref(),
            reply_to: None,
        })
        .unwrap();
    remote_a_pubsub
        .tell(LocalPubSubMsg::SubscribeGroup {
            topic: topic.clone(),
            group: "red".to_string(),
            subscriber: remote_red.actor_ref(),
            reply_to: None,
        })
        .unwrap();
    remote_c_pubsub
        .tell(LocalPubSubMsg::SubscribeGroup {
            topic: topic.clone(),
            group: "blue".to_string(),
            subscriber: remote_blue.actor_ref(),
            reply_to: None,
        })
        .unwrap();

    let plan =
        PubSubDeliveryPlan::for_registry(&local_registry, topic, TopicPublishMode::OnePerGroup);
    let mut transport = PubSubDeliveryTransport::new().with_local(local_pubsub);
    transport.set_remote_targets([
        PubSubRemoteTarget::new(node_a.clone(), remote_a_pubsub),
        PubSubRemoteTarget::new(node_c.clone(), remote_c_pubsub),
    ]);
    let report = transport.publish(&plan, "run".to_string());

    assert_eq!(
        report.sent_to(),
        &[
            PubSubDeliveryTarget::LocalGroup {
                group: "blue".to_string(),
            },
            PubSubDeliveryTarget::RemoteGroup {
                group: "red".to_string(),
                node: node_a,
            },
        ]
    );
    assert!(report.is_success());
    local_blue
        .expect_msg_eq("run".to_string(), Duration::from_millis(500))
        .unwrap();
    remote_red
        .expect_msg_eq("run".to_string(), Duration::from_millis(500))
        .unwrap();
    local_red.expect_no_msg(Duration::from_millis(30)).unwrap();
    remote_blue
        .expect_no_msg(Duration::from_millis(30))
        .unwrap();
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn pubsub_delivery_transport_reports_missing_remote_targets() {
    let node_b = node("b", 2);
    let topic = TopicName::new("orders");
    let plan = PubSubDeliveryPlan {
        topic,
        mode: TopicPublishMode::Broadcast,
        targets: vec![
            PubSubDeliveryTarget::LocalTopic,
            PubSubDeliveryTarget::RemoteTopic {
                node: node_b.clone(),
            },
        ],
    };
    let kit = ActorSystemTestKit::new("pubsub-delivery-missing").unwrap();
    let local_pubsub = kit
        .system()
        .spawn("pubsub-local", Props::new(LocalPubSubActor::<String>::new))
        .unwrap();
    let transport = PubSubDeliveryTransport::new().with_local(local_pubsub);

    let report = transport.publish(&plan, "created".to_string());

    assert_eq!(report.sent_to(), &[PubSubDeliveryTarget::LocalTopic]);
    assert_eq!(
        report.failures(),
        &[PubSubDeliveryFailure::MissingTarget {
            target: PubSubDeliveryTarget::RemoteTopic { node: node_b },
        }]
    );
    assert!(!report.is_success());
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
