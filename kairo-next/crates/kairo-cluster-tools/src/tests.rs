use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use kairo_actor::{Address, Props};
use kairo_cluster::{ClusterEvent, Member, MemberEvent, MemberStatus, UniqueAddress};
use kairo_testkit::ActorSystemTestKit;

use crate::{
    CurrentTopics, LocalPubSub, LocalPubSubActor, LocalPubSubMsg, LocalTopic,
    PubSubDeliveryFailure, PubSubDeliveryPlan, PubSubDeliveryTarget, PubSubDeliveryTransport,
    PubSubGossipActor, PubSubGossipMsg, PubSubGossipPeer, PubSubRegistryKey, PubSubRegistryState,
    PubSubRemoteTarget, PubSubSubscribeAck, PubSubTopicReport, SingletonManagerEffect,
    SingletonManagerRuntime, SingletonManagerState, SingletonOldestChange, SingletonOldestTracker,
    SingletonScope, TopicName, TopicPublishMode,
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
