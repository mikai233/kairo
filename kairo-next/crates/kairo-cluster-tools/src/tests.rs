use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use kairo_actor::{Address, Props};
use kairo_cluster::{ClusterEvent, Member, MemberEvent, MemberStatus, UniqueAddress};
use kairo_testkit::ActorSystemTestKit;

use crate::{
    CurrentTopics, LocalPubSub, LocalPubSubActor, LocalPubSubMsg, LocalTopic, PubSubRegistryKey,
    PubSubRegistryState, PubSubSubscribeAck, PubSubTopicReport, SingletonManagerEffect,
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
