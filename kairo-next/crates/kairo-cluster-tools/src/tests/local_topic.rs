use super::*;

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
fn local_topic_publish_prunes_stopped_direct_subscribers() {
    let kit = ActorSystemTestKit::new("topic-prune-direct").unwrap();
    let direct = kit.create_probe::<String>("direct").unwrap();
    let mut topic = LocalTopic::new(TopicName::new("events"));

    topic.subscribe(direct.actor_ref());
    kit.system().stop(&direct.actor_ref());
    assert!(direct.actor_ref().wait_for_stop(Duration::from_millis(500)));

    let report = topic.publish("ignored".to_string(), TopicPublishMode::Broadcast);

    assert_eq!(report.delivered, 0);
    assert_eq!(report.failed, 0);
    assert!(report.no_subscribers);
    assert!(topic.is_empty());
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn local_topic_group_publish_skips_and_prunes_stopped_subscriber() {
    let kit = ActorSystemTestKit::new("topic-prune-group").unwrap();
    let red_a = kit.create_probe::<String>("red-a").unwrap();
    let red_b = kit.create_probe::<String>("red-b").unwrap();
    let mut topic = LocalTopic::new(TopicName::new("jobs"));

    topic.subscribe_group("red", red_a.actor_ref());
    topic.subscribe_group("red", red_b.actor_ref());
    kit.system().stop(&red_a.actor_ref());
    assert!(red_a.actor_ref().wait_for_stop(Duration::from_millis(500)));

    let report = topic.publish_group("red", "work".to_string());

    assert_eq!(report.delivered, 1);
    assert_eq!(report.failed, 0);
    assert!(!report.no_subscribers);
    assert_eq!(topic.group_subscriber_count("red"), 1);
    red_b
        .expect_msg_eq("work".to_string(), Duration::from_millis(200))
        .unwrap();
    kit.shutdown(Duration::from_secs(1)).unwrap();
}
