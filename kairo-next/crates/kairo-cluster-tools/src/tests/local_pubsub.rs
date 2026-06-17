use super::*;

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
fn local_pubsub_publish_prunes_stopped_subscriber_and_removes_empty_topic() {
    let kit = ActorSystemTestKit::new("pubsub-prune-stopped").unwrap();
    let subscriber = kit.create_probe::<String>("subscriber").unwrap();
    let topic = TopicName::new("orders");
    let mut pubsub = LocalPubSub::new();

    pubsub.subscribe(topic.clone(), subscriber.actor_ref());
    kit.system().stop(&subscriber.actor_ref());
    assert!(
        subscriber
            .actor_ref()
            .wait_for_stop(Duration::from_millis(500))
    );

    let report = pubsub.publish(&topic, "ignored".to_string(), TopicPublishMode::Broadcast);

    assert_eq!(report.topic, topic);
    assert_eq!(report.report.delivered, 0);
    assert_eq!(report.report.failed, 0);
    assert!(report.report.no_subscribers);
    assert!(pubsub.current_topics().is_empty());
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn local_pubsub_registers_path_sends_and_prunes_stopped_actor() {
    let kit = ActorSystemTestKit::new("pubsub-path-send").unwrap();
    let routee = kit.create_probe::<String>("routee").unwrap();
    let mut pubsub = LocalPubSub::new();
    let path = "/user/routee".to_string();

    assert_eq!(
        pubsub.register_path(routee.actor_ref()),
        PubSubPathRegistration {
            path: path.clone(),
            changed: true,
        }
    );
    assert_eq!(pubsub.current_paths(), BTreeSet::from([path.clone()]));

    let report = pubsub.send_path(&path, "first".to_string());
    assert_eq!(
        report,
        PubSubPathReport {
            path: path.clone(),
            report: crate::TopicPublishReport {
                delivered: 1,
                failed: 0,
                no_subscribers: false,
            },
        }
    );
    routee
        .expect_msg_eq("first".to_string(), Duration::from_millis(200))
        .unwrap();

    kit.system().stop(&routee.actor_ref());
    assert!(routee.actor_ref().wait_for_stop(Duration::from_millis(500)));
    let stopped = pubsub.send_path(&path, "dropped".to_string());
    assert!(stopped.report.no_subscribers);
    assert!(pubsub.current_paths().is_empty());
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
