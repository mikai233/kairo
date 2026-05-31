use super::*;

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
