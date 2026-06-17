use super::*;

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
fn distributed_pubsub_mediator_sends_to_registered_path_with_local_affinity() {
    let node_a = node("path-remote", 1);
    let node_b = node("path-local", 2);
    let path = "/user/worker".to_string();
    let local_kit = ActorSystemTestKit::new("distributed-pubsub-path-local").unwrap();
    let remote_kit = ActorSystemTestKit::new("distributed-pubsub-path-remote").unwrap();
    let local_routee = local_kit.create_probe::<String>("worker").unwrap();
    let remote_routee = remote_kit.create_probe::<String>("worker").unwrap();
    let registry_probe = remote_kit
        .create_probe::<PubSubRegistryState>("registry")
        .unwrap();
    let report_probe = local_kit
        .create_probe::<DistributedPubSubSendReport>("reports")
        .unwrap();
    let mediator_a = remote_kit
        .system()
        .spawn(
            "mediator-a",
            Props::new({
                let node_a = node_a.clone();
                move || DistributedPubSubMediatorActor::<String>::new(node_a)
            }),
        )
        .unwrap();
    let mediator_b = local_kit
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
        .tell(DistributedPubSubMediatorMsg::Put {
            actor: remote_routee.actor_ref(),
            reply_to: None,
        })
        .unwrap();
    mediator_b
        .tell(DistributedPubSubMediatorMsg::Put {
            actor: local_routee.actor_ref(),
            reply_to: None,
        })
        .unwrap();
    mediator_a
        .tell(DistributedPubSubMediatorMsg::GetRegistry {
            reply_to: registry_probe.actor_ref(),
        })
        .unwrap();
    let registry_a = registry_probe
        .expect_msg(Duration::from_millis(500))
        .unwrap();
    mediator_b
        .tell(DistributedPubSubMediatorMsg::AddRemoteMediator {
            node: node_a.clone(),
            mediator: mediator_a,
        })
        .unwrap();
    mediator_b
        .tell(DistributedPubSubMediatorMsg::MergeDelta {
            delta: registry_a.collect_delta(&BTreeMap::new(), 10),
        })
        .unwrap();

    mediator_b
        .tell(DistributedPubSubMediatorMsg::Send {
            path: path.clone(),
            message: "local".to_string(),
            local_affinity: true,
            reply_to: Some(report_probe.actor_ref()),
        })
        .unwrap();
    let local_report = report_probe.expect_msg(Duration::from_millis(500)).unwrap();
    assert_eq!(local_report.path, path);
    assert_eq!(
        local_report.plan.targets,
        vec![PubSubPathDeliveryTarget::LocalPath]
    );
    assert!(local_report.delivery.is_success());
    local_routee
        .expect_msg_eq("local".to_string(), Duration::from_millis(500))
        .unwrap();
    remote_routee
        .expect_no_msg(Duration::from_millis(50))
        .unwrap();

    mediator_b
        .tell(DistributedPubSubMediatorMsg::RemovePath {
            path: "/user/worker".to_string(),
            reply_to: None,
        })
        .unwrap();
    mediator_b
        .tell(DistributedPubSubMediatorMsg::Send {
            path: "/user/worker".to_string(),
            message: "remote".to_string(),
            local_affinity: false,
            reply_to: Some(report_probe.actor_ref()),
        })
        .unwrap();
    let remote_report = report_probe.expect_msg(Duration::from_millis(500)).unwrap();
    assert_eq!(
        remote_report.plan.targets,
        vec![PubSubPathDeliveryTarget::RemotePath { node: node_a }]
    );
    assert!(remote_report.delivery.is_success());
    remote_routee
        .expect_msg_eq("remote".to_string(), Duration::from_millis(500))
        .unwrap();
    local_routee
        .expect_no_msg(Duration::from_millis(50))
        .unwrap();
    local_kit.shutdown(Duration::from_secs(1)).unwrap();
    remote_kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn distributed_pubsub_mediator_sends_path_to_all_except_self() {
    let node_a = node("path-all-remote", 1);
    let node_b = node("path-all-local", 2);
    let path = "/user/worker".to_string();
    let local_kit = ActorSystemTestKit::new("distributed-pubsub-path-all-local").unwrap();
    let remote_kit = ActorSystemTestKit::new("distributed-pubsub-path-all-remote").unwrap();
    let local_routee = local_kit.create_probe::<String>("worker").unwrap();
    let remote_routee = remote_kit.create_probe::<String>("worker").unwrap();
    let registry_probe = remote_kit
        .create_probe::<PubSubRegistryState>("registry")
        .unwrap();
    let report_probe = local_kit
        .create_probe::<DistributedPubSubSendReport>("reports")
        .unwrap();
    let mediator_a = remote_kit
        .system()
        .spawn(
            "mediator-a",
            Props::new({
                let node_a = node_a.clone();
                move || DistributedPubSubMediatorActor::<String>::new(node_a)
            }),
        )
        .unwrap();
    let mediator_b = local_kit
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
        .tell(DistributedPubSubMediatorMsg::Put {
            actor: remote_routee.actor_ref(),
            reply_to: None,
        })
        .unwrap();
    mediator_b
        .tell(DistributedPubSubMediatorMsg::Put {
            actor: local_routee.actor_ref(),
            reply_to: None,
        })
        .unwrap();
    mediator_a
        .tell(DistributedPubSubMediatorMsg::GetRegistry {
            reply_to: registry_probe.actor_ref(),
        })
        .unwrap();
    let registry_a = registry_probe
        .expect_msg(Duration::from_millis(500))
        .unwrap();
    mediator_b
        .tell(DistributedPubSubMediatorMsg::AddRemoteMediator {
            node: node_a.clone(),
            mediator: mediator_a,
        })
        .unwrap();
    mediator_b
        .tell(DistributedPubSubMediatorMsg::MergeDelta {
            delta: registry_a.collect_delta(&BTreeMap::new(), 10),
        })
        .unwrap();

    mediator_b
        .tell(DistributedPubSubMediatorMsg::SendToAll {
            path: path.clone(),
            message: "broadcast".to_string(),
            all_but_self: true,
            reply_to: Some(report_probe.actor_ref()),
        })
        .unwrap();

    let report = report_probe.expect_msg(Duration::from_millis(500)).unwrap();
    assert_eq!(report.path, path);
    assert_eq!(
        report.mode,
        PubSubPathDeliveryMode::All { all_but_self: true }
    );
    assert_eq!(
        report.plan.targets,
        vec![PubSubPathDeliveryTarget::RemotePath { node: node_a }]
    );
    assert!(report.delivery.is_success());
    remote_routee
        .expect_msg_eq("broadcast".to_string(), Duration::from_millis(500))
        .unwrap();
    local_routee
        .expect_no_msg(Duration::from_millis(50))
        .unwrap();
    local_kit.shutdown(Duration::from_secs(1)).unwrap();
    remote_kit.shutdown(Duration::from_secs(1)).unwrap();
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
