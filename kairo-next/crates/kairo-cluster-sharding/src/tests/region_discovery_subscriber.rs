use super::*;

#[test]
fn region_discovery_subscriber_forwards_cluster_snapshot_to_region_registration() {
    let kit = kairo_testkit::ActorSystemTestKit::new("region-discovery-subscription").unwrap();
    let self_node = remote_unique_node("region-discovery-subscription", "127.0.0.1", 2660, 11);
    let coordinator_node =
        remote_unique_node("region-discovery-subscription", "127.0.0.1", 2661, 12);
    let publisher = kit
        .system()
        .spawn(
            "cluster-events",
            Props::new(move || ClusterEventPublisher::new(self_node.clone())),
        )
        .unwrap();
    let cluster = Cluster::new(publisher.clone());
    let current_state = kit
        .create_probe::<CurrentClusterState>("current-cluster-state")
        .unwrap();
    publisher
        .tell(ClusterEventPublisherMsg::PublishChanges(
            Gossip::from_members([cluster_member(
                coordinator_node.clone(),
                MemberStatus::Up,
                ["backend"],
                1,
            )]),
        ))
        .unwrap();
    publisher
        .tell(ClusterEventPublisherMsg::SendCurrentState {
            reply_to: current_state.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        current_state
            .expect_msg(Duration::from_millis(500))
            .unwrap()
            .members
            .len(),
        1
    );
    let coordinator = kit
        .system()
        .spawn(
            "coordinator",
            ShardCoordinatorActor::props_with_handoff(
                CoordinatorState::new(),
                LeastShardAllocationStrategy::default(),
                "stop".to_string(),
                Duration::from_millis(500),
                HandoffTransport::new(),
            ),
        )
        .unwrap();
    let discovery = RegionCoordinatorDiscoveryConfig::new(
        CoordinatorDiscoverySettings::default().with_required_role("backend"),
        Duration::from_millis(20),
    )
    .with_local_coordinator(coordinator_node, coordinator.clone());
    let region = kit
        .system()
        .spawn(
            "region-a",
            ShardRegionActor::<String>::props_with_local_shards_and_coordinator_discovery(
                "region-a", 10, 10, discovery,
            ),
        )
        .unwrap();
    let subscriber = kit
        .system()
        .spawn(
            "region-discovery",
            ShardRegionDiscoverySubscriber::<String>::props(cluster, region),
        )
        .unwrap();
    let subscriber_state = kit
        .create_probe::<ShardRegionDiscoverySubscriberSnapshot>("subscriber-state")
        .unwrap();
    let coordinator_state = kit
        .create_probe::<CoordinatorStateSnapshot>("subscription-coordinator-state")
        .unwrap();

    let mut registered = false;
    for _ in 0..20 {
        coordinator
            .tell(ShardCoordinatorMsg::GetState {
                reply_to: coordinator_state.actor_ref(),
            })
            .unwrap();
        let state = coordinator_state
            .expect_msg(Duration::from_millis(500))
            .unwrap();
        registered = state.allocations.contains_key("region-a");
        if registered {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        registered,
        "subscriber should forward the cluster snapshot into region discovery"
    );

    subscriber
        .tell(ShardRegionDiscoverySubscriberMsg::Snapshot {
            reply_to: subscriber_state.actor_ref(),
        })
        .unwrap();
    let state = subscriber_state
        .expect_msg(Duration::from_millis(500))
        .unwrap();
    assert!(state.subscribed);
    assert_eq!(state.forwarded_snapshots, 1);
    assert_eq!(state.last_error, None);
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn region_discovery_subscriber_reregisters_when_coordinator_moves() {
    let kit = kairo_testkit::ActorSystemTestKit::new("region-discovery-moves").unwrap();
    let self_node = remote_unique_node("region-discovery-moves", "127.0.0.1", 2670, 21);
    let coordinator_node_a = remote_unique_node("region-discovery-moves", "127.0.0.1", 2671, 22);
    let coordinator_node_b = remote_unique_node("region-discovery-moves", "127.0.0.1", 2672, 23);
    let publisher = kit
        .system()
        .spawn(
            "cluster-events",
            Props::new(move || ClusterEventPublisher::new(self_node.clone())),
        )
        .unwrap();
    let cluster = Cluster::new(publisher.clone());
    let coordinator_a = kit
        .system()
        .spawn(
            "coordinator-a",
            ShardCoordinatorActor::props_with_handoff(
                CoordinatorState::new(),
                LeastShardAllocationStrategy::default(),
                "stop".to_string(),
                Duration::from_millis(500),
                HandoffTransport::new(),
            ),
        )
        .unwrap();
    let coordinator_b = kit
        .system()
        .spawn(
            "coordinator-b",
            ShardCoordinatorActor::props_with_handoff(
                CoordinatorState::new(),
                LeastShardAllocationStrategy::default(),
                "stop".to_string(),
                Duration::from_millis(500),
                HandoffTransport::new(),
            ),
        )
        .unwrap();

    publisher
        .tell(ClusterEventPublisherMsg::PublishChanges(
            Gossip::from_members([
                cluster_member(coordinator_node_a.clone(), MemberStatus::Up, ["backend"], 1),
                cluster_member(coordinator_node_b.clone(), MemberStatus::Up, ["backend"], 2),
            ]),
        ))
        .unwrap();

    let discovery = RegionCoordinatorDiscoveryConfig::new(
        CoordinatorDiscoverySettings::default().with_required_role("backend"),
        Duration::from_millis(20),
    )
    .with_local_coordinator(coordinator_node_a.clone(), coordinator_a.clone())
    .with_local_coordinator(coordinator_node_b.clone(), coordinator_b.clone());
    let region = kit
        .system()
        .spawn(
            "region-a",
            ShardRegionActor::<String>::props_with_local_shards_and_coordinator_discovery(
                "region-a", 10, 10, discovery,
            ),
        )
        .unwrap();
    let subscriber = kit
        .system()
        .spawn(
            "region-discovery",
            ShardRegionDiscoverySubscriber::<String>::props(cluster, region),
        )
        .unwrap();

    wait_for_coordinator_registration(&kit, &coordinator_a, "coordinator-a-state", "region-a");

    publisher
        .tell(ClusterEventPublisherMsg::PublishChanges(
            Gossip::from_members([cluster_member(
                coordinator_node_b,
                MemberStatus::Up,
                ["backend"],
                2,
            )]),
        ))
        .unwrap();

    wait_for_coordinator_registration(&kit, &coordinator_b, "coordinator-b-state", "region-a");

    let subscriber_state = kit
        .create_probe::<ShardRegionDiscoverySubscriberSnapshot>("moved-subscriber-state")
        .unwrap();
    subscriber
        .tell(ShardRegionDiscoverySubscriberMsg::Snapshot {
            reply_to: subscriber_state.actor_ref(),
        })
        .unwrap();
    let state = subscriber_state
        .expect_msg(Duration::from_millis(500))
        .unwrap();
    assert!(state.subscribed);
    assert_eq!(state.forwarded_snapshots, 1);
    assert!(
        state.forwarded_events > 0,
        "removing the first coordinator should be forwarded as a cluster event"
    );
    assert_eq!(state.last_error, None);
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn multi_node_region_discovery_registers_and_routes_via_coordinator_node() {
    let nodes = kairo_testkit::MultiNodeTestKit::new([
        "sharding-discovery-coordinator",
        "sharding-discovery-region",
    ])
    .unwrap();
    let coordinator_kit = nodes.kit("sharding-discovery-coordinator").unwrap();
    let region_kit = nodes.kit("sharding-discovery-region").unwrap();
    let region_node = remote_unique_node("sharding-discovery-region", "127.0.0.1", 2680, 31);
    let coordinator_node =
        remote_unique_node("sharding-discovery-coordinator", "127.0.0.1", 2681, 32);
    let publisher = region_kit
        .system()
        .spawn(
            "cluster-events",
            Props::new(move || ClusterEventPublisher::new(region_node.clone())),
        )
        .unwrap();
    let cluster = Cluster::new(publisher.clone());
    let coordinator = coordinator_kit
        .system()
        .spawn(
            "coordinator",
            ShardCoordinatorActor::props_with_handoff(
                CoordinatorState::new(),
                LeastShardAllocationStrategy::default(),
                "stop".to_string(),
                Duration::from_millis(500),
                HandoffTransport::new(),
            ),
        )
        .unwrap();
    let discovery = RegionCoordinatorDiscoveryConfig::new(
        CoordinatorDiscoverySettings::default().with_required_role("backend"),
        Duration::from_millis(20),
    )
    .with_local_coordinator(coordinator_node.clone(), coordinator.clone());
    let region = region_kit
        .system()
        .spawn(
            "region-a",
            ShardRegionActor::<String>::props_with_local_shards_and_coordinator_discovery(
                "region-a", 10, 10, discovery,
            ),
        )
        .unwrap();
    let subscriber = region_kit
        .system()
        .spawn(
            "region-discovery",
            ShardRegionDiscoverySubscriber::<String>::props(cluster, region.clone()),
        )
        .unwrap();
    let coordinator_state = nodes
        .create_probe_on::<CoordinatorStateSnapshot>(
            "sharding-discovery-coordinator",
            "coordinator-state",
        )
        .unwrap();
    let region_state = nodes
        .create_probe_on::<ShardRegionSnapshot>("sharding-discovery-region", "region-state")
        .unwrap();
    let subscriber_state = nodes
        .create_probe_on::<ShardRegionDiscoverySubscriberSnapshot>(
            "sharding-discovery-region",
            "subscriber-state",
        )
        .unwrap();
    let routes = nodes
        .create_probe_on::<RegionLocalRoutePlan<String>>(
            "sharding-discovery-region",
            "region-routes",
        )
        .unwrap();
    let deliveries = nodes
        .create_probe_on::<ShardDeliverPlan<String>>("sharding-discovery-region", "deliveries")
        .unwrap();

    publisher
        .tell(ClusterEventPublisherMsg::PublishChanges(
            Gossip::from_members([cluster_member(
                coordinator_node,
                MemberStatus::Up,
                ["backend"],
                1,
            )]),
        ))
        .unwrap();

    let mut registered = false;
    for _ in 0..20 {
        coordinator
            .tell(ShardCoordinatorMsg::GetState {
                reply_to: coordinator_state.actor_ref(),
            })
            .unwrap();
        let state = coordinator_state
            .expect_msg(Duration::from_millis(500))
            .unwrap();
        registered = state.allocations.contains_key("region-a");
        if registered {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        registered,
        "multi-node region discovery should register with coordinator node"
    );

    region
        .tell(ShardRegionMsg::GetState {
            reply_to: region_state.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        region_state
            .expect_msg(Duration::from_millis(500))
            .unwrap()
            .registration_status,
        RegionRegistrationStatus::Registered
    );
    subscriber
        .tell(ShardRegionDiscoverySubscriberMsg::Snapshot {
            reply_to: subscriber_state.actor_ref(),
        })
        .unwrap();
    let state = subscriber_state
        .expect_msg(Duration::from_millis(500))
        .unwrap();
    assert!(state.subscribed);
    assert_eq!(state.forwarded_snapshots, 1);
    assert_eq!(state.last_error, None);

    region
        .tell(ShardRegionMsg::RouteToLocalShard {
            shard: "shard-1".to_string(),
            message: ShardingEnvelope::new("entity-1", "first".to_string()),
            route_reply_to: routes.actor_ref(),
            delivery_reply_to: deliveries.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        routes.expect_msg(Duration::from_millis(500)).unwrap(),
        RegionLocalRoutePlan::Buffered {
            shard: "shard-1".to_string(),
            request: Some(GetShardHome {
                shard_id: "shard-1".to_string(),
            }),
        }
    );
    assert_eq!(
        deliveries.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardDeliverPlan::StartEntity {
            delivery: EntityDelivery::new("entity-1", "first".to_string()),
        }
    );

    nodes.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn multi_node_region_discovery_allocates_remembered_shard_on_registration() {
    let nodes = kairo_testkit::MultiNodeTestKit::new([
        "sharding-remembered-discovery-coordinator",
        "sharding-remembered-discovery-region",
    ])
    .unwrap();
    let coordinator_kit = nodes
        .kit("sharding-remembered-discovery-coordinator")
        .unwrap();
    let region_kit = nodes.kit("sharding-remembered-discovery-region").unwrap();
    let region_node = remote_unique_node(
        "sharding-remembered-discovery-region",
        "127.0.0.1",
        2690,
        41,
    );
    let coordinator_node = remote_unique_node(
        "sharding-remembered-discovery-coordinator",
        "127.0.0.1",
        2691,
        42,
    );
    let publisher = region_kit
        .system()
        .spawn(
            "cluster-events",
            Props::new(move || ClusterEventPublisher::new(region_node.clone())),
        )
        .unwrap();
    let cluster = Cluster::new(publisher.clone());
    let mut state = CoordinatorState::new().with_remember_entities(true);
    state.merge_remembered_shards(["shard-1".to_string()]);
    let coordinator = coordinator_kit
        .system()
        .spawn(
            "coordinator",
            ShardCoordinatorActor::props_with_handoff(
                state,
                LeastShardAllocationStrategy::default(),
                "stop".to_string(),
                Duration::from_millis(500),
                HandoffTransport::new(),
            ),
        )
        .unwrap();
    let discovery = RegionCoordinatorDiscoveryConfig::new(
        CoordinatorDiscoverySettings::default().with_required_role("backend"),
        Duration::from_millis(20),
    )
    .with_local_coordinator(coordinator_node.clone(), coordinator.clone());
    let region = region_kit
        .system()
        .spawn(
            "region-a",
            ShardRegionActor::<String>::props_with_local_shards_and_coordinator_discovery(
                "region-a", 10, 10, discovery,
            ),
        )
        .unwrap();
    let _subscriber = region_kit
        .system()
        .spawn(
            "region-discovery",
            ShardRegionDiscoverySubscriber::<String>::props(cluster, region.clone()),
        )
        .unwrap();
    let coordinator_state = nodes
        .create_probe_on::<CoordinatorStateSnapshot>(
            "sharding-remembered-discovery-coordinator",
            "coordinator-state",
        )
        .unwrap();
    let region_state = nodes
        .create_probe_on::<ShardRegionSnapshot>(
            "sharding-remembered-discovery-region",
            "region-state",
        )
        .unwrap();

    publisher
        .tell(ClusterEventPublisherMsg::PublishChanges(
            Gossip::from_members([cluster_member(
                coordinator_node,
                MemberStatus::Up,
                ["backend"],
                1,
            )]),
        ))
        .unwrap();

    let mut allocated = false;
    for _ in 0..20 {
        coordinator
            .tell(ShardCoordinatorMsg::GetState {
                reply_to: coordinator_state.actor_ref(),
            })
            .unwrap();
        let snapshot = coordinator_state
            .expect_msg(Duration::from_millis(500))
            .unwrap();
        allocated = snapshot.unallocated_shards.is_empty()
            && snapshot
                .allocations
                .get("region-a")
                .is_some_and(|shards| shards.contains(&"shard-1".to_string()));
        if allocated {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        allocated,
        "remembered shard should be allocated after discovered region registers"
    );

    let mut hosted = false;
    for _ in 0..20 {
        region
            .tell(ShardRegionMsg::GetState {
                reply_to: region_state.actor_ref(),
            })
            .unwrap();
        hosted = region_state
            .expect_msg(Duration::from_millis(500))
            .unwrap()
            .local_shards
            .contains("shard-1");
        if hosted {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        hosted,
        "remembered shard should be hosted by the registered region node"
    );
    nodes.shutdown(Duration::from_secs(1)).unwrap();
}

fn wait_for_coordinator_registration(
    kit: &kairo_testkit::ActorSystemTestKit,
    coordinator: &ActorRef<ShardCoordinatorMsg<String>>,
    probe_name: &str,
    region: &str,
) {
    let state = kit
        .create_probe::<CoordinatorStateSnapshot>(probe_name)
        .unwrap();
    for _ in 0..20 {
        coordinator
            .tell(ShardCoordinatorMsg::GetState {
                reply_to: state.actor_ref(),
            })
            .unwrap();
        let snapshot = state.expect_msg(Duration::from_millis(500)).unwrap();
        if snapshot.allocations.contains_key(region) {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("timed out waiting for coordinator to register region `{region}`");
}
