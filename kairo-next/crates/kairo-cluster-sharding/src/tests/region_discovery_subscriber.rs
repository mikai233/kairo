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
    wait_for_coordinator_registration(
        &kit,
        &coordinator,
        "subscription-coordinator-state",
        "region-a",
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
fn region_bootstrap_spawns_region_and_discovery_subscriber() {
    let kit = kairo_testkit::ActorSystemTestKit::new("region-bootstrap-discovery").unwrap();
    let self_node = remote_unique_node("region-bootstrap-discovery", "127.0.0.1", 2662, 13);
    let coordinator_node = remote_unique_node("region-bootstrap-discovery", "127.0.0.1", 2663, 14);
    let publisher = kit
        .system()
        .spawn(
            "cluster-events",
            Props::new(move || ClusterEventPublisher::new(self_node.clone())),
        )
        .unwrap();
    let cluster = Cluster::new(publisher.clone());
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
    let discovery = RegionCoordinatorDiscoveryConfig::new(
        CoordinatorDiscoverySettings::default().with_required_role("backend"),
        Duration::from_millis(20),
    )
    .with_local_coordinator(coordinator_node, coordinator.clone());

    let bootstrap = ShardRegionBootstrap::spawn_local_shards_with_coordinator_discovery(
        kit.system(),
        ShardRegionBootstrapConfig::new(
            "region-a",
            "region-a-discovery",
            cluster,
            "region-a",
            10,
            10,
            discovery,
        ),
    )
    .unwrap();

    wait_for_coordinator_registration(
        &kit,
        &coordinator,
        "bootstrap-coordinator-state",
        "region-a",
    );
    let subscriber_state = kit
        .create_probe::<ShardRegionDiscoverySubscriberSnapshot>("bootstrap-subscriber-state")
        .unwrap();
    bootstrap
        .discovery_subscriber()
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

    let region_state = kit
        .create_probe::<ShardRegionSnapshot>("bootstrap-region-state")
        .unwrap();
    bootstrap
        .region()
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
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn region_bootstrap_stops_region_when_discovery_subscriber_spawn_fails() {
    let kit = kairo_testkit::ActorSystemTestKit::new("region-bootstrap-cleanup").unwrap();
    let self_node = remote_unique_node("region-bootstrap-cleanup", "127.0.0.1", 2664, 15);
    let publisher = kit
        .system()
        .spawn(
            "cluster-events",
            Props::new(move || ClusterEventPublisher::new(self_node.clone())),
        )
        .unwrap();
    let cluster = Cluster::new(publisher);
    let _occupied_subscriber_name = kit
        .system()
        .spawn(
            "region-a-discovery",
            ShardRegionActor::<String>::props_with_local_shards("occupied", 10, 10),
        )
        .unwrap();
    let discovery = RegionCoordinatorDiscoveryConfig::<String>::new(
        CoordinatorDiscoverySettings::default().with_required_role("backend"),
        Duration::from_millis(20),
    );

    let error = match ShardRegionBootstrap::spawn_local_shards_with_coordinator_discovery(
        kit.system(),
        ShardRegionBootstrapConfig::new(
            "region-a",
            "region-a-discovery",
            cluster,
            "region-a",
            10,
            10,
            discovery,
        ),
    ) {
        Ok(_) => panic!("duplicate subscriber name should fail region bootstrap"),
        Err(error) => error,
    };

    assert!(
        matches!(error, ActorError::DuplicateName(ref name) if name == "region-a-discovery"),
        "unexpected bootstrap failure: {error:?}"
    );
    let replacement = kairo_testkit::await_assert(
        polling_timeout(),
        Duration::from_millis(10),
        || -> Result<ActorRef<ShardRegionMsg<String>>, String> {
            kit.system()
                .spawn(
                    "region-a",
                    ShardRegionActor::<String>::props_with_local_shards("region-a", 10, 10),
                )
                .map_err(|error| format!("region name is not reusable yet: {error}"))
        },
    )
    .unwrap();
    kit.system().stop(&replacement);
    assert!(replacement.wait_for_stop(Duration::from_secs(1)));
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
fn region_discovery_reissues_buffered_remembered_home_after_coordinator_moves() {
    let kit =
        kairo_testkit::ActorSystemTestKit::new("region-discovery-moves-buffered-home").unwrap();
    let self_node = remote_unique_node(
        "region-discovery-moves-buffered-home",
        "127.0.0.1",
        2673,
        24,
    );
    let coordinator_node_a = remote_unique_node(
        "region-discovery-moves-buffered-home",
        "127.0.0.1",
        2674,
        25,
    );
    let coordinator_node_b = remote_unique_node(
        "region-discovery-moves-buffered-home",
        "127.0.0.1",
        2675,
        26,
    );
    let publisher = kit
        .system()
        .spawn(
            "cluster-events",
            Props::new(move || ClusterEventPublisher::new(self_node.clone())),
        )
        .unwrap();
    let cluster = Cluster::new(publisher.clone());
    let shard_store = kit
        .system()
        .spawn(
            "remember-store",
            RememberShardStoreActor::props(RememberShardStoreState::new("orders", "shard-1")),
        )
        .unwrap();
    let (registration_tx_a, registration_rx_a) = mpsc::channel();
    let (request_tx_a, request_rx_a) = mpsc::channel();
    let coordinator_a = kit
        .system()
        .spawn(
            "coordinator-a",
            Props::new(move || CoordinatorMoveProbeCoordinator {
                registration_tx: registration_tx_a,
                request_tx: request_tx_a,
                ack_registration: false,
            }),
        )
        .unwrap();
    let (registration_tx_b, registration_rx_b) = mpsc::channel();
    let (request_tx_b, request_rx_b) = mpsc::channel();
    let coordinator_b = kit
        .system()
        .spawn(
            "coordinator-b",
            Props::new(move || CoordinatorMoveProbeCoordinator {
                registration_tx: registration_tx_b,
                request_tx: request_tx_b,
                ack_registration: true,
            }),
        )
        .unwrap();

    publisher
        .tell(ClusterEventPublisherMsg::PublishChanges(
            Gossip::from_members([cluster_member(
                coordinator_node_a.clone(),
                MemberStatus::Up,
                ["backend"],
                1,
            )]),
        ))
        .unwrap();

    let discovery = RegionCoordinatorDiscoveryConfig::new(
        CoordinatorDiscoverySettings::default().with_required_role("backend"),
        Duration::from_millis(20),
    )
    .with_local_coordinator(coordinator_node_a.clone(), coordinator_a)
    .with_local_coordinator(coordinator_node_b.clone(), coordinator_b);
    let region_store = shard_store.clone();
    let region = kit
        .system()
        .spawn(
            "region-a",
            Props::new(move || {
                ShardRegionActor::<String>::new_with_remember_store_shards(
                    "region-a",
                    10,
                    10,
                    BTreeMap::from([("shard-1".to_string(), region_store.clone())]),
                    Duration::from_millis(500),
                )
                .with_coordinator_discovery(discovery.clone())
            }),
        )
        .unwrap();
    let _subscriber = kit
        .system()
        .spawn(
            "region-discovery",
            ShardRegionDiscoverySubscriber::<String>::props(cluster, region.clone()),
        )
        .unwrap();
    let routes = kit
        .create_probe::<RegionLocalRoutePlan<String>>("routes")
        .unwrap();
    let delivery = kit
        .create_probe::<ShardDeliverPlan<String>>("delivery")
        .unwrap();
    let local_shard = kit
        .create_probe::<Option<ActorRef<ShardMsg<String>>>>("local-shard")
        .unwrap();
    let shard_state = kit.create_probe::<ShardSnapshot>("shard-state").unwrap();
    let store_state = kit
        .create_probe::<RememberShardStoreSnapshot>("store-state")
        .unwrap();

    assert_eq!(
        registration_rx_a
            .recv_timeout(Duration::from_millis(500))
            .unwrap(),
        "region-a"
    );

    region
        .tell(ShardRegionMsg::RouteToLocalShard {
            shard: "shard-1".to_string(),
            message: ShardingEnvelope::new("entity-1", "first".to_string()),
            route_reply_to: routes.actor_ref(),
            delivery_reply_to: delivery.actor_ref(),
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
    assert_eq!(delivery.expect_no_msg(Duration::from_millis(50)), Ok(()));
    assert!(
        request_rx_a
            .recv_timeout(Duration::from_millis(50))
            .is_err(),
        "unacknowledged old coordinator must not receive shard-home requests"
    );

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

    assert_eq!(
        registration_rx_b
            .recv_timeout(Duration::from_millis(500))
            .unwrap(),
        "region-a"
    );
    assert_eq!(
        request_rx_b
            .recv_timeout(Duration::from_millis(500))
            .unwrap(),
        "shard-1"
    );
    assert_eq!(
        delivery.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardDeliverPlan::RememberUpdate {
            update: RememberShardUpdate::new(
                ["entity-1".to_string()],
                std::iter::empty::<String>(),
            ),
        }
    );

    kairo_testkit::await_assert(
        polling_timeout(),
        Duration::from_millis(10),
        || -> Result<(), String> {
            shard_store
                .tell(RememberShardStoreMsg::GetState {
                    reply_to: store_state.actor_ref(),
                })
                .map_err(|error| error.to_string())?;
            let remembered_entities = store_state
                .expect_msg(Duration::from_millis(500))
                .map_err(|error| error.to_string())?
                .entities_by_key
                .values()
                .flat_map(|entities| entities.iter().cloned())
                .collect::<BTreeSet<_>>();

            region
                .tell(ShardRegionMsg::GetLocalShard {
                    shard: "shard-1".to_string(),
                    reply_to: local_shard.actor_ref(),
                })
                .map_err(|error| error.to_string())?;
            let Some(shard) = local_shard
                .expect_msg(Duration::from_millis(500))
                .map_err(|error| error.to_string())?
            else {
                return Err(format!(
                    "buffered remembered delivery should be persisted and activated after coordinator move; remembered entities: {remembered_entities:?}; local shard unavailable"
                ));
            };

            shard
                .tell(ShardMsg::GetState {
                    reply_to: shard_state.actor_ref(),
                })
                .map_err(|error| error.to_string())?;
            let snapshot = shard_state
                .expect_msg(Duration::from_millis(500))
                .map_err(|error| error.to_string())?;
            if remembered_entities == BTreeSet::from(["entity-1".to_string()])
                && snapshot.active_entities == vec!["entity-1".to_string()]
                && snapshot.total_buffered == 0
            {
                Ok(())
            } else {
                Err(format!(
                    "buffered remembered delivery should be persisted and activated after coordinator move; remembered entities: {remembered_entities:?}; shard snapshot: {snapshot:?}"
                ))
            }
        },
    )
    .unwrap();

    region
        .tell(ShardRegionMsg::RouteToLocalShard {
            shard: "shard-1".to_string(),
            message: ShardingEnvelope::new("entity-1", "after-coordinator-move".to_string()),
            route_reply_to: routes.actor_ref(),
            delivery_reply_to: delivery.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        routes.expect_msg(Duration::from_millis(500)).unwrap(),
        RegionLocalRoutePlan::DeliveredToLocalShard {
            shard: "shard-1".to_string(),
        }
    );
    assert_eq!(
        delivery.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardDeliverPlan::Deliver {
            delivery: EntityDelivery::new("entity-1", "after-coordinator-move".to_string()),
        }
    );
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

    wait_for_coordinator_snapshot(
        &coordinator,
        &coordinator_state,
        "multi-node region discovery should register with coordinator node",
        |snapshot| snapshot.allocations.contains_key("region-a"),
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
    let remembered_entities_by_shard = BTreeMap::from([(
        "shard-1".to_string(),
        BTreeSet::from(["entity-1".to_string()]),
    )]);
    let region = region_kit
        .system()
        .spawn(
            "region-a",
            Props::new(move || {
                ShardRegionActor::<String>::new_with_local_remember_store_shards(
                    "region-a",
                    "orders",
                    10,
                    10,
                    remembered_entities_by_shard.clone(),
                    Duration::from_millis(500),
                )
                .with_coordinator_discovery(discovery.clone())
            }),
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
    let routes = nodes
        .create_probe_on::<RegionLocalRoutePlan<String>>(
            "sharding-remembered-discovery-region",
            "remembered-routes",
        )
        .unwrap();
    let deliveries = nodes
        .create_probe_on::<ShardDeliverPlan<String>>(
            "sharding-remembered-discovery-region",
            "deliveries",
        )
        .unwrap();
    let local_shard = nodes
        .create_probe_on::<Option<ActorRef<ShardMsg<String>>>>(
            "sharding-remembered-discovery-region",
            "local-shard",
        )
        .unwrap();
    let shard_state = nodes
        .create_probe_on::<ShardSnapshot>("sharding-remembered-discovery-region", "shard-state")
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

    wait_for_coordinator_snapshot(
        &coordinator,
        &coordinator_state,
        "remembered shard should be allocated after discovered region registers",
        remembered_shard_allocated,
    );

    wait_for_region_snapshot(
        &region,
        &region_state,
        "remembered shard should be hosted by the registered region node",
        |snapshot| snapshot.local_shards.contains("shard-1"),
    );

    region
        .tell(ShardRegionMsg::RouteToLocalShard {
            shard: "shard-1".to_string(),
            message: ShardingEnvelope::new("entity-1", "after-discovery".to_string()),
            route_reply_to: routes.actor_ref(),
            delivery_reply_to: deliveries.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        routes.expect_msg(Duration::from_millis(500)).unwrap(),
        RegionLocalRoutePlan::DeliveredToLocalShard {
            shard: "shard-1".to_string()
        }
    );
    assert_eq!(
        deliveries.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardDeliverPlan::Deliver {
            delivery: EntityDelivery::new("entity-1", "after-discovery".to_string()),
        }
    );

    region
        .tell(ShardRegionMsg::GetLocalShard {
            shard: "shard-1".to_string(),
            reply_to: local_shard.actor_ref(),
        })
        .unwrap();
    let shard = local_shard
        .expect_msg(Duration::from_millis(500))
        .unwrap()
        .expect("remembered shard should be available for state inspection");

    region
        .tell(ShardRegionMsg::RouteToLocalShard {
            shard: "shard-1".to_string(),
            message: ShardingEnvelope::new("entity-2", "first-new".to_string()),
            route_reply_to: routes.actor_ref(),
            delivery_reply_to: deliveries.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        routes.expect_msg(Duration::from_millis(500)).unwrap(),
        RegionLocalRoutePlan::DeliveredToLocalShard {
            shard: "shard-1".to_string()
        }
    );
    assert_eq!(
        deliveries.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardDeliverPlan::RememberUpdate {
            update: RememberShardUpdate::new(
                ["entity-2".to_string()],
                std::iter::empty::<String>(),
            ),
        }
    );

    wait_for_active_entity(
        &shard,
        &shard_state,
        "entity-2",
        "remember-start update should activate the newly delivered entity",
    );

    region
        .tell(ShardRegionMsg::RouteToLocalShard {
            shard: "shard-1".to_string(),
            message: ShardingEnvelope::new("entity-2", "after-remember-start".to_string()),
            route_reply_to: routes.actor_ref(),
            delivery_reply_to: deliveries.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        routes.expect_msg(Duration::from_millis(500)).unwrap(),
        RegionLocalRoutePlan::DeliveredToLocalShard {
            shard: "shard-1".to_string()
        }
    );
    assert_eq!(
        deliveries.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardDeliverPlan::Deliver {
            delivery: EntityDelivery::new("entity-2", "after-remember-start".to_string()),
        }
    );
    nodes.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn multi_node_region_discovery_delivers_recovered_and_new_shared_store_entities() {
    let nodes = kairo_testkit::MultiNodeTestKit::new([
        "sharding-discovery-shared-store",
        "sharding-discovery-shared-coordinator",
        "sharding-discovery-shared-region",
    ])
    .unwrap();
    let store_kit = nodes.kit("sharding-discovery-shared-store").unwrap();
    let coordinator_kit = nodes.kit("sharding-discovery-shared-coordinator").unwrap();
    let region_kit = nodes.kit("sharding-discovery-shared-region").unwrap();
    let region_node = remote_unique_node("sharding-discovery-shared-region", "127.0.0.1", 2692, 43);
    let coordinator_node = remote_unique_node(
        "sharding-discovery-shared-coordinator",
        "127.0.0.1",
        2693,
        44,
    );
    let remember_store = store_kit
        .system()
        .spawn(
            "remember-store-shard-1",
            RememberShardStoreActor::props(RememberShardStoreState::with_entities(
                "orders",
                "shard-1",
                ["entity-1".to_string()],
            )),
        )
        .unwrap();
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
    let region_remember_store = remember_store.clone();
    let region = region_kit
        .system()
        .spawn(
            "region-a",
            Props::new(move || {
                ShardRegionActor::<String>::new_with_remember_store_shards(
                    "region-a",
                    10,
                    10,
                    BTreeMap::from([("shard-1".to_string(), region_remember_store.clone())]),
                    Duration::from_millis(500),
                )
                .with_coordinator_discovery(discovery.clone())
            }),
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
            "sharding-discovery-shared-coordinator",
            "coordinator-state",
        )
        .unwrap();
    let routes = nodes
        .create_probe_on::<RegionLocalRoutePlan<String>>(
            "sharding-discovery-shared-region",
            "routes",
        )
        .unwrap();
    let deliveries = nodes
        .create_probe_on::<ShardDeliverPlan<String>>(
            "sharding-discovery-shared-region",
            "deliveries",
        )
        .unwrap();
    let store_state = nodes
        .create_probe_on::<RememberShardStoreSnapshot>(
            "sharding-discovery-shared-store",
            "store-state",
        )
        .unwrap();
    let local_shard = nodes
        .create_probe_on::<Option<ActorRef<ShardMsg<String>>>>(
            "sharding-discovery-shared-region",
            "local-shard",
        )
        .unwrap();
    let shard_state = nodes
        .create_probe_on::<ShardSnapshot>("sharding-discovery-shared-region", "shard-state")
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

    wait_for_coordinator_snapshot(
        &coordinator,
        &coordinator_state,
        "shared-store remembered shard should be allocated after discovery registration",
        remembered_shard_allocated,
    );

    region
        .tell(ShardRegionMsg::RouteToLocalShard {
            shard: "shard-1".to_string(),
            message: ShardingEnvelope::new("entity-1", "recovered-first".to_string()),
            route_reply_to: routes.actor_ref(),
            delivery_reply_to: deliveries.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        routes.expect_msg(Duration::from_millis(500)).unwrap(),
        RegionLocalRoutePlan::DeliveredToLocalShard {
            shard: "shard-1".to_string()
        }
    );
    assert_eq!(
        deliveries.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardDeliverPlan::Deliver {
            delivery: EntityDelivery::new("entity-1", "recovered-first".to_string()),
        }
    );

    region
        .tell(ShardRegionMsg::GetLocalShard {
            shard: "shard-1".to_string(),
            reply_to: local_shard.actor_ref(),
        })
        .unwrap();
    let shard = local_shard
        .expect_msg(Duration::from_millis(500))
        .unwrap()
        .expect("shared-store remembered shard should stay local after allocation");

    wait_for_active_entity(
        &shard,
        &shard_state,
        "entity-1",
        "shared remember-store recovery should activate the remembered entity before first delivery",
    );

    region
        .tell(ShardRegionMsg::RouteToLocalShard {
            shard: "shard-1".to_string(),
            message: ShardingEnvelope::new("entity-2", "first".to_string()),
            route_reply_to: routes.actor_ref(),
            delivery_reply_to: deliveries.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        routes.expect_msg(Duration::from_millis(500)).unwrap(),
        RegionLocalRoutePlan::DeliveredToLocalShard {
            shard: "shard-1".to_string()
        }
    );
    assert_eq!(
        deliveries.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardDeliverPlan::RememberUpdate {
            update: RememberShardUpdate::new(
                ["entity-2".to_string()],
                std::iter::empty::<String>(),
            ),
        }
    );

    let remembered = wait_for_remembered_entities(
        &remember_store,
        &store_state,
        "shared remember-store should persist both delivered entities",
        |entities| entities.contains("entity-1") && entities.contains("entity-2"),
    );
    assert_eq!(
        remembered,
        BTreeSet::from(["entity-1".to_string(), "entity-2".to_string()])
    );

    wait_for_active_entity(
        &shard,
        &shard_state,
        "entity-2",
        "shared remember-store write should activate the first-delivered entity",
    );

    region
        .tell(ShardRegionMsg::RouteToLocalShard {
            shard: "shard-1".to_string(),
            message: ShardingEnvelope::new("entity-2", "after-remember-start".to_string()),
            route_reply_to: routes.actor_ref(),
            delivery_reply_to: deliveries.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        routes.expect_msg(Duration::from_millis(500)).unwrap(),
        RegionLocalRoutePlan::DeliveredToLocalShard {
            shard: "shard-1".to_string()
        }
    );
    assert_eq!(
        deliveries.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardDeliverPlan::Deliver {
            delivery: EntityDelivery::new("entity-2", "after-remember-start".to_string()),
        }
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
    wait_for_coordinator_snapshot(
        coordinator,
        &state,
        &format!("timed out waiting for coordinator to register region `{region}`"),
        |snapshot| snapshot.allocations.contains_key(region),
    );
}

fn polling_timeout() -> Duration {
    Duration::from_millis(10_200)
}

fn remembered_shard_allocated(snapshot: &CoordinatorStateSnapshot) -> bool {
    snapshot.unallocated_shards.is_empty()
        && snapshot
            .allocations
            .get("region-a")
            .is_some_and(|shards| shards.contains(&"shard-1".to_string()))
}

fn wait_for_coordinator_snapshot(
    coordinator: &ActorRef<ShardCoordinatorMsg<String>>,
    state: &kairo_testkit::TestProbe<CoordinatorStateSnapshot>,
    description: &str,
    mut matches: impl FnMut(&CoordinatorStateSnapshot) -> bool,
) -> CoordinatorStateSnapshot {
    kairo_testkit::await_assert(
        polling_timeout(),
        Duration::from_millis(10),
        || -> Result<CoordinatorStateSnapshot, String> {
            coordinator
                .tell(ShardCoordinatorMsg::GetState {
                    reply_to: state.actor_ref(),
                })
                .map_err(|error| error.to_string())?;
            let snapshot = state
                .expect_msg(Duration::from_millis(500))
                .map_err(|error| error.to_string())?;
            if matches(&snapshot) {
                Ok(snapshot)
            } else {
                Err(format!("{description}; last snapshot: {snapshot:?}"))
            }
        },
    )
    .unwrap()
}

fn wait_for_region_snapshot(
    region: &ActorRef<ShardRegionMsg<String>>,
    state: &kairo_testkit::TestProbe<ShardRegionSnapshot>,
    description: &str,
    mut matches: impl FnMut(&ShardRegionSnapshot) -> bool,
) -> ShardRegionSnapshot {
    kairo_testkit::await_assert(
        polling_timeout(),
        Duration::from_millis(10),
        || -> Result<ShardRegionSnapshot, String> {
            region
                .tell(ShardRegionMsg::GetState {
                    reply_to: state.actor_ref(),
                })
                .map_err(|error| error.to_string())?;
            let snapshot = state
                .expect_msg(Duration::from_millis(500))
                .map_err(|error| error.to_string())?;
            if matches(&snapshot) {
                Ok(snapshot)
            } else {
                Err(format!("{description}; last snapshot: {snapshot:?}"))
            }
        },
    )
    .unwrap()
}

fn wait_for_active_entity(
    shard: &ActorRef<ShardMsg<String>>,
    state: &kairo_testkit::TestProbe<ShardSnapshot>,
    entity: &str,
    description: &str,
) -> ShardSnapshot {
    kairo_testkit::await_assert(
        polling_timeout(),
        Duration::from_millis(10),
        || -> Result<ShardSnapshot, String> {
            shard
                .tell(ShardMsg::GetState {
                    reply_to: state.actor_ref(),
                })
                .map_err(|error| error.to_string())?;
            let snapshot = state
                .expect_msg(Duration::from_millis(500))
                .map_err(|error| error.to_string())?;
            if snapshot.active_entities.contains(&entity.to_string()) {
                Ok(snapshot)
            } else {
                Err(format!("{description}; last snapshot: {snapshot:?}"))
            }
        },
    )
    .unwrap()
}

fn wait_for_remembered_entities(
    remember_store: &ActorRef<RememberShardStoreMsg>,
    state: &kairo_testkit::TestProbe<RememberShardStoreSnapshot>,
    description: &str,
    mut matches: impl FnMut(&BTreeSet<String>) -> bool,
) -> BTreeSet<String> {
    kairo_testkit::await_assert(
        polling_timeout(),
        Duration::from_millis(10),
        || -> Result<BTreeSet<String>, String> {
            remember_store
                .tell(RememberShardStoreMsg::GetState {
                    reply_to: state.actor_ref(),
                })
                .map_err(|error| error.to_string())?;
            let snapshot = state
                .expect_msg(Duration::from_millis(500))
                .map_err(|error| error.to_string())?;
            let entities = snapshot
                .entities_by_key
                .values()
                .flat_map(|entities| entities.iter().cloned())
                .collect::<BTreeSet<_>>();
            if matches(&entities) {
                Ok(entities)
            } else {
                Err(format!("{description}; remembered entities: {entities:?}"))
            }
        },
    )
    .unwrap()
}

struct CoordinatorMoveProbeCoordinator {
    registration_tx: mpsc::Sender<String>,
    request_tx: mpsc::Sender<String>,
    ack_registration: bool,
}

impl Actor for CoordinatorMoveProbeCoordinator {
    type Msg = ShardCoordinatorMsg<String>;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            ShardCoordinatorMsg::RegisterLocalRegion { target, reply_to } => {
                let region = target.region().clone();
                self.registration_tx
                    .send(region.clone())
                    .map_err(|error| ActorError::Message(error.to_string()))?;
                if self.ack_registration {
                    let _ = reply_to.tell(Ok(CoordinatorStateSnapshot {
                        allocations: BTreeMap::from([(region, Vec::new())]),
                        proxies: BTreeSet::new(),
                        unallocated_shards: BTreeSet::new(),
                        rebalance_in_progress: BTreeMap::new(),
                        remember_entities: true,
                    }));
                }
            }
            ShardCoordinatorMsg::RequestShardHome {
                requester,
                shard,
                reply_to,
            } => {
                self.request_tx
                    .send(shard.clone())
                    .map_err(|error| ActorError::Message(error.to_string()))?;
                let _ = reply_to.tell(Ok(GetShardHomePlan::Allocated {
                    event: CoordinatorEvent::ShardHomeAllocated {
                        shard: shard.clone(),
                        region: requester.clone(),
                    },
                    host_region: requester,
                    host_shard: HostShard { shard_id: shard },
                }));
            }
            _ => {}
        }
        Ok(())
    }
}
