use super::*;

#[test]
fn coordinator_bootstrap_builds_state_and_transport_from_local_regions() {
    let kit = kairo_testkit::ActorSystemTestKit::new("coordinator-bootstrap").unwrap();
    let region_a = kit
        .system()
        .spawn(
            "region-a",
            ShardRegionActor::<String>::props_with_local_shards("region-a", 10, 10),
        )
        .unwrap();
    let region_b = kit
        .system()
        .spawn(
            "region-b",
            ShardRegionActor::<String>::props_with_local_shards("region-b", 10, 10),
        )
        .unwrap();

    let bootstrap = ShardCoordinatorBootstrap::local_regions([
        HandoffRegionTarget::new("region-a", region_a.clone()),
        HandoffRegionTarget::new("region-b", region_b),
    ])
    .unwrap();

    assert_eq!(
        bootstrap.region_ids().cloned().collect::<Vec<_>>(),
        vec!["region-a".to_string(), "region-b".to_string()]
    );
    assert_eq!(bootstrap.handoff_transport().target_count(), 2);

    let duplicate = ShardCoordinatorBootstrap::local_regions([
        HandoffRegionTarget::new("region-a", region_a.clone()),
        HandoffRegionTarget::new("region-a", region_a),
    ]);
    assert!(matches!(
        duplicate,
        Err(ShardingError::RegionAlreadyRegistered(region)) if region == "region-a"
    ));
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn coordinator_actor_registers_local_regions_for_handoff_transport() {
    let kit = kairo_testkit::ActorSystemTestKit::new("coordinator-local-registration").unwrap();
    let region_a = kit
        .system()
        .spawn(
            "region-a",
            ShardRegionActor::<String>::props_with_local_remember_store_shards(
                "region-a",
                "orders",
                10,
                10,
                BTreeMap::from([(
                    "shard-1".to_string(),
                    BTreeSet::from(["entity-1".to_string()]),
                )]),
                Duration::from_millis(500),
            ),
        )
        .unwrap();
    let region_b = kit
        .system()
        .spawn(
            "region-b",
            ShardRegionActor::<String>::props_with_local_remember_store_shards(
                "region-b",
                "orders",
                10,
                10,
                BTreeMap::new(),
                Duration::from_millis(500),
            ),
        )
        .unwrap();
    let coordinator = kit
        .system()
        .spawn(
            "coordinator",
            ShardCoordinatorActor::props_with_handoff(
                CoordinatorState::new(),
                RebalanceThenAllocateStrategy::new(["shard-1"], "region-b"),
                "stop".to_string(),
                Duration::from_millis(500),
                HandoffTransport::new(),
            ),
        )
        .unwrap();
    let registered = kit
        .create_probe::<Result<CoordinatorStateSnapshot, ShardingError>>("registered")
        .unwrap();
    let host = kit.create_probe::<HostShardPlan<String>>("host").unwrap();
    let rebalance = kit
        .create_probe::<Result<RebalancePlan, ShardingError>>("rebalance")
        .unwrap();
    let snapshot = kit
        .create_probe::<CoordinatorStateSnapshot>("snapshot")
        .unwrap();
    let region_b_state = kit
        .create_probe::<ShardRegionSnapshot>("region-b-state")
        .unwrap();

    for (id, region) in [
        ("region-a", region_a.clone()),
        ("region-b", region_b.clone()),
    ] {
        coordinator
            .tell(ShardCoordinatorMsg::RegisterLocalRegion {
                target: HandoffRegionTarget::new(id, region),
                reply_to: registered.actor_ref(),
            })
            .unwrap();
        assert!(
            registered
                .expect_msg(Duration::from_millis(500))
                .unwrap()
                .unwrap()
                .allocations
                .contains_key(id)
        );
    }

    coordinator
        .tell(ShardCoordinatorMsg::RegisterLocalRegion {
            target: HandoffRegionTarget::new("region-a", region_a.clone()),
            reply_to: registered.actor_ref(),
        })
        .unwrap();
    assert!(
        registered
            .expect_msg(Duration::from_millis(500))
            .unwrap()
            .unwrap()
            .allocations
            .contains_key("region-a")
    );

    region_a
        .tell(ShardRegionMsg::HostShard {
            shard: "shard-1".to_string(),
            reply_to: host.actor_ref(),
        })
        .unwrap();
    host.expect_msg(Duration::from_millis(500)).unwrap();
    coordinator
        .tell(ShardCoordinatorMsg::ApplyEvent {
            event: CoordinatorEvent::ShardHomeAllocated {
                shard: "shard-1".to_string(),
                region: "region-a".to_string(),
            },
            reply_to: None,
        })
        .unwrap();

    coordinator
        .tell(ShardCoordinatorMsg::PlanRebalance {
            reply_to: rebalance.actor_ref(),
        })
        .unwrap();
    assert!(matches!(
        rebalance
            .expect_msg(Duration::from_millis(500))
            .unwrap()
            .unwrap(),
        RebalancePlan::Started { ref shards }
            if shards.len() == 1 && shards[0].shard == "shard-1"
    ));

    wait_for_coordinator_snapshot(
        &coordinator,
        &snapshot,
        "registered region targets should be available to handoff workers",
        shard_reallocated_to_region_b,
    );
    region_b
        .tell(ShardRegionMsg::GetState {
            reply_to: region_b_state.actor_ref(),
        })
        .unwrap();
    assert!(
        region_b_state
            .expect_msg(Duration::from_millis(500))
            .unwrap()
            .local_shards
            .contains("shard-1")
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn region_actor_self_registers_with_local_coordinator_for_handoff() {
    let kit = kairo_testkit::ActorSystemTestKit::new("region-self-registration").unwrap();
    let coordinator = kit
        .system()
        .spawn(
            "coordinator",
            ShardCoordinatorActor::props_with_handoff(
                CoordinatorState::new(),
                RebalanceThenAllocateStrategy::new(["shard-1"], "region-b"),
                "stop".to_string(),
                Duration::from_millis(500),
                HandoffTransport::new(),
            ),
        )
        .unwrap();
    let region_a = kit
        .system()
        .spawn(
            "region-a",
            ShardRegionActor::<String>::props_with_local_remember_store_shards_and_registration(
                "region-a",
                "orders",
                10,
                10,
                BTreeMap::from([(
                    "shard-1".to_string(),
                    BTreeSet::from(["entity-1".to_string()]),
                )]),
                Duration::from_millis(500),
                RegionRegistrationConfig::new(coordinator.clone(), Duration::from_millis(20)),
            ),
        )
        .unwrap();
    let region_b = kit
        .system()
        .spawn(
            "region-b",
            ShardRegionActor::<String>::props_with_local_remember_store_shards_and_registration(
                "region-b",
                "orders",
                10,
                10,
                BTreeMap::new(),
                Duration::from_millis(500),
                RegionRegistrationConfig::new(coordinator.clone(), Duration::from_millis(20)),
            ),
        )
        .unwrap();
    let coordinator_state = kit
        .create_probe::<CoordinatorStateSnapshot>("coordinator-state")
        .unwrap();
    let region_a_state = kit
        .create_probe::<ShardRegionSnapshot>("region-a-state")
        .unwrap();
    let host = kit.create_probe::<HostShardPlan<String>>("host").unwrap();
    let rebalance = kit
        .create_probe::<Result<RebalancePlan, ShardingError>>("rebalance")
        .unwrap();
    let region_b_state = kit
        .create_probe::<ShardRegionSnapshot>("region-b-state")
        .unwrap();

    wait_for_coordinator_snapshot(
        &coordinator,
        &coordinator_state,
        "regions should register themselves with coordinator",
        both_regions_registered,
    );

    region_a
        .tell(ShardRegionMsg::GetState {
            reply_to: region_a_state.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        region_a_state
            .expect_msg(Duration::from_millis(500))
            .unwrap()
            .registration_status,
        RegionRegistrationStatus::Registered
    );

    region_a
        .tell(ShardRegionMsg::HostShard {
            shard: "shard-1".to_string(),
            reply_to: host.actor_ref(),
        })
        .unwrap();
    host.expect_msg(Duration::from_millis(500)).unwrap();
    coordinator
        .tell(ShardCoordinatorMsg::ApplyEvent {
            event: CoordinatorEvent::ShardHomeAllocated {
                shard: "shard-1".to_string(),
                region: "region-a".to_string(),
            },
            reply_to: None,
        })
        .unwrap();

    coordinator
        .tell(ShardCoordinatorMsg::PlanRebalance {
            reply_to: rebalance.actor_ref(),
        })
        .unwrap();
    assert!(matches!(
        rebalance
            .expect_msg(Duration::from_millis(500))
            .unwrap()
            .unwrap(),
        RebalancePlan::Started { ref shards }
            if shards.len() == 1 && shards[0].shard == "shard-1"
    ));

    wait_for_coordinator_snapshot(
        &coordinator,
        &coordinator_state,
        "self-registered region targets should be available to handoff workers",
        shard_reallocated_to_region_b,
    );
    region_b
        .tell(ShardRegionMsg::GetState {
            reply_to: region_b_state.actor_ref(),
        })
        .unwrap();
    assert!(
        region_b_state
            .expect_msg(Duration::from_millis(500))
            .unwrap()
            .local_shards
            .contains("shard-1")
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn region_actor_self_registers_with_shared_remember_store_refs_for_rehost() {
    let kit =
        kairo_testkit::ActorSystemTestKit::new("region-shared-remember-registration").unwrap();
    let remember_store = kit
        .system()
        .spawn(
            "remember-shard-1",
            RememberShardStoreActor::props(RememberShardStoreState::with_entities(
                "orders",
                "shard-1",
                ["entity-1".to_string()],
            )),
        )
        .unwrap();
    let store_refs = BTreeMap::from([("shard-1".to_string(), remember_store)]);
    let coordinator = kit
        .system()
        .spawn(
            "coordinator",
            ShardCoordinatorActor::props_with_handoff(
                CoordinatorState::new(),
                RebalanceThenAllocateStrategy::new(["shard-1"], "region-b"),
                "stop".to_string(),
                Duration::from_millis(500),
                HandoffTransport::new(),
            ),
        )
        .unwrap();
    let region_a = kit
        .system()
        .spawn(
            "region-a",
            ShardRegionActor::<String>::props_with_remember_store_shards_and_registration(
                "region-a",
                10,
                10,
                store_refs.clone(),
                Duration::from_millis(500),
                RegionRegistrationConfig::new(coordinator.clone(), Duration::from_millis(20)),
            ),
        )
        .unwrap();
    let region_b = kit
        .system()
        .spawn(
            "region-b",
            ShardRegionActor::<String>::props_with_remember_store_shards_and_registration(
                "region-b",
                10,
                10,
                store_refs,
                Duration::from_millis(500),
                RegionRegistrationConfig::new(coordinator.clone(), Duration::from_millis(20)),
            ),
        )
        .unwrap();
    let coordinator_state = kit
        .create_probe::<CoordinatorStateSnapshot>("coordinator-state")
        .unwrap();
    let host = kit.create_probe::<HostShardPlan<String>>("host").unwrap();
    let rebalance = kit
        .create_probe::<Result<RebalancePlan, ShardingError>>("rebalance")
        .unwrap();
    let region_b_state = kit
        .create_probe::<ShardRegionSnapshot>("region-b-state")
        .unwrap();
    let local_shard = kit
        .create_probe::<Option<kairo_actor::ActorRef<ShardMsg<String>>>>("local-shard")
        .unwrap();
    let deliveries = kit
        .create_probe::<ShardDeliverPlan<String>>("deliveries")
        .unwrap();

    wait_for_coordinator_snapshot(
        &coordinator,
        &coordinator_state,
        "shared-store regions should self-register with coordinator",
        both_regions_registered,
    );

    region_a
        .tell(ShardRegionMsg::HostShard {
            shard: "shard-1".to_string(),
            reply_to: host.actor_ref(),
        })
        .unwrap();
    host.expect_msg(Duration::from_millis(500)).unwrap();
    coordinator
        .tell(ShardCoordinatorMsg::ApplyEvent {
            event: CoordinatorEvent::ShardHomeAllocated {
                shard: "shard-1".to_string(),
                region: "region-a".to_string(),
            },
            reply_to: None,
        })
        .unwrap();
    coordinator
        .tell(ShardCoordinatorMsg::PlanRebalance {
            reply_to: rebalance.actor_ref(),
        })
        .unwrap();
    assert!(matches!(
        rebalance
            .expect_msg(Duration::from_millis(500))
            .unwrap()
            .unwrap(),
        RebalancePlan::Started { ref shards }
            if shards.len() == 1 && shards[0].shard == "shard-1"
    ));

    wait_for_coordinator_snapshot(
        &coordinator,
        &coordinator_state,
        "handoff should reallocate remembered shard to region-b",
        shard_reallocated_to_region_b,
    );

    region_b
        .tell(ShardRegionMsg::GetState {
            reply_to: region_b_state.actor_ref(),
        })
        .unwrap();
    assert!(
        region_b_state
            .expect_msg(Duration::from_millis(500))
            .unwrap()
            .local_shards
            .contains("shard-1")
    );
    region_b
        .tell(ShardRegionMsg::GetLocalShard {
            shard: "shard-1".to_string(),
            reply_to: local_shard.actor_ref(),
        })
        .unwrap();
    let shard = local_shard
        .expect_msg(Duration::from_millis(500))
        .unwrap()
        .unwrap();
    shard
        .tell(ShardMsg::Deliver {
            message: ShardingEnvelope::new("entity-1", "after-rehost".to_string()),
            reply_to: deliveries.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        deliveries.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardDeliverPlan::Deliver {
            delivery: EntityDelivery::new("entity-1", "after-rehost".to_string()),
        }
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn region_actor_registers_with_discovered_local_coordinator() {
    let kit = kairo_testkit::ActorSystemTestKit::new("region-discovered-registration").unwrap();
    let coordinator_node = remote_node("region-discovered-registration", "127.0.0.1", 2651);
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
    .with_local_coordinator(coordinator_node.clone(), coordinator.clone());
    let region = kit
        .system()
        .spawn(
            "region-a",
            ShardRegionActor::<String>::props_with_local_shards_and_coordinator_discovery(
                "region-a", 10, 10, discovery,
            ),
        )
        .unwrap();
    let coordinator_state = kit
        .create_probe::<CoordinatorStateSnapshot>("discovered-coordinator-state")
        .unwrap();
    let region_state = kit
        .create_probe::<ShardRegionSnapshot>("discovered-region-state")
        .unwrap();

    region
        .tell(ShardRegionMsg::CoordinatorDiscoverySnapshot {
            state: cluster_state(vec![cluster_member(
                coordinator_node,
                MemberStatus::Up,
                ["backend"],
                1,
            )]),
        })
        .unwrap();

    wait_for_coordinator_snapshot(
        &coordinator,
        &coordinator_state,
        "region should register after coordinator discovery snapshot",
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
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

fn polling_timeout() -> Duration {
    Duration::from_millis(10_200)
}

fn both_regions_registered(snapshot: &CoordinatorStateSnapshot) -> bool {
    snapshot.allocations.contains_key("region-a") && snapshot.allocations.contains_key("region-b")
}

fn shard_reallocated_to_region_b(snapshot: &CoordinatorStateSnapshot) -> bool {
    snapshot
        .allocations
        .get("region-b")
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
