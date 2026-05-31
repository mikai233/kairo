use super::*;

#[test]
fn handoff_worker_completes_store_backed_region_shard_handoff() {
    let kit = kairo_testkit::ActorSystemTestKit::new("handoff-worker-store-backed-region").unwrap();
    let region = kit
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
    let host = kit.create_probe::<HostShardPlan<String>>("host").unwrap();
    let done = kit.create_probe::<HandoffWorkerDone>("done").unwrap();
    let state = kit.create_probe::<ShardRegionSnapshot>("state").unwrap();

    region
        .tell(ShardRegionMsg::HostShard {
            shard: "shard-1".to_string(),
            reply_to: host.actor_ref(),
        })
        .unwrap();
    host.expect_msg(Duration::from_millis(500)).unwrap();

    let plan = ShardRebalancePlan {
        shard: "shard-1".to_string(),
        from_region: "region-a".to_string(),
        participants: BTreeSet::from(["region-a".to_string()]),
        begin_handoff: crate::BeginHandOff {
            shard_id: "shard-1".to_string(),
        },
    };
    let mut transport = HandoffTransport::new();
    transport.insert_target(HandoffRegionTarget::new("region-a", region.clone()));
    let worker = kit
        .system()
        .spawn(
            "handoff-worker",
            HandoffWorkerActor::props(
                plan,
                "stop".to_string(),
                Duration::from_millis(500),
                transport,
            ),
        )
        .unwrap();

    worker
        .tell(HandoffWorkerMsg::Start {
            reply_to: done.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        done.expect_msg(Duration::from_millis(500)).unwrap(),
        HandoffWorkerDone {
            shard: "shard-1".to_string(),
            ok: true,
        }
    );

    region
        .tell(ShardRegionMsg::GetState {
            reply_to: state.actor_ref(),
        })
        .unwrap();
    let snapshot = state.expect_msg(Duration::from_millis(500)).unwrap();
    assert!(!snapshot.local_shards.contains("shard-1"));
    assert!(!snapshot.handing_off_shards.contains("shard-1"));
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn coordinator_actor_spawns_worker_and_observes_handoff_completion() {
    let kit = kairo_testkit::ActorSystemTestKit::new("coordinator-handoff-worker").unwrap();
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
    let host = kit.create_probe::<HostShardPlan<String>>("host").unwrap();
    region_a
        .tell(ShardRegionMsg::HostShard {
            shard: "shard-1".to_string(),
            reply_to: host.actor_ref(),
        })
        .unwrap();
    host.expect_msg(Duration::from_millis(500)).unwrap();

    let bootstrap = ShardCoordinatorBootstrap::local_regions([
        HandoffRegionTarget::new("region-a", region_a.clone()),
        HandoffRegionTarget::new("region-b", region_b.clone()),
    ])
    .unwrap();
    let (mut state, transport) = bootstrap.into_parts();
    state
        .apply(CoordinatorEvent::ShardHomeAllocated {
            shard: "shard-1".to_string(),
            region: "region-a".to_string(),
        })
        .unwrap();

    let coordinator = kit
        .system()
        .spawn(
            "coordinator",
            ShardCoordinatorActor::props_with_handoff(
                state,
                RebalanceThenAllocateStrategy::new(["shard-1"], "region-b"),
                "stop".to_string(),
                Duration::from_millis(500),
                transport,
            ),
        )
        .unwrap();
    let rebalance = kit
        .create_probe::<Result<RebalancePlan, ShardingError>>("rebalance")
        .unwrap();
    let snapshot = kit
        .create_probe::<CoordinatorStateSnapshot>("snapshot")
        .unwrap();
    let region_b_state = kit
        .create_probe::<ShardRegionSnapshot>("region-b-state")
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

    let mut completed = false;
    for _ in 0..20 {
        coordinator
            .tell(ShardCoordinatorMsg::GetState {
                reply_to: snapshot.actor_ref(),
            })
            .unwrap();
        let state = snapshot.expect_msg(Duration::from_millis(500)).unwrap();
        completed = !state.rebalance_in_progress.contains_key("shard-1")
            && state
                .allocations
                .get("region-b")
                .is_some_and(|shards| shards.contains(&"shard-1".to_string()));
        if completed {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        completed,
        "coordinator should clear rebalance and reallocate shard after worker completion"
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
            .contains("shard-1"),
        "new owner region should receive HostShard after reallocation"
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn coordinator_actor_graceful_shutdown_rebalances_region_shards() {
    let kit = kairo_testkit::ActorSystemTestKit::new("coordinator-graceful-shutdown").unwrap();
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
    let host = kit.create_probe::<HostShardPlan<String>>("host").unwrap();
    region_a
        .tell(ShardRegionMsg::HostShard {
            shard: "shard-1".to_string(),
            reply_to: host.actor_ref(),
        })
        .unwrap();
    host.expect_msg(Duration::from_millis(500)).unwrap();

    let bootstrap = ShardCoordinatorBootstrap::local_regions([
        HandoffRegionTarget::new("region-a", region_a.clone()),
        HandoffRegionTarget::new("region-b", region_b.clone()),
    ])
    .unwrap();
    let (mut state, transport) = bootstrap.into_parts();
    state
        .apply(CoordinatorEvent::ShardHomeAllocated {
            shard: "shard-1".to_string(),
            region: "region-a".to_string(),
        })
        .unwrap();
    let coordinator = kit
        .system()
        .spawn(
            "coordinator",
            ShardCoordinatorActor::props_with_handoff(
                state,
                RebalanceThenAllocateStrategy::new(["shard-1"], "region-b"),
                "stop".to_string(),
                Duration::from_millis(500),
                transport,
            ),
        )
        .unwrap();
    let shutdown = kit.create_probe::<RegionShutdownPlan>("shutdown").unwrap();
    let snapshot = kit
        .create_probe::<CoordinatorStateSnapshot>("snapshot")
        .unwrap();
    let region_b_state = kit
        .create_probe::<ShardRegionSnapshot>("region-b-state")
        .unwrap();

    coordinator
        .tell(ShardCoordinatorMsg::GracefulShutdownReq {
            region: "region-a".to_string(),
            reply_to: Some(shutdown.actor_ref()),
        })
        .unwrap();
    assert!(matches!(
        shutdown.expect_msg(Duration::from_millis(500)).unwrap(),
        RegionShutdownPlan::Started { region, ref shards }
            if region == "region-a" && shards.len() == 1 && shards[0].shard == "shard-1"
    ));

    let mut completed = false;
    for _ in 0..20 {
        coordinator
            .tell(ShardCoordinatorMsg::GetState {
                reply_to: snapshot.actor_ref(),
            })
            .unwrap();
        let state = snapshot.expect_msg(Duration::from_millis(500)).unwrap();
        completed = !state.rebalance_in_progress.contains_key("shard-1")
            && !state
                .allocations
                .get("region-a")
                .is_some_and(|shards| shards.contains(&"shard-1".to_string()))
            && state
                .allocations
                .get("region-b")
                .is_some_and(|shards| shards.contains(&"shard-1".to_string()));
        if completed {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        completed,
        "graceful shutdown should hand off and reallocate region-a shard"
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
fn region_actor_graceful_shutdown_notifies_registered_coordinator() {
    let kit = kairo_testkit::ActorSystemTestKit::new("region-graceful-shutdown").unwrap();
    let coordinator = kit
        .create_probe::<ShardCoordinatorMsg<String>>("coordinator")
        .unwrap();
    let region = kit
        .system()
        .spawn(
            "region",
            ShardRegionActor::<String>::props_with_local_shards_and_registration(
                "region-a",
                10,
                10,
                coordinator.actor_ref(),
                Duration::from_secs(10),
            ),
        )
        .unwrap();
    let registration = coordinator.expect_msg(Duration::from_millis(500)).unwrap();
    let ShardCoordinatorMsg::RegisterLocalRegion { reply_to, .. } = registration else {
        panic!("expected local region registration");
    };
    reply_to
        .tell(Ok(CoordinatorStateSnapshot {
            allocations: BTreeMap::from([("region-a".to_string(), Vec::new())]),
            proxies: BTreeSet::new(),
            unallocated_shards: BTreeSet::new(),
            rebalance_in_progress: BTreeMap::new(),
            remember_entities: false,
        }))
        .unwrap();

    region
        .tell(ShardRegionMsg::GracefulShutdown { reply_to: None })
        .unwrap();
    let shutdown = coordinator.expect_msg(Duration::from_millis(500)).unwrap();
    assert!(matches!(
        shutdown,
        ShardCoordinatorMsg::GracefulShutdownReq { region, .. } if region == "region-a"
    ));
    kit.shutdown(Duration::from_secs(1)).unwrap();
}
