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
fn multi_node_graceful_shutdown_rebalances_region_shard_across_nodes() {
    let nodes = kairo_testkit::MultiNodeTestKit::new([
        "sharding-graceful-coordinator",
        "sharding-graceful-region-a",
        "sharding-graceful-region-b",
    ])
    .unwrap();
    let coordinator_kit = nodes.kit("sharding-graceful-coordinator").unwrap();
    let region_a_kit = nodes.kit("sharding-graceful-region-a").unwrap();
    let region_b_kit = nodes.kit("sharding-graceful-region-b").unwrap();
    let remember_store = coordinator_kit
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
    let region_a = region_a_kit
        .system()
        .spawn(
            "region-a",
            ShardRegionActor::<String>::props_with_remember_store_shards(
                "region-a",
                10,
                10,
                BTreeMap::from([("shard-1".to_string(), remember_store.clone())]),
                Duration::from_millis(500),
            ),
        )
        .unwrap();
    let region_b = region_b_kit
        .system()
        .spawn(
            "region-b",
            ShardRegionActor::<String>::props_with_remember_store_shards(
                "region-b",
                10,
                10,
                BTreeMap::from([("shard-1".to_string(), remember_store)]),
                Duration::from_millis(500),
            ),
        )
        .unwrap();
    let host = nodes
        .create_probe_on::<HostShardPlan<String>>("sharding-graceful-region-a", "host")
        .unwrap();
    region_a
        .tell(ShardRegionMsg::HostShard {
            shard: "shard-1".to_string(),
            reply_to: host.actor_ref(),
        })
        .unwrap();
    host.expect_msg(Duration::from_millis(500)).unwrap();
    assert!(
        !nodes
            .enter_barrier("initial-shard-hosted", "sharding-graceful-region-a")
            .unwrap()
            .passed()
    );
    assert!(
        !nodes
            .enter_barrier("initial-shard-hosted", "sharding-graceful-region-b")
            .unwrap()
            .passed()
    );
    assert!(
        nodes
            .enter_barrier("initial-shard-hosted", "sharding-graceful-coordinator")
            .unwrap()
            .passed()
    );

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
    let coordinator = coordinator_kit
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
    let shutdown = nodes
        .create_probe_on::<RegionShutdownPlan>("sharding-graceful-coordinator", "shutdown")
        .unwrap();
    let snapshot = nodes
        .create_probe_on::<CoordinatorStateSnapshot>(
            "sharding-graceful-coordinator",
            "coordinator-state",
        )
        .unwrap();
    let region_a_state = nodes
        .create_probe_on::<ShardRegionSnapshot>("sharding-graceful-region-a", "region-a-state")
        .unwrap();
    let region_b_state = nodes
        .create_probe_on::<ShardRegionSnapshot>("sharding-graceful-region-b", "region-b-state")
        .unwrap();
    let region_b_local_shard = nodes
        .create_probe_on::<Option<ActorRef<ShardMsg<String>>>>(
            "sharding-graceful-region-b",
            "region-b-local-shard",
        )
        .unwrap();
    let region_b_delivery = nodes
        .create_probe_on::<ShardDeliverPlan<String>>(
            "sharding-graceful-region-b",
            "region-b-delivery",
        )
        .unwrap();
    assert!(
        !nodes
            .enter_barrier("coordinator-ready", "sharding-graceful-coordinator")
            .unwrap()
            .passed()
    );
    assert!(
        !nodes
            .enter_barrier("coordinator-ready", "sharding-graceful-region-a")
            .unwrap()
            .passed()
    );
    assert!(
        nodes
            .enter_barrier("coordinator-ready", "sharding-graceful-region-b")
            .unwrap()
            .passed()
    );

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
        "multi-node graceful shutdown should hand off and reallocate region-a shard"
    );
    assert!(
        !nodes
            .enter_barrier(
                "graceful-shutdown-complete",
                "sharding-graceful-coordinator"
            )
            .unwrap()
            .passed()
    );
    assert!(
        !nodes
            .enter_barrier("graceful-shutdown-complete", "sharding-graceful-region-a")
            .unwrap()
            .passed()
    );
    assert!(
        nodes
            .enter_barrier("graceful-shutdown-complete", "sharding-graceful-region-b")
            .unwrap()
            .passed()
    );

    region_a
        .tell(ShardRegionMsg::GetState {
            reply_to: region_a_state.actor_ref(),
        })
        .unwrap();
    let region_a_snapshot = region_a_state
        .expect_msg(Duration::from_millis(500))
        .unwrap();
    assert!(!region_a_snapshot.local_shards.contains("shard-1"));
    assert!(!region_a_snapshot.handing_off_shards.contains("shard-1"));

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
            reply_to: region_b_local_shard.actor_ref(),
        })
        .unwrap();
    let shard = region_b_local_shard
        .expect_msg(Duration::from_millis(500))
        .unwrap()
        .expect("region-b should host shard-1 after graceful shutdown");
    shard
        .tell(ShardMsg::Deliver {
            message: ShardingEnvelope::new("entity-1", "after-handoff".to_string()),
            reply_to: region_b_delivery.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        region_b_delivery
            .expect_msg(Duration::from_millis(500))
            .unwrap(),
        ShardDeliverPlan::Deliver {
            delivery: EntityDelivery::new("entity-1", "after-handoff".to_string()),
        }
    );
    nodes.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn multi_node_passivated_entity_is_not_recovered_after_rehost() {
    let nodes = kairo_testkit::MultiNodeTestKit::new([
        "sharding-passivate-store",
        "sharding-passivate-region-a",
        "sharding-passivate-region-b",
    ])
    .unwrap();
    let store_kit = nodes.kit("sharding-passivate-store").unwrap();
    let region_a_kit = nodes.kit("sharding-passivate-region-a").unwrap();
    let region_b_kit = nodes.kit("sharding-passivate-region-b").unwrap();
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
    let region_a = region_a_kit
        .system()
        .spawn(
            "region-a",
            ShardRegionActor::<String>::props_with_remember_store_shards(
                "region-a",
                10,
                10,
                BTreeMap::from([("shard-1".to_string(), remember_store.clone())]),
                Duration::from_millis(500),
            ),
        )
        .unwrap();
    let region_b = region_b_kit
        .system()
        .spawn(
            "region-b",
            ShardRegionActor::<String>::props_with_remember_store_shards(
                "region-b",
                10,
                10,
                BTreeMap::from([("shard-1".to_string(), remember_store.clone())]),
                Duration::from_millis(500),
            ),
        )
        .unwrap();
    let host_a = nodes
        .create_probe_on::<HostShardPlan<String>>("sharding-passivate-region-a", "host-a")
        .unwrap();
    let shard_a_ref = nodes
        .create_probe_on::<Option<ActorRef<ShardMsg<String>>>>(
            "sharding-passivate-region-a",
            "shard-a-ref",
        )
        .unwrap();
    let delivery_a = nodes
        .create_probe_on::<ShardDeliverPlan<String>>("sharding-passivate-region-a", "delivery-a")
        .unwrap();
    let passivation = nodes
        .create_probe_on::<PassivatePlan<String>>("sharding-passivate-region-a", "passivation")
        .unwrap();
    let termination = nodes
        .create_probe_on::<crate::EntityTerminatedPlan<String>>(
            "sharding-passivate-region-a",
            "termination",
        )
        .unwrap();
    let store_state = nodes
        .create_probe_on::<RememberShardStoreSnapshot>("sharding-passivate-store", "store-state")
        .unwrap();
    let host_b = nodes
        .create_probe_on::<HostShardPlan<String>>("sharding-passivate-region-b", "host-b")
        .unwrap();
    let route_b = nodes
        .create_probe_on::<RegionLocalRoutePlan<String>>("sharding-passivate-region-b", "route-b")
        .unwrap();
    let delivery_b = nodes
        .create_probe_on::<ShardDeliverPlan<String>>("sharding-passivate-region-b", "delivery-b")
        .unwrap();

    region_a
        .tell(ShardRegionMsg::HostShard {
            shard: "shard-1".to_string(),
            reply_to: host_a.actor_ref(),
        })
        .unwrap();
    host_a.expect_msg(Duration::from_millis(500)).unwrap();
    region_a
        .tell(ShardRegionMsg::GetLocalShard {
            shard: "shard-1".to_string(),
            reply_to: shard_a_ref.actor_ref(),
        })
        .unwrap();
    let shard_a = shard_a_ref
        .expect_msg(Duration::from_millis(500))
        .unwrap()
        .expect("region-a should host shard-1");
    shard_a
        .tell(ShardMsg::Deliver {
            message: ShardingEnvelope::new("entity-1", "before-passivation".to_string()),
            reply_to: delivery_a.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        delivery_a.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardDeliverPlan::Deliver {
            delivery: EntityDelivery::new("entity-1", "before-passivation".to_string()),
        }
    );

    shard_a
        .tell(ShardMsg::Passivate {
            entity_id: "entity-1".to_string(),
            stop_message: "stop".to_string(),
            reply_to: passivation.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        passivation.expect_msg(Duration::from_millis(500)).unwrap(),
        PassivatePlan::SendStop {
            entity_id: "entity-1".to_string(),
            stop_message: "stop".to_string(),
        }
    );
    shard_a
        .tell(ShardMsg::EntityTerminated {
            entity_id: "entity-1".to_string(),
            reply_to: termination.actor_ref(),
        })
        .unwrap();
    let stop_update =
        RememberShardUpdate::new(std::iter::empty::<String>(), ["entity-1".to_string()]);
    assert_eq!(
        termination.expect_msg(Duration::from_millis(500)).unwrap(),
        crate::EntityTerminatedPlan::RememberUpdate {
            update: stop_update,
        }
    );

    let mut store_empty = false;
    for _ in 0..20 {
        remember_store
            .tell(RememberShardStoreMsg::GetState {
                reply_to: store_state.actor_ref(),
            })
            .unwrap();
        let snapshot = store_state.expect_msg(Duration::from_millis(500)).unwrap();
        store_empty = snapshot.entities_by_key.values().all(BTreeSet::is_empty);
        if store_empty {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        store_empty,
        "passivated entity should be removed from shared remember store"
    );

    region_b
        .tell(ShardRegionMsg::HostShard {
            shard: "shard-1".to_string(),
            reply_to: host_b.actor_ref(),
        })
        .unwrap();
    host_b.expect_msg(Duration::from_millis(500)).unwrap();
    region_b
        .tell(ShardRegionMsg::RouteToLocalShard {
            shard: "shard-1".to_string(),
            message: ShardingEnvelope::new("entity-1", "after-rehost".to_string()),
            route_reply_to: route_b.actor_ref(),
            delivery_reply_to: delivery_b.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        route_b.expect_msg(Duration::from_millis(500)).unwrap(),
        RegionLocalRoutePlan::DeliveredToLocalShard {
            shard: "shard-1".to_string(),
        }
    );
    assert_eq!(
        delivery_b.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardDeliverPlan::RememberUpdate {
            update: RememberShardUpdate::new(
                ["entity-1".to_string()],
                std::iter::empty::<String>(),
            ),
        }
    );
    nodes.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn coordinator_system_inbound_remote_graceful_shutdown_rebalances_to_local_region() {
    let kit =
        kairo_testkit::ActorSystemTestKit::new("coordinator-remote-graceful-shutdown").unwrap();
    let mut registry = Registry::new();
    register_sharding_protocol_codecs(&mut registry).unwrap();
    let registry = Arc::new(registry);
    let coordinator_wire =
        ActorRefWireData::new("kairo://local@127.0.0.1:2551/system/sharding/coordinator").unwrap();
    let remote_region =
        ActorRefWireData::new("kairo://remote@127.0.0.1:2552/system/sharding/region").unwrap();
    let remote_region_id = remote_region_id(&remote_region);
    let remote_outbound = kit
        .create_probe::<RemoteEnvelope>("remote-region-outbound")
        .unwrap();
    let region_b = kit
        .system()
        .spawn(
            "region-b",
            ShardRegionActor::<String>::props_with_local_shards("region-b", 10, 10),
        )
        .unwrap();
    let mut state = CoordinatorState::new();
    state
        .apply(CoordinatorEvent::ShardRegionRegistered {
            region: remote_region_id.clone(),
        })
        .unwrap();
    state
        .apply(CoordinatorEvent::ShardRegionRegistered {
            region: "region-b".to_string(),
        })
        .unwrap();
    state
        .apply(CoordinatorEvent::ShardHomeAllocated {
            shard: "shard-1".to_string(),
            region: remote_region_id.clone(),
        })
        .unwrap();
    let mut transport = HandoffTransport::new();
    transport.insert_target(HandoffRegionTarget::new("region-b", region_b.clone()));
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
    let inbound = ShardCoordinatorSystemInbound::<String>::new(
        coordinator.clone(),
        coordinator_wire.clone(),
        registry.clone(),
        remote_outbound.actor_ref(),
    );

    inbound
        .receive(RemoteEnvelope::new(
            coordinator_wire.clone(),
            Some(remote_region.clone()),
            registry
                .serialize(&Register {
                    region: remote_region.clone(),
                })
                .unwrap(),
        ))
        .unwrap();
    let register_ack = remote_outbound
        .expect_msg(Duration::from_millis(500))
        .unwrap();
    assert_eq!(register_ack.recipient, remote_region);
    assert_eq!(register_ack.sender, Some(coordinator_wire.clone()));
    assert_eq!(
        registry
            .deserialize::<RegisterAck>(register_ack.message)
            .unwrap(),
        RegisterAck {
            coordinator: coordinator_wire.clone()
        }
    );

    inbound
        .receive(RemoteEnvelope::new(
            coordinator_wire.clone(),
            Some(remote_region.clone()),
            registry
                .serialize(&GracefulShutdownReq {
                    region: remote_region.clone(),
                })
                .unwrap(),
        ))
        .unwrap();
    let begin = remote_outbound
        .expect_msg(Duration::from_millis(500))
        .unwrap();
    assert_eq!(begin.recipient, remote_region);
    assert_eq!(begin.sender, Some(coordinator_wire.clone()));
    assert_eq!(
        registry.deserialize::<BeginHandOff>(begin.message).unwrap(),
        BeginHandOff {
            shard_id: "shard-1".to_string()
        }
    );

    inbound
        .receive(RemoteEnvelope::new(
            coordinator_wire.clone(),
            Some(remote_region.clone()),
            registry
                .serialize(&BeginHandOffAck {
                    shard_id: "shard-1".to_string(),
                })
                .unwrap(),
        ))
        .unwrap();
    let handoff = remote_outbound
        .expect_msg(Duration::from_millis(500))
        .unwrap();
    assert_eq!(handoff.recipient, remote_region.clone());
    assert_eq!(handoff.sender, Some(coordinator_wire.clone()));
    assert_eq!(
        registry.deserialize::<HandOff>(handoff.message).unwrap(),
        HandOff {
            shard_id: "shard-1".to_string()
        }
    );

    inbound
        .receive(RemoteEnvelope::new(
            coordinator_wire,
            Some(remote_region),
            registry
                .serialize(&ShardStopped {
                    shard_id: "shard-1".to_string(),
                })
                .unwrap(),
        ))
        .unwrap();

    let snapshot = kit
        .create_probe::<CoordinatorStateSnapshot>("coordinator-state")
        .unwrap();
    let region_b_state = kit
        .create_probe::<ShardRegionSnapshot>("region-b-state")
        .unwrap();
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
                .get(&remote_region_id)
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
        "remote graceful shutdown should hand off and reallocate shard to region-b"
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

#[test]
fn region_actor_repeats_graceful_shutdown_when_host_shard_arrives_during_shutdown() {
    let kit = kairo_testkit::ActorSystemTestKit::new("region-graceful-host-shard").unwrap();
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
    let host = kit.create_probe::<HostShardPlan<String>>("host").unwrap();

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
        .tell(ShardRegionMsg::HostShard {
            shard: "shard-1".to_string(),
            reply_to: host.actor_ref(),
        })
        .unwrap();
    assert!(matches!(
        host.expect_msg(Duration::from_millis(500)).unwrap(),
        HostShardPlan::AlreadyStarted { ref shard, .. } if shard == "shard-1"
    ));

    region
        .tell(ShardRegionMsg::GracefulShutdown { reply_to: None })
        .unwrap();
    assert!(matches!(
        coordinator.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardCoordinatorMsg::GracefulShutdownReq { region, .. } if region == "region-a"
    ));

    region
        .tell(ShardRegionMsg::HostShard {
            shard: "shard-2".to_string(),
            reply_to: host.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        host.expect_msg(Duration::from_millis(500)).unwrap(),
        HostShardPlan::IgnoredGracefulShutdown {
            shard: "shard-2".to_string(),
        }
    );
    assert!(matches!(
        coordinator.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardCoordinatorMsg::GracefulShutdownReq { region, .. } if region == "region-a"
    ));

    let buffered = kit
        .create_probe::<RegionBufferedReplayPlan>("buffered-host")
        .unwrap();
    let delivery = kit
        .create_probe::<ShardDeliverPlan<String>>("buffered-delivery")
        .unwrap();
    region
        .tell(ShardRegionMsg::HostShardAndReplayBuffered {
            shard: "shard-3".to_string(),
            reply_to: buffered.actor_ref(),
            delivery_reply_to: delivery.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        buffered.expect_msg(Duration::from_millis(500)).unwrap(),
        RegionBufferedReplayPlan::IgnoredGracefulShutdown {
            shard: "shard-3".to_string(),
        }
    );
    assert!(matches!(
        coordinator.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardCoordinatorMsg::GracefulShutdownReq { region, .. } if region == "region-a"
    ));
    kit.shutdown(Duration::from_secs(1)).unwrap();
}
