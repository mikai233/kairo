use std::collections::BTreeSet;
use std::sync::mpsc;
use std::time::Duration;

use kairo_actor::{Actor, ActorError, ActorResult, ActorSystem, Context, Props};

use crate::{
    BeginHandOffPlan, CoordinatorEvent, CoordinatorRuntime, CoordinatorState,
    CoordinatorStateSnapshot, EntityRef, GetShardHome, GetShardHomeIgnoreReason, GetShardHomePlan,
    HandOff, HandOffPlan, HostShard, HostShardPlan, LeastShardAllocationStrategy,
    RebalanceCompletionPlan, RebalancePlan, RebalanceSkipReason, RegionDropReason, RegionRoutePlan,
    RememberCoordinatorStoreState, RememberShardStoreState, RememberShardUpdate, ShardActor,
    ShardAllocationStrategy, ShardAllocations, ShardCoordinatorActor, ShardCoordinatorMsg,
    ShardDeliverPlan, ShardDropReason, ShardEntityState, ShardHandOffPlan, ShardHomePlan, ShardMsg,
    ShardRegionActor, ShardRegionMsg, ShardRegionRuntime, ShardRegionSnapshot, ShardRuntime,
    ShardSnapshot, ShardStarted, ShardStartedPlan, ShardStopped, ShardingEnvelope, ShardingError,
    default_shard_id_for, remember_entity_key_index, remember_entity_key_index_for,
    remember_entity_shard_key, shard_id_for, stable_hash_entity_id,
};

#[test]
fn sharding_envelope_keeps_entity_id_outside_business_message() {
    let envelope = ShardingEnvelope::new("counter-1", "increment");

    assert_eq!(envelope.entity_id(), "counter-1");
    assert_eq!(envelope.message(), &"increment");
    assert_eq!(
        envelope.into_parts(),
        ("counter-1".to_string(), "increment")
    );
}

#[test]
fn entity_ref_wraps_business_message_in_sharding_envelope() {
    let system = ActorSystem::builder("sharding").build().unwrap();
    let (tx, rx) = mpsc::channel();
    let region = system
        .spawn("region", Props::new(move || RegionProbe { observed: tx }))
        .unwrap();
    let entity_ref = EntityRef::new("counter-1", region);

    entity_ref.tell("increment").unwrap();

    assert_eq!(
        rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        ("counter-1".to_string(), "increment")
    );
}

#[test]
fn shard_ids_use_documented_stable_hash() {
    assert_eq!(stable_hash_entity_id("counter-1"), 0x31c4c004cce265c1);
    assert_eq!(shard_id_for("counter-1", 100).unwrap(), "65");
    assert_eq!(default_shard_id_for("counter-1"), "65");
}

#[test]
fn shard_id_rejects_zero_shards() {
    assert_eq!(
        shard_id_for("counter-1", 0),
        Err(ShardingError::InvalidShardCount)
    );
}

#[test]
fn remember_entity_keys_use_pekkos_stable_partitioning() {
    assert_eq!(remember_entity_key_index("entity-1"), 3);
    assert_eq!(remember_entity_key_index("entity-2"), 2);
    assert_eq!(remember_entity_key_index("counter-1"), 2);
    assert_eq!(
        remember_entity_shard_key("orders", "shard-1", 3).unwrap(),
        "shard-orders-shard-1-3"
    );
}

#[test]
fn remember_entity_key_helpers_reject_invalid_counts_and_indexes() {
    assert_eq!(
        remember_entity_key_index_for("entity-1", 0),
        Err(ShardingError::InvalidRememberEntityKeyCount)
    );
    assert_eq!(
        remember_entity_shard_key("orders", "shard-1", 5),
        Err(ShardingError::InvalidRememberEntityKeyIndex {
            index: 5,
            key_count: 5,
        })
    );
}

#[test]
fn remember_shard_store_loads_and_updates_partitioned_entities() {
    let mut state = RememberShardStoreState::with_entities(
        "orders",
        "shard-1",
        ["entity-1".to_string(), "entity-2".to_string()],
    );

    assert_eq!(
        state.remembered_entities(),
        BTreeSet::from(["entity-1".to_string(), "entity-2".to_string()])
    );
    assert_eq!(
        state.entities_for_key(3),
        Some(&BTreeSet::from(["entity-1".to_string()]))
    );
    assert_eq!(
        state.entities_for_key(2),
        Some(&BTreeSet::from(["entity-2".to_string()]))
    );

    let done = state
        .apply_update(RememberShardUpdate::new(
            ["entity-3".to_string()],
            ["entity-1".to_string()],
        ))
        .unwrap();

    assert_eq!(done.started, BTreeSet::from(["entity-3".to_string()]));
    assert_eq!(done.stopped, BTreeSet::from(["entity-1".to_string()]));
    assert_eq!(
        state.remembered_entities(),
        BTreeSet::from(["entity-2".to_string(), "entity-3".to_string()])
    );
}

#[test]
fn remember_shard_store_treats_stopping_unknown_entity_as_idempotent() {
    let mut state = RememberShardStoreState::new("orders", "shard-1");

    state
        .apply_update(RememberShardUpdate::new(
            std::iter::empty::<String>(),
            ["missing".to_string()],
        ))
        .unwrap();

    assert!(state.remembered_entities().is_empty());
}

#[test]
fn remember_coordinator_store_remembers_shards_additively() {
    let mut state = RememberCoordinatorStoreState::with_shards(["1".to_string()]);

    assert_eq!(state.get_shards().shards, BTreeSet::from(["1".to_string()]));
    assert_eq!(state.add_shard("2").shard, "2");
    assert_eq!(state.add_shard("2").shard, "2");
    assert_eq!(
        state.remembered_shards(),
        &BTreeSet::from(["1".to_string(), "2".to_string()])
    );
}

#[test]
fn shard_allocations_track_single_region_owner_per_shard() {
    let mut allocations =
        ShardAllocations::from_regions(["region-a".to_string(), "region-b".to_string()]);
    let region_a = "region-a".to_string();
    let region_b = "region-b".to_string();
    let shard = "shard-1".to_string();

    assert!(
        allocations
            .allocate_shard(&region_a, shard.clone())
            .unwrap()
    );
    assert!(
        !allocations
            .allocate_shard(&region_a, shard.clone())
            .unwrap()
    );
    assert_eq!(allocations.region_for_shard(&shard), Some(&region_a));

    assert!(
        allocations
            .allocate_shard(&region_b, shard.clone())
            .unwrap()
    );
    assert_eq!(allocations.region_for_shard(&shard), Some(&region_b));
    assert_eq!(allocations.shards_for(&region_a), Some([].as_slice()));
    assert_eq!(allocations.shards_for(&region_b), Some([shard].as_slice()));
}

#[test]
fn least_shard_strategy_allocates_to_region_with_fewest_shards() {
    let strategy = LeastShardAllocationStrategy::default();
    let mut allocations = ShardAllocations::from_regions([
        "region-a".to_string(),
        "region-b".to_string(),
        "region-c".to_string(),
    ]);
    allocations
        .allocate_shard(&"region-a".to_string(), "1")
        .unwrap();
    allocations
        .allocate_shard(&"region-a".to_string(), "2")
        .unwrap();
    allocations
        .allocate_shard(&"region-b".to_string(), "3")
        .unwrap();

    let allocated = strategy
        .allocate_shard(&"region-a".to_string(), &"4".to_string(), &allocations)
        .unwrap();

    assert_eq!(allocated, "region-c");
}

#[test]
fn least_shard_strategy_rebalances_from_overloaded_regions() {
    let strategy = LeastShardAllocationStrategy::new(3, 1.0).unwrap();
    let mut allocations = ShardAllocations::from_regions([
        "region-a".to_string(),
        "region-b".to_string(),
        "region-c".to_string(),
    ]);
    for shard in ["1", "2", "3", "4", "5"] {
        allocations
            .allocate_shard(&"region-a".to_string(), shard)
            .unwrap();
    }
    allocations
        .allocate_shard(&"region-b".to_string(), "6")
        .unwrap();

    let rebalanced = strategy.rebalance(&allocations, &BTreeSet::new()).unwrap();

    assert_eq!(
        rebalanced,
        BTreeSet::from(["1".to_string(), "2".to_string(), "3".to_string()])
    );
}

#[test]
fn least_shard_strategy_limits_rebalance_and_skips_when_in_progress() {
    let strategy = LeastShardAllocationStrategy::new(2, 0.25).unwrap();
    let mut allocations =
        ShardAllocations::from_regions(["region-a".to_string(), "region-b".to_string()]);
    for shard in ["1", "2", "3", "4", "5", "6", "7", "8"] {
        allocations
            .allocate_shard(&"region-a".to_string(), shard)
            .unwrap();
    }

    let rebalanced = strategy.rebalance(&allocations, &BTreeSet::new()).unwrap();
    assert_eq!(rebalanced.len(), 2);

    let skipped = strategy
        .rebalance(&allocations, &BTreeSet::from(["1".to_string()]))
        .unwrap();
    assert!(skipped.is_empty());
}

#[test]
fn least_shard_strategy_phase_two_moves_one_shard_to_empty_region() {
    let strategy = LeastShardAllocationStrategy::new(10, 1.0).unwrap();
    let mut allocations = ShardAllocations::from_regions([
        "region-a".to_string(),
        "region-b".to_string(),
        "region-c".to_string(),
    ]);
    allocations
        .allocate_shard(&"region-a".to_string(), "1")
        .unwrap();
    allocations
        .allocate_shard(&"region-a".to_string(), "2")
        .unwrap();
    allocations
        .allocate_shard(&"region-b".to_string(), "3")
        .unwrap();
    allocations
        .allocate_shard(&"region-b".to_string(), "4")
        .unwrap();

    let rebalanced = strategy.rebalance(&allocations, &BTreeSet::new()).unwrap();

    assert_eq!(rebalanced, BTreeSet::from(["1".to_string()]));
}

#[test]
fn coordinator_state_applies_region_and_proxy_registration_events() {
    let mut state = CoordinatorState::new();

    state
        .apply(CoordinatorEvent::ShardRegionRegistered {
            region: "region-a".to_string(),
        })
        .unwrap();
    state
        .apply(CoordinatorEvent::ShardRegionProxyRegistered {
            proxy: "proxy-a".to_string(),
        })
        .unwrap();

    assert!(state.allocations().contains_region(&"region-a".to_string()));
    assert!(state.proxies().contains("proxy-a"));
    assert!(!state.is_empty());
    assert_eq!(
        state
            .apply(CoordinatorEvent::ShardRegionRegistered {
                region: "region-a".to_string(),
            })
            .unwrap_err(),
        ShardingError::RegionAlreadyRegistered("region-a".to_string())
    );
}

#[test]
fn coordinator_state_allocates_and_deallocates_shard_homes() {
    let mut state = CoordinatorState::new();
    state
        .apply(CoordinatorEvent::ShardRegionRegistered {
            region: "region-a".to_string(),
        })
        .unwrap();
    state
        .apply(CoordinatorEvent::ShardHomeAllocated {
            shard: "shard-1".to_string(),
            region: "region-a".to_string(),
        })
        .unwrap();

    assert_eq!(
        state.shard_home(&"shard-1".to_string()),
        Some(&"region-a".to_string())
    );
    assert_eq!(state.all_shards(), BTreeSet::from(["shard-1".to_string()]));
    assert_eq!(
        state
            .apply(CoordinatorEvent::ShardHomeAllocated {
                shard: "shard-1".to_string(),
                region: "region-a".to_string(),
            })
            .unwrap_err(),
        ShardingError::ShardAlreadyAllocated("shard-1".to_string())
    );

    state
        .apply(CoordinatorEvent::ShardHomeDeallocated {
            shard: "shard-1".to_string(),
        })
        .unwrap();
    assert_eq!(state.shard_home(&"shard-1".to_string()), None);
    assert!(state.all_shards().is_empty());
}

#[test]
fn coordinator_state_remembers_unallocated_shards_when_enabled() {
    let mut state = CoordinatorState::new().with_remember_entities(true);
    state
        .apply(CoordinatorEvent::ShardRegionRegistered {
            region: "region-a".to_string(),
        })
        .unwrap();
    state
        .apply(CoordinatorEvent::ShardHomeAllocated {
            shard: "shard-1".to_string(),
            region: "region-a".to_string(),
        })
        .unwrap();
    state
        .apply(CoordinatorEvent::ShardHomeDeallocated {
            shard: "shard-1".to_string(),
        })
        .unwrap();

    assert_eq!(
        state.unallocated_shards(),
        &BTreeSet::from(["shard-1".to_string()])
    );
    assert_eq!(state.all_shards(), BTreeSet::from(["shard-1".to_string()]));

    state
        .apply(CoordinatorEvent::ShardHomeAllocated {
            shard: "shard-1".to_string(),
            region: "region-a".to_string(),
        })
        .unwrap();
    assert!(state.unallocated_shards().is_empty());
}

#[test]
fn coordinator_state_terminates_regions_and_proxies() {
    let mut state = CoordinatorState::new().with_remember_entities(true);
    state
        .apply(CoordinatorEvent::ShardRegionRegistered {
            region: "region-a".to_string(),
        })
        .unwrap();
    state
        .apply(CoordinatorEvent::ShardRegionProxyRegistered {
            proxy: "proxy-a".to_string(),
        })
        .unwrap();
    state
        .apply(CoordinatorEvent::ShardHomeAllocated {
            shard: "shard-1".to_string(),
            region: "region-a".to_string(),
        })
        .unwrap();

    state
        .apply(CoordinatorEvent::ShardRegionTerminated {
            region: "region-a".to_string(),
        })
        .unwrap();
    state
        .apply(CoordinatorEvent::ShardRegionProxyTerminated {
            proxy: "proxy-a".to_string(),
        })
        .unwrap();

    assert!(!state.allocations().contains_region(&"region-a".to_string()));
    assert!(!state.proxies().contains("proxy-a"));
    assert_eq!(
        state.unallocated_shards(),
        &BTreeSet::from(["shard-1".to_string()])
    );
    assert_eq!(
        state
            .apply(CoordinatorEvent::ShardRegionTerminated {
                region: "region-a".to_string(),
            })
            .unwrap_err(),
        ShardingError::UnknownRegion("region-a".to_string())
    );
}

#[test]
fn coordinator_runtime_replies_with_known_shard_home() {
    let strategy = LeastShardAllocationStrategy::default();
    let mut runtime = coordinator_runtime_with_regions(["region-a"]);
    runtime
        .apply_event(CoordinatorEvent::ShardHomeAllocated {
            shard: "shard-1".to_string(),
            region: "region-a".to_string(),
        })
        .unwrap();

    let plan = runtime
        .request_shard_home("requester", "shard-1", &strategy)
        .unwrap();

    assert_eq!(
        plan,
        GetShardHomePlan::Reply {
            shard: "shard-1".to_string(),
            region: "region-a".to_string(),
        }
    );
}

#[test]
fn coordinator_runtime_allocates_unknown_shard_and_plans_host_shard() {
    let strategy = LeastShardAllocationStrategy::default();
    let mut runtime = coordinator_runtime_with_regions(["region-a", "region-b"]);
    runtime
        .apply_event(CoordinatorEvent::ShardHomeAllocated {
            shard: "existing".to_string(),
            region: "region-a".to_string(),
        })
        .unwrap();

    let plan = runtime
        .request_shard_home("requester", "new-shard", &strategy)
        .unwrap();

    assert_eq!(
        plan,
        GetShardHomePlan::Allocated {
            event: CoordinatorEvent::ShardHomeAllocated {
                shard: "new-shard".to_string(),
                region: "region-b".to_string(),
            },
            host_region: "region-b".to_string(),
            host_shard: crate::HostShard {
                shard_id: "new-shard".to_string(),
            },
        }
    );
    assert_eq!(
        runtime.state().shard_home(&"new-shard".to_string()),
        Some(&"region-b".to_string())
    );
}

#[test]
fn coordinator_actor_applies_registration_and_allocates_shard_home() {
    let kit = kairo_testkit::ActorSystemTestKit::new("coordinator-actor-allocation").unwrap();
    let coordinator = kit
        .system()
        .spawn(
            "coordinator",
            ShardCoordinatorActor::props_with_least_shard_strategy(CoordinatorState::new()),
        )
        .unwrap();
    let state = kit
        .create_probe::<Result<CoordinatorStateSnapshot, ShardingError>>("state")
        .unwrap();
    let home = kit
        .create_probe::<Result<GetShardHomePlan, ShardingError>>("home")
        .unwrap();

    coordinator
        .tell(ShardCoordinatorMsg::ApplyEvent {
            event: CoordinatorEvent::ShardRegionRegistered {
                region: "region-a".to_string(),
            },
            reply_to: Some(state.actor_ref()),
        })
        .unwrap();
    assert!(
        state
            .expect_msg(Duration::from_millis(500))
            .unwrap()
            .unwrap()
            .allocations
            .contains_key("region-a")
    );
    coordinator
        .tell(ShardCoordinatorMsg::ApplyEvent {
            event: CoordinatorEvent::ShardRegionRegistered {
                region: "region-b".to_string(),
            },
            reply_to: Some(state.actor_ref()),
        })
        .unwrap();
    state
        .expect_msg(Duration::from_millis(500))
        .unwrap()
        .unwrap();

    coordinator
        .tell(ShardCoordinatorMsg::RequestShardHome {
            requester: "region-b".to_string(),
            shard: "new-shard".to_string(),
            reply_to: home.actor_ref(),
        })
        .unwrap();

    assert_eq!(
        home.expect_msg(Duration::from_millis(500))
            .unwrap()
            .unwrap(),
        GetShardHomePlan::Allocated {
            event: CoordinatorEvent::ShardHomeAllocated {
                shard: "new-shard".to_string(),
                region: "region-a".to_string(),
            },
            host_region: "region-a".to_string(),
            host_shard: HostShard {
                shard_id: "new-shard".to_string(),
            },
        }
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn coordinator_actor_plans_rebalance_and_defers_shard_home_requests() {
    let mut state = CoordinatorState::new();
    for region in ["region-a", "region-b"] {
        state
            .apply(CoordinatorEvent::ShardRegionRegistered {
                region: region.to_string(),
            })
            .unwrap();
    }
    for shard in ["s1", "s2"] {
        state
            .apply(CoordinatorEvent::ShardHomeAllocated {
                shard: shard.to_string(),
                region: "region-a".to_string(),
            })
            .unwrap();
    }

    let kit = kairo_testkit::ActorSystemTestKit::new("coordinator-actor-rebalance").unwrap();
    let coordinator = kit
        .system()
        .spawn(
            "coordinator",
            ShardCoordinatorActor::props(state, FixedRebalanceStrategy::new(["s1"])),
        )
        .unwrap();
    let rebalance = kit
        .create_probe::<Result<RebalancePlan, ShardingError>>("rebalance")
        .unwrap();
    let home = kit
        .create_probe::<Result<GetShardHomePlan, ShardingError>>("home")
        .unwrap();
    let completion = kit
        .create_probe::<Result<RebalanceCompletionPlan, ShardingError>>("completion")
        .unwrap();

    coordinator
        .tell(ShardCoordinatorMsg::PlanRebalance {
            reply_to: rebalance.actor_ref(),
        })
        .unwrap();
    let plan = rebalance
        .expect_msg(Duration::from_millis(500))
        .unwrap()
        .unwrap();
    assert!(
        matches!(plan, RebalancePlan::Started { ref shards } if shards.len() == 1 && shards[0].shard == "s1")
    );

    coordinator
        .tell(ShardCoordinatorMsg::RequestShardHome {
            requester: "region-b".to_string(),
            shard: "s1".to_string(),
            reply_to: home.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        home.expect_msg(Duration::from_millis(500))
            .unwrap()
            .unwrap(),
        GetShardHomePlan::Deferred {
            shard: "s1".to_string(),
            requester: "region-b".to_string(),
        }
    );

    coordinator
        .tell(ShardCoordinatorMsg::CompleteRebalance {
            shard: "s1".to_string(),
            ok: true,
            reply_to: completion.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        completion
            .expect_msg(Duration::from_millis(500))
            .unwrap()
            .unwrap(),
        RebalanceCompletionPlan::Deallocated {
            shard: "s1".to_string(),
            event: CoordinatorEvent::ShardHomeDeallocated {
                shard: "s1".to_string(),
            },
            pending_requesters: vec!["region-b".to_string()],
            retry_get_shard_home: GetShardHome {
                shard_id: "s1".to_string(),
            },
        }
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn coordinator_runtime_defers_requests_during_rebalance() {
    let strategy = LeastShardAllocationStrategy::default();
    let mut runtime = coordinator_runtime_with_regions(["region-a"]);
    assert!(runtime.begin_rebalance("shard-1"));

    let plan = runtime
        .request_shard_home("requester-a", "shard-1", &strategy)
        .unwrap();

    assert_eq!(
        plan,
        GetShardHomePlan::Deferred {
            shard: "shard-1".to_string(),
            requester: "requester-a".to_string(),
        }
    );
    assert_eq!(
        runtime.pending_rebalance_requesters(&"shard-1".to_string()),
        Some(&BTreeSet::from(["requester-a".to_string()]))
    );
    assert_eq!(
        runtime.clear_rebalance(&"shard-1".to_string()),
        vec!["requester-a".to_string()]
    );
    assert_eq!(
        runtime.pending_rebalance_requesters(&"shard-1".to_string()),
        None
    );
}

#[test]
fn coordinator_runtime_ignores_requests_until_regions_are_registered() {
    let strategy = LeastShardAllocationStrategy::default();
    let mut runtime = coordinator_runtime_with_regions(["region-a"]);
    runtime.set_all_regions_registered(false);

    let plan = runtime
        .request_shard_home("requester", "shard-1", &strategy)
        .unwrap();

    assert_eq!(
        plan,
        GetShardHomePlan::Ignored {
            shard: "shard-1".to_string(),
            reason: GetShardHomeIgnoreReason::NotAllRegionsRegistered,
        }
    );
}

#[test]
fn coordinator_runtime_excludes_shutdown_and_terminating_regions_from_allocation() {
    let strategy = LeastShardAllocationStrategy::default();
    let mut runtime = coordinator_runtime_with_regions(["region-a", "region-b", "region-c"]);
    runtime.mark_graceful_shutdown("region-a");
    runtime.mark_region_terminating("region-b");

    let plan = runtime
        .request_shard_home("requester", "shard-1", &strategy)
        .unwrap();

    assert_eq!(
        plan,
        GetShardHomePlan::Allocated {
            event: CoordinatorEvent::ShardHomeAllocated {
                shard: "shard-1".to_string(),
                region: "region-c".to_string(),
            },
            host_region: "region-c".to_string(),
            host_shard: crate::HostShard {
                shard_id: "shard-1".to_string(),
            },
        }
    );
}

#[test]
fn coordinator_runtime_ignores_known_home_when_region_is_terminating() {
    let strategy = LeastShardAllocationStrategy::default();
    let mut runtime = coordinator_runtime_with_regions(["region-a"]);
    runtime
        .apply_event(CoordinatorEvent::ShardHomeAllocated {
            shard: "shard-1".to_string(),
            region: "region-a".to_string(),
        })
        .unwrap();
    runtime.mark_region_terminating("region-a");

    let plan = runtime
        .request_shard_home("requester", "shard-1", &strategy)
        .unwrap();

    assert_eq!(
        plan,
        GetShardHomePlan::Ignored {
            shard: "shard-1".to_string(),
            reason: GetShardHomeIgnoreReason::HomeRegionTerminating {
                region: "region-a".to_string(),
            },
        }
    );
}

#[test]
fn coordinator_runtime_ignores_unknown_shard_when_no_active_region_exists() {
    let strategy = LeastShardAllocationStrategy::default();
    let mut runtime = coordinator_runtime_with_regions(["region-a"]);
    runtime.mark_region_terminating("region-a");

    let plan = runtime
        .request_shard_home("requester", "shard-1", &strategy)
        .unwrap();

    assert_eq!(
        plan,
        GetShardHomePlan::Ignored {
            shard: "shard-1".to_string(),
            reason: GetShardHomeIgnoreReason::NoActiveRegions,
        }
    );
}

#[test]
fn coordinator_runtime_plans_rebalance_workers_for_selected_owned_shards() {
    let strategy = LeastShardAllocationStrategy::new(1, 1.0).unwrap();
    let mut runtime = coordinator_runtime_with_regions(["region-a", "region-b"]);
    runtime
        .apply_event(CoordinatorEvent::ShardRegionProxyRegistered {
            proxy: "proxy-a".to_string(),
        })
        .unwrap();
    for shard in ["1", "2", "3", "4"] {
        runtime
            .apply_event(CoordinatorEvent::ShardHomeAllocated {
                shard: shard.to_string(),
                region: "region-a".to_string(),
            })
            .unwrap();
    }

    let plan = runtime.plan_rebalance(&strategy).unwrap();

    assert_eq!(
        plan,
        RebalancePlan::Started {
            shards: vec![crate::ShardRebalancePlan {
                shard: "1".to_string(),
                from_region: "region-a".to_string(),
                participants: BTreeSet::from([
                    "proxy-a".to_string(),
                    "region-a".to_string(),
                    "region-b".to_string(),
                ]),
                begin_handoff: crate::BeginHandOff {
                    shard_id: "1".to_string(),
                },
            }],
        }
    );
    assert_eq!(
        runtime.pending_rebalance_requesters(&"1".to_string()),
        Some(&BTreeSet::new())
    );
}

#[test]
fn coordinator_runtime_skips_rebalance_when_preparing_for_shutdown() {
    let strategy = LeastShardAllocationStrategy::default();
    let mut runtime = coordinator_runtime_with_regions(["region-a"]);
    runtime.set_preparing_for_shutdown(true);

    assert_eq!(
        runtime.plan_rebalance(&strategy).unwrap(),
        RebalancePlan::Skipped {
            reason: RebalanceSkipReason::PreparingForShutdown,
        }
    );
}

#[test]
fn coordinator_runtime_skips_rebalance_when_strategy_selects_no_shards() {
    let strategy = LeastShardAllocationStrategy::default();
    let mut runtime = coordinator_runtime_with_regions(["region-a", "region-b"]);

    assert_eq!(
        runtime.plan_rebalance(&strategy).unwrap(),
        RebalancePlan::Skipped {
            reason: RebalanceSkipReason::StrategySelectedNoShards,
        }
    );
}

#[test]
fn coordinator_runtime_ignores_strategy_selected_shards_without_homes() {
    let strategy = FixedRebalanceStrategy::new(["missing"]);
    let mut runtime = coordinator_runtime_with_regions(["region-a"]);

    assert_eq!(
        runtime.plan_rebalance(&strategy).unwrap(),
        RebalancePlan::Skipped {
            reason: RebalanceSkipReason::SelectedShardsMissingHomes,
        }
    );
}

#[test]
fn coordinator_runtime_deallocates_successful_rebalance_and_returns_pending_requesters() {
    let strategy = LeastShardAllocationStrategy::new(1, 1.0).unwrap();
    let mut runtime = coordinator_runtime_with_regions(["region-a", "region-b"]);
    for shard in ["1", "2", "3", "4"] {
        runtime
            .apply_event(CoordinatorEvent::ShardHomeAllocated {
                shard: shard.to_string(),
                region: "region-a".to_string(),
            })
            .unwrap();
    }
    assert!(matches!(
        runtime.plan_rebalance(&strategy).unwrap(),
        RebalancePlan::Started { .. }
    ));
    assert_eq!(
        runtime
            .request_shard_home("requester-a", "1", &strategy)
            .unwrap(),
        GetShardHomePlan::Deferred {
            shard: "1".to_string(),
            requester: "requester-a".to_string(),
        }
    );

    let completion = runtime.complete_rebalance("1", true).unwrap();

    assert_eq!(
        completion,
        RebalanceCompletionPlan::Deallocated {
            shard: "1".to_string(),
            event: CoordinatorEvent::ShardHomeDeallocated {
                shard: "1".to_string(),
            },
            pending_requesters: vec!["requester-a".to_string()],
            retry_get_shard_home: GetShardHome {
                shard_id: "1".to_string(),
            },
        }
    );
    assert_eq!(runtime.state().shard_home(&"1".to_string()), None);
    assert_eq!(runtime.pending_rebalance_requesters(&"1".to_string()), None);
}

#[test]
fn coordinator_runtime_timeout_clears_rebalance_without_deallocating() {
    let strategy = LeastShardAllocationStrategy::new(1, 1.0).unwrap();
    let mut runtime = coordinator_runtime_with_regions(["region-a", "region-b"]);
    for shard in ["1", "2", "3", "4"] {
        runtime
            .apply_event(CoordinatorEvent::ShardHomeAllocated {
                shard: shard.to_string(),
                region: "region-a".to_string(),
            })
            .unwrap();
    }
    runtime.plan_rebalance(&strategy).unwrap();
    runtime
        .request_shard_home("requester-a", "1", &strategy)
        .unwrap();

    assert_eq!(
        runtime.complete_rebalance("1", false).unwrap(),
        RebalanceCompletionPlan::TimedOut {
            shard: "1".to_string(),
            pending_requesters: vec!["requester-a".to_string()],
        }
    );
    assert_eq!(
        runtime.state().shard_home(&"1".to_string()),
        Some(&"region-a".to_string())
    );
    assert_eq!(runtime.pending_rebalance_requesters(&"1".to_string()), None);
}

#[test]
fn coordinator_runtime_completion_without_in_progress_rebalance_is_ignored() {
    let mut runtime = coordinator_runtime_with_regions(["region-a"]);
    runtime
        .apply_event(CoordinatorEvent::ShardHomeAllocated {
            shard: "1".to_string(),
            region: "region-a".to_string(),
        })
        .unwrap();

    assert_eq!(
        runtime.complete_rebalance("1", true).unwrap(),
        RebalanceCompletionPlan::Cleared {
            shard: "1".to_string(),
            pending_requesters: Vec::new(),
        }
    );
    assert_eq!(
        runtime.state().shard_home(&"1".to_string()),
        Some(&"region-a".to_string())
    );
}

#[test]
fn region_runtime_buffers_unknown_shard_and_requests_home_once() {
    let mut runtime = ShardRegionRuntime::new("region-a", 10);

    let first = runtime.route("shard-1", ShardingEnvelope::new("entity-1", "first"));
    let second = runtime.route("shard-1", ShardingEnvelope::new("entity-1", "second"));

    assert_eq!(
        first,
        RegionRoutePlan::Buffered {
            shard: "shard-1".to_string(),
            request: Some(GetShardHome {
                shard_id: "shard-1".to_string(),
            }),
        }
    );
    assert_eq!(
        second,
        RegionRoutePlan::Buffered {
            shard: "shard-1".to_string(),
            request: None,
        }
    );
    assert_eq!(runtime.buffered_count(&"shard-1".to_string()), 2);
}

#[test]
fn region_runtime_records_remote_home_and_forwards_buffered_messages() {
    let mut runtime = ShardRegionRuntime::new("region-a", 10);
    assert!(matches!(
        runtime.route("shard-1", ShardingEnvelope::new("entity-1", "first")),
        RegionRoutePlan::Buffered { .. }
    ));
    assert!(matches!(
        runtime.route("shard-1", ShardingEnvelope::new("entity-2", "second")),
        RegionRoutePlan::Buffered { .. }
    ));

    let plan = runtime.record_shard_home("shard-1", "region-b").unwrap();

    assert_eq!(
        plan,
        ShardHomePlan::Forward {
            shard: "shard-1".to_string(),
            region: "region-b".to_string(),
            buffered: vec![
                ShardingEnvelope::new("entity-1", "first"),
                ShardingEnvelope::new("entity-2", "second"),
            ],
        }
    );
    assert_eq!(
        runtime.route("shard-1", ShardingEnvelope::new("entity-3", "third")),
        RegionRoutePlan::Forward {
            shard: "shard-1".to_string(),
            region: "region-b".to_string(),
            message: ShardingEnvelope::new("entity-3", "third"),
        }
    );
}

#[test]
fn region_runtime_starts_local_shard_then_delivers_buffered_messages() {
    let mut runtime = ShardRegionRuntime::new("region-a", 10);
    assert!(matches!(
        runtime.route("shard-1", ShardingEnvelope::new("entity-1", "first")),
        RegionRoutePlan::Buffered { .. }
    ));

    let home = runtime.record_shard_home("shard-1", "region-a").unwrap();
    assert_eq!(
        home,
        ShardHomePlan::StartLocalShard {
            shard: "shard-1".to_string(),
            command: HostShard {
                shard_id: "shard-1".to_string(),
            },
        }
    );
    assert!(runtime.starting_shards().contains("shard-1"));

    let started = runtime.mark_shard_started("shard-1");
    assert_eq!(
        started.started,
        ShardStarted {
            shard_id: "shard-1".to_string(),
        }
    );
    assert_eq!(
        started.buffered,
        vec![ShardingEnvelope::new("entity-1", "first")]
    );
    assert!(runtime.local_shards().contains("shard-1"));
    assert_eq!(
        runtime.route("shard-1", ShardingEnvelope::new("entity-1", "second")),
        RegionRoutePlan::DeliverLocal {
            shard: "shard-1".to_string(),
            message: ShardingEnvelope::new("entity-1", "second"),
        }
    );
}

#[test]
fn region_actor_buffers_unknown_shard_and_requests_home_once() {
    let kit = kairo_testkit::ActorSystemTestKit::new("region-actor-buffer").unwrap();
    let region = kit
        .system()
        .spawn("region", ShardRegionActor::<String>::props("region-a", 10))
        .unwrap();
    let routes = kit
        .create_probe::<RegionRoutePlan<String>>("routes")
        .unwrap();
    let state = kit.create_probe::<ShardRegionSnapshot>("state").unwrap();

    region
        .tell(ShardRegionMsg::Route {
            shard: "shard-1".to_string(),
            message: ShardingEnvelope::new("entity-1", "first".to_string()),
            reply_to: routes.actor_ref(),
        })
        .unwrap();
    region
        .tell(ShardRegionMsg::Route {
            shard: "shard-1".to_string(),
            message: ShardingEnvelope::new("entity-1", "second".to_string()),
            reply_to: routes.actor_ref(),
        })
        .unwrap();

    assert_eq!(
        routes.expect_msg(Duration::from_millis(500)).unwrap(),
        RegionRoutePlan::Buffered {
            shard: "shard-1".to_string(),
            request: Some(GetShardHome {
                shard_id: "shard-1".to_string(),
            }),
        }
    );
    assert_eq!(
        routes.expect_msg(Duration::from_millis(500)).unwrap(),
        RegionRoutePlan::Buffered {
            shard: "shard-1".to_string(),
            request: None,
        }
    );

    region
        .tell(ShardRegionMsg::GetState {
            reply_to: state.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        state.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardRegionSnapshot {
            self_region: "region-a".to_string(),
            local_shards: BTreeSet::new(),
            starting_shards: BTreeSet::new(),
            handing_off_shards: BTreeSet::new(),
            total_buffered: 2,
        }
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn region_actor_records_local_home_and_delivers_after_start() {
    let kit = kairo_testkit::ActorSystemTestKit::new("region-actor-local-home").unwrap();
    let region = kit
        .system()
        .spawn("region", ShardRegionActor::<String>::props("region-a", 10))
        .unwrap();
    let routes = kit
        .create_probe::<RegionRoutePlan<String>>("routes")
        .unwrap();
    let homes = kit
        .create_probe::<Result<ShardHomePlan<String>, ShardingError>>("homes")
        .unwrap();
    let started = kit
        .create_probe::<ShardStartedPlan<String>>("started")
        .unwrap();

    region
        .tell(ShardRegionMsg::Route {
            shard: "shard-1".to_string(),
            message: ShardingEnvelope::new("entity-1", "first".to_string()),
            reply_to: routes.actor_ref(),
        })
        .unwrap();
    routes.expect_msg(Duration::from_millis(500)).unwrap();

    region
        .tell(ShardRegionMsg::RecordShardHome {
            shard: "shard-1".to_string(),
            region: "region-a".to_string(),
            reply_to: homes.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        homes
            .expect_msg(Duration::from_millis(500))
            .unwrap()
            .unwrap(),
        ShardHomePlan::StartLocalShard {
            shard: "shard-1".to_string(),
            command: HostShard {
                shard_id: "shard-1".to_string(),
            },
        }
    );

    region
        .tell(ShardRegionMsg::MarkShardStarted {
            shard: "shard-1".to_string(),
            reply_to: started.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        started.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardStartedPlan {
            started: ShardStarted {
                shard_id: "shard-1".to_string(),
            },
            buffered: vec![ShardingEnvelope::new("entity-1", "first".to_string())],
        }
    );

    region
        .tell(ShardRegionMsg::Route {
            shard: "shard-1".to_string(),
            message: ShardingEnvelope::new("entity-1", "second".to_string()),
            reply_to: routes.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        routes.expect_msg(Duration::from_millis(500)).unwrap(),
        RegionRoutePlan::DeliverLocal {
            shard: "shard-1".to_string(),
            message: ShardingEnvelope::new("entity-1", "second".to_string()),
        }
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn region_actor_handoff_drops_buffer_and_marks_handing_off() {
    let kit = kairo_testkit::ActorSystemTestKit::new("region-actor-handoff").unwrap();
    let region = kit
        .system()
        .spawn("region", ShardRegionActor::<String>::props("region-a", 10))
        .unwrap();
    let host = kit.create_probe::<HostShardPlan<String>>("host").unwrap();
    let started = kit
        .create_probe::<ShardStartedPlan<String>>("started")
        .unwrap();
    let begin = kit.create_probe::<BeginHandOffPlan>("begin").unwrap();
    let routes = kit
        .create_probe::<RegionRoutePlan<String>>("routes")
        .unwrap();
    let handoff = kit.create_probe::<HandOffPlan>("handoff").unwrap();
    let state = kit.create_probe::<ShardRegionSnapshot>("state").unwrap();

    region
        .tell(ShardRegionMsg::HostShard {
            shard: "shard-1".to_string(),
            reply_to: host.actor_ref(),
        })
        .unwrap();
    host.expect_msg(Duration::from_millis(500)).unwrap();
    region
        .tell(ShardRegionMsg::MarkShardStarted {
            shard: "shard-1".to_string(),
            reply_to: started.actor_ref(),
        })
        .unwrap();
    started.expect_msg(Duration::from_millis(500)).unwrap();

    region
        .tell(ShardRegionMsg::BeginHandOff {
            shard: "shard-1".to_string(),
            reply_to: begin.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        begin.expect_msg(Duration::from_millis(500)).unwrap(),
        BeginHandOffPlan::Ack {
            shard: "shard-1".to_string(),
            ack: crate::BeginHandOffAck {
                shard_id: "shard-1".to_string(),
            },
        }
    );

    region
        .tell(ShardRegionMsg::Route {
            shard: "shard-1".to_string(),
            message: ShardingEnvelope::new("entity-1", "buffered-after-begin".to_string()),
            reply_to: routes.actor_ref(),
        })
        .unwrap();
    routes.expect_msg(Duration::from_millis(500)).unwrap();

    region
        .tell(ShardRegionMsg::HandOff {
            shard: "shard-1".to_string(),
            reply_to: handoff.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        handoff.expect_msg(Duration::from_millis(500)).unwrap(),
        HandOffPlan::ForwardToLocalShard {
            shard: "shard-1".to_string(),
            command: HandOff {
                shard_id: "shard-1".to_string(),
            },
            dropped_buffered: 1,
        }
    );

    region
        .tell(ShardRegionMsg::GetState {
            reply_to: state.actor_ref(),
        })
        .unwrap();
    let snapshot = state.expect_msg(Duration::from_millis(500)).unwrap();
    assert_eq!(snapshot.total_buffered, 0);
    assert!(snapshot.handing_off_shards.contains("shard-1"));
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn region_runtime_host_shard_ignores_graceful_shutdown() {
    let mut runtime = ShardRegionRuntime::<&str>::new("region-a", 10);
    runtime.set_graceful_shutdown_in_progress(true);

    assert_eq!(
        runtime.host_shard("shard-1"),
        HostShardPlan::IgnoredGracefulShutdown {
            shard: "shard-1".to_string(),
        }
    );
    assert_eq!(runtime.region_for_shard(&"shard-1".to_string()), None);
}

#[test]
fn region_runtime_host_shard_marks_local_starting() {
    let mut runtime = ShardRegionRuntime::<&str>::new("region-a", 10);

    assert_eq!(
        runtime.host_shard("shard-1"),
        HostShardPlan::StartLocalShard {
            shard: "shard-1".to_string(),
            command: HostShard {
                shard_id: "shard-1".to_string(),
            },
        }
    );
    assert_eq!(
        runtime.region_for_shard(&"shard-1".to_string()),
        Some(&"region-a".to_string())
    );
    assert!(runtime.starting_shards().contains("shard-1"));
}

#[test]
fn region_runtime_rejects_inconsistent_remote_home_for_known_local_shard() {
    let mut runtime = ShardRegionRuntime::<&str>::new("region-a", 10);
    runtime.host_shard("shard-1");
    runtime.mark_shard_started("shard-1");

    assert_eq!(
        runtime
            .record_shard_home("shard-1", "region-b")
            .unwrap_err(),
        ShardingError::InconsistentShardHome {
            shard: "shard-1".to_string(),
            current_region: "region-a".to_string(),
            new_region: "region-b".to_string(),
        }
    );
    assert_eq!(
        runtime.region_for_shard(&"shard-1".to_string()),
        Some(&"region-a".to_string())
    );
    assert!(runtime.local_shards().contains("shard-1"));
}

#[test]
fn region_runtime_begin_handoff_removes_shard_home_and_acks() {
    let mut runtime = ShardRegionRuntime::<&str>::new("region-a", 10);
    runtime.host_shard("shard-1");
    runtime.mark_shard_started("shard-1");

    assert_eq!(
        runtime.begin_handoff("shard-1"),
        BeginHandOffPlan::Ack {
            shard: "shard-1".to_string(),
            ack: crate::BeginHandOffAck {
                shard_id: "shard-1".to_string(),
            },
        }
    );
    assert_eq!(runtime.region_for_shard(&"shard-1".to_string()), None);
    assert_eq!(
        runtime.route("shard-1", ShardingEnvelope::new("entity-1", "after-begin")),
        RegionRoutePlan::Buffered {
            shard: "shard-1".to_string(),
            request: Some(GetShardHome {
                shard_id: "shard-1".to_string(),
            }),
        }
    );
}

#[test]
fn region_runtime_begin_handoff_is_ignored_while_preparing_shutdown() {
    let mut runtime = ShardRegionRuntime::<&str>::new("region-a", 10);
    runtime.host_shard("shard-1");
    runtime.set_preparing_for_shutdown(true);

    assert_eq!(
        runtime.begin_handoff("shard-1"),
        BeginHandOffPlan::IgnoredPreparingForShutdown {
            shard: "shard-1".to_string(),
        }
    );
    assert_eq!(
        runtime.region_for_shard(&"shard-1".to_string()),
        Some(&"region-a".to_string())
    );
}

#[test]
fn region_runtime_handoff_drops_buffer_to_preserve_order_and_forwards_local_shard() {
    let mut runtime = ShardRegionRuntime::new("region-a", 10);
    runtime.host_shard("shard-1");
    runtime.mark_shard_started("shard-1");
    runtime.begin_handoff("shard-1");
    assert!(matches!(
        runtime.route(
            "shard-1",
            ShardingEnvelope::new("entity-1", "buffered-after-begin")
        ),
        RegionRoutePlan::Buffered { .. }
    ));

    assert_eq!(
        runtime.handoff("shard-1"),
        HandOffPlan::ForwardToLocalShard {
            shard: "shard-1".to_string(),
            command: HandOff {
                shard_id: "shard-1".to_string(),
            },
            dropped_buffered: 1,
        }
    );
    assert_eq!(runtime.buffered_count(&"shard-1".to_string()), 0);
    assert!(runtime.handing_off_shards().contains("shard-1"));
}

#[test]
fn region_runtime_handoff_replies_stopped_when_local_shard_is_absent() {
    let mut runtime = ShardRegionRuntime::new("region-a", 10);
    assert!(matches!(
        runtime.route("shard-1", ShardingEnvelope::new("entity-1", "buffered")),
        RegionRoutePlan::Buffered { .. }
    ));

    assert_eq!(
        runtime.handoff("shard-1"),
        HandOffPlan::ReplyShardStopped {
            shard: "shard-1".to_string(),
            stopped: ShardStopped {
                shard_id: "shard-1".to_string(),
            },
            dropped_buffered: 1,
        }
    );
}

#[test]
fn region_runtime_drops_empty_shard_and_full_buffer_messages() {
    let mut runtime = ShardRegionRuntime::new("region-a", 1);

    assert_eq!(
        runtime.route("", ShardingEnvelope::new("entity-1", "empty")),
        RegionRoutePlan::Dropped {
            shard: None,
            reason: RegionDropReason::EmptyShardId,
            message: ShardingEnvelope::new("entity-1", "empty"),
        }
    );
    assert!(matches!(
        runtime.route("shard-1", ShardingEnvelope::new("entity-1", "first")),
        RegionRoutePlan::Buffered { .. }
    ));
    assert_eq!(
        runtime.route("shard-2", ShardingEnvelope::new("entity-2", "second")),
        RegionRoutePlan::Dropped {
            shard: Some("shard-2".to_string()),
            reason: RegionDropReason::BufferFull,
            message: ShardingEnvelope::new("entity-2", "second"),
        }
    );
}

#[test]
fn shard_runtime_starts_entity_on_first_message_and_then_delivers_directly() {
    let mut runtime = ShardRuntime::new("shard-1", 10);

    let first = runtime.deliver(ShardingEnvelope::new("entity-1", "first"));
    let second = runtime.deliver(ShardingEnvelope::new("entity-1", "second"));

    match first {
        ShardDeliverPlan::StartEntity { delivery } => {
            assert_eq!(delivery.entity_id(), "entity-1");
            assert_eq!(delivery.message(), &"first");
        }
        other => panic!("unexpected plan: {other:?}"),
    }
    match second {
        ShardDeliverPlan::Deliver { delivery } => {
            assert_eq!(delivery.into_parts(), ("entity-1".to_string(), "second"));
        }
        other => panic!("unexpected plan: {other:?}"),
    }
    assert_eq!(
        runtime.entity_state(&"entity-1".to_string()),
        Some(ShardEntityState::Active)
    );
}

#[test]
fn shard_actor_starts_entity_then_delivers_directly() {
    let kit = kairo_testkit::ActorSystemTestKit::new("shard-actor-deliver").unwrap();
    let shard = kit
        .system()
        .spawn("shard", ShardActor::<String>::props("shard-1", 10))
        .unwrap();
    let deliveries = kit
        .create_probe::<ShardDeliverPlan<String>>("deliveries")
        .unwrap();
    let state = kit.create_probe::<ShardSnapshot>("state").unwrap();

    shard
        .tell(ShardMsg::Deliver {
            message: ShardingEnvelope::new("entity-1", "first".to_string()),
            reply_to: deliveries.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        deliveries.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardDeliverPlan::StartEntity {
            delivery: crate::EntityDelivery::new("entity-1", "first".to_string()),
        }
    );

    shard
        .tell(ShardMsg::Deliver {
            message: ShardingEnvelope::new("entity-1", "second".to_string()),
            reply_to: deliveries.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        deliveries.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardDeliverPlan::Deliver {
            delivery: crate::EntityDelivery::new("entity-1", "second".to_string()),
        }
    );

    shard
        .tell(ShardMsg::GetState {
            reply_to: state.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        state.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardSnapshot {
            shard_id: "shard-1".to_string(),
            active_entities: vec!["entity-1".to_string()],
            entity_count: 1,
            total_buffered: 0,
            handoff_in_progress: false,
        }
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn shard_actor_buffers_passivating_entity_and_restarts_on_termination() {
    let kit = kairo_testkit::ActorSystemTestKit::new("shard-actor-passivation").unwrap();
    let shard = kit
        .system()
        .spawn("shard", ShardActor::<String>::props("shard-1", 10))
        .unwrap();
    let deliveries = kit
        .create_probe::<ShardDeliverPlan<String>>("deliveries")
        .unwrap();
    let passivation = kit
        .create_probe::<crate::PassivatePlan<String>>("passivation")
        .unwrap();
    let termination = kit
        .create_probe::<crate::EntityTerminatedPlan<String>>("termination")
        .unwrap();

    shard
        .tell(ShardMsg::Deliver {
            message: ShardingEnvelope::new("entity-1", "first".to_string()),
            reply_to: deliveries.actor_ref(),
        })
        .unwrap();
    deliveries.expect_msg(Duration::from_millis(500)).unwrap();
    shard
        .tell(ShardMsg::Passivate {
            entity_id: "entity-1".to_string(),
            stop_message: "stop".to_string(),
            reply_to: passivation.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        passivation.expect_msg(Duration::from_millis(500)).unwrap(),
        crate::PassivatePlan::SendStop {
            entity_id: "entity-1".to_string(),
            stop_message: "stop".to_string(),
        }
    );

    for message in ["second", "third"] {
        shard
            .tell(ShardMsg::Deliver {
                message: ShardingEnvelope::new("entity-1", message.to_string()),
                reply_to: deliveries.actor_ref(),
            })
            .unwrap();
        assert_eq!(
            deliveries.expect_msg(Duration::from_millis(500)).unwrap(),
            ShardDeliverPlan::Buffered {
                entity_id: "entity-1".to_string(),
            }
        );
    }

    shard
        .tell(ShardMsg::EntityTerminated {
            entity_id: "entity-1".to_string(),
            reply_to: termination.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        termination.expect_msg(Duration::from_millis(500)).unwrap(),
        crate::EntityTerminatedPlan::Restart {
            buffered: vec![
                crate::EntityDelivery::new("entity-1", "second".to_string()),
                crate::EntityDelivery::new("entity-1", "third".to_string()),
            ],
        }
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn shard_actor_handoff_tracks_stopper_and_completion() {
    let kit = kairo_testkit::ActorSystemTestKit::new("shard-actor-handoff").unwrap();
    let shard = kit
        .system()
        .spawn("shard", ShardActor::<String>::props("shard-1", 10))
        .unwrap();
    let deliveries = kit
        .create_probe::<ShardDeliverPlan<String>>("deliveries")
        .unwrap();
    let handoff = kit
        .create_probe::<ShardHandOffPlan<String>>("handoff")
        .unwrap();
    let stopper = kit.create_probe::<bool>("stopper").unwrap();
    let state = kit.create_probe::<ShardSnapshot>("state").unwrap();

    for entity in ["entity-b", "entity-a"] {
        shard
            .tell(ShardMsg::Deliver {
                message: ShardingEnvelope::new(entity, "message".to_string()),
                reply_to: deliveries.actor_ref(),
            })
            .unwrap();
        deliveries.expect_msg(Duration::from_millis(500)).unwrap();
    }

    shard
        .tell(ShardMsg::HandOff {
            stop_message: "stop".to_string(),
            reply_to: handoff.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        handoff.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardHandOffPlan::StartEntityStopper {
            shard: "shard-1".to_string(),
            entities: vec!["entity-a".to_string(), "entity-b".to_string()],
            stop_message: "stop".to_string(),
        }
    );
    shard
        .tell(ShardMsg::GetState {
            reply_to: state.actor_ref(),
        })
        .unwrap();
    assert!(
        state
            .expect_msg(Duration::from_millis(500))
            .unwrap()
            .handoff_in_progress
    );

    shard
        .tell(ShardMsg::HandOffStopperTerminated {
            reply_to: stopper.actor_ref(),
        })
        .unwrap();
    assert!(stopper.expect_msg(Duration::from_millis(500)).unwrap());
    shard
        .tell(ShardMsg::GetState {
            reply_to: state.actor_ref(),
        })
        .unwrap();
    assert!(
        !state
            .expect_msg(Duration::from_millis(500))
            .unwrap()
            .handoff_in_progress
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn shard_runtime_drops_empty_entity_id() {
    let mut runtime = ShardRuntime::new("shard-1", 10);

    assert_eq!(
        runtime.deliver(ShardingEnvelope::new("", "message")),
        ShardDeliverPlan::Dropped {
            entity_id: None,
            reason: ShardDropReason::EmptyEntityId,
            message: "message",
        }
    );
    assert_eq!(runtime.entity_count(), 0);
}

#[test]
fn shard_runtime_passivates_active_entity_with_stop_message() {
    let mut runtime = ShardRuntime::new("shard-1", 10);
    assert!(matches!(
        runtime.deliver(ShardingEnvelope::new("entity-1", "message")),
        ShardDeliverPlan::StartEntity { .. }
    ));

    assert_eq!(
        runtime.passivate("entity-1", "stop"),
        crate::PassivatePlan::SendStop {
            entity_id: "entity-1".to_string(),
            stop_message: "stop",
        }
    );
    assert_eq!(
        runtime.entity_state(&"entity-1".to_string()),
        Some(ShardEntityState::Passivating)
    );
}

#[test]
fn shard_runtime_ignores_unknown_or_duplicate_passivation() {
    let mut runtime = ShardRuntime::new("shard-1", 10);

    assert_eq!(
        runtime.passivate("entity-1", "stop"),
        crate::PassivatePlan::Ignored {
            entity_id: "entity-1".to_string(),
            reason: crate::PassivateIgnoreReason::UnknownEntity,
        }
    );

    runtime.deliver(ShardingEnvelope::new("entity-1", "message"));
    runtime.passivate("entity-1", "stop");

    assert_eq!(
        runtime.passivate("entity-1", "stop-again"),
        crate::PassivatePlan::Ignored {
            entity_id: "entity-1".to_string(),
            reason: crate::PassivateIgnoreReason::AlreadyPassivating,
        }
    );
}

#[test]
fn shard_runtime_buffers_messages_while_entity_is_passivating() {
    let mut runtime = ShardRuntime::new("shard-1", 10);
    runtime.deliver(ShardingEnvelope::new("entity-1", "first"));
    runtime.passivate("entity-1", "stop");

    assert_eq!(
        runtime.deliver(ShardingEnvelope::new("entity-1", "second")),
        ShardDeliverPlan::Buffered {
            entity_id: "entity-1".to_string(),
        }
    );
    assert_eq!(runtime.buffered_count(&"entity-1".to_string()), 1);
}

#[test]
fn shard_runtime_drops_when_passivation_buffer_is_full() {
    let mut runtime = ShardRuntime::new("shard-1", 1);
    runtime.deliver(ShardingEnvelope::new("entity-1", "first"));
    runtime.passivate("entity-1", "stop");
    assert!(matches!(
        runtime.deliver(ShardingEnvelope::new("entity-1", "second")),
        ShardDeliverPlan::Buffered { .. }
    ));

    assert_eq!(
        runtime.deliver(ShardingEnvelope::new("entity-2", "third")),
        ShardDeliverPlan::StartEntity {
            delivery: crate::EntityDelivery::new("entity-2", "third"),
        }
    );
    runtime.passivate("entity-2", "stop");
    assert_eq!(
        runtime.deliver(ShardingEnvelope::new("entity-2", "fourth")),
        ShardDeliverPlan::Dropped {
            entity_id: Some("entity-2".to_string()),
            reason: ShardDropReason::BufferFull,
            message: "fourth",
        }
    );
}

#[test]
fn shard_runtime_removes_active_entity_on_unexpected_termination() {
    let mut runtime = ShardRuntime::new("shard-1", 10);
    runtime.deliver(ShardingEnvelope::new("entity-1", "first"));

    assert_eq!(
        runtime.entity_terminated("entity-1"),
        crate::EntityTerminatedPlan::Removed {
            entity_id: "entity-1".to_string(),
        }
    );
    assert_eq!(runtime.entity_state(&"entity-1".to_string()), None);
}

#[test]
fn shard_runtime_removes_passivated_entity_when_no_buffered_messages_exist() {
    let mut runtime = ShardRuntime::new("shard-1", 10);
    runtime.deliver(ShardingEnvelope::new("entity-1", "first"));
    runtime.passivate("entity-1", "stop");

    assert_eq!(
        runtime.entity_terminated("entity-1"),
        crate::EntityTerminatedPlan::Removed {
            entity_id: "entity-1".to_string(),
        }
    );
    assert_eq!(runtime.entity_state(&"entity-1".to_string()), None);
}

#[test]
fn shard_runtime_restarts_passivated_entity_when_buffered_messages_exist() {
    let mut runtime = ShardRuntime::new("shard-1", 10);
    runtime.deliver(ShardingEnvelope::new("entity-1", "first"));
    runtime.passivate("entity-1", "stop");
    runtime.deliver(ShardingEnvelope::new("entity-1", "second"));
    runtime.deliver(ShardingEnvelope::new("entity-1", "third"));

    let restarted = runtime.entity_terminated("entity-1");

    match restarted {
        crate::EntityTerminatedPlan::Restart { buffered } => {
            assert_eq!(buffered.len(), 2);
            assert_eq!(
                buffered
                    .into_iter()
                    .map(crate::EntityDelivery::into_parts)
                    .collect::<Vec<_>>(),
                vec![
                    ("entity-1".to_string(), "second"),
                    ("entity-1".to_string(), "third"),
                ]
            );
        }
        other => panic!("unexpected plan: {other:?}"),
    }
    assert_eq!(
        runtime.entity_state(&"entity-1".to_string()),
        Some(ShardEntityState::Active)
    );
    assert_eq!(runtime.buffered_count(&"entity-1".to_string()), 0);
}

#[test]
fn shard_runtime_ignores_unknown_entity_termination() {
    let mut runtime = ShardRuntime::<&str>::new("shard-1", 10);

    assert_eq!(
        runtime.entity_terminated("entity-1"),
        crate::EntityTerminatedPlan::IgnoredUnknown {
            entity_id: "entity-1".to_string(),
        }
    );
}

#[test]
fn shard_runtime_handoff_replies_stopped_when_no_entities_are_active() {
    let mut runtime = ShardRuntime::new("shard-1", 10);

    assert_eq!(
        runtime.handoff("stop"),
        ShardHandOffPlan::ReplyShardStopped {
            shard: "shard-1".to_string(),
            stopped: ShardStopped {
                shard_id: "shard-1".to_string(),
            },
        }
    );
}

#[test]
fn shard_runtime_handoff_starts_entity_stopper_for_active_entities() {
    let mut runtime = ShardRuntime::new("shard-1", 10);
    runtime.deliver(ShardingEnvelope::new("entity-b", "first"));
    runtime.deliver(ShardingEnvelope::new("entity-a", "first"));
    runtime.passivate("entity-b", "stop-b");

    assert_eq!(
        runtime.handoff("stop"),
        ShardHandOffPlan::StartEntityStopper {
            shard: "shard-1".to_string(),
            entities: vec!["entity-a".to_string(), "entity-b".to_string()],
            stop_message: "stop",
        }
    );
    assert_eq!(
        runtime.handoff("stop-again"),
        ShardHandOffPlan::AlreadyInProgress {
            shard: "shard-1".to_string(),
        }
    );
    assert!(runtime.handoff_stopper_terminated());
}

#[test]
fn shard_runtime_handoff_stops_immediately_while_preparing_shutdown() {
    let mut runtime = ShardRuntime::new("shard-1", 10);
    runtime.deliver(ShardingEnvelope::new("entity-1", "first"));
    runtime.set_preparing_for_shutdown(true);

    assert_eq!(
        runtime.handoff("stop"),
        ShardHandOffPlan::StopImmediately {
            shard: "shard-1".to_string(),
            entities: vec!["entity-1".to_string()],
            stop_message: "stop",
            stopped: ShardStopped {
                shard_id: "shard-1".to_string(),
            },
        }
    );
}

fn coordinator_runtime_with_regions<const N: usize>(regions: [&str; N]) -> CoordinatorRuntime {
    let mut state = CoordinatorState::new();
    for region in regions {
        state
            .apply(CoordinatorEvent::ShardRegionRegistered {
                region: region.to_string(),
            })
            .unwrap();
    }
    CoordinatorRuntime::new(state)
}

struct FixedRebalanceStrategy {
    shards: BTreeSet<String>,
}

impl FixedRebalanceStrategy {
    fn new<const N: usize>(shards: [&str; N]) -> Self {
        Self {
            shards: shards.into_iter().map(str::to_string).collect(),
        }
    }
}

impl ShardAllocationStrategy for FixedRebalanceStrategy {
    fn allocate_shard(
        &self,
        _requester: &String,
        _shard: &String,
        _current: &ShardAllocations,
    ) -> Result<String, ShardingError> {
        Err(ShardingError::NoShardRegions)
    }

    fn rebalance(
        &self,
        _current: &ShardAllocations,
        _in_progress: &BTreeSet<String>,
    ) -> Result<BTreeSet<String>, ShardingError> {
        Ok(self.shards.clone())
    }
}

struct RegionProbe {
    observed: mpsc::Sender<(String, &'static str)>,
}

impl Actor for RegionProbe {
    type Msg = ShardingEnvelope<&'static str>;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        let (entity_id, message) = msg.into_parts();
        self.observed
            .send((entity_id, message))
            .map_err(|error| ActorError::Message(error.to_string()))
    }
}
