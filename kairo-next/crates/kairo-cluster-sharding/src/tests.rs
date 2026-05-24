use std::collections::BTreeSet;
use std::sync::mpsc;
use std::time::Duration;

use kairo_actor::{Actor, ActorError, ActorResult, ActorSystem, Context, Props};

use crate::{
    BeginHandOffPlan, CoordinatorEvent, CoordinatorRuntime, CoordinatorState, EntityRef,
    GetShardHome, GetShardHomeIgnoreReason, GetShardHomePlan, HandOff, HandOffPlan, HostShard,
    HostShardPlan, LeastShardAllocationStrategy, RegionDropReason, RegionRoutePlan,
    ShardAllocationStrategy, ShardAllocations, ShardDeliverPlan, ShardDropReason, ShardEntityState,
    ShardHandOffPlan, ShardHomePlan, ShardRegionRuntime, ShardRuntime, ShardStarted, ShardStopped,
    ShardingEnvelope, ShardingError, default_shard_id_for, shard_id_for, stable_hash_entity_id,
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
