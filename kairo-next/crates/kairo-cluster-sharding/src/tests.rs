use std::collections::BTreeSet;
use std::sync::mpsc;
use std::time::Duration;

use kairo_actor::{Actor, ActorError, ActorResult, ActorSystem, Context, Props};

use crate::{
    CoordinatorEvent, CoordinatorRuntime, CoordinatorState, EntityRef, GetShardHomeIgnoreReason,
    GetShardHomePlan, LeastShardAllocationStrategy, ShardAllocationStrategy, ShardAllocations,
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
