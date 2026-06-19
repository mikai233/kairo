use super::*;

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
fn coordinator_runtime_reports_remembered_shard_home_requests() {
    let mut runtime = CoordinatorRuntime::new(CoordinatorState::new().with_remember_entities(true));
    runtime.merge_remembered_shards(["shard-1".to_string(), "shard-2".to_string()]);

    assert_eq!(
        runtime.remembered_shard_home_requests(),
        vec![
            GetShardHome {
                shard_id: "shard-1".to_string(),
            },
            GetShardHome {
                shard_id: "shard-2".to_string(),
            },
        ]
    );
}

#[test]
fn coordinator_runtime_allocates_remembered_shard_homes() {
    let strategy = LeastShardAllocationStrategy::default();
    let mut runtime = CoordinatorRuntime::new(CoordinatorState::new().with_remember_entities(true));
    runtime
        .apply_event(CoordinatorEvent::ShardRegionRegistered {
            region: "region-a".to_string(),
        })
        .unwrap();
    runtime.merge_remembered_shards(["shard-1".to_string()]);

    let plans = runtime
        .allocate_remembered_shard_homes("coordinator", &strategy)
        .unwrap();

    assert_eq!(
        plans,
        vec![GetShardHomePlan::Allocated {
            event: CoordinatorEvent::ShardHomeAllocated {
                shard: "shard-1".to_string(),
                region: "region-a".to_string(),
            },
            host_region: "region-a".to_string(),
            host_shard: HostShard {
                shard_id: "shard-1".to_string(),
            },
        }]
    );
    assert!(runtime.state().unallocated_shards().is_empty());
    assert_eq!(
        runtime.state().shard_home(&"shard-1".to_string()),
        Some(&"region-a".to_string())
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
fn coordinator_runtime_skips_rebalance_when_regions_are_unavailable() {
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
    runtime.mark_region_unavailable("region-b");

    assert_eq!(
        runtime.plan_rebalance(&strategy).unwrap(),
        RebalancePlan::Skipped {
            reason: RebalanceSkipReason::RegionsUnavailable {
                regions: BTreeSet::from(["region-b".to_string()]),
            },
        }
    );
    assert!(runtime.rebalance_in_progress().is_empty());
}

#[test]
fn coordinator_runtime_rebalances_after_unavailable_region_heals() {
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
    runtime.mark_region_unavailable("region-b");
    assert!(matches!(
        runtime.plan_rebalance(&strategy).unwrap(),
        RebalancePlan::Skipped {
            reason: RebalanceSkipReason::RegionsUnavailable { .. },
        }
    ));

    assert_eq!(
        runtime
            .request_shard_home("region-b", "1", &strategy)
            .unwrap(),
        GetShardHomePlan::Reply {
            shard: "1".to_string(),
            region: "region-a".to_string(),
        }
    );
    runtime.unmark_region_unavailable(&"region-b".to_string());

    assert_eq!(
        runtime.plan_rebalance(&strategy).unwrap(),
        RebalancePlan::Started {
            shards: vec![crate::ShardRebalancePlan {
                shard: "1".to_string(),
                from_region: "region-a".to_string(),
                participants: BTreeSet::from(["region-a".to_string(), "region-b".to_string()]),
                begin_handoff: crate::BeginHandOff {
                    shard_id: "1".to_string(),
                },
            }],
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
