use super::*;

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
fn coordinator_actor_dispatches_host_shard_on_new_allocation() {
    let kit = kairo_testkit::ActorSystemTestKit::new("coordinator-actor-host-dispatch").unwrap();
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
    let region = kit
        .create_probe::<ShardRegionMsg<String>>("region-a")
        .unwrap();
    let register = kit
        .create_probe::<Result<CoordinatorStateSnapshot, ShardingError>>("register")
        .unwrap();
    let home = kit
        .create_probe::<Result<GetShardHomePlan, ShardingError>>("home")
        .unwrap();

    coordinator
        .tell(ShardCoordinatorMsg::RegisterLocalRegion {
            target: HandoffRegionTarget::new("region-a", region.actor_ref()),
            reply_to: register.actor_ref(),
        })
        .unwrap();
    register
        .expect_msg(Duration::from_millis(500))
        .unwrap()
        .unwrap();

    coordinator
        .tell(ShardCoordinatorMsg::RequestShardHome {
            requester: "region-a".to_string(),
            shard: "new-shard".to_string(),
            reply_to: home.actor_ref(),
        })
        .unwrap();

    match region.expect_msg(Duration::from_millis(500)).unwrap() {
        ShardRegionMsg::HostShard { shard, .. } => assert_eq!(shard, "new-shard"),
        _ => panic!("expected HostShard dispatch"),
    }
    assert!(matches!(
        home.expect_msg(Duration::from_millis(500))
            .unwrap()
            .unwrap(),
        GetShardHomePlan::Allocated { host_region, .. } if host_region == "region-a"
    ));
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn coordinator_actor_observes_registered_local_region_stop() {
    let kit = kairo_testkit::ActorSystemTestKit::new("coordinator-region-watch").unwrap();
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
    let region = kit
        .create_probe::<ShardRegionMsg<String>>("region-a")
        .unwrap();
    let register = kit
        .create_probe::<Result<CoordinatorStateSnapshot, ShardingError>>("register")
        .unwrap();
    let home = kit
        .create_probe::<Result<GetShardHomePlan, ShardingError>>("home")
        .unwrap();
    let state = kit
        .create_probe::<CoordinatorStateSnapshot>("coordinator-state")
        .unwrap();

    coordinator
        .tell(ShardCoordinatorMsg::RegisterLocalRegion {
            target: HandoffRegionTarget::from_actor_ref("region-a", region.actor_ref()),
            reply_to: register.actor_ref(),
        })
        .unwrap();
    register
        .expect_msg(Duration::from_millis(500))
        .unwrap()
        .unwrap();

    coordinator
        .tell(ShardCoordinatorMsg::RequestShardHome {
            requester: "region-a".to_string(),
            shard: "shard-1".to_string(),
            reply_to: home.actor_ref(),
        })
        .unwrap();
    match region.expect_msg(Duration::from_millis(500)).unwrap() {
        ShardRegionMsg::HostShard { shard, .. } => assert_eq!(shard, "shard-1"),
        _ => panic!("expected HostShard dispatch"),
    }
    assert!(matches!(
        home.expect_msg(Duration::from_millis(500))
            .unwrap()
            .unwrap(),
        GetShardHomePlan::Allocated { host_region, .. } if host_region == "region-a"
    ));

    region.stop();
    region.expect_stopped(Duration::from_secs(1)).unwrap();
    kairo_testkit::await_assert(
        Duration::from_secs(1),
        Duration::from_millis(10),
        || -> Result<(), String> {
            coordinator
                .tell(ShardCoordinatorMsg::GetState {
                    reply_to: state.actor_ref(),
                })
                .map_err(|error| error.reason().to_string())?;
            let snapshot = state
                .expect_msg(Duration::from_millis(100))
                .map_err(|error| error.to_string())?;
            if snapshot.allocations.contains_key("region-a") {
                Err("coordinator still has watched region allocation".to_string())
            } else {
                Ok(())
            }
        },
    )
    .unwrap();
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn coordinator_actor_reallocates_remembered_shards_after_watched_region_stop() {
    let kit = kairo_testkit::ActorSystemTestKit::new("coordinator-remembered-region-stop").unwrap();
    let mut state = CoordinatorState::new().with_remember_entities(true);
    state.merge_remembered_shards(["shard-1".to_string()]);
    let coordinator = kit
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
    let region_a = kit
        .create_probe::<ShardRegionMsg<String>>("region-a")
        .unwrap();
    let region_b = kit
        .create_probe::<ShardRegionMsg<String>>("region-b")
        .unwrap();
    let register_a = kit
        .create_probe::<Result<CoordinatorStateSnapshot, ShardingError>>("register-a")
        .unwrap();
    let register_b = kit
        .create_probe::<Result<CoordinatorStateSnapshot, ShardingError>>("register-b")
        .unwrap();
    let state_probe = kit
        .create_probe::<CoordinatorStateSnapshot>("coordinator-state")
        .unwrap();

    coordinator
        .tell(ShardCoordinatorMsg::RegisterLocalRegion {
            target: HandoffRegionTarget::from_actor_ref("region-a", region_a.actor_ref()),
            reply_to: register_a.actor_ref(),
        })
        .unwrap();
    register_a
        .expect_msg(Duration::from_millis(500))
        .unwrap()
        .unwrap();
    match region_a.expect_msg(Duration::from_millis(500)).unwrap() {
        ShardRegionMsg::HostShard { shard, .. } => assert_eq!(shard, "shard-1"),
        _ => panic!("expected initial HostShard dispatch"),
    }

    coordinator
        .tell(ShardCoordinatorMsg::RegisterLocalRegion {
            target: HandoffRegionTarget::from_actor_ref("region-b", region_b.actor_ref()),
            reply_to: register_b.actor_ref(),
        })
        .unwrap();
    register_b
        .expect_msg(Duration::from_millis(500))
        .unwrap()
        .unwrap();

    region_a.stop();
    region_a.expect_stopped(Duration::from_secs(1)).unwrap();
    match region_b.expect_msg(Duration::from_millis(500)).unwrap() {
        ShardRegionMsg::HostShard { shard, .. } => assert_eq!(shard, "shard-1"),
        _ => panic!("expected reallocated HostShard dispatch"),
    }

    coordinator
        .tell(ShardCoordinatorMsg::GetState {
            reply_to: state_probe.actor_ref(),
        })
        .unwrap();
    let snapshot = state_probe.expect_msg(Duration::from_millis(500)).unwrap();
    assert!(
        snapshot.unallocated_shards.is_empty(),
        "remembered shard should be allocated after watched region stop"
    );
    assert!(
        !snapshot.allocations.contains_key("region-a"),
        "stopped region should be removed"
    );
    assert_eq!(
        snapshot.allocations.get("region-b"),
        Some(&vec!["shard-1".to_string()])
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn coordinator_actor_loads_remembered_shards_before_serving_requests() {
    let kit = kairo_testkit::ActorSystemTestKit::new("coordinator-actor-remember-load").unwrap();
    let coordinator = kit
        .system()
        .spawn(
            "coordinator",
            ShardCoordinatorActor::props_with_local_remember_store(
                CoordinatorState::new(),
                LeastShardAllocationStrategy::default(),
                RememberCoordinatorStoreState::with_shards(["remembered".to_string()]),
                Duration::from_millis(500),
                8,
            ),
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
    coordinator
        .tell(ShardCoordinatorMsg::RequestShardHome {
            requester: "region-a".to_string(),
            shard: "remembered".to_string(),
            reply_to: home.actor_ref(),
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
    assert_eq!(
        home.expect_msg(Duration::from_millis(500))
            .unwrap()
            .unwrap(),
        GetShardHomePlan::Allocated {
            event: CoordinatorEvent::ShardHomeAllocated {
                shard: "remembered".to_string(),
                region: "region-a".to_string(),
            },
            host_region: "region-a".to_string(),
            host_shard: HostShard {
                shard_id: "remembered".to_string(),
            },
        }
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn coordinator_actor_persists_allocated_shards_to_remember_store() {
    let kit = kairo_testkit::ActorSystemTestKit::new("coordinator-actor-remember-update").unwrap();
    let store = kit
        .system()
        .spawn(
            "store",
            RememberCoordinatorStoreActor::props(RememberCoordinatorStoreState::new()),
        )
        .unwrap();
    let coordinator = kit
        .system()
        .spawn(
            "coordinator",
            ShardCoordinatorActor::props_with_remember_store(
                CoordinatorState::new(),
                LeastShardAllocationStrategy::default(),
                store.clone(),
                Duration::from_millis(500),
                8,
            ),
        )
        .unwrap();
    let state = kit
        .create_probe::<Result<CoordinatorStateSnapshot, ShardingError>>("state")
        .unwrap();
    let home = kit
        .create_probe::<Result<GetShardHomePlan, ShardingError>>("home")
        .unwrap();
    let store_state = kit
        .create_probe::<RememberCoordinatorStoreSnapshot>("store-state")
        .unwrap();

    coordinator
        .tell(ShardCoordinatorMsg::ApplyEvent {
            event: CoordinatorEvent::ShardRegionRegistered {
                region: "region-a".to_string(),
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
            requester: "region-a".to_string(),
            shard: "new-shard".to_string(),
            reply_to: home.actor_ref(),
        })
        .unwrap();
    assert!(matches!(
        home.expect_msg(Duration::from_millis(500))
            .unwrap()
            .unwrap(),
        GetShardHomePlan::Allocated { .. }
    ));

    wait_for_remembered_coordinator_shard(
        &store,
        &store_state,
        "new-shard",
        "remember coordinator store should include new-shard",
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
fn coordinator_actor_retries_pending_home_after_cleared_rebalance() {
    let mut state = CoordinatorState::new();
    for region in ["region-a", "region-b"] {
        state
            .apply(CoordinatorEvent::ShardRegionRegistered {
                region: region.to_string(),
            })
            .unwrap();
    }
    state
        .apply(CoordinatorEvent::ShardHomeAllocated {
            shard: "s1".to_string(),
            region: "region-a".to_string(),
        })
        .unwrap();

    let kit =
        kairo_testkit::ActorSystemTestKit::new("coordinator-cleared-rebalance-retry").unwrap();
    let coordinator = kit
        .system()
        .spawn(
            "coordinator",
            ShardCoordinatorActor::props(
                state,
                RebalanceThenAllocateStrategy::new(["s1"], "region-b"),
            ),
        )
        .unwrap();
    let rebalance = kit
        .create_probe::<Result<RebalancePlan, ShardingError>>("rebalance")
        .unwrap();
    let home = kit
        .create_probe::<Result<GetShardHomePlan, ShardingError>>("home")
        .unwrap();
    let snapshot = kit
        .create_probe::<CoordinatorStateSnapshot>("snapshot")
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
        RebalancePlan::Started { ref shards } if shards.len() == 1 && shards[0].shard == "s1"
    ));

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
        .tell(ShardCoordinatorMsg::RegionStopped {
            region: "region-a".to_string(),
        })
        .unwrap();
    coordinator
        .tell(ShardCoordinatorMsg::HandoffWorkerDone(HandoffWorkerDone {
            shard: "s1".to_string(),
            ok: true,
        }))
        .unwrap();

    wait_for_coordinator_snapshot(
        &coordinator,
        &snapshot,
        "pending shard-home requester should be retried after source region termination",
        |state| {
            !state.rebalance_in_progress.contains_key("s1")
                && !state
                    .allocations
                    .get("region-a")
                    .is_some_and(|shards| shards.contains(&"s1".to_string()))
                && state
                    .allocations
                    .get("region-b")
                    .is_some_and(|shards| shards.contains(&"s1".to_string()))
        },
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn coordinator_actor_retries_all_pending_homes_after_cleared_rebalance() {
    let mut state = CoordinatorState::new();
    for region in ["region-a", "region-b"] {
        state
            .apply(CoordinatorEvent::ShardRegionRegistered {
                region: region.to_string(),
            })
            .unwrap();
    }
    state
        .apply(CoordinatorEvent::ShardHomeAllocated {
            shard: "s1".to_string(),
            region: "region-a".to_string(),
        })
        .unwrap();

    let kit =
        kairo_testkit::ActorSystemTestKit::new("coordinator-cleared-rebalance-retry-all").unwrap();
    let coordinator = kit
        .system()
        .spawn(
            "coordinator",
            ShardCoordinatorActor::props(state, RequesterAllocationStrategy::new(["s1"])),
        )
        .unwrap();
    let rebalance = kit
        .create_probe::<Result<RebalancePlan, ShardingError>>("rebalance")
        .unwrap();
    let home = kit
        .create_probe::<Result<GetShardHomePlan, ShardingError>>("home")
        .unwrap();
    let snapshot = kit
        .create_probe::<CoordinatorStateSnapshot>("snapshot")
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
        RebalancePlan::Started { ref shards } if shards.len() == 1 && shards[0].shard == "s1"
    ));

    for requester in ["region-a", "region-b"] {
        coordinator
            .tell(ShardCoordinatorMsg::RequestShardHome {
                requester: requester.to_string(),
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
                requester: requester.to_string(),
            }
        );
    }

    coordinator
        .tell(ShardCoordinatorMsg::RegionStopped {
            region: "region-a".to_string(),
        })
        .unwrap();
    coordinator
        .tell(ShardCoordinatorMsg::HandoffWorkerDone(HandoffWorkerDone {
            shard: "s1".to_string(),
            ok: true,
        }))
        .unwrap();

    wait_for_coordinator_snapshot(
        &coordinator,
        &snapshot,
        "all pending shard-home requesters should be retried after source region termination",
        |state| {
            !state.rebalance_in_progress.contains_key("s1")
                && !state
                    .allocations
                    .get("region-a")
                    .is_some_and(|shards| shards.contains(&"s1".to_string()))
                && state
                    .allocations
                    .get("region-b")
                    .is_some_and(|shards| shards.contains(&"s1".to_string()))
        },
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn coordinator_actor_retries_pending_home_after_timed_out_rebalance() {
    let mut state = CoordinatorState::new();
    for region in ["region-a", "region-b"] {
        state
            .apply(CoordinatorEvent::ShardRegionRegistered {
                region: region.to_string(),
            })
            .unwrap();
    }
    state
        .apply(CoordinatorEvent::ShardHomeAllocated {
            shard: "s1".to_string(),
            region: "region-a".to_string(),
        })
        .unwrap();

    let kit =
        kairo_testkit::ActorSystemTestKit::new("coordinator-timed-out-rebalance-retry").unwrap();
    let coordinator = kit
        .system()
        .spawn(
            "coordinator",
            ShardCoordinatorActor::props(
                state,
                RebalanceThenAllocateStrategy::new(["s1"], "region-b"),
            ),
        )
        .unwrap();
    let rebalance = kit
        .create_probe::<Result<RebalancePlan, ShardingError>>("rebalance")
        .unwrap();
    let home = kit
        .create_probe::<Result<GetShardHomePlan, ShardingError>>("home")
        .unwrap();
    let snapshot = kit
        .create_probe::<CoordinatorStateSnapshot>("snapshot")
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
        RebalancePlan::Started { ref shards } if shards.len() == 1 && shards[0].shard == "s1"
    ));

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
        .tell(ShardCoordinatorMsg::RegionStopped {
            region: "region-a".to_string(),
        })
        .unwrap();
    coordinator
        .tell(ShardCoordinatorMsg::HandoffWorkerDone(HandoffWorkerDone {
            shard: "s1".to_string(),
            ok: false,
        }))
        .unwrap();

    wait_for_coordinator_snapshot(
        &coordinator,
        &snapshot,
        "pending shard-home requester should be retried after timed-out rebalance",
        |state| {
            !state.rebalance_in_progress.contains_key("s1")
                && !state
                    .allocations
                    .get("region-a")
                    .is_some_and(|shards| shards.contains(&"s1".to_string()))
                && state
                    .allocations
                    .get("region-b")
                    .is_some_and(|shards| shards.contains(&"s1".to_string()))
        },
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn coordinator_actor_rebalance_tick_uses_allocation_strategy() {
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

    let kit = kairo_testkit::ActorSystemTestKit::new("coordinator-actor-rebalance-tick").unwrap();
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

    coordinator
        .tell(ShardCoordinatorMsg::RebalanceTick {
            reply_to: Some(rebalance.actor_ref()),
        })
        .unwrap();

    assert!(matches!(
        rebalance
            .expect_msg(Duration::from_millis(500))
            .unwrap()
            .unwrap(),
        RebalancePlan::Started { ref shards } if shards.len() == 1 && shards[0].shard == "s1"
    ));
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn coordinator_actor_rebalance_timer_starts_and_cancels_with_shutdown_preparation() {
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

    let (kit, time) =
        kairo_testkit::ActorSystemTestKit::with_manual_time("coordinator-rebalance-timer").unwrap();
    let coordinator = kit
        .system()
        .spawn(
            "coordinator",
            ShardCoordinatorActor::props_with_rebalance_interval(
                state,
                FixedRebalanceStrategy::new(["s1"]),
                Duration::from_secs(1),
            ),
        )
        .unwrap();
    let snapshot = kit
        .create_probe::<CoordinatorStateSnapshot>("snapshot")
        .unwrap();

    coordinator
        .tell(ShardCoordinatorMsg::SetPreparingForShutdown { preparing: true })
        .unwrap();
    time.advance(Duration::from_secs(1));
    coordinator
        .tell(ShardCoordinatorMsg::GetState {
            reply_to: snapshot.actor_ref(),
        })
        .unwrap();
    assert!(
        snapshot
            .expect_msg(Duration::from_millis(500))
            .unwrap()
            .rebalance_in_progress
            .is_empty()
    );

    coordinator
        .tell(ShardCoordinatorMsg::SetPreparingForShutdown { preparing: false })
        .unwrap();
    coordinator
        .tell(ShardCoordinatorMsg::GetState {
            reply_to: snapshot.actor_ref(),
        })
        .unwrap();
    assert!(
        snapshot
            .expect_msg(Duration::from_millis(500))
            .unwrap()
            .rebalance_in_progress
            .is_empty()
    );
    time.advance(Duration::from_secs(1));
    coordinator
        .tell(ShardCoordinatorMsg::GetState {
            reply_to: snapshot.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        snapshot
            .expect_msg(Duration::from_millis(500))
            .unwrap()
            .rebalance_in_progress
            .get("s1"),
        Some(&Vec::<String>::new())
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn coordinator_actor_allocates_remembered_shards_after_local_region_registration() {
    let kit = kairo_testkit::ActorSystemTestKit::new("remembered-registration-allocation").unwrap();
    let mut state = CoordinatorState::new().with_remember_entities(true);
    state.merge_remembered_shards(["shard-1".to_string()]);
    let coordinator = kit
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
    let region = kit
        .system()
        .spawn(
            "region-a",
            ShardRegionActor::<String>::props_with_local_shards_and_registration(
                "region-a",
                10,
                10,
                coordinator.clone(),
                Duration::from_millis(20),
            ),
        )
        .unwrap();
    let coordinator_state = kit
        .create_probe::<CoordinatorStateSnapshot>("coordinator-state")
        .unwrap();
    let region_state = kit
        .create_probe::<ShardRegionSnapshot>("region-state")
        .unwrap();

    wait_for_coordinator_snapshot(
        &coordinator,
        &coordinator_state,
        "remembered shard should be allocated when region registers",
        remembered_shard_allocated,
    );

    wait_for_region_snapshot(
        &region,
        &region_state,
        "allocated remembered shard should be hosted on registered local region",
        |snapshot| snapshot.local_shards.contains("shard-1"),
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn coordinator_actor_allocates_remembered_shard_to_remember_store_region() {
    let kit = kairo_testkit::ActorSystemTestKit::new("remembered-store-registration").unwrap();
    let mut state = CoordinatorState::new().with_remember_entities(true);
    state.merge_remembered_shards(["shard-1".to_string()]);
    let coordinator = kit
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
    let region = kit
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
    let coordinator_state = kit
        .create_probe::<CoordinatorStateSnapshot>("coordinator-state")
        .unwrap();
    let region_state = kit
        .create_probe::<ShardRegionSnapshot>("region-state")
        .unwrap();

    wait_for_coordinator_snapshot(
        &coordinator,
        &coordinator_state,
        "remembered shard should be allocated to remember-store region",
        remembered_shard_allocated,
    );

    wait_for_region_snapshot(
        &region,
        &region_state,
        "remember-store region should host allocated remembered shard",
        |snapshot| snapshot.local_shards.contains("shard-1"),
    );
    let local_shard = wait_for_local_shard(&kit, &region, "shard-1");
    let shard_state = kit.create_probe::<ShardSnapshot>("shard-state").unwrap();
    wait_for_shard_snapshot(
        &local_shard,
        &shard_state,
        "remember-store shard should load remembered entity after coordinator allocation",
        |snapshot| snapshot.active_entities == vec!["entity-1".to_string()],
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn coordinator_actor_loads_remembered_shard_and_hosts_remember_store_region() {
    let kit = kairo_testkit::ActorSystemTestKit::new("remembered-store-load-registration").unwrap();
    let coordinator = kit
        .system()
        .spawn(
            "coordinator",
            Props::new(|| {
                ShardCoordinatorActor::with_local_remember_store_and_handoff(
                    CoordinatorState::new(),
                    LeastShardAllocationStrategy::default(),
                    RememberCoordinatorStoreState::with_shards(["shard-1".to_string()]),
                    Duration::from_millis(500),
                    "stop".to_string(),
                    Duration::from_millis(500),
                    HandoffTransport::new(),
                )
            })
            .with_stash_capacity(8),
        )
        .unwrap();
    let region = kit
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
    let coordinator_state = kit
        .create_probe::<CoordinatorStateSnapshot>("coordinator-state")
        .unwrap();
    let region_state = kit
        .create_probe::<ShardRegionSnapshot>("region-state")
        .unwrap();

    wait_for_coordinator_snapshot(
        &coordinator,
        &coordinator_state,
        "loaded remembered shard should be allocated to registered region",
        remembered_shard_allocated,
    );

    wait_for_region_snapshot(
        &region,
        &region_state,
        "region should host remembered shard allocated from coordinator store load",
        |snapshot| snapshot.local_shards.contains("shard-1"),
    );
    let local_shard = wait_for_local_shard(&kit, &region, "shard-1");
    let shard_state = kit.create_probe::<ShardSnapshot>("shard-state").unwrap();
    wait_for_shard_snapshot(
        &local_shard,
        &shard_state,
        "remember-store shard should recover entity after coordinator store load",
        |snapshot| snapshot.active_entities == vec!["entity-1".to_string()],
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn coordinator_actor_loads_shared_remembered_shard_and_hosts_shared_store_region() {
    let kit =
        kairo_testkit::ActorSystemTestKit::new("shared-remember-store-load-registration").unwrap();
    let coordinator_store = kit
        .system()
        .spawn(
            "coordinator-store",
            RememberCoordinatorStoreActor::props(RememberCoordinatorStoreState::with_shards([
                "shard-1".to_string(),
            ])),
        )
        .unwrap();
    let shard_store = kit
        .system()
        .spawn(
            "shard-store",
            RememberShardStoreActor::props(RememberShardStoreState::with_entities(
                "orders",
                "shard-1",
                ["entity-1".to_string()],
            )),
        )
        .unwrap();
    let coordinator = kit
        .system()
        .spawn(
            "coordinator",
            Props::new(move || {
                ShardCoordinatorActor::with_remember_store_and_handoff(
                    CoordinatorState::new(),
                    LeastShardAllocationStrategy::default(),
                    coordinator_store.clone(),
                    Duration::from_millis(500),
                    "stop".to_string(),
                    Duration::from_millis(500),
                    HandoffTransport::new(),
                )
            })
            .with_stash_capacity(8),
        )
        .unwrap();
    let region = kit
        .system()
        .spawn(
            "region-a",
            ShardRegionActor::<String>::props_with_remember_store_shards_and_registration(
                "region-a",
                10,
                10,
                BTreeMap::from([("shard-1".to_string(), shard_store)]),
                Duration::from_millis(500),
                RegionRegistrationConfig::new(coordinator.clone(), Duration::from_millis(20)),
            ),
        )
        .unwrap();
    let coordinator_state = kit
        .create_probe::<CoordinatorStateSnapshot>("coordinator-state")
        .unwrap();
    let region_state = kit
        .create_probe::<ShardRegionSnapshot>("region-state")
        .unwrap();

    wait_for_coordinator_snapshot(
        &coordinator,
        &coordinator_state,
        "shared remembered shard should be allocated to registered region",
        remembered_shard_allocated,
    );

    wait_for_region_snapshot(
        &region,
        &region_state,
        "region should host shard allocated from shared coordinator store",
        |snapshot| snapshot.local_shards.contains("shard-1"),
    );
    let local_shard = wait_for_local_shard(&kit, &region, "shard-1");
    let shard_state = kit.create_probe::<ShardSnapshot>("shard-state").unwrap();
    wait_for_shard_snapshot(
        &local_shard,
        &shard_state,
        "shared remember-store shard should recover entity after coordinator store load",
        |snapshot| snapshot.active_entities == vec!["entity-1".to_string()],
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

struct RequesterAllocationStrategy {
    rebalance_shards: BTreeSet<String>,
}

impl RequesterAllocationStrategy {
    fn new<const N: usize>(rebalance_shards: [&str; N]) -> Self {
        Self {
            rebalance_shards: rebalance_shards.into_iter().map(str::to_string).collect(),
        }
    }
}

impl ShardAllocationStrategy for RequesterAllocationStrategy {
    fn allocate_shard(
        &self,
        requester: &String,
        _shard: &String,
        _current: &ShardAllocations,
    ) -> Result<String, ShardingError> {
        Ok(requester.clone())
    }

    fn rebalance(
        &self,
        _current: &ShardAllocations,
        _in_progress: &BTreeSet<String>,
    ) -> Result<BTreeSet<String>, ShardingError> {
        Ok(self.rebalance_shards.clone())
    }
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

fn wait_for_remembered_coordinator_shard(
    store: &ActorRef<RememberCoordinatorStoreMsg>,
    state: &kairo_testkit::TestProbe<RememberCoordinatorStoreSnapshot>,
    shard: &str,
    description: &str,
) -> RememberCoordinatorStoreSnapshot {
    kairo_testkit::await_assert(
        polling_timeout(),
        Duration::from_millis(10),
        || -> Result<RememberCoordinatorStoreSnapshot, String> {
            store
                .tell(RememberCoordinatorStoreMsg::GetState {
                    reply_to: state.actor_ref(),
                })
                .map_err(|error| error.to_string())?;
            let snapshot = state
                .expect_msg(Duration::from_millis(500))
                .map_err(|error| error.to_string())?;
            if snapshot.shards.contains(shard) {
                Ok(snapshot)
            } else {
                Err(format!("{description}; last snapshot: {snapshot:?}"))
            }
        },
    )
    .unwrap()
}

fn wait_for_coordinator_snapshot<M>(
    coordinator: &ActorRef<ShardCoordinatorMsg<M>>,
    state: &kairo_testkit::TestProbe<CoordinatorStateSnapshot>,
    description: &str,
    mut matches: impl FnMut(&CoordinatorStateSnapshot) -> bool,
) -> CoordinatorStateSnapshot
where
    M: Clone + Send + 'static,
{
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

fn wait_for_local_shard(
    kit: &kairo_testkit::ActorSystemTestKit,
    region: &ActorRef<ShardRegionMsg<String>>,
    shard: &str,
) -> ActorRef<ShardMsg<String>> {
    let reply = kit
        .create_probe::<Option<ActorRef<ShardMsg<String>>>>("local-shard")
        .unwrap();
    kairo_testkit::await_assert(
        polling_timeout(),
        Duration::from_millis(10),
        || -> Result<ActorRef<ShardMsg<String>>, String> {
            region
                .tell(ShardRegionMsg::GetLocalShard {
                    shard: shard.to_string(),
                    reply_to: reply.actor_ref(),
                })
                .map_err(|error| error.to_string())?;
            match reply.expect_msg(Duration::from_millis(500)) {
                Ok(Some(shard_ref)) => Ok(shard_ref),
                Ok(None) => Err(format!("local shard `{shard}` is not available yet")),
                Err(error) => Err(format!(
                    "timed out waiting for local shard `{shard}` response: {error}"
                )),
            }
        },
    )
    .unwrap()
}

fn wait_for_shard_snapshot(
    shard: &ActorRef<ShardMsg<String>>,
    state: &kairo_testkit::TestProbe<ShardSnapshot>,
    description: &str,
    mut matches: impl FnMut(&ShardSnapshot) -> bool,
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
            if matches(&snapshot) {
                Ok(snapshot)
            } else {
                Err(format!("{description}; last snapshot: {snapshot:?}"))
            }
        },
    )
    .unwrap()
}
