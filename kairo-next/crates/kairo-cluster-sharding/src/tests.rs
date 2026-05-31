use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::mpsc;
use std::time::Duration;

use bytes::Bytes;
use kairo_actor::{
    Actor, ActorError, ActorRef, ActorResult, ActorSystem, Address, Context, Props, Recipient,
};
use kairo_cluster::{
    Cluster, ClusterEventPublisher, ClusterEventPublisherMsg, CurrentClusterState, Gossip, Member,
    MemberStatus, UniqueAddress,
};
use kairo_distributed_data::{GSet, ORSet, ReplicaId, ReplicatorActor};
use kairo_serialization::{
    ActorRefWireData, MessageCodec, Registry, RemoteEnvelope, RemoteMessage, SerializationRegistry,
    WireReader, WireWriter,
};

use crate::{
    BeginHandOffPlan, CoordinatorDiscoverySettings, CoordinatorEvent, CoordinatorRuntime,
    CoordinatorState, CoordinatorStateSnapshot, DEFAULT_SHARD_COUNT, EntityActorFactory,
    EntityDelivery, EntityRef, EntityShardActor, GetShardHome, GetShardHomeIgnoreReason,
    GetShardHomePlan, GracefulShutdownReq, HandOff, HandOffPlan, HandoffDeliveryFailure,
    HandoffDeliveryTarget, HandoffRegionTarget, HandoffTransport, HandoffWorkerActor,
    HandoffWorkerDone, HandoffWorkerMsg, HostShard, HostShardPlan, LeastShardAllocationStrategy,
    PassivatePlan, RebalanceCompletionPlan, RebalancePlan, RebalanceSkipReason,
    RegionBufferedReplayPlan, RegionCoordinatorDiscoveryConfig, RegionDropReason,
    RegionLocalHandOffCompletionPlan, RegionLocalHandOffPlan, RegionLocalRoutePlan,
    RegionRegistrationConfig, RegionRegistrationStatus, RegionRemoteCoordinatorTransport,
    RegionRouteDelivery, RegionRoutePlan, RegionRouteTarget, RegionRouteTransport,
    RegionShutdownPlan, RegionStopped, Register, RememberCoordinatorDDataStoreActor,
    RememberCoordinatorDDataStoreMsg, RememberCoordinatorDDataStoreSnapshot,
    RememberCoordinatorStoreActor, RememberCoordinatorStoreMsg, RememberCoordinatorStoreSnapshot,
    RememberCoordinatorStoreState, RememberShardDDataStoreActor, RememberShardDDataStoreMsg,
    RememberShardDDataStoreSnapshot, RememberShardStoreActor, RememberShardStoreMsg,
    RememberShardStoreSnapshot, RememberShardStoreState, RememberShardUpdate,
    RememberUpdateDonePlan, RememberedEntities, RememberedEntitiesPlan, ShardActor,
    ShardAllocationStrategy, ShardAllocations, ShardCoordinatorActor, ShardCoordinatorBootstrap,
    ShardCoordinatorMsg, ShardCoordinatorRemoteHome, ShardCoordinatorRemoteRegistrationAck,
    ShardCoordinatorRemoteTarget, ShardDeliverPlan, ShardDropReason, ShardEntityState,
    ShardHandOffPlan, ShardHomePlan, ShardMsg, ShardRebalancePlan, ShardRegionActor,
    ShardRegionDiscoverySubscriber, ShardRegionDiscoverySubscriberMsg,
    ShardRegionDiscoverySubscriberSnapshot, ShardRegionMsg, ShardRegionRemoteInbound,
    ShardRegionRemoteOutbound, ShardRegionRuntime, ShardRegionSnapshot, ShardRuntime,
    ShardSnapshot, ShardStarted, ShardStartedPlan, ShardStopped, ShardingEnvelope,
    ShardingEnvelopeRouter, ShardingError, default_shard_id_for, register_sharding_protocol_codecs,
    remember_coordinator_shards_key, remember_entity_key_index, remember_entity_key_index_for,
    remember_entity_shard_key, remember_entity_shard_replicator_key, shard_id_for,
    stable_hash_entity_id,
};

mod allocation;
mod coordinator_runtime;
mod coordinator_state;
mod entity_routing;
mod region_runtime;
mod remember;
mod shard_runtime;

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

    let mut persisted = false;
    for _ in 0..20 {
        store
            .tell(RememberCoordinatorStoreMsg::GetState {
                reply_to: store_state.actor_ref(),
            })
            .unwrap();
        persisted = store_state
            .expect_msg(Duration::from_millis(500))
            .unwrap()
            .shards
            .contains("new-shard");
        if persisted {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        persisted,
        "remember coordinator store should include new-shard"
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
            registration_status: RegionRegistrationStatus::Disabled,
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
fn region_actor_with_local_shards_spawns_child_on_host_shard() {
    let kit = kairo_testkit::ActorSystemTestKit::new("region-actor-local-shard-child").unwrap();
    let region = kit
        .system()
        .spawn(
            "region",
            ShardRegionActor::<String>::props_with_local_shards("region-a", 10, 10),
        )
        .unwrap();
    let host = kit.create_probe::<HostShardPlan<String>>("host").unwrap();
    let local_shard = kit
        .create_probe::<Option<kairo_actor::ActorRef<ShardMsg<String>>>>("local-shard")
        .unwrap();

    region
        .tell(ShardRegionMsg::HostShard {
            shard: "shard-1".to_string(),
            reply_to: host.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        host.expect_msg(Duration::from_millis(500)).unwrap(),
        HostShardPlan::AlreadyStarted {
            shard: "shard-1".to_string(),
            started: ShardStarted {
                shard_id: "shard-1".to_string(),
            },
            buffered: Vec::new(),
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
        .unwrap();
    assert_eq!(shard.path().name(), Some("shard-73686172642d31"));
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn region_actor_spawns_store_backed_shard_and_recovers_entities() {
    let kit =
        kairo_testkit::ActorSystemTestKit::new("region-actor-local-remember-shard-child").unwrap();
    let region = kit
        .system()
        .spawn(
            "region",
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
    let local_shard = kit
        .create_probe::<Option<kairo_actor::ActorRef<ShardMsg<String>>>>("local-shard")
        .unwrap();
    let deliveries = kit
        .create_probe::<ShardDeliverPlan<String>>("deliveries")
        .unwrap();

    region
        .tell(ShardRegionMsg::HostShard {
            shard: "shard-1".to_string(),
            reply_to: host.actor_ref(),
        })
        .unwrap();
    assert!(matches!(
        host.expect_msg(Duration::from_millis(500)).unwrap(),
        HostShardPlan::AlreadyStarted { .. }
    ));
    region
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
            message: ShardingEnvelope::new("entity-1", "loaded".to_string()),
            reply_to: deliveries.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        deliveries.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardDeliverPlan::Deliver {
            delivery: crate::EntityDelivery::new("entity-1", "loaded".to_string()),
        }
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn region_actor_routes_to_spawned_local_shard_child() {
    let kit = kairo_testkit::ActorSystemTestKit::new("region-actor-local-route-child").unwrap();
    let region = kit
        .system()
        .spawn(
            "region",
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
    let routes = kit
        .create_probe::<RegionLocalRoutePlan<String>>("routes")
        .unwrap();
    let deliveries = kit
        .create_probe::<ShardDeliverPlan<String>>("deliveries")
        .unwrap();

    region
        .tell(ShardRegionMsg::HostShard {
            shard: "shard-1".to_string(),
            reply_to: host.actor_ref(),
        })
        .unwrap();
    host.expect_msg(Duration::from_millis(500)).unwrap();

    region
        .tell(ShardRegionMsg::RouteToLocalShard {
            shard: "shard-1".to_string(),
            message: ShardingEnvelope::new("entity-1", "loaded".to_string()),
            route_reply_to: routes.actor_ref(),
            delivery_reply_to: deliveries.actor_ref(),
        })
        .unwrap();

    assert_eq!(
        routes.expect_msg(Duration::from_millis(500)).unwrap(),
        RegionLocalRoutePlan::DeliveredToLocalShard {
            shard: "shard-1".to_string(),
        }
    );
    assert_eq!(
        deliveries.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardDeliverPlan::Deliver {
            delivery: crate::EntityDelivery::new("entity-1", "loaded".to_string()),
        }
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn region_actor_replays_buffered_routes_to_spawned_local_shard_child() {
    let kit = kairo_testkit::ActorSystemTestKit::new("region-actor-buffered-replay-child").unwrap();
    let region = kit
        .system()
        .spawn(
            "region",
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
    let routes = kit
        .create_probe::<RegionLocalRoutePlan<String>>("routes")
        .unwrap();
    let replay = kit
        .create_probe::<RegionBufferedReplayPlan>("replay")
        .unwrap();
    let deliveries = kit
        .create_probe::<ShardDeliverPlan<String>>("deliveries")
        .unwrap();

    region
        .tell(ShardRegionMsg::RouteToLocalShard {
            shard: "shard-1".to_string(),
            message: ShardingEnvelope::new("entity-1", "buffered".to_string()),
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

    region
        .tell(ShardRegionMsg::HostShardAndReplayBuffered {
            shard: "shard-1".to_string(),
            reply_to: replay.actor_ref(),
            delivery_reply_to: deliveries.actor_ref(),
        })
        .unwrap();

    assert_eq!(
        replay.expect_msg(Duration::from_millis(500)).unwrap(),
        RegionBufferedReplayPlan::Replayed {
            shard: "shard-1".to_string(),
            started: ShardStarted {
                shard_id: "shard-1".to_string(),
            },
            replayed: 1,
        }
    );
    assert_eq!(
        deliveries.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardDeliverPlan::Deliver {
            delivery: crate::EntityDelivery::new("entity-1", "buffered".to_string()),
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
fn region_actor_forwards_handoff_to_spawned_store_backed_shard_child() {
    let kit = kairo_testkit::ActorSystemTestKit::new("region-actor-local-handoff-child").unwrap();
    let region = kit
        .system()
        .spawn(
            "region",
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
    let deliveries = kit
        .create_probe::<ShardDeliverPlan<String>>("deliveries")
        .unwrap();
    let routes = kit
        .create_probe::<RegionLocalRoutePlan<String>>("routes")
        .unwrap();
    let begin = kit.create_probe::<BeginHandOffPlan>("begin").unwrap();
    let handoff = kit
        .create_probe::<RegionLocalHandOffPlan>("region-handoff")
        .unwrap();
    let shard_handoff = kit
        .create_probe::<ShardHandOffPlan<String>>("shard-handoff")
        .unwrap();

    region
        .tell(ShardRegionMsg::HostShard {
            shard: "shard-1".to_string(),
            reply_to: host.actor_ref(),
        })
        .unwrap();
    host.expect_msg(Duration::from_millis(500)).unwrap();

    region
        .tell(ShardRegionMsg::RouteToLocalShard {
            shard: "shard-1".to_string(),
            message: ShardingEnvelope::new("entity-1", "loaded".to_string()),
            route_reply_to: routes.actor_ref(),
            delivery_reply_to: deliveries.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        routes.expect_msg(Duration::from_millis(500)).unwrap(),
        RegionLocalRoutePlan::DeliveredToLocalShard {
            shard: "shard-1".to_string(),
        }
    );
    deliveries.expect_msg(Duration::from_millis(500)).unwrap();

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
        .tell(ShardRegionMsg::RouteToLocalShard {
            shard: "shard-1".to_string(),
            message: ShardingEnvelope::new("entity-1", "buffered-after-begin".to_string()),
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

    region
        .tell(ShardRegionMsg::HandOffToLocalShard {
            shard: "shard-1".to_string(),
            stop_message: "stop".to_string(),
            region_reply_to: handoff.actor_ref(),
            shard_reply_to: shard_handoff.actor_ref(),
        })
        .unwrap();

    assert_eq!(
        handoff.expect_msg(Duration::from_millis(500)).unwrap(),
        RegionLocalHandOffPlan::ForwardedToLocalShard {
            shard: "shard-1".to_string(),
            command: HandOff {
                shard_id: "shard-1".to_string(),
            },
            dropped_buffered: 1,
        }
    );
    assert_eq!(
        shard_handoff
            .expect_msg(Duration::from_millis(500))
            .unwrap(),
        ShardHandOffPlan::StartEntityStopper {
            shard: "shard-1".to_string(),
            entities: vec!["entity-1".to_string()],
            stop_message: "stop".to_string(),
        }
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn region_actor_completes_store_backed_shard_child_handoff() {
    let kit =
        kairo_testkit::ActorSystemTestKit::new("region-actor-local-handoff-complete").unwrap();
    let region = kit
        .system()
        .spawn(
            "region",
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
    let handoff = kit
        .create_probe::<RegionLocalHandOffPlan>("region-handoff")
        .unwrap();
    let shard_handoff = kit
        .create_probe::<ShardHandOffPlan<String>>("shard-handoff")
        .unwrap();
    let completion = kit
        .create_probe::<RegionLocalHandOffCompletionPlan>("completion")
        .unwrap();
    let state = kit.create_probe::<ShardRegionSnapshot>("state").unwrap();
    let local_shard = kit
        .create_probe::<Option<kairo_actor::ActorRef<ShardMsg<String>>>>("local-shard")
        .unwrap();

    region
        .tell(ShardRegionMsg::HostShard {
            shard: "shard-1".to_string(),
            reply_to: host.actor_ref(),
        })
        .unwrap();
    host.expect_msg(Duration::from_millis(500)).unwrap();

    region
        .tell(ShardRegionMsg::HandOffToLocalShard {
            shard: "shard-1".to_string(),
            stop_message: "stop".to_string(),
            region_reply_to: handoff.actor_ref(),
            shard_reply_to: shard_handoff.actor_ref(),
        })
        .unwrap();
    assert!(matches!(
        handoff.expect_msg(Duration::from_millis(500)).unwrap(),
        RegionLocalHandOffPlan::ForwardedToLocalShard { .. }
    ));
    assert!(matches!(
        shard_handoff
            .expect_msg(Duration::from_millis(500))
            .unwrap(),
        ShardHandOffPlan::StartEntityStopper { .. }
    ));

    region
        .tell(ShardRegionMsg::CompleteLocalShardHandOff {
            shard: "shard-1".to_string(),
            timeout: Duration::from_millis(500),
            reply_to: completion.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        completion.expect_msg(Duration::from_millis(500)).unwrap(),
        RegionLocalHandOffCompletionPlan::Completed {
            shard: "shard-1".to_string(),
            stopped: ShardStopped {
                shard_id: "shard-1".to_string(),
            },
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

    region
        .tell(ShardRegionMsg::GetLocalShard {
            shard: "shard-1".to_string(),
            reply_to: local_shard.actor_ref(),
        })
        .unwrap();
    assert!(
        local_shard
            .expect_msg(Duration::from_millis(500))
            .unwrap()
            .is_none()
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

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

    let mut completed = false;
    for _ in 0..20 {
        coordinator
            .tell(ShardCoordinatorMsg::GetState {
                reply_to: snapshot.actor_ref(),
            })
            .unwrap();
        let state = snapshot.expect_msg(Duration::from_millis(500)).unwrap();
        completed = state
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
        "registered region targets should be available to handoff workers"
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
        registered = state.allocations.contains_key("region-a")
            && state.allocations.contains_key("region-b");
        if registered {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        registered,
        "regions should register themselves with coordinator"
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

    let mut completed = false;
    for _ in 0..20 {
        coordinator
            .tell(ShardCoordinatorMsg::GetState {
                reply_to: coordinator_state.actor_ref(),
            })
            .unwrap();
        let state = coordinator_state
            .expect_msg(Duration::from_millis(500))
            .unwrap();
        completed = state
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
        "self-registered region targets should be available to handoff workers"
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
        "region should register after coordinator discovery snapshot"
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

#[test]
fn region_actor_marks_remote_coordinator_registered_from_decoded_ack() {
    let kit = kairo_testkit::ActorSystemTestKit::new("region-remote-registration-ack").unwrap();
    let coordinator_node =
        remote_unique_node("region-remote-registration-ack", "127.0.0.1", 2671, 7);
    let remote_target = ShardCoordinatorRemoteTarget::for_node(
        coordinator_node.clone(),
        crate::DEFAULT_SHARD_COORDINATOR_REMOTE_PATH,
    )
    .unwrap();
    let discovery = RegionCoordinatorDiscoveryConfig::<String>::new(
        CoordinatorDiscoverySettings::default().with_required_role("backend"),
        Duration::from_millis(20),
    )
    .with_remote_coordinator(remote_target.clone());
    let region = kit
        .system()
        .spawn(
            "region-a",
            ShardRegionActor::<String>::props_with_local_shards_and_coordinator_discovery(
                "region-a", 10, 10, discovery,
            ),
        )
        .unwrap();
    let region_state = kit
        .create_probe::<ShardRegionSnapshot>("remote-registration-region-state")
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
        RegionRegistrationStatus::Registering
    );

    region
        .tell(ShardRegionMsg::RemoteCoordinatorRegistrationAck {
            ack: ShardCoordinatorRemoteRegistrationAck {
                sender: Some(remote_target.recipient().clone()),
                coordinator: remote_target.recipient().clone(),
            },
        })
        .unwrap();
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

#[test]
fn region_actor_sends_remote_register_on_discovery_and_retry() {
    let kit = kairo_testkit::ActorSystemTestKit::new("region-remote-register-send").unwrap();
    let coordinator_node = remote_unique_node("region-remote-register-send", "127.0.0.1", 2672, 8);
    let remote_target = ShardCoordinatorRemoteTarget::for_node(
        coordinator_node.clone(),
        crate::DEFAULT_SHARD_COORDINATOR_REMOTE_PATH,
    )
    .unwrap();
    let mut registry = Registry::new();
    register_sharding_protocol_codecs(&mut registry).unwrap();
    let registry = std::sync::Arc::new(registry);
    let remote_envelopes = kit
        .create_probe::<RemoteEnvelope>("remote-register-envelopes")
        .unwrap();
    let region_wire =
        ActorRefWireData::new("kairo://local@127.0.0.1:2551/system/sharding/region").unwrap();
    let transport = RegionRemoteCoordinatorTransport::new(
        region_wire.clone(),
        registry,
        remote_envelopes.actor_ref(),
    );
    let discovery = RegionCoordinatorDiscoveryConfig::<String>::new(
        CoordinatorDiscoverySettings::default().with_required_role("backend"),
        Duration::from_millis(20),
    )
    .with_remote_coordinator(remote_target.clone());
    let region = kit
        .system()
        .spawn(
            "region-a",
            Props::new(move || {
                ShardRegionActor::<String>::new_with_local_shards("region-a", 10, 10)
                    .with_coordinator_discovery(discovery.clone())
                    .with_remote_coordinator_transport(transport.clone())
            }),
        )
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
    let first = remote_envelopes
        .expect_msg(Duration::from_millis(500))
        .unwrap();
    assert_eq!(first.recipient, remote_target.recipient().clone());
    assert_eq!(first.sender, Some(region_wire.clone()));
    assert_eq!(first.message.manifest.as_str(), Register::MANIFEST);

    region
        .tell(ShardRegionMsg::RetryCoordinatorRegistration)
        .unwrap();
    let retry = remote_envelopes
        .expect_msg(Duration::from_millis(500))
        .unwrap();
    assert_eq!(retry.recipient, remote_target.recipient().clone());
    assert_eq!(retry.sender, Some(region_wire));
    assert_eq!(retry.message.manifest.as_str(), Register::MANIFEST);
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn region_actor_sends_remote_shard_home_after_registration_ack() {
    let kit = kairo_testkit::ActorSystemTestKit::new("region-remote-home-send").unwrap();
    let coordinator_node = remote_unique_node("region-remote-home-send", "127.0.0.1", 2673, 9);
    let remote_target = ShardCoordinatorRemoteTarget::for_node(
        coordinator_node.clone(),
        crate::DEFAULT_SHARD_COORDINATOR_REMOTE_PATH,
    )
    .unwrap();
    let mut registry = Registry::new();
    register_sharding_protocol_codecs(&mut registry).unwrap();
    let registry = std::sync::Arc::new(registry);
    let remote_envelopes = kit
        .create_probe::<RemoteEnvelope>("remote-home-envelopes")
        .unwrap();
    let routes = kit
        .create_probe::<RegionLocalRoutePlan<String>>("remote-home-routes")
        .unwrap();
    let deliveries = kit
        .create_probe::<ShardDeliverPlan<String>>("remote-home-deliveries")
        .unwrap();
    let region_wire =
        ActorRefWireData::new("kairo://local@127.0.0.1:2551/system/sharding/region").unwrap();
    let transport = RegionRemoteCoordinatorTransport::new(
        region_wire.clone(),
        registry,
        remote_envelopes.actor_ref(),
    );
    let discovery = RegionCoordinatorDiscoveryConfig::<String>::new(
        CoordinatorDiscoverySettings::default().with_required_role("backend"),
        Duration::from_secs(5),
    )
    .with_remote_coordinator(remote_target.clone());
    let region = kit
        .system()
        .spawn(
            "region-a",
            Props::new(move || {
                ShardRegionActor::<String>::new_with_local_shards("region-a", 10, 10)
                    .with_coordinator_discovery(discovery.clone())
                    .with_remote_coordinator_transport(transport.clone())
            }),
        )
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
    let register = remote_envelopes
        .expect_msg(Duration::from_millis(500))
        .unwrap();
    assert_eq!(register.message.manifest.as_str(), Register::MANIFEST);

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

    region
        .tell(ShardRegionMsg::RemoteCoordinatorRegistrationAck {
            ack: ShardCoordinatorRemoteRegistrationAck {
                sender: Some(remote_target.recipient().clone()),
                coordinator: remote_target.recipient().clone(),
            },
        })
        .unwrap();
    let request = remote_envelopes
        .expect_msg(Duration::from_millis(500))
        .unwrap();
    assert_eq!(request.recipient, remote_target.recipient().clone());
    assert_eq!(request.sender, Some(region_wire));
    assert_eq!(request.message.manifest.as_str(), GetShardHome::MANIFEST);
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn region_actor_sends_remote_graceful_shutdown_and_region_stopped() {
    let kit = kairo_testkit::ActorSystemTestKit::new("region-remote-graceful-shutdown").unwrap();
    let coordinator_node =
        remote_unique_node("region-remote-graceful-shutdown", "127.0.0.1", 2674, 10);
    let remote_target = ShardCoordinatorRemoteTarget::for_node(
        coordinator_node.clone(),
        crate::DEFAULT_SHARD_COORDINATOR_REMOTE_PATH,
    )
    .unwrap();
    let mut registry = Registry::new();
    register_sharding_protocol_codecs(&mut registry).unwrap();
    let registry = std::sync::Arc::new(registry);
    let remote_envelopes = kit
        .create_probe::<RemoteEnvelope>("remote-shutdown-envelopes")
        .unwrap();
    let region_wire =
        ActorRefWireData::new("kairo://local@127.0.0.1:2551/system/sharding/region").unwrap();
    let transport = RegionRemoteCoordinatorTransport::new(
        region_wire.clone(),
        registry,
        remote_envelopes.actor_ref(),
    );
    let discovery = RegionCoordinatorDiscoveryConfig::<String>::new(
        CoordinatorDiscoverySettings::default().with_required_role("backend"),
        Duration::from_secs(5),
    )
    .with_remote_coordinator(remote_target.clone());
    let region = kit
        .system()
        .spawn(
            "region-a",
            Props::new(move || {
                ShardRegionActor::<String>::new_with_local_shards("region-a", 10, 10)
                    .with_coordinator_discovery(discovery.clone())
                    .with_remote_coordinator_transport(transport.clone())
            }),
        )
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
    let register = remote_envelopes
        .expect_msg(Duration::from_millis(500))
        .unwrap();
    assert_eq!(register.message.manifest.as_str(), Register::MANIFEST);

    region
        .tell(ShardRegionMsg::RemoteCoordinatorRegistrationAck {
            ack: ShardCoordinatorRemoteRegistrationAck {
                sender: Some(remote_target.recipient().clone()),
                coordinator: remote_target.recipient().clone(),
            },
        })
        .unwrap();
    region
        .tell(ShardRegionMsg::GracefulShutdown { reply_to: None })
        .unwrap();

    let graceful = remote_envelopes
        .expect_msg(Duration::from_millis(500))
        .unwrap();
    assert_eq!(graceful.recipient, remote_target.recipient().clone());
    assert_eq!(graceful.sender, Some(region_wire.clone()));
    assert_eq!(
        graceful.message.manifest.as_str(),
        GracefulShutdownReq::MANIFEST
    );

    let stopped = remote_envelopes
        .expect_msg(Duration::from_millis(500))
        .unwrap();
    assert_eq!(stopped.recipient, remote_target.recipient().clone());
    assert_eq!(stopped.sender, Some(region_wire));
    assert_eq!(stopped.message.manifest.as_str(), RegionStopped::MANIFEST);
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

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
fn region_actor_requests_shard_home_from_registered_coordinator_for_local_route() {
    let kit = kairo_testkit::ActorSystemTestKit::new("region-route-coordinator-home").unwrap();
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
    let region_state = kit
        .create_probe::<ShardRegionSnapshot>("region-state")
        .unwrap();
    let route = kit
        .create_probe::<RegionLocalRoutePlan<String>>("route")
        .unwrap();
    let delivery = kit
        .create_probe::<ShardDeliverPlan<String>>("delivery")
        .unwrap();

    let mut registered = false;
    for _ in 0..20 {
        region
            .tell(ShardRegionMsg::GetState {
                reply_to: region_state.actor_ref(),
            })
            .unwrap();
        registered = region_state
            .expect_msg(Duration::from_millis(500))
            .unwrap()
            .registration_status
            == RegionRegistrationStatus::Registered;
        if registered {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(registered, "region should register before route resolution");

    region
        .tell(ShardRegionMsg::RouteToLocalShard {
            shard: "shard-1".to_string(),
            message: ShardingEnvelope::new("entity-1", "first".to_string()),
            route_reply_to: route.actor_ref(),
            delivery_reply_to: delivery.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        route.expect_msg(Duration::from_millis(500)).unwrap(),
        RegionLocalRoutePlan::Buffered {
            shard: "shard-1".to_string(),
            request: Some(GetShardHome {
                shard_id: "shard-1".to_string(),
            }),
        }
    );
    assert_eq!(
        delivery.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardDeliverPlan::StartEntity {
            delivery: EntityDelivery::new("entity-1", "first".to_string()),
        }
    );

    region
        .tell(ShardRegionMsg::RouteToLocalShard {
            shard: "shard-1".to_string(),
            message: ShardingEnvelope::new("entity-1", "second".to_string()),
            route_reply_to: route.actor_ref(),
            delivery_reply_to: delivery.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        route.expect_msg(Duration::from_millis(500)).unwrap(),
        RegionLocalRoutePlan::DeliveredToLocalShard {
            shard: "shard-1".to_string(),
        }
    );
    assert_eq!(
        delivery.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardDeliverPlan::Deliver {
            delivery: EntityDelivery::new("entity-1", "second".to_string()),
        }
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn region_actor_requests_buffered_shard_home_after_registration_ack() {
    let kit = kairo_testkit::ActorSystemTestKit::new("region-route-after-registration").unwrap();
    let (request_tx, request_rx) = mpsc::channel();
    let coordinator = kit
        .system()
        .spawn(
            "coordinator",
            Props::new(move || DelayedRegistrationCoordinator {
                pending_registration: None,
                request_tx,
            }),
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
                Duration::from_millis(500),
            ),
        )
        .unwrap();
    let route = kit
        .create_probe::<RegionLocalRoutePlan<String>>("route")
        .unwrap();
    let delivery = kit
        .create_probe::<ShardDeliverPlan<String>>("delivery")
        .unwrap();
    let second_delivery = kit
        .create_probe::<ShardDeliverPlan<String>>("second-delivery")
        .unwrap();

    region
        .tell(ShardRegionMsg::RouteToLocalShard {
            shard: "shard-1".to_string(),
            message: ShardingEnvelope::new("entity-1", "first".to_string()),
            route_reply_to: route.actor_ref(),
            delivery_reply_to: delivery.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        route.expect_msg(Duration::from_millis(500)).unwrap(),
        RegionLocalRoutePlan::Buffered {
            shard: "shard-1".to_string(),
            request: Some(GetShardHome {
                shard_id: "shard-1".to_string(),
            }),
        }
    );
    region
        .tell(ShardRegionMsg::RouteToLocalShard {
            shard: "shard-1".to_string(),
            message: ShardingEnvelope::new("entity-1", "second".to_string()),
            route_reply_to: route.actor_ref(),
            delivery_reply_to: second_delivery.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        route.expect_msg(Duration::from_millis(500)).unwrap(),
        RegionLocalRoutePlan::Buffered {
            shard: "shard-1".to_string(),
            request: None,
        }
    );
    assert!(
        request_rx.recv_timeout(Duration::from_millis(50)).is_err(),
        "region must not request shard homes before registration ack"
    );

    coordinator
        .tell(ShardCoordinatorMsg::SetAllRegionsRegistered {
            all_registered: true,
        })
        .unwrap();
    assert_eq!(
        request_rx.recv_timeout(Duration::from_millis(500)).unwrap(),
        "shard-1".to_string()
    );
    assert_eq!(
        delivery.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardDeliverPlan::StartEntity {
            delivery: EntityDelivery::new("entity-1", "first".to_string()),
        }
    );
    assert_eq!(
        second_delivery
            .expect_msg(Duration::from_millis(500))
            .unwrap(),
        ShardDeliverPlan::Deliver {
            delivery: EntityDelivery::new("entity-1", "second".to_string()),
        }
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn region_actor_forwards_known_remote_home_to_region_target() {
    let kit = kairo_testkit::ActorSystemTestKit::new("region-forward-known-home").unwrap();
    let region_b = kit
        .system()
        .spawn(
            "region-b",
            ShardRegionActor::<String>::props_with_local_shards("region-b", 10, 10),
        )
        .unwrap();
    let host = kit.create_probe::<HostShardPlan<String>>("host").unwrap();
    region_b
        .tell(ShardRegionMsg::HostShard {
            shard: "shard-1".to_string(),
            reply_to: host.actor_ref(),
        })
        .unwrap();
    host.expect_msg(Duration::from_millis(500)).unwrap();

    let mut route_transport = RegionRouteTransport::new();
    route_transport.insert_target(RegionRouteTarget::new("region-b", region_b));
    let region_a = kit
        .system()
        .spawn(
            "region-a",
            Props::new(move || {
                ShardRegionActor::<String>::new_with_local_shards("region-a", 10, 10)
                    .with_region_route_transport(route_transport)
            }),
        )
        .unwrap();
    let home = kit
        .create_probe::<Result<ShardHomePlan<String>, ShardingError>>("home")
        .unwrap();
    let routes = kit
        .create_probe::<RegionLocalRoutePlan<String>>("routes")
        .unwrap();
    let deliveries = kit
        .create_probe::<ShardDeliverPlan<String>>("deliveries")
        .unwrap();

    region_a
        .tell(ShardRegionMsg::RecordShardHome {
            shard: "shard-1".to_string(),
            region: "region-b".to_string(),
            reply_to: home.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        home.expect_msg(Duration::from_millis(500)).unwrap(),
        Ok(ShardHomePlan::Forward {
            shard: "shard-1".to_string(),
            region: "region-b".to_string(),
            buffered: Vec::new(),
        })
    );

    region_a
        .tell(ShardRegionMsg::RouteToLocalShard {
            shard: "shard-1".to_string(),
            message: ShardingEnvelope::new("entity-1", "first".to_string()),
            route_reply_to: routes.actor_ref(),
            delivery_reply_to: deliveries.actor_ref(),
        })
        .unwrap();

    assert_eq!(
        routes.expect_msg(Duration::from_millis(500)).unwrap(),
        RegionLocalRoutePlan::DeliveredToLocalShard {
            shard: "shard-1".to_string(),
        }
    );
    assert_eq!(
        deliveries.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardDeliverPlan::StartEntity {
            delivery: EntityDelivery::new("entity-1", "first".to_string()),
        }
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn region_actor_forwards_buffered_remote_home_after_resolution() {
    let kit =
        kairo_testkit::ActorSystemTestKit::new("region-forward-buffered-remote-home").unwrap();
    let region_b = kit
        .system()
        .spawn(
            "region-b",
            ShardRegionActor::<String>::props_with_local_shards("region-b", 10, 10),
        )
        .unwrap();
    let host = kit.create_probe::<HostShardPlan<String>>("host").unwrap();
    region_b
        .tell(ShardRegionMsg::HostShard {
            shard: "shard-1".to_string(),
            reply_to: host.actor_ref(),
        })
        .unwrap();
    host.expect_msg(Duration::from_millis(500)).unwrap();

    let mut route_transport = RegionRouteTransport::new();
    route_transport.insert_target(RegionRouteTarget::new("region-b", region_b));
    let region_a = kit
        .system()
        .spawn(
            "region-a",
            Props::new(move || {
                ShardRegionActor::<String>::new_with_local_shards("region-a", 10, 10)
                    .with_region_route_transport(route_transport)
            }),
        )
        .unwrap();
    let routes = kit
        .create_probe::<RegionLocalRoutePlan<String>>("routes")
        .unwrap();
    let first_delivery = kit
        .create_probe::<ShardDeliverPlan<String>>("first-delivery")
        .unwrap();
    let second_delivery = kit
        .create_probe::<ShardDeliverPlan<String>>("second-delivery")
        .unwrap();

    region_a
        .tell(ShardRegionMsg::RouteToLocalShard {
            shard: "shard-1".to_string(),
            message: ShardingEnvelope::new("entity-1", "first".to_string()),
            route_reply_to: routes.actor_ref(),
            delivery_reply_to: first_delivery.actor_ref(),
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
    region_a
        .tell(ShardRegionMsg::RouteToLocalShard {
            shard: "shard-1".to_string(),
            message: ShardingEnvelope::new("entity-1", "second".to_string()),
            route_reply_to: routes.actor_ref(),
            delivery_reply_to: second_delivery.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        routes.expect_msg(Duration::from_millis(500)).unwrap(),
        RegionLocalRoutePlan::Buffered {
            shard: "shard-1".to_string(),
            request: None,
        }
    );

    region_a
        .tell(ShardRegionMsg::CoordinatorShardHomeResult {
            requested_shard: "shard-1".to_string(),
            result: Ok(GetShardHomePlan::Reply {
                shard: "shard-1".to_string(),
                region: "region-b".to_string(),
            }),
        })
        .unwrap();
    assert_eq!(
        first_delivery
            .expect_msg(Duration::from_millis(500))
            .unwrap(),
        ShardDeliverPlan::StartEntity {
            delivery: EntityDelivery::new("entity-1", "first".to_string()),
        }
    );
    assert_eq!(
        second_delivery
            .expect_msg(Duration::from_millis(500))
            .unwrap(),
        ShardDeliverPlan::Deliver {
            delivery: EntityDelivery::new("entity-1", "second".to_string()),
        }
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn region_actor_applies_decoded_remote_shard_home_reply() {
    let kit = kairo_testkit::ActorSystemTestKit::new("region-remote-home-reply").unwrap();
    let region_b = kit
        .system()
        .spawn(
            "region-b",
            ShardRegionActor::<String>::props_with_local_shards("region-b", 10, 10),
        )
        .unwrap();
    let host = kit.create_probe::<HostShardPlan<String>>("host").unwrap();
    region_b
        .tell(ShardRegionMsg::HostShard {
            shard: "shard-1".to_string(),
            reply_to: host.actor_ref(),
        })
        .unwrap();
    host.expect_msg(Duration::from_millis(500)).unwrap();

    let remote_region =
        ActorRefWireData::new("kairo://remote@127.0.0.1:2552/system/sharding/region").unwrap();
    let mut route_transport = RegionRouteTransport::new();
    route_transport.insert_target(RegionRouteTarget::new(
        remote_region.path().to_string(),
        region_b,
    ));
    let region_a = kit
        .system()
        .spawn(
            "region-a",
            Props::new(move || {
                ShardRegionActor::<String>::new_with_local_shards("region-a", 10, 10)
                    .with_region_route_transport(route_transport)
            }),
        )
        .unwrap();
    let routes = kit
        .create_probe::<RegionLocalRoutePlan<String>>("routes")
        .unwrap();
    let first_delivery = kit
        .create_probe::<ShardDeliverPlan<String>>("first-remote-delivery")
        .unwrap();
    let second_delivery = kit
        .create_probe::<ShardDeliverPlan<String>>("second-remote-delivery")
        .unwrap();

    region_a
        .tell(ShardRegionMsg::RouteToLocalShard {
            shard: "shard-1".to_string(),
            message: ShardingEnvelope::new("entity-1", "first".to_string()),
            route_reply_to: routes.actor_ref(),
            delivery_reply_to: first_delivery.actor_ref(),
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
    region_a
        .tell(ShardRegionMsg::RouteToLocalShard {
            shard: "shard-1".to_string(),
            message: ShardingEnvelope::new("entity-1", "second".to_string()),
            route_reply_to: routes.actor_ref(),
            delivery_reply_to: second_delivery.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        routes.expect_msg(Duration::from_millis(500)).unwrap(),
        RegionLocalRoutePlan::Buffered {
            shard: "shard-1".to_string(),
            request: None,
        }
    );

    region_a
        .tell(ShardRegionMsg::RemoteCoordinatorShardHome {
            home: ShardCoordinatorRemoteHome {
                sender: None,
                shard_id: "shard-1".to_string(),
                region: remote_region,
            },
        })
        .unwrap();
    assert_eq!(
        first_delivery
            .expect_msg(Duration::from_millis(500))
            .unwrap(),
        ShardDeliverPlan::StartEntity {
            delivery: EntityDelivery::new("entity-1", "first".to_string()),
        }
    );
    assert_eq!(
        second_delivery
            .expect_msg(Duration::from_millis(500))
            .unwrap(),
        ShardDeliverPlan::Deliver {
            delivery: EntityDelivery::new("entity-1", "second".to_string()),
        }
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

    let mut allocated = false;
    for _ in 0..20 {
        coordinator
            .tell(ShardCoordinatorMsg::GetState {
                reply_to: coordinator_state.actor_ref(),
            })
            .unwrap();
        let state = coordinator_state
            .expect_msg(Duration::from_millis(500))
            .unwrap();
        allocated = state.unallocated_shards.is_empty()
            && state
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
        "remembered shard should be allocated when region registers"
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
        "allocated remembered shard should be hosted on registered local region"
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn handoff_transport_sends_begin_to_participants_then_handoff_to_owner() {
    let kit = kairo_testkit::ActorSystemTestKit::new("handoff-transport").unwrap();
    let region_a = kit
        .system()
        .spawn(
            "region-a",
            ShardRegionActor::<String>::props("region-a", 10),
        )
        .unwrap();
    let region_b = kit
        .system()
        .spawn(
            "region-b",
            ShardRegionActor::<String>::props("region-b", 10),
        )
        .unwrap();
    let host = kit.create_probe::<HostShardPlan<String>>("host").unwrap();
    let started = kit
        .create_probe::<ShardStartedPlan<String>>("started")
        .unwrap();
    region_a
        .tell(ShardRegionMsg::HostShard {
            shard: "shard-1".to_string(),
            reply_to: host.actor_ref(),
        })
        .unwrap();
    host.expect_msg(Duration::from_millis(500)).unwrap();
    region_a
        .tell(ShardRegionMsg::MarkShardStarted {
            shard: "shard-1".to_string(),
            reply_to: started.actor_ref(),
        })
        .unwrap();
    started.expect_msg(Duration::from_millis(500)).unwrap();

    let mut transport = HandoffTransport::new();
    transport.set_targets([
        HandoffRegionTarget::new("region-a", region_a),
        HandoffRegionTarget::new("region-b", region_b),
    ]);
    let plan = ShardRebalancePlan {
        shard: "shard-1".to_string(),
        from_region: "region-a".to_string(),
        participants: BTreeSet::from(["region-a".to_string(), "region-b".to_string()]),
        begin_handoff: crate::BeginHandOff {
            shard_id: "shard-1".to_string(),
        },
    };
    let begin = kit.create_probe::<BeginHandOffPlan>("begin").unwrap();
    let handoff = kit.create_probe::<HandOffPlan>("handoff").unwrap();

    let begin_report = transport.send_begin_handoff(&plan, begin.actor_ref());

    assert!(begin_report.is_success());
    assert_eq!(
        begin_report.sent_to(),
        &[
            HandoffDeliveryTarget::BeginHandOff {
                region: "region-a".to_string(),
            },
            HandoffDeliveryTarget::BeginHandOff {
                region: "region-b".to_string(),
            },
        ]
    );
    for _ in 0..2 {
        assert_eq!(
            begin.expect_msg(Duration::from_millis(500)).unwrap(),
            BeginHandOffPlan::Ack {
                shard: "shard-1".to_string(),
                ack: crate::BeginHandOffAck {
                    shard_id: "shard-1".to_string(),
                },
            }
        );
    }

    let handoff_report = transport.send_handoff(&plan, handoff.actor_ref());

    assert!(handoff_report.is_success());
    assert_eq!(
        handoff_report.sent_to(),
        &[HandoffDeliveryTarget::HandOff {
            region: "region-a".to_string(),
        }]
    );
    assert_eq!(
        handoff.expect_msg(Duration::from_millis(500)).unwrap(),
        HandOffPlan::ForwardToLocalShard {
            shard: "shard-1".to_string(),
            command: HandOff {
                shard_id: "shard-1".to_string(),
            },
            dropped_buffered: 0,
        }
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn handoff_transport_reports_missing_targets_without_stopping_other_sends() {
    let kit = kairo_testkit::ActorSystemTestKit::new("handoff-transport-missing").unwrap();
    let region_a = kit
        .system()
        .spawn(
            "region-a",
            ShardRegionActor::<String>::props("region-a", 10),
        )
        .unwrap();
    let mut transport = HandoffTransport::new();
    transport.insert_target(HandoffRegionTarget::new("region-a", region_a));
    let plan = ShardRebalancePlan {
        shard: "shard-1".to_string(),
        from_region: "region-c".to_string(),
        participants: BTreeSet::from(["region-a".to_string(), "region-b".to_string()]),
        begin_handoff: crate::BeginHandOff {
            shard_id: "shard-1".to_string(),
        },
    };
    let begin = kit.create_probe::<BeginHandOffPlan>("begin").unwrap();
    let handoff = kit.create_probe::<HandOffPlan>("handoff").unwrap();

    let begin_report = transport.send_begin_handoff(&plan, begin.actor_ref());

    assert_eq!(
        begin_report.sent_to(),
        &[HandoffDeliveryTarget::BeginHandOff {
            region: "region-a".to_string(),
        }]
    );
    assert_eq!(
        begin_report.failures(),
        &[HandoffDeliveryFailure::MissingTarget {
            target: HandoffDeliveryTarget::BeginHandOff {
                region: "region-b".to_string(),
            },
        }]
    );
    begin.expect_msg(Duration::from_millis(500)).unwrap();

    let handoff_report = transport.send_handoff(&plan, handoff.actor_ref());

    assert_eq!(handoff_report.sent_to(), &[]);
    assert_eq!(
        handoff_report.failures(),
        &[HandoffDeliveryFailure::MissingTarget {
            target: HandoffDeliveryTarget::HandOff {
                region: "region-c".to_string(),
            },
        }]
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
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
fn entity_shard_actor_spawns_child_and_delivers_business_messages() {
    let kit = kairo_testkit::ActorSystemTestKit::new("entity-shard-actor-deliver").unwrap();
    let (observed_tx, observed_rx) = mpsc::channel();
    let factory = EntityActorFactory::new(move |entity_id| RecordingEntity {
        entity_id,
        observed: observed_tx.clone(),
    });
    let shard = kit
        .system()
        .spawn("shard", EntityShardActor::props("shard-1", 10, factory))
        .unwrap();
    let deliveries = kit
        .create_probe::<ShardDeliverPlan<String>>("deliveries")
        .unwrap();
    let passivation = kit
        .create_probe::<PassivatePlan<String>>("passivation")
        .unwrap();

    shard
        .tell(ShardMsg::Deliver {
            message: ShardingEnvelope::new("entity-1", "first".to_string()),
            reply_to: deliveries.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        deliveries.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardDeliverPlan::StartEntity {
            delivery: EntityDelivery::new("entity-1", "first".to_string()),
        }
    );
    assert_eq!(
        observed_rx
            .recv_timeout(Duration::from_millis(500))
            .unwrap(),
        ("entity-1".to_string(), "first".to_string())
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
            delivery: EntityDelivery::new("entity-1", "second".to_string()),
        }
    );
    assert_eq!(
        observed_rx
            .recv_timeout(Duration::from_millis(500))
            .unwrap(),
        ("entity-1".to_string(), "second".to_string())
    );

    shard
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
    assert_eq!(
        observed_rx
            .recv_timeout(Duration::from_millis(500))
            .unwrap(),
        ("entity-1".to_string(), "stop".to_string())
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn entity_shard_actor_handoff_sends_stop_to_entity_children() {
    let kit = kairo_testkit::ActorSystemTestKit::new("entity-shard-actor-handoff").unwrap();
    let (observed_tx, observed_rx) = mpsc::channel();
    let factory = EntityActorFactory::new(move |entity_id| RecordingEntity {
        entity_id,
        observed: observed_tx.clone(),
    });
    let shard = kit
        .system()
        .spawn("shard", EntityShardActor::props("shard-1", 10, factory))
        .unwrap();
    let deliveries = kit
        .create_probe::<ShardDeliverPlan<String>>("deliveries")
        .unwrap();
    let handoff = kit
        .create_probe::<ShardHandOffPlan<String>>("handoff")
        .unwrap();
    let stopper = kit.create_probe::<bool>("stopper").unwrap();

    for entity_id in ["entity-b", "entity-a"] {
        shard
            .tell(ShardMsg::Deliver {
                message: ShardingEnvelope::new(entity_id, "start".to_string()),
                reply_to: deliveries.actor_ref(),
            })
            .unwrap();
        deliveries.expect_msg(Duration::from_millis(500)).unwrap();
        observed_rx
            .recv_timeout(Duration::from_millis(500))
            .unwrap();
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

    let mut stopped = vec![
        observed_rx
            .recv_timeout(Duration::from_millis(500))
            .unwrap(),
        observed_rx
            .recv_timeout(Duration::from_millis(500))
            .unwrap(),
    ];
    stopped.sort();
    assert_eq!(
        stopped,
        vec![
            ("entity-a".to_string(), "stop".to_string()),
            ("entity-b".to_string(), "stop".to_string()),
        ]
    );

    let mut completed = false;
    for _ in 0..20 {
        shard
            .tell(ShardMsg::HandOffStopperTerminated {
                reply_to: stopper.actor_ref(),
            })
            .unwrap();
        completed = stopper.expect_msg(Duration::from_millis(500)).unwrap();
        if completed {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        completed,
        "handoff should complete after entity children stop"
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn shard_runtime_recovers_remembered_entities_as_active() {
    let mut runtime = ShardRuntime::<String>::new("shard-1", 10);
    runtime.deliver(ShardingEnvelope::new("entity-b", "first".to_string()));

    let plan = runtime.recover_remembered_entities([
        "entity-c".to_string(),
        "entity-a".to_string(),
        "entity-b".to_string(),
        "".to_string(),
    ]);

    assert_eq!(
        plan,
        RememberedEntitiesPlan {
            started: vec!["entity-a".to_string(), "entity-c".to_string()],
            already_active: vec!["entity-b".to_string()],
            ignored_empty: 1,
        }
    );
    assert_eq!(
        runtime.active_entity_ids(),
        vec![
            "entity-a".to_string(),
            "entity-b".to_string(),
            "entity-c".to_string()
        ]
    );
    assert_eq!(
        runtime.deliver(ShardingEnvelope::new("entity-a", "message".to_string())),
        ShardDeliverPlan::Deliver {
            delivery: crate::EntityDelivery::new("entity-a", "message".to_string()),
        }
    );
}

#[test]
fn shard_actor_recovers_remembered_entities_before_delivery() {
    let kit = kairo_testkit::ActorSystemTestKit::new("shard-actor-remembered-recovery").unwrap();
    let shard = kit
        .system()
        .spawn("shard", ShardActor::<String>::props("shard-1", 10))
        .unwrap();
    let recovery = kit
        .create_probe::<RememberedEntitiesPlan>("recovery")
        .unwrap();
    let deliveries = kit
        .create_probe::<ShardDeliverPlan<String>>("deliveries")
        .unwrap();
    let state = kit.create_probe::<ShardSnapshot>("state").unwrap();

    shard
        .tell(ShardMsg::RecoverRememberedEntities {
            entities: vec!["entity-2".to_string(), "entity-1".to_string()],
            reply_to: recovery.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        recovery.expect_msg(Duration::from_millis(500)).unwrap(),
        RememberedEntitiesPlan {
            started: vec!["entity-1".to_string(), "entity-2".to_string()],
            already_active: Vec::new(),
            ignored_empty: 0,
        }
    );

    shard
        .tell(ShardMsg::Deliver {
            message: ShardingEnvelope::new("entity-1", "message".to_string()),
            reply_to: deliveries.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        deliveries.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardDeliverPlan::Deliver {
            delivery: crate::EntityDelivery::new("entity-1", "message".to_string()),
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
            active_entities: vec!["entity-1".to_string(), "entity-2".to_string()],
            entity_count: 2,
            total_buffered: 0,
            handoff_in_progress: false,
        }
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn shard_actor_stashes_delivery_until_remembered_entities_loaded() {
    let kit = kairo_testkit::ActorSystemTestKit::new("shard-actor-loading-stash").unwrap();
    let shard = kit
        .system()
        .spawn(
            "shard",
            ShardActor::<String>::props_loading_remembered_entities("shard-1", 10),
        )
        .unwrap();
    let deliveries = kit
        .create_probe::<ShardDeliverPlan<String>>("deliveries")
        .unwrap();
    let recovery = kit
        .create_probe::<RememberedEntitiesPlan>("recovery")
        .unwrap();

    shard
        .tell(ShardMsg::Deliver {
            message: ShardingEnvelope::new("entity-1", "message".to_string()),
            reply_to: deliveries.actor_ref(),
        })
        .unwrap();
    deliveries.expect_no_msg(Duration::from_millis(30)).unwrap();

    shard
        .tell(ShardMsg::RememberedEntitiesLoaded {
            entities: vec!["entity-1".to_string()],
            reply_to: recovery.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        recovery.expect_msg(Duration::from_millis(500)).unwrap(),
        RememberedEntitiesPlan {
            started: vec!["entity-1".to_string()],
            already_active: Vec::new(),
            ignored_empty: 0,
        }
    );
    assert_eq!(
        deliveries.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardDeliverPlan::Deliver {
            delivery: crate::EntityDelivery::new("entity-1", "message".to_string()),
        }
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn shard_actor_replays_stashed_new_entity_as_remember_start_after_load() {
    let kit = kairo_testkit::ActorSystemTestKit::new("shard-actor-loading-new-entity").unwrap();
    let shard = kit
        .system()
        .spawn(
            "shard",
            ShardActor::<String>::props_loading_remembered_entities("shard-1", 10),
        )
        .unwrap();
    let deliveries = kit
        .create_probe::<ShardDeliverPlan<String>>("deliveries")
        .unwrap();
    let recovery = kit
        .create_probe::<RememberedEntitiesPlan>("recovery")
        .unwrap();
    let update = RememberShardUpdate::new(["entity-2".to_string()], std::iter::empty::<String>());

    shard
        .tell(ShardMsg::Deliver {
            message: ShardingEnvelope::new("entity-2", "message".to_string()),
            reply_to: deliveries.actor_ref(),
        })
        .unwrap();
    deliveries.expect_no_msg(Duration::from_millis(30)).unwrap();

    shard
        .tell(ShardMsg::RememberedEntitiesLoaded {
            entities: Vec::new(),
            reply_to: recovery.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        recovery.expect_msg(Duration::from_millis(500)).unwrap(),
        RememberedEntitiesPlan {
            started: Vec::new(),
            already_active: Vec::new(),
            ignored_empty: 0,
        }
    );
    assert_eq!(
        deliveries.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardDeliverPlan::RememberUpdate { update }
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn shard_actor_with_remember_store_loads_entities_on_start() {
    let kit = kairo_testkit::ActorSystemTestKit::new("shard-actor-store-load").unwrap();
    let store = kit
        .system()
        .spawn(
            "store",
            RememberShardStoreActor::props(RememberShardStoreState::with_entities(
                "orders",
                "shard-1",
                ["entity-1".to_string()],
            )),
        )
        .unwrap();
    let shard = kit
        .system()
        .spawn(
            "shard",
            ShardActor::<String>::props_with_remember_store(
                "shard-1",
                10,
                store,
                Duration::from_millis(500),
            ),
        )
        .unwrap();
    let deliveries = kit
        .create_probe::<ShardDeliverPlan<String>>("deliveries")
        .unwrap();

    shard
        .tell(ShardMsg::Deliver {
            message: ShardingEnvelope::new("entity-1", "loaded".to_string()),
            reply_to: deliveries.actor_ref(),
        })
        .unwrap();

    assert_eq!(
        deliveries.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardDeliverPlan::Deliver {
            delivery: crate::EntityDelivery::new("entity-1", "loaded".to_string()),
        }
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn shard_actor_with_remember_store_persists_start_updates() {
    let kit = kairo_testkit::ActorSystemTestKit::new("shard-actor-store-start").unwrap();
    let store = kit
        .system()
        .spawn(
            "store",
            RememberShardStoreActor::props(RememberShardStoreState::new("orders", "shard-1")),
        )
        .unwrap();
    let shard = kit
        .system()
        .spawn(
            "shard",
            ShardActor::<String>::props_with_remember_store(
                "shard-1",
                10,
                store.clone(),
                Duration::from_millis(500),
            ),
        )
        .unwrap();
    let deliveries = kit
        .create_probe::<ShardDeliverPlan<String>>("deliveries")
        .unwrap();
    let store_state = kit
        .create_probe::<RememberShardStoreSnapshot>("store-state")
        .unwrap();
    let shard_state = kit.create_probe::<ShardSnapshot>("shard-state").unwrap();
    let update = RememberShardUpdate::new(["entity-1".to_string()], std::iter::empty::<String>());

    shard
        .tell(ShardMsg::Deliver {
            message: ShardingEnvelope::new("entity-1", "first".to_string()),
            reply_to: deliveries.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        deliveries.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardDeliverPlan::RememberUpdate { update }
    );

    let mut persisted = false;
    let mut activated = false;
    for _ in 0..20 {
        store
            .tell(RememberShardStoreMsg::GetState {
                reply_to: store_state.actor_ref(),
            })
            .unwrap();
        let snapshot = store_state.expect_msg(Duration::from_millis(500)).unwrap();
        let remembered = snapshot
            .entities_by_key
            .values()
            .flat_map(|entities| entities.iter().cloned())
            .collect::<BTreeSet<_>>();
        persisted = remembered.contains("entity-1");

        shard
            .tell(ShardMsg::GetState {
                reply_to: shard_state.actor_ref(),
            })
            .unwrap();
        let snapshot = shard_state.expect_msg(Duration::from_millis(500)).unwrap();
        activated = snapshot.active_entities == vec!["entity-1".to_string()]
            && snapshot.total_buffered == 0;

        if persisted && activated {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    assert!(persisted, "remember store should contain entity-1");
    assert!(activated, "shard runtime should mark entity-1 active");
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn shard_actor_spawns_local_remember_store_and_loads_entities() {
    let kit = kairo_testkit::ActorSystemTestKit::new("shard-actor-local-store-load").unwrap();
    let shard = kit
        .system()
        .spawn(
            "shard",
            ShardActor::<String>::props_with_local_remember_store(
                10,
                RememberShardStoreState::with_entities(
                    "orders",
                    "shard-1",
                    ["entity-1".to_string()],
                ),
                Duration::from_millis(500),
            ),
        )
        .unwrap();
    let deliveries = kit
        .create_probe::<ShardDeliverPlan<String>>("deliveries")
        .unwrap();

    shard
        .tell(ShardMsg::Deliver {
            message: ShardingEnvelope::new("entity-1", "loaded".to_string()),
            reply_to: deliveries.actor_ref(),
        })
        .unwrap();

    assert_eq!(
        deliveries.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardDeliverPlan::Deliver {
            delivery: crate::EntityDelivery::new("entity-1", "loaded".to_string()),
        }
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn shard_actor_spawns_local_remember_store_and_persists_start_updates() {
    let kit = kairo_testkit::ActorSystemTestKit::new("shard-actor-local-store-start").unwrap();
    let shard = kit
        .system()
        .spawn(
            "shard",
            ShardActor::<String>::props_with_local_remember_store(
                10,
                RememberShardStoreState::new("orders", "shard-1"),
                Duration::from_millis(500),
            ),
        )
        .unwrap();
    let deliveries = kit
        .create_probe::<ShardDeliverPlan<String>>("deliveries")
        .unwrap();
    let shard_state = kit.create_probe::<ShardSnapshot>("shard-state").unwrap();
    let update = RememberShardUpdate::new(["entity-1".to_string()], std::iter::empty::<String>());

    shard
        .tell(ShardMsg::Deliver {
            message: ShardingEnvelope::new("entity-1", "first".to_string()),
            reply_to: deliveries.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        deliveries.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardDeliverPlan::RememberUpdate { update }
    );

    let mut activated = false;
    for _ in 0..20 {
        shard
            .tell(ShardMsg::GetState {
                reply_to: shard_state.actor_ref(),
            })
            .unwrap();
        let snapshot = shard_state.expect_msg(Duration::from_millis(500)).unwrap();
        activated = snapshot.active_entities == vec!["entity-1".to_string()]
            && snapshot.total_buffered == 0;
        if activated {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    assert!(
        activated,
        "local remember store reply should activate entity-1"
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn shard_runtime_remember_entities_writes_start_before_delivery() {
    let mut runtime = ShardRuntime::<String>::new_with_remember_entities("shard-1", 10);

    let update = RememberShardUpdate::new(["entity-1".to_string()], std::iter::empty::<String>());
    assert_eq!(
        runtime.deliver(ShardingEnvelope::new("entity-1", "first".to_string())),
        ShardDeliverPlan::RememberUpdate {
            update: update.clone(),
        }
    );
    assert!(runtime.remember_update_in_progress());
    assert_eq!(runtime.entity_state(&"entity-1".to_string()), None);
    assert_eq!(runtime.buffered_count(&"entity-1".to_string()), 1);

    assert_eq!(
        runtime.remember_update_done(update),
        RememberUpdateDonePlan {
            deliveries: vec![crate::EntityDelivery::new("entity-1", "first".to_string())],
            next_update: None,
        }
    );
    assert!(!runtime.remember_update_in_progress());
    assert_eq!(
        runtime.entity_state(&"entity-1".to_string()),
        Some(ShardEntityState::Active)
    );
}

#[test]
fn shard_runtime_batches_remember_starts_while_update_is_in_progress() {
    let mut runtime = ShardRuntime::<String>::new_with_remember_entities("shard-1", 10);
    let entity_1_update =
        RememberShardUpdate::new(["entity-1".to_string()], std::iter::empty::<String>());
    let entity_2_update =
        RememberShardUpdate::new(["entity-2".to_string()], std::iter::empty::<String>());

    assert_eq!(
        runtime.deliver(ShardingEnvelope::new("entity-1", "first".to_string())),
        ShardDeliverPlan::RememberUpdate {
            update: entity_1_update.clone(),
        }
    );
    assert_eq!(
        runtime.deliver(ShardingEnvelope::new("entity-2", "second".to_string())),
        ShardDeliverPlan::Buffered {
            entity_id: "entity-2".to_string(),
        }
    );
    assert_eq!(runtime.total_buffered_count(), 2);

    assert_eq!(
        runtime.remember_update_done(entity_1_update),
        RememberUpdateDonePlan {
            deliveries: vec![crate::EntityDelivery::new("entity-1", "first".to_string())],
            next_update: Some(entity_2_update.clone()),
        }
    );
    assert_eq!(
        runtime.entity_state(&"entity-1".to_string()),
        Some(ShardEntityState::Active)
    );
    assert_eq!(runtime.entity_state(&"entity-2".to_string()), None);
    assert!(runtime.remember_update_in_progress());

    assert_eq!(
        runtime.remember_update_done(entity_2_update),
        RememberUpdateDonePlan {
            deliveries: vec![crate::EntityDelivery::new("entity-2", "second".to_string())],
            next_update: None,
        }
    );
    assert_eq!(
        runtime.entity_state(&"entity-2".to_string()),
        Some(ShardEntityState::Active)
    );
    assert_eq!(runtime.total_buffered_count(), 0);
}

#[test]
fn shard_actor_completes_remember_update_before_buffered_delivery() {
    let kit = kairo_testkit::ActorSystemTestKit::new("shard-actor-remember-update").unwrap();
    let shard = kit
        .system()
        .spawn(
            "shard",
            ShardActor::<String>::props_with_remember_entities("shard-1", 10),
        )
        .unwrap();
    let deliveries = kit
        .create_probe::<ShardDeliverPlan<String>>("deliveries")
        .unwrap();
    let done = kit
        .create_probe::<RememberUpdateDonePlan<String>>("remember-done")
        .unwrap();
    let update = RememberShardUpdate::new(["entity-1".to_string()], std::iter::empty::<String>());

    shard
        .tell(ShardMsg::Deliver {
            message: ShardingEnvelope::new("entity-1", "first".to_string()),
            reply_to: deliveries.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        deliveries.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardDeliverPlan::RememberUpdate {
            update: update.clone(),
        }
    );

    shard
        .tell(ShardMsg::RememberUpdateDone {
            update,
            reply_to: done.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        done.expect_msg(Duration::from_millis(500)).unwrap(),
        RememberUpdateDonePlan {
            deliveries: vec![crate::EntityDelivery::new("entity-1", "first".to_string())],
            next_update: None,
        }
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn shard_runtime_remember_entities_writes_stop_after_passivated_termination() {
    let mut runtime = ShardRuntime::<String>::new_with_remember_entities("shard-1", 10);
    runtime.recover_remembered_entities(["entity-1".to_string()]);
    runtime.passivate("entity-1", "stop".to_string());
    let stop_update =
        RememberShardUpdate::new(std::iter::empty::<String>(), ["entity-1".to_string()]);

    assert_eq!(
        runtime.entity_terminated("entity-1"),
        crate::EntityTerminatedPlan::RememberUpdate {
            update: stop_update.clone(),
        }
    );
    assert_eq!(
        runtime.entity_state(&"entity-1".to_string()),
        Some(ShardEntityState::RememberingStop)
    );

    assert_eq!(
        runtime.remember_update_done(stop_update),
        RememberUpdateDonePlan {
            deliveries: Vec::new(),
            next_update: None,
        }
    );
    assert_eq!(runtime.entity_state(&"entity-1".to_string()), None);
    assert!(!runtime.remember_update_in_progress());
}

#[test]
fn shard_runtime_remember_entities_restarts_buffered_after_stop_update() {
    let mut runtime = ShardRuntime::<String>::new_with_remember_entities("shard-1", 10);
    runtime.recover_remembered_entities(["entity-1".to_string()]);
    runtime.passivate("entity-1", "stop".to_string());
    assert_eq!(
        runtime.deliver(ShardingEnvelope::new("entity-1", "next".to_string())),
        ShardDeliverPlan::Buffered {
            entity_id: "entity-1".to_string(),
        }
    );
    let stop_update =
        RememberShardUpdate::new(std::iter::empty::<String>(), ["entity-1".to_string()]);
    let start_update =
        RememberShardUpdate::new(["entity-1".to_string()], std::iter::empty::<String>());

    assert_eq!(
        runtime.entity_terminated("entity-1"),
        crate::EntityTerminatedPlan::RememberUpdate {
            update: stop_update.clone(),
        }
    );
    assert_eq!(
        runtime.remember_update_done(stop_update),
        RememberUpdateDonePlan {
            deliveries: Vec::new(),
            next_update: Some(start_update.clone()),
        }
    );
    assert_eq!(runtime.entity_state(&"entity-1".to_string()), None);
    assert_eq!(runtime.buffered_count(&"entity-1".to_string()), 1);

    assert_eq!(
        runtime.remember_update_done(start_update),
        RememberUpdateDonePlan {
            deliveries: vec![crate::EntityDelivery::new("entity-1", "next".to_string())],
            next_update: None,
        }
    );
    assert_eq!(
        runtime.entity_state(&"entity-1".to_string()),
        Some(ShardEntityState::Active)
    );
}

#[test]
fn shard_runtime_batches_remember_stop_while_start_update_is_in_progress() {
    let mut runtime = ShardRuntime::<String>::new_with_remember_entities("shard-1", 10);
    runtime.recover_remembered_entities(["entity-1".to_string()]);
    runtime.passivate("entity-1", "stop".to_string());
    let start_update =
        RememberShardUpdate::new(["entity-2".to_string()], std::iter::empty::<String>());
    let stop_update =
        RememberShardUpdate::new(std::iter::empty::<String>(), ["entity-1".to_string()]);

    assert_eq!(
        runtime.deliver(ShardingEnvelope::new("entity-2", "first".to_string())),
        ShardDeliverPlan::RememberUpdate {
            update: start_update.clone(),
        }
    );
    assert_eq!(
        runtime.entity_terminated("entity-1"),
        crate::EntityTerminatedPlan::RememberUpdateQueued {
            entity_id: "entity-1".to_string(),
        }
    );

    assert_eq!(
        runtime.remember_update_done(start_update),
        RememberUpdateDonePlan {
            deliveries: vec![crate::EntityDelivery::new("entity-2", "first".to_string())],
            next_update: Some(stop_update.clone()),
        }
    );
    assert_eq!(
        runtime.remember_update_done(stop_update),
        RememberUpdateDonePlan {
            deliveries: Vec::new(),
            next_update: None,
        }
    );
    assert_eq!(runtime.entity_state(&"entity-1".to_string()), None);
    assert_eq!(
        runtime.entity_state(&"entity-2".to_string()),
        Some(ShardEntityState::Active)
    );
}

#[test]
fn shard_actor_completes_remember_stop_update_before_removal() {
    let kit = kairo_testkit::ActorSystemTestKit::new("shard-actor-remember-stop").unwrap();
    let shard = kit
        .system()
        .spawn(
            "shard",
            ShardActor::<String>::props_with_remember_entities("shard-1", 10),
        )
        .unwrap();
    let recovery = kit
        .create_probe::<RememberedEntitiesPlan>("recovery")
        .unwrap();
    let passivation = kit
        .create_probe::<crate::PassivatePlan<String>>("passivation")
        .unwrap();
    let termination = kit
        .create_probe::<crate::EntityTerminatedPlan<String>>("termination")
        .unwrap();
    let done = kit
        .create_probe::<RememberUpdateDonePlan<String>>("remember-done")
        .unwrap();
    let stop_update =
        RememberShardUpdate::new(std::iter::empty::<String>(), ["entity-1".to_string()]);

    shard
        .tell(ShardMsg::RecoverRememberedEntities {
            entities: vec!["entity-1".to_string()],
            reply_to: recovery.actor_ref(),
        })
        .unwrap();
    recovery.expect_msg(Duration::from_millis(500)).unwrap();
    shard
        .tell(ShardMsg::Passivate {
            entity_id: "entity-1".to_string(),
            stop_message: "stop".to_string(),
            reply_to: passivation.actor_ref(),
        })
        .unwrap();
    passivation.expect_msg(Duration::from_millis(500)).unwrap();
    shard
        .tell(ShardMsg::EntityTerminated {
            entity_id: "entity-1".to_string(),
            reply_to: termination.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        termination.expect_msg(Duration::from_millis(500)).unwrap(),
        crate::EntityTerminatedPlan::RememberUpdate {
            update: stop_update.clone(),
        }
    );

    shard
        .tell(ShardMsg::RememberUpdateDone {
            update: stop_update,
            reply_to: done.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        done.expect_msg(Duration::from_millis(500)).unwrap(),
        RememberUpdateDonePlan {
            deliveries: Vec::new(),
            next_update: None,
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

struct RebalanceThenAllocateStrategy {
    rebalance_shards: BTreeSet<String>,
    allocation_region: String,
}

impl RebalanceThenAllocateStrategy {
    fn new<const N: usize>(rebalance_shards: [&str; N], allocation_region: &str) -> Self {
        Self {
            rebalance_shards: rebalance_shards.into_iter().map(str::to_string).collect(),
            allocation_region: allocation_region.to_string(),
        }
    }
}

impl ShardAllocationStrategy for RebalanceThenAllocateStrategy {
    fn allocate_shard(
        &self,
        _requester: &String,
        _shard: &String,
        _current: &ShardAllocations,
    ) -> Result<String, ShardingError> {
        Ok(self.allocation_region.clone())
    }

    fn rebalance(
        &self,
        _current: &ShardAllocations,
        _in_progress: &BTreeSet<String>,
    ) -> Result<BTreeSet<String>, ShardingError> {
        Ok(self.rebalance_shards.clone())
    }
}

struct DelayedRegistrationCoordinator {
    pending_registration: Option<ActorRef<Result<CoordinatorStateSnapshot, ShardingError>>>,
    request_tx: mpsc::Sender<String>,
}

impl Actor for DelayedRegistrationCoordinator {
    type Msg = ShardCoordinatorMsg<String>;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            ShardCoordinatorMsg::RegisterLocalRegion { reply_to, .. } => {
                self.pending_registration = Some(reply_to);
            }
            ShardCoordinatorMsg::SetAllRegionsRegistered {
                all_registered: true,
            } => {
                if let Some(reply_to) = self.pending_registration.take() {
                    let _ = reply_to.tell(Ok(CoordinatorStateSnapshot {
                        allocations: BTreeMap::from([("region-a".to_string(), Vec::new())]),
                        proxies: BTreeSet::new(),
                        unallocated_shards: BTreeSet::new(),
                        rebalance_in_progress: BTreeMap::new(),
                        remember_entities: false,
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

struct RegionProbe {
    observed: mpsc::Sender<(String, &'static str)>,
}

struct RecordingEntity {
    entity_id: String,
    observed: mpsc::Sender<(String, String)>,
}

impl Actor for RecordingEntity {
    type Msg = String;

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        self.observed
            .send((self.entity_id.clone(), msg.clone()))
            .map_err(|error| ActorError::Message(error.to_string()))?;
        if msg == "stop" {
            ctx.stop(ctx.myself())?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RemoteRouteMessage(String);

impl RemoteMessage for RemoteRouteMessage {
    const MANIFEST: &'static str = "kairo.sharding.test.remote-route";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, Copy)]
struct RemoteRouteMessageCodec;

impl MessageCodec<RemoteRouteMessage> for RemoteRouteMessageCodec {
    fn serializer_id(&self) -> u32 {
        49_001
    }

    fn encode(&self, message: &RemoteRouteMessage) -> kairo_serialization::Result<Bytes> {
        let mut writer = WireWriter::new();
        writer.write_string(&message.0)?;
        Ok(writer.finish())
    }

    fn decode(
        &self,
        payload: Bytes,
        version: u16,
    ) -> kairo_serialization::Result<RemoteRouteMessage> {
        assert_eq!(version, RemoteRouteMessage::VERSION);
        let mut reader = WireReader::new(&payload);
        Ok(RemoteRouteMessage(reader.read_string()?))
    }
}

struct RecordingRemoteEntity {
    entity_id: String,
    observed: mpsc::Sender<(String, String)>,
}

impl Actor for RecordingRemoteEntity {
    type Msg = RemoteRouteMessage;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        self.observed
            .send((self.entity_id.clone(), msg.0))
            .map_err(|error| ActorError::Message(error.to_string()))
    }
}

fn remote_node(system: &str, host: &str, port: u16) -> UniqueAddress {
    UniqueAddress::new(
        Address::new("kairo", system, Some(host.to_string()), Some(port)),
        1,
    )
}

fn remote_unique_node(system: &str, host: &str, port: u16, uid: u64) -> UniqueAddress {
    UniqueAddress::new(
        Address::new("kairo", system, Some(host.to_string()), Some(port)),
        uid,
    )
}

fn cluster_member(
    unique_address: UniqueAddress,
    status: MemberStatus,
    roles: impl IntoIterator<Item = &'static str>,
    up_number: u64,
) -> Member {
    Member::new(
        unique_address,
        roles.into_iter().map(ToString::to_string).collect(),
    )
    .with_status(status)
    .with_up_number(up_number)
}

fn cluster_state(members: Vec<Member>) -> CurrentClusterState {
    CurrentClusterState {
        members,
        unreachable: Vec::new(),
        seen_by: HashSet::new(),
        leader: None,
        role_leaders: HashMap::new(),
        member_tombstones: HashSet::new(),
    }
}

fn wait_for_local_shard(
    kit: &kairo_testkit::ActorSystemTestKit,
    region: &ActorRef<ShardRegionMsg<String>>,
    shard: &str,
) -> ActorRef<ShardMsg<String>> {
    let reply = kit
        .create_probe::<Option<ActorRef<ShardMsg<String>>>>("local-shard")
        .unwrap();
    for _ in 0..20 {
        region
            .tell(ShardRegionMsg::GetLocalShard {
                shard: shard.to_string(),
                reply_to: reply.actor_ref(),
            })
            .unwrap();
        if let Some(shard_ref) = reply.expect_msg(Duration::from_millis(500)).unwrap() {
            return shard_ref;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("timed out waiting for local shard `{shard}`");
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
