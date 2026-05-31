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
mod coordinator_actor;
mod coordinator_runtime;
mod coordinator_state;
mod entity_routing;
mod entity_shard_actor;
mod handoff_orchestration;
mod handoff_transport;
mod region_actor_handoff;
mod region_actor_local;
mod region_registration;
mod region_remote_coordinator_actor;
mod region_route_resolution;
mod region_runtime;
mod remember;
mod shard_remember_runtime;
mod shard_runtime;

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
