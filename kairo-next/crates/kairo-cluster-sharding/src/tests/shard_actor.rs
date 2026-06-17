use super::*;
use crate::RestartRememberedEntityPlan;

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
    let state = kit.create_probe::<ShardSnapshot>("state").unwrap();
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
        .tell(ShardMsg::GetState {
            reply_to: state.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        state.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardSnapshot {
            shard_id: "shard-1".to_string(),
            active_entities: Vec::new(),
            entity_count: 1,
            total_buffered: 0,
            handoff_in_progress: false,
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
    shard
        .tell(ShardMsg::GetState {
            reply_to: state.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        state.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardSnapshot {
            shard_id: "shard-1".to_string(),
            active_entities: Vec::new(),
            entity_count: 0,
            total_buffered: 0,
            handoff_in_progress: false,
        }
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn shard_actor_defers_handoff_until_pending_remember_stop_update_completes() {
    let kit = kairo_testkit::ActorSystemTestKit::new("shard-actor-handoff-remember-stop").unwrap();
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
    let handoff = kit
        .create_probe::<ShardHandOffPlan<String>>("handoff")
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
        .tell(ShardMsg::HandOff {
            stop_message: "handoff-stop".to_string(),
            reply_to: handoff.actor_ref(),
        })
        .unwrap();
    handoff.expect_no_msg(Duration::from_millis(30)).unwrap();

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
    assert_eq!(
        handoff.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardHandOffPlan::ReplyShardStopped {
            shard: "shard-1".to_string(),
            stopped: ShardStopped {
                shard_id: "shard-1".to_string(),
            },
        }
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn shard_actor_remembered_entity_waits_for_restart_after_unexpected_termination() {
    let kit = kairo_testkit::ActorSystemTestKit::new("shard-actor-remember-restart").unwrap();
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
    let termination = kit
        .create_probe::<crate::EntityTerminatedPlan<String>>("termination")
        .unwrap();
    let state = kit.create_probe::<ShardSnapshot>("state").unwrap();
    let deliveries = kit
        .create_probe::<ShardDeliverPlan<String>>("deliveries")
        .unwrap();
    let restart = kit
        .create_probe::<RestartRememberedEntityPlan>("restart")
        .unwrap();

    shard
        .tell(ShardMsg::RecoverRememberedEntities {
            entities: vec!["entity-1".to_string()],
            reply_to: recovery.actor_ref(),
        })
        .unwrap();
    recovery.expect_msg(Duration::from_millis(500)).unwrap();
    shard
        .tell(ShardMsg::EntityTerminated {
            entity_id: "entity-1".to_string(),
            reply_to: termination.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        termination.expect_msg(Duration::from_millis(500)).unwrap(),
        crate::EntityTerminatedPlan::RestartRemembered {
            entity_id: "entity-1".to_string(),
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
            active_entities: Vec::new(),
            entity_count: 1,
            total_buffered: 0,
            handoff_in_progress: false,
        }
    );

    shard
        .tell(ShardMsg::RestartRememberedEntity {
            entity_id: "entity-1".to_string(),
            reply_to: restart.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        restart.expect_msg(Duration::from_millis(500)).unwrap(),
        RestartRememberedEntityPlan::Started {
            entity_id: "entity-1".to_string(),
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

    shard
        .tell(ShardMsg::Deliver {
            message: ShardingEnvelope::new("entity-1", "after-restart".to_string()),
            reply_to: deliveries.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        deliveries.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardDeliverPlan::Deliver {
            delivery: crate::EntityDelivery::new("entity-1", "after-restart".to_string()),
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
