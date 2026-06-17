use super::*;
use crate::{RestartRememberedEntityIgnoreReason, RestartRememberedEntityPlan};

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
fn shard_runtime_remember_entities_waits_for_restart_after_unexpected_termination() {
    let mut runtime = ShardRuntime::<String>::new_with_remember_entities("shard-1", 10);
    runtime.recover_remembered_entities(["entity-1".to_string()]);

    assert_eq!(
        runtime.entity_terminated("entity-1"),
        crate::EntityTerminatedPlan::RestartRemembered {
            entity_id: "entity-1".to_string(),
        }
    );
    assert_eq!(
        runtime.entity_state(&"entity-1".to_string()),
        Some(ShardEntityState::WaitingForRestart)
    );
    assert!(!runtime.remember_update_in_progress());
    assert_eq!(runtime.active_entity_ids(), Vec::<String>::new());
}

#[test]
fn shard_runtime_remember_entities_restarts_waiting_entity_on_next_message() {
    let mut runtime = ShardRuntime::<String>::new_with_remember_entities("shard-1", 10);
    runtime.recover_remembered_entities(["entity-1".to_string()]);
    runtime.entity_terminated("entity-1");

    assert_eq!(
        runtime.deliver(ShardingEnvelope::new("entity-1", "restart".to_string())),
        ShardDeliverPlan::StartEntity {
            delivery: crate::EntityDelivery::new("entity-1", "restart".to_string()),
        }
    );
    assert_eq!(
        runtime.entity_state(&"entity-1".to_string()),
        Some(ShardEntityState::Active)
    );
    assert!(!runtime.remember_update_in_progress());
}

#[test]
fn shard_runtime_restart_remembered_entity_starts_waiting_entity() {
    let mut runtime = ShardRuntime::<String>::new_with_remember_entities("shard-1", 10);
    runtime.recover_remembered_entities(["entity-1".to_string()]);
    runtime.entity_terminated("entity-1");

    assert_eq!(
        runtime.restart_remembered_entity("entity-1"),
        RestartRememberedEntityPlan::Started {
            entity_id: "entity-1".to_string(),
        }
    );
    assert_eq!(
        runtime.entity_state(&"entity-1".to_string()),
        Some(ShardEntityState::Active)
    );
    assert_eq!(
        runtime.restart_remembered_entity("entity-1"),
        RestartRememberedEntityPlan::AlreadyActive {
            entity_id: "entity-1".to_string(),
        }
    );
    assert!(!runtime.remember_update_in_progress());
}

#[test]
fn shard_runtime_restart_remembered_entity_ignores_non_waiting_states() {
    let mut non_remembering = ShardRuntime::<String>::new("shard-1", 10);
    non_remembering.deliver(ShardingEnvelope::new("entity-1", "first".to_string()));
    assert_eq!(
        non_remembering.restart_remembered_entity("entity-1"),
        RestartRememberedEntityPlan::Ignored {
            entity_id: "entity-1".to_string(),
            reason: RestartRememberedEntityIgnoreReason::NotRememberingEntities,
        }
    );

    let mut remembering = ShardRuntime::<String>::new_with_remember_entities("shard-1", 10);
    remembering.recover_remembered_entities(["entity-1".to_string()]);
    remembering.passivate("entity-1", "stop".to_string());
    assert_eq!(
        remembering.restart_remembered_entity("entity-1"),
        RestartRememberedEntityPlan::Ignored {
            entity_id: "entity-1".to_string(),
            reason: RestartRememberedEntityIgnoreReason::NotWaitingForRestart,
        }
    );
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
