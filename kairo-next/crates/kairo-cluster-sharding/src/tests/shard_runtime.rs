use super::*;

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
fn shard_runtime_does_not_restart_remembered_entity_during_handoff() {
    let mut runtime = ShardRuntime::new_with_remember_entities("shard-1", 10);
    runtime.recover_remembered_entities(["entity-1".to_string()]);
    assert!(matches!(
        runtime.handoff("stop"),
        ShardHandOffPlan::StartEntityStopper { .. }
    ));

    assert_eq!(
        runtime.entity_terminated("entity-1"),
        crate::EntityTerminatedPlan::Removed {
            entity_id: "entity-1".to_string(),
        }
    );
    assert_eq!(runtime.entity_state(&"entity-1".to_string()), None);
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
