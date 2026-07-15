use super::*;
use crate::RestartRememberedEntityPlan;

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
fn entity_shard_actor_buffers_passivating_delivery_and_restarts_child_after_termination() {
    let kit =
        kairo_testkit::ActorSystemTestKit::new("entity-shard-actor-passivation-restart").unwrap();
    let (observed_tx, observed_rx) = mpsc::channel();
    let (refs_tx, refs_rx) = mpsc::channel();
    let factory = EntityActorFactory::new(move |entity_id| ControlledEntity {
        entity_id,
        observed: observed_tx.clone(),
        refs: refs_tx.clone(),
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
    let first_ref = refs_rx.recv_timeout(Duration::from_millis(500)).unwrap();
    assert_eq!(
        observed_rx
            .recv_timeout(Duration::from_millis(500))
            .unwrap(),
        ("entity-1".to_string(), "first".to_string())
    );

    shard
        .tell(ShardMsg::Passivate {
            entity_id: "entity-1".to_string(),
            stop_message: "prepare-stop".to_string(),
            reply_to: passivation.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        passivation.expect_msg(Duration::from_millis(500)).unwrap(),
        PassivatePlan::SendStop {
            entity_id: "entity-1".to_string(),
            stop_message: "prepare-stop".to_string(),
        }
    );
    assert_eq!(
        observed_rx
            .recv_timeout(Duration::from_millis(500))
            .unwrap(),
        ("entity-1".to_string(), "prepare-stop".to_string())
    );

    shard
        .tell(ShardMsg::Deliver {
            message: ShardingEnvelope::new("entity-1", "buffered".to_string()),
            reply_to: deliveries.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        deliveries.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardDeliverPlan::Buffered {
            entity_id: "entity-1".to_string()
        }
    );
    assert!(
        observed_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err(),
        "buffered delivery must not reach the passivating child"
    );

    first_ref.tell("stop".to_string()).unwrap();
    assert_eq!(
        observed_rx
            .recv_timeout(Duration::from_millis(500))
            .unwrap(),
        ("entity-1".to_string(), "stop".to_string())
    );
    assert!(first_ref.wait_for_stop(Duration::from_secs(1)));

    let mut new_ref = None;
    let mut replayed = None;
    let (restarted_ref, replayed) = kairo_testkit::await_assert(
        Duration::from_millis(2_200),
        Duration::from_millis(10),
        || -> Result<(ActorRef<String>, (String, String)), String> {
            if new_ref.is_none()
                && let Ok(ref_after_restart) = refs_rx.recv_timeout(Duration::from_millis(50))
            {
                new_ref = Some(ref_after_restart);
            }
            if replayed.is_none()
                && let Ok(observed) = observed_rx.recv_timeout(Duration::from_millis(50))
            {
                replayed = Some(observed);
            }
            match (new_ref.clone(), replayed.clone()) {
                (Some(restarted_ref), Some(replayed)) => Ok((restarted_ref, replayed)),
                (maybe_ref, maybe_replayed) => Err(format!(
                    "buffered delivery should restart child and replay message; \
                     new_ref observed: {}; replayed observed: {}",
                    maybe_ref.is_some(),
                    maybe_replayed.is_some()
                )),
            }
        },
    )
    .unwrap();

    assert_ne!(first_ref.path(), restarted_ref.path());
    assert_eq!(replayed, ("entity-1".to_string(), "buffered".to_string()));
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn entity_shard_actor_recovery_starts_remembered_entities() {
    let kit = kairo_testkit::ActorSystemTestKit::new("entity-shard-actor-recover-starts").unwrap();
    let (observed_tx, observed_rx) = mpsc::channel();
    let (refs_tx, refs_rx) = mpsc::channel();
    let factory = EntityActorFactory::new(move |entity_id| ControlledEntity {
        entity_id,
        observed: observed_tx.clone(),
        refs: refs_tx.clone(),
    });
    let shard = kit
        .system()
        .spawn(
            "shard",
            EntityShardActor::props_with_remember_entities("shard-1", 10, factory),
        )
        .unwrap();
    let recovery = kit
        .create_probe::<RememberedEntitiesPlan>("recovery")
        .unwrap();
    let delivery = kit
        .create_probe::<ShardDeliverPlan<String>>("delivery")
        .unwrap();

    shard
        .tell(ShardMsg::RecoverRememberedEntities {
            entities: vec![
                "entity-b".to_string(),
                "entity-a".to_string(),
                String::new(),
            ],
            reply_to: recovery.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        recovery.expect_msg(Duration::from_millis(500)).unwrap(),
        RememberedEntitiesPlan {
            started: vec!["entity-a".to_string(), "entity-b".to_string()],
            already_active: Vec::new(),
            ignored_empty: 1,
        }
    );

    let first_ref = refs_rx
        .recv_timeout(Duration::from_millis(500))
        .expect("first remembered entity should start during recovery");
    let second_ref = refs_rx
        .recv_timeout(Duration::from_millis(500))
        .expect("second remembered entity should start during recovery");
    assert_ne!(first_ref.path(), second_ref.path());

    shard
        .tell(ShardMsg::Deliver {
            message: ShardingEnvelope::new("entity-a", "after-recovery".to_string()),
            reply_to: delivery.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        delivery.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardDeliverPlan::Deliver {
            delivery: EntityDelivery::new("entity-a", "after-recovery".to_string()),
        }
    );
    assert_eq!(
        observed_rx
            .recv_timeout(Duration::from_millis(500))
            .unwrap(),
        ("entity-a".to_string(), "after-recovery".to_string())
    );
    assert!(
        refs_rx.recv_timeout(Duration::from_millis(100)).is_err(),
        "delivery to a recovered entity should reuse the recovery-started child"
    );

    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn entity_shard_actor_recovers_entities_from_ddata_store_before_delivery() {
    let kit = kairo_testkit::ActorSystemTestKit::new("entity-shard-actor-ddata-recovery").unwrap();
    let replicator = kit
        .system()
        .spawn(
            "replicator",
            Props::new(ReplicatorActor::<ORSet<String>>::new),
        )
        .unwrap();
    let (observed_tx, observed_rx) = mpsc::channel();
    let (refs_tx, refs_rx) = mpsc::channel();
    let factory = EntityActorFactory::new(move |entity_id| ControlledEntity {
        entity_id,
        observed: observed_tx.clone(),
        refs: refs_tx.clone(),
    });
    let props = || {
        EntityShardActor::props_with_ddata_remember_store(
            "orders",
            "shard-1",
            10,
            factory.clone(),
            ReplicaId::new("node-a"),
            replicator.clone(),
            Duration::from_millis(500),
        )
    };
    let shard = kit.system().spawn("shard", props()).unwrap();
    let deliveries = kit
        .create_probe::<ShardDeliverPlan<String>>("deliveries")
        .unwrap();

    shard
        .tell(ShardMsg::Deliver {
            message: ShardingEnvelope::new("entity-1", "first".to_string()),
            reply_to: deliveries.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        deliveries.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardDeliverPlan::RememberUpdate {
            update: RememberShardUpdate::new(["entity-1".to_string()], std::iter::empty())
        }
    );
    let first_ref = refs_rx
        .recv_timeout(Duration::from_millis(500))
        .expect("persisted entity should start after the ddata update");
    assert_eq!(
        observed_rx
            .recv_timeout(Duration::from_millis(500))
            .unwrap(),
        ("entity-1".to_string(), "first".to_string())
    );

    kit.system().stop(&shard);
    assert!(shard.wait_for_stop(Duration::from_secs(1)));
    assert!(first_ref.wait_for_stop(Duration::from_secs(1)));

    let recovered = kit.system().spawn("shard", props()).unwrap();
    let recovered_ref = refs_rx
        .recv_timeout(Duration::from_millis(500))
        .expect("remembered entity should start during ddata recovery");
    assert_ne!(first_ref.path(), recovered_ref.path());
    recovered
        .tell(ShardMsg::Deliver {
            message: ShardingEnvelope::new("entity-1", "after-recovery".to_string()),
            reply_to: deliveries.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        deliveries.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardDeliverPlan::Deliver {
            delivery: EntityDelivery::new("entity-1", "after-recovery".to_string())
        }
    );
    assert_eq!(
        observed_rx
            .recv_timeout(Duration::from_millis(500))
            .unwrap(),
        ("entity-1".to_string(), "after-recovery".to_string())
    );

    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn entity_shard_actor_stops_child_for_moved_remembered_entity() {
    let kit =
        kairo_testkit::ActorSystemTestKit::new("entity-shard-actor-moved-remembered").unwrap();
    let (observed_tx, _observed_rx) = mpsc::channel();
    let (refs_tx, refs_rx) = mpsc::channel();
    let factory = EntityActorFactory::new(move |entity_id| ControlledEntity {
        entity_id,
        observed: observed_tx.clone(),
        refs: refs_tx.clone(),
    });
    let shard = kit
        .system()
        .spawn(
            "shard",
            EntityShardActor::props_with_remember_entities("shard-1", 10, factory),
        )
        .unwrap();
    let recovery = kit
        .create_probe::<RememberedEntitiesPlan>("recovery")
        .unwrap();
    let moved = kit
        .create_probe::<MovedRememberedEntitiesPlan>("moved")
        .unwrap();
    let state = kit.create_probe::<ShardSnapshot>("state").unwrap();

    shard
        .tell(ShardMsg::RecoverRememberedEntities {
            entities: vec!["entity-1".to_string()],
            reply_to: recovery.actor_ref(),
        })
        .unwrap();
    recovery.expect_msg(Duration::from_millis(500)).unwrap();
    let first_ref = refs_rx
        .recv_timeout(Duration::from_millis(500))
        .expect("recovery should start the remembered child");

    shard
        .tell(ShardMsg::RememberedEntitiesMovedToOtherShard {
            entities: vec!["entity-1".to_string()],
            reply_to: moved.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        moved.expect_msg(Duration::from_millis(500)).unwrap(),
        MovedRememberedEntitiesPlan {
            removed: vec!["entity-1".to_string()],
            ignored: Vec::new(),
            update: Some(RememberShardUpdate::new(
                std::iter::empty::<String>(),
                ["entity-1".to_string()],
            )),
        }
    );

    assert!(first_ref.wait_for_stop(Duration::from_secs(1)));
    shard
        .tell(ShardMsg::GetState {
            reply_to: state.actor_ref(),
        })
        .unwrap();
    let snapshot = state.expect_msg(Duration::from_millis(500)).unwrap();
    assert!(snapshot.active_entities.is_empty());
    assert_eq!(snapshot.entity_count, 0);

    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn entity_shard_actor_automatically_restarts_remembered_entity_after_unexpected_stop() {
    let kit =
        kairo_testkit::ActorSystemTestKit::new("entity-shard-actor-auto-remember-restart").unwrap();
    let (observed_tx, observed_rx) = mpsc::channel();
    let (refs_tx, refs_rx) = mpsc::channel();
    let factory = EntityActorFactory::new(move |entity_id| ControlledEntity {
        entity_id,
        observed: observed_tx.clone(),
        refs: refs_tx.clone(),
    });
    let shard = kit
        .system()
        .spawn(
            "shard",
            EntityShardActor::props_with_remember_entities("shard-1", 10, factory),
        )
        .unwrap();
    let recovery = kit
        .create_probe::<RememberedEntitiesPlan>("recovery")
        .unwrap();
    let restart = kit
        .create_probe::<RestartRememberedEntityPlan>("restart")
        .unwrap();
    let delivery = kit
        .create_probe::<ShardDeliverPlan<String>>("delivery")
        .unwrap();

    shard
        .tell(ShardMsg::RecoverRememberedEntities {
            entities: vec!["entity-1".to_string()],
            reply_to: recovery.actor_ref(),
        })
        .unwrap();
    recovery.expect_msg(Duration::from_millis(500)).unwrap();

    let initial_ref = refs_rx
        .recv_timeout(Duration::from_millis(500))
        .expect("recovery should start the remembered child");
    initial_ref.tell("stop".to_string()).unwrap();
    assert_eq!(
        observed_rx
            .recv_timeout(Duration::from_millis(500))
            .unwrap(),
        ("entity-1".to_string(), "stop".to_string())
    );
    assert!(initial_ref.wait_for_stop(Duration::from_secs(1)));

    let restarted_ref = refs_rx
        .recv_timeout(Duration::from_millis(500))
        .expect("remembered entity should be restarted automatically after termination");
    assert_ne!(initial_ref.path(), restarted_ref.path());

    shard
        .tell(ShardMsg::RestartRememberedEntity {
            entity_id: "entity-1".to_string(),
            reply_to: restart.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        restart.expect_msg(Duration::from_millis(500)).unwrap(),
        RestartRememberedEntityPlan::AlreadyActive {
            entity_id: "entity-1".to_string(),
        }
    );

    shard
        .tell(ShardMsg::Deliver {
            message: ShardingEnvelope::new("entity-1", "after-restart".to_string()),
            reply_to: delivery.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        delivery.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardDeliverPlan::Deliver {
            delivery: EntityDelivery::new("entity-1", "after-restart".to_string()),
        }
    );

    assert_eq!(
        observed_rx
            .recv_timeout(Duration::from_millis(500))
            .unwrap(),
        ("entity-1".to_string(), "after-restart".to_string())
    );
    assert!(
        !restarted_ref.is_stopped(),
        "restarted remembered entity child should remain live"
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
    shard
        .tell(ShardMsg::HandOffStopperTerminated {
            reply_to: stopper.actor_ref(),
        })
        .unwrap();

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

    assert!(stopper.expect_msg(Duration::from_millis(500)).unwrap());
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

struct ControlledEntity {
    entity_id: String,
    observed: mpsc::Sender<(String, String)>,
    refs: mpsc::Sender<ActorRef<String>>,
}

impl Actor for ControlledEntity {
    type Msg = String;

    fn started(&mut self, ctx: &mut Context<Self::Msg>) -> ActorResult {
        self.refs
            .send(ctx.myself().clone())
            .map_err(|error| ActorError::Message(error.to_string()))
    }

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
