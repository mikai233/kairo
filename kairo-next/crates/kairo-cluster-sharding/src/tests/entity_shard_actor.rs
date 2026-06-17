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
    for _ in 0..20 {
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
        if new_ref.is_some() && replayed.is_some() {
            break;
        }
    }

    let restarted_ref = new_ref.expect("buffered delivery should start a new child incarnation");
    assert_ne!(first_ref.path(), restarted_ref.path());
    assert_eq!(
        replayed.expect("buffered delivery should be replayed to the restarted child"),
        ("entity-1".to_string(), "buffered".to_string())
    );
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
fn entity_shard_actor_restart_remembered_entity_spawns_child() {
    let kit =
        kairo_testkit::ActorSystemTestKit::new("entity-shard-actor-remember-restart").unwrap();
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

    let mut restart_plan = None;
    for _ in 0..20 {
        shard
            .tell(ShardMsg::RestartRememberedEntity {
                entity_id: "entity-1".to_string(),
                reply_to: restart.actor_ref(),
            })
            .unwrap();
        let plan = restart.expect_msg(Duration::from_millis(500)).unwrap();
        match plan {
            RestartRememberedEntityPlan::Started { .. } => {
                restart_plan = Some(plan);
                break;
            }
            RestartRememberedEntityPlan::AlreadyActive { .. } => {
                std::thread::sleep(Duration::from_millis(10));
            }
            RestartRememberedEntityPlan::Ignored { .. } => {
                panic!("remembered entity restart should not be ignored");
            }
        }
    }
    assert_eq!(
        restart_plan.expect("remembered restart should eventually start after termination"),
        RestartRememberedEntityPlan::Started {
            entity_id: "entity-1".to_string(),
        }
    );
    let restarted_ref = refs_rx
        .recv_timeout(Duration::from_millis(500))
        .expect("remembered restart should spawn a child entity");
    assert_ne!(initial_ref.path(), restarted_ref.path());

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
