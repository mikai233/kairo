use super::*;

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
