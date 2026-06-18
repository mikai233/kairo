use super::*;

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
fn region_actor_remote_host_shard_spawns_store_backed_shard_and_recovers_entities() {
    let kit =
        kairo_testkit::ActorSystemTestKit::new("region-remote-host-remember-shard-child").unwrap();
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
    let remote_replies = kit
        .create_probe::<RemoteEnvelope>("remote-replies")
        .unwrap();
    let local_shard = kit
        .create_probe::<Option<kairo_actor::ActorRef<ShardMsg<String>>>>("local-shard")
        .unwrap();
    let deliveries = kit
        .create_probe::<ShardDeliverPlan<String>>("deliveries")
        .unwrap();
    let mut registry = Registry::new();
    register_sharding_protocol_codecs(&mut registry).unwrap();
    let registry = Arc::new(registry);
    let reply = crate::ShardRegionRemoteControlReplyTarget::new(
        ActorRefWireData::new("kairo://region@127.0.0.1:25520/system/sharding/region-a").unwrap(),
        ActorRefWireData::new("kairo://coordinator@127.0.0.1:25521/system/sharding/coordinator")
            .unwrap(),
        registry.clone(),
        remote_replies.actor_ref(),
    );

    region
        .tell(ShardRegionMsg::RemoteHostShard {
            shard: "shard-1".to_string(),
            reply,
        })
        .unwrap();

    let reply_envelope = remote_replies
        .expect_msg(Duration::from_millis(500))
        .unwrap();
    let started: ShardStarted = registry.deserialize(reply_envelope.message).unwrap();
    assert_eq!(started.shard_id, "shard-1");

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
fn region_actor_with_shared_remember_store_ref_recovers_and_persists_entities() {
    let kit = kairo_testkit::ActorSystemTestKit::new("region-actor-shared-store-ref").unwrap();
    let remember_store = kit
        .system()
        .spawn(
            "remember-store",
            RememberShardStoreActor::props(RememberShardStoreState::with_entities(
                "orders",
                "shard-1",
                ["entity-1".to_string()],
            )),
        )
        .unwrap();
    let region = kit
        .system()
        .spawn(
            "region",
            ShardRegionActor::<String>::props_with_remember_store_shards(
                "region-a",
                10,
                10,
                BTreeMap::from([("shard-1".to_string(), remember_store.clone())]),
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
    let store_state = kit
        .create_probe::<RememberShardStoreSnapshot>("store-state")
        .unwrap();
    let local_shard = kit
        .create_probe::<Option<ActorRef<ShardMsg<String>>>>("local-shard")
        .unwrap();
    let shard_state = kit.create_probe::<ShardSnapshot>("shard-state").unwrap();

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
            message: ShardingEnvelope::new("entity-1", "recovered-first".to_string()),
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
            delivery: crate::EntityDelivery::new("entity-1", "recovered-first".to_string()),
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
        .expect("hosted shard should remain local after shared store recovery");

    region
        .tell(ShardRegionMsg::RouteToLocalShard {
            shard: "shard-1".to_string(),
            message: ShardingEnvelope::new("entity-2", "first".to_string()),
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
        ShardDeliverPlan::RememberUpdate {
            update: RememberShardUpdate::new(
                ["entity-2".to_string()],
                std::iter::empty::<String>(),
            ),
        }
    );

    let (remembered, active_entities) = wait_for_remembered_entities_and_shard_activation(
        &remember_store,
        &store_state,
        &shard,
        &shard_state,
        "shared remember-store write should persist and activate entity-2",
    );
    assert_eq!(
        remembered,
        BTreeSet::from(["entity-1".to_string(), "entity-2".to_string()])
    );
    assert!(
        active_entities.contains(&"entity-2".to_string()),
        "shared remember-store write should activate the first-delivered entity"
    );

    region
        .tell(ShardRegionMsg::RouteToLocalShard {
            shard: "shard-1".to_string(),
            message: ShardingEnvelope::new("entity-2", "after-remember-start".to_string()),
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
            delivery: crate::EntityDelivery::new("entity-2", "after-remember-start".to_string(),),
        }
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

fn polling_timeout() -> Duration {
    Duration::from_millis(10_200)
}

fn wait_for_remembered_entities_and_shard_activation(
    remember_store: &ActorRef<RememberShardStoreMsg>,
    store_state: &kairo_testkit::TestProbe<RememberShardStoreSnapshot>,
    shard: &ActorRef<ShardMsg<String>>,
    shard_state: &kairo_testkit::TestProbe<ShardSnapshot>,
    description: &str,
) -> (BTreeSet<String>, Vec<String>) {
    kairo_testkit::await_assert(
        polling_timeout(),
        Duration::from_millis(10),
        || -> Result<(BTreeSet<String>, Vec<String>), String> {
            remember_store
                .tell(RememberShardStoreMsg::GetState {
                    reply_to: store_state.actor_ref(),
                })
                .map_err(|error| error.to_string())?;
            let store_snapshot = store_state
                .expect_msg(Duration::from_millis(500))
                .map_err(|error| error.to_string())?;
            let remembered = store_snapshot
                .entities_by_key
                .values()
                .flat_map(|entities| entities.iter().cloned())
                .collect::<BTreeSet<_>>();

            shard
                .tell(ShardMsg::GetState {
                    reply_to: shard_state.actor_ref(),
                })
                .map_err(|error| error.to_string())?;
            let shard_snapshot = shard_state
                .expect_msg(Duration::from_millis(500))
                .map_err(|error| error.to_string())?;
            let active_entities = shard_snapshot.active_entities;

            if remembered.contains("entity-1")
                && remembered.contains("entity-2")
                && active_entities.contains(&"entity-2".to_string())
            {
                Ok((remembered, active_entities))
            } else {
                Err(format!(
                    "{description}; remembered: {remembered:?}; active entities: {active_entities:?}",
                ))
            }
        },
    )
    .unwrap()
}

#[test]
fn region_actor_restarts_shared_store_remembered_local_shard_after_unexpected_stop() {
    let (kit, time) = kairo_testkit::ActorSystemTestKit::with_manual_time(
        "region-shared-store-remember-shard-restart",
    )
    .unwrap();
    let remember_store = kit
        .system()
        .spawn(
            "remember-store",
            RememberShardStoreActor::props(RememberShardStoreState::with_entities(
                "orders",
                "shard-1",
                ["entity-1".to_string()],
            )),
        )
        .unwrap();
    let region = kit
        .system()
        .spawn(
            "region",
            Props::new(move || {
                ShardRegionActor::<String>::new_with_remember_store_shards(
                    "region-a",
                    10,
                    10,
                    BTreeMap::from([("shard-1".to_string(), remember_store.clone())]),
                    Duration::from_millis(500),
                )
                .with_remember_shard_failure_backoff(Duration::from_secs(1))
            }),
        )
        .unwrap();
    let host = kit.create_probe::<HostShardPlan<String>>("host").unwrap();
    let local_shard = kit
        .create_probe::<Option<kairo_actor::ActorRef<ShardMsg<String>>>>("local-shard")
        .unwrap();
    let state = kit.create_probe::<ShardRegionSnapshot>("state").unwrap();
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
        .tell(ShardRegionMsg::GetLocalShard {
            shard: "shard-1".to_string(),
            reply_to: local_shard.actor_ref(),
        })
        .unwrap();
    let first_shard = local_shard
        .expect_msg(Duration::from_millis(500))
        .unwrap()
        .unwrap();

    kit.system().stop(&first_shard);
    assert!(first_shard.wait_for_stop(Duration::from_secs(1)));
    region
        .tell(ShardRegionMsg::MarkShardStopped {
            shard: "shard-1".to_string(),
            reply_to: Some(state.actor_ref()),
        })
        .unwrap();
    assert!(
        state
            .expect_msg(Duration::from_millis(500))
            .unwrap()
            .local_shards
            .is_empty()
    );

    time.advance(Duration::from_secs(1));
    region
        .tell(ShardRegionMsg::GetLocalShard {
            shard: "shard-1".to_string(),
            reply_to: local_shard.actor_ref(),
        })
        .unwrap();
    let restarted_shard = local_shard
        .expect_msg(Duration::from_millis(500))
        .unwrap()
        .unwrap();
    assert_ne!(first_shard.path(), restarted_shard.path());

    restarted_shard
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
fn region_actor_suppresses_shared_store_remembered_shard_restart_after_handoff_begins() {
    let (kit, time) = kairo_testkit::ActorSystemTestKit::with_manual_time(
        "region-shared-store-remember-shard-handoff-race",
    )
    .unwrap();
    let remember_store = kit
        .system()
        .spawn(
            "remember-store",
            RememberShardStoreActor::props(RememberShardStoreState::with_entities(
                "orders",
                "shard-1",
                ["entity-1".to_string()],
            )),
        )
        .unwrap();
    let region = kit
        .system()
        .spawn(
            "region",
            Props::new(move || {
                ShardRegionActor::<String>::new_with_remember_store_shards(
                    "region-a",
                    10,
                    10,
                    BTreeMap::from([("shard-1".to_string(), remember_store.clone())]),
                    Duration::from_millis(500),
                )
                .with_remember_shard_failure_backoff(Duration::from_secs(1))
            }),
        )
        .unwrap();
    let host = kit.create_probe::<HostShardPlan<String>>("host").unwrap();
    let begin = kit.create_probe::<BeginHandOffPlan>("begin").unwrap();
    let local_shard = kit
        .create_probe::<Option<kairo_actor::ActorRef<ShardMsg<String>>>>("local-shard")
        .unwrap();
    let state = kit.create_probe::<ShardRegionSnapshot>("state").unwrap();

    region
        .tell(ShardRegionMsg::HostShard {
            shard: "shard-1".to_string(),
            reply_to: host.actor_ref(),
        })
        .unwrap();
    host.expect_msg(Duration::from_millis(500)).unwrap();
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

    kit.system().stop(&shard);
    assert!(shard.wait_for_stop(Duration::from_secs(1)));
    region
        .tell(ShardRegionMsg::MarkShardStopped {
            shard: "shard-1".to_string(),
            reply_to: Some(state.actor_ref()),
        })
        .unwrap();
    assert!(
        state
            .expect_msg(Duration::from_millis(500))
            .unwrap()
            .local_shards
            .is_empty()
    );
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

    time.advance(Duration::from_secs(1));
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
fn region_actor_suppresses_shared_store_remembered_shard_restart_after_graceful_shutdown_starts() {
    let (kit, time) = kairo_testkit::ActorSystemTestKit::with_manual_time(
        "region-shared-store-remember-shard-shutdown-race",
    )
    .unwrap();
    let remember_store = kit
        .system()
        .spawn(
            "remember-store",
            RememberShardStoreActor::props(RememberShardStoreState::with_entities(
                "orders",
                "shard-1",
                ["entity-1".to_string()],
            )),
        )
        .unwrap();
    let region = kit
        .system()
        .spawn(
            "region",
            Props::new(move || {
                ShardRegionActor::<String>::new_with_remember_store_shards(
                    "region-a",
                    10,
                    10,
                    BTreeMap::from([("shard-1".to_string(), remember_store.clone())]),
                    Duration::from_millis(500),
                )
                .with_remember_shard_failure_backoff(Duration::from_secs(1))
            }),
        )
        .unwrap();
    let host = kit.create_probe::<HostShardPlan<String>>("host").unwrap();
    let local_shard = kit
        .create_probe::<Option<kairo_actor::ActorRef<ShardMsg<String>>>>("local-shard")
        .unwrap();
    let state = kit.create_probe::<ShardRegionSnapshot>("state").unwrap();

    region
        .tell(ShardRegionMsg::HostShard {
            shard: "shard-1".to_string(),
            reply_to: host.actor_ref(),
        })
        .unwrap();
    host.expect_msg(Duration::from_millis(500)).unwrap();
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

    kit.system().stop(&shard);
    assert!(shard.wait_for_stop(Duration::from_secs(1)));
    region
        .tell(ShardRegionMsg::MarkShardStopped {
            shard: "shard-1".to_string(),
            reply_to: Some(state.actor_ref()),
        })
        .unwrap();
    assert!(
        state
            .expect_msg(Duration::from_millis(500))
            .unwrap()
            .local_shards
            .is_empty()
    );
    region
        .tell(ShardRegionMsg::SetGracefulShutdown { in_progress: true })
        .unwrap();

    time.advance(Duration::from_secs(1));
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
fn region_actor_restarts_remembered_local_shard_after_unexpected_stop() {
    let (kit, time) =
        kairo_testkit::ActorSystemTestKit::with_manual_time("region-remember-shard-restart")
            .unwrap();
    let region = kit
        .system()
        .spawn(
            "region",
            Props::new(|| {
                ShardRegionActor::<String>::new_with_local_remember_store_shards(
                    "region-a",
                    "orders",
                    10,
                    10,
                    BTreeMap::from([(
                        "shard-1".to_string(),
                        BTreeSet::from(["entity-1".to_string()]),
                    )]),
                    Duration::from_millis(500),
                )
                .with_remember_shard_failure_backoff(Duration::from_secs(1))
            }),
        )
        .unwrap();
    let host = kit.create_probe::<HostShardPlan<String>>("host").unwrap();
    let local_shard = kit
        .create_probe::<Option<kairo_actor::ActorRef<ShardMsg<String>>>>("local-shard")
        .unwrap();
    let state = kit.create_probe::<ShardRegionSnapshot>("state").unwrap();
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
        .tell(ShardRegionMsg::GetLocalShard {
            shard: "shard-1".to_string(),
            reply_to: local_shard.actor_ref(),
        })
        .unwrap();
    let first_shard = local_shard
        .expect_msg(Duration::from_millis(500))
        .unwrap()
        .unwrap();

    kit.system().stop(&first_shard);
    assert!(first_shard.wait_for_stop(Duration::from_secs(1)));
    region
        .tell(ShardRegionMsg::MarkShardStopped {
            shard: "shard-1".to_string(),
            reply_to: Some(state.actor_ref()),
        })
        .unwrap();
    assert!(
        state
            .expect_msg(Duration::from_millis(500))
            .unwrap()
            .local_shards
            .is_empty()
    );

    time.advance(Duration::from_secs(1));
    region
        .tell(ShardRegionMsg::GetLocalShard {
            shard: "shard-1".to_string(),
            reply_to: local_shard.actor_ref(),
        })
        .unwrap();
    let restarted_shard = local_shard
        .expect_msg(Duration::from_millis(500))
        .unwrap()
        .unwrap();
    assert_ne!(first_shard.path(), restarted_shard.path());

    restarted_shard
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
fn region_actor_observes_remembered_local_shard_stop_and_restarts_without_mark_message() {
    let (kit, time) =
        kairo_testkit::ActorSystemTestKit::with_manual_time("region-remember-shard-watch-restart")
            .unwrap();
    let region = kit
        .system()
        .spawn(
            "region",
            Props::new(|| {
                ShardRegionActor::<String>::new_with_local_remember_store_shards(
                    "region-a",
                    "orders",
                    10,
                    10,
                    BTreeMap::from([(
                        "shard-1".to_string(),
                        BTreeSet::from(["entity-1".to_string()]),
                    )]),
                    Duration::from_millis(500),
                )
                .with_remember_shard_failure_backoff(Duration::from_secs(1))
            }),
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
    host.expect_msg(Duration::from_millis(500)).unwrap();
    region
        .tell(ShardRegionMsg::GetLocalShard {
            shard: "shard-1".to_string(),
            reply_to: local_shard.actor_ref(),
        })
        .unwrap();
    let first_shard = local_shard
        .expect_msg(Duration::from_millis(500))
        .unwrap()
        .unwrap();

    kit.system().stop(&first_shard);
    assert!(first_shard.wait_for_stop(Duration::from_secs(1)));

    let restarted_shard = kairo_testkit::await_assert(
        Duration::from_secs(1),
        Duration::from_millis(10),
        || -> Result<kairo_actor::ActorRef<ShardMsg<String>>, String> {
            time.advance_to_next();
            region
                .tell(ShardRegionMsg::GetLocalShard {
                    shard: "shard-1".to_string(),
                    reply_to: local_shard.actor_ref(),
                })
                .map_err(|error| error.to_string())?;
            let Some(shard) = local_shard
                .expect_msg(Duration::from_millis(100))
                .map_err(|error| error.to_string())?
            else {
                return Err("remembered shard has not restarted yet".to_string());
            };
            if shard.path() == first_shard.path() {
                Err("remembered shard still points at stopped ref".to_string())
            } else {
                Ok(shard)
            }
        },
    )
    .unwrap();

    restarted_shard
        .tell(ShardMsg::Deliver {
            message: ShardingEnvelope::new("entity-1", "after-watch-restart".to_string()),
            reply_to: deliveries.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        deliveries.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardDeliverPlan::Deliver {
            delivery: crate::EntityDelivery::new("entity-1", "after-watch-restart".to_string()),
        }
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn region_actor_ignores_stale_remembered_local_shard_restart_timer() {
    let (kit, time) =
        kairo_testkit::ActorSystemTestKit::with_manual_time("region-remember-shard-stale-restart")
            .unwrap();
    let region = kit
        .system()
        .spawn(
            "region",
            Props::new(|| {
                ShardRegionActor::<String>::new_with_local_remember_store_shards(
                    "region-a",
                    "orders",
                    10,
                    10,
                    BTreeMap::from([(
                        "shard-1".to_string(),
                        BTreeSet::from(["entity-1".to_string()]),
                    )]),
                    Duration::from_millis(500),
                )
                .with_remember_shard_failure_backoff(Duration::from_secs(1))
            }),
        )
        .unwrap();
    let host = kit.create_probe::<HostShardPlan<String>>("host").unwrap();
    let local_shard = kit
        .create_probe::<Option<kairo_actor::ActorRef<ShardMsg<String>>>>("local-shard")
        .unwrap();
    let state = kit.create_probe::<ShardRegionSnapshot>("state").unwrap();
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
        .tell(ShardRegionMsg::GetLocalShard {
            shard: "shard-1".to_string(),
            reply_to: local_shard.actor_ref(),
        })
        .unwrap();
    let first_shard = local_shard
        .expect_msg(Duration::from_millis(500))
        .unwrap()
        .unwrap();

    kit.system().stop(&first_shard);
    assert!(first_shard.wait_for_stop(Duration::from_secs(1)));
    region
        .tell(ShardRegionMsg::MarkShardStopped {
            shard: "shard-1".to_string(),
            reply_to: Some(state.actor_ref()),
        })
        .unwrap();
    assert!(
        state
            .expect_msg(Duration::from_millis(500))
            .unwrap()
            .local_shards
            .is_empty()
    );

    region
        .tell(ShardRegionMsg::HostShard {
            shard: "shard-1".to_string(),
            reply_to: host.actor_ref(),
        })
        .unwrap();
    host.expect_msg(Duration::from_millis(500)).unwrap();
    region
        .tell(ShardRegionMsg::GetLocalShard {
            shard: "shard-1".to_string(),
            reply_to: local_shard.actor_ref(),
        })
        .unwrap();
    let restarted_before_timer = local_shard
        .expect_msg(Duration::from_millis(500))
        .unwrap()
        .unwrap();
    assert_ne!(first_shard.path(), restarted_before_timer.path());

    restarted_before_timer
        .tell(ShardMsg::Deliver {
            message: ShardingEnvelope::new("entity-1", "before-stale-timer".to_string()),
            reply_to: deliveries.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        deliveries.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardDeliverPlan::Deliver {
            delivery: crate::EntityDelivery::new("entity-1", "before-stale-timer".to_string()),
        }
    );

    time.advance(Duration::from_secs(1));
    region
        .tell(ShardRegionMsg::GetLocalShard {
            shard: "shard-1".to_string(),
            reply_to: local_shard.actor_ref(),
        })
        .unwrap();
    let shard_after_stale_timer = local_shard
        .expect_msg(Duration::from_millis(500))
        .unwrap()
        .unwrap();
    assert_eq!(
        restarted_before_timer.path(),
        shard_after_stale_timer.path()
    );

    shard_after_stale_timer
        .tell(ShardMsg::Deliver {
            message: ShardingEnvelope::new("entity-1", "after-stale-timer".to_string()),
            reply_to: deliveries.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        deliveries.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardDeliverPlan::Deliver {
            delivery: crate::EntityDelivery::new("entity-1", "after-stale-timer".to_string()),
        }
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn region_actor_ignores_prior_remembered_local_shard_restart_timer_after_new_failure() {
    let (kit, time) = kairo_testkit::ActorSystemTestKit::with_manual_time(
        "region-remember-shard-stale-restart-generation",
    )
    .unwrap();
    let region = kit
        .system()
        .spawn(
            "region",
            Props::new(|| {
                ShardRegionActor::<String>::new_with_local_remember_store_shards(
                    "region-a",
                    "orders",
                    10,
                    10,
                    BTreeMap::from([(
                        "shard-1".to_string(),
                        BTreeSet::from(["entity-1".to_string()]),
                    )]),
                    Duration::from_millis(500),
                )
                .with_remember_shard_failure_backoff(Duration::from_secs(1))
            }),
        )
        .unwrap();
    let host = kit.create_probe::<HostShardPlan<String>>("host").unwrap();
    let local_shard = kit
        .create_probe::<Option<kairo_actor::ActorRef<ShardMsg<String>>>>("local-shard")
        .unwrap();
    let state = kit.create_probe::<ShardRegionSnapshot>("state").unwrap();

    region
        .tell(ShardRegionMsg::HostShard {
            shard: "shard-1".to_string(),
            reply_to: host.actor_ref(),
        })
        .unwrap();
    host.expect_msg(Duration::from_millis(500)).unwrap();
    region
        .tell(ShardRegionMsg::GetLocalShard {
            shard: "shard-1".to_string(),
            reply_to: local_shard.actor_ref(),
        })
        .unwrap();
    let first_shard = local_shard
        .expect_msg(Duration::from_millis(500))
        .unwrap()
        .unwrap();

    kit.system().stop(&first_shard);
    assert!(first_shard.wait_for_stop(Duration::from_secs(1)));
    region
        .tell(ShardRegionMsg::MarkShardStopped {
            shard: "shard-1".to_string(),
            reply_to: Some(state.actor_ref()),
        })
        .unwrap();
    assert!(
        state
            .expect_msg(Duration::from_millis(500))
            .unwrap()
            .local_shards
            .is_empty()
    );

    time.advance(Duration::from_millis(500));
    region
        .tell(ShardRegionMsg::HostShard {
            shard: "shard-1".to_string(),
            reply_to: host.actor_ref(),
        })
        .unwrap();
    host.expect_msg(Duration::from_millis(500)).unwrap();
    region
        .tell(ShardRegionMsg::GetLocalShard {
            shard: "shard-1".to_string(),
            reply_to: local_shard.actor_ref(),
        })
        .unwrap();
    let second_shard = local_shard
        .expect_msg(Duration::from_millis(500))
        .unwrap()
        .unwrap();
    assert_ne!(first_shard.path(), second_shard.path());

    kit.system().stop(&second_shard);
    assert!(second_shard.wait_for_stop(Duration::from_secs(1)));
    region
        .tell(ShardRegionMsg::MarkShardStopped {
            shard: "shard-1".to_string(),
            reply_to: Some(state.actor_ref()),
        })
        .unwrap();
    assert!(
        state
            .expect_msg(Duration::from_millis(500))
            .unwrap()
            .local_shards
            .is_empty()
    );

    time.advance(Duration::from_millis(500));
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

    time.advance(Duration::from_millis(500));
    region
        .tell(ShardRegionMsg::GetLocalShard {
            shard: "shard-1".to_string(),
            reply_to: local_shard.actor_ref(),
        })
        .unwrap();
    let restarted_after_second_backoff = local_shard
        .expect_msg(Duration::from_millis(500))
        .unwrap()
        .unwrap();
    assert_ne!(second_shard.path(), restarted_after_second_backoff.path());
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn region_actor_does_not_restart_remembered_local_shard_after_graceful_shutdown_starts() {
    let (kit, time) =
        kairo_testkit::ActorSystemTestKit::with_manual_time("region-remember-shard-shutdown-race")
            .unwrap();
    let region = kit
        .system()
        .spawn(
            "region",
            Props::new(|| {
                ShardRegionActor::<String>::new_with_local_remember_store_shards(
                    "region-a",
                    "orders",
                    10,
                    10,
                    BTreeMap::from([(
                        "shard-1".to_string(),
                        BTreeSet::from(["entity-1".to_string()]),
                    )]),
                    Duration::from_millis(500),
                )
                .with_remember_shard_failure_backoff(Duration::from_secs(1))
            }),
        )
        .unwrap();
    let host = kit.create_probe::<HostShardPlan<String>>("host").unwrap();
    let local_shard = kit
        .create_probe::<Option<kairo_actor::ActorRef<ShardMsg<String>>>>("local-shard")
        .unwrap();
    let state = kit.create_probe::<ShardRegionSnapshot>("state").unwrap();

    region
        .tell(ShardRegionMsg::HostShard {
            shard: "shard-1".to_string(),
            reply_to: host.actor_ref(),
        })
        .unwrap();
    host.expect_msg(Duration::from_millis(500)).unwrap();
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

    kit.system().stop(&shard);
    assert!(shard.wait_for_stop(Duration::from_secs(1)));
    region
        .tell(ShardRegionMsg::MarkShardStopped {
            shard: "shard-1".to_string(),
            reply_to: Some(state.actor_ref()),
        })
        .unwrap();
    assert!(
        state
            .expect_msg(Duration::from_millis(500))
            .unwrap()
            .local_shards
            .is_empty()
    );
    region
        .tell(ShardRegionMsg::SetGracefulShutdown { in_progress: true })
        .unwrap();

    time.advance(Duration::from_secs(1));
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
fn region_actor_does_not_restart_remembered_local_shard_after_handoff_begins() {
    let (kit, time) =
        kairo_testkit::ActorSystemTestKit::with_manual_time("region-remember-shard-handoff-race")
            .unwrap();
    let region = kit
        .system()
        .spawn(
            "region",
            Props::new(|| {
                ShardRegionActor::<String>::new_with_local_remember_store_shards(
                    "region-a",
                    "orders",
                    10,
                    10,
                    BTreeMap::from([(
                        "shard-1".to_string(),
                        BTreeSet::from(["entity-1".to_string()]),
                    )]),
                    Duration::from_millis(500),
                )
                .with_remember_shard_failure_backoff(Duration::from_secs(1))
            }),
        )
        .unwrap();
    let host = kit.create_probe::<HostShardPlan<String>>("host").unwrap();
    let begin = kit.create_probe::<BeginHandOffPlan>("begin").unwrap();
    let local_shard = kit
        .create_probe::<Option<kairo_actor::ActorRef<ShardMsg<String>>>>("local-shard")
        .unwrap();
    let state = kit.create_probe::<ShardRegionSnapshot>("state").unwrap();

    region
        .tell(ShardRegionMsg::HostShard {
            shard: "shard-1".to_string(),
            reply_to: host.actor_ref(),
        })
        .unwrap();
    host.expect_msg(Duration::from_millis(500)).unwrap();
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

    kit.system().stop(&shard);
    assert!(shard.wait_for_stop(Duration::from_secs(1)));
    region
        .tell(ShardRegionMsg::MarkShardStopped {
            shard: "shard-1".to_string(),
            reply_to: Some(state.actor_ref()),
        })
        .unwrap();
    assert!(
        state
            .expect_msg(Duration::from_millis(500))
            .unwrap()
            .local_shards
            .is_empty()
    );
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

    time.advance(Duration::from_secs(1));
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
fn region_actor_does_not_restart_remembered_local_shard_after_handoff_stop() {
    let (kit, time) =
        kairo_testkit::ActorSystemTestKit::with_manual_time("region-remember-shard-handoff-stop")
            .unwrap();
    let region = kit
        .system()
        .spawn(
            "region",
            Props::new(|| {
                ShardRegionActor::<String>::new_with_local_remember_store_shards(
                    "region-a",
                    "orders",
                    10,
                    10,
                    BTreeMap::from([(
                        "shard-1".to_string(),
                        BTreeSet::from(["entity-1".to_string()]),
                    )]),
                    Duration::from_millis(500),
                )
                .with_remember_shard_failure_backoff(Duration::from_secs(1))
            }),
        )
        .unwrap();
    let host = kit.create_probe::<HostShardPlan<String>>("host").unwrap();
    let handoff = kit.create_probe::<HandOffPlan>("handoff").unwrap();
    let local_shard = kit
        .create_probe::<Option<kairo_actor::ActorRef<ShardMsg<String>>>>("local-shard")
        .unwrap();
    let state = kit.create_probe::<ShardRegionSnapshot>("state").unwrap();

    region
        .tell(ShardRegionMsg::HostShard {
            shard: "shard-1".to_string(),
            reply_to: host.actor_ref(),
        })
        .unwrap();
    host.expect_msg(Duration::from_millis(500)).unwrap();
    region
        .tell(ShardRegionMsg::HandOff {
            shard: "shard-1".to_string(),
            reply_to: handoff.actor_ref(),
        })
        .unwrap();
    assert!(matches!(
        handoff.expect_msg(Duration::from_millis(500)).unwrap(),
        HandOffPlan::ForwardToLocalShard { .. }
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

    kit.system().stop(&shard);
    assert!(shard.wait_for_stop(Duration::from_secs(1)));
    region
        .tell(ShardRegionMsg::MarkShardStopped {
            shard: "shard-1".to_string(),
            reply_to: Some(state.actor_ref()),
        })
        .unwrap();
    assert!(
        state
            .expect_msg(Duration::from_millis(500))
            .unwrap()
            .local_shards
            .is_empty()
    );

    time.advance(Duration::from_secs(1));
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
