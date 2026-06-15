use super::*;

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
fn region_actor_direct_handoff_stops_routing_to_local_shard() {
    let kit =
        kairo_testkit::ActorSystemTestKit::new("region-actor-direct-handoff-routing").unwrap();
    let region_b = kit
        .system()
        .spawn(
            "region-b",
            ShardRegionActor::<String>::props_with_local_shards("region-b", 10, 10),
        )
        .unwrap();
    let host_b = kit.create_probe::<HostShardPlan<String>>("host-b").unwrap();
    region_b
        .tell(ShardRegionMsg::HostShard {
            shard: "shard-1".to_string(),
            reply_to: host_b.actor_ref(),
        })
        .unwrap();
    host_b.expect_msg(Duration::from_millis(500)).unwrap();

    let mut route_transport = RegionRouteTransport::new();
    route_transport.insert_target(RegionRouteTarget::new("region-b", region_b));
    let region = kit
        .system()
        .spawn(
            "region",
            Props::new(move || {
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
                .with_region_route_transport(route_transport)
            }),
        )
        .unwrap();
    let host = kit.create_probe::<HostShardPlan<String>>("host").unwrap();
    let handoff = kit
        .create_probe::<RegionLocalHandOffPlan>("region-handoff")
        .unwrap();
    let shard_handoff = kit
        .create_probe::<ShardHandOffPlan<String>>("shard-handoff")
        .unwrap();
    let routes = kit
        .create_probe::<RegionLocalRoutePlan<String>>("routes")
        .unwrap();
    let deliveries = kit
        .create_probe::<ShardDeliverPlan<String>>("deliveries")
        .unwrap();
    let homes = kit
        .create_probe::<Result<ShardHomePlan<String>, ShardingError>>("homes")
        .unwrap();
    let completion = kit
        .create_probe::<RegionLocalHandOffCompletionPlan>("completion")
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
        RegionLocalHandOffPlan::ForwardedToLocalShard {
            ref shard,
            dropped_buffered: 0,
            ..
        } if shard == "shard-1"
    ));
    shard_handoff
        .expect_msg(Duration::from_millis(500))
        .unwrap();

    region
        .tell(ShardRegionMsg::RouteToLocalShard {
            shard: "shard-1".to_string(),
            message: ShardingEnvelope::new("entity-1", "after-handoff".to_string()),
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
    deliveries.expect_no_msg(Duration::from_millis(50)).unwrap();

    region
        .tell(ShardRegionMsg::RecordShardHome {
            shard: "shard-1".to_string(),
            region: "region-a".to_string(),
            reply_to: homes.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        homes.expect_msg(Duration::from_millis(500)).unwrap(),
        Err(ShardingError::ShardHomeDuringHandOff {
            shard: "shard-1".to_string(),
            region: "region-a".to_string(),
        })
    );

    region
        .tell(ShardRegionMsg::RouteToLocalShard {
            shard: "shard-1".to_string(),
            message: ShardingEnvelope::new("entity-1", "after-stale-home".to_string()),
            route_reply_to: routes.actor_ref(),
            delivery_reply_to: deliveries.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        routes.expect_msg(Duration::from_millis(500)).unwrap(),
        RegionLocalRoutePlan::Buffered {
            shard: "shard-1".to_string(),
            request: None,
        }
    );
    deliveries.expect_no_msg(Duration::from_millis(50)).unwrap();

    region
        .tell(ShardRegionMsg::CompleteLocalShardHandOff {
            shard: "shard-1".to_string(),
            timeout: Duration::from_millis(500),
            reply_to: completion.actor_ref(),
        })
        .unwrap();
    assert!(matches!(
        completion.expect_msg(Duration::from_millis(500)).unwrap(),
        RegionLocalHandOffCompletionPlan::Completed { ref shard, .. } if shard == "shard-1"
    ));

    region
        .tell(ShardRegionMsg::CoordinatorShardHomeResult {
            requested_shard: "shard-1".to_string(),
            result: Ok(GetShardHomePlan::Reply {
                shard: "shard-1".to_string(),
                region: "region-b".to_string(),
            }),
        })
        .unwrap();
    assert_eq!(
        deliveries.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardDeliverPlan::StartEntity {
            delivery: EntityDelivery::new("entity-1", "after-handoff".to_string()),
        }
    );
    assert_eq!(
        deliveries.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardDeliverPlan::Deliver {
            delivery: EntityDelivery::new("entity-1", "after-stale-home".to_string()),
        }
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn region_actor_remote_shard_home_during_handoff_keeps_buffered_replies() {
    let kit = kairo_testkit::ActorSystemTestKit::new("region-remote-home-during-handoff").unwrap();
    let region_b = kit
        .system()
        .spawn(
            "region-b",
            ShardRegionActor::<String>::props_with_local_shards("region-b", 10, 10),
        )
        .unwrap();
    let host_b = kit.create_probe::<HostShardPlan<String>>("host-b").unwrap();
    region_b
        .tell(ShardRegionMsg::HostShard {
            shard: "shard-1".to_string(),
            reply_to: host_b.actor_ref(),
        })
        .unwrap();
    host_b.expect_msg(Duration::from_millis(500)).unwrap();

    let remote_region =
        ActorRefWireData::new("kairo://remote@127.0.0.1:2552/system/sharding/region").unwrap();
    let stale_region =
        ActorRefWireData::new("kairo://stale@127.0.0.1:2553/system/sharding/region").unwrap();
    let mut route_transport = RegionRouteTransport::new();
    route_transport.insert_target(RegionRouteTarget::new(
        remote_region.path().to_string(),
        region_b,
    ));
    let region = kit
        .system()
        .spawn(
            "region",
            Props::new(move || {
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
                .with_region_route_transport(route_transport)
            }),
        )
        .unwrap();
    let host = kit.create_probe::<HostShardPlan<String>>("host").unwrap();
    let handoff = kit
        .create_probe::<RegionLocalHandOffPlan>("region-handoff")
        .unwrap();
    let shard_handoff = kit
        .create_probe::<ShardHandOffPlan<String>>("shard-handoff")
        .unwrap();
    let routes = kit
        .create_probe::<RegionLocalRoutePlan<String>>("routes")
        .unwrap();
    let deliveries = kit
        .create_probe::<ShardDeliverPlan<String>>("deliveries")
        .unwrap();
    let completion = kit
        .create_probe::<RegionLocalHandOffCompletionPlan>("completion")
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
        RegionLocalHandOffPlan::ForwardedToLocalShard { ref shard, .. } if shard == "shard-1"
    ));
    shard_handoff
        .expect_msg(Duration::from_millis(500))
        .unwrap();

    region
        .tell(ShardRegionMsg::RouteToLocalShard {
            shard: "shard-1".to_string(),
            message: ShardingEnvelope::new("entity-1", "after-handoff".to_string()),
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
        .tell(ShardRegionMsg::RemoteCoordinatorShardHome {
            home: ShardCoordinatorRemoteHome {
                sender: None,
                shard_id: "shard-1".to_string(),
                region: stale_region,
            },
        })
        .unwrap();
    deliveries.expect_no_msg(Duration::from_millis(50)).unwrap();

    region
        .tell(ShardRegionMsg::RouteToLocalShard {
            shard: "shard-1".to_string(),
            message: ShardingEnvelope::new("entity-1", "after-stale-home".to_string()),
            route_reply_to: routes.actor_ref(),
            delivery_reply_to: deliveries.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        routes.expect_msg(Duration::from_millis(500)).unwrap(),
        RegionLocalRoutePlan::Buffered {
            shard: "shard-1".to_string(),
            request: None,
        }
    );

    region
        .tell(ShardRegionMsg::CompleteLocalShardHandOff {
            shard: "shard-1".to_string(),
            timeout: Duration::from_millis(500),
            reply_to: completion.actor_ref(),
        })
        .unwrap();
    assert!(matches!(
        completion.expect_msg(Duration::from_millis(500)).unwrap(),
        RegionLocalHandOffCompletionPlan::Completed { ref shard, .. } if shard == "shard-1"
    ));

    region
        .tell(ShardRegionMsg::RemoteCoordinatorShardHome {
            home: ShardCoordinatorRemoteHome {
                sender: None,
                shard_id: "shard-1".to_string(),
                region: remote_region,
            },
        })
        .unwrap();
    assert_eq!(
        deliveries.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardDeliverPlan::StartEntity {
            delivery: EntityDelivery::new("entity-1", "after-handoff".to_string()),
        }
    );
    assert_eq!(
        deliveries.expect_msg(Duration::from_millis(500)).unwrap(),
        ShardDeliverPlan::Deliver {
            delivery: EntityDelivery::new("entity-1", "after-stale-home".to_string()),
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
