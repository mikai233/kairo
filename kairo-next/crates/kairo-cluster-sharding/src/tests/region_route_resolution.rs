use super::*;

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
fn region_actor_allocates_remembered_shard_and_persists_first_delivery() {
    let kit =
        kairo_testkit::ActorSystemTestKit::new("region-route-remembered-first-delivery").unwrap();
    let shard_store = kit
        .system()
        .spawn(
            "shard-store",
            RememberShardStoreActor::props(RememberShardStoreState::new("orders", "shard-1")),
        )
        .unwrap();
    let coordinator = kit
        .system()
        .spawn(
            "coordinator",
            ShardCoordinatorActor::props_with_handoff(
                CoordinatorState::new().with_remember_entities(true),
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
            ShardRegionActor::<String>::props_with_remember_store_shards_and_registration(
                "region-a",
                10,
                10,
                BTreeMap::from([("shard-1".to_string(), shard_store.clone())]),
                Duration::from_millis(500),
                RegionRegistrationConfig::new(coordinator.clone(), Duration::from_millis(20)),
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
    let local_shard = kit
        .create_probe::<Option<ActorRef<ShardMsg<String>>>>("local-shard")
        .unwrap();
    let shard_state = kit.create_probe::<ShardSnapshot>("shard-state").unwrap();
    let shard_store_state = kit
        .create_probe::<RememberShardStoreSnapshot>("shard-store-state")
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
        ShardDeliverPlan::RememberUpdate {
            update: RememberShardUpdate::new(
                ["entity-1".to_string()],
                std::iter::empty::<String>(),
            ),
        }
    );

    let mut activated = false;
    for _ in 0..20 {
        shard_store
            .tell(RememberShardStoreMsg::GetState {
                reply_to: shard_store_state.actor_ref(),
            })
            .unwrap();
        let shard_snapshot = shard_store_state
            .expect_msg(Duration::from_millis(500))
            .unwrap();
        let remembered_entities = shard_snapshot
            .entities_by_key
            .values()
            .flat_map(|entities| entities.iter().cloned())
            .collect::<BTreeSet<_>>();

        region
            .tell(ShardRegionMsg::GetLocalShard {
                shard: "shard-1".to_string(),
                reply_to: local_shard.actor_ref(),
            })
            .unwrap();
        let shard = local_shard.expect_msg(Duration::from_millis(500)).unwrap();
        if let Some(shard) = shard {
            shard
                .tell(ShardMsg::GetState {
                    reply_to: shard_state.actor_ref(),
                })
                .unwrap();
            let snapshot = shard_state.expect_msg(Duration::from_millis(500)).unwrap();
            activated = remembered_entities.contains("entity-1")
                && snapshot.active_entities == vec!["entity-1".to_string()]
                && snapshot.total_buffered == 0;
        }

        if activated {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        activated,
        "automatic shard home allocation should persist and activate first delivery"
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
