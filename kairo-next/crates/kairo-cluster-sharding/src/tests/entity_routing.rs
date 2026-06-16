use super::*;

#[test]
fn sharding_envelope_keeps_entity_id_outside_business_message() {
    let envelope = ShardingEnvelope::new("counter-1", "increment");

    assert_eq!(envelope.entity_id(), "counter-1");
    assert_eq!(envelope.message(), &"increment");
    assert_eq!(
        envelope.into_parts(),
        ("counter-1".to_string(), "increment")
    );
}

#[test]
fn entity_ref_wraps_business_message_in_sharding_envelope() {
    let system = ActorSystem::builder("sharding").build().unwrap();
    let (tx, rx) = mpsc::channel();
    let region = system
        .spawn("region", Props::new(move || RegionProbe { observed: tx }))
        .unwrap();
    let entity_ref = EntityRef::new("counter-1", region);

    entity_ref.tell("increment").unwrap();

    assert_eq!(
        rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        ("counter-1".to_string(), "increment")
    );
}

#[test]
fn entity_ref_routes_through_sharding_envelope_router_to_local_shard() {
    let kit = kairo_testkit::ActorSystemTestKit::new("sharding-entity-ref-router").unwrap();
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
                coordinator,
                Duration::from_millis(20),
            ),
        )
        .unwrap();
    let router = kit
        .system()
        .spawn(
            "entity-router",
            ShardingEnvelopeRouter::props(region.clone(), DEFAULT_SHARD_COUNT),
        )
        .unwrap();
    let entity_ref = EntityRef::new("entity-1", router);
    let shard = default_shard_id_for("entity-1");

    entity_ref.tell("first".to_string()).unwrap();

    let shard_ref = wait_for_local_shard(&kit, &region, &shard);
    let snapshot = kit.create_probe::<ShardSnapshot>("shard-snapshot").unwrap();
    let mut active = false;
    for _ in 0..20 {
        shard_ref
            .tell(ShardMsg::GetState {
                reply_to: snapshot.actor_ref(),
            })
            .unwrap();
        let state = snapshot.expect_msg(Duration::from_millis(500)).unwrap();
        active = state.active_entities == vec!["entity-1".to_string()];
        if active {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    assert!(
        active,
        "router should deliver the entity envelope to a local shard"
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn entity_ref_routes_through_registered_region_to_entity_actor() {
    let kit = kairo_testkit::ActorSystemTestKit::new("sharding-entity-ref-entity-child").unwrap();
    let (observed_tx, observed_rx) = mpsc::channel();
    let entity_factory = EntityActorFactory::new(move |entity_id| RecordingEntity {
        entity_id,
        observed: observed_tx.clone(),
    });
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
            ShardRegionActor::<String>::props_with_local_entity_shards_and_registration(
                "region-a",
                10,
                10,
                entity_factory,
                coordinator,
                Duration::from_millis(20),
            ),
        )
        .unwrap();
    let router = kit
        .system()
        .spawn(
            "entity-router",
            ShardingEnvelopeRouter::props(region, DEFAULT_SHARD_COUNT),
        )
        .unwrap();
    let entity_ref = EntityRef::new("entity-1", router);

    entity_ref.tell("first".to_string()).unwrap();

    assert_eq!(
        observed_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        ("entity-1".to_string(), "first".to_string())
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn entity_message_extractor_router_routes_extracted_id_to_entity_actor() {
    let kit = kairo_testkit::ActorSystemTestKit::new("sharding-extractor-entity-child").unwrap();
    let (observed_tx, observed_rx) = mpsc::channel();
    let entity_factory = EntityActorFactory::new(move |entity_id| RecordingEntity {
        entity_id,
        observed: observed_tx.clone(),
    });
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
            ShardRegionActor::<String>::props_with_local_entity_shards_and_registration(
                "region-a",
                10,
                10,
                entity_factory,
                coordinator,
                Duration::from_millis(20),
            ),
        )
        .unwrap();
    let router = kit
        .system()
        .spawn(
            "extractor-router",
            EntityMessageExtractorRouter::props(region, DEFAULT_SHARD_COUNT, RoutedInputExtractor),
        )
        .unwrap();

    router
        .tell(RoutedInput::new("entity-1", None, "first"))
        .unwrap();

    assert_eq!(
        observed_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        ("entity-1".to_string(), "first".to_string())
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn entity_message_extractor_router_honors_extracted_shard_id() {
    let kit = kairo_testkit::ActorSystemTestKit::new("sharding-extractor-explicit-shard").unwrap();
    let (observed_tx, observed_rx) = mpsc::channel();
    let entity_factory = EntityActorFactory::new(move |entity_id| RecordingEntity {
        entity_id,
        observed: observed_tx.clone(),
    });
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
            ShardRegionActor::<String>::props_with_local_entity_shards_and_registration(
                "region-a",
                10,
                10,
                entity_factory,
                coordinator,
                Duration::from_millis(20),
            ),
        )
        .unwrap();
    let router = kit
        .system()
        .spawn(
            "extractor-router",
            EntityMessageExtractorRouter::props(
                region.clone(),
                DEFAULT_SHARD_COUNT,
                RoutedInputExtractor,
            ),
        )
        .unwrap();

    router
        .tell(RoutedInput::new(
            "entity-1",
            Some("explicit-shard"),
            "first",
        ))
        .unwrap();

    assert_eq!(
        observed_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        ("entity-1".to_string(), "first".to_string())
    );
    let shard_ref = wait_for_local_shard(&kit, &region, "explicit-shard");
    let snapshot = kit.create_probe::<ShardSnapshot>("shard-snapshot").unwrap();
    shard_ref
        .tell(ShardMsg::GetState {
            reply_to: snapshot.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        snapshot
            .expect_msg(Duration::from_millis(500))
            .unwrap()
            .active_entities,
        vec!["entity-1".to_string()]
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn entity_message_extractor_router_ignores_unmatched_input() {
    let kit = kairo_testkit::ActorSystemTestKit::new("sharding-extractor-unmatched").unwrap();
    let region = kit
        .create_probe::<ShardRegionMsg<String>>("region")
        .unwrap();
    let router = kit
        .system()
        .spawn(
            "extractor-router",
            EntityMessageExtractorRouter::props(
                region.actor_ref(),
                DEFAULT_SHARD_COUNT,
                RoutedInputExtractor,
            ),
        )
        .unwrap();

    router.tell(RoutedInput::new("", None, "dropped")).unwrap();

    region.expect_no_msg(Duration::from_millis(100)).unwrap();
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn remote_region_route_envelope_reenters_local_region_delivery() {
    let kit = kairo_testkit::ActorSystemTestKit::new("sharding-remote-region-route").unwrap();
    let mut registry = Registry::new();
    register_sharding_protocol_codecs(&mut registry).unwrap();
    registry
        .register::<RemoteRouteMessage, _>(RemoteRouteMessageCodec)
        .unwrap();
    let registry = std::sync::Arc::new(registry);
    let self_node = remote_node("sharding", "127.0.0.1", 25520);
    let remote_envelopes = kit
        .create_probe::<RemoteEnvelope>("remote-envelopes")
        .unwrap();
    let route_replies = kit
        .create_probe::<RegionLocalRoutePlan<RemoteRouteMessage>>("route-replies")
        .unwrap();
    let delivery_replies = kit
        .create_probe::<ShardDeliverPlan<RemoteRouteMessage>>("delivery-replies")
        .unwrap();
    let (observed_tx, observed_rx) = mpsc::channel();
    let entity_factory = EntityActorFactory::new(move |entity_id| RecordingRemoteEntity {
        entity_id,
        observed: observed_tx.clone(),
    });
    let region = kit
        .system()
        .spawn(
            "region",
            ShardRegionActor::<RemoteRouteMessage>::props_with_local_entity_shards(
                "region-a",
                10,
                10,
                entity_factory,
            ),
        )
        .unwrap();
    let host = kit
        .create_probe::<HostShardPlan<RemoteRouteMessage>>("host")
        .unwrap();

    region
        .tell(ShardRegionMsg::HostShard {
            shard: "shard-1".to_string(),
            reply_to: host.actor_ref(),
        })
        .unwrap();
    host.expect_msg(Duration::from_millis(500)).unwrap();

    let outbound = ShardRegionRemoteOutbound::<RemoteRouteMessage>::new(
        self_node.clone(),
        registry.clone(),
        remote_envelopes.actor_ref(),
    );
    outbound
        .tell(ShardRegionMsg::RouteToLocalShard {
            shard: "shard-1".to_string(),
            message: ShardingEnvelope::new("entity-1", RemoteRouteMessage("first".to_string())),
            route_reply_to: route_replies.actor_ref(),
            delivery_reply_to: delivery_replies.actor_ref(),
        })
        .unwrap();
    let envelope = remote_envelopes
        .expect_msg(Duration::from_millis(500))
        .unwrap();

    let inbound = ShardRegionRemoteInbound::new(
        self_node,
        registry,
        region,
        route_replies.actor_ref(),
        delivery_replies.actor_ref(),
    );
    inbound.receive(envelope).unwrap();

    assert_eq!(
        route_replies
            .expect_msg(Duration::from_millis(500))
            .unwrap(),
        RegionLocalRoutePlan::DeliveredToLocalShard {
            shard: "shard-1".to_string(),
        }
    );
    assert_eq!(
        delivery_replies
            .expect_msg(Duration::from_millis(500))
            .unwrap(),
        ShardDeliverPlan::StartEntity {
            delivery: EntityDelivery::new("entity-1", RemoteRouteMessage("first".to_string())),
        }
    );
    assert_eq!(
        observed_rx
            .recv_timeout(Duration::from_millis(500))
            .unwrap(),
        ("entity-1".to_string(), "first".to_string())
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RoutedInput {
    entity_id: String,
    shard_id: Option<String>,
    payload: String,
}

impl RoutedInput {
    fn new(entity_id: &str, shard_id: Option<&str>, payload: &str) -> Self {
        Self {
            entity_id: entity_id.to_string(),
            shard_id: shard_id.map(str::to_string),
            payload: payload.to_string(),
        }
    }
}

struct RoutedInputExtractor;

impl EntityMessageExtractor<RoutedInput, String> for RoutedInputExtractor {
    fn extract(&mut self, message: RoutedInput) -> Option<ExtractedEntityMessage<String>> {
        if message.entity_id.is_empty() {
            return None;
        }
        match message.shard_id {
            Some(shard_id) => Some(ExtractedEntityMessage::with_shard_id(
                message.entity_id,
                shard_id,
                message.payload,
            )),
            None => Some(ExtractedEntityMessage::new(
                message.entity_id,
                message.payload,
            )),
        }
    }
}

#[test]
fn region_route_transport_can_target_remote_region_envelopes() {
    let kit =
        kairo_testkit::ActorSystemTestKit::new("sharding-remote-region-route-target").unwrap();
    let mut registry = Registry::new();
    register_sharding_protocol_codecs(&mut registry).unwrap();
    registry
        .register::<RemoteRouteMessage, _>(RemoteRouteMessageCodec)
        .unwrap();
    let registry = std::sync::Arc::new(registry);
    let remote_envelopes = kit
        .create_probe::<RemoteEnvelope>("remote-envelopes")
        .unwrap();
    let route_replies = kit
        .create_probe::<RegionLocalRoutePlan<RemoteRouteMessage>>("route-replies")
        .unwrap();
    let delivery_replies = kit
        .create_probe::<ShardDeliverPlan<RemoteRouteMessage>>("delivery-replies")
        .unwrap();
    let remote_target = ShardRegionRemoteOutbound::<RemoteRouteMessage>::new(
        remote_node("sharding", "127.0.0.1", 25521),
        registry.clone(),
        remote_envelopes.actor_ref(),
    )
    .into_region_route_target("region-b");
    let mut transport = RegionRouteTransport::new();
    transport.insert_target(remote_target);

    assert_eq!(
        transport.send_route_to(
            &"region-b".to_string(),
            "shard-1".to_string(),
            ShardingEnvelope::new("entity-1", RemoteRouteMessage("first".to_string())),
            route_replies.actor_ref(),
            delivery_replies.actor_ref(),
        ),
        RegionRouteDelivery::Sent {
            shard: "shard-1".to_string(),
            region: "region-b".to_string(),
        }
    );
    let envelope = remote_envelopes
        .expect_msg(Duration::from_millis(500))
        .unwrap();
    assert_eq!(
        envelope.recipient.path(),
        "kairo://sharding@127.0.0.1:25521/system/sharding/region"
    );
    let routed = registry
        .deserialize::<crate::RoutedShardEnvelope>(envelope.message)
        .unwrap();
    assert_eq!(routed.shard_id, "shard-1");
    assert_eq!(routed.entity_id, "entity-1");
    assert_eq!(
        registry
            .deserialize::<RemoteRouteMessage>(routed.message)
            .unwrap(),
        RemoteRouteMessage("first".to_string())
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}
