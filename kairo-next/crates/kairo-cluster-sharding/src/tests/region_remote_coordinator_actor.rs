use super::*;

#[test]
fn region_actor_marks_remote_coordinator_registered_from_decoded_ack() {
    let kit = kairo_testkit::ActorSystemTestKit::new("region-remote-registration-ack").unwrap();
    let coordinator_node =
        remote_unique_node("region-remote-registration-ack", "127.0.0.1", 2671, 7);
    let remote_target = ShardCoordinatorRemoteTarget::for_node(
        coordinator_node.clone(),
        crate::DEFAULT_SHARD_COORDINATOR_REMOTE_PATH,
    )
    .unwrap();
    let discovery = RegionCoordinatorDiscoveryConfig::<String>::new(
        CoordinatorDiscoverySettings::default().with_required_role("backend"),
        Duration::from_millis(20),
    )
    .with_remote_coordinator(remote_target.clone());
    let region = kit
        .system()
        .spawn(
            "region-a",
            ShardRegionActor::<String>::props_with_local_shards_and_coordinator_discovery(
                "region-a", 10, 10, discovery,
            ),
        )
        .unwrap();
    let region_state = kit
        .create_probe::<ShardRegionSnapshot>("remote-registration-region-state")
        .unwrap();

    region
        .tell(ShardRegionMsg::CoordinatorDiscoverySnapshot {
            state: cluster_state(vec![cluster_member(
                coordinator_node,
                MemberStatus::Up,
                ["backend"],
                1,
            )]),
        })
        .unwrap();
    region
        .tell(ShardRegionMsg::GetState {
            reply_to: region_state.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        region_state
            .expect_msg(Duration::from_millis(500))
            .unwrap()
            .registration_status,
        RegionRegistrationStatus::Registering
    );

    region
        .tell(ShardRegionMsg::RemoteCoordinatorRegistrationAck {
            ack: ShardCoordinatorRemoteRegistrationAck {
                sender: Some(remote_target.recipient().clone()),
                coordinator: remote_target.recipient().clone(),
            },
        })
        .unwrap();
    region
        .tell(ShardRegionMsg::GetState {
            reply_to: region_state.actor_ref(),
        })
        .unwrap();
    assert_eq!(
        region_state
            .expect_msg(Duration::from_millis(500))
            .unwrap()
            .registration_status,
        RegionRegistrationStatus::Registered
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn region_actor_sends_remote_register_on_discovery_and_retry() {
    let kit = kairo_testkit::ActorSystemTestKit::new("region-remote-register-send").unwrap();
    let coordinator_node = remote_unique_node("region-remote-register-send", "127.0.0.1", 2672, 8);
    let remote_target = ShardCoordinatorRemoteTarget::for_node(
        coordinator_node.clone(),
        crate::DEFAULT_SHARD_COORDINATOR_REMOTE_PATH,
    )
    .unwrap();
    let mut registry = Registry::new();
    register_sharding_protocol_codecs(&mut registry).unwrap();
    let registry = std::sync::Arc::new(registry);
    let remote_envelopes = kit
        .create_probe::<RemoteEnvelope>("remote-register-envelopes")
        .unwrap();
    let region_wire =
        ActorRefWireData::new("kairo://local@127.0.0.1:2551/system/sharding/region").unwrap();
    let transport = RegionRemoteCoordinatorTransport::new(
        region_wire.clone(),
        registry,
        remote_envelopes.actor_ref(),
    );
    let discovery = RegionCoordinatorDiscoveryConfig::<String>::new(
        CoordinatorDiscoverySettings::default().with_required_role("backend"),
        Duration::from_millis(20),
    )
    .with_remote_coordinator(remote_target.clone());
    let region = kit
        .system()
        .spawn(
            "region-a",
            Props::new(move || {
                ShardRegionActor::<String>::new_with_local_shards("region-a", 10, 10)
                    .with_coordinator_discovery(discovery.clone())
                    .with_remote_coordinator_transport(transport.clone())
            }),
        )
        .unwrap();

    region
        .tell(ShardRegionMsg::CoordinatorDiscoverySnapshot {
            state: cluster_state(vec![cluster_member(
                coordinator_node,
                MemberStatus::Up,
                ["backend"],
                1,
            )]),
        })
        .unwrap();
    let first = remote_envelopes
        .expect_msg(Duration::from_millis(500))
        .unwrap();
    assert_eq!(first.recipient, remote_target.recipient().clone());
    assert_eq!(first.sender, Some(region_wire.clone()));
    assert_eq!(first.message.manifest.as_str(), Register::MANIFEST);

    region
        .tell(ShardRegionMsg::RetryCoordinatorRegistration)
        .unwrap();
    let retry = remote_envelopes
        .expect_msg(Duration::from_millis(500))
        .unwrap();
    assert_eq!(retry.recipient, remote_target.recipient().clone());
    assert_eq!(retry.sender, Some(region_wire));
    assert_eq!(retry.message.manifest.as_str(), Register::MANIFEST);
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn region_actor_sends_remote_shard_home_after_registration_ack() {
    let kit = kairo_testkit::ActorSystemTestKit::new("region-remote-home-send").unwrap();
    let coordinator_node = remote_unique_node("region-remote-home-send", "127.0.0.1", 2673, 9);
    let remote_target = ShardCoordinatorRemoteTarget::for_node(
        coordinator_node.clone(),
        crate::DEFAULT_SHARD_COORDINATOR_REMOTE_PATH,
    )
    .unwrap();
    let mut registry = Registry::new();
    register_sharding_protocol_codecs(&mut registry).unwrap();
    let registry = std::sync::Arc::new(registry);
    let remote_envelopes = kit
        .create_probe::<RemoteEnvelope>("remote-home-envelopes")
        .unwrap();
    let routes = kit
        .create_probe::<RegionLocalRoutePlan<String>>("remote-home-routes")
        .unwrap();
    let deliveries = kit
        .create_probe::<ShardDeliverPlan<String>>("remote-home-deliveries")
        .unwrap();
    let region_wire =
        ActorRefWireData::new("kairo://local@127.0.0.1:2551/system/sharding/region").unwrap();
    let transport = RegionRemoteCoordinatorTransport::new(
        region_wire.clone(),
        registry,
        remote_envelopes.actor_ref(),
    );
    let discovery = RegionCoordinatorDiscoveryConfig::<String>::new(
        CoordinatorDiscoverySettings::default().with_required_role("backend"),
        Duration::from_secs(5),
    )
    .with_remote_coordinator(remote_target.clone());
    let region = kit
        .system()
        .spawn(
            "region-a",
            Props::new(move || {
                ShardRegionActor::<String>::new_with_local_shards("region-a", 10, 10)
                    .with_coordinator_discovery(discovery.clone())
                    .with_remote_coordinator_transport(transport.clone())
            }),
        )
        .unwrap();

    region
        .tell(ShardRegionMsg::CoordinatorDiscoverySnapshot {
            state: cluster_state(vec![cluster_member(
                coordinator_node,
                MemberStatus::Up,
                ["backend"],
                1,
            )]),
        })
        .unwrap();
    let register = remote_envelopes
        .expect_msg(Duration::from_millis(500))
        .unwrap();
    assert_eq!(register.message.manifest.as_str(), Register::MANIFEST);

    region
        .tell(ShardRegionMsg::RouteToLocalShard {
            shard: "shard-1".to_string(),
            message: ShardingEnvelope::new("entity-1", "first".to_string()),
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
        .tell(ShardRegionMsg::RemoteCoordinatorRegistrationAck {
            ack: ShardCoordinatorRemoteRegistrationAck {
                sender: Some(remote_target.recipient().clone()),
                coordinator: remote_target.recipient().clone(),
            },
        })
        .unwrap();
    let request = remote_envelopes
        .expect_msg(Duration::from_millis(500))
        .unwrap();
    assert_eq!(request.recipient, remote_target.recipient().clone());
    assert_eq!(request.sender, Some(region_wire));
    assert_eq!(request.message.manifest.as_str(), GetShardHome::MANIFEST);
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn region_actor_sends_remote_graceful_shutdown_and_region_stopped() {
    let kit = kairo_testkit::ActorSystemTestKit::new("region-remote-graceful-shutdown").unwrap();
    let coordinator_node =
        remote_unique_node("region-remote-graceful-shutdown", "127.0.0.1", 2674, 10);
    let remote_target = ShardCoordinatorRemoteTarget::for_node(
        coordinator_node.clone(),
        crate::DEFAULT_SHARD_COORDINATOR_REMOTE_PATH,
    )
    .unwrap();
    let mut registry = Registry::new();
    register_sharding_protocol_codecs(&mut registry).unwrap();
    let registry = std::sync::Arc::new(registry);
    let remote_envelopes = kit
        .create_probe::<RemoteEnvelope>("remote-shutdown-envelopes")
        .unwrap();
    let region_wire =
        ActorRefWireData::new("kairo://local@127.0.0.1:2551/system/sharding/region").unwrap();
    let transport = RegionRemoteCoordinatorTransport::new(
        region_wire.clone(),
        registry,
        remote_envelopes.actor_ref(),
    );
    let discovery = RegionCoordinatorDiscoveryConfig::<String>::new(
        CoordinatorDiscoverySettings::default().with_required_role("backend"),
        Duration::from_secs(5),
    )
    .with_remote_coordinator(remote_target.clone());
    let region = kit
        .system()
        .spawn(
            "region-a",
            Props::new(move || {
                ShardRegionActor::<String>::new_with_local_shards("region-a", 10, 10)
                    .with_coordinator_discovery(discovery.clone())
                    .with_remote_coordinator_transport(transport.clone())
            }),
        )
        .unwrap();

    region
        .tell(ShardRegionMsg::CoordinatorDiscoverySnapshot {
            state: cluster_state(vec![cluster_member(
                coordinator_node,
                MemberStatus::Up,
                ["backend"],
                1,
            )]),
        })
        .unwrap();
    let register = remote_envelopes
        .expect_msg(Duration::from_millis(500))
        .unwrap();
    assert_eq!(register.message.manifest.as_str(), Register::MANIFEST);

    region
        .tell(ShardRegionMsg::RemoteCoordinatorRegistrationAck {
            ack: ShardCoordinatorRemoteRegistrationAck {
                sender: Some(remote_target.recipient().clone()),
                coordinator: remote_target.recipient().clone(),
            },
        })
        .unwrap();
    region
        .tell(ShardRegionMsg::GracefulShutdown { reply_to: None })
        .unwrap();

    let graceful = remote_envelopes
        .expect_msg(Duration::from_millis(500))
        .unwrap();
    assert_eq!(graceful.recipient, remote_target.recipient().clone());
    assert_eq!(graceful.sender, Some(region_wire.clone()));
    assert_eq!(
        graceful.message.manifest.as_str(),
        GracefulShutdownReq::MANIFEST
    );

    let stopped = remote_envelopes
        .expect_msg(Duration::from_millis(500))
        .unwrap();
    assert_eq!(stopped.recipient, remote_target.recipient().clone());
    assert_eq!(stopped.sender, Some(region_wire));
    assert_eq!(stopped.message.manifest.as_str(), RegionStopped::MANIFEST);
    kit.shutdown(Duration::from_secs(1)).unwrap();
}
