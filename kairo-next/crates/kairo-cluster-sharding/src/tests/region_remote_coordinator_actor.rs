mod support;

use super::*;
use support::*;

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
    let fixture = RemoteCoordinatorFixture::new(&kit, "region-remote-register-send", 2672, 8, 20);
    let region = fixture.spawn_region(&kit, "region-a");

    fixture.publish_discovery(&region);
    let first = fixture.expect_remote_message::<Register>();
    assert_eq!(first.region, fixture.region_wire().clone());

    region
        .tell(ShardRegionMsg::RetryCoordinatorRegistration)
        .unwrap();
    let retry = fixture.expect_remote_message::<Register>();
    assert_eq!(retry.region, fixture.region_wire().clone());
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn region_actor_sends_remote_shard_home_after_registration_ack() {
    let kit = kairo_testkit::ActorSystemTestKit::new("region-remote-home-send").unwrap();
    let fixture = RemoteCoordinatorFixture::new(&kit, "region-remote-home-send", 2673, 9, 5_000);
    let routes = kit
        .create_probe::<RegionLocalRoutePlan<String>>("remote-home-routes")
        .unwrap();
    let deliveries = kit
        .create_probe::<ShardDeliverPlan<String>>("remote-home-deliveries")
        .unwrap();
    let region = fixture.spawn_region(&kit, "region-a");

    fixture.publish_discovery(&region);
    let register = fixture.expect_remote_message::<Register>();
    assert_eq!(register.region, fixture.region_wire().clone());

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
                sender: Some(fixture.target().recipient().clone()),
                coordinator: fixture.target().recipient().clone(),
            },
        })
        .unwrap();
    let request = fixture.expect_remote_message::<GetShardHome>();
    assert_eq!(request.shard_id, "shard-1");
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn region_actor_sends_remote_graceful_shutdown_and_region_stopped() {
    let kit = kairo_testkit::ActorSystemTestKit::new("region-remote-graceful-shutdown").unwrap();
    let fixture =
        RemoteCoordinatorFixture::new(&kit, "region-remote-graceful-shutdown", 2674, 10, 5_000);
    let region = fixture.spawn_region(&kit, "region-a");

    fixture.publish_discovery(&region);
    let register = fixture.expect_remote_message::<Register>();
    assert_eq!(register.region, fixture.region_wire().clone());

    region
        .tell(ShardRegionMsg::RemoteCoordinatorRegistrationAck {
            ack: ShardCoordinatorRemoteRegistrationAck {
                sender: Some(fixture.target().recipient().clone()),
                coordinator: fixture.target().recipient().clone(),
            },
        })
        .unwrap();
    region
        .tell(ShardRegionMsg::GracefulShutdown { reply_to: None })
        .unwrap();

    let graceful = fixture.expect_remote_message::<GracefulShutdownReq>();
    assert_eq!(graceful.region, fixture.region_wire().clone());

    let stopped = fixture.expect_remote_message::<RegionStopped>();
    assert_eq!(stopped.region, fixture.region_wire().clone());
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn region_actor_delays_remote_region_stopped_until_hosted_shard_handoff() {
    let kit = kairo_testkit::ActorSystemTestKit::new("region-remote-graceful-handoff").unwrap();
    let fixture =
        RemoteCoordinatorFixture::new(&kit, "region-remote-graceful-handoff", 2675, 11, 5_000);
    let region = fixture.spawn_region_with_remote_handoff_stop_message(
        &kit,
        "region-a",
        "stop",
        Duration::from_millis(100),
    );
    let host = kit
        .create_probe::<HostShardPlan<String>>("host-shard")
        .unwrap();

    fixture.publish_discovery(&region);
    let register = fixture.expect_remote_message::<Register>();
    assert_eq!(register.region, fixture.region_wire().clone());
    region
        .tell(ShardRegionMsg::RemoteCoordinatorRegistrationAck {
            ack: ShardCoordinatorRemoteRegistrationAck {
                sender: Some(fixture.target().recipient().clone()),
                coordinator: fixture.target().recipient().clone(),
            },
        })
        .unwrap();
    region
        .tell(ShardRegionMsg::HostShard {
            shard: "shard-1".to_string(),
            reply_to: host.actor_ref(),
        })
        .unwrap();
    assert!(matches!(
        host.expect_msg(Duration::from_millis(500)).unwrap(),
        HostShardPlan::AlreadyStarted { ref shard, .. } if shard == "shard-1"
    ));

    region
        .tell(ShardRegionMsg::GracefulShutdown { reply_to: None })
        .unwrap();
    let graceful = fixture.expect_remote_message::<GracefulShutdownReq>();
    assert_eq!(graceful.region, fixture.region_wire().clone());
    fixture.expect_no_remote_message(Duration::from_millis(50));

    let reply = fixture.remote_control_reply_target();
    region
        .tell(ShardRegionMsg::RemoteBeginHandOff {
            shard: "shard-1".to_string(),
            reply: reply.clone(),
        })
        .unwrap();
    let ack = fixture.expect_remote_message::<BeginHandOffAck>();
    assert_eq!(ack.shard_id, "shard-1");

    region
        .tell(ShardRegionMsg::RemoteHandOff {
            shard: "shard-1".to_string(),
            reply,
        })
        .unwrap();
    let stopped = fixture.expect_remote_message::<ShardStopped>();
    assert_eq!(stopped.shard_id, "shard-1");
    let region_stopped = fixture.expect_remote_message::<RegionStopped>();
    assert_eq!(region_stopped.region, fixture.region_wire().clone());
    kit.shutdown(Duration::from_secs(1)).unwrap();
}
