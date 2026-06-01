use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use kairo_actor::{ActorRef, Address};
use kairo_cluster::UniqueAddress;
use kairo_serialization::{
    ActorRefWireData, MessageCodec, Registry, RemoteEnvelope, RemoteMessage, SerializationRegistry,
    SerializedMessage,
};
use kairo_testkit::ActorSystemTestKit;

use crate::{
    BeginHandOff, BeginHandOffAck, DEFAULT_SHARD_REGION_REMOTE_PATH, HandOff, HostShard,
    RegionLocalRoutePlan, RegisterAck, RoutedShardEnvelope, ShardCoordinatorRemoteHomeInbound,
    ShardCoordinatorRemoteRegistrationInbound, ShardDeliverPlan, ShardHome, ShardMsg,
    ShardRegionActor, ShardRegionMsg, ShardRegionRemoteControlInbound, ShardRegionRemoteInbound,
    ShardStarted, ShardStopped, ShardingEnvelope, register_sharding_protocol_codecs,
};

use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
struct TestMessage {
    value: String,
}

impl RemoteMessage for TestMessage {
    const MANIFEST: &'static str = "kairo.sharding.test.region-system-message";
    const VERSION: u16 = 1;
}

#[derive(Debug, Clone, Copy)]
struct TestMessageCodec;

impl MessageCodec<TestMessage> for TestMessageCodec {
    fn serializer_id(&self) -> u32 {
        79_101
    }

    fn encode(&self, message: &TestMessage) -> kairo_serialization::Result<Bytes> {
        Ok(Bytes::from(message.value.clone()))
    }

    fn decode(&self, payload: Bytes, version: u16) -> kairo_serialization::Result<TestMessage> {
        assert_eq!(version, TestMessage::VERSION);
        Ok(TestMessage {
            value: String::from_utf8(payload.to_vec()).unwrap(),
        })
    }
}

#[test]
fn region_system_inbound_routes_routed_shard_envelopes() {
    let kit = ActorSystemTestKit::new("sharding-system-inbound-route").unwrap();
    let registry = registry();
    let self_node = node("self", 1);
    let region = kit
        .create_probe::<ShardRegionMsg<TestMessage>>("region")
        .unwrap();
    let route_reply = kit
        .create_probe::<RegionLocalRoutePlan<TestMessage>>("route-reply")
        .unwrap();
    let delivery_reply = kit
        .create_probe::<ShardDeliverPlan<TestMessage>>("delivery-reply")
        .unwrap();
    let inbound = ShardRegionSystemInbound::new(region.actor_ref()).with_routes(
        ShardRegionRemoteInbound::new(
            self_node.clone(),
            registry.clone(),
            region.actor_ref(),
            route_reply.actor_ref(),
            delivery_reply.actor_ref(),
        ),
    );
    let routed = RoutedShardEnvelope {
        shard_id: "shard-1".to_string(),
        entity_id: "entity-1".to_string(),
        message: registry
            .serialize(&TestMessage {
                value: "first".to_string(),
            })
            .unwrap(),
    };

    inbound
        .receive(RemoteEnvelope::new(
            recipient_for(&self_node, DEFAULT_SHARD_REGION_REMOTE_PATH),
            None,
            registry.serialize(&routed).unwrap(),
        ))
        .unwrap();

    match region.expect_msg(Duration::from_secs(1)).unwrap() {
        ShardRegionMsg::RouteToLocalShard {
            shard,
            message,
            route_reply_to: _,
            delivery_reply_to: _,
        } => {
            assert_eq!(shard, "shard-1");
            assert_eq!(
                message,
                ShardingEnvelope::new(
                    "entity-1",
                    TestMessage {
                        value: "first".to_string(),
                    },
                )
            );
        }
        _ => panic!("expected routed envelope to enter local region delivery"),
    }
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn region_system_inbound_routes_registration_and_shard_home_replies() {
    let kit = ActorSystemTestKit::new("sharding-system-inbound-coordinator-replies").unwrap();
    let registry = registry();
    let region = kit
        .create_probe::<ShardRegionMsg<TestMessage>>("region")
        .unwrap();
    let region_wire = region_wire();
    let coordinator = actor_ref("kairo://remote@127.0.0.1:2552/system/sharding/coordinator");
    let remote_region = actor_ref("kairo://remote@127.0.0.1:2552/system/sharding/region");
    let inbound = ShardRegionSystemInbound::new(region.actor_ref())
        .with_registration(ShardCoordinatorRemoteRegistrationInbound::new(
            region_wire.clone(),
            registry.clone(),
        ))
        .with_shard_home(ShardCoordinatorRemoteHomeInbound::new(
            region_wire.clone(),
            registry.clone(),
        ));

    inbound
        .receive(RemoteEnvelope::new(
            region_wire.clone(),
            Some(coordinator.clone()),
            registry.serialize(&RegisterAck { coordinator }).unwrap(),
        ))
        .unwrap();
    match region.expect_msg(Duration::from_secs(1)).unwrap() {
        ShardRegionMsg::RemoteCoordinatorRegistrationAck { ack } => {
            assert_eq!(
                ack.coordinator.path(),
                "kairo://remote@127.0.0.1:2552/system/sharding/coordinator"
            );
        }
        _ => panic!("expected decoded RegisterAck to route to region"),
    }

    inbound
        .receive(RemoteEnvelope::new(
            region_wire,
            None,
            registry
                .serialize(&ShardHome {
                    shard_id: "shard-1".to_string(),
                    region: remote_region.clone(),
                })
                .unwrap(),
        ))
        .unwrap();
    match region.expect_msg(Duration::from_secs(1)).unwrap() {
        ShardRegionMsg::RemoteCoordinatorShardHome { home } => {
            assert_eq!(home.shard_id, "shard-1");
            assert_eq!(home.region, remote_region);
        }
        _ => panic!("expected decoded ShardHome to route to region"),
    }
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn region_system_inbound_routes_remote_control_commands_and_replies() {
    let kit = ActorSystemTestKit::new("sharding-system-inbound-control").unwrap();
    let registry = registry();
    let region_wire = region_wire();
    let coordinator = actor_ref("kairo://remote@127.0.0.1:2552/system/sharding/coordinator");
    let outbound = kit
        .create_probe::<RemoteEnvelope>("remote-control-replies")
        .unwrap();
    let region = kit
        .system()
        .spawn(
            "region",
            kairo_actor::Props::new(|| {
                ShardRegionActor::<TestMessage>::new_with_local_shards("region-a", 10, 10)
            }),
        )
        .unwrap();
    let inbound =
        ShardRegionSystemInbound::new(region).with_control(ShardRegionRemoteControlInbound::new(
            region_wire.clone(),
            registry.clone(),
            outbound.actor_ref(),
        ));

    inbound
        .receive(RemoteEnvelope::new(
            region_wire.clone(),
            Some(coordinator.clone()),
            registry
                .serialize(&HostShard {
                    shard_id: "shard-1".to_string(),
                })
                .unwrap(),
        ))
        .unwrap();
    let started = outbound.expect_msg(Duration::from_secs(1)).unwrap();
    assert_eq!(started.recipient, coordinator);
    assert_eq!(started.sender, Some(region_wire.clone()));
    assert_eq!(
        registry
            .deserialize::<ShardStarted>(started.message)
            .unwrap(),
        ShardStarted {
            shard_id: "shard-1".to_string()
        }
    );

    inbound
        .receive(RemoteEnvelope::new(
            region_wire.clone(),
            Some(coordinator.clone()),
            registry
                .serialize(&BeginHandOff {
                    shard_id: "shard-1".to_string(),
                })
                .unwrap(),
        ))
        .unwrap();
    let begin_ack = outbound.expect_msg(Duration::from_secs(1)).unwrap();
    assert_eq!(begin_ack.recipient, coordinator);
    assert_eq!(begin_ack.sender, Some(region_wire.clone()));
    assert_eq!(
        registry
            .deserialize::<BeginHandOffAck>(begin_ack.message)
            .unwrap(),
        BeginHandOffAck {
            shard_id: "shard-1".to_string()
        }
    );

    inbound
        .receive(RemoteEnvelope::new(
            region_wire.clone(),
            Some(coordinator.clone()),
            registry
                .serialize(&HandOff {
                    shard_id: "missing-shard".to_string(),
                })
                .unwrap(),
        ))
        .unwrap();
    let stopped = outbound.expect_msg(Duration::from_secs(1)).unwrap();
    assert_eq!(stopped.recipient, coordinator);
    assert_eq!(stopped.sender, Some(region_wire));
    assert_eq!(
        registry
            .deserialize::<ShardStopped>(stopped.message)
            .unwrap(),
        ShardStopped {
            shard_id: "missing-shard".to_string()
        }
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn region_system_inbound_completes_hosted_remote_handoff_with_local_stop_message() {
    let kit = ActorSystemTestKit::new("sharding-system-inbound-hosted-handoff").unwrap();
    let registry = registry();
    let region_wire = region_wire();
    let coordinator = actor_ref("kairo://remote@127.0.0.1:2552/system/sharding/coordinator");
    let outbound = kit
        .create_probe::<RemoteEnvelope>("remote-control-replies")
        .unwrap();
    let region = kit
        .system()
        .spawn(
            "region",
            kairo_actor::Props::new(|| {
                ShardRegionActor::<TestMessage>::new_with_local_shards("region-a", 10, 10)
                    .with_remote_handoff_stop_message(
                        TestMessage {
                            value: "stop".to_string(),
                        },
                        Duration::from_millis(100),
                    )
            }),
        )
        .unwrap();
    let inbound = ShardRegionSystemInbound::new(region.clone()).with_control(
        ShardRegionRemoteControlInbound::new(
            region_wire.clone(),
            registry.clone(),
            outbound.actor_ref(),
        ),
    );

    inbound
        .receive(RemoteEnvelope::new(
            region_wire.clone(),
            Some(coordinator.clone()),
            registry
                .serialize(&HostShard {
                    shard_id: "shard-1".to_string(),
                })
                .unwrap(),
        ))
        .unwrap();
    assert_eq!(
        registry
            .deserialize::<ShardStarted>(
                outbound.expect_msg(Duration::from_secs(1)).unwrap().message,
            )
            .unwrap(),
        ShardStarted {
            shard_id: "shard-1".to_string()
        }
    );

    let route_reply = kit
        .create_probe::<RegionLocalRoutePlan<TestMessage>>("route-reply")
        .unwrap();
    let delivery_reply = kit
        .create_probe::<ShardDeliverPlan<TestMessage>>("delivery-reply")
        .unwrap();
    region
        .tell(ShardRegionMsg::RouteToLocalShard {
            shard: "shard-1".to_string(),
            message: ShardingEnvelope::new(
                "entity-1",
                TestMessage {
                    value: "work".to_string(),
                },
            ),
            route_reply_to: route_reply.actor_ref(),
            delivery_reply_to: delivery_reply.actor_ref(),
        })
        .unwrap();
    assert!(matches!(
        route_reply.expect_msg(Duration::from_secs(1)).unwrap(),
        RegionLocalRoutePlan::DeliveredToLocalShard { .. }
    ));
    assert!(matches!(
        delivery_reply.expect_msg(Duration::from_secs(1)).unwrap(),
        ShardDeliverPlan::StartEntity { .. }
    ));

    inbound
        .receive(RemoteEnvelope::new(
            region_wire.clone(),
            Some(coordinator.clone()),
            registry
                .serialize(&BeginHandOff {
                    shard_id: "shard-1".to_string(),
                })
                .unwrap(),
        ))
        .unwrap();
    assert_eq!(
        registry
            .deserialize::<BeginHandOffAck>(
                outbound.expect_msg(Duration::from_secs(1)).unwrap().message,
            )
            .unwrap(),
        BeginHandOffAck {
            shard_id: "shard-1".to_string()
        }
    );

    inbound
        .receive(RemoteEnvelope::new(
            region_wire.clone(),
            Some(coordinator.clone()),
            registry
                .serialize(&HandOff {
                    shard_id: "shard-1".to_string(),
                })
                .unwrap(),
        ))
        .unwrap();
    let stopped = outbound.expect_msg(Duration::from_secs(1)).unwrap();
    assert_eq!(stopped.recipient, coordinator);
    assert_eq!(stopped.sender, Some(region_wire));
    assert_eq!(
        registry
            .deserialize::<ShardStopped>(stopped.message)
            .unwrap(),
        ShardStopped {
            shard_id: "shard-1".to_string()
        }
    );

    let local_shard = kit
        .create_probe::<Option<ActorRef<ShardMsg<TestMessage>>>>("local-shard")
        .unwrap();
    region
        .tell(ShardRegionMsg::GetLocalShard {
            shard: "shard-1".to_string(),
            reply_to: local_shard.actor_ref(),
        })
        .unwrap();
    assert!(
        local_shard
            .expect_msg(Duration::from_secs(1))
            .unwrap()
            .is_none()
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn region_system_inbound_rejects_missing_handler_wrong_recipient_and_unknown_manifest() {
    let kit = ActorSystemTestKit::new("sharding-system-inbound-reject").unwrap();
    let registry = registry();
    let self_node = node("self", 1);
    let region = kit
        .create_probe::<ShardRegionMsg<TestMessage>>("region")
        .unwrap();
    let inbound = ShardRegionSystemInbound::<TestMessage>::new(region.actor_ref());

    let routed = RoutedShardEnvelope {
        shard_id: "shard-1".to_string(),
        entity_id: "entity-1".to_string(),
        message: registry
            .serialize(&TestMessage {
                value: "first".to_string(),
            })
            .unwrap(),
    };
    assert!(matches!(
        inbound
            .receive(RemoteEnvelope::new(
                recipient_for(&self_node, DEFAULT_SHARD_REGION_REMOTE_PATH),
                None,
                registry.serialize(&routed).unwrap(),
            ))
            .unwrap_err(),
        ShardRegionSystemInboundError::MissingHandler("region route")
    ));

    let wrong_recipient = ShardRegionSystemInbound::new(region.actor_ref()).with_registration(
        ShardCoordinatorRemoteRegistrationInbound::new(region_wire(), registry.clone()),
    );
    assert!(matches!(
        wrong_recipient
            .receive(RemoteEnvelope::new(
                actor_ref("kairo://other@127.0.0.1:2559/system/sharding/region"),
                None,
                registry
                    .serialize(&RegisterAck {
                        coordinator: actor_ref(
                            "kairo://remote@127.0.0.1:2552/system/sharding/coordinator",
                        ),
                    })
                    .unwrap(),
            ))
            .unwrap_err(),
        ShardRegionSystemInboundError::Registration(
            ShardCoordinatorRemoteRegistrationError::WrongRecipient { .. }
        )
    ));

    assert!(matches!(
        inbound
            .receive(RemoteEnvelope::new(
                region_wire(),
                None,
                SerializedMessage::new(
                    99_999,
                    kairo_serialization::Manifest::new("kairo.sharding.unknown"),
                    1,
                    Bytes::new(),
                ),
            ))
            .unwrap_err(),
        ShardRegionSystemInboundError::UnsupportedManifest(_)
    ));
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

fn registry() -> Arc<Registry> {
    let mut registry = Registry::new();
    register_sharding_protocol_codecs(&mut registry).unwrap();
    registry
        .register::<TestMessage, _>(TestMessageCodec)
        .unwrap();
    Arc::new(registry)
}

fn node(name: &str, uid: u64) -> UniqueAddress {
    UniqueAddress::new(
        Address::new(
            "kairo",
            "sharding",
            Some(format!("{name}.example.test")),
            Some(2552),
        ),
        uid,
    )
}

fn recipient_for(node: &UniqueAddress, path: &str) -> ActorRefWireData {
    ActorRefWireData::new(format!("{}{}", node.address, path)).unwrap()
}

fn region_wire() -> ActorRefWireData {
    actor_ref("kairo://local@127.0.0.1:2551/system/sharding/region")
}

fn actor_ref(path: &str) -> ActorRefWireData {
    ActorRefWireData::new(path).unwrap()
}
