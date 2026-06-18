use std::sync::{Arc, Mutex};
use std::time::Duration;

use kairo_remote::{RemoteAssociationAddress, RemoteAssociationCache, RemoteOutbound};
use kairo_serialization::{
    ActorRefWireData, Manifest, Registry, RemoteEnvelope, SerializedMessage,
};
use kairo_testkit::{ActorSystemTestKit, await_assert};

use crate::{
    CoordinatorState, HandoffTransport, HostShard, LeastShardAllocationStrategy, RegisterAck,
    ShardCoordinatorActor, ShardHome, register_sharding_protocol_codecs,
};

use super::*;

#[test]
fn coordinator_system_inbound_routes_register_and_get_shard_home() {
    let kit = ActorSystemTestKit::new("sharding-coordinator-system-inbound").unwrap();
    let registry = registry();
    let outbound = kit
        .create_probe::<RemoteEnvelope>("remote-outbound")
        .unwrap();
    let coordinator = kit
        .system()
        .spawn(
            "coordinator",
            ShardCoordinatorActor::props_with_handoff(
                CoordinatorState::new(),
                LeastShardAllocationStrategy::default(),
                (),
                Duration::from_millis(500),
                HandoffTransport::new(),
            ),
        )
        .unwrap();
    let inbound = ShardCoordinatorSystemInbound::<()>::new(
        coordinator,
        coordinator_wire(),
        registry.clone(),
        outbound.actor_ref(),
    );

    inbound
        .receive(RemoteEnvelope::new(
            coordinator_wire(),
            Some(region_wire()),
            registry
                .serialize(&Register {
                    region: region_wire(),
                })
                .unwrap(),
        ))
        .unwrap();

    let ack = outbound.expect_msg(Duration::from_millis(500)).unwrap();
    assert_eq!(ack.recipient, region_wire());
    assert_eq!(ack.sender, Some(coordinator_wire()));
    assert_eq!(
        registry.deserialize::<RegisterAck>(ack.message).unwrap(),
        RegisterAck {
            coordinator: coordinator_wire(),
        }
    );

    inbound
        .receive(RemoteEnvelope::new(
            coordinator_wire(),
            Some(region_wire()),
            registry
                .serialize(&GetShardHome {
                    shard_id: "12".to_string(),
                })
                .unwrap(),
        ))
        .unwrap();

    let host = outbound.expect_msg(Duration::from_millis(500)).unwrap();
    assert_eq!(host.recipient, region_wire());
    assert_eq!(host.sender, Some(coordinator_wire()));
    assert_eq!(
        registry.deserialize::<HostShard>(host.message).unwrap(),
        HostShard {
            shard_id: "12".to_string(),
        }
    );

    let home = outbound.expect_msg(Duration::from_millis(500)).unwrap();
    assert_eq!(home.recipient, region_wire());
    assert_eq!(home.sender, Some(coordinator_wire()));
    assert_eq!(
        registry.deserialize::<ShardHome>(home.message).unwrap(),
        ShardHome {
            shard_id: "12".to_string(),
            region: region_wire(),
        }
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn coordinator_system_inbound_replies_through_remote_association_cache() {
    let kit = ActorSystemTestKit::new("sharding-coordinator-system-association-cache").unwrap();
    let registry = registry();
    let collecting = Arc::new(CollectingRemoteOutbound::default());
    let cache = RemoteAssociationCache::new();
    cache.insert_route(
        RemoteAssociationAddress::new("kairo", "remote", "127.0.0.1", Some(2552)).unwrap(),
        collecting.clone() as Arc<dyn RemoteOutbound>,
    );
    let coordinator = kit
        .system()
        .spawn(
            "coordinator",
            ShardCoordinatorActor::props_with_handoff(
                CoordinatorState::new(),
                LeastShardAllocationStrategy::default(),
                (),
                Duration::from_millis(500),
                HandoffTransport::new(),
            ),
        )
        .unwrap();
    let inbound = ShardCoordinatorSystemInbound::<()>::new(
        coordinator,
        coordinator_wire(),
        registry.clone(),
        kairo_remote::RemoteOutboundRecipient::from_arc(Arc::new(cache) as Arc<dyn RemoteOutbound>),
    );

    inbound
        .receive(RemoteEnvelope::new(
            coordinator_wire(),
            Some(region_wire()),
            registry
                .serialize(&Register {
                    region: region_wire(),
                })
                .unwrap(),
        ))
        .unwrap();
    inbound
        .receive(RemoteEnvelope::new(
            coordinator_wire(),
            Some(region_wire()),
            registry
                .serialize(&GetShardHome {
                    shard_id: "12".to_string(),
                })
                .unwrap(),
        ))
        .unwrap();

    let envelopes = await_assert(
        Duration::from_millis(200),
        Duration::from_millis(10),
        || -> Result<Vec<RemoteEnvelope>, String> {
            let envelopes = collecting.envelopes();
            if envelopes.len() >= 3 {
                Ok(envelopes)
            } else {
                Err(format!(
                    "expected 3 remote envelopes, found {}",
                    envelopes.len()
                ))
            }
        },
    )
    .unwrap();
    assert_eq!(envelopes.len(), 3);
    assert!(
        envelopes
            .iter()
            .all(|envelope| envelope.recipient == region_wire())
    );
    assert!(
        envelopes
            .iter()
            .all(|envelope| envelope.sender == Some(coordinator_wire()))
    );
    assert_eq!(
        registry
            .deserialize::<RegisterAck>(envelopes[0].message.clone())
            .unwrap(),
        RegisterAck {
            coordinator: coordinator_wire(),
        }
    );
    assert_eq!(
        registry
            .deserialize::<HostShard>(envelopes[1].message.clone())
            .unwrap(),
        HostShard {
            shard_id: "12".to_string(),
        }
    );
    assert_eq!(
        registry
            .deserialize::<ShardHome>(envelopes[2].message.clone())
            .unwrap(),
        ShardHome {
            shard_id: "12".to_string(),
            region: region_wire(),
        }
    );
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn coordinator_system_inbound_routes_region_control_replies() {
    let kit = ActorSystemTestKit::new("sharding-coordinator-system-control-replies").unwrap();
    let registry = registry();
    let outbound = kit
        .create_probe::<RemoteEnvelope>("remote-outbound")
        .unwrap();
    let coordinator = kit
        .create_probe::<ShardCoordinatorMsg<()>>("coordinator")
        .unwrap();
    let inbound = ShardCoordinatorSystemInbound::<()>::new(
        coordinator.actor_ref(),
        coordinator_wire(),
        registry.clone(),
        outbound.actor_ref(),
    );

    inbound
        .receive(RemoteEnvelope::new(
            coordinator_wire(),
            Some(region_wire()),
            registry
                .serialize(&ShardStarted {
                    shard_id: "12".to_string(),
                })
                .unwrap(),
        ))
        .unwrap();
    match coordinator.expect_msg(Duration::from_millis(500)).unwrap() {
        ShardCoordinatorMsg::RemoteHostShardObserved { region, started } => {
            assert_eq!(region, region_wire());
            assert_eq!(started.shard_id, "12");
        }
        _ => panic!("expected remote host-shard observation"),
    }

    inbound
        .receive(RemoteEnvelope::new(
            coordinator_wire(),
            Some(region_wire()),
            registry
                .serialize(&BeginHandOffAck {
                    shard_id: "12".to_string(),
                })
                .unwrap(),
        ))
        .unwrap();
    match coordinator.expect_msg(Duration::from_millis(500)).unwrap() {
        ShardCoordinatorMsg::RemoteBeginHandOffAck { region, ack } => {
            assert_eq!(region, region_wire());
            assert_eq!(ack.shard_id, "12");
        }
        _ => panic!("expected remote begin-handoff ack"),
    }

    inbound
        .receive(RemoteEnvelope::new(
            coordinator_wire(),
            Some(region_wire()),
            registry
                .serialize(&ShardStopped {
                    shard_id: "12".to_string(),
                })
                .unwrap(),
        ))
        .unwrap();
    match coordinator.expect_msg(Duration::from_millis(500)).unwrap() {
        ShardCoordinatorMsg::RemoteShardStopped { region, stopped } => {
            assert_eq!(region, region_wire());
            assert_eq!(stopped.shard_id, "12");
        }
        _ => panic!("expected remote shard-stopped"),
    }
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn coordinator_system_inbound_routes_region_shutdown_messages() {
    let kit = ActorSystemTestKit::new("sharding-coordinator-system-shutdown").unwrap();
    let registry = registry();
    let outbound = kit
        .create_probe::<RemoteEnvelope>("remote-outbound")
        .unwrap();
    let coordinator = kit
        .create_probe::<ShardCoordinatorMsg<()>>("coordinator")
        .unwrap();
    let inbound = ShardCoordinatorSystemInbound::<()>::new(
        coordinator.actor_ref(),
        coordinator_wire(),
        registry.clone(),
        outbound.actor_ref(),
    );

    inbound
        .receive(RemoteEnvelope::new(
            coordinator_wire(),
            Some(region_wire()),
            registry
                .serialize(&GracefulShutdownReq {
                    region: region_wire(),
                })
                .unwrap(),
        ))
        .unwrap();
    match coordinator.expect_msg(Duration::from_millis(500)).unwrap() {
        ShardCoordinatorMsg::RemoteGracefulShutdownReq { region } => {
            assert_eq!(region, region_wire());
        }
        _ => panic!("expected remote graceful-shutdown request"),
    }

    inbound
        .receive(RemoteEnvelope::new(
            coordinator_wire(),
            Some(region_wire()),
            registry
                .serialize(&RegionStopped {
                    region: region_wire(),
                })
                .unwrap(),
        ))
        .unwrap();
    match coordinator.expect_msg(Duration::from_millis(500)).unwrap() {
        ShardCoordinatorMsg::RemoteRegionStopped { region } => {
            assert_eq!(region, region_wire());
        }
        _ => panic!("expected remote region-stopped"),
    }
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

#[test]
fn coordinator_system_inbound_rejects_wrong_recipient_sender_or_manifest() {
    let kit = ActorSystemTestKit::new("sharding-coordinator-system-inbound-errors").unwrap();
    let registry = registry();
    let outbound = kit
        .create_probe::<RemoteEnvelope>("remote-outbound")
        .unwrap();
    let coordinator = kit
        .system()
        .spawn(
            "coordinator",
            ShardCoordinatorActor::props(
                CoordinatorState::new(),
                LeastShardAllocationStrategy::default(),
            ),
        )
        .unwrap();
    let inbound = ShardCoordinatorSystemInbound::<()>::new(
        coordinator,
        coordinator_wire(),
        registry.clone(),
        outbound.actor_ref(),
    );

    let wrong_recipient = RemoteEnvelope::new(
        ActorRefWireData::new("kairo://remote@127.0.0.1:2552/user/not-coordinator").unwrap(),
        Some(region_wire()),
        registry
            .serialize(&Register {
                region: region_wire(),
            })
            .unwrap(),
    );
    assert!(matches!(
        inbound.receive(wrong_recipient).unwrap_err(),
        ShardCoordinatorSystemInboundError::WrongRecipient { .. }
    ));

    let missing_sender = RemoteEnvelope::new(
        coordinator_wire(),
        None,
        registry
            .serialize(&GetShardHome {
                shard_id: "12".to_string(),
            })
            .unwrap(),
    );
    assert!(matches!(
        inbound.receive(missing_sender).unwrap_err(),
        ShardCoordinatorSystemInboundError::MissingSender(_)
    ));

    for message in [
        registry
            .serialize(&ShardStarted {
                shard_id: "12".to_string(),
            })
            .unwrap(),
        registry
            .serialize(&BeginHandOffAck {
                shard_id: "12".to_string(),
            })
            .unwrap(),
        registry
            .serialize(&ShardStopped {
                shard_id: "12".to_string(),
            })
            .unwrap(),
    ] {
        let missing_sender = RemoteEnvelope::new(coordinator_wire(), None, message);
        assert!(matches!(
            inbound.receive(missing_sender).unwrap_err(),
            ShardCoordinatorSystemInboundError::MissingSender(_)
        ));
    }

    let wrong_manifest = RemoteEnvelope::new(
        coordinator_wire(),
        Some(region_wire()),
        SerializedMessage {
            serializer_id: 4_000,
            manifest: Manifest::new("kairo.sharding.unsupported-coordinator"),
            version: 1,
            payload: bytes::Bytes::new(),
        },
    );
    assert!(matches!(
        inbound.receive(wrong_manifest).unwrap_err(),
        ShardCoordinatorSystemInboundError::UnsupportedManifest(_)
    ));
    kit.shutdown(Duration::from_secs(1)).unwrap();
}

fn registry() -> Arc<Registry> {
    let mut registry = Registry::new();
    register_sharding_protocol_codecs(&mut registry).unwrap();
    Arc::new(registry)
}

fn coordinator_wire() -> ActorRefWireData {
    ActorRefWireData::new("kairo://remote@127.0.0.1:2552/system/sharding/coordinator").unwrap()
}

fn region_wire() -> ActorRefWireData {
    ActorRefWireData::new("kairo://remote@127.0.0.1:2552/system/sharding/region").unwrap()
}

#[derive(Default)]
struct CollectingRemoteOutbound {
    envelopes: Mutex<Vec<RemoteEnvelope>>,
}

impl CollectingRemoteOutbound {
    fn envelopes(&self) -> Vec<RemoteEnvelope> {
        self.envelopes
            .lock()
            .expect("collecting remote outbound lock poisoned")
            .clone()
    }
}

impl RemoteOutbound for CollectingRemoteOutbound {
    fn send(&self, envelope: RemoteEnvelope) -> kairo_remote::Result<()> {
        self.envelopes
            .lock()
            .expect("collecting remote outbound lock poisoned")
            .push(envelope);
        Ok(())
    }
}
