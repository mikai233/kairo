use std::fmt::{self, Display, Formatter};
use std::marker::PhantomData;

use kairo_actor::ActorRef;
use kairo_serialization::{RemoteEnvelope, RemoteMessage};

use crate::{
    RegisterAck, RoutedShardEnvelope, ShardCoordinatorRemoteHomeError,
    ShardCoordinatorRemoteHomeInbound, ShardCoordinatorRemoteRegistrationError,
    ShardCoordinatorRemoteRegistrationInbound, ShardHome, ShardRegionMsg, ShardRegionRemoteError,
    ShardRegionRemoteInbound,
};

#[derive(Debug)]
pub enum ShardRegionSystemInboundError {
    MissingHandler(&'static str),
    RegionRemote(ShardRegionRemoteError),
    Registration(ShardCoordinatorRemoteRegistrationError),
    ShardHome(ShardCoordinatorRemoteHomeError),
    Send { target: String, reason: String },
    UnsupportedManifest(String),
}

impl Display for ShardRegionSystemInboundError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingHandler(handler) => {
                write!(f, "shard-region system inbound has no {handler} handler")
            }
            Self::RegionRemote(error) => write!(f, "{error}"),
            Self::Registration(error) => write!(f, "{error}"),
            Self::ShardHome(error) => write!(f, "{error}"),
            Self::Send { target, reason } => {
                write!(
                    f,
                    "shard-region system inbound delivery to `{target}` failed: {reason}"
                )
            }
            Self::UnsupportedManifest(manifest) => {
                write!(f, "unsupported shard-region system manifest `{manifest}`")
            }
        }
    }
}

impl std::error::Error for ShardRegionSystemInboundError {}

impl From<ShardRegionRemoteError> for ShardRegionSystemInboundError {
    fn from(error: ShardRegionRemoteError) -> Self {
        Self::RegionRemote(error)
    }
}

impl From<ShardCoordinatorRemoteRegistrationError> for ShardRegionSystemInboundError {
    fn from(error: ShardCoordinatorRemoteRegistrationError) -> Self {
        Self::Registration(error)
    }
}

impl From<ShardCoordinatorRemoteHomeError> for ShardRegionSystemInboundError {
    fn from(error: ShardCoordinatorRemoteHomeError) -> Self {
        Self::ShardHome(error)
    }
}

#[derive(Clone)]
pub struct ShardRegionSystemInbound<M>
where
    M: Send + 'static,
{
    region: ActorRef<ShardRegionMsg<M>>,
    routes: Option<ShardRegionRemoteInbound<M>>,
    registration: Option<ShardCoordinatorRemoteRegistrationInbound>,
    shard_home: Option<ShardCoordinatorRemoteHomeInbound>,
    _message: PhantomData<fn(M)>,
}

impl<M> ShardRegionSystemInbound<M>
where
    M: Send + 'static,
{
    pub fn new(region: ActorRef<ShardRegionMsg<M>>) -> Self {
        Self {
            region,
            routes: None,
            registration: None,
            shard_home: None,
            _message: PhantomData,
        }
    }

    pub fn with_routes(mut self, inbound: ShardRegionRemoteInbound<M>) -> Self {
        self.routes = Some(inbound);
        self
    }

    pub fn with_registration(mut self, inbound: ShardCoordinatorRemoteRegistrationInbound) -> Self {
        self.registration = Some(inbound);
        self
    }

    pub fn with_shard_home(mut self, inbound: ShardCoordinatorRemoteHomeInbound) -> Self {
        self.shard_home = Some(inbound);
        self
    }

    pub fn receive(&self, envelope: RemoteEnvelope) -> Result<(), ShardRegionSystemInboundError>
    where
        M: RemoteMessage,
    {
        match envelope.message.manifest.as_str() {
            RoutedShardEnvelope::MANIFEST => self
                .routes
                .as_ref()
                .ok_or(ShardRegionSystemInboundError::MissingHandler(
                    "region route",
                ))?
                .receive(envelope)
                .map_err(Into::into),
            RegisterAck::MANIFEST => {
                let ack = self
                    .registration
                    .as_ref()
                    .ok_or(ShardRegionSystemInboundError::MissingHandler(
                        "coordinator registration",
                    ))?
                    .receive(envelope)?;
                self.region
                    .tell(ShardRegionMsg::RemoteCoordinatorRegistrationAck { ack })
                    .map_err(|error| ShardRegionSystemInboundError::Send {
                        target: self.region.path().to_string(),
                        reason: error.reason().to_string(),
                    })
            }
            ShardHome::MANIFEST => {
                let home = self
                    .shard_home
                    .as_ref()
                    .ok_or(ShardRegionSystemInboundError::MissingHandler(
                        "coordinator shard-home",
                    ))?
                    .receive(envelope)?;
                self.region
                    .tell(ShardRegionMsg::RemoteCoordinatorShardHome { home })
                    .map_err(|error| ShardRegionSystemInboundError::Send {
                        target: self.region.path().to_string(),
                        reason: error.reason().to_string(),
                    })
            }
            manifest => Err(ShardRegionSystemInboundError::UnsupportedManifest(
                manifest.to_string(),
            )),
        }
    }
}

pub fn is_shard_region_system_manifest(manifest: &str) -> bool {
    matches!(
        manifest,
        RoutedShardEnvelope::MANIFEST | RegisterAck::MANIFEST | ShardHome::MANIFEST
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use kairo_actor::Address;
    use kairo_cluster::UniqueAddress;
    use kairo_serialization::{
        ActorRefWireData, MessageCodec, Registry, SerializationRegistry, SerializedMessage,
    };
    use kairo_testkit::ActorSystemTestKit;

    use crate::{
        DEFAULT_SHARD_REGION_REMOTE_PATH, RegionLocalRoutePlan, RegisterAck, RoutedShardEnvelope,
        ShardCoordinatorRemoteHomeInbound, ShardCoordinatorRemoteRegistrationInbound,
        ShardDeliverPlan, ShardHome, ShardRegionRemoteInbound, ShardingEnvelope,
        register_sharding_protocol_codecs,
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
                        }
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
                                "kairo://remote@127.0.0.1:2552/system/sharding/coordinator"
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
}
