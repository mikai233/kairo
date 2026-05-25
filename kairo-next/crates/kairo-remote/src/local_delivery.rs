use std::marker::PhantomData;

use kairo_actor::ActorSystem;
use kairo_serialization::ActorRefWireData;

use crate::local_address::CanonicalLocalAddress;
use crate::{InboundMessage, RemoteError, RemoteInboundDelivery, RemoteSettings, Result};

#[derive(Clone)]
pub struct LocalActorInboundDelivery<M> {
    system: ActorSystem,
    canonical_address: Option<CanonicalLocalAddress>,
    _message: PhantomData<fn(M)>,
}

impl<M> LocalActorInboundDelivery<M> {
    pub fn new(system: ActorSystem) -> Self {
        Self {
            system,
            canonical_address: None,
            _message: PhantomData,
        }
    }

    pub fn with_remote_settings(system: ActorSystem, settings: RemoteSettings) -> Self {
        Self {
            canonical_address: Some(CanonicalLocalAddress::from_system_settings(
                &system, settings,
            )),
            system,
            _message: PhantomData,
        }
    }

    pub fn system(&self) -> &ActorSystem {
        &self.system
    }

    fn local_recipient_path(&self, recipient: &ActorRefWireData) -> String {
        self.canonical_address
            .as_ref()
            .and_then(|canonical| canonical.local_recipient_path(recipient))
            .unwrap_or_else(|| recipient.path().to_string())
    }
}

impl<M> RemoteInboundDelivery<M> for LocalActorInboundDelivery<M>
where
    M: Send + 'static,
{
    fn deliver(&self, inbound: InboundMessage<M>) -> Result<()> {
        let recipient_path = self.local_recipient_path(&inbound.recipient);
        let recipient = self.system.resolve_local_or_missing(&recipient_path);
        recipient.tell(inbound.message).map_err(|error| {
            RemoteError::Inbound(format!(
                "failed to deliver remote message to `{recipient_path}`: {}",
                error.reason()
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, mpsc};
    use std::time::Duration;

    use bytes::Bytes;
    use kairo_actor::{Actor, ActorRef, ActorResult, Context, Props};
    use kairo_serialization::{
        ActorRefWireData, MessageCodec, Registry, RemoteEnvelope, RemoteMessage,
        SerializationRegistry,
    };

    use super::*;
    use crate::RemoteInbound;

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct Ping {
        value: u8,
    }

    impl RemoteMessage for Ping {
        const MANIFEST: &'static str = "kairo.remote.test.LocalDeliveryPing";
        const VERSION: u16 = 1;
    }

    struct PingCodec;

    impl MessageCodec<Ping> for PingCodec {
        fn serializer_id(&self) -> u32 {
            801
        }

        fn encode(&self, message: &Ping) -> kairo_serialization::Result<Bytes> {
            Ok(Bytes::from(vec![message.value]))
        }

        fn decode(&self, payload: Bytes, _version: u16) -> kairo_serialization::Result<Ping> {
            Ok(Ping { value: payload[0] })
        }
    }

    struct Probe {
        received: mpsc::Sender<u8>,
    }

    impl Actor for Probe {
        type Msg = Ping;

        fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
            self.received
                .send(msg.value)
                .map_err(|error| kairo_actor::ActorError::Message(error.to_string()))
        }
    }

    struct UnitProbe;

    impl Actor for UnitProbe {
        type Msg = ();

        fn receive(&mut self, _ctx: &mut Context<Self::Msg>, _msg: Self::Msg) -> ActorResult {
            Ok(())
        }
    }

    fn registry() -> Arc<Registry> {
        let mut registry = Registry::new();
        registry.register::<Ping, _>(PingCodec).unwrap();
        Arc::new(registry)
    }

    fn envelope(registry: &Registry, recipient: &ActorRef<Ping>, value: u8) -> RemoteEnvelope {
        RemoteEnvelope::new(
            ActorRefWireData::new(recipient.path().to_string()).unwrap(),
            Some(ActorRefWireData::new("kairo://sender@127.0.0.1:25521/user/source#1").unwrap()),
            registry.serialize(&Ping { value }).unwrap(),
        )
    }

    fn canonical_envelope(
        registry: &Registry,
        recipient: &ActorRef<Ping>,
        value: u8,
    ) -> RemoteEnvelope {
        let recipient_path = recipient.path().as_str().replacen(
            "kairo://receiver",
            "kairo://receiver@127.0.0.1:25520",
            1,
        );
        RemoteEnvelope::new(
            ActorRefWireData::new(recipient_path).unwrap(),
            Some(ActorRefWireData::new("kairo://sender@127.0.0.1:25521/user/source#1").unwrap()),
            registry.serialize(&Ping { value }).unwrap(),
        )
    }

    #[test]
    fn local_delivery_resolves_recipient_and_tells_actor() {
        let system = ActorSystem::builder("receiver").build().unwrap();
        let registry = registry();
        let (received_tx, received_rx) = mpsc::channel();
        let target = system
            .spawn(
                "target",
                Props::new(move || Probe {
                    received: received_tx,
                }),
            )
            .unwrap();
        let inbound = RemoteInbound::<Ping>::new(
            registry.clone(),
            Arc::new(LocalActorInboundDelivery::new(system.clone())),
        );

        inbound.receive(envelope(&registry, &target, 42)).unwrap();

        assert_eq!(
            received_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            42
        );
        assert!(system.dead_letters().is_empty());
    }

    #[test]
    fn local_delivery_maps_canonical_remote_address_to_local_actor_path() {
        let system = ActorSystem::builder("receiver").build().unwrap();
        let registry = registry();
        let (received_tx, received_rx) = mpsc::channel();
        let target = system
            .spawn(
                "target",
                Props::new(move || Probe {
                    received: received_tx,
                }),
            )
            .unwrap();
        let inbound = RemoteInbound::<Ping>::new(
            registry.clone(),
            Arc::new(LocalActorInboundDelivery::with_remote_settings(
                system.clone(),
                crate::RemoteSettings::new("127.0.0.1", 25520),
            )),
        );

        inbound
            .receive(canonical_envelope(&registry, &target, 44))
            .unwrap();

        assert_eq!(
            received_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            44
        );
        assert!(system.dead_letters().is_empty());
    }

    #[test]
    fn local_delivery_does_not_map_other_remote_addresses_to_local_path() {
        let system = ActorSystem::builder("receiver").build().unwrap();
        let registry = registry();
        let inbound = RemoteInbound::<Ping>::new(
            registry.clone(),
            Arc::new(LocalActorInboundDelivery::with_remote_settings(
                system.clone(),
                crate::RemoteSettings::new("127.0.0.1", 25520),
            )),
        );
        let envelope = RemoteEnvelope::new(
            ActorRefWireData::new("kairo://receiver@127.0.0.2:25520/user/target#1").unwrap(),
            None,
            registry.serialize(&Ping { value: 45 }).unwrap(),
        );

        let error = inbound
            .receive(envelope)
            .expect_err("different remote address should not resolve locally");

        assert!(matches!(error, RemoteError::Inbound(_)));
        assert!(
            system
                .dead_letters()
                .wait_for_len(1, Duration::from_secs(1))
        );
        assert_eq!(
            system.dead_letters().records()[0].recipient().as_str(),
            "kairo://receiver@127.0.0.2:25520/user/target#1"
        );
    }

    #[test]
    fn local_delivery_reports_unknown_recipient_through_dead_letters() {
        let system = ActorSystem::builder("receiver").build().unwrap();
        let registry = registry();
        let inbound = RemoteInbound::<Ping>::new(
            registry.clone(),
            Arc::new(LocalActorInboundDelivery::new(system.clone())),
        );
        let envelope = RemoteEnvelope::new(
            ActorRefWireData::new("kairo://receiver/user/missing#404").unwrap(),
            None,
            registry.serialize(&Ping { value: 7 }).unwrap(),
        );

        let error = inbound
            .receive(envelope)
            .expect_err("missing local recipient should be reported");

        assert!(matches!(error, RemoteError::Inbound(_)));
        assert!(error.to_string().contains("actor does not exist"));
        assert!(
            system
                .dead_letters()
                .wait_for_len(1, Duration::from_secs(1))
        );
        let records = system.dead_letters().records();
        assert_eq!(
            records[0].recipient().as_str(),
            "kairo://receiver/user/missing#404"
        );
        assert_eq!(records[0].reason(), "actor does not exist");
    }

    #[test]
    fn local_delivery_type_mismatch_routes_to_missing_ref_diagnostics() {
        let system = ActorSystem::builder("receiver").build().unwrap();
        let registry = registry();
        let wrong_type = system.spawn("target", Props::new(|| UnitProbe)).unwrap();
        let inbound = RemoteInbound::<Ping>::new(
            registry.clone(),
            Arc::new(LocalActorInboundDelivery::new(system.clone())),
        );
        let envelope = RemoteEnvelope::new(
            ActorRefWireData::new(wrong_type.path().to_string()).unwrap(),
            None,
            registry.serialize(&Ping { value: 9 }).unwrap(),
        );

        let error = inbound
            .receive(envelope)
            .expect_err("wrong typed recipient should be reported");

        assert!(matches!(error, RemoteError::Inbound(_)));
        assert!(
            system
                .dead_letters()
                .wait_for_len(1, Duration::from_secs(1))
        );
        assert_eq!(
            system.dead_letters().records()[0].recipient(),
            wrong_type.path()
        );
    }
}
