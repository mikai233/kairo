use std::marker::PhantomData;
use std::sync::Arc;

use kairo_serialization::{
    ActorRefWireData, Registry, RemoteEnvelope, RemoteMessage, SerializerId,
};

use crate::Result;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundMessage<M> {
    pub recipient: ActorRefWireData,
    pub sender: Option<ActorRefWireData>,
    pub message: M,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteInboundDiagnostic {
    SerializationFailure {
        recipient: ActorRefWireData,
        sender: Option<ActorRefWireData>,
        serializer_id: SerializerId,
        manifest: String,
        version: u16,
        reason: String,
    },
    DeliveryFailure {
        recipient: ActorRefWireData,
        sender: Option<ActorRefWireData>,
        reason: String,
    },
}

pub trait RemoteInboundDiagnostics: Send + Sync + 'static {
    fn record(&self, diagnostic: RemoteInboundDiagnostic);
}

impl<F> RemoteInboundDiagnostics for F
where
    F: Fn(RemoteInboundDiagnostic) + Send + Sync + 'static,
{
    fn record(&self, diagnostic: RemoteInboundDiagnostic) {
        self(diagnostic);
    }
}

pub trait RemoteInboundDelivery<M>: Send + Sync + 'static
where
    M: Send + 'static,
{
    fn deliver(&self, message: InboundMessage<M>) -> Result<()>;
}

impl<M, F> RemoteInboundDelivery<M> for F
where
    M: Send + 'static,
    F: Fn(InboundMessage<M>) -> Result<()> + Send + Sync + 'static,
{
    fn deliver(&self, message: InboundMessage<M>) -> Result<()> {
        self(message)
    }
}

pub struct RemoteInbound<M> {
    registry: Arc<Registry>,
    delivery: Arc<dyn RemoteInboundDelivery<M>>,
    diagnostics: Option<Arc<dyn RemoteInboundDiagnostics>>,
    _message: PhantomData<fn(M)>,
}

impl<M> RemoteInbound<M>
where
    M: RemoteMessage,
{
    pub fn new(registry: Arc<Registry>, delivery: Arc<dyn RemoteInboundDelivery<M>>) -> Self {
        Self {
            registry,
            delivery,
            diagnostics: None,
            _message: PhantomData,
        }
    }

    pub fn with_diagnostics(mut self, diagnostics: Arc<dyn RemoteInboundDiagnostics>) -> Self {
        self.diagnostics = Some(diagnostics);
        self
    }

    pub fn receive(&self, envelope: RemoteEnvelope) -> Result<()> {
        let recipient = envelope.recipient;
        let sender = envelope.sender;
        let serialized = envelope.message;
        let serializer_id = serialized.serializer_id;
        let manifest = serialized.manifest.as_str().to_string();
        let version = serialized.version;
        let message = match self.registry.deserialize::<M>(serialized) {
            Ok(message) => message,
            Err(error) => {
                self.record_diagnostic(RemoteInboundDiagnostic::SerializationFailure {
                    recipient,
                    sender,
                    serializer_id,
                    manifest,
                    version,
                    reason: error.to_string(),
                });
                return Err(error.into());
            }
        };
        let diagnostic_recipient = recipient.clone();
        let diagnostic_sender = sender.clone();
        let inbound = InboundMessage {
            recipient,
            sender,
            message,
        };
        self.delivery.deliver(inbound).inspect_err(|error| {
            self.record_diagnostic(RemoteInboundDiagnostic::DeliveryFailure {
                recipient: diagnostic_recipient,
                sender: diagnostic_sender,
                reason: error.to_string(),
            });
        })
    }

    fn record_diagnostic(&self, diagnostic: RemoteInboundDiagnostic) {
        if let Some(diagnostics) = &self.diagnostics {
            diagnostics.record(diagnostic);
        }
    }
}

impl<M> Clone for RemoteInbound<M> {
    fn clone(&self) -> Self {
        Self {
            registry: Arc::clone(&self.registry),
            delivery: Arc::clone(&self.delivery),
            diagnostics: self.diagnostics.clone(),
            _message: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use bytes::Bytes;
    use kairo_serialization::{
        Manifest, MessageCodec, RemoteMessage, SerializationRegistry, SerializedMessage,
    };

    use super::*;
    use crate::RemoteError;

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct Ping {
        value: u8,
    }

    impl RemoteMessage for Ping {
        const MANIFEST: &'static str = "kairo.remote.test.InboundPing";
        const VERSION: u16 = 1;
    }

    struct PingCodec;

    impl MessageCodec<Ping> for PingCodec {
        fn serializer_id(&self) -> u32 {
            99
        }

        fn encode(&self, message: &Ping) -> kairo_serialization::Result<Bytes> {
            Ok(Bytes::from(vec![message.value]))
        }

        fn decode(&self, payload: Bytes, _version: u16) -> kairo_serialization::Result<Ping> {
            Ok(Ping { value: payload[0] })
        }
    }

    #[derive(Default)]
    struct CollectingDelivery {
        messages: Mutex<Vec<InboundMessage<Ping>>>,
    }

    #[derive(Default)]
    struct CollectingDiagnostics {
        records: Mutex<Vec<RemoteInboundDiagnostic>>,
    }

    impl CollectingDelivery {
        fn messages(&self) -> Vec<InboundMessage<Ping>> {
            self.messages.lock().expect("delivery poisoned").clone()
        }
    }

    impl CollectingDiagnostics {
        fn records(&self) -> Vec<RemoteInboundDiagnostic> {
            self.records.lock().expect("diagnostics poisoned").clone()
        }
    }

    impl RemoteInboundDelivery<Ping> for CollectingDelivery {
        fn deliver(&self, message: InboundMessage<Ping>) -> Result<()> {
            self.messages
                .lock()
                .expect("delivery poisoned")
                .push(message);
            Ok(())
        }
    }

    impl RemoteInboundDiagnostics for CollectingDiagnostics {
        fn record(&self, diagnostic: RemoteInboundDiagnostic) {
            self.records
                .lock()
                .expect("diagnostics poisoned")
                .push(diagnostic);
        }
    }

    fn registry() -> Arc<Registry> {
        let mut registry = Registry::new();
        registry.register::<Ping, _>(PingCodec).unwrap();
        Arc::new(registry)
    }

    fn envelope(registry: &Registry, value: u8) -> RemoteEnvelope {
        RemoteEnvelope::new(
            ActorRefWireData::new("kairo://local/user/target").unwrap(),
            Some(ActorRefWireData::new("kairo://remote@127.0.0.1:25520/user/source").unwrap()),
            registry.serialize(&Ping { value }).unwrap(),
        )
    }

    #[test]
    fn inbound_deserializes_and_delivers_typed_message() {
        let registry = registry();
        let delivery = Arc::new(CollectingDelivery::default());
        let inbound = RemoteInbound::<Ping>::new(
            Arc::clone(&registry),
            delivery.clone() as Arc<dyn RemoteInboundDelivery<Ping>>,
        );

        inbound.receive(envelope(&registry, 5)).unwrap();

        let messages = delivery.messages();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].recipient.path(), "kairo://local/user/target");
        assert_eq!(
            messages[0].sender.as_ref().map(ActorRefWireData::system),
            Some("remote")
        );
        assert_eq!(messages[0].message, Ping { value: 5 });
    }

    #[test]
    fn inbound_reports_missing_wire_codec() {
        let diagnostics = Arc::new(CollectingDiagnostics::default());
        let inbound = RemoteInbound::<Ping>::new(
            Arc::new(Registry::new()),
            Arc::new(|_message: InboundMessage<Ping>| Ok(())),
        )
        .with_diagnostics(diagnostics.clone() as Arc<dyn RemoteInboundDiagnostics>);
        let recipient = ActorRefWireData::new("kairo://local/user/target").unwrap();
        let sender =
            Some(ActorRefWireData::new("kairo://remote@127.0.0.1:25520/user/source").unwrap());
        let envelope = RemoteEnvelope::new(
            recipient.clone(),
            sender.clone(),
            SerializedMessage::new(
                99,
                Manifest::new(Ping::MANIFEST),
                Ping::VERSION,
                Bytes::from_static(&[1]),
            ),
        );

        let error = inbound
            .receive(envelope)
            .expect_err("missing wire codec should fail");

        assert!(error.to_string().contains("no codec registered"));
        assert_eq!(
            diagnostics.records(),
            vec![RemoteInboundDiagnostic::SerializationFailure {
                recipient,
                sender,
                serializer_id: 99,
                manifest: Ping::MANIFEST.to_string(),
                version: Ping::VERSION,
                reason: error.to_string(),
            }]
        );
    }

    #[test]
    fn inbound_propagates_delivery_failure() {
        let registry = registry();
        let diagnostics = Arc::new(CollectingDiagnostics::default());
        let inbound = RemoteInbound::<Ping>::new(
            Arc::clone(&registry),
            Arc::new(|_message: InboundMessage<Ping>| {
                Err(RemoteError::Inbound("target stopped".to_string()))
            }),
        )
        .with_diagnostics(diagnostics.clone() as Arc<dyn RemoteInboundDiagnostics>);
        let envelope = envelope(&registry, 7);
        let recipient = envelope.recipient.clone();
        let sender = envelope.sender.clone();

        let error = inbound
            .receive(envelope)
            .expect_err("delivery failure should propagate");

        assert!(matches!(error, RemoteError::Inbound(_)));
        assert_eq!(
            diagnostics.records(),
            vec![RemoteInboundDiagnostic::DeliveryFailure {
                recipient,
                sender,
                reason: error.to_string(),
            }]
        );
    }
}
