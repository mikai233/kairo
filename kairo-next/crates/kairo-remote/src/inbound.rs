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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RemoteInboundDiagnosticFilter {
    serialization_failures: bool,
    delivery_failures: bool,
}

impl RemoteInboundDiagnosticFilter {
    pub fn new(serialization_failures: bool, delivery_failures: bool) -> Self {
        Self {
            serialization_failures,
            delivery_failures,
        }
    }

    pub fn all() -> Self {
        Self::new(true, true)
    }

    pub fn disabled() -> Self {
        Self::new(false, false)
    }

    pub fn serialization_failures(&self) -> bool {
        self.serialization_failures
    }

    pub fn delivery_failures(&self) -> bool {
        self.delivery_failures
    }

    pub fn observes(&self, diagnostic: &RemoteInboundDiagnostic) -> bool {
        match diagnostic {
            RemoteInboundDiagnostic::SerializationFailure { .. } => self.serialization_failures,
            RemoteInboundDiagnostic::DeliveryFailure { .. } => self.delivery_failures,
        }
    }

    pub fn wrap(
        self,
        diagnostics: Arc<dyn RemoteInboundDiagnostics>,
    ) -> Option<Arc<dyn RemoteInboundDiagnostics>> {
        if self == Self::disabled() {
            None
        } else if self == Self::all() {
            Some(diagnostics)
        } else {
            Some(Arc::new(FilteredRemoteInboundDiagnostics {
                filter: self,
                diagnostics,
            }))
        }
    }
}

impl Default for RemoteInboundDiagnosticFilter {
    fn default() -> Self {
        Self::all()
    }
}

struct FilteredRemoteInboundDiagnostics {
    filter: RemoteInboundDiagnosticFilter,
    diagnostics: Arc<dyn RemoteInboundDiagnostics>,
}

impl RemoteInboundDiagnostics for FilteredRemoteInboundDiagnostics {
    fn record(&self, diagnostic: RemoteInboundDiagnostic) {
        if self.filter.observes(&diagnostic) {
            self.diagnostics.record(diagnostic);
        }
    }
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

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct Pong {
        value: u8,
    }

    impl RemoteMessage for Ping {
        const MANIFEST: &'static str = "kairo.remote.test.InboundPing";
        const VERSION: u16 = 1;
    }

    impl RemoteMessage for Pong {
        const MANIFEST: &'static str = "kairo.remote.test.InboundPong";
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

    struct PongCodec;

    impl MessageCodec<Pong> for PongCodec {
        fn serializer_id(&self) -> u32 {
            100
        }

        fn encode(&self, message: &Pong) -> kairo_serialization::Result<Bytes> {
            Ok(Bytes::from(vec![message.value]))
        }

        fn decode(&self, payload: Bytes, _version: u16) -> kairo_serialization::Result<Pong> {
            Ok(Pong { value: payload[0] })
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

    fn registry_with_pong() -> Arc<Registry> {
        let mut registry = Registry::new();
        registry.register::<Ping, _>(PingCodec).unwrap();
        registry.register::<Pong, _>(PongCodec).unwrap();
        Arc::new(registry)
    }

    fn envelope(registry: &Registry, value: u8) -> RemoteEnvelope {
        RemoteEnvelope::new(
            ActorRefWireData::new("kairo://local/user/target").unwrap(),
            Some(ActorRefWireData::new("kairo://remote@127.0.0.1:25520/user/source").unwrap()),
            registry.serialize(&Ping { value }).unwrap(),
        )
    }

    fn serialization_diagnostic() -> RemoteInboundDiagnostic {
        RemoteInboundDiagnostic::SerializationFailure {
            recipient: ActorRefWireData::new("kairo://local/user/target").unwrap(),
            sender: None,
            serializer_id: 99,
            manifest: Ping::MANIFEST.to_string(),
            version: Ping::VERSION,
            reason: "decode failed".to_string(),
        }
    }

    fn delivery_diagnostic() -> RemoteInboundDiagnostic {
        RemoteInboundDiagnostic::DeliveryFailure {
            recipient: ActorRefWireData::new("kairo://local/user/target").unwrap(),
            sender: None,
            reason: "delivery failed".to_string(),
        }
    }

    #[test]
    fn diagnostic_filter_observes_configured_categories() {
        let serialization_only = RemoteInboundDiagnosticFilter::new(true, false);
        let delivery_only = RemoteInboundDiagnosticFilter::new(false, true);

        assert!(serialization_only.observes(&serialization_diagnostic()));
        assert!(!serialization_only.observes(&delivery_diagnostic()));
        assert!(!delivery_only.observes(&serialization_diagnostic()));
        assert!(delivery_only.observes(&delivery_diagnostic()));
    }

    #[test]
    fn diagnostic_filter_wraps_and_drops_disabled_categories() {
        let diagnostics = Arc::new(CollectingDiagnostics::default());
        let observer = RemoteInboundDiagnosticFilter::new(true, false)
            .wrap(diagnostics.clone() as Arc<dyn RemoteInboundDiagnostics>)
            .expect("serialization diagnostic observer should be installed");

        observer.record(serialization_diagnostic());
        observer.record(delivery_diagnostic());

        assert_eq!(diagnostics.records(), vec![serialization_diagnostic()]);
    }

    #[test]
    fn diagnostic_filter_returns_none_when_disabled() {
        let diagnostics = Arc::new(CollectingDiagnostics::default());

        assert!(
            RemoteInboundDiagnosticFilter::disabled()
                .wrap(diagnostics as Arc<dyn RemoteInboundDiagnostics>)
                .is_none()
        );
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
    fn inbound_reports_registered_wrong_message_type_as_serialization_failure() {
        let registry = registry_with_pong();
        let diagnostics = Arc::new(CollectingDiagnostics::default());
        let delivery = Arc::new(CollectingDelivery::default());
        let inbound = RemoteInbound::<Ping>::new(
            Arc::clone(&registry),
            delivery.clone() as Arc<dyn RemoteInboundDelivery<Ping>>,
        )
        .with_diagnostics(diagnostics.clone() as Arc<dyn RemoteInboundDiagnostics>);
        let recipient = ActorRefWireData::new("kairo://local/user/target").unwrap();
        let sender =
            Some(ActorRefWireData::new("kairo://remote@127.0.0.1:25520/user/source").unwrap());
        let envelope = RemoteEnvelope::new(
            recipient.clone(),
            sender.clone(),
            registry.serialize(&Pong { value: 9 }).unwrap(),
        );

        let error = inbound
            .receive(envelope)
            .expect_err("wrong registered message type should fail");

        assert!(matches!(
            error,
            RemoteError::Serialization(kairo_serialization::SerializationError::UnexpectedManifest {
                expected,
                ref actual,
            }) if expected == Ping::MANIFEST && actual == Pong::MANIFEST
        ));
        assert!(delivery.messages().is_empty());
        assert_eq!(
            diagnostics.records(),
            vec![RemoteInboundDiagnostic::SerializationFailure {
                recipient,
                sender,
                serializer_id: 100,
                manifest: Pong::MANIFEST.to_string(),
                version: Pong::VERSION,
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
