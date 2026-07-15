use std::marker::PhantomData;
use std::sync::Arc;

use bytes::Bytes;
use kairo_actor::{ActorRef, ActorSystem};
use kairo_serialization::{Registry, RemoteMessage};

use crate::{
    AssociationRemoteInbound, LocalActorInboundDelivery, ManifestRemoteInboundRouter,
    RemoteDeathWatchCommand, RemoteDeathWatchProtocolDelivery, RemoteDeathWatchSystemInbound,
    RemoteFrameHandler, RemoteInbound, RemoteInboundDiagnostics, RemoteInboundFrameRouter,
    RemoteSettings, RemoteStreamId, Result,
};

/// ActorSystem-backed manifest registry used by the composed remoting runtime.
pub struct ActorSystemRemoteInboundRegistry {
    system: ActorSystem,
    registry: Arc<Registry>,
    settings: Option<RemoteSettings>,
    diagnostics: Option<Arc<dyn RemoteInboundDiagnostics>>,
    router: ManifestRemoteInboundRouter,
}

impl ActorSystemRemoteInboundRegistry {
    pub fn new(
        system: ActorSystem,
        registry: Arc<Registry>,
        death_watch: ActorRef<RemoteDeathWatchCommand>,
        local_system_uid: u64,
    ) -> Self {
        Self::build(system, registry, death_watch, local_system_uid, None, None)
    }

    pub fn with_remote_settings(
        system: ActorSystem,
        registry: Arc<Registry>,
        death_watch: ActorRef<RemoteDeathWatchCommand>,
        local_system_uid: u64,
        settings: RemoteSettings,
    ) -> Self {
        Self::build(
            system,
            registry,
            death_watch,
            local_system_uid,
            Some(settings),
            None,
        )
    }

    pub fn with_remote_settings_and_diagnostics(
        system: ActorSystem,
        registry: Arc<Registry>,
        death_watch: ActorRef<RemoteDeathWatchCommand>,
        local_system_uid: u64,
        settings: RemoteSettings,
        diagnostics: Arc<dyn RemoteInboundDiagnostics>,
    ) -> Self {
        Self::build(
            system,
            registry,
            death_watch,
            local_system_uid,
            Some(settings),
            Some(diagnostics),
        )
    }

    fn build(
        system: ActorSystem,
        registry: Arc<Registry>,
        death_watch: ActorRef<RemoteDeathWatchCommand>,
        local_system_uid: u64,
        settings: Option<RemoteSettings>,
        diagnostics: Option<Arc<dyn RemoteInboundDiagnostics>>,
    ) -> Self {
        let death_watch = RemoteDeathWatchSystemInbound::new(
            registry.clone(),
            RemoteDeathWatchProtocolDelivery::new(death_watch, local_system_uid),
        );
        Self {
            system,
            registry,
            settings,
            diagnostics,
            router: ManifestRemoteInboundRouter::new(death_watch),
        }
    }

    pub fn register<M>(&mut self) -> Result<&mut Self>
    where
        M: RemoteMessage,
    {
        let delivery = match &self.settings {
            Some(settings) => LocalActorInboundDelivery::<M>::with_remote_settings(
                self.system.clone(),
                settings.clone(),
            ),
            None => LocalActorInboundDelivery::<M>::new(self.system.clone()),
        };
        let mut inbound = RemoteInbound::new(self.registry.clone(), Arc::new(delivery));
        if let Some(diagnostics) = &self.diagnostics {
            inbound = inbound.with_diagnostics(diagnostics.clone());
        }
        self.router.register(inbound)?;
        Ok(self)
    }

    pub fn router(&self) -> &ManifestRemoteInboundRouter {
        &self.router
    }

    pub(crate) fn router_mut(&mut self) -> &mut ManifestRemoteInboundRouter {
        &mut self.router
    }
}

impl RemoteFrameHandler for ActorSystemRemoteInboundRegistry {
    fn handle_frame(&self, stream_id: RemoteStreamId, frame: Bytes) -> Result<()> {
        self.router.handle_frame(stream_id, frame)
    }
}

pub struct ActorSystemRemoteInbound<M> {
    router: RemoteInboundFrameRouter<M>,
    _message: PhantomData<fn(M)>,
}

impl<M> ActorSystemRemoteInbound<M>
where
    M: RemoteMessage,
{
    pub fn new(
        system: ActorSystem,
        registry: Arc<Registry>,
        death_watch: ActorRef<RemoteDeathWatchCommand>,
        local_system_uid: u64,
    ) -> Self {
        Self::with_delivery(
            system,
            registry,
            death_watch,
            local_system_uid,
            LocalActorInboundDelivery::<M>::new,
            None,
        )
    }

    pub fn with_diagnostics(
        system: ActorSystem,
        registry: Arc<Registry>,
        death_watch: ActorRef<RemoteDeathWatchCommand>,
        local_system_uid: u64,
        diagnostics: Arc<dyn RemoteInboundDiagnostics>,
    ) -> Self {
        Self::with_delivery(
            system,
            registry,
            death_watch,
            local_system_uid,
            LocalActorInboundDelivery::<M>::new,
            Some(diagnostics),
        )
    }

    pub fn with_remote_settings(
        system: ActorSystem,
        registry: Arc<Registry>,
        death_watch: ActorRef<RemoteDeathWatchCommand>,
        local_system_uid: u64,
        settings: RemoteSettings,
    ) -> Self {
        Self::with_delivery(
            system,
            registry,
            death_watch,
            local_system_uid,
            move |system| LocalActorInboundDelivery::<M>::with_remote_settings(system, settings),
            None,
        )
    }

    pub fn with_remote_settings_and_diagnostics(
        system: ActorSystem,
        registry: Arc<Registry>,
        death_watch: ActorRef<RemoteDeathWatchCommand>,
        local_system_uid: u64,
        settings: RemoteSettings,
        diagnostics: Arc<dyn RemoteInboundDiagnostics>,
    ) -> Self {
        Self::with_delivery(
            system,
            registry,
            death_watch,
            local_system_uid,
            move |system| LocalActorInboundDelivery::<M>::with_remote_settings(system, settings),
            Some(diagnostics),
        )
    }

    fn with_delivery(
        system: ActorSystem,
        registry: Arc<Registry>,
        death_watch: ActorRef<RemoteDeathWatchCommand>,
        local_system_uid: u64,
        delivery: impl FnOnce(ActorSystem) -> LocalActorInboundDelivery<M>,
        diagnostics: Option<Arc<dyn RemoteInboundDiagnostics>>,
    ) -> Self {
        let mut business = RemoteInbound::new(registry.clone(), Arc::new(delivery(system)));
        if let Some(diagnostics) = diagnostics {
            business = business.with_diagnostics(diagnostics);
        }
        let death_watch = RemoteDeathWatchSystemInbound::new(
            registry,
            RemoteDeathWatchProtocolDelivery::new(death_watch, local_system_uid),
        );
        Self {
            router: RemoteInboundFrameRouter::new(business, death_watch),
            _message: PhantomData,
        }
    }

    pub fn router(&self) -> &RemoteInboundFrameRouter<M> {
        &self.router
    }

    pub fn into_association_inbound(self) -> AssociationRemoteInbound<M> {
        AssociationRemoteInbound::from_handler(Arc::new(self))
    }
}

impl<M> RemoteFrameHandler for ActorSystemRemoteInbound<M>
where
    M: RemoteMessage,
{
    fn handle_frame(&self, stream_id: RemoteStreamId, frame: Bytes) -> Result<()> {
        self.router.handle_frame(stream_id, frame)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Condvar, Mutex, mpsc};
    use std::time::{Duration, Instant};

    use bytes::Bytes;
    use kairo_actor::{Actor, ActorResult, Context, Props};
    use kairo_serialization::{
        ActorRefWireData, MessageCodec, RemoteEnvelope, SerializationRegistry, SerializedMessage,
    };

    use super::*;
    use crate::WatchRemote;
    use crate::{
        RemoteDeathWatchActor, RemoteDeathWatchEffect, RemoteDeathWatchEffectSink, RemoteError,
        RemoteInboundDiagnostic, RemoteStreamEncoder, encode_remote_envelope_frame,
        register_remote_protocol_codecs,
    };

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct Ping {
        value: u8,
    }

    impl RemoteMessage for Ping {
        const MANIFEST: &'static str = "kairo.remote.test.SystemInboundPing";
        const VERSION: u16 = 1;
    }

    struct PingCodec;

    impl MessageCodec<Ping> for PingCodec {
        fn serializer_id(&self) -> u32 {
            901
        }

        fn encode(&self, message: &Ping) -> kairo_serialization::Result<Bytes> {
            Ok(Bytes::from(vec![message.value]))
        }

        fn decode(&self, payload: Bytes, _version: u16) -> kairo_serialization::Result<Ping> {
            Ok(Ping { value: payload[0] })
        }
    }

    struct PanickingPingCodec;

    impl MessageCodec<Ping> for PanickingPingCodec {
        fn serializer_id(&self) -> u32 {
            901
        }

        fn encode(&self, message: &Ping) -> kairo_serialization::Result<Bytes> {
            Ok(Bytes::from(vec![message.value]))
        }

        fn decode(&self, _payload: Bytes, _version: u16) -> kairo_serialization::Result<Ping> {
            panic!("remote business codec decode failed")
        }
    }

    struct Target {
        received: mpsc::Sender<u8>,
    }

    impl Actor for Target {
        type Msg = Ping;

        fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
            self.received
                .send(msg.value)
                .map_err(|error| kairo_actor::ActorError::Message(error.to_string()))
        }
    }

    struct Probe<T> {
        sender: mpsc::Sender<T>,
    }

    impl<T> Actor for Probe<T>
    where
        T: Send + 'static,
    {
        type Msg = T;

        fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
            self.sender
                .send(msg)
                .map_err(|error| kairo_actor::ActorError::Message(error.to_string()))
        }
    }

    #[derive(Default)]
    struct RecordingEffectSink {
        effects: Mutex<Vec<RemoteDeathWatchEffect>>,
        changed: Condvar,
    }

    impl RecordingEffectSink {
        fn wait_for_len(&self, len: usize, timeout: Duration) -> Vec<RemoteDeathWatchEffect> {
            let deadline = Instant::now() + timeout;
            let mut effects = self.effects.lock().expect("effect sink poisoned");
            while effects.len() < len {
                let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                    break;
                };
                let (next_effects, wait) = self
                    .changed
                    .wait_timeout(effects, remaining)
                    .expect("effect sink poisoned");
                effects = next_effects;
                if wait.timed_out() {
                    break;
                }
            }
            effects.clone()
        }
    }

    impl RemoteDeathWatchEffectSink for RecordingEffectSink {
        fn apply(&self, effects: Vec<RemoteDeathWatchEffect>) -> Result<()> {
            self.effects
                .lock()
                .expect("effect sink poisoned")
                .extend(effects);
            self.changed.notify_all();
            Ok(())
        }
    }

    #[derive(Default)]
    struct CollectingDiagnostics {
        records: Mutex<Vec<RemoteInboundDiagnostic>>,
    }

    impl CollectingDiagnostics {
        fn records(&self) -> Vec<RemoteInboundDiagnostic> {
            self.records.lock().expect("diagnostics poisoned").clone()
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
        register_remote_protocol_codecs(&mut registry).unwrap();
        Arc::new(registry)
    }

    fn panicking_registry() -> Arc<Registry> {
        let mut registry = Registry::new();
        registry.register::<Ping, _>(PanickingPingCodec).unwrap();
        register_remote_protocol_codecs(&mut registry).unwrap();
        Arc::new(registry)
    }

    fn local_watcher() -> ActorRefWireData {
        ActorRefWireData::new("kairo://receiver@127.0.0.1:25520/system/remote-watch").unwrap()
    }

    fn remote_watcher() -> ActorRefWireData {
        ActorRefWireData::new("kairo://sender@127.0.0.1:25521/system/remote-watch").unwrap()
    }

    fn remote_actor(name: &str) -> ActorRefWireData {
        ActorRefWireData::new(format!("kairo://sender@127.0.0.1:25521/user/{name}")).unwrap()
    }

    fn local_actor(name: &str) -> ActorRefWireData {
        ActorRefWireData::new(format!("kairo://receiver@127.0.0.1:25520/user/{name}")).unwrap()
    }

    #[test]
    fn system_inbound_routes_business_frames_to_local_actor_refs() {
        let system = ActorSystem::builder("receiver").build().unwrap();
        let registry = registry();
        let (tx, rx) = mpsc::channel();
        let target = system
            .spawn("target", Props::new(move || Target { received: tx }))
            .unwrap();
        let effects = Arc::new(RecordingEffectSink::default());
        let death_watch = system
            .spawn(
                "remote-watch",
                RemoteDeathWatchActor::props(effects as Arc<dyn RemoteDeathWatchEffectSink>),
            )
            .unwrap();
        let inbound = ActorSystemRemoteInbound::<Ping>::new(
            system.clone(),
            registry.clone(),
            death_watch,
            42,
        );
        let envelope = RemoteEnvelope::new(
            ActorRefWireData::new(target.path().to_string()).unwrap(),
            Some(remote_actor("source")),
            registry.serialize(&Ping { value: 11 }).unwrap(),
        );

        inbound
            .handle_frame(
                RemoteStreamId::Ordinary,
                encode_remote_envelope_frame(&envelope).unwrap(),
            )
            .unwrap();

        assert_eq!(rx.recv_timeout(Duration::from_secs(1)).unwrap(), 11);
        assert!(system.dead_letters().is_empty());
    }

    #[test]
    fn system_inbound_reports_business_deserialization_diagnostics() {
        let system = ActorSystem::builder("receiver-business-decode-diagnostics")
            .build()
            .unwrap();
        let registry = Arc::new(Registry::new());
        let effects = Arc::new(RecordingEffectSink::default());
        let death_watch = system
            .spawn(
                "remote-watch",
                RemoteDeathWatchActor::props(effects as Arc<dyn RemoteDeathWatchEffectSink>),
            )
            .unwrap();
        let diagnostics = Arc::new(CollectingDiagnostics::default());
        let inbound = ActorSystemRemoteInbound::<Ping>::with_diagnostics(
            system,
            registry,
            death_watch,
            42,
            diagnostics.clone() as Arc<dyn RemoteInboundDiagnostics>,
        );
        let envelope = RemoteEnvelope::new(
            local_actor("target"),
            Some(remote_actor("source")),
            SerializedMessage::new(
                901,
                Ping::MANIFEST.into(),
                Ping::VERSION,
                Bytes::from(vec![7]),
            ),
        );

        let error = inbound
            .handle_frame(
                RemoteStreamId::Ordinary,
                encode_remote_envelope_frame(&envelope).unwrap(),
            )
            .expect_err("missing business codec should fail");

        assert!(matches!(error, RemoteError::Serialization(_)));
        assert_eq!(
            diagnostics.records(),
            vec![RemoteInboundDiagnostic::SerializationFailure {
                recipient: local_actor("target"),
                sender: Some(remote_actor("source")),
                serializer_id: 901,
                manifest: Ping::MANIFEST.to_string(),
                version: Ping::VERSION,
                reason: error.to_string(),
            }]
        );
    }

    #[test]
    fn system_inbound_reports_business_codec_panics_as_serialization_diagnostics() {
        let system = ActorSystem::builder("receiver-business-panic-diagnostics")
            .build()
            .unwrap();
        let registry = panicking_registry();
        let effects = Arc::new(RecordingEffectSink::default());
        let death_watch = system
            .spawn(
                "remote-watch",
                RemoteDeathWatchActor::props(effects as Arc<dyn RemoteDeathWatchEffectSink>),
            )
            .unwrap();
        let diagnostics = Arc::new(CollectingDiagnostics::default());
        let inbound = ActorSystemRemoteInbound::<Ping>::with_diagnostics(
            system.clone(),
            registry,
            death_watch,
            42,
            diagnostics.clone() as Arc<dyn RemoteInboundDiagnostics>,
        );
        let envelope = RemoteEnvelope::new(
            local_actor("target"),
            Some(remote_actor("source")),
            SerializedMessage::new(
                901,
                Ping::MANIFEST.into(),
                Ping::VERSION,
                Bytes::from(vec![7]),
            ),
        );

        let error = inbound
            .handle_frame(
                RemoteStreamId::Ordinary,
                encode_remote_envelope_frame(&envelope).unwrap(),
            )
            .expect_err("panicking business codec should fail as serialization");

        assert!(matches!(error, RemoteError::Serialization(_)));
        assert!(error.to_string().contains("codec decode panicked"));
        assert!(system.dead_letters().is_empty());
        assert_eq!(
            diagnostics.records(),
            vec![RemoteInboundDiagnostic::SerializationFailure {
                recipient: local_actor("target"),
                sender: Some(remote_actor("source")),
                serializer_id: 901,
                manifest: Ping::MANIFEST.to_string(),
                version: Ping::VERSION,
                reason: error.to_string(),
            }]
        );
    }

    #[test]
    fn system_inbound_reports_business_delivery_diagnostics() {
        let system = ActorSystem::builder("receiver-business-delivery-diagnostics")
            .build()
            .unwrap();
        let registry = registry();
        let effects = Arc::new(RecordingEffectSink::default());
        let death_watch = system
            .spawn(
                "remote-watch",
                RemoteDeathWatchActor::props(effects as Arc<dyn RemoteDeathWatchEffectSink>),
            )
            .unwrap();
        let diagnostics = Arc::new(CollectingDiagnostics::default());
        let inbound = ActorSystemRemoteInbound::<Ping>::with_remote_settings_and_diagnostics(
            system,
            registry.clone(),
            death_watch,
            42,
            RemoteSettings::new("127.0.0.1", 25520),
            diagnostics.clone() as Arc<dyn RemoteInboundDiagnostics>,
        );
        let envelope = RemoteEnvelope::new(
            local_actor("missing"),
            Some(remote_actor("source")),
            registry.serialize(&Ping { value: 15 }).unwrap(),
        );

        let error = inbound
            .handle_frame(
                RemoteStreamId::Ordinary,
                encode_remote_envelope_frame(&envelope).unwrap(),
            )
            .expect_err("missing local recipient should fail delivery");

        assert!(matches!(error, RemoteError::Inbound(_)));
        assert_eq!(
            diagnostics.records(),
            vec![RemoteInboundDiagnostic::DeliveryFailure {
                recipient: local_actor("missing"),
                sender: Some(remote_actor("source")),
                reason: error.to_string(),
            }]
        );
    }

    #[test]
    fn system_inbound_routes_control_frames_to_remote_death_watch() {
        let system = ActorSystem::builder("receiver-watch").build().unwrap();
        let registry = registry();
        let effects = Arc::new(RecordingEffectSink::default());
        let death_watch =
            system
                .spawn(
                    "remote-watch",
                    RemoteDeathWatchActor::props(
                        effects.clone() as Arc<dyn RemoteDeathWatchEffectSink>
                    ),
                )
                .unwrap();
        let (stats_tx, stats_rx) = mpsc::channel();
        let stats_probe = system
            .spawn("stats", Props::new(move || Probe { sender: stats_tx }))
            .unwrap();
        let inbound = ActorSystemRemoteInbound::<Ping>::new(
            system,
            registry.clone(),
            death_watch.clone(),
            42,
        );
        let watch = WatchRemote {
            watchee: local_actor("target"),
            watcher: remote_actor("watcher"),
        };
        let envelope = RemoteEnvelope::new(
            local_watcher(),
            Some(remote_watcher()),
            registry.serialize(&watch).unwrap(),
        );

        inbound
            .handle_frame(
                RemoteStreamId::Control,
                encode_remote_envelope_frame(&envelope).unwrap(),
            )
            .unwrap();

        death_watch
            .tell(RemoteDeathWatchCommand::GetStats {
                reply_to: stats_probe,
            })
            .unwrap();
        let stats = stats_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(stats.inbound_watching, 1);
        assert_eq!(stats.watching, 0);
        assert_eq!(stats.watched_addresses, 0);
        assert!(
            effects
                .wait_for_len(1, Duration::from_millis(50))
                .is_empty()
        );
    }

    #[test]
    fn system_inbound_can_build_association_lane_reader() {
        let system = ActorSystem::builder("receiver-association")
            .build()
            .unwrap();
        let registry = registry();
        let (tx, rx) = mpsc::channel();
        let target = system
            .spawn("target", Props::new(move || Target { received: tx }))
            .unwrap();
        let effects = Arc::new(RecordingEffectSink::default());
        let death_watch = system
            .spawn(
                "remote-watch",
                RemoteDeathWatchActor::props(effects as Arc<dyn RemoteDeathWatchEffectSink>),
            )
            .unwrap();
        let envelope = RemoteEnvelope::new(
            ActorRefWireData::new(target.path().to_string()).unwrap(),
            None,
            registry.serialize(&Ping { value: 12 }).unwrap(),
        );
        let frame = encode_remote_envelope_frame(&envelope).unwrap();
        let mut inbound = ActorSystemRemoteInbound::<Ping>::new(system, registry, death_watch, 42)
            .into_association_inbound();
        let stream_bytes = RemoteStreamEncoder::new(RemoteStreamId::Ordinary)
            .encode_frame(&frame)
            .unwrap();

        inbound.push_ordinary_bytes(stream_bytes).unwrap();
        inbound.finish().unwrap();

        assert_eq!(rx.recv_timeout(Duration::from_secs(1)).unwrap(), 12);
    }

    #[test]
    fn system_inbound_rejects_control_manifest_on_ordinary_lane() {
        let system = ActorSystem::builder("receiver-wrong-lane").build().unwrap();
        let registry = registry();
        let effects = Arc::new(RecordingEffectSink::default());
        let death_watch = system
            .spawn(
                "remote-watch",
                RemoteDeathWatchActor::props(effects as Arc<dyn RemoteDeathWatchEffectSink>),
            )
            .unwrap();
        let inbound =
            ActorSystemRemoteInbound::<Ping>::new(system, registry.clone(), death_watch, 42);
        let watch = WatchRemote {
            watchee: remote_actor("target"),
            watcher: ActorRefWireData::new("kairo://receiver@127.0.0.1:25520/user/watcher")
                .unwrap(),
        };
        let envelope = RemoteEnvelope::new(
            local_watcher(),
            Some(remote_watcher()),
            registry.serialize(&watch).unwrap(),
        );

        let error = inbound
            .handle_frame(
                RemoteStreamId::Ordinary,
                encode_remote_envelope_frame(&envelope).unwrap(),
            )
            .expect_err("remote system manifests must stay on control lane");

        assert!(matches!(error, RemoteError::Inbound(_)));
        assert!(error.to_string().contains("arrived on Ordinary lane"));
    }
}
