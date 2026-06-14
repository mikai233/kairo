use std::marker::PhantomData;

use bytes::Bytes;
use kairo_serialization::RemoteMessage;

use crate::{
    AddressTerminated, RemoteDeathWatchSystemInbound, RemoteError, RemoteFrameHandler,
    RemoteHeartbeat, RemoteHeartbeatAck, RemoteInbound, RemoteStreamId, Result, UnwatchRemote,
    WatchRemote, decode_remote_envelope_frame,
};

pub struct RemoteInboundFrameRouter<M> {
    business: RemoteInbound<M>,
    death_watch: RemoteDeathWatchSystemInbound,
    _message: PhantomData<fn(M)>,
}

impl<M> RemoteInboundFrameRouter<M>
where
    M: RemoteMessage,
{
    pub fn new(business: RemoteInbound<M>, death_watch: RemoteDeathWatchSystemInbound) -> Self {
        Self {
            business,
            death_watch,
            _message: PhantomData,
        }
    }

    pub fn business(&self) -> &RemoteInbound<M> {
        &self.business
    }

    pub fn death_watch(&self) -> &RemoteDeathWatchSystemInbound {
        &self.death_watch
    }
}

impl<M> RemoteFrameHandler for RemoteInboundFrameRouter<M>
where
    M: RemoteMessage,
{
    fn handle_frame(&self, stream_id: RemoteStreamId, frame: Bytes) -> Result<()> {
        let envelope = decode_remote_envelope_frame(frame)?;
        if is_remote_death_watch_manifest(envelope.message.manifest.as_str()) {
            if stream_id != RemoteStreamId::Control {
                return Err(RemoteError::Inbound(format!(
                    "remote death-watch manifest `{}` arrived on {:?} lane",
                    envelope.message.manifest.as_str(),
                    stream_id
                )));
            }
            self.death_watch.receive(envelope)
        } else {
            self.business.receive(envelope)
        }
    }
}

pub fn is_remote_death_watch_manifest(manifest: &str) -> bool {
    matches!(
        manifest,
        WatchRemote::MANIFEST
            | UnwatchRemote::MANIFEST
            | RemoteHeartbeat::MANIFEST
            | RemoteHeartbeatAck::MANIFEST
            | AddressTerminated::MANIFEST
    )
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Condvar, Mutex};
    use std::time::{Duration, Instant};

    use bytes::Bytes;
    use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, ActorSystem, Context, Props};
    use kairo_serialization::{
        ActorRefWireData, MessageCodec, Registry, RemoteEnvelope, SerializationRegistry,
    };

    use super::*;
    use crate::{
        InboundMessage, RemoteDeathWatchActor, RemoteDeathWatchEffect, RemoteDeathWatchEffectSink,
        RemoteInboundDelivery, encode_remote_envelope_frame, register_remote_protocol_codecs,
    };

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct Business {
        value: u8,
    }

    impl RemoteMessage for Business {
        const MANIFEST: &'static str = "kairo.remote.test.RouterBusiness";
        const VERSION: u16 = 1;
    }

    struct BusinessCodec;

    impl MessageCodec<Business> for BusinessCodec {
        fn serializer_id(&self) -> u32 {
            771
        }

        fn encode(&self, message: &Business) -> kairo_serialization::Result<Bytes> {
            Ok(Bytes::from(vec![message.value]))
        }

        fn decode(&self, payload: Bytes, _version: u16) -> kairo_serialization::Result<Business> {
            Ok(Business { value: payload[0] })
        }
    }

    #[derive(Default)]
    struct CollectingBusinessDelivery {
        messages: Mutex<Vec<InboundMessage<Business>>>,
    }

    impl CollectingBusinessDelivery {
        fn messages(&self) -> Vec<InboundMessage<Business>> {
            self.messages
                .lock()
                .expect("business delivery poisoned")
                .clone()
        }
    }

    impl RemoteInboundDelivery<Business> for CollectingBusinessDelivery {
        fn deliver(&self, message: InboundMessage<Business>) -> Result<()> {
            self.messages
                .lock()
                .expect("business delivery poisoned")
                .push(message);
            Ok(())
        }
    }

    struct Probe<T> {
        sender: std::sync::mpsc::Sender<T>,
    }

    impl<T> Actor for Probe<T>
    where
        T: Send + 'static,
    {
        type Msg = T;

        fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
            self.sender
                .send(msg)
                .map_err(|error| ActorError::Message(error.to_string()))
        }
    }

    #[derive(Default)]
    struct RecordingEffectSink {
        effects: Mutex<Vec<RemoteDeathWatchEffect>>,
        changed: Condvar,
    }

    impl RecordingEffectSink {
        fn effects(&self) -> Vec<RemoteDeathWatchEffect> {
            self.effects.lock().expect("effect sink poisoned").clone()
        }

        fn wait_for_len(
            self: &Arc<Self>,
            len: usize,
            timeout: Duration,
        ) -> Vec<RemoteDeathWatchEffect> {
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

    fn registry() -> Arc<Registry> {
        let mut registry = Registry::new();
        registry.register::<Business, _>(BusinessCodec).unwrap();
        register_remote_protocol_codecs(&mut registry).unwrap();
        Arc::new(registry)
    }

    fn local_watcher() -> ActorRefWireData {
        ActorRefWireData::new("kairo://local@127.0.0.1:25521/system/remote-watch").unwrap()
    }

    fn remote_watcher() -> ActorRefWireData {
        ActorRefWireData::new("kairo://remote@127.0.0.1:25520/system/remote-watch").unwrap()
    }

    fn watchee(name: &str) -> ActorRefWireData {
        ActorRefWireData::new(format!("kairo://local@127.0.0.1:25521/user/{name}")).unwrap()
    }

    fn watcher(name: &str) -> ActorRefWireData {
        ActorRefWireData::new(format!("kairo://remote@127.0.0.1:25520/user/{name}")).unwrap()
    }

    fn business_envelope(registry: &Registry, value: u8) -> RemoteEnvelope {
        RemoteEnvelope::new(
            ActorRefWireData::new("kairo://local@127.0.0.1:25521/user/target").unwrap(),
            Some(ActorRefWireData::new("kairo://remote@127.0.0.1:25520/user/source").unwrap()),
            registry.serialize(&Business { value }).unwrap(),
        )
    }

    fn watch_envelope(registry: &Registry) -> RemoteEnvelope {
        let watchee = watchee("target");
        let watcher = watcher("observer");
        RemoteEnvelope::new(
            local_watcher(),
            Some(remote_watcher()),
            registry
                .serialize(&WatchRemote { watchee, watcher })
                .unwrap(),
        )
    }

    fn address_terminated_envelope(registry: &Registry) -> RemoteEnvelope {
        RemoteEnvelope::new(
            local_watcher(),
            Some(remote_watcher()),
            registry
                .serialize(&AddressTerminated {
                    address: "kairo://remote@127.0.0.1:25520".to_string(),
                    uid: Some(11),
                })
                .unwrap(),
        )
    }

    struct RouterFixture {
        router: RemoteInboundFrameRouter<Business>,
        death_watch: ActorRef<crate::RemoteDeathWatchCommand>,
        system: ActorSystem,
    }

    fn router(
        registry: Arc<Registry>,
        delivery: Arc<CollectingBusinessDelivery>,
        effects: Arc<RecordingEffectSink>,
    ) -> RouterFixture {
        let system = ActorSystem::builder("local").build().unwrap();
        let watcher = system
            .spawn(
                "remote-watch",
                RemoteDeathWatchActor::props(effects as Arc<dyn RemoteDeathWatchEffectSink>),
            )
            .unwrap();
        let router = RemoteInboundFrameRouter::new(
            RemoteInbound::new(
                registry.clone(),
                delivery as Arc<dyn RemoteInboundDelivery<Business>>,
            ),
            RemoteDeathWatchSystemInbound::new(
                registry,
                crate::RemoteDeathWatchProtocolDelivery::new(watcher.clone(), 42),
            ),
        );
        RouterFixture {
            router,
            death_watch: watcher,
            system,
        }
    }

    #[test]
    fn router_sends_business_frames_to_business_inbound() {
        let registry = registry();
        let delivery = Arc::new(CollectingBusinessDelivery::default());
        let effects = Arc::new(RecordingEffectSink::default());
        let fixture = router(registry.clone(), delivery.clone(), effects.clone());
        let frame = encode_remote_envelope_frame(&business_envelope(&registry, 9)).unwrap();

        fixture
            .router
            .handle_frame(RemoteStreamId::Ordinary, frame)
            .expect("business frame should route");

        assert_eq!(delivery.messages().len(), 1);
        assert_eq!(delivery.messages()[0].message, Business { value: 9 });
        assert!(effects.effects().is_empty());
    }

    #[test]
    fn router_sends_death_watch_frames_to_system_inbound() {
        let registry = registry();
        let delivery = Arc::new(CollectingBusinessDelivery::default());
        let effects = Arc::new(RecordingEffectSink::default());
        let fixture = router(registry.clone(), delivery.clone(), effects.clone());
        let frame = encode_remote_envelope_frame(&watch_envelope(&registry)).unwrap();
        let (stats_tx, stats_rx) = std::sync::mpsc::channel();
        let stats_probe = fixture
            .system
            .spawn("stats", Props::new(move || Probe { sender: stats_tx }))
            .unwrap();

        fixture
            .router
            .handle_frame(RemoteStreamId::Control, frame)
            .expect("watch frame should route");
        fixture
            .death_watch
            .tell(crate::RemoteDeathWatchCommand::GetStats {
                reply_to: stats_probe,
            })
            .unwrap();

        assert!(delivery.messages().is_empty());
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
    fn router_sends_address_terminated_frames_to_system_inbound() {
        let registry = registry();
        let delivery = Arc::new(CollectingBusinessDelivery::default());
        let effects = Arc::new(RecordingEffectSink::default());
        let fixture = router(registry.clone(), delivery.clone(), effects.clone());
        let terminated_frame =
            encode_remote_envelope_frame(&address_terminated_envelope(&registry)).unwrap();

        fixture
            .death_watch
            .tell(crate::RemoteDeathWatchCommand::Watch(WatchRemote {
                watchee: watcher("target"),
                watcher: watchee("observer"),
            }))
            .unwrap();
        fixture
            .router
            .handle_frame(RemoteStreamId::Control, terminated_frame)
            .expect("address-terminated frame should route");

        assert!(delivery.messages().is_empty());
        let observed = effects.wait_for_len(3, Duration::from_secs(1));
        assert!(matches!(
            observed.last(),
            Some(RemoteDeathWatchEffect::AddressTerminated(AddressTerminated {
                address,
                uid: Some(11),
            })) if address == "kairo://remote@127.0.0.1:25520"
        ));
    }

    #[test]
    fn router_rejects_death_watch_frames_on_non_control_lane() {
        let registry = registry();
        let delivery = Arc::new(CollectingBusinessDelivery::default());
        let effects = Arc::new(RecordingEffectSink::default());
        let fixture = router(registry.clone(), delivery, effects);
        let frame = encode_remote_envelope_frame(&watch_envelope(&registry)).unwrap();

        let error = fixture
            .router
            .handle_frame(RemoteStreamId::Ordinary, frame)
            .expect_err("death-watch frame on ordinary lane should fail");

        assert!(matches!(error, RemoteError::Inbound(_)));
        assert!(error.to_string().contains("arrived on Ordinary lane"));
    }

    #[test]
    fn death_watch_manifest_helper_matches_only_remote_watch_protocol() {
        assert!(is_remote_death_watch_manifest(WatchRemote::MANIFEST));
        assert!(is_remote_death_watch_manifest(UnwatchRemote::MANIFEST));
        assert!(is_remote_death_watch_manifest(RemoteHeartbeat::MANIFEST));
        assert!(is_remote_death_watch_manifest(RemoteHeartbeatAck::MANIFEST));
        assert!(is_remote_death_watch_manifest(AddressTerminated::MANIFEST));
        assert!(!is_remote_death_watch_manifest(Business::MANIFEST));
    }
}
