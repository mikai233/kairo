use std::marker::PhantomData;
use std::sync::Arc;

use bytes::Bytes;
use kairo_actor::{ActorRef, ActorSystem};
use kairo_serialization::{Registry, RemoteMessage};

use crate::{
    AssociationRemoteInbound, LocalActorInboundDelivery, RemoteDeathWatchCommand,
    RemoteDeathWatchProtocolDelivery, RemoteDeathWatchSystemInbound, RemoteFrameHandler,
    RemoteInbound, RemoteInboundFrameRouter, RemoteStreamId, Result,
};

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
        let business = RemoteInbound::new(
            registry.clone(),
            Arc::new(LocalActorInboundDelivery::<M>::new(system)),
        );
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
        ActorRefWireData, MessageCodec, RemoteEnvelope, SerializationRegistry,
    };

    use super::*;
    use crate::WatchRemote;
    use crate::{
        RemoteByteSink, RemoteDeathWatchActor, RemoteDeathWatchEffect, RemoteDeathWatchEffectSink,
        RemoteError, RemoteLaneSink, RemoteStreamEncoder, StreamLaneSink,
        encode_remote_envelope_frame, register_remote_protocol_codecs, stream_send_failure,
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

    fn registry() -> Arc<Registry> {
        let mut registry = Registry::new();
        registry.register::<Ping, _>(PingCodec).unwrap();
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

        inbound
            .handle_frame(
                RemoteStreamId::Control,
                encode_remote_envelope_frame(&envelope).unwrap(),
            )
            .unwrap();

        let effects = effects.wait_for_len(2, Duration::from_secs(1));
        assert!(matches!(
            effects.as_slice(),
            [
                RemoteDeathWatchEffect::StartHeartbeat { .. },
                RemoteDeathWatchEffect::SendWatchRemote(_)
            ]
        ));
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

    #[test]
    fn stream_send_failure_helper_remains_available_for_lane_errors() {
        let error = stream_send_failure(RemoteStreamId::Ordinary, "closed".to_string());
        assert!(matches!(error, RemoteError::Outbound(_)));

        let bytes = Arc::new(|_bytes: Bytes| Ok(())) as Arc<dyn RemoteByteSink>;
        let sink = StreamLaneSink::new(bytes.clone(), bytes.clone(), bytes);
        sink.send_lane_frame(RemoteStreamId::Ordinary, Bytes::new())
            .unwrap();
    }
}
