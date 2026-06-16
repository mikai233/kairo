use std::sync::Arc;

use kairo_serialization::{Registry, RemoteEnvelope, RemoteMessage};

use crate::{
    AddressTerminated, InboundMessage, RemoteDeathWatchProtocolDelivery, RemoteError,
    RemoteHeartbeat, RemoteHeartbeatAck, RemoteInboundDelivery, RemoteTerminated, Result,
    UnwatchRemote, WatchRemote,
};

#[derive(Clone)]
pub struct RemoteDeathWatchSystemInbound {
    registry: Arc<Registry>,
    delivery: RemoteDeathWatchProtocolDelivery,
}

impl RemoteDeathWatchSystemInbound {
    pub fn new(registry: Arc<Registry>, delivery: RemoteDeathWatchProtocolDelivery) -> Self {
        Self { registry, delivery }
    }

    pub fn registry(&self) -> &Arc<Registry> {
        &self.registry
    }

    pub fn delivery(&self) -> &RemoteDeathWatchProtocolDelivery {
        &self.delivery
    }

    pub fn receive(&self, envelope: RemoteEnvelope) -> Result<()> {
        match envelope.message.manifest.as_str() {
            WatchRemote::MANIFEST => self.receive_typed::<WatchRemote>(envelope),
            UnwatchRemote::MANIFEST => self.receive_typed::<UnwatchRemote>(envelope),
            RemoteTerminated::MANIFEST => self.receive_typed::<RemoteTerminated>(envelope),
            RemoteHeartbeat::MANIFEST => self.receive_typed::<RemoteHeartbeat>(envelope),
            RemoteHeartbeatAck::MANIFEST => self.receive_typed::<RemoteHeartbeatAck>(envelope),
            AddressTerminated::MANIFEST => self.receive_typed::<AddressTerminated>(envelope),
            manifest => Err(RemoteError::Inbound(format!(
                "unsupported remote death-watch manifest `{manifest}`"
            ))),
        }
    }

    fn receive_typed<M>(&self, envelope: RemoteEnvelope) -> Result<()>
    where
        M: RemoteMessage,
        RemoteDeathWatchProtocolDelivery: RemoteInboundDelivery<M>,
    {
        let message = self.registry.deserialize::<M>(envelope.message)?;
        self.delivery.deliver(InboundMessage {
            recipient: envelope.recipient,
            sender: envelope.sender,
            message,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Condvar, Mutex};
    use std::time::{Duration, Instant};

    use bytes::Bytes;
    use kairo_actor::ActorSystem;
    use kairo_serialization::{ActorRefWireData, Manifest, RemoteMessage, SerializedMessage};

    use super::*;
    use crate::{
        RemoteDeathWatchActor, RemoteDeathWatchEffect, RemoteDeathWatchEffectSink,
        RemoteTerminated, register_remote_protocol_codecs,
    };

    #[derive(Default)]
    struct RecordingEffectSink {
        effects: Mutex<Vec<RemoteDeathWatchEffect>>,
        changed: Condvar,
    }

    impl RecordingEffectSink {
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
        ActorRefWireData::new(format!("kairo://remote@127.0.0.1:25520/user/{name}")).unwrap()
    }

    fn watcher(name: &str) -> ActorRefWireData {
        ActorRefWireData::new(format!("kairo://local@127.0.0.1:25521/user/{name}")).unwrap()
    }

    fn envelope<M>(
        registry: &Registry,
        message: &M,
        sender: Option<ActorRefWireData>,
    ) -> RemoteEnvelope
    where
        M: RemoteMessage,
    {
        RemoteEnvelope::new(
            local_watcher(),
            sender,
            registry.serialize(message).unwrap(),
        )
    }

    fn inbound(
        registry: Arc<Registry>,
        sink: Arc<RecordingEffectSink>,
    ) -> (
        RemoteDeathWatchSystemInbound,
        kairo_actor::ActorRef<crate::RemoteDeathWatchCommand>,
    ) {
        let system = ActorSystem::builder("local").build().unwrap();
        let watcher = system
            .spawn(
                "remote-watch",
                RemoteDeathWatchActor::props(sink as Arc<dyn RemoteDeathWatchEffectSink>),
            )
            .unwrap();
        (
            RemoteDeathWatchSystemInbound::new(
                registry,
                RemoteDeathWatchProtocolDelivery::new(watcher.clone(), 42),
            ),
            watcher,
        )
    }

    #[test]
    fn system_inbound_routes_watch_unwatch_and_heartbeat_envelopes() {
        let registry = registry();
        let sink = Arc::new(RecordingEffectSink::default());
        let (inbound, _watcher_actor) = inbound(registry.clone(), sink.clone());
        let watchee = watchee("target");
        let watcher = watcher("observer");

        inbound
            .receive(envelope(
                &registry,
                &WatchRemote {
                    watchee: watchee.clone(),
                    watcher: watcher.clone(),
                },
                Some(remote_watcher()),
            ))
            .unwrap();
        inbound
            .receive(envelope(
                &registry,
                &RemoteHeartbeat { from_uid: 7 },
                Some(remote_watcher()),
            ))
            .unwrap();
        inbound
            .receive(envelope(
                &registry,
                &UnwatchRemote {
                    watchee: watchee.clone(),
                    watcher: watcher.clone(),
                },
                Some(remote_watcher()),
            ))
            .unwrap();

        assert_eq!(
            sink.wait_for_len(1, Duration::from_secs(1)),
            vec![RemoteDeathWatchEffect::SendHeartbeatAck {
                address: "kairo://remote@127.0.0.1:25520".to_string(),
                message: RemoteHeartbeatAck { uid: 42 }
            }]
        );
    }

    #[test]
    fn system_inbound_deserializes_ack_and_drives_rewatch() {
        let registry = registry();
        let sink = Arc::new(RecordingEffectSink::default());
        let (inbound, watcher_actor) = inbound(registry.clone(), sink.clone());
        let watchee = watchee("target");
        let watcher = watcher("observer");

        watcher_actor
            .tell(crate::RemoteDeathWatchCommand::Watch(WatchRemote {
                watchee: watchee.clone(),
                watcher: watcher.clone(),
            }))
            .unwrap();
        inbound
            .receive(envelope(
                &registry,
                &RemoteHeartbeatAck { uid: 7 },
                Some(remote_watcher()),
            ))
            .unwrap();

        let effects = sink.wait_for_len(3, Duration::from_secs(1));
        assert_eq!(
            effects[2],
            RemoteDeathWatchEffect::RewatchRemote(WatchRemote { watchee, watcher })
        );
    }

    #[test]
    fn system_inbound_deserializes_address_terminated_and_marks_unreachable() {
        let registry = registry();
        let sink = Arc::new(RecordingEffectSink::default());
        let (inbound, watcher_actor) = inbound(registry.clone(), sink.clone());
        let watchee = watchee("target");
        let watcher = watcher("observer");

        watcher_actor
            .tell(crate::RemoteDeathWatchCommand::Watch(WatchRemote {
                watchee,
                watcher,
            }))
            .unwrap();
        inbound
            .receive(envelope(
                &registry,
                &AddressTerminated {
                    address: "kairo://remote@127.0.0.1:25520".to_string(),
                    uid: Some(9),
                },
                Some(remote_watcher()),
            ))
            .unwrap();

        let effects = sink.wait_for_len(3, Duration::from_secs(1));
        assert!(matches!(
            effects.last(),
            Some(RemoteDeathWatchEffect::AddressTerminated(AddressTerminated {
                address,
                uid: Some(9),
            })) if address == "kairo://remote@127.0.0.1:25520"
        ));
    }

    #[test]
    fn system_inbound_deserializes_remote_terminated_and_clears_watch() {
        let registry = registry();
        let sink = Arc::new(RecordingEffectSink::default());
        let (inbound, watcher_actor) = inbound(registry.clone(), sink.clone());
        let watchee = watchee("target");
        let watcher = watcher("observer");

        watcher_actor
            .tell(crate::RemoteDeathWatchCommand::Watch(WatchRemote {
                watchee: watchee.clone(),
                watcher,
            }))
            .unwrap();
        inbound
            .receive(envelope(
                &registry,
                &RemoteTerminated {
                    watchee: watchee.clone(),
                    existence_confirmed: true,
                },
                Some(remote_watcher()),
            ))
            .unwrap();

        let effects = sink.wait_for_len(4, Duration::from_secs(1));
        assert_eq!(
            effects[2..],
            [
                RemoteDeathWatchEffect::RemoteTerminated(RemoteTerminated {
                    watchee,
                    existence_confirmed: true
                }),
                RemoteDeathWatchEffect::StopHeartbeat {
                    address: "kairo://remote@127.0.0.1:25520".to_string()
                }
            ]
        );
    }

    #[test]
    fn system_inbound_rejects_unknown_remote_watch_manifest() {
        let registry = registry();
        let sink = Arc::new(RecordingEffectSink::default());
        let (inbound, _watcher_actor) = inbound(registry, sink);
        let envelope = RemoteEnvelope::new(
            local_watcher(),
            Some(remote_watcher()),
            SerializedMessage::new(
                999,
                Manifest::new("kairo.remote.unknown-watch-message"),
                1,
                Bytes::new(),
            ),
        );

        let error = inbound
            .receive(envelope)
            .expect_err("unknown remote death-watch manifest should fail");

        assert!(matches!(error, RemoteError::Inbound(_)));
        assert!(error.to_string().contains("unsupported remote death-watch"));
    }

    #[test]
    fn system_inbound_reports_missing_registered_codec_for_known_manifest() {
        let mut encoding_registry = Registry::new();
        register_remote_protocol_codecs(&mut encoding_registry).unwrap();
        let sink = Arc::new(RecordingEffectSink::default());
        let (inbound, _watcher_actor) = inbound(Arc::new(Registry::new()), sink);

        let error = inbound
            .receive(envelope(
                &encoding_registry,
                &RemoteHeartbeat { from_uid: 7 },
                Some(remote_watcher()),
            ))
            .expect_err("missing registered codec should fail");

        assert!(matches!(error, RemoteError::Serialization(_)));
    }
}
