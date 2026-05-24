use std::sync::Arc;

use kairo_serialization::{
    ActorRefWireData, Registry, RemoteEnvelope, RemoteMessage, SerializationError,
};

use crate::{
    RemoteDeathWatchEffect, RemoteDeathWatchEffectSink, RemoteError, RemoteHeartbeat,
    RemoteHeartbeatAck, RemoteOutbound, Result, UnwatchRemote, WatchRemote,
};

const REMOTE_WATCHER_PATH: &str = "/system/remote-watch";

pub trait RemoteDeathWatchEffectObserver: Send + Sync + 'static {
    fn observe(&self, effect: &RemoteDeathWatchEffect) -> Result<()>;
}

#[derive(Debug, Default)]
pub struct IgnoreRemoteDeathWatchEffects;

impl RemoteDeathWatchEffectObserver for IgnoreRemoteDeathWatchEffects {
    fn observe(&self, _effect: &RemoteDeathWatchEffect) -> Result<()> {
        Ok(())
    }
}

pub struct RemoteDeathWatchOutboundSink {
    registry: Arc<Registry>,
    outbound: Arc<dyn RemoteOutbound>,
    observer: Arc<dyn RemoteDeathWatchEffectObserver>,
    local_watcher: Option<ActorRefWireData>,
}

impl RemoteDeathWatchOutboundSink {
    pub fn new(registry: Arc<Registry>, outbound: Arc<dyn RemoteOutbound>) -> Self {
        Self::with_observer(
            registry,
            outbound,
            Arc::new(IgnoreRemoteDeathWatchEffects) as Arc<dyn RemoteDeathWatchEffectObserver>,
        )
    }

    pub fn with_observer(
        registry: Arc<Registry>,
        outbound: Arc<dyn RemoteOutbound>,
        observer: Arc<dyn RemoteDeathWatchEffectObserver>,
    ) -> Self {
        Self {
            registry,
            outbound,
            observer,
            local_watcher: None,
        }
    }

    pub fn with_local_watcher(
        registry: Arc<Registry>,
        outbound: Arc<dyn RemoteOutbound>,
        observer: Arc<dyn RemoteDeathWatchEffectObserver>,
        local_watcher: ActorRefWireData,
    ) -> Self {
        Self {
            registry,
            outbound,
            observer,
            local_watcher: Some(local_watcher),
        }
    }

    pub fn registry(&self) -> &Arc<Registry> {
        &self.registry
    }

    pub fn outbound(&self) -> &Arc<dyn RemoteOutbound> {
        &self.outbound
    }

    pub fn observer(&self) -> &Arc<dyn RemoteDeathWatchEffectObserver> {
        &self.observer
    }

    pub fn local_watcher(&self) -> Option<&ActorRefWireData> {
        self.local_watcher.as_ref()
    }

    fn send_watch(&self, message: WatchRemote) -> Result<()> {
        let recipient = watcher_recipient_for_actor(&message.watchee)?;
        self.send_remote(recipient, &message)
    }

    fn send_unwatch(&self, message: UnwatchRemote) -> Result<()> {
        let recipient = watcher_recipient_for_actor(&message.watchee)?;
        self.send_remote(recipient, &message)
    }

    fn send_heartbeat(&self, address: &str, message: &RemoteHeartbeat) -> Result<()> {
        let recipient = watcher_recipient_for_address(address)?;
        self.send_remote(recipient, message)
    }

    fn send_heartbeat_ack(&self, address: &str, message: &RemoteHeartbeatAck) -> Result<()> {
        let recipient = watcher_recipient_for_address(address)?;
        self.send_remote(recipient, message)
    }

    fn send_remote<M>(&self, recipient: ActorRefWireData, message: &M) -> Result<()>
    where
        M: RemoteMessage,
    {
        let serialized = self.registry.serialize(message)?;
        self.outbound.send(RemoteEnvelope::new(
            recipient,
            self.local_watcher.clone(),
            serialized,
        ))
    }
}

impl RemoteDeathWatchEffectSink for RemoteDeathWatchOutboundSink {
    fn apply(&self, effects: Vec<RemoteDeathWatchEffect>) -> Result<()> {
        for effect in effects {
            self.observer.observe(&effect)?;
            match effect {
                RemoteDeathWatchEffect::SendWatchRemote(message)
                | RemoteDeathWatchEffect::RewatchRemote(message) => self.send_watch(message)?,
                RemoteDeathWatchEffect::SendUnwatchRemote(message) => self.send_unwatch(message)?,
                RemoteDeathWatchEffect::SendHeartbeat { address, message } => {
                    self.send_heartbeat(&address, &message)?
                }
                RemoteDeathWatchEffect::SendHeartbeatAck { address, message } => {
                    self.send_heartbeat_ack(&address, &message)?
                }
                RemoteDeathWatchEffect::StartHeartbeat { .. }
                | RemoteDeathWatchEffect::StopHeartbeat { .. }
                | RemoteDeathWatchEffect::ResetFailureDetector { .. }
                | RemoteDeathWatchEffect::AddressTerminated(_) => {}
            }
        }
        Ok(())
    }
}

pub fn watcher_recipient_for_actor(actor: &ActorRefWireData) -> Result<ActorRefWireData> {
    watcher_recipient_for_address(&wire_address(actor))
}

pub fn watcher_recipient_for_address(address: &str) -> Result<ActorRefWireData> {
    ActorRefWireData::new(format!("{address}{REMOTE_WATCHER_PATH}")).map_err(|error| {
        RemoteError::InvalidRemoteRef(
            format!("{address}{REMOTE_WATCHER_PATH}"),
            invalid_ref_reason(error),
        )
    })
}

fn wire_address(wire: &ActorRefWireData) -> String {
    let mut address = format!("{}://{}", wire.protocol(), wire.system());
    if let Some(host) = wire.host() {
        address.push('@');
        address.push_str(host);
        if let Some(port) = wire.port() {
            address.push(':');
            address.push_str(&port.to_string());
        }
    }
    address
}

fn invalid_ref_reason(error: SerializationError) -> String {
    error.to_string()
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use kairo_serialization::RemoteMessage;

    use crate::{
        AddressTerminated, RemoteError, RemoteHeartbeatAck, register_remote_protocol_codecs,
    };

    #[derive(Default)]
    struct CollectingOutbound {
        envelopes: Mutex<Vec<RemoteEnvelope>>,
        fail_with: Mutex<Option<String>>,
    }

    impl CollectingOutbound {
        fn envelopes(&self) -> Vec<RemoteEnvelope> {
            self.envelopes.lock().expect("outbound poisoned").clone()
        }

        fn fail_with(&self, reason: impl Into<String>) {
            *self.fail_with.lock().expect("outbound poisoned") = Some(reason.into());
        }
    }

    impl RemoteOutbound for CollectingOutbound {
        fn send(&self, envelope: RemoteEnvelope) -> Result<()> {
            if let Some(reason) = self.fail_with.lock().expect("outbound poisoned").clone() {
                return Err(RemoteError::Outbound(reason));
            }
            self.envelopes
                .lock()
                .expect("outbound poisoned")
                .push(envelope);
            Ok(())
        }
    }

    #[derive(Default)]
    struct CollectingObserver {
        effects: Mutex<Vec<RemoteDeathWatchEffect>>,
    }

    impl CollectingObserver {
        fn effects(&self) -> Vec<RemoteDeathWatchEffect> {
            self.effects.lock().expect("observer poisoned").clone()
        }
    }

    impl RemoteDeathWatchEffectObserver for CollectingObserver {
        fn observe(&self, effect: &RemoteDeathWatchEffect) -> Result<()> {
            self.effects
                .lock()
                .expect("observer poisoned")
                .push(effect.clone());
            Ok(())
        }
    }

    fn registry() -> Arc<Registry> {
        let mut registry = Registry::new();
        register_remote_protocol_codecs(&mut registry).unwrap();
        Arc::new(registry)
    }

    fn watchee(name: &str) -> ActorRefWireData {
        ActorRefWireData::new(format!("kairo://remote@127.0.0.1:25520/user/{name}")).unwrap()
    }

    fn watcher(name: &str) -> ActorRefWireData {
        ActorRefWireData::new(format!("kairo://local@127.0.0.1:25521/user/{name}")).unwrap()
    }

    fn local_watcher() -> ActorRefWireData {
        ActorRefWireData::new("kairo://local@127.0.0.1:25521/system/remote-watch").unwrap()
    }

    #[test]
    fn watcher_recipient_uses_stable_system_actor_path() {
        let recipient = watcher_recipient_for_address("kairo://remote@127.0.0.1:25520").unwrap();

        assert_eq!(
            recipient.path(),
            "kairo://remote@127.0.0.1:25520/system/remote-watch"
        );
    }

    #[test]
    fn outbound_sink_serializes_remote_watch_effects_to_remote_watcher() {
        let outbound = Arc::new(CollectingOutbound::default());
        let observer = Arc::new(CollectingObserver::default());
        let sink = RemoteDeathWatchOutboundSink::with_observer(
            registry(),
            outbound.clone() as Arc<dyn RemoteOutbound>,
            observer.clone() as Arc<dyn RemoteDeathWatchEffectObserver>,
        );
        let watchee = watchee("target");
        let watcher = watcher("observer");

        sink.apply(vec![
            RemoteDeathWatchEffect::StartHeartbeat {
                address: "kairo://remote@127.0.0.1:25520".to_string(),
            },
            RemoteDeathWatchEffect::SendWatchRemote(WatchRemote {
                watchee: watchee.clone(),
                watcher: watcher.clone(),
            }),
            RemoteDeathWatchEffect::SendHeartbeat {
                address: "kairo://remote@127.0.0.1:25520".to_string(),
                message: RemoteHeartbeat { from_uid: 99 },
            },
            RemoteDeathWatchEffect::SendUnwatchRemote(UnwatchRemote {
                watchee: watchee.clone(),
                watcher: watcher.clone(),
            }),
        ])
        .unwrap();

        let envelopes = outbound.envelopes();
        assert_eq!(envelopes.len(), 3);
        assert!(
            observer
                .effects()
                .iter()
                .any(|effect| matches!(effect, RemoteDeathWatchEffect::StartHeartbeat { .. }))
        );
        for envelope in &envelopes {
            assert_eq!(
                envelope.recipient.path(),
                "kairo://remote@127.0.0.1:25520/system/remote-watch"
            );
            assert!(envelope.sender.is_none());
        }
        assert_eq!(
            envelopes[0].message.manifest.as_str(),
            WatchRemote::MANIFEST
        );
        assert_eq!(
            envelopes[1].message.manifest.as_str(),
            RemoteHeartbeat::MANIFEST
        );
        assert_eq!(
            envelopes[2].message.manifest.as_str(),
            UnwatchRemote::MANIFEST
        );
    }

    #[test]
    fn outbound_sink_treats_rewatch_as_another_watch_message() {
        let outbound = Arc::new(CollectingOutbound::default());
        let sink = RemoteDeathWatchOutboundSink::new(
            registry(),
            outbound.clone() as Arc<dyn RemoteOutbound>,
        );

        sink.apply(vec![RemoteDeathWatchEffect::RewatchRemote(WatchRemote {
            watchee: watchee("target"),
            watcher: watcher("observer"),
        })])
        .unwrap();

        let envelopes = outbound.envelopes();
        assert_eq!(envelopes.len(), 1);
        assert_eq!(
            envelopes[0].message.manifest.as_str(),
            WatchRemote::MANIFEST
        );
    }

    #[test]
    fn outbound_sink_serializes_heartbeat_ack_with_local_watcher_sender() {
        let outbound = Arc::new(CollectingOutbound::default());
        let sink = RemoteDeathWatchOutboundSink::with_local_watcher(
            registry(),
            outbound.clone() as Arc<dyn RemoteOutbound>,
            Arc::new(IgnoreRemoteDeathWatchEffects) as Arc<dyn RemoteDeathWatchEffectObserver>,
            local_watcher(),
        );

        sink.apply(vec![RemoteDeathWatchEffect::SendHeartbeatAck {
            address: "kairo://remote@127.0.0.1:25520".to_string(),
            message: RemoteHeartbeatAck { uid: 42 },
        }])
        .unwrap();

        let envelopes = outbound.envelopes();
        assert_eq!(envelopes.len(), 1);
        assert_eq!(
            envelopes[0].recipient.path(),
            "kairo://remote@127.0.0.1:25520/system/remote-watch"
        );
        assert_eq!(
            envelopes[0].sender.as_ref().map(ActorRefWireData::path),
            Some("kairo://local@127.0.0.1:25521/system/remote-watch")
        );
        assert_eq!(
            envelopes[0].message.manifest.as_str(),
            RemoteHeartbeatAck::MANIFEST
        );
    }

    #[test]
    fn outbound_sink_reports_missing_system_codec() {
        let outbound = Arc::new(CollectingOutbound::default());
        let sink = RemoteDeathWatchOutboundSink::new(
            Arc::new(Registry::new()),
            outbound.clone() as Arc<dyn RemoteOutbound>,
        );

        let error = sink
            .apply(vec![RemoteDeathWatchEffect::SendHeartbeat {
                address: "kairo://remote@127.0.0.1:25520".to_string(),
                message: RemoteHeartbeat { from_uid: 99 },
            }])
            .expect_err("unregistered heartbeat codec should fail");

        assert!(matches!(error, RemoteError::Serialization(_)));
        assert!(outbound.envelopes().is_empty());
    }

    #[test]
    fn outbound_sink_propagates_outbound_failure() {
        let outbound = Arc::new(CollectingOutbound::default());
        outbound.fail_with("association closed");
        let sink = RemoteDeathWatchOutboundSink::new(
            registry(),
            outbound.clone() as Arc<dyn RemoteOutbound>,
        );

        let error = sink
            .apply(vec![RemoteDeathWatchEffect::SendHeartbeat {
                address: "kairo://remote@127.0.0.1:25520".to_string(),
                message: RemoteHeartbeat { from_uid: 99 },
            }])
            .expect_err("outbound failure should propagate");

        assert!(matches!(error, RemoteError::Outbound(_)));
        assert!(error.to_string().contains("association closed"));
    }

    #[test]
    fn outbound_sink_observes_address_terminated_without_remote_send() {
        let outbound = Arc::new(CollectingOutbound::default());
        let observer = Arc::new(CollectingObserver::default());
        let sink = RemoteDeathWatchOutboundSink::with_observer(
            registry(),
            outbound.clone() as Arc<dyn RemoteOutbound>,
            observer.clone() as Arc<dyn RemoteDeathWatchEffectObserver>,
        );

        sink.apply(vec![RemoteDeathWatchEffect::AddressTerminated(
            AddressTerminated {
                address: "kairo://remote@127.0.0.1:25520".to_string(),
                uid: Some(7),
            },
        )])
        .unwrap();

        assert!(outbound.envelopes().is_empty());
        assert_eq!(observer.effects().len(), 1);
        assert!(
            observer
                .effects()
                .iter()
                .all(|effect| matches!(effect, RemoteDeathWatchEffect::AddressTerminated(_)))
        );
    }

    #[test]
    fn remote_heartbeat_ack_manifest_stays_registered_for_actor_inputs() {
        let registry = registry();
        let encoded = registry
            .serialize(&RemoteHeartbeatAck { uid: 17 })
            .expect("heartbeat ack codec should be registered");

        assert_eq!(encoded.manifest.as_str(), RemoteHeartbeatAck::MANIFEST);
    }
}
