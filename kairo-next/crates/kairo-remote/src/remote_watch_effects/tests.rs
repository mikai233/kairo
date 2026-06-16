use std::sync::{Arc, Mutex};

use kairo_serialization::RemoteMessage;

use crate::{
    AddressTerminated, RemoteError, RemoteHeartbeatAck, RemoteTerminated,
    register_remote_protocol_codecs,
};

use super::*;

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

fn local_watchee(name: &str) -> ActorRefWireData {
    ActorRefWireData::new(format!("kairo://local@127.0.0.1:25521/user/{name}")).unwrap()
}

fn remote_watcher(name: &str) -> ActorRefWireData {
    ActorRefWireData::new(format!("kairo://remote@127.0.0.1:25520/user/{name}")).unwrap()
}

fn local_watcher() -> ActorRefWireData {
    ActorRefWireData::new("kairo://local@127.0.0.1:25521/system/remote-watch").unwrap()
}

fn assert_remote_watcher_envelope(envelope: &RemoteEnvelope) {
    assert_eq!(
        envelope.recipient.path(),
        "kairo://remote@127.0.0.1:25520/system/remote-watch"
    );
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
    let registry = registry();
    let sink = RemoteDeathWatchOutboundSink::with_observer(
        registry.clone(),
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
        assert_remote_watcher_envelope(envelope);
        assert!(envelope.sender.is_none());
    }
    let decoded_watch: WatchRemote = registry.deserialize(envelopes[0].message.clone()).unwrap();
    let decoded_heartbeat: RemoteHeartbeat =
        registry.deserialize(envelopes[1].message.clone()).unwrap();
    let decoded_unwatch: UnwatchRemote =
        registry.deserialize(envelopes[2].message.clone()).unwrap();

    assert_eq!(decoded_watch.watchee, watchee);
    assert_eq!(decoded_watch.watcher, watcher);
    assert_eq!(decoded_heartbeat.from_uid, 99);
    assert_eq!(decoded_unwatch.watchee, decoded_watch.watchee);
    assert_eq!(decoded_unwatch.watcher, decoded_watch.watcher);
}

#[test]
fn outbound_sink_treats_rewatch_as_another_watch_message() {
    let outbound = Arc::new(CollectingOutbound::default());
    let registry = registry();
    let sink = RemoteDeathWatchOutboundSink::new(
        registry.clone(),
        outbound.clone() as Arc<dyn RemoteOutbound>,
    );
    let watchee = watchee("target");
    let watcher = watcher("observer");

    sink.apply(vec![RemoteDeathWatchEffect::RewatchRemote(WatchRemote {
        watchee: watchee.clone(),
        watcher: watcher.clone(),
    })])
    .unwrap();

    let envelopes = outbound.envelopes();
    assert_eq!(envelopes.len(), 1);
    assert_remote_watcher_envelope(&envelopes[0]);
    let decoded: WatchRemote = registry.deserialize(envelopes[0].message.clone()).unwrap();
    assert_eq!(decoded.watchee, watchee);
    assert_eq!(decoded.watcher, watcher);
}

#[test]
fn outbound_sink_serializes_remote_terminated_to_remote_watcher() {
    let outbound = Arc::new(CollectingOutbound::default());
    let registry = registry();
    let sink = RemoteDeathWatchOutboundSink::with_local_watcher(
        registry.clone(),
        outbound.clone() as Arc<dyn RemoteOutbound>,
        Arc::new(IgnoreRemoteDeathWatchEffects) as Arc<dyn RemoteDeathWatchEffectObserver>,
        local_watcher(),
    );
    let watchee = local_watchee("target");
    let watcher = remote_watcher("observer");

    sink.apply(vec![RemoteDeathWatchEffect::SendRemoteTerminated {
        watcher,
        message: RemoteTerminated {
            watchee: watchee.clone(),
            existence_confirmed: true,
        },
    }])
    .unwrap();

    let envelopes = outbound.envelopes();
    assert_eq!(envelopes.len(), 1);
    assert_remote_watcher_envelope(&envelopes[0]);
    assert_eq!(
        envelopes[0].sender.as_ref().map(ActorRefWireData::path),
        Some("kairo://local@127.0.0.1:25521/system/remote-watch")
    );
    let decoded: RemoteTerminated = registry.deserialize(envelopes[0].message.clone()).unwrap();
    assert_eq!(decoded.watchee, watchee);
    assert!(decoded.existence_confirmed);
}

#[test]
fn outbound_sink_serializes_heartbeat_ack_with_local_watcher_sender() {
    let outbound = Arc::new(CollectingOutbound::default());
    let registry = registry();
    let sink = RemoteDeathWatchOutboundSink::with_local_watcher(
        registry.clone(),
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
    assert_remote_watcher_envelope(&envelopes[0]);
    assert_eq!(
        envelopes[0].sender.as_ref().map(ActorRefWireData::path),
        Some("kairo://local@127.0.0.1:25521/system/remote-watch")
    );
    let decoded: RemoteHeartbeatAck = registry.deserialize(envelopes[0].message.clone()).unwrap();
    assert_eq!(decoded.uid, 42);
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
    let sink =
        RemoteDeathWatchOutboundSink::new(registry(), outbound.clone() as Arc<dyn RemoteOutbound>);

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
