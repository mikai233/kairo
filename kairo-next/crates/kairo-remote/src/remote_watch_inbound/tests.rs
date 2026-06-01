use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::time::{Duration, Instant};

use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, ActorSystem, Context, Props};

use crate::{
    RemoteDeathWatchActor, RemoteDeathWatchEffect, RemoteDeathWatchEffectSink,
    RemoteDeathWatchStats,
};

use super::*;

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
            .map_err(|error| ActorError::Message(error.to_string()))
    }
}

fn spawn_remote_watcher() -> (
    ActorSystem,
    ActorRef<RemoteDeathWatchCommand>,
    Arc<RecordingEffectSink>,
) {
    let system = ActorSystem::builder("local").build().unwrap();
    let sink = Arc::new(RecordingEffectSink::default());
    let actor = system
        .spawn(
            "remote-watch",
            RemoteDeathWatchActor::props(sink.clone() as Arc<dyn RemoteDeathWatchEffectSink>),
        )
        .unwrap();
    (system, actor, sink)
}

fn stats_probe(
    system: &ActorSystem,
) -> (
    ActorRef<RemoteDeathWatchStats>,
    mpsc::Receiver<RemoteDeathWatchStats>,
) {
    let (stats_tx, stats_rx) = mpsc::channel();
    let stats_probe = system
        .spawn("stats", Props::new(move || Probe { sender: stats_tx }))
        .unwrap();
    (stats_probe, stats_rx)
}

fn local_remote_watcher() -> ActorRefWireData {
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

fn watch_message(name: &str) -> WatchRemote {
    WatchRemote {
        watchee: watchee(name),
        watcher: watcher("observer"),
    }
}

fn inbound_message<M>(message: M) -> InboundMessage<M> {
    InboundMessage {
        recipient: local_remote_watcher(),
        sender: Some(remote_watcher()),
        message,
    }
}

#[test]
fn inbound_watch_records_remote_watcher_without_outbound_watch_effect() {
    let (system, actor, sink) = spawn_remote_watcher();
    let delivery = RemoteDeathWatchProtocolDelivery::new(actor.clone(), 42);
    let (stats_probe, stats_rx) = stats_probe(&system);

    delivery
        .deliver(inbound_message(watch_message("target")))
        .unwrap();
    actor
        .tell(RemoteDeathWatchCommand::GetStats {
            reply_to: stats_probe,
        })
        .unwrap();

    let stats = stats_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(stats.watching, 0);
    assert_eq!(stats.watched_addresses, 0);
    assert_eq!(stats.inbound_watching, 1);
    assert!(sink.wait_for_len(1, Duration::from_millis(50)).is_empty());
}

#[test]
fn inbound_unwatch_removes_remote_watcher_without_outbound_effect() {
    let (system, actor, sink) = spawn_remote_watcher();
    let delivery = RemoteDeathWatchProtocolDelivery::new(actor.clone(), 42);
    let (stats_probe, stats_rx) = stats_probe(&system);
    let watch = watch_message("target");

    delivery.deliver(inbound_message(watch.clone())).unwrap();
    delivery
        .deliver(inbound_message(UnwatchRemote {
            watchee: watch.watchee,
            watcher: watch.watcher,
        }))
        .unwrap();
    actor
        .tell(RemoteDeathWatchCommand::GetStats {
            reply_to: stats_probe,
        })
        .unwrap();

    let stats = stats_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(stats.watching, 0);
    assert_eq!(stats.watched_addresses, 0);
    assert_eq!(stats.inbound_watching, 0);
    assert!(sink.wait_for_len(1, Duration::from_millis(50)).is_empty());
}

#[test]
fn inbound_heartbeat_replies_with_local_uid_ack_effect() {
    let (_system, actor, sink) = spawn_remote_watcher();
    let delivery = RemoteDeathWatchProtocolDelivery::new(actor, 42);

    delivery
        .deliver(inbound_message(RemoteHeartbeat { from_uid: 7 }))
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
fn inbound_heartbeat_ack_drives_rewatch_for_new_remote_uid() {
    let (_system, actor, sink) = spawn_remote_watcher();
    let delivery = RemoteDeathWatchProtocolDelivery::new(actor.clone(), 42);
    let watchee = watchee("target");
    let watcher = watcher("observer");
    actor
        .tell(RemoteDeathWatchCommand::Watch(WatchRemote {
            watchee: watchee.clone(),
            watcher: watcher.clone(),
        }))
        .unwrap();

    delivery
        .deliver(inbound_message(RemoteHeartbeatAck { uid: 7 }))
        .unwrap();

    let effects = sink.wait_for_len(3, Duration::from_secs(1));
    assert_eq!(
        effects[2],
        RemoteDeathWatchEffect::RewatchRemote(WatchRemote { watchee, watcher })
    );
}

#[test]
fn inbound_heartbeat_requires_sender_for_remote_address() {
    let system = ActorSystem::builder("local").build().unwrap();
    let sink = Arc::new(RecordingEffectSink::default());
    let actor = system
        .spawn(
            "remote-watch",
            Props::new(move || {
                RemoteDeathWatchActor::new(sink as Arc<dyn RemoteDeathWatchEffectSink>)
            }),
        )
        .unwrap();
    let delivery = RemoteDeathWatchProtocolDelivery::new(actor, 42);

    let error =
        <RemoteDeathWatchProtocolDelivery as RemoteInboundDelivery<RemoteHeartbeat>>::deliver(
            &delivery,
            InboundMessage {
                recipient: local_remote_watcher(),
                sender: None,
                message: RemoteHeartbeat { from_uid: 7 },
            },
        )
        .expect_err("heartbeat without sender should fail");

    assert!(matches!(error, RemoteError::Inbound(_)));
    assert!(error.to_string().contains("missing sender"));
}
