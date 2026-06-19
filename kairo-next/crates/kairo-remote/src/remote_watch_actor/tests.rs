use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::time::{Duration, Instant};

use kairo_actor::{Actor, ActorError, ActorResult, ActorSystem, Context, Props};
use kairo_serialization::ActorRefWireData;

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
    fn apply(&self, effects: Vec<RemoteDeathWatchEffect>) -> crate::Result<()> {
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

fn spawn_remote_watcher(
    system_name: &str,
) -> (
    ActorSystem,
    ActorRef<RemoteDeathWatchCommand>,
    Arc<RecordingEffectSink>,
) {
    let system = ActorSystem::builder(system_name).build().unwrap();
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

#[test]
fn remote_watch_actor_emits_watch_heartbeat_and_unwatch_effects() {
    let (_system, actor, sink) = spawn_remote_watcher("watcher");
    let watchee = watchee("target");
    let watcher = watcher("observer");

    actor
        .tell(RemoteDeathWatchCommand::Watch(WatchRemote {
            watchee: watchee.clone(),
            watcher: watcher.clone(),
        }))
        .unwrap();
    actor
        .tell(RemoteDeathWatchCommand::HeartbeatTick { local_uid: 42 })
        .unwrap();
    actor
        .tell(RemoteDeathWatchCommand::Unwatch(UnwatchRemote {
            watchee: watchee.clone(),
            watcher: watcher.clone(),
        }))
        .unwrap();

    let effects = sink.wait_for_len(5, Duration::from_secs(1));
    assert_eq!(
        effects,
        vec![
            RemoteDeathWatchEffect::StartHeartbeat {
                address: "kairo://remote@127.0.0.1:25520".to_string()
            },
            RemoteDeathWatchEffect::SendWatchRemote(WatchRemote {
                watchee: watchee.clone(),
                watcher: watcher.clone()
            }),
            RemoteDeathWatchEffect::SendHeartbeat {
                address: "kairo://remote@127.0.0.1:25520".to_string(),
                message: crate::RemoteHeartbeat { from_uid: 42 },
            },
            RemoteDeathWatchEffect::SendUnwatchRemote(UnwatchRemote { watchee, watcher }),
            RemoteDeathWatchEffect::StopHeartbeat {
                address: "kairo://remote@127.0.0.1:25520".to_string()
            },
        ]
    );
}

#[test]
fn remote_watch_actor_rewatches_after_uid_changes() {
    let (_system, actor, sink) = spawn_remote_watcher("watcher");
    let watchee = watchee("target");
    let watcher = watcher("observer");

    actor
        .tell(RemoteDeathWatchCommand::Watch(WatchRemote {
            watchee: watchee.clone(),
            watcher: watcher.clone(),
        }))
        .unwrap();
    actor
        .tell(RemoteDeathWatchCommand::HeartbeatAck {
            address: "kairo://remote@127.0.0.1:25520".to_string(),
            ack: RemoteHeartbeatAck { uid: 7 },
        })
        .unwrap();
    actor
        .tell(RemoteDeathWatchCommand::HeartbeatAck {
            address: "kairo://remote@127.0.0.1:25520".to_string(),
            ack: RemoteHeartbeatAck { uid: 8 },
        })
        .unwrap();

    let effects = sink.wait_for_len(4, Duration::from_secs(1));
    assert_eq!(
        effects[2..],
        [
            RemoteDeathWatchEffect::RewatchRemote(WatchRemote {
                watchee: watchee.clone(),
                watcher: watcher.clone()
            }),
            RemoteDeathWatchEffect::RewatchRemote(WatchRemote { watchee, watcher })
        ]
    );
}

#[test]
fn remote_watch_actor_remote_terminated_keeps_other_watch_on_same_address() {
    let (system, actor, sink) = spawn_remote_watcher("watcher");
    let (stats_probe, stats_rx) = stats_probe(&system);
    let first = watchee("first");
    let second = watchee("second");
    let watcher = watcher("observer");

    actor
        .tell(RemoteDeathWatchCommand::Watch(WatchRemote {
            watchee: first.clone(),
            watcher: watcher.clone(),
        }))
        .unwrap();
    actor
        .tell(RemoteDeathWatchCommand::Watch(WatchRemote {
            watchee: second.clone(),
            watcher: watcher.clone(),
        }))
        .unwrap();
    actor
        .tell(RemoteDeathWatchCommand::HeartbeatAck {
            address: "kairo://remote@127.0.0.1:25520".to_string(),
            ack: RemoteHeartbeatAck { uid: 7 },
        })
        .unwrap();
    actor
        .tell(RemoteDeathWatchCommand::RemoteTerminated(
            RemoteTerminated {
                watchee: first.clone(),
                existence_confirmed: true,
            },
        ))
        .unwrap();
    actor
        .tell(RemoteDeathWatchCommand::HeartbeatTick { local_uid: 42 })
        .unwrap();
    actor
        .tell(RemoteDeathWatchCommand::GetStats {
            reply_to: stats_probe,
        })
        .unwrap();

    let effects = sink.wait_for_len(7, Duration::from_secs(1));
    assert_eq!(
        effects[5..],
        [
            RemoteDeathWatchEffect::RemoteTerminated(RemoteTerminated {
                watchee: first,
                existence_confirmed: true,
            }),
            RemoteDeathWatchEffect::SendHeartbeat {
                address: "kairo://remote@127.0.0.1:25520".to_string(),
                message: RemoteHeartbeat { from_uid: 42 },
            },
        ]
    );
    assert_eq!(
        stats_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        RemoteDeathWatchStats {
            watching: 1,
            watched_addresses: 1,
            inbound_watching: 0,
            unreachable_addresses: 0,
            watching_refs: vec![WatchRemote {
                watchee: second,
                watcher,
            }],
            watching_addresses: vec!["kairo://remote@127.0.0.1:25520".to_string()],
        }
    );
}

#[test]
fn remote_watch_actor_reports_stats_after_ordered_commands() {
    let (system, actor, _sink) = spawn_remote_watcher("watcher");
    let (stats_probe, stats_rx) = stats_probe(&system);
    let watchee = watchee("target");
    let watcher = watcher("observer");

    actor
        .tell(RemoteDeathWatchCommand::Watch(WatchRemote {
            watchee: watchee.clone(),
            watcher: watcher.clone(),
        }))
        .unwrap();
    actor
        .tell(RemoteDeathWatchCommand::AddressUnreachable {
            address: "kairo://remote@127.0.0.1:25520".to_string(),
            uid: None,
        })
        .unwrap();
    actor
        .tell(RemoteDeathWatchCommand::GetStats {
            reply_to: stats_probe,
        })
        .unwrap();

    assert_eq!(
        stats_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        RemoteDeathWatchStats {
            watching: 0,
            watched_addresses: 0,
            inbound_watching: 0,
            unreachable_addresses: 1,
            watching_refs: vec![],
            watching_addresses: vec![],
        }
    );
}

#[test]
fn remote_watch_actor_records_inbound_watch_without_outbound_effects() {
    let (system, actor, sink) = spawn_remote_watcher("watcher");
    let (stats_probe, stats_rx) = stats_probe(&system);

    actor
        .tell(RemoteDeathWatchCommand::InboundWatch(WatchRemote {
            watchee: watchee("target"),
            watcher: watcher("observer"),
        }))
        .unwrap();
    actor
        .tell(RemoteDeathWatchCommand::GetStats {
            reply_to: stats_probe,
        })
        .unwrap();

    assert_eq!(
        stats_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        RemoteDeathWatchStats {
            watching: 0,
            watched_addresses: 0,
            inbound_watching: 1,
            unreachable_addresses: 0,
            watching_refs: Vec::new(),
            watching_addresses: Vec::new(),
        }
    );
    assert!(sink.wait_for_len(1, Duration::from_millis(50)).is_empty());
}

#[test]
fn remote_watch_actor_removes_inbound_watch_without_outbound_effects() {
    let (system, actor, sink) = spawn_remote_watcher("watcher");
    let (stats_probe, stats_rx) = stats_probe(&system);
    let watchee = watchee("target");
    let watcher = watcher("observer");

    actor
        .tell(RemoteDeathWatchCommand::InboundWatch(WatchRemote {
            watchee: watchee.clone(),
            watcher: watcher.clone(),
        }))
        .unwrap();
    actor
        .tell(RemoteDeathWatchCommand::InboundUnwatch(UnwatchRemote {
            watchee,
            watcher,
        }))
        .unwrap();
    actor
        .tell(RemoteDeathWatchCommand::GetStats {
            reply_to: stats_probe,
        })
        .unwrap();

    assert_eq!(
        stats_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        RemoteDeathWatchStats {
            watching: 0,
            watched_addresses: 0,
            inbound_watching: 0,
            unreachable_addresses: 0,
            watching_refs: Vec::new(),
            watching_addresses: Vec::new(),
        }
    );
    assert!(sink.wait_for_len(1, Duration::from_millis(50)).is_empty());
}

#[test]
fn remote_watch_actor_notifies_remote_watchers_when_local_watchee_terminates() {
    let (system, actor, sink) = spawn_remote_watcher("watcher");
    let (stats_probe, stats_rx) = stats_probe(&system);
    let watchee = local_watchee("target");
    let watcher = remote_watcher("observer");

    actor
        .tell(RemoteDeathWatchCommand::InboundWatch(WatchRemote {
            watchee: watchee.clone(),
            watcher: watcher.clone(),
        }))
        .unwrap();
    actor
        .tell(RemoteDeathWatchCommand::LocalWatcheeTerminated {
            watchee: watchee.clone(),
            existence_confirmed: true,
        })
        .unwrap();
    actor
        .tell(RemoteDeathWatchCommand::GetStats {
            reply_to: stats_probe,
        })
        .unwrap();

    assert_eq!(
        sink.wait_for_len(1, Duration::from_secs(1)),
        vec![RemoteDeathWatchEffect::SendRemoteTerminated {
            watcher,
            message: RemoteTerminated {
                watchee,
                existence_confirmed: true
            }
        }]
    );
    assert_eq!(
        stats_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        RemoteDeathWatchStats {
            watching: 0,
            watched_addresses: 0,
            inbound_watching: 0,
            unreachable_addresses: 0,
            watching_refs: Vec::new(),
            watching_addresses: Vec::new(),
        }
    );
}
