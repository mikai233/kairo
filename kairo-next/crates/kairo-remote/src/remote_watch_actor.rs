use std::sync::Arc;

use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, Context, Props};

use crate::{
    RemoteDeathWatchEffect, RemoteDeathWatchState, RemoteError, RemoteHeartbeatAck, UnwatchRemote,
    WatchRemote,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteDeathWatchStats {
    pub watching: usize,
    pub watched_addresses: usize,
    pub unreachable_addresses: usize,
}

#[derive(Debug, Clone)]
pub enum RemoteDeathWatchCommand {
    Watch(WatchRemote),
    Unwatch(UnwatchRemote),
    HeartbeatTick {
        local_uid: u64,
    },
    HeartbeatAck {
        address: String,
        ack: RemoteHeartbeatAck,
    },
    AddressUnreachable {
        address: String,
    },
    GetStats {
        reply_to: ActorRef<RemoteDeathWatchStats>,
    },
}

pub trait RemoteDeathWatchEffectSink: Send + Sync + 'static {
    fn apply(&self, effects: Vec<RemoteDeathWatchEffect>) -> crate::Result<()>;
}

pub struct RemoteDeathWatchActor {
    state: RemoteDeathWatchState,
    effect_sink: Arc<dyn RemoteDeathWatchEffectSink>,
}

impl RemoteDeathWatchActor {
    pub fn new(effect_sink: Arc<dyn RemoteDeathWatchEffectSink>) -> Self {
        Self {
            state: RemoteDeathWatchState::new(),
            effect_sink,
        }
    }

    pub fn with_state(
        state: RemoteDeathWatchState,
        effect_sink: Arc<dyn RemoteDeathWatchEffectSink>,
    ) -> Self {
        Self { state, effect_sink }
    }

    pub fn props(effect_sink: Arc<dyn RemoteDeathWatchEffectSink>) -> Props<Self> {
        Props::new(move || Self::new(effect_sink))
    }

    fn stats(&self) -> RemoteDeathWatchStats {
        RemoteDeathWatchStats {
            watching: self.state.watching_count(),
            watched_addresses: self.state.watched_address_count(),
            unreachable_addresses: self.state.unreachable_address_count(),
        }
    }

    fn apply_effects(&self, effects: Vec<RemoteDeathWatchEffect>) -> ActorResult {
        if effects.is_empty() {
            return Ok(());
        }

        self.effect_sink
            .apply(effects)
            .map_err(remote_error_to_actor_error)
    }
}

impl Actor for RemoteDeathWatchActor {
    type Msg = RemoteDeathWatchCommand;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            RemoteDeathWatchCommand::Watch(message) => {
                let effects = self.state.watch(message.watchee, message.watcher);
                self.apply_effects(effects)
            }
            RemoteDeathWatchCommand::Unwatch(message) => {
                let effects = self.state.unwatch(&message.watchee, &message.watcher);
                self.apply_effects(effects)
            }
            RemoteDeathWatchCommand::HeartbeatTick { local_uid } => {
                self.apply_effects(self.state.heartbeat_due(local_uid))
            }
            RemoteDeathWatchCommand::HeartbeatAck { address, ack } => {
                let effects = self.state.heartbeat_ack(address, ack.uid);
                self.apply_effects(effects)
            }
            RemoteDeathWatchCommand::AddressUnreachable { address } => {
                let effects = self.state.mark_unreachable(address);
                self.apply_effects(effects)
            }
            RemoteDeathWatchCommand::GetStats { reply_to } => reply_to
                .tell(self.stats())
                .map_err(|error| ActorError::Message(error.to_string())),
        }
    }
}

fn remote_error_to_actor_error(error: RemoteError) -> ActorError {
    ActorError::Message(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Condvar, Mutex, mpsc};
    use std::time::{Duration, Instant};

    use kairo_actor::{ActorSystem, Props};
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

    fn watchee(name: &str) -> ActorRefWireData {
        ActorRefWireData::new(format!("kairo://remote@127.0.0.1:25520/user/{name}")).unwrap()
    }

    fn watcher(name: &str) -> ActorRefWireData {
        ActorRefWireData::new(format!("kairo://local@127.0.0.1:25521/user/{name}")).unwrap()
    }

    #[test]
    fn remote_watch_actor_emits_watch_heartbeat_and_unwatch_effects() {
        let system = ActorSystem::builder("watcher").build().unwrap();
        let sink = Arc::new(RecordingEffectSink::default());
        let actor = system
            .spawn(
                "remote-watch",
                RemoteDeathWatchActor::props(sink.clone() as Arc<dyn RemoteDeathWatchEffectSink>),
            )
            .unwrap();
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
        let system = ActorSystem::builder("watcher").build().unwrap();
        let sink = Arc::new(RecordingEffectSink::default());
        let actor = system
            .spawn(
                "remote-watch",
                RemoteDeathWatchActor::props(sink.clone() as Arc<dyn RemoteDeathWatchEffectSink>),
            )
            .unwrap();
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
    fn remote_watch_actor_reports_stats_after_ordered_commands() {
        let system = ActorSystem::builder("watcher").build().unwrap();
        let sink = Arc::new(RecordingEffectSink::default());
        let actor = system
            .spawn(
                "remote-watch",
                RemoteDeathWatchActor::props(sink as Arc<dyn RemoteDeathWatchEffectSink>),
            )
            .unwrap();
        let (stats_tx, stats_rx) = mpsc::channel();
        let stats_probe = system
            .spawn("stats", Props::new(move || Probe { sender: stats_tx }))
            .unwrap();

        actor
            .tell(RemoteDeathWatchCommand::Watch(WatchRemote {
                watchee: watchee("target"),
                watcher: watcher("observer"),
            }))
            .unwrap();
        actor
            .tell(RemoteDeathWatchCommand::AddressUnreachable {
                address: "kairo://remote@127.0.0.1:25520".to_string(),
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
                watching: 1,
                watched_addresses: 1,
                unreachable_addresses: 1,
            }
        );
    }
}
