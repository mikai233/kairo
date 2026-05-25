use kairo_actor::ActorRef;
use kairo_serialization::{ActorRefWireData, RemoteMessage};

use crate::{
    InboundMessage, RemoteDeathWatchCommand, RemoteError, RemoteHeartbeat, RemoteHeartbeatAck,
    RemoteInboundDelivery, Result, UnwatchRemote, WatchRemote,
};

#[derive(Clone)]
pub struct RemoteDeathWatchProtocolDelivery {
    watcher: ActorRef<RemoteDeathWatchCommand>,
    local_uid: u64,
}

impl RemoteDeathWatchProtocolDelivery {
    pub fn new(watcher: ActorRef<RemoteDeathWatchCommand>, local_uid: u64) -> Self {
        Self { watcher, local_uid }
    }

    pub fn watcher(&self) -> &ActorRef<RemoteDeathWatchCommand> {
        &self.watcher
    }

    pub fn local_uid(&self) -> u64 {
        self.local_uid
    }

    fn tell(&self, command: RemoteDeathWatchCommand) -> Result<()> {
        self.watcher.tell(command).map_err(|error| {
            RemoteError::Inbound(format!(
                "failed to deliver remote death-watch protocol message: {}",
                error.reason()
            ))
        })
    }
}

impl RemoteInboundDelivery<WatchRemote> for RemoteDeathWatchProtocolDelivery {
    fn deliver(&self, inbound: InboundMessage<WatchRemote>) -> Result<()> {
        self.tell(RemoteDeathWatchCommand::InboundWatch(inbound.message))
    }
}

impl RemoteInboundDelivery<UnwatchRemote> for RemoteDeathWatchProtocolDelivery {
    fn deliver(&self, inbound: InboundMessage<UnwatchRemote>) -> Result<()> {
        self.tell(RemoteDeathWatchCommand::InboundUnwatch(inbound.message))
    }
}

impl RemoteInboundDelivery<RemoteHeartbeat> for RemoteDeathWatchProtocolDelivery {
    fn deliver(&self, inbound: InboundMessage<RemoteHeartbeat>) -> Result<()> {
        let address = sender_address(&inbound.sender, RemoteHeartbeat::MANIFEST)?;
        self.tell(RemoteDeathWatchCommand::Heartbeat {
            address,
            heartbeat: inbound.message,
            local_uid: self.local_uid,
        })
    }
}

impl RemoteInboundDelivery<RemoteHeartbeatAck> for RemoteDeathWatchProtocolDelivery {
    fn deliver(&self, inbound: InboundMessage<RemoteHeartbeatAck>) -> Result<()> {
        let address = sender_address(&inbound.sender, RemoteHeartbeatAck::MANIFEST)?;
        self.tell(RemoteDeathWatchCommand::HeartbeatAck {
            address,
            ack: inbound.message,
        })
    }
}

fn sender_address(sender: &Option<ActorRefWireData>, manifest: &'static str) -> Result<String> {
    let Some(sender) = sender else {
        return Err(RemoteError::Inbound(format!(
            "remote death-watch `{manifest}` message is missing sender"
        )));
    };
    Ok(wire_address(sender))
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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Condvar, Mutex};
    use std::time::{Duration, Instant};

    use kairo_actor::{Actor, ActorError, ActorResult, ActorSystem, Context, Props};

    use super::*;
    use crate::{RemoteDeathWatchActor, RemoteDeathWatchEffect, RemoteDeathWatchEffectSink};

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

    fn watchee(name: &str) -> ActorRefWireData {
        ActorRefWireData::new(format!("kairo://remote@127.0.0.1:25520/user/{name}")).unwrap()
    }

    fn watcher(name: &str) -> ActorRefWireData {
        ActorRefWireData::new(format!("kairo://local@127.0.0.1:25521/user/{name}")).unwrap()
    }

    fn remote_watcher() -> ActorRefWireData {
        ActorRefWireData::new("kairo://remote@127.0.0.1:25520/system/remote-watch").unwrap()
    }

    #[test]
    fn inbound_watch_records_remote_watcher_without_outbound_watch_effect() {
        let system = ActorSystem::builder("local").build().unwrap();
        let sink = Arc::new(RecordingEffectSink::default());
        let actor = system
            .spawn(
                "remote-watch",
                RemoteDeathWatchActor::props(sink.clone() as Arc<dyn RemoteDeathWatchEffectSink>),
            )
            .unwrap();
        let delivery = RemoteDeathWatchProtocolDelivery::new(actor.clone(), 42);
        let (stats_tx, stats_rx) = std::sync::mpsc::channel();
        let stats_probe = system
            .spawn("stats", Props::new(move || Probe { sender: stats_tx }))
            .unwrap();

        delivery
            .deliver(InboundMessage {
                recipient: ActorRefWireData::new(
                    "kairo://local@127.0.0.1:25521/system/remote-watch",
                )
                .unwrap(),
                sender: Some(remote_watcher()),
                message: WatchRemote {
                    watchee: watchee("target"),
                    watcher: watcher("observer"),
                },
            })
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
    fn inbound_heartbeat_replies_with_local_uid_ack_effect() {
        let system = ActorSystem::builder("local").build().unwrap();
        let sink = Arc::new(RecordingEffectSink::default());
        let actor = system
            .spawn(
                "remote-watch",
                RemoteDeathWatchActor::props(sink.clone() as Arc<dyn RemoteDeathWatchEffectSink>),
            )
            .unwrap();
        let delivery = RemoteDeathWatchProtocolDelivery::new(actor, 42);

        delivery
            .deliver(InboundMessage {
                recipient: ActorRefWireData::new(
                    "kairo://local@127.0.0.1:25521/system/remote-watch",
                )
                .unwrap(),
                sender: Some(remote_watcher()),
                message: RemoteHeartbeat { from_uid: 7 },
            })
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
        let system = ActorSystem::builder("local").build().unwrap();
        let sink = Arc::new(RecordingEffectSink::default());
        let actor = system
            .spawn(
                "remote-watch",
                RemoteDeathWatchActor::props(sink.clone() as Arc<dyn RemoteDeathWatchEffectSink>),
            )
            .unwrap();
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
            .deliver(InboundMessage {
                recipient: ActorRefWireData::new(
                    "kairo://local@127.0.0.1:25521/system/remote-watch",
                )
                .unwrap(),
                sender: Some(remote_watcher()),
                message: RemoteHeartbeatAck { uid: 7 },
            })
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
                    recipient: ActorRefWireData::new(
                        "kairo://local@127.0.0.1:25521/system/remote-watch",
                    )
                    .unwrap(),
                    sender: None,
                    message: RemoteHeartbeat { from_uid: 7 },
                },
            )
            .expect_err("heartbeat without sender should fail");

        assert!(matches!(error, RemoteError::Inbound(_)));
        assert!(error.to_string().contains("missing sender"));
    }
}
