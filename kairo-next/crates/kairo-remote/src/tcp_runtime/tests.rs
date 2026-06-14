use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::time::{Duration, Instant};

use bytes::Bytes;
use kairo_actor::{
    Actor, ActorError, ActorRef, ActorResult, ActorSystem, Context, PHASE_SERVICE_UNBIND, Props,
};
use kairo_serialization::{
    ActorRefWireData, MessageCodec, Registry, RemoteMessage, SerializationRegistry,
};

use super::TcpRemoteActorSystem;
use crate::{
    AssociationState, RemoteAssociationAddress, RemoteDeathWatchCommand, RemoteDeathWatchEffect,
    RemoteDeathWatchEffectObserver, RemoteDeathWatchStats, RemoteSettings, TcpAssociationIdentity,
    WatchRemote, register_remote_protocol_codecs,
};

#[derive(Debug, Clone, PartialEq, Eq)]
struct Ping {
    value: u8,
}

impl RemoteMessage for Ping {
    const MANIFEST: &'static str = "kairo.remote.test.TcpRuntimePing";
    const VERSION: u16 = 1;
}

struct PingCodec;

impl MessageCodec<Ping> for PingCodec {
    fn serializer_id(&self) -> u32 {
        991
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

#[derive(Default)]
struct RecordingObserver {
    effects: Mutex<Vec<RemoteDeathWatchEffect>>,
    changed: Condvar,
}

impl RecordingObserver {
    fn wait_for(
        &self,
        timeout: Duration,
        predicate: impl Fn(&RemoteDeathWatchEffect) -> bool,
    ) -> Option<RemoteDeathWatchEffect> {
        let deadline = Instant::now() + timeout;
        let mut effects = self.effects.lock().expect("observer poisoned");
        loop {
            if let Some(effect) = effects.iter().find(|effect| predicate(effect)).cloned() {
                return Some(effect);
            }
            let remaining = deadline.checked_duration_since(Instant::now())?;
            let (next_effects, wait) = self
                .changed
                .wait_timeout(effects, remaining)
                .expect("observer poisoned");
            effects = next_effects;
            if wait.timed_out() {
                return effects.iter().find(|effect| predicate(effect)).cloned();
            }
        }
    }
}

impl RemoteDeathWatchEffectObserver for RecordingObserver {
    fn observe(&self, effect: &RemoteDeathWatchEffect) -> crate::Result<()> {
        self.effects
            .lock()
            .expect("observer poisoned")
            .push(effect.clone());
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

fn remote_path_for(local_path: &str, settings: &RemoteSettings) -> String {
    remote_path_for_system(local_path, "receiver", settings)
}

fn remote_path_for_system(local_path: &str, system: &str, settings: &RemoteSettings) -> String {
    local_path.replacen(
        &format!("kairo://{system}"),
        &format!(
            "kairo://{system}@{}:{}",
            settings.canonical_hostname, settings.canonical_port
        ),
        1,
    )
}

fn wait_for_receiver_inbound_watch(
    death_watch: &ActorRef<RemoteDeathWatchCommand>,
    reply_to: &ActorRef<RemoteDeathWatchStats>,
    stats_rx: &mpsc::Receiver<RemoteDeathWatchStats>,
) -> RemoteDeathWatchStats {
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        death_watch
            .tell(RemoteDeathWatchCommand::GetStats {
                reply_to: reply_to.clone(),
            })
            .unwrap();
        if let Ok(stats) = stats_rx.recv_timeout(Duration::from_millis(20))
            && stats.inbound_watching == 1
        {
            return stats;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for inbound remote-watch registration"
        );
    }
}

#[test]
fn tcp_remote_actor_system_sends_remote_ref_to_local_actor_over_loopback() {
    let receiver = ActorSystem::builder("receiver").build().unwrap();
    let sender = ActorSystem::builder("sender").build().unwrap();
    let registry = registry();
    let (received_tx, received_rx) = mpsc::channel();
    let (sender_received_tx, sender_received_rx) = mpsc::channel();
    let target = receiver
        .spawn(
            "target",
            Props::new(move || Target {
                received: received_tx,
            }),
        )
        .unwrap();
    let sender_target = sender
        .spawn(
            "target",
            Props::new(move || Target {
                received: sender_received_tx,
            }),
        )
        .unwrap();
    let receiver_remote = TcpRemoteActorSystem::<Ping>::bind(
        receiver.clone(),
        registry.clone(),
        RemoteSettings::new("127.0.0.1", 0),
        11,
    )
    .unwrap();
    let sender_remote = TcpRemoteActorSystem::<Ping>::bind(
        sender,
        registry,
        RemoteSettings::new("127.0.0.1", 0),
        22,
    )
    .unwrap();
    assert!(
        receiver_remote
            .death_watch()
            .path()
            .as_str()
            .starts_with("kairo://receiver/system/remote-watch#")
    );
    assert!(
        sender_remote
            .death_watch()
            .path()
            .as_str()
            .starts_with("kairo://sender/system/remote-watch#")
    );
    let sender_identity = TcpAssociationIdentity::new(
        RemoteAssociationAddress::new(
            "kairo",
            "sender",
            sender_remote.settings().canonical_hostname.clone(),
            Some(sender_remote.settings().canonical_port),
        )
        .unwrap(),
        22,
    );
    let receiver_address = RemoteAssociationAddress::new(
        "kairo",
        "receiver",
        receiver_remote.settings().canonical_hostname.clone(),
        Some(receiver_remote.settings().canonical_port),
    )
    .unwrap();
    let local_canonical_target = receiver_remote
        .resolve_actor_ref::<Ping>(remote_path_for(
            target.path().as_str(),
            receiver_remote.settings(),
        ))
        .unwrap();
    assert!(local_canonical_target.is_local());
    local_canonical_target.tell(Ping { value: 76 }).unwrap();
    assert_eq!(
        received_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        76
    );

    let registration = sender_remote.dial(receiver_address).unwrap();
    let remote_target = sender_remote
        .resolve::<Ping>(remote_path_for(
            target.path().as_str(),
            receiver_remote.settings(),
        ))
        .unwrap();

    remote_target.tell(Ping { value: 77 }).unwrap();

    assert_eq!(
        received_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        77
    );
    let receiver_association = receiver_remote
        .association_registry()
        .association_by_uid(22);
    assert!(receiver_association.is_some());
    assert_eq!(
        receiver_association
            .unwrap()
            .lock()
            .expect("remote association lock poisoned")
            .state(),
        &AssociationState::Active {
            remote_uid: Some(22)
        }
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    while receiver_remote.association_cache().route_count() == 0
        && std::time::Instant::now() < deadline
    {
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(receiver_remote.association_cache().route_count(), 1);
    let reverse_target = receiver_remote
        .resolve::<Ping>(remote_path_for_system(
            sender_target.path().as_str(),
            "sender",
            sender_remote.settings(),
        ))
        .unwrap();

    reverse_target.tell(Ping { value: 78 }).unwrap();

    assert_eq!(
        sender_received_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap(),
        78
    );

    drop(registration);
    let sender_watch = sender_remote.death_watch().clone();
    let receiver_watch = receiver_remote.death_watch().clone();
    let sender_report = sender_remote.shutdown().unwrap();
    assert!(sender_watch.wait_for_stop(Duration::from_secs(1)));
    assert_eq!(sender_report.accepted_associations, 0);
    let receiver_report = receiver_remote.shutdown().unwrap();
    assert!(receiver_watch.wait_for_stop(Duration::from_secs(1)));
    assert_eq!(receiver_report.accepted_associations, 1);
    assert_eq!(receiver_report.remote_identities, vec![sender_identity]);
    assert_eq!(receiver_report.read.streams, 3);
    assert_eq!(receiver_report.read.frames, 1);
}

#[test]
fn tcp_remote_actor_system_round_trips_remote_death_watch_heartbeat_ack() {
    let receiver = ActorSystem::builder("receiver").build().unwrap();
    let sender = ActorSystem::builder("sender").build().unwrap();
    let registry = registry();
    let (received_tx, _received_rx) = mpsc::channel();
    let (sender_received_tx, _sender_received_rx) = mpsc::channel();
    let target = receiver
        .spawn(
            "target",
            Props::new(move || Target {
                received: received_tx,
            }),
        )
        .unwrap();
    let watcher = sender
        .spawn(
            "watcher",
            Props::new(move || Target {
                received: sender_received_tx,
            }),
        )
        .unwrap();
    let receiver_observer = Arc::new(RecordingObserver::default());
    let sender_observer = Arc::new(RecordingObserver::default());
    let receiver_remote = TcpRemoteActorSystem::<Ping>::bind_with_observer(
        receiver.clone(),
        registry.clone(),
        RemoteSettings::new("127.0.0.1", 0),
        11,
        receiver_observer.clone() as Arc<dyn RemoteDeathWatchEffectObserver>,
    )
    .unwrap();
    let sender_remote = TcpRemoteActorSystem::<Ping>::bind_with_observer(
        sender.clone(),
        registry,
        RemoteSettings::new("127.0.0.1", 0),
        22,
        sender_observer.clone() as Arc<dyn RemoteDeathWatchEffectObserver>,
    )
    .unwrap();
    let receiver_address = RemoteAssociationAddress::new(
        "kairo",
        "receiver",
        receiver_remote.settings().canonical_hostname.clone(),
        Some(receiver_remote.settings().canonical_port),
    )
    .unwrap();
    let registration = sender_remote.dial(receiver_address).unwrap();
    let deadline = Instant::now() + Duration::from_secs(1);
    while receiver_remote.association_cache().route_count() == 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(receiver_remote.association_cache().route_count(), 1);
    let (stats_tx, stats_rx) = mpsc::channel();
    let stats_probe = receiver_remote
        .system()
        .spawn(
            "receiver-watch-stats",
            Props::new(move || Probe { sender: stats_tx }),
        )
        .unwrap();
    let watchee = ActorRefWireData::new(remote_path_for(
        target.path().as_str(),
        receiver_remote.settings(),
    ))
    .unwrap();
    let watcher = ActorRefWireData::new(remote_path_for_system(
        watcher.path().as_str(),
        "sender",
        sender_remote.settings(),
    ))
    .unwrap();

    sender_remote
        .death_watch()
        .tell(RemoteDeathWatchCommand::Watch(WatchRemote {
            watchee: watchee.clone(),
            watcher: watcher.clone(),
        }))
        .unwrap();
    sender_observer
        .wait_for(Duration::from_secs(1), |effect| {
            matches!(effect, RemoteDeathWatchEffect::SendWatchRemote(_))
        })
        .expect("sender should send remote watch over control lane");
    let stats =
        wait_for_receiver_inbound_watch(receiver_remote.death_watch(), &stats_probe, &stats_rx);
    assert_eq!(stats.watching, 0);
    assert_eq!(stats.watched_addresses, 0);

    sender_remote
        .death_watch()
        .tell(RemoteDeathWatchCommand::HeartbeatTick { local_uid: 22 })
        .unwrap();

    receiver_observer
        .wait_for(Duration::from_secs(1), |effect| {
            matches!(effect, RemoteDeathWatchEffect::SendHeartbeatAck { .. })
        })
        .expect("receiver should ack sender heartbeat");
    sender_observer
        .wait_for(Duration::from_secs(1), |effect| {
            matches!(
                effect,
                RemoteDeathWatchEffect::RewatchRemote(WatchRemote {
                    watchee: effect_watchee,
                    watcher: effect_watcher,
                }) if effect_watchee == &watchee && effect_watcher == &watcher
            )
        })
        .expect("sender should rewatch after first heartbeat ack UID");

    drop(registration);
    let sender_report = sender_remote.shutdown().unwrap();
    assert_eq!(sender_report.accepted_associations, 0);
    let receiver_report = receiver_remote.shutdown().unwrap();
    assert_eq!(receiver_report.accepted_associations, 1);
}

#[test]
fn tcp_remote_actor_system_coordinated_shutdown_stops_runtime_once() {
    let receiver = ActorSystem::builder("receiver").build().unwrap();
    let sender = ActorSystem::builder("sender").build().unwrap();
    let registry = registry();
    let receiver_remote = TcpRemoteActorSystem::<Ping>::bind(
        receiver.clone(),
        registry.clone(),
        RemoteSettings::new("127.0.0.1", 0),
        11,
    )
    .unwrap();
    let sender_remote = TcpRemoteActorSystem::<Ping>::bind(
        sender.clone(),
        registry,
        RemoteSettings::new("127.0.0.1", 0),
        22,
    )
    .unwrap();
    sender_remote
        .register_coordinated_shutdown(
            PHASE_SERVICE_UNBIND,
            "remote-tcp-runtime-shutdown",
            Duration::from_secs(1),
        )
        .unwrap();
    let receiver_address = RemoteAssociationAddress::new(
        "kairo",
        "receiver",
        receiver_remote.settings().canonical_hostname.clone(),
        Some(receiver_remote.settings().canonical_port),
    )
    .unwrap();
    let registration = sender_remote.dial(receiver_address).unwrap();
    assert_eq!(sender_remote.association_cache().route_count(), 1);

    sender
        .coordinated_shutdown()
        .run_from(
            "remote runtime coordinated shutdown",
            Some(PHASE_SERVICE_UNBIND),
        )
        .unwrap();

    assert!(
        sender_remote
            .death_watch()
            .wait_for_stop(Duration::from_secs(1))
    );
    assert_eq!(sender_remote.association_cache().route_count(), 0);
    let second_report = sender_remote.shutdown().unwrap();
    assert_eq!(second_report.accepted_associations, 0);

    drop(registration);
    receiver_remote.shutdown().unwrap();
    sender.terminate(Duration::from_secs(1)).unwrap();
    receiver.terminate(Duration::from_secs(1)).unwrap();
}
