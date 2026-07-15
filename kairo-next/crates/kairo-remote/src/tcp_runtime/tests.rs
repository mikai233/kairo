use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::time::{Duration, Instant};

use bytes::Bytes;
use kairo_actor::{
    Actor, ActorError, ActorPath, ActorRef, ActorRefProvider, ActorRefResolveResult, ActorResult,
    ActorSystem, Context, PHASE_SERVICE_UNBIND, Props, Signal,
};
use kairo_serialization::{
    ActorRefResolver, ActorRefWireData, MessageCodec, Registry, RemoteEnvelope, RemoteMessage,
    SerializationRegistry,
};
use kairo_testkit::await_assert;

use super::{TcpRemoteActorRuntime, TcpRemoteActorSystem};
use crate::{
    AddressTerminated, AssociationState, RemoteAssociationAddress, RemoteAssociationCache,
    RemoteDeathWatchCommand, RemoteDeathWatchEffect, RemoteDeathWatchEffectObserver,
    RemoteDeathWatchStats, RemoteOutbound, RemoteSettings, RemoteTerminated,
    TcpAssociationIdentity, UnwatchRemote, WatchRemote, register_remote_protocol_codecs,
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct Pong {
    value: u16,
}

impl RemoteMessage for Pong {
    const MANIFEST: &'static str = "kairo.remote.test.TcpRuntimePong";
    const VERSION: u16 = 1;
}

struct PongCodec;

impl MessageCodec<Pong> for PongCodec {
    fn serializer_id(&self) -> u32 {
        992
    }

    fn encode(&self, message: &Pong) -> kairo_serialization::Result<Bytes> {
        Ok(Bytes::copy_from_slice(&message.value.to_be_bytes()))
    }

    fn decode(&self, payload: Bytes, _version: u16) -> kairo_serialization::Result<Pong> {
        Ok(Pong {
            value: u16::from_be_bytes([payload[0], payload[1]]),
        })
    }
}

struct Target {
    received: mpsc::Sender<u8>,
}

struct PongTarget {
    received: mpsc::Sender<u16>,
}

impl Actor for PongTarget {
    type Msg = Pong;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        self.received
            .send(msg.value)
            .map_err(|error| kairo_actor::ActorError::Message(error.to_string()))
    }
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

struct LocalWatchMsg;

struct TerminationWatcher {
    terminated: mpsc::Sender<ActorPath>,
}

impl Actor for TerminationWatcher {
    type Msg = LocalWatchMsg;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, _msg: Self::Msg) -> ActorResult {
        Ok(())
    }

    fn signal(&mut self, _ctx: &mut Context<Self::Msg>, signal: Signal) -> ActorResult {
        if let Signal::Terminated(actor) = signal {
            self.terminated
                .send(actor.path().clone())
                .map_err(|error| ActorError::Message(error.to_string()))?;
        }
        Ok(())
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

    fn count(&self, predicate: impl Fn(&RemoteDeathWatchEffect) -> bool) -> usize {
        self.effects
            .lock()
            .expect("observer poisoned")
            .iter()
            .filter(|effect| predicate(effect))
            .count()
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

#[derive(Default)]
struct NoopOutbound;

impl RemoteOutbound for NoopOutbound {
    fn send(&self, _envelope: RemoteEnvelope) -> crate::Result<()> {
        Ok(())
    }
}

struct LateRouteOnClose {
    cache: RemoteAssociationCache,
    late_address: RemoteAssociationAddress,
}

impl RemoteOutbound for LateRouteOnClose {
    fn send(&self, _envelope: RemoteEnvelope) -> crate::Result<()> {
        Ok(())
    }

    fn close(&self, _reason: &str) -> crate::Result<()> {
        self.cache
            .insert_route(self.late_address.clone(), Arc::new(NoopOutbound));
        Ok(())
    }
}

fn registry() -> Arc<Registry> {
    let mut registry = Registry::new();
    registry.register::<Ping, _>(PingCodec).unwrap();
    registry.register::<Pong, _>(PongCodec).unwrap();
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
    wait_for_watch_stats(
        death_watch,
        reply_to,
        stats_rx,
        |stats| stats.inbound_watching == 1,
        "inbound remote-watch registration",
    )
}

fn wait_for_watch_stats(
    death_watch: &ActorRef<RemoteDeathWatchCommand>,
    reply_to: &ActorRef<RemoteDeathWatchStats>,
    stats_rx: &mpsc::Receiver<RemoteDeathWatchStats>,
    predicate: impl Fn(&RemoteDeathWatchStats) -> bool,
    reason: &str,
) -> RemoteDeathWatchStats {
    await_assert(
        Duration::from_secs(1),
        Duration::from_millis(1),
        || -> Result<RemoteDeathWatchStats, String> {
            death_watch
                .tell(RemoteDeathWatchCommand::GetStats {
                    reply_to: reply_to.clone(),
                })
                .map_err(|error| error.reason().to_string())?;
            let stats = stats_rx
                .recv_timeout(Duration::from_millis(20))
                .map_err(|error| format!("waiting for {reason}: {error}"))?;
            if predicate(&stats) {
                Ok(stats)
            } else {
                Err(format!("waiting for {reason}: {stats:?}"))
            }
        },
    )
    .unwrap_or_else(|error| panic!("{error}"))
}

fn await_route_count(cache: &RemoteAssociationCache, expected: usize) {
    await_assert(
        Duration::from_secs(1),
        Duration::from_millis(1),
        || -> Result<(), String> {
            let actual = cache.route_count();
            if actual == expected {
                Ok(())
            } else {
                Err(format!(
                    "expected {expected} association routes, found {actual}"
                ))
            }
        },
    )
    .unwrap();
}

fn await_bidirectional_routes(
    sender_remote: &TcpRemoteActorSystem<Ping>,
    receiver_remote: &TcpRemoteActorSystem<Ping>,
) {
    await_assert(
        Duration::from_secs(1),
        Duration::from_millis(1),
        || -> Result<(), String> {
            let sender_routes = sender_remote.association_cache().route_count();
            let receiver_routes = receiver_remote.association_cache().route_count();
            if sender_routes == 1 && receiver_routes == 1 {
                Ok(())
            } else {
                Err(format!(
                    "expected bidirectional association routes, found sender={sender_routes}, receiver={receiver_routes}"
                ))
            }
        },
    )
    .unwrap();
}

#[test]
fn tcp_remote_runtime_rejects_duplicate_protocol_before_bind() {
    let system = ActorSystem::builder("duplicate-protocol").build().unwrap();
    let mut builder =
        TcpRemoteActorRuntime::builder(system, registry(), RemoteSettings::new("127.0.0.1", 0), 11);

    builder.register::<Ping>().unwrap();
    let error = match builder.register::<Ping>() {
        Ok(_) => panic!("duplicate manifest should fail before bind"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        crate::RemoteError::DuplicateProtocolManifest(manifest)
            if manifest == Ping::MANIFEST
    ));
}

#[test]
fn tcp_remote_runtime_delivers_two_protocols_on_one_association() {
    let receiver = ActorSystem::builder("receiver").build().unwrap();
    let sender = ActorSystem::builder("sender").build().unwrap();
    let registry = registry();
    let (ping_tx, ping_rx) = mpsc::channel();
    let (pong_tx, pong_rx) = mpsc::channel();
    let ping_target = receiver
        .spawn(
            "ping-target",
            Props::new(move || Target { received: ping_tx }),
        )
        .unwrap();
    let pong_target = receiver
        .spawn(
            "pong-target",
            Props::new(move || PongTarget { received: pong_tx }),
        )
        .unwrap();

    let mut receiver_builder = TcpRemoteActorRuntime::builder(
        receiver,
        registry.clone(),
        RemoteSettings::new("127.0.0.1", 0),
        11,
    );
    receiver_builder.register::<Ping>().unwrap();
    receiver_builder.register::<Pong>().unwrap();
    let receiver_remote = receiver_builder.bind().unwrap();

    let mut sender_builder =
        TcpRemoteActorRuntime::builder(sender, registry, RemoteSettings::new("127.0.0.1", 0), 22);
    sender_builder.register::<Ping>().unwrap();
    sender_builder.register::<Pong>().unwrap();
    let sender_remote = sender_builder.bind().unwrap();

    let receiver_address = RemoteAssociationAddress::new(
        "kairo",
        "receiver",
        receiver_remote.settings().canonical_hostname.clone(),
        Some(receiver_remote.settings().canonical_port),
    )
    .unwrap();
    let registration = sender_remote.dial(receiver_address).unwrap();
    let remote_ping = sender_remote
        .resolve::<Ping>(remote_path_for(
            ping_target.path().as_str(),
            receiver_remote.settings(),
        ))
        .unwrap();
    let remote_pong = sender_remote
        .resolve::<Pong>(remote_path_for(
            pong_target.path().as_str(),
            receiver_remote.settings(),
        ))
        .unwrap();

    remote_ping.tell(Ping { value: 7 }).unwrap();
    remote_pong.tell(Pong { value: 700 }).unwrap();

    assert_eq!(ping_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 7);
    assert_eq!(pong_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 700);
    assert_eq!(sender_remote.association_cache().route_count(), 1);
    await_route_count(receiver_remote.association_cache(), 1);

    registration.close_owned_route("heterogeneous protocol test done");
    sender_remote.shutdown().unwrap();
    let report = receiver_remote.shutdown().unwrap();
    assert_eq!(report.accepted_associations, 1);
}

#[test]
fn tcp_remote_actor_system_provider_delegates_local_provider_boundary() {
    let receiver = ActorSystem::builder("receiver").build().unwrap();
    let (received_tx, _received_rx) = mpsc::channel();
    let target = receiver
        .spawn(
            "target",
            Props::new(move || Target {
                received: received_tx,
            }),
        )
        .unwrap();
    let receiver_remote = TcpRemoteActorSystem::<Ping>::bind(
        receiver,
        registry(),
        RemoteSettings::new("127.0.0.1", 0),
        11,
    )
    .unwrap();
    let provider = receiver_remote.provider();
    let local_provider = receiver_remote.system().provider();

    assert_eq!(
        provider.root_guardian().path().as_str(),
        local_provider.root_guardian().path().as_str()
    );
    assert_eq!(
        provider.user_guardian().path().as_str(),
        local_provider.user_guardian().path().as_str()
    );
    assert_eq!(
        provider.system_guardian().path().as_str(),
        local_provider.system_guardian().path().as_str()
    );
    assert_eq!(
        provider.temp_guardian().path().as_str(),
        local_provider.temp_guardian().path().as_str()
    );
    assert_eq!(
        provider.dead_letters().path().as_str(),
        local_provider.dead_letters().path().as_str()
    );
    assert_eq!(
        provider.temp_path("tcp-provider").parent(),
        Some(local_provider.temp_guardian().path().clone())
    );

    let canonical_path = ActorPath::new(remote_path_for(
        target.path().as_str(),
        receiver_remote.settings(),
    ));
    let resolved = ActorRefProvider::resolve(provider, &canonical_path);

    assert!(matches!(resolved, ActorRefResolveResult::Local(_)));
    assert_eq!(resolved.path().as_str(), target.path().as_str());

    let foreign_path = ActorPath::new("kairo://sender@127.0.0.1:25521/user/target#1");
    let resolved = ActorRefProvider::resolve(provider, &foreign_path);

    assert!(matches!(resolved, ActorRefResolveResult::NonLocal(_)));
    assert_eq!(resolved.path().as_str(), foreign_path.as_str());

    receiver_remote.shutdown().unwrap();
}

#[test]
fn tcp_remote_actor_system_resolver_trait_resolves_local_and_remote_refs() {
    let receiver = ActorSystem::builder("receiver").build().unwrap();
    let registry = registry();
    let (received_tx, received_rx) = mpsc::channel();
    let target = receiver
        .spawn(
            "target",
            Props::new(move || Target {
                received: received_tx,
            }),
        )
        .unwrap();
    let receiver_remote = TcpRemoteActorSystem::<Ping>::bind(
        receiver,
        registry,
        RemoteSettings::new("127.0.0.1", 0),
        11,
    )
    .unwrap();
    let resolver = receiver_remote.resolver::<Ping>();
    let local_wire = ActorRefWireData::new(remote_path_for(
        target.path().as_str(),
        receiver_remote.settings(),
    ))
    .unwrap();

    let local_resolved = resolver.resolve_actor_ref(&local_wire).unwrap();

    assert!(local_resolved.is_local());
    local_resolved.tell(Ping { value: 31 }).unwrap();
    assert_eq!(
        received_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        31
    );

    let remote_wire = ActorRefWireData::new("kairo://sender@127.0.0.1:25521/user/target").unwrap();
    let remote_resolved = resolver.resolve_actor_ref(&remote_wire).unwrap();

    assert!(remote_resolved.is_remote());
    assert_eq!(
        remote_resolved.path().as_str(),
        "kairo://sender@127.0.0.1:25521/user/target"
    );

    receiver_remote.shutdown().unwrap();
}

#[test]
fn tcp_remote_actor_system_resolver_trait_resolves_local_system_refs() {
    let receiver = ActorSystem::builder("receiver").build().unwrap();
    let registry = registry();
    let (received_tx, received_rx) = mpsc::channel();
    let target = receiver
        .spawn_system(
            "system-target",
            Props::new(move || Target {
                received: received_tx,
            }),
        )
        .unwrap();
    let receiver_remote = TcpRemoteActorSystem::<Ping>::bind(
        receiver,
        registry,
        RemoteSettings::new("127.0.0.1", 0),
        11,
    )
    .unwrap();
    let resolver = receiver_remote.resolver::<Ping>();
    let local_wire = ActorRefWireData::new(remote_path_for(
        target.path().as_str(),
        receiver_remote.settings(),
    ))
    .unwrap();

    let local_resolved = resolver.resolve_actor_ref(&local_wire).unwrap();

    assert!(local_resolved.is_local());
    assert!(
        local_resolved
            .path()
            .as_str()
            .starts_with("kairo://receiver/system/system-target#")
    );
    local_resolved.tell(Ping { value: 41 }).unwrap();
    assert_eq!(
        received_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        41
    );

    receiver_remote.shutdown().unwrap();
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
    await_route_count(receiver_remote.association_cache(), 1);
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
    await_route_count(receiver_remote.association_cache(), 1);
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
fn tcp_remote_actor_system_routes_remote_terminated_to_watcher() {
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
    await_bidirectional_routes(&sender_remote, &receiver_remote);

    let (receiver_stats_tx, receiver_stats_rx) = mpsc::channel();
    let receiver_stats_probe = receiver_remote
        .system()
        .spawn(
            "receiver-watch-stats",
            Props::new(move || Probe {
                sender: receiver_stats_tx,
            }),
        )
        .unwrap();
    let (sender_stats_tx, sender_stats_rx) = mpsc::channel();
    let sender_stats_probe = sender_remote
        .system()
        .spawn(
            "sender-watch-stats",
            Props::new(move || Probe {
                sender: sender_stats_tx,
            }),
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
            matches!(
                effect,
                RemoteDeathWatchEffect::SendWatchRemote(WatchRemote {
                    watchee: effect_watchee,
                    watcher: effect_watcher,
                }) if effect_watchee == &watchee && effect_watcher == &watcher
            )
        })
        .expect("sender should send remote watch before termination");
    let receiver_stats = wait_for_receiver_inbound_watch(
        receiver_remote.death_watch(),
        &receiver_stats_probe,
        &receiver_stats_rx,
    );
    assert_eq!(receiver_stats.inbound_watching, 1);

    receiver_remote
        .death_watch()
        .tell(RemoteDeathWatchCommand::LocalWatcheeTerminated {
            watchee: watchee.clone(),
            existence_confirmed: true,
        })
        .unwrap();

    receiver_observer
        .wait_for(Duration::from_secs(1), |effect| {
            matches!(
                effect,
                RemoteDeathWatchEffect::SendRemoteTerminated {
                    watcher: effect_watcher,
                    message: RemoteTerminated {
                        watchee: effect_watchee,
                        existence_confirmed: true,
                    },
                } if effect_watchee == &watchee && effect_watcher == &watcher
            )
        })
        .expect("receiver should send remote terminated over control lane");
    sender_observer
        .wait_for(Duration::from_secs(1), |effect| {
            matches!(
                effect,
                RemoteDeathWatchEffect::RemoteTerminated(RemoteTerminated {
                    watchee: effect_watchee,
                    existence_confirmed: true,
                }) if effect_watchee == &watchee
            )
        })
        .expect("sender should route remote terminated to remote death-watch");
    let sender_stats = wait_for_watch_stats(
        sender_remote.death_watch(),
        &sender_stats_probe,
        &sender_stats_rx,
        |stats| stats.watching == 0 && stats.watched_addresses == 0,
        "sender watch state cleanup after remote terminated",
    );
    assert_eq!(sender_stats.watching, 0);
    assert_eq!(sender_stats.watched_addresses, 0);
    let receiver_stats = wait_for_watch_stats(
        receiver_remote.death_watch(),
        &receiver_stats_probe,
        &receiver_stats_rx,
        |stats| stats.inbound_watching == 0,
        "receiver inbound watch cleanup after local termination",
    );
    assert_eq!(receiver_stats.inbound_watching, 0);

    drop(registration);
    let sender_report = sender_remote.shutdown().unwrap();
    assert_eq!(sender_report.accepted_associations, 0);
    let receiver_report = receiver_remote.shutdown().unwrap();
    assert_eq!(receiver_report.accepted_associations, 1);
}

#[test]
fn tcp_remote_actor_system_watch_remote_notifies_local_watcher() {
    let receiver = ActorSystem::builder("receiver").build().unwrap();
    let sender = ActorSystem::builder("sender").build().unwrap();
    let registry = registry();
    let (target_received_tx, _target_received_rx) = mpsc::channel();
    let target = receiver
        .spawn(
            "target",
            Props::new(move || Target {
                received: target_received_tx,
            }),
        )
        .unwrap();
    let (terminated_tx, terminated_rx) = mpsc::channel();
    let watcher = sender
        .spawn(
            "watcher",
            Props::new(move || TerminationWatcher {
                terminated: terminated_tx,
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
    await_bidirectional_routes(&sender_remote, &receiver_remote);

    let (receiver_stats_tx, receiver_stats_rx) = mpsc::channel();
    let receiver_stats_probe = receiver_remote
        .system()
        .spawn(
            "receiver-watch-stats",
            Props::new(move || Probe {
                sender: receiver_stats_tx,
            }),
        )
        .unwrap();
    let (sender_stats_tx, sender_stats_rx) = mpsc::channel();
    let sender_stats_probe = sender_remote
        .system()
        .spawn(
            "sender-watch-stats",
            Props::new(move || Probe {
                sender: sender_stats_tx,
            }),
        )
        .unwrap();
    let remote_target = sender_remote
        .resolve::<Ping>(remote_path_for(
            target.path().as_str(),
            receiver_remote.settings(),
        ))
        .unwrap();
    let watchee = remote_target.recipient().clone();
    let watcher_wire = sender_remote
        .provider()
        .local_actor_ref_to_wire_data(&watcher)
        .unwrap();

    sender_remote
        .watch_remote(watcher.clone(), &remote_target)
        .unwrap();
    sender_observer
        .wait_for(Duration::from_secs(1), |effect| {
            matches!(
                effect,
                RemoteDeathWatchEffect::SendWatchRemote(WatchRemote {
                    watchee: effect_watchee,
                    watcher: effect_watcher,
                }) if effect_watchee == &watchee && effect_watcher == &watcher_wire
            )
        })
        .expect("sender should send remote watch for local watcher");
    let receiver_stats = wait_for_receiver_inbound_watch(
        receiver_remote.death_watch(),
        &receiver_stats_probe,
        &receiver_stats_rx,
    );
    assert_eq!(receiver_stats.inbound_watching, 1);

    receiver_remote
        .death_watch()
        .tell(RemoteDeathWatchCommand::LocalWatcheeTerminated {
            watchee: watchee.clone(),
            existence_confirmed: true,
        })
        .unwrap();

    assert_eq!(
        terminated_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        remote_target.path().clone()
    );
    sender_observer
        .wait_for(Duration::from_secs(1), |effect| {
            matches!(
                effect,
                RemoteDeathWatchEffect::RemoteTerminated(RemoteTerminated {
                    watchee: effect_watchee,
                    existence_confirmed: true,
                }) if effect_watchee == &watchee
            )
        })
        .expect("sender should observe remote terminated");
    let sender_stats = wait_for_watch_stats(
        sender_remote.death_watch(),
        &sender_stats_probe,
        &sender_stats_rx,
        |stats| stats.watching == 0 && stats.watched_addresses == 0,
        "sender watch state cleanup after remote terminated",
    );
    assert_eq!(sender_stats.watching, 0);
    assert_eq!(sender_stats.watched_addresses, 0);
    let receiver_stats = wait_for_watch_stats(
        receiver_remote.death_watch(),
        &receiver_stats_probe,
        &receiver_stats_rx,
        |stats| stats.inbound_watching == 0,
        "receiver inbound watch cleanup after local termination",
    );
    assert_eq!(receiver_stats.inbound_watching, 0);

    drop(registration);
    let sender_report = sender_remote.shutdown().unwrap();
    assert_eq!(sender_report.accepted_associations, 0);
    let receiver_report = receiver_remote.shutdown().unwrap();
    assert_eq!(receiver_report.accepted_associations, 1);
}

#[test]
fn tcp_remote_actor_system_duplicate_watch_remote_is_idempotent() {
    let receiver = ActorSystem::builder("receiver").build().unwrap();
    let sender = ActorSystem::builder("sender").build().unwrap();
    let registry = registry();
    let (target_received_tx, _target_received_rx) = mpsc::channel();
    let target = receiver
        .spawn(
            "target",
            Props::new(move || Target {
                received: target_received_tx,
            }),
        )
        .unwrap();
    let (terminated_tx, terminated_rx) = mpsc::channel();
    let watcher = sender
        .spawn(
            "watcher",
            Props::new(move || TerminationWatcher {
                terminated: terminated_tx,
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
    await_bidirectional_routes(&sender_remote, &receiver_remote);

    let (receiver_stats_tx, receiver_stats_rx) = mpsc::channel();
    let receiver_stats_probe = receiver_remote
        .system()
        .spawn(
            "receiver-watch-stats",
            Props::new(move || Probe {
                sender: receiver_stats_tx,
            }),
        )
        .unwrap();
    let (sender_stats_tx, sender_stats_rx) = mpsc::channel();
    let sender_stats_probe = sender_remote
        .system()
        .spawn(
            "sender-watch-stats",
            Props::new(move || Probe {
                sender: sender_stats_tx,
            }),
        )
        .unwrap();
    let remote_target = sender_remote
        .resolve::<Ping>(remote_path_for(
            target.path().as_str(),
            receiver_remote.settings(),
        ))
        .unwrap();
    let watchee = remote_target.recipient().clone();
    let watcher_wire = sender_remote
        .provider()
        .local_actor_ref_to_wire_data(&watcher)
        .unwrap();

    sender_remote
        .watch_remote(watcher.clone(), &remote_target)
        .unwrap();
    sender_observer
        .wait_for(Duration::from_secs(1), |effect| {
            matches!(
                effect,
                RemoteDeathWatchEffect::SendWatchRemote(WatchRemote {
                    watchee: effect_watchee,
                    watcher: effect_watcher,
                }) if effect_watchee == &watchee && effect_watcher == &watcher_wire
            )
        })
        .expect("sender should send first remote watch");
    let receiver_stats = wait_for_receiver_inbound_watch(
        receiver_remote.death_watch(),
        &receiver_stats_probe,
        &receiver_stats_rx,
    );
    assert_eq!(receiver_stats.inbound_watching, 1);

    sender_remote
        .watch_remote(watcher.clone(), &remote_target)
        .unwrap();
    let sender_stats = wait_for_watch_stats(
        sender_remote.death_watch(),
        &sender_stats_probe,
        &sender_stats_rx,
        |stats| stats.watching == 1 && stats.watched_addresses == 1,
        "sender duplicate remote watch to be processed",
    );
    assert_eq!(sender_stats.watching_refs.len(), 1);
    assert_eq!(
        sender_observer.count(|effect| {
            matches!(
                effect,
                RemoteDeathWatchEffect::SendWatchRemote(WatchRemote {
                    watchee: effect_watchee,
                    watcher: effect_watcher,
                }) if effect_watchee == &watchee && effect_watcher == &watcher_wire
            )
        }),
        1,
        "duplicate watch_remote must not emit a second wire watch"
    );
    let receiver_stats = wait_for_watch_stats(
        receiver_remote.death_watch(),
        &receiver_stats_probe,
        &receiver_stats_rx,
        |stats| stats.inbound_watching == 1,
        "receiver duplicate inbound remote watch to remain singular",
    );
    assert_eq!(receiver_stats.inbound_watching, 1);

    receiver_remote
        .death_watch()
        .tell(RemoteDeathWatchCommand::LocalWatcheeTerminated {
            watchee,
            existence_confirmed: true,
        })
        .unwrap();
    assert_eq!(
        terminated_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        remote_target.path().clone()
    );
    assert!(
        terminated_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err(),
        "duplicate watch_remote must not produce duplicate local termination delivery"
    );

    drop(registration);
    let sender_report = sender_remote.shutdown().unwrap();
    assert_eq!(sender_report.accepted_associations, 0);
    let receiver_report = receiver_remote.shutdown().unwrap();
    assert_eq!(receiver_report.accepted_associations, 1);
}

#[test]
fn tcp_remote_actor_system_unwatch_remote_removes_local_and_remote_watch() {
    let receiver = ActorSystem::builder("receiver").build().unwrap();
    let sender = ActorSystem::builder("sender").build().unwrap();
    let registry = registry();
    let (target_received_tx, _target_received_rx) = mpsc::channel();
    let target = receiver
        .spawn(
            "target",
            Props::new(move || Target {
                received: target_received_tx,
            }),
        )
        .unwrap();
    let (terminated_tx, terminated_rx) = mpsc::channel();
    let watcher = sender
        .spawn(
            "watcher",
            Props::new(move || TerminationWatcher {
                terminated: terminated_tx,
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
    await_bidirectional_routes(&sender_remote, &receiver_remote);

    let (receiver_stats_tx, receiver_stats_rx) = mpsc::channel();
    let receiver_stats_probe = receiver_remote
        .system()
        .spawn(
            "receiver-watch-stats",
            Props::new(move || Probe {
                sender: receiver_stats_tx,
            }),
        )
        .unwrap();
    let (sender_stats_tx, sender_stats_rx) = mpsc::channel();
    let sender_stats_probe = sender_remote
        .system()
        .spawn(
            "sender-watch-stats",
            Props::new(move || Probe {
                sender: sender_stats_tx,
            }),
        )
        .unwrap();
    let remote_target = sender_remote
        .resolve::<Ping>(remote_path_for(
            target.path().as_str(),
            receiver_remote.settings(),
        ))
        .unwrap();
    let watchee = remote_target.recipient().clone();
    let watcher_wire = sender_remote
        .provider()
        .local_actor_ref_to_wire_data(&watcher)
        .unwrap();

    sender_remote
        .watch_remote(watcher.clone(), &remote_target)
        .unwrap();
    wait_for_receiver_inbound_watch(
        receiver_remote.death_watch(),
        &receiver_stats_probe,
        &receiver_stats_rx,
    );

    sender_remote
        .unwatch_remote(&watcher, &remote_target)
        .unwrap();
    sender_observer
        .wait_for(Duration::from_secs(1), |effect| {
            matches!(
                effect,
                RemoteDeathWatchEffect::SendUnwatchRemote(UnwatchRemote {
                    watchee: effect_watchee,
                    watcher: effect_watcher,
                }) if effect_watchee == &watchee && effect_watcher == &watcher_wire
            )
        })
        .expect("sender should send remote unwatch");
    let sender_stats = wait_for_watch_stats(
        sender_remote.death_watch(),
        &sender_stats_probe,
        &sender_stats_rx,
        |stats| stats.watching == 0 && stats.watched_addresses == 0,
        "sender watch state cleanup after remote unwatch",
    );
    assert_eq!(sender_stats.watching, 0);
    assert_eq!(sender_stats.watched_addresses, 0);
    let receiver_stats = wait_for_watch_stats(
        receiver_remote.death_watch(),
        &receiver_stats_probe,
        &receiver_stats_rx,
        |stats| stats.inbound_watching == 0,
        "receiver inbound watch cleanup after remote unwatch",
    );
    assert_eq!(receiver_stats.inbound_watching, 0);

    receiver_remote
        .death_watch()
        .tell(RemoteDeathWatchCommand::LocalWatcheeTerminated {
            watchee,
            existence_confirmed: true,
        })
        .unwrap();
    assert!(
        terminated_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err()
    );

    drop(registration);
    let sender_report = sender_remote.shutdown().unwrap();
    assert_eq!(sender_report.accepted_associations, 0);
    let receiver_report = receiver_remote.shutdown().unwrap();
    assert_eq!(receiver_report.accepted_associations, 1);
}

#[test]
fn tcp_remote_actor_system_unwatch_one_of_two_remote_watchers_keeps_other_watch() {
    let receiver = ActorSystem::builder("receiver").build().unwrap();
    let sender = ActorSystem::builder("sender").build().unwrap();
    let registry = registry();
    let (target_received_tx, _target_received_rx) = mpsc::channel();
    let target = receiver
        .spawn(
            "target",
            Props::new(move || Target {
                received: target_received_tx,
            }),
        )
        .unwrap();
    let (first_terminated_tx, first_terminated_rx) = mpsc::channel();
    let first_watcher = sender
        .spawn(
            "first-watcher",
            Props::new(move || TerminationWatcher {
                terminated: first_terminated_tx,
            }),
        )
        .unwrap();
    let (second_terminated_tx, second_terminated_rx) = mpsc::channel();
    let second_watcher = sender
        .spawn(
            "second-watcher",
            Props::new(move || TerminationWatcher {
                terminated: second_terminated_tx,
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
    await_bidirectional_routes(&sender_remote, &receiver_remote);

    let (receiver_stats_tx, receiver_stats_rx) = mpsc::channel();
    let receiver_stats_probe = receiver_remote
        .system()
        .spawn(
            "receiver-watch-stats",
            Props::new(move || Probe {
                sender: receiver_stats_tx,
            }),
        )
        .unwrap();
    let (sender_stats_tx, sender_stats_rx) = mpsc::channel();
    let sender_stats_probe = sender_remote
        .system()
        .spawn(
            "sender-watch-stats",
            Props::new(move || Probe {
                sender: sender_stats_tx,
            }),
        )
        .unwrap();
    let remote_target = sender_remote
        .resolve::<Ping>(remote_path_for(
            target.path().as_str(),
            receiver_remote.settings(),
        ))
        .unwrap();
    let watchee = remote_target.recipient().clone();
    let first_watcher_wire = sender_remote
        .provider()
        .local_actor_ref_to_wire_data(&first_watcher)
        .unwrap();
    let second_watcher_wire = sender_remote
        .provider()
        .local_actor_ref_to_wire_data(&second_watcher)
        .unwrap();

    sender_remote
        .watch_remote(first_watcher.clone(), &remote_target)
        .unwrap();
    sender_remote
        .watch_remote(second_watcher.clone(), &remote_target)
        .unwrap();
    sender_observer
        .wait_for(Duration::from_secs(1), |effect| {
            matches!(
                effect,
                RemoteDeathWatchEffect::SendWatchRemote(WatchRemote {
                    watchee: effect_watchee,
                    watcher: effect_watcher,
                }) if effect_watchee == &watchee && effect_watcher == &first_watcher_wire
            )
        })
        .expect("sender should send remote watch for first local watcher");
    sender_observer
        .wait_for(Duration::from_secs(1), |effect| {
            matches!(
                effect,
                RemoteDeathWatchEffect::SendWatchRemote(WatchRemote {
                    watchee: effect_watchee,
                    watcher: effect_watcher,
                }) if effect_watchee == &watchee && effect_watcher == &second_watcher_wire
            )
        })
        .expect("sender should send remote watch for second local watcher");
    let receiver_stats = wait_for_watch_stats(
        receiver_remote.death_watch(),
        &receiver_stats_probe,
        &receiver_stats_rx,
        |stats| stats.inbound_watching == 2,
        "receiver inbound watch registrations for two local watchers",
    );
    assert_eq!(receiver_stats.inbound_watching, 2);

    sender_remote
        .unwatch_remote(&first_watcher, &remote_target)
        .unwrap();
    sender_observer
        .wait_for(Duration::from_secs(1), |effect| {
            matches!(
                effect,
                RemoteDeathWatchEffect::SendUnwatchRemote(UnwatchRemote {
                    watchee: effect_watchee,
                    watcher: effect_watcher,
                }) if effect_watchee == &watchee && effect_watcher == &first_watcher_wire
            )
        })
        .expect("sender should send remote unwatch for first watcher");
    let sender_stats = wait_for_watch_stats(
        sender_remote.death_watch(),
        &sender_stats_probe,
        &sender_stats_rx,
        |stats| stats.watching == 1 && stats.watched_addresses == 1,
        "sender keeps second remote watch after first unwatch",
    );
    assert_eq!(sender_stats.watching, 1);
    assert_eq!(sender_stats.watched_addresses, 1);
    let receiver_stats = wait_for_watch_stats(
        receiver_remote.death_watch(),
        &receiver_stats_probe,
        &receiver_stats_rx,
        |stats| stats.inbound_watching == 1,
        "receiver keeps second inbound watch after first unwatch",
    );
    assert_eq!(receiver_stats.inbound_watching, 1);

    receiver_remote
        .death_watch()
        .tell(RemoteDeathWatchCommand::LocalWatcheeTerminated {
            watchee: watchee.clone(),
            existence_confirmed: true,
        })
        .unwrap();
    sender_observer
        .wait_for(Duration::from_secs(1), |effect| {
            matches!(
                effect,
                RemoteDeathWatchEffect::RemoteTerminated(RemoteTerminated {
                    watchee: effect_watchee,
                    existence_confirmed: true,
                }) if effect_watchee == &watchee
            )
        })
        .expect("sender should observe remote terminated for remaining watch");
    assert_eq!(
        second_terminated_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap(),
        remote_target.path().clone()
    );
    assert!(
        first_terminated_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err()
    );
    let sender_stats = wait_for_watch_stats(
        sender_remote.death_watch(),
        &sender_stats_probe,
        &sender_stats_rx,
        |stats| stats.watching == 0 && stats.watched_addresses == 0,
        "sender watch state cleanup after remaining watcher termination",
    );
    assert_eq!(sender_stats.watching, 0);
    assert_eq!(sender_stats.watched_addresses, 0);
    let receiver_stats = wait_for_watch_stats(
        receiver_remote.death_watch(),
        &receiver_stats_probe,
        &receiver_stats_rx,
        |stats| stats.inbound_watching == 0,
        "receiver inbound watch cleanup after local termination",
    );
    assert_eq!(receiver_stats.inbound_watching, 0);

    drop(registration);
    let sender_report = sender_remote.shutdown().unwrap();
    assert_eq!(sender_report.accepted_associations, 0);
    let receiver_report = receiver_remote.shutdown().unwrap();
    assert_eq!(receiver_report.accepted_associations, 1);
}

#[test]
fn tcp_remote_actor_system_routes_address_terminated_to_remote_death_watch() {
    let receiver = ActorSystem::builder("receiver").build().unwrap();
    let sender = ActorSystem::builder("sender").build().unwrap();
    let registry = registry();
    let (sender_received_tx, _sender_received_rx) = mpsc::channel();
    let (terminated_tx, terminated_rx) = mpsc::channel();
    let sender_target = sender
        .spawn(
            "target",
            Props::new(move || Target {
                received: sender_received_tx,
            }),
        )
        .unwrap();
    let receiver_watcher = receiver
        .spawn(
            "watcher",
            Props::new(move || TerminationWatcher {
                terminated: terminated_tx,
            }),
        )
        .unwrap();
    let receiver_observer = Arc::new(RecordingObserver::default());
    let sender_remote = TcpRemoteActorSystem::<Ping>::bind(
        sender.clone(),
        registry.clone(),
        RemoteSettings::new("127.0.0.1", 0),
        22,
    )
    .unwrap();
    let receiver_remote = TcpRemoteActorSystem::<Ping>::bind_with_observer(
        receiver.clone(),
        registry.clone(),
        RemoteSettings::new("127.0.0.1", 0),
        11,
        receiver_observer.clone() as Arc<dyn RemoteDeathWatchEffectObserver>,
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
    await_bidirectional_routes(&sender_remote, &receiver_remote);

    let (receiver_stats_tx, receiver_stats_rx) = mpsc::channel();
    let receiver_stats_probe = receiver_remote
        .system()
        .spawn(
            "receiver-watch-stats",
            Props::new(move || Probe {
                sender: receiver_stats_tx,
            }),
        )
        .unwrap();
    let remote_target = receiver_remote
        .resolve::<Ping>(remote_path_for_system(
            sender_target.path().as_str(),
            "sender",
            sender_remote.settings(),
        ))
        .unwrap();
    receiver_remote
        .watch_remote(receiver_watcher.clone(), &remote_target)
        .unwrap();
    receiver_observer
        .wait_for(Duration::from_secs(1), |effect| {
            matches!(effect, RemoteDeathWatchEffect::SendWatchRemote(_))
        })
        .expect("receiver should register sender watch before address termination");

    let recipient = ActorRefWireData::new(remote_path_for(
        "kairo://receiver/system/remote-watch",
        receiver_remote.settings(),
    ))
    .unwrap();
    let sender_watcher = ActorRefWireData::new(remote_path_for_system(
        "kairo://sender/system/remote-watch",
        "sender",
        sender_remote.settings(),
    ))
    .unwrap();
    let sender_address = format!(
        "kairo://sender@{}:{}",
        sender_remote.settings().canonical_hostname,
        sender_remote.settings().canonical_port
    );
    sender_remote
        .association_cache()
        .send(RemoteEnvelope::new(
            recipient,
            Some(sender_watcher),
            registry
                .serialize(&AddressTerminated {
                    address: sender_address.clone(),
                    uid: Some(22),
                })
                .unwrap(),
        ))
        .unwrap();

    receiver_observer
        .wait_for(Duration::from_secs(1), |effect| {
            matches!(
                effect,
                RemoteDeathWatchEffect::AddressTerminated(AddressTerminated {
                    address,
                    uid: Some(22),
                }) if address == &sender_address
            )
        })
        .expect("receiver should route address-terminated frame to remote death-watch");
    assert_eq!(
        terminated_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        remote_target.path().clone()
    );
    let receiver_stats = wait_for_watch_stats(
        receiver_remote.death_watch(),
        &receiver_stats_probe,
        &receiver_stats_rx,
        |stats| stats.watching == 0 && stats.watched_addresses == 0,
        "receiver watch state cleanup after address terminated",
    );
    assert_eq!(receiver_stats.watching, 0);
    assert_eq!(receiver_stats.watched_addresses, 0);

    drop(registration);
    let sender_report = sender_remote.shutdown().unwrap();
    assert_eq!(sender_report.accepted_associations, 0);
    let receiver_report = receiver_remote.shutdown().unwrap();
    assert_eq!(receiver_report.accepted_associations, 1);
}

#[test]
fn tcp_remote_actor_system_shutdown_clears_late_routes_registered_during_shutdown() {
    let system = ActorSystem::builder("remote-late-route-shutdown")
        .build()
        .unwrap();
    let remote = TcpRemoteActorSystem::<Ping>::bind(
        system,
        registry(),
        RemoteSettings::new("127.0.0.1", 0),
        11,
    )
    .unwrap();
    let cache = remote.association_cache().clone();
    let initial_address =
        RemoteAssociationAddress::new("kairo", "initial", "127.0.0.1", Some(2552)).unwrap();
    let late_address =
        RemoteAssociationAddress::new("kairo", "late", "127.0.0.1", Some(2553)).unwrap();
    cache.insert_route(
        initial_address,
        Arc::new(LateRouteOnClose {
            cache: cache.clone(),
            late_address,
        }),
    );
    assert_eq!(cache.route_count(), 1);

    let report = remote.shutdown().unwrap();

    assert_eq!(report.accepted_associations, 0);
    assert_eq!(cache.route_count(), 0);
}

#[test]
fn tcp_remote_actor_system_coordinated_shutdown_stops_runtime_once() {
    let receiver = ActorSystem::builder("receiver").build().unwrap();
    let sender = ActorSystem::builder("sender").build().unwrap();
    let registry = registry();
    let (received_tx, received_rx) = mpsc::channel();
    let target = receiver
        .spawn(
            "target",
            Props::new(move || Target {
                received: received_tx,
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
        sender.clone(),
        registry,
        RemoteSettings::new("127.0.0.1", 0),
        22,
    )
    .unwrap();
    let remote_target = sender_remote
        .resolve::<Ping>(remote_path_for(
            target.path().as_str(),
            receiver_remote.settings(),
        ))
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
    assert_eq!(
        sender_remote
            .outbound_pipelines
            .lock()
            .expect("outbound pipelines lock poisoned")
            .len(),
        1
    );
    assert_eq!(
        sender_remote
            .outbound_readers
            .lock()
            .expect("outbound readers lock poisoned")
            .len(),
        1
    );
    remote_target.tell(Ping { value: 17 }).unwrap();
    assert_eq!(
        received_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        17
    );

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
    assert!(
        sender_remote
            .outbound_pipelines
            .lock()
            .expect("outbound pipelines lock poisoned")
            .is_empty(),
        "coordinated shutdown should release owned outbound pipelines"
    );
    assert!(
        sender_remote
            .outbound_readers
            .lock()
            .expect("outbound readers lock poisoned")
            .is_empty(),
        "coordinated shutdown should release owned outbound readers"
    );
    assert!(matches!(
        registration
            .pipeline()
            .association()
            .lock()
            .expect("association mutex poisoned")
            .state(),
        AssociationState::Closed { reason }
            if reason == "tcp remote actor system shutdown"
    ));
    let error = remote_target
        .tell(Ping { value: 18 })
        .expect_err("remote ref should reject sends after coordinated shutdown clears routes");
    assert!(
        error.reason().contains("no remote association route"),
        "{}",
        error.reason()
    );
    assert!(
        received_rx.recv_timeout(Duration::from_millis(50)).is_err(),
        "receiver should not get sends from a cloned remote ref after shutdown"
    );
    let second_report = sender_remote.shutdown().unwrap();
    assert_eq!(second_report.accepted_associations, 0);

    drop(registration);
    receiver_remote.shutdown().unwrap();
    sender.terminate(Duration::from_secs(1)).unwrap();
    receiver.terminate(Duration::from_secs(1)).unwrap();
}
