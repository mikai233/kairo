use std::collections::HashSet;
use std::marker::PhantomData;
use std::net::TcpListener;
use std::ops::Deref;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use kairo_actor::{ActorError, ActorPath, ActorRef, ActorSystem};
use kairo_serialization::{ActorRefWireData, Manifest, Registry, RemoteMessage};

use crate::reliable_runtime::{
    IgnoreReliableSystemDeliveryFailures, ReliableSystemDeliveryRuntime,
    ReliableSystemInboundHandler, ReliableSystemRuntimeActor, ReliableSystemRuntimeCommand,
    is_reliable_protocol_manifest, reliable_delivery_actor_name,
};
use crate::{
    ActorSystemRemoteInboundRegistry, AssociationOutboundPipeline, ReliableSystemDeliveryObserver,
    ReliableSystemDeliverySettings, ReliableSystemDeliveryStats, RemoteActorRef,
    RemoteActorRefProvider, RemoteActorRefResolver, RemoteAssociationAddress,
    RemoteAssociationCache, RemoteAssociationRegistry, RemoteAssociationRouteInstaller,
    RemoteAssociationRouteRegistration, RemoteDeathWatchCommand, RemoteDeathWatchEffect,
    RemoteDeathWatchEffectObserver, RemoteDeathWatchOutboundSink, RemoteEnvelopeHandler,
    RemoteError, RemoteLaneClassifier, RemoteOutbound, RemoteOutboundQueueSettings, RemoteSettings,
    RemoteStreamId, ResolvedActorRef, Result, TcpAssociationAssemblySettings, TcpAssociationDialer,
    TcpAssociationListener, TcpAssociationListenerHandle, TcpAssociationListenerReport,
    TcpAssociationReaderHandle, TcpHandshakeReadSettings, UnwatchRemote, WatchRemote,
    is_remote_death_watch_manifest,
};

mod reconnect;

pub use reconnect::TcpRemoteReconnectSettings;
use reconnect::{ReconnectResources, TcpRemoteReconnectManager};

const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);
const TCP_REMOTE_SHUTDOWN_REASON: &str = "tcp remote actor system shutdown";

struct RuntimeSystemActorRefs<'a> {
    reliable_delivery: &'a ActorRef<ReliableSystemRuntimeCommand>,
    death_watch: &'a ActorRef<RemoteDeathWatchCommand>,
}

struct RuntimeTransportRefs<'a> {
    association_cache: &'a RemoteAssociationCache,
    reconnect: &'a Arc<TcpRemoteReconnectManager>,
    listener: &'a Arc<Mutex<Option<TcpAssociationListenerHandle>>>,
    outbound_readers: &'a Arc<Mutex<Vec<TcpAssociationReaderHandle>>>,
    outbound_pipelines: &'a Arc<Mutex<Vec<AssociationOutboundPipeline>>>,
}

/// One non-generic TCP remoting lifecycle for an ActorSystem.
pub struct TcpRemoteActorRuntime {
    system: ActorSystem,
    registry: Arc<Registry>,
    settings: RemoteSettings,
    outbound_queue_settings: RemoteOutboundQueueSettings,
    association_cache: RemoteAssociationCache,
    association_registry: RemoteAssociationRegistry,
    outbound: Arc<dyn RemoteOutbound>,
    provider: RemoteActorRefProvider,
    dialer: TcpAssociationDialer,
    reconnect_settings: TcpRemoteReconnectSettings,
    reconnect: Arc<TcpRemoteReconnectManager>,
    reliable_delivery: Arc<ReliableSystemDeliveryRuntime>,
    reliable_delivery_actor: ActorRef<ReliableSystemRuntimeCommand>,
    outbound_readers: Arc<Mutex<Vec<TcpAssociationReaderHandle>>>,
    outbound_pipelines: Arc<Mutex<Vec<AssociationOutboundPipeline>>>,
    death_watch: ActorRef<RemoteDeathWatchCommand>,
    listener: Arc<Mutex<Option<TcpAssociationListenerHandle>>>,
}

type InboundProtocolRegistration = Box<
    dyn FnOnce(&TcpRemoteActorRuntimeContext, &mut ActorSystemRemoteInboundRegistry) -> Result<()>
        + Send,
>;

/// Effective bind-time resources exposed to protocol handler factories.
#[derive(Clone)]
pub struct TcpRemoteActorRuntimeContext {
    system: ActorSystem,
    registry: Arc<Registry>,
    settings: RemoteSettings,
    local_system_uid: u64,
    association_cache: RemoteAssociationCache,
    outbound: Arc<dyn RemoteOutbound>,
}

impl TcpRemoteActorRuntimeContext {
    pub fn system(&self) -> &ActorSystem {
        &self.system
    }

    pub fn registry(&self) -> &Arc<Registry> {
        &self.registry
    }

    pub fn settings(&self) -> &RemoteSettings {
        &self.settings
    }

    pub fn local_system_uid(&self) -> u64 {
        self.local_system_uid
    }

    pub fn association_cache(&self) -> &RemoteAssociationCache {
        &self.association_cache
    }

    pub fn outbound(&self) -> &Arc<dyn RemoteOutbound> {
        &self.outbound
    }
}

/// Pre-bind protocol registration for [`TcpRemoteActorRuntime`].
pub struct TcpRemoteActorRuntimeBuilder {
    system: ActorSystem,
    registry: Arc<Registry>,
    settings: RemoteSettings,
    local_system_uid: u64,
    observer: Arc<dyn RemoteDeathWatchEffectObserver>,
    manifests: HashSet<String>,
    lane_classifier: RemoteLaneClassifier,
    outbound_queue_settings: RemoteOutboundQueueSettings,
    handshake_read_settings: TcpHandshakeReadSettings,
    association_assembly_settings: TcpAssociationAssemblySettings,
    reconnect_settings: TcpRemoteReconnectSettings,
    reliable_delivery_settings: ReliableSystemDeliverySettings,
    reliable_delivery_observer: Arc<dyn ReliableSystemDeliveryObserver>,
    reliable_manifests: HashSet<String>,
    protocols: Vec<InboundProtocolRegistration>,
}

/// Compatibility facade for the former single-protocol runtime.
///
/// New composed runtimes should use [`TcpRemoteActorRuntime::builder`] and
/// register every business protocol before bind.
pub struct TcpRemoteActorSystem<M> {
    runtime: TcpRemoteActorRuntime,
    _message: PhantomData<fn(M)>,
}

struct ActorSystemRemoteDeathWatchObserver {
    system: ActorSystem,
    inner: Arc<dyn RemoteDeathWatchEffectObserver>,
}

impl RemoteDeathWatchEffectObserver for ActorSystemRemoteDeathWatchObserver {
    fn observe(&self, effect: &RemoteDeathWatchEffect) -> Result<()> {
        self.inner.observe(effect)?;
        if let RemoteDeathWatchEffect::RemoteTerminated(message) = effect {
            self.system
                .notify_watched_path_terminated(&ActorPath::new(message.watchee.path()));
        } else if let RemoteDeathWatchEffect::AddressTerminated(message) = effect {
            self.system
                .notify_watched_address_terminated(&message.address);
        }
        Ok(())
    }
}

impl<M> TcpRemoteActorSystem<M>
where
    M: RemoteMessage,
{
    pub fn bind(
        system: ActorSystem,
        registry: Arc<Registry>,
        settings: RemoteSettings,
        local_system_uid: u64,
    ) -> Result<Self> {
        Self::bind_with_observer(
            system,
            registry,
            settings,
            local_system_uid,
            Arc::new(crate::IgnoreRemoteDeathWatchEffects),
        )
    }

    pub fn bind_with_observer(
        system: ActorSystem,
        registry: Arc<Registry>,
        settings: RemoteSettings,
        local_system_uid: u64,
        observer: Arc<dyn RemoteDeathWatchEffectObserver>,
    ) -> Result<Self> {
        let mut builder =
            TcpRemoteActorRuntime::builder(system, registry, settings, local_system_uid)
                .with_observer(observer);
        builder.register::<M>()?;
        Ok(Self {
            runtime: builder.bind()?,
            _message: PhantomData,
        })
    }

    pub fn shutdown(self) -> Result<TcpAssociationListenerReport> {
        self.runtime.shutdown()
    }

    pub fn shutdown_with_timeout(self, timeout: Duration) -> Result<TcpAssociationListenerReport> {
        self.runtime.shutdown_with_timeout(timeout)
    }
}

impl<M> Deref for TcpRemoteActorSystem<M> {
    type Target = TcpRemoteActorRuntime;

    fn deref(&self) -> &Self::Target {
        &self.runtime
    }
}

impl TcpRemoteActorRuntime {
    pub fn builder(
        system: ActorSystem,
        registry: Arc<Registry>,
        settings: RemoteSettings,
        local_system_uid: u64,
    ) -> TcpRemoteActorRuntimeBuilder {
        let mut lane_classifier = RemoteLaneClassifier::default();
        lane_classifier.add_control_manifest(crate::ReliableSystemEnvelope::MANIFEST);
        lane_classifier.add_control_manifest(crate::ReliableSystemAck::MANIFEST);
        lane_classifier.add_control_manifest(crate::ReliableSystemNack::MANIFEST);
        TcpRemoteActorRuntimeBuilder {
            system,
            registry,
            settings,
            local_system_uid,
            observer: Arc::new(crate::IgnoreRemoteDeathWatchEffects),
            manifests: HashSet::new(),
            lane_classifier,
            outbound_queue_settings: RemoteOutboundQueueSettings::default(),
            handshake_read_settings: TcpHandshakeReadSettings::default(),
            association_assembly_settings: TcpAssociationAssemblySettings::default(),
            reconnect_settings: TcpRemoteReconnectSettings::default(),
            reliable_delivery_settings: ReliableSystemDeliverySettings::default(),
            reliable_delivery_observer: Arc::new(IgnoreReliableSystemDeliveryFailures),
            reliable_manifests: default_reliable_manifests(),
            protocols: Vec::new(),
        }
    }
}

impl TcpRemoteActorRuntimeBuilder {
    pub fn with_observer(mut self, observer: Arc<dyn RemoteDeathWatchEffectObserver>) -> Self {
        self.observer = observer;
        self
    }

    pub fn with_outbound_queue_settings(mut self, settings: RemoteOutboundQueueSettings) -> Self {
        self.outbound_queue_settings = settings;
        self
    }

    pub fn with_handshake_read_settings(mut self, settings: TcpHandshakeReadSettings) -> Self {
        self.handshake_read_settings = settings;
        self
    }

    pub fn with_association_assembly_settings(
        mut self,
        settings: TcpAssociationAssemblySettings,
    ) -> Self {
        self.association_assembly_settings = settings;
        self
    }

    pub fn with_reconnect_settings(mut self, settings: TcpRemoteReconnectSettings) -> Self {
        self.reconnect_settings = settings;
        self
    }

    pub fn with_reliable_system_delivery_settings(
        mut self,
        settings: ReliableSystemDeliverySettings,
    ) -> Self {
        self.reliable_delivery_settings = settings;
        self
    }

    pub fn with_reliable_system_delivery_observer(
        mut self,
        observer: Arc<dyn ReliableSystemDeliveryObserver>,
    ) -> Self {
        self.reliable_delivery_observer = observer;
        self
    }

    pub fn register<M>(&mut self) -> Result<&mut Self>
    where
        M: RemoteMessage,
    {
        Manifest::try_new(M::MANIFEST)?;
        if is_remote_death_watch_manifest(M::MANIFEST)
            || is_reliable_protocol_manifest(M::MANIFEST)
            || !self.manifests.insert(M::MANIFEST.to_string())
        {
            return Err(RemoteError::DuplicateProtocolManifest(
                M::MANIFEST.to_string(),
            ));
        }
        self.protocols
            .push(Box::new(|_, inbound| inbound.register::<M>().map(|_| ())));
        Ok(self)
    }

    pub fn register_control_handler<F, H>(
        &mut self,
        manifests: &[&'static str],
        factory: F,
    ) -> Result<&mut Self>
    where
        F: FnOnce(&TcpRemoteActorRuntimeContext) -> Result<H> + Send + 'static,
        H: RemoteEnvelopeHandler,
    {
        self.register_control_handler_with_reliability(manifests, false, factory)
    }

    pub fn register_reliable_control_handler<F, H>(
        &mut self,
        manifests: &[&'static str],
        factory: F,
    ) -> Result<&mut Self>
    where
        F: FnOnce(&TcpRemoteActorRuntimeContext) -> Result<H> + Send + 'static,
        H: RemoteEnvelopeHandler,
    {
        self.register_control_handler_with_reliability(manifests, true, factory)
    }

    fn register_control_handler_with_reliability<F, H>(
        &mut self,
        manifests: &[&'static str],
        reliable: bool,
        factory: F,
    ) -> Result<&mut Self>
    where
        F: FnOnce(&TcpRemoteActorRuntimeContext) -> Result<H> + Send + 'static,
        H: RemoteEnvelopeHandler,
    {
        let mut pending = HashSet::new();
        for manifest in manifests {
            Manifest::try_new(*manifest)?;
            if is_remote_death_watch_manifest(manifest)
                || is_reliable_protocol_manifest(manifest)
                || self.manifests.contains(*manifest)
                || !pending.insert((*manifest).to_string())
            {
                return Err(RemoteError::DuplicateProtocolManifest(
                    (*manifest).to_string(),
                ));
            }
        }

        let manifests = manifests
            .iter()
            .map(|manifest| (*manifest).to_string())
            .collect::<Vec<_>>();
        for manifest in &manifests {
            self.manifests.insert(manifest.clone());
            self.lane_classifier.add_control_manifest(manifest.clone());
            if reliable {
                self.reliable_manifests.insert(manifest.clone());
            }
        }
        self.protocols.push(Box::new(move |context, inbound| {
            let handler = Arc::new(factory(context)?) as Arc<dyn RemoteEnvelopeHandler>;
            inbound
                .router_mut()
                .register_handler(&manifests, RemoteStreamId::Control, handler)
        }));
        Ok(self)
    }

    pub fn bind(self) -> Result<TcpRemoteActorRuntime> {
        let Self {
            system,
            registry,
            settings,
            local_system_uid,
            observer,
            lane_classifier,
            outbound_queue_settings,
            handshake_read_settings,
            association_assembly_settings,
            reconnect_settings,
            reliable_delivery_settings,
            reliable_delivery_observer,
            reliable_manifests,
            protocols,
            ..
        } = self;
        let listener = TcpListener::bind((
            settings.canonical_hostname.as_str(),
            settings.canonical_port,
        ))
        .map_err(|error| RemoteError::Inbound(format!("tcp bind failed: {error}")))?;
        let local_addr = listener
            .local_addr()
            .map_err(|error| RemoteError::Inbound(format!("tcp local address failed: {error}")))?;
        let effective_settings = RemoteSettings {
            canonical_hostname: settings.canonical_hostname.clone(),
            canonical_port: if settings.canonical_port == 0 {
                local_addr.port()
            } else {
                settings.canonical_port
            },
            connect_timeout: settings.connect_timeout,
        };

        let association_cache = RemoteAssociationCache::new();
        let association_registry = RemoteAssociationRegistry::new();
        let local_address = local_association_address(&system, &effective_settings)?;
        let reliable_delivery = Arc::new(ReliableSystemDeliveryRuntime::new(
            local_address.clone(),
            local_system_uid,
            registry.clone(),
            association_cache.clone(),
            association_registry.clone(),
            reliable_manifests,
            reliable_delivery_settings,
            reliable_delivery_observer,
        ));
        let outbound = reliable_delivery.clone() as Arc<dyn RemoteOutbound>;
        let local_watcher = local_watcher_for(&system, &effective_settings)?;
        let observer = Arc::new(ActorSystemRemoteDeathWatchObserver {
            system: system.clone(),
            inner: observer,
        });
        let death_watch_sink = Arc::new(RemoteDeathWatchOutboundSink::with_local_watcher(
            registry.clone(),
            outbound.clone(),
            observer,
            local_watcher,
        ));
        let death_watch = system
            .spawn_system(
                "remote-watch",
                crate::RemoteDeathWatchActor::props(death_watch_sink),
            )
            .map_err(|error| RemoteError::Inbound(error.to_string()))?;

        let mut inbound = ActorSystemRemoteInboundRegistry::with_remote_settings(
            system.clone(),
            registry.clone(),
            death_watch.clone(),
            local_system_uid,
            effective_settings.clone(),
        );
        let context = TcpRemoteActorRuntimeContext {
            system: system.clone(),
            registry: registry.clone(),
            settings: effective_settings.clone(),
            local_system_uid,
            association_cache: association_cache.clone(),
            outbound: outbound.clone(),
        };
        for protocol in protocols {
            if let Err(error) = protocol(&context, &mut inbound) {
                system.stop(&death_watch);
                death_watch.wait_for_stop(DEFAULT_SHUTDOWN_TIMEOUT);
                return Err(error);
            }
        }
        let inbound = Arc::new(inbound);
        let reliable_delivery_actor = system
            .spawn_system(
                reliable_delivery_actor_name(),
                ReliableSystemRuntimeActor::props(reliable_delivery.clone()),
            )
            .map_err(|error| RemoteError::Inbound(error.to_string()))?;
        let installer = RemoteAssociationRouteInstaller::new(association_cache.clone())
            .with_association_registry(association_registry.clone())
            .with_classifier(lane_classifier)
            .with_outbound_queue_settings(outbound_queue_settings);
        let base_inbound = inbound.clone() as Arc<dyn crate::RemoteFrameHandler>;
        let listener_delivery = reliable_delivery.clone();
        let listener_inbound = base_inbound.clone();
        let listener = TcpAssociationListener::from_listener(listener, base_inbound)
            .with_handler_factory(Arc::new(
                move |identity: Option<&crate::TcpAssociationIdentity>| match identity {
                    Some(identity) => Arc::new(ReliableSystemInboundHandler::new(
                        listener_delivery.clone(),
                        listener_inbound.clone(),
                        identity.address().clone(),
                    )) as Arc<dyn crate::RemoteFrameHandler>,
                    None => listener_inbound.clone(),
                },
            ))
            .with_local_identity(local_address.clone(), local_system_uid)
            .with_handshake_read_settings(handshake_read_settings)
            .with_association_assembly_settings(association_assembly_settings)
            .with_association_registry(association_registry.clone())
            .with_route_installer(installer.clone())
            .spawn_accept_loop()?;
        let dialer = TcpAssociationDialer::new(installer)
            .with_local_identity(local_address, local_system_uid)
            .with_handshake_response_required()
            .with_handshake_read_settings(handshake_read_settings)
            .with_connect_timeout(effective_settings.connect_timeout_or_default());
        let outbound_readers = Arc::new(Mutex::new(Vec::new()));
        let outbound_pipelines = Arc::new(Mutex::new(Vec::new()));
        let reconnect = Arc::new(TcpRemoteReconnectManager::new(
            reconnect_settings,
            ReconnectResources::new(
                dialer.clone(),
                association_cache.clone(),
                association_registry.clone(),
                inbound.clone(),
                reliable_delivery.clone(),
                Arc::clone(&outbound_readers),
                Arc::clone(&outbound_pipelines),
            ),
        ));
        let provider = RemoteActorRefProvider::with_actor_system(
            system.clone(),
            effective_settings.clone(),
            registry.clone(),
            outbound.clone(),
        );

        Ok(TcpRemoteActorRuntime {
            system,
            registry,
            settings: effective_settings,
            outbound_queue_settings,
            association_cache,
            association_registry,
            outbound,
            provider,
            dialer,
            reconnect_settings,
            reconnect,
            reliable_delivery,
            reliable_delivery_actor,
            outbound_readers,
            outbound_pipelines,
            death_watch,
            listener: Arc::new(Mutex::new(Some(listener))),
        })
    }
}

impl TcpRemoteActorRuntime {
    pub fn system(&self) -> &ActorSystem {
        &self.system
    }

    pub fn registry(&self) -> &Arc<Registry> {
        &self.registry
    }

    pub fn settings(&self) -> &RemoteSettings {
        &self.settings
    }

    pub fn outbound_queue_settings(&self) -> RemoteOutboundQueueSettings {
        self.outbound_queue_settings
    }

    pub fn association_cache(&self) -> &RemoteAssociationCache {
        &self.association_cache
    }

    pub fn outbound(&self) -> &Arc<dyn RemoteOutbound> {
        &self.outbound
    }

    pub fn association_registry(&self) -> &RemoteAssociationRegistry {
        &self.association_registry
    }

    pub fn reliable_system_delivery_stats(&self) -> ReliableSystemDeliveryStats {
        self.reliable_delivery.stats()
    }

    pub fn provider(&self) -> &RemoteActorRefProvider {
        &self.provider
    }

    pub fn dialer(&self) -> &TcpAssociationDialer {
        &self.dialer
    }

    pub fn death_watch(&self) -> &ActorRef<RemoteDeathWatchCommand> {
        &self.death_watch
    }

    pub fn dial(
        &self,
        address: RemoteAssociationAddress,
    ) -> Result<RemoteAssociationRouteRegistration> {
        self.reconnect.dial(address)
    }

    pub fn reconnect_settings(&self) -> TcpRemoteReconnectSettings {
        self.reconnect_settings
    }

    pub fn managed_reconnect_peer_count(&self) -> usize {
        self.reconnect.managed_peer_count()
    }

    pub fn disconnect(&self, address: &RemoteAssociationAddress, reason: &str) -> Result<bool> {
        let removed_intent = self.reconnect.disconnect(address);
        let Some(closed) = self
            .association_cache
            .remove_route_and_close(address, reason)
        else {
            return Ok(removed_intent);
        };
        closed?;
        Ok(true)
    }

    pub fn resolve<N>(&self, path: impl Into<String>) -> Result<RemoteActorRef<N>>
    where
        N: RemoteMessage,
    {
        self.provider.resolve(path)
    }

    pub fn resolve_actor_ref<N>(&self, path: impl Into<String>) -> Result<ResolvedActorRef<N>>
    where
        N: RemoteMessage,
    {
        self.provider.resolve_actor_ref(path)
    }

    pub fn resolver<N>(&self) -> RemoteActorRefResolver<N>
    where
        N: RemoteMessage,
    {
        self.provider.resolver()
    }

    pub fn watch_remote<W, N>(
        &self,
        watcher: ActorRef<W>,
        watchee: &RemoteActorRef<N>,
    ) -> Result<()>
    where
        W: Send + 'static,
        N: RemoteMessage,
    {
        let watcher_wire = self.provider.local_actor_ref_to_wire_data(&watcher)?;
        self.system
            .watch_path(watcher.clone(), watchee.path().clone())
            .map_err(|error| RemoteError::Inbound(error.to_string()))?;
        if let Err(error) = self
            .death_watch
            .tell(RemoteDeathWatchCommand::Watch(WatchRemote {
                watchee: watchee.recipient().clone(),
                watcher: watcher_wire,
            }))
        {
            self.system.unwatch_path(watcher.path(), watchee.path());
            return Err(RemoteError::Inbound(format!(
                "failed to register remote watch: {}",
                error.reason()
            )));
        }
        Ok(())
    }

    pub fn unwatch_remote<W, N>(
        &self,
        watcher: &ActorRef<W>,
        watchee: &RemoteActorRef<N>,
    ) -> Result<()>
    where
        W: Send + 'static,
        N: RemoteMessage,
    {
        let watcher_wire = self.provider.local_actor_ref_to_wire_data(watcher)?;
        self.system.unwatch_path(watcher.path(), watchee.path());
        self.death_watch
            .tell(RemoteDeathWatchCommand::Unwatch(UnwatchRemote {
                watchee: watchee.recipient().clone(),
                watcher: watcher_wire,
            }))
            .map_err(|error| {
                RemoteError::Inbound(format!(
                    "failed to unregister remote watch: {}",
                    error.reason()
                ))
            })
    }

    pub fn shutdown(self) -> Result<TcpAssociationListenerReport> {
        self.shutdown_with_timeout(DEFAULT_SHUTDOWN_TIMEOUT)
    }

    pub fn shutdown_with_timeout(self, timeout: Duration) -> Result<TcpAssociationListenerReport> {
        shutdown_runtime(
            &self.system,
            RuntimeSystemActorRefs {
                reliable_delivery: &self.reliable_delivery_actor,
                death_watch: &self.death_watch,
            },
            RuntimeTransportRefs {
                association_cache: &self.association_cache,
                reconnect: &self.reconnect,
                listener: &self.listener,
                outbound_readers: &self.outbound_readers,
                outbound_pipelines: &self.outbound_pipelines,
            },
            timeout,
        )
    }

    pub fn register_coordinated_shutdown(
        &self,
        phase: impl AsRef<str>,
        task_name: impl Into<String>,
        timeout: Duration,
    ) -> Result<()> {
        let shutdown = self.system.coordinated_shutdown();
        let system = self.system.clone();
        let death_watch = self.death_watch.clone();
        let reliable_delivery_actor = self.reliable_delivery_actor.clone();
        let association_cache = self.association_cache.clone();
        let reconnect = Arc::clone(&self.reconnect);
        let listener = Arc::clone(&self.listener);
        let outbound_readers = Arc::clone(&self.outbound_readers);
        let outbound_pipelines = Arc::clone(&self.outbound_pipelines);
        shutdown
            .add_task(phase, task_name, move || {
                shutdown_runtime(
                    &system,
                    RuntimeSystemActorRefs {
                        reliable_delivery: &reliable_delivery_actor,
                        death_watch: &death_watch,
                    },
                    RuntimeTransportRefs {
                        association_cache: &association_cache,
                        reconnect: &reconnect,
                        listener: &listener,
                        outbound_readers: &outbound_readers,
                        outbound_pipelines: &outbound_pipelines,
                    },
                    timeout,
                )
                .map(|_| ())
                .map_err(|error| ActorError::ShutdownTaskFailed(error.to_string()))
            })
            .map_err(|error| RemoteError::Inbound(error.to_string()))
    }
}

fn shutdown_runtime(
    system: &ActorSystem,
    system_actors: RuntimeSystemActorRefs<'_>,
    transport: RuntimeTransportRefs<'_>,
    timeout: Duration,
) -> Result<TcpAssociationListenerReport> {
    let reconnect_stopped = transport.reconnect.stop_until(Instant::now() + timeout);
    system.stop(system_actors.reliable_delivery);
    system.stop(system_actors.death_watch);
    let reliable_delivery_stopped = system_actors.reliable_delivery.wait_for_stop(timeout);
    let death_watch_stopped = system_actors.death_watch.wait_for_stop(timeout);
    for result in transport
        .association_cache
        .clear_routes_and_close(TCP_REMOTE_SHUTDOWN_REASON)
    {
        result?;
    }
    let listener = transport
        .listener
        .lock()
        .expect("tcp remote actor system listener lock poisoned")
        .take();

    let Some(listener) = listener else {
        if !reconnect_stopped || !reliable_delivery_stopped || !death_watch_stopped {
            return Err(RemoteError::Inbound(
                "remote reconnect worker or system actors did not stop during tcp shutdown"
                    .to_string(),
            ));
        }
        return Ok(empty_listener_report());
    };

    listener.stop();
    transport
        .outbound_pipelines
        .lock()
        .expect("tcp remote actor system outbound pipelines lock poisoned")
        .clear();
    let outbound_readers = transport
        .outbound_readers
        .lock()
        .expect("tcp remote actor system outbound readers lock poisoned")
        .drain(..)
        .collect::<Vec<_>>();
    for reader in outbound_readers {
        let _ = reader.join_after_stop_until(Instant::now());
    }
    let listener_report = listener.join();
    for result in transport
        .association_cache
        .clear_routes_and_close(TCP_REMOTE_SHUTDOWN_REASON)
    {
        result?;
    }

    if !reconnect_stopped || !reliable_delivery_stopped || !death_watch_stopped {
        return Err(RemoteError::Inbound(
            "remote reconnect worker or system actors did not stop during tcp shutdown".to_string(),
        ));
    }

    listener_report
}

fn default_reliable_manifests() -> HashSet<String> {
    [
        crate::WatchRemote::MANIFEST,
        crate::UnwatchRemote::MANIFEST,
        crate::RemoteTerminated::MANIFEST,
        crate::AddressTerminated::MANIFEST,
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

fn empty_listener_report() -> TcpAssociationListenerReport {
    TcpAssociationListenerReport {
        accepted_associations: 0,
        remote_identities: Vec::new(),
        read: Default::default(),
        supervision: Vec::new(),
    }
}

fn local_watcher_for(system: &ActorSystem, settings: &RemoteSettings) -> Result<ActorRefWireData> {
    ActorRefWireData::new(format!(
        "{}://{}@{}:{}/system/remote-watch",
        system.address().protocol(),
        system.name(),
        settings.canonical_hostname,
        settings.canonical_port
    ))
    .map_err(RemoteError::from)
}

fn local_association_address(
    system: &ActorSystem,
    settings: &RemoteSettings,
) -> Result<RemoteAssociationAddress> {
    RemoteAssociationAddress::new(
        system.address().protocol(),
        system.name(),
        settings.canonical_hostname.clone(),
        Some(settings.canonical_port),
    )
}

#[cfg(test)]
mod tests;
