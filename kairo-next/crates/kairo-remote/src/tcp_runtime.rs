use std::marker::PhantomData;
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use kairo_actor::{ActorRef, ActorSystem};
use kairo_serialization::{ActorRefWireData, Registry, RemoteMessage};

use crate::{
    ActorSystemRemoteInbound, RemoteActorRef, RemoteActorRefProvider, RemoteAssociationAddress,
    RemoteAssociationCache, RemoteAssociationRegistry, RemoteAssociationRouteInstaller,
    RemoteAssociationRouteRegistration, RemoteDeathWatchCommand, RemoteDeathWatchEffectObserver,
    RemoteDeathWatchOutboundSink, RemoteError, RemoteOutbound, RemoteSettings, ResolvedActorRef,
    Result, TcpAssociationDialer, TcpAssociationListener, TcpAssociationListenerHandle,
    TcpAssociationListenerReport, TcpAssociationReaderHandle, TcpAssociationStreamReader,
};

const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);

pub struct TcpRemoteActorSystem<M> {
    system: ActorSystem,
    registry: Arc<Registry>,
    settings: RemoteSettings,
    association_cache: RemoteAssociationCache,
    association_registry: RemoteAssociationRegistry,
    provider: RemoteActorRefProvider,
    dialer: TcpAssociationDialer,
    outbound_reader: TcpAssociationStreamReader,
    outbound_readers: Arc<Mutex<Vec<TcpAssociationReaderHandle>>>,
    death_watch: ActorRef<RemoteDeathWatchCommand>,
    listener: TcpAssociationListenerHandle,
    _message: PhantomData<fn(M)>,
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
        let listener = TcpListener::bind((
            settings.canonical_hostname.as_str(),
            settings.canonical_port,
        ))
        .map_err(|error| RemoteError::Inbound(format!("tcp bind failed: {error}")))?;
        let local_addr = listener
            .local_addr()
            .map_err(|error| RemoteError::Inbound(format!("tcp local address failed: {error}")))?;
        let effective_settings = RemoteSettings::new(
            settings.canonical_hostname.clone(),
            if settings.canonical_port == 0 {
                local_addr.port()
            } else {
                settings.canonical_port
            },
        );

        let association_cache = RemoteAssociationCache::new();
        let association_registry = RemoteAssociationRegistry::new();
        let outbound = Arc::new(association_cache.clone()) as Arc<dyn RemoteOutbound>;
        let local_watcher = local_watcher_for(&system, &effective_settings)?;
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

        let inbound = Arc::new(ActorSystemRemoteInbound::<M>::with_remote_settings(
            system.clone(),
            registry.clone(),
            death_watch.clone(),
            local_system_uid,
            effective_settings.clone(),
        ));
        let installer = RemoteAssociationRouteInstaller::new(association_cache.clone());
        let outbound_reader = TcpAssociationStreamReader::new(inbound.clone());
        let listener = TcpAssociationListener::from_listener(listener, inbound)
            .with_local_address(local_association_address(&system, &effective_settings)?)
            .with_association_registry(association_registry.clone())
            .with_route_installer(installer.clone())
            .spawn_accept_loop()?;
        let dialer = TcpAssociationDialer::new(installer)
            .with_local_identity(
                local_association_address(&system, &effective_settings)?,
                local_system_uid,
            )
            .with_connect_timeout(Duration::from_secs(1));
        let provider = RemoteActorRefProvider::with_actor_system(
            system.clone(),
            effective_settings.clone(),
            registry.clone(),
            outbound,
        );

        Ok(Self {
            system,
            registry,
            settings: effective_settings,
            association_cache,
            association_registry,
            provider,
            dialer,
            outbound_reader,
            outbound_readers: Arc::new(Mutex::new(Vec::new())),
            death_watch,
            listener,
            _message: PhantomData,
        })
    }

    pub fn system(&self) -> &ActorSystem {
        &self.system
    }

    pub fn registry(&self) -> &Arc<Registry> {
        &self.registry
    }

    pub fn settings(&self) -> &RemoteSettings {
        &self.settings
    }

    pub fn association_cache(&self) -> &RemoteAssociationCache {
        &self.association_cache
    }

    pub fn association_registry(&self) -> &RemoteAssociationRegistry {
        &self.association_registry
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
        let (registration, reader_handle) = self
            .dialer
            .dial_with_reader(address, self.outbound_reader.clone())?;
        self.outbound_readers
            .lock()
            .expect("tcp remote actor system outbound readers lock poisoned")
            .push(reader_handle);
        Ok(registration)
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

    pub fn shutdown(self) -> Result<TcpAssociationListenerReport> {
        self.shutdown_with_timeout(DEFAULT_SHUTDOWN_TIMEOUT)
    }

    pub fn shutdown_with_timeout(self, timeout: Duration) -> Result<TcpAssociationListenerReport> {
        self.system.stop(&self.death_watch);
        let death_watch_stopped = self.death_watch.wait_for_stop(timeout);
        self.association_cache.clear_routes();
        self.listener.stop();
        let outbound_readers = self
            .outbound_readers
            .lock()
            .expect("tcp remote actor system outbound readers lock poisoned")
            .drain(..)
            .collect::<Vec<_>>();
        for reader in outbound_readers {
            reader.join()?;
        }
        let listener_report = self.listener.join();

        if !death_watch_stopped {
            return Err(RemoteError::Inbound(
                "remote death-watch actor did not stop during tcp shutdown".to_string(),
            ));
        }

        listener_report
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
