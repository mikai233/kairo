use std::marker::PhantomData;
use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use kairo_actor::{ActorRef, ActorSystem};
use kairo_serialization::{ActorRefWireData, Registry, RemoteMessage};

use crate::{
    ActorSystemRemoteInbound, RemoteActorRef, RemoteActorRefProvider, RemoteAssociationAddress,
    RemoteAssociationCache, RemoteAssociationRouteInstaller, RemoteAssociationRouteRegistration,
    RemoteDeathWatchCommand, RemoteDeathWatchEffectObserver, RemoteDeathWatchOutboundSink,
    RemoteError, RemoteOutbound, RemoteSettings, Result, TcpAssociationDialer,
    TcpAssociationListener, TcpAssociationListenerHandle, TcpAssociationListenerReport,
};

pub struct TcpRemoteActorSystem<M> {
    system: ActorSystem,
    registry: Arc<Registry>,
    settings: RemoteSettings,
    association_cache: RemoteAssociationCache,
    provider: RemoteActorRefProvider,
    dialer: TcpAssociationDialer,
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
        let outbound = Arc::new(association_cache.clone()) as Arc<dyn RemoteOutbound>;
        let local_watcher = local_watcher_for(&system, &effective_settings)?;
        let death_watch_sink = Arc::new(RemoteDeathWatchOutboundSink::with_local_watcher(
            registry.clone(),
            outbound.clone(),
            observer,
            local_watcher,
        ));
        let death_watch = system
            .spawn(
                "remote-watch",
                crate::RemoteDeathWatchActor::props(death_watch_sink),
            )
            .map_err(|error| RemoteError::Inbound(error.to_string()))?;

        let inbound = ActorSystemRemoteInbound::<M>::with_remote_settings(
            system.clone(),
            registry.clone(),
            death_watch.clone(),
            local_system_uid,
            effective_settings.clone(),
        );
        let listener = TcpAssociationListener::from_listener(listener, Arc::new(inbound))
            .spawn_accept_loop()?;
        let installer = RemoteAssociationRouteInstaller::new(association_cache.clone());
        let dialer =
            TcpAssociationDialer::new(installer).with_connect_timeout(Duration::from_secs(1));
        let provider = RemoteActorRefProvider::new(
            system.name().to_string(),
            effective_settings.clone(),
            registry.clone(),
            outbound,
        );

        Ok(Self {
            system,
            registry,
            settings: effective_settings,
            association_cache,
            provider,
            dialer,
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
        self.dialer.dial(address)
    }

    pub fn resolve<N>(&self, path: impl Into<String>) -> Result<RemoteActorRef<N>>
    where
        N: RemoteMessage,
    {
        self.provider.resolve(path)
    }

    pub fn shutdown(self) -> Result<TcpAssociationListenerReport> {
        self.association_cache.clear_routes();
        self.listener.stop();
        self.listener.join()
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

#[cfg(test)]
mod tests;
