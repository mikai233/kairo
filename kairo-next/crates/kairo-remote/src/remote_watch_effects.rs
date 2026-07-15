#![deny(missing_docs)]

use std::sync::Arc;

use kairo_serialization::{
    ActorRefWireData, Registry, RemoteEnvelope, RemoteMessage, SerializationError,
};

use crate::{
    RemoteDeathWatchEffect, RemoteDeathWatchEffectSink, RemoteError, RemoteHeartbeat,
    RemoteHeartbeatAck, RemoteOutbound, RemoteTerminated, Result, UnwatchRemote, WatchRemote,
};

const REMOTE_WATCHER_PATH: &str = "/system/remote-watch";

/// Observer invoked before each remote death-watch effect is applied.
pub trait RemoteDeathWatchEffectObserver: Send + Sync + 'static {
    /// Observes one effect and may reject further application by returning an
    /// error.
    fn observe(&self, effect: &RemoteDeathWatchEffect) -> Result<()>;
}

/// No-op remote death-watch effect observer.
#[derive(Debug, Default)]
pub struct IgnoreRemoteDeathWatchEffects;

impl RemoteDeathWatchEffectObserver for IgnoreRemoteDeathWatchEffects {
    fn observe(&self, _effect: &RemoteDeathWatchEffect) -> Result<()> {
        Ok(())
    }
}

/// Applies transport-facing remote death-watch effects by serializing stable
/// system protocol messages through a [`RemoteOutbound`].
///
/// Scheduler, failure-detector, and local notification effects are exposed to
/// the observer but intentionally require composition by the owning runtime.
pub struct RemoteDeathWatchOutboundSink {
    registry: Arc<Registry>,
    outbound: Arc<dyn RemoteOutbound>,
    observer: Arc<dyn RemoteDeathWatchEffectObserver>,
    local_watcher: Option<ActorRefWireData>,
}

impl RemoteDeathWatchOutboundSink {
    /// Creates an outbound sink with a no-op effect observer and no explicit
    /// sender metadata.
    pub fn new(registry: Arc<Registry>, outbound: Arc<dyn RemoteOutbound>) -> Self {
        Self::with_observer(
            registry,
            outbound,
            Arc::new(IgnoreRemoteDeathWatchEffects) as Arc<dyn RemoteDeathWatchEffectObserver>,
        )
    }

    /// Creates an outbound sink with an effect observer and no explicit sender
    /// metadata.
    pub fn with_observer(
        registry: Arc<Registry>,
        outbound: Arc<dyn RemoteOutbound>,
        observer: Arc<dyn RemoteDeathWatchEffectObserver>,
    ) -> Self {
        Self {
            registry,
            outbound,
            observer,
            local_watcher: None,
        }
    }

    /// Creates an outbound sink that includes the local remote-watcher actor as
    /// sender metadata on protocol envelopes.
    pub fn with_local_watcher(
        registry: Arc<Registry>,
        outbound: Arc<dyn RemoteOutbound>,
        observer: Arc<dyn RemoteDeathWatchEffectObserver>,
        local_watcher: ActorRefWireData,
    ) -> Self {
        Self {
            registry,
            outbound,
            observer,
            local_watcher: Some(local_watcher),
        }
    }

    /// Returns the registry used to serialize remote-watch protocol messages.
    pub fn registry(&self) -> &Arc<Registry> {
        &self.registry
    }

    /// Returns the remote outbound used for protocol envelopes.
    pub fn outbound(&self) -> &Arc<dyn RemoteOutbound> {
        &self.outbound
    }

    /// Returns the observer invoked for every effect.
    pub fn observer(&self) -> &Arc<dyn RemoteDeathWatchEffectObserver> {
        &self.observer
    }

    /// Returns the local remote-watcher sender metadata, if configured.
    pub fn local_watcher(&self) -> Option<&ActorRefWireData> {
        self.local_watcher.as_ref()
    }

    fn send_watch(&self, message: WatchRemote) -> Result<()> {
        let recipient = watcher_recipient_for_actor(&message.watchee)?;
        self.send_remote(recipient, &message)
    }

    fn send_unwatch(&self, message: UnwatchRemote) -> Result<()> {
        let recipient = watcher_recipient_for_actor(&message.watchee)?;
        self.send_remote(recipient, &message)
    }

    fn send_heartbeat(&self, address: &str, message: &RemoteHeartbeat) -> Result<()> {
        let recipient = watcher_recipient_for_address(address)?;
        self.send_remote(recipient, message)
    }

    fn send_heartbeat_ack(&self, address: &str, message: &RemoteHeartbeatAck) -> Result<()> {
        let recipient = watcher_recipient_for_address(address)?;
        self.send_remote(recipient, message)
    }

    fn send_remote_terminated(
        &self,
        watcher: &ActorRefWireData,
        message: &RemoteTerminated,
    ) -> Result<()> {
        let recipient = watcher_recipient_for_actor(watcher)?;
        self.send_remote(recipient, message)
    }

    fn send_remote<M>(&self, recipient: ActorRefWireData, message: &M) -> Result<()>
    where
        M: RemoteMessage,
    {
        let serialized = self.registry.serialize(message)?;
        self.outbound.send(RemoteEnvelope::new(
            recipient,
            self.local_watcher.clone(),
            serialized,
        ))
    }
}

impl RemoteDeathWatchEffectSink for RemoteDeathWatchOutboundSink {
    fn apply(&self, effects: Vec<RemoteDeathWatchEffect>) -> Result<()> {
        for effect in effects {
            self.observer.observe(&effect)?;
            match effect {
                RemoteDeathWatchEffect::SendWatchRemote(message)
                | RemoteDeathWatchEffect::RewatchRemote(message) => self.send_watch(message)?,
                RemoteDeathWatchEffect::SendUnwatchRemote(message) => self.send_unwatch(message)?,
                RemoteDeathWatchEffect::SendHeartbeat { address, message } => {
                    self.send_heartbeat(&address, &message)?
                }
                RemoteDeathWatchEffect::SendHeartbeatAck { address, message } => {
                    self.send_heartbeat_ack(&address, &message)?
                }
                RemoteDeathWatchEffect::SendRemoteTerminated { watcher, message } => {
                    self.send_remote_terminated(&watcher, &message)?
                }
                RemoteDeathWatchEffect::StartHeartbeat { .. }
                | RemoteDeathWatchEffect::StopHeartbeat { .. }
                | RemoteDeathWatchEffect::ResetFailureDetector { .. }
                | RemoteDeathWatchEffect::RemoteTerminated(_)
                | RemoteDeathWatchEffect::AddressTerminated(_) => {}
            }
        }
        Ok(())
    }
}

/// Resolves the stable remote-watcher system actor path for an actor's owning
/// address.
pub fn watcher_recipient_for_actor(actor: &ActorRefWireData) -> Result<ActorRefWireData> {
    watcher_recipient_for_address(&wire_address(actor))
}

/// Resolves the stable remote-watcher system actor path under `address`.
pub fn watcher_recipient_for_address(address: &str) -> Result<ActorRefWireData> {
    ActorRefWireData::new(format!("{address}{REMOTE_WATCHER_PATH}")).map_err(|error| {
        RemoteError::InvalidRemoteRef(
            format!("{address}{REMOTE_WATCHER_PATH}"),
            invalid_ref_reason(error),
        )
    })
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

fn invalid_ref_reason(error: SerializationError) -> String {
    error.to_string()
}

#[cfg(test)]
mod tests;
