use std::sync::Arc;

use kairo_serialization::{
    ActorRefWireData, Registry, RemoteEnvelope, RemoteMessage, SerializationError,
};

use crate::{
    RemoteDeathWatchEffect, RemoteDeathWatchEffectSink, RemoteError, RemoteHeartbeat,
    RemoteHeartbeatAck, RemoteOutbound, RemoteTerminated, Result, UnwatchRemote, WatchRemote,
};

const REMOTE_WATCHER_PATH: &str = "/system/remote-watch";

pub trait RemoteDeathWatchEffectObserver: Send + Sync + 'static {
    fn observe(&self, effect: &RemoteDeathWatchEffect) -> Result<()>;
}

#[derive(Debug, Default)]
pub struct IgnoreRemoteDeathWatchEffects;

impl RemoteDeathWatchEffectObserver for IgnoreRemoteDeathWatchEffects {
    fn observe(&self, _effect: &RemoteDeathWatchEffect) -> Result<()> {
        Ok(())
    }
}

pub struct RemoteDeathWatchOutboundSink {
    registry: Arc<Registry>,
    outbound: Arc<dyn RemoteOutbound>,
    observer: Arc<dyn RemoteDeathWatchEffectObserver>,
    local_watcher: Option<ActorRefWireData>,
}

impl RemoteDeathWatchOutboundSink {
    pub fn new(registry: Arc<Registry>, outbound: Arc<dyn RemoteOutbound>) -> Self {
        Self::with_observer(
            registry,
            outbound,
            Arc::new(IgnoreRemoteDeathWatchEffects) as Arc<dyn RemoteDeathWatchEffectObserver>,
        )
    }

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

    pub fn registry(&self) -> &Arc<Registry> {
        &self.registry
    }

    pub fn outbound(&self) -> &Arc<dyn RemoteOutbound> {
        &self.outbound
    }

    pub fn observer(&self) -> &Arc<dyn RemoteDeathWatchEffectObserver> {
        &self.observer
    }

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

pub fn watcher_recipient_for_actor(actor: &ActorRefWireData) -> Result<ActorRefWireData> {
    watcher_recipient_for_address(&wire_address(actor))
}

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
