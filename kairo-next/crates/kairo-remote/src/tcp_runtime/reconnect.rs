use std::collections::BTreeMap;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::reliable_runtime::{ReliableSystemDeliveryRuntime, ReliableSystemInboundHandler};
use crate::{
    ActorSystemRemoteInboundRegistry, AssociationOutboundPipeline, AssociationState,
    RemoteAssociationAddress, RemoteAssociationCache, RemoteAssociationRegistry,
    RemoteAssociationRouteRegistration, RemoteError, RemoteFrameHandler, Result,
    TcpAssociationDialer, TcpAssociationReaderHandle, TcpAssociationStreamReader,
};

const DEFAULT_MIN_BACKOFF: Duration = Duration::from_secs(1);
const DEFAULT_MAX_BACKOFF: Duration = Duration::from_secs(30);
/// Bounded retry policy for routes managed by [`super::TcpRemoteActorRuntime`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpRemoteReconnectSettings {
    min_backoff: Duration,
    max_backoff: Duration,
}

impl TcpRemoteReconnectSettings {
    pub fn new(min_backoff: Duration, max_backoff: Duration) -> Result<Self> {
        if min_backoff.is_zero() {
            return Err(RemoteError::InvalidTcpReconnectSettings(
                "minimum backoff must be greater than zero".to_string(),
            ));
        }
        if max_backoff < min_backoff {
            return Err(RemoteError::InvalidTcpReconnectSettings(format!(
                "maximum backoff {max_backoff:?} is less than minimum {min_backoff:?}"
            )));
        }
        Ok(Self {
            min_backoff,
            max_backoff,
        })
    }

    pub fn min_backoff(self) -> Duration {
        self.min_backoff
    }

    pub fn max_backoff(self) -> Duration {
        self.max_backoff
    }

    fn backoff_for_attempt(self, attempts: u32) -> Duration {
        let exponent = attempts.saturating_sub(1).min(31);
        self.min_backoff
            .saturating_mul(1_u32 << exponent)
            .min(self.max_backoff)
    }
}

impl Default for TcpRemoteReconnectSettings {
    fn default() -> Self {
        Self {
            min_backoff: DEFAULT_MIN_BACKOFF,
            max_backoff: DEFAULT_MAX_BACKOFF,
        }
    }
}

#[derive(Debug)]
struct ManagedPeer {
    attempts: u32,
    next_attempt: Instant,
    dialing: bool,
}

#[derive(Debug, Default)]
struct ReconnectState {
    stopping: bool,
    active_attempts: usize,
    peers: BTreeMap<RemoteAssociationAddress, ManagedPeer>,
}

pub(super) struct ReconnectResources {
    dialer: TcpAssociationDialer,
    association_cache: RemoteAssociationCache,
    association_registry: RemoteAssociationRegistry,
    inbound: Arc<ActorSystemRemoteInboundRegistry>,
    reliable_delivery: Arc<ReliableSystemDeliveryRuntime>,
    outbound_readers: Arc<Mutex<Vec<TcpAssociationReaderHandle>>>,
    outbound_pipelines: Arc<Mutex<Vec<AssociationOutboundPipeline>>>,
}

impl ReconnectResources {
    pub(super) fn new(
        dialer: TcpAssociationDialer,
        association_cache: RemoteAssociationCache,
        association_registry: RemoteAssociationRegistry,
        inbound: Arc<ActorSystemRemoteInboundRegistry>,
        reliable_delivery: Arc<ReliableSystemDeliveryRuntime>,
        outbound_readers: Arc<Mutex<Vec<TcpAssociationReaderHandle>>>,
        outbound_pipelines: Arc<Mutex<Vec<AssociationOutboundPipeline>>>,
    ) -> Self {
        Self {
            dialer,
            association_cache,
            association_registry,
            inbound,
            reliable_delivery,
            outbound_readers,
            outbound_pipelines,
        }
    }

    fn dial_transport(
        &self,
        address: RemoteAssociationAddress,
    ) -> Result<(
        RemoteAssociationRouteRegistration,
        TcpAssociationReaderHandle,
    )> {
        let reader = TcpAssociationStreamReader::new(Arc::new(ReliableSystemInboundHandler::new(
            self.reliable_delivery.clone(),
            self.inbound.clone() as Arc<dyn RemoteFrameHandler>,
            address.clone(),
        )));
        self.dialer.dial_with_reader(address, reader)
    }

    fn retain(
        &self,
        registration: &RemoteAssociationRouteRegistration,
        reader: TcpAssociationReaderHandle,
    ) {
        let mut pipelines = self
            .outbound_pipelines
            .lock()
            .expect("tcp remote reconnect outbound pipelines lock poisoned");
        pipelines.retain(|pipeline| {
            !matches!(
                pipeline
                    .association()
                    .lock()
                    .expect("remote association lock poisoned")
                    .state(),
                AssociationState::Quarantined { .. } | AssociationState::Closed { .. }
            )
        });
        pipelines.push(registration.pipeline().clone());
        drop(pipelines);

        let mut readers = self
            .outbound_readers
            .lock()
            .expect("tcp remote reconnect outbound readers lock poisoned");
        let mut index = 0;
        while index < readers.len() {
            if readers[index].is_finished() {
                let completed = readers.swap_remove(index);
                let _ = completed.join_after_stop();
            } else {
                index += 1;
            }
        }
        readers.push(reader);
    }

    fn route_is_healthy(&self, address: &RemoteAssociationAddress) -> bool {
        if !self.association_cache.contains_route(address) {
            return false;
        }
        self.association_registry
            .association_for_address(address)
            .is_some_and(|association| {
                matches!(
                    association
                        .lock()
                        .expect("remote association lock poisoned")
                        .state(),
                    AssociationState::Active { .. }
                )
            })
    }

    fn remove_terminal_route(&self, address: &RemoteAssociationAddress) {
        let _ = self.association_cache.remove_route_and_close(
            address,
            "tcp remote reconnect replacing terminal managed route",
        );
    }
}

struct ReconnectCore {
    settings: TcpRemoteReconnectSettings,
    state: Mutex<ReconnectState>,
    wake: Condvar,
    resources: ReconnectResources,
}

pub(super) struct TcpRemoteReconnectManager {
    core: Arc<ReconnectCore>,
    join: Mutex<Option<JoinHandle<()>>>,
}

impl TcpRemoteReconnectManager {
    pub(super) fn new(settings: TcpRemoteReconnectSettings, resources: ReconnectResources) -> Self {
        let core = Arc::new(ReconnectCore {
            settings,
            state: Mutex::new(ReconnectState::default()),
            wake: Condvar::new(),
            resources,
        });
        let worker_core = Arc::clone(&core);
        let join = thread::Builder::new()
            .name("kairo-remote-reconnect".to_string())
            .spawn(move || run_reconnect_worker(worker_core))
            .expect("failed to spawn tcp remote reconnect worker");
        Self {
            core,
            join: Mutex::new(Some(join)),
        }
    }

    pub(super) fn dial(
        &self,
        address: RemoteAssociationAddress,
    ) -> Result<RemoteAssociationRouteRegistration> {
        self.core.begin_manual_attempt(address.clone())?;
        let result = self.core.resources.dial_transport(address.clone());
        self.core.complete_attempt(address, result)
    }

    pub(super) fn disconnect(&self, address: &RemoteAssociationAddress) -> bool {
        let removed = self
            .core
            .state
            .lock()
            .expect("tcp remote reconnect state lock poisoned")
            .peers
            .remove(address)
            .is_some();
        self.core.wake.notify_all();
        removed
    }

    pub(super) fn managed_peer_count(&self) -> usize {
        self.core
            .state
            .lock()
            .expect("tcp remote reconnect state lock poisoned")
            .peers
            .len()
    }

    pub(super) fn stop_until(&self, deadline: Instant) -> bool {
        {
            let mut state = self
                .core
                .state
                .lock()
                .expect("tcp remote reconnect state lock poisoned");
            state.stopping = true;
            state.peers.clear();
            self.core.wake.notify_all();
            while state.active_attempts > 0 {
                let now = Instant::now();
                if now >= deadline {
                    return false;
                }
                let (next, _) = self
                    .core
                    .wake
                    .wait_timeout(state, deadline - now)
                    .expect("tcp remote reconnect state lock poisoned");
                state = next;
            }
        }

        loop {
            let mut join = self
                .join
                .lock()
                .expect("tcp remote reconnect join lock poisoned");
            let Some(handle) = join.as_ref() else {
                return true;
            };
            if handle.is_finished() {
                let handle = join.take().expect("reconnect handle disappeared");
                drop(join);
                return handle.join().is_ok();
            }
            drop(join);
            if Instant::now() >= deadline {
                return false;
            }
            thread::sleep(Duration::from_millis(1));
        }
    }
}

impl Drop for TcpRemoteReconnectManager {
    fn drop(&mut self) {
        {
            let mut state = self
                .core
                .state
                .lock()
                .expect("tcp remote reconnect state lock poisoned");
            state.stopping = true;
            state.peers.clear();
        }
        self.core.wake.notify_all();
        if let Some(join) = self
            .join
            .get_mut()
            .expect("tcp remote reconnect join lock poisoned")
            .take()
        {
            let _ = join.join();
        }
    }
}

impl ReconnectCore {
    fn begin_manual_attempt(&self, address: RemoteAssociationAddress) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .expect("tcp remote reconnect state lock poisoned");
        if state.stopping {
            return Err(RemoteError::Outbound(
                "tcp remote runtime is shutting down".to_string(),
            ));
        }
        let now = Instant::now();
        let peer = state.peers.entry(address).or_insert(ManagedPeer {
            attempts: 0,
            next_attempt: now + self.settings.min_backoff(),
            dialing: false,
        });
        if peer.dialing {
            return Err(RemoteError::Outbound(
                "tcp remote association dial is already in progress".to_string(),
            ));
        }
        peer.dialing = true;
        state.active_attempts += 1;
        Ok(())
    }

    fn complete_attempt(
        &self,
        address: RemoteAssociationAddress,
        result: Result<(
            RemoteAssociationRouteRegistration,
            TcpAssociationReaderHandle,
        )>,
    ) -> Result<RemoteAssociationRouteRegistration> {
        match result {
            Ok((registration, reader)) => {
                let keep = {
                    let mut state = self
                        .state
                        .lock()
                        .expect("tcp remote reconnect state lock poisoned");
                    let keep = !state.stopping && state.peers.contains_key(&address);
                    if keep {
                        self.resources.retain(&registration, reader);
                        let peer = state
                            .peers
                            .get_mut(&address)
                            .expect("managed peer disappeared while retaining route");
                        peer.attempts = 0;
                        peer.next_attempt = Instant::now() + self.settings.min_backoff();
                        peer.dialing = false;
                    } else {
                        drop(reader);
                    }
                    state.active_attempts = state.active_attempts.saturating_sub(1);
                    self.wake.notify_all();
                    keep
                };
                if keep {
                    Ok(registration)
                } else {
                    registration.close_owned_route("tcp remote reconnect intent removed");
                    Err(RemoteError::Outbound(
                        "tcp remote reconnect intent was removed during dial".to_string(),
                    ))
                }
            }
            Err(error) => {
                let mut state = self
                    .state
                    .lock()
                    .expect("tcp remote reconnect state lock poisoned");
                if !state.stopping
                    && let Some(peer) = state.peers.get_mut(&address)
                {
                    peer.attempts = peer.attempts.saturating_add(1);
                    peer.next_attempt =
                        Instant::now() + self.settings.backoff_for_attempt(peer.attempts);
                    peer.dialing = false;
                }
                state.active_attempts = state.active_attempts.saturating_sub(1);
                self.wake.notify_all();
                Err(error)
            }
        }
    }

    fn due_attempts(&self) -> Option<Vec<RemoteAssociationAddress>> {
        let mut state = self
            .state
            .lock()
            .expect("tcp remote reconnect state lock poisoned");
        loop {
            if state.stopping {
                return None;
            }
            let now = Instant::now();
            let due = state
                .peers
                .iter()
                .filter(|(_, peer)| !peer.dialing && peer.next_attempt <= now)
                .map(|(address, _)| address.clone())
                .collect::<Vec<_>>();
            if !due.is_empty() {
                for address in &due {
                    state
                        .peers
                        .get_mut(address)
                        .expect("due managed peer disappeared")
                        .dialing = true;
                }
                state.active_attempts += due.len();
                return Some(due);
            }
            let wait = state
                .peers
                .values()
                .filter(|peer| !peer.dialing)
                .map(|peer| peer.next_attempt.saturating_duration_since(now))
                .min();
            state = match wait {
                Some(wait) => {
                    self.wake
                        .wait_timeout(state, wait)
                        .expect("tcp remote reconnect state lock poisoned")
                        .0
                }
                None => self
                    .wake
                    .wait(state)
                    .expect("tcp remote reconnect state lock poisoned"),
            };
        }
    }

    fn attempt_is_current(&self, address: &RemoteAssociationAddress) -> bool {
        let state = self
            .state
            .lock()
            .expect("tcp remote reconnect state lock poisoned");
        !state.stopping && state.peers.get(address).is_some_and(|peer| peer.dialing)
    }

    fn complete_healthy_observation(&self, address: &RemoteAssociationAddress) {
        let mut state = self
            .state
            .lock()
            .expect("tcp remote reconnect state lock poisoned");
        if !state.stopping
            && let Some(peer) = state.peers.get_mut(address)
        {
            peer.attempts = 0;
            peer.next_attempt = Instant::now() + self.settings.min_backoff();
            peer.dialing = false;
        }
        state.active_attempts = state.active_attempts.saturating_sub(1);
        self.wake.notify_all();
    }

    fn cancel_attempt(&self) {
        let mut state = self
            .state
            .lock()
            .expect("tcp remote reconnect state lock poisoned");
        state.active_attempts = state.active_attempts.saturating_sub(1);
        self.wake.notify_all();
    }
}

fn run_reconnect_worker(core: Arc<ReconnectCore>) {
    while let Some(addresses) = core.due_attempts() {
        for address in addresses {
            if !core.attempt_is_current(&address) {
                core.cancel_attempt();
                continue;
            }
            if core.resources.route_is_healthy(&address) {
                core.complete_healthy_observation(&address);
                continue;
            }
            core.resources.remove_terminal_route(&address);
            let result = core.resources.dial_transport(address.clone());
            let _ = core.complete_attempt(address, result);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconnect_settings_reject_invalid_bounds() {
        assert!(TcpRemoteReconnectSettings::new(Duration::ZERO, Duration::from_secs(1)).is_err());
        assert!(
            TcpRemoteReconnectSettings::new(Duration::from_secs(2), Duration::from_secs(1))
                .is_err()
        );
    }

    #[test]
    fn reconnect_backoff_grows_and_caps() {
        let settings =
            TcpRemoteReconnectSettings::new(Duration::from_millis(10), Duration::from_millis(40))
                .unwrap();

        assert_eq!(settings.backoff_for_attempt(1), Duration::from_millis(10));
        assert_eq!(settings.backoff_for_attempt(2), Duration::from_millis(20));
        assert_eq!(settings.backoff_for_attempt(3), Duration::from_millis(40));
        assert_eq!(settings.backoff_for_attempt(30), Duration::from_millis(40));
    }
}
