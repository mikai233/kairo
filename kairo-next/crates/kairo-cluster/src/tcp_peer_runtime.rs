#![deny(missing_docs)]

use std::fmt::{self, Display, Formatter};
use std::time::Duration;

use kairo_remote::{
    RemoteAssociationAddress, RemoteAssociationCache, RemoteAssociationRegistry, RemoteSettings,
    Result as RemoteResult, TcpAssociationListenerReport,
};

use crate::{
    ClusterAssociationPeerChange, ClusterAssociationPeerError, ClusterAssociationPeerState,
    ClusterEvent, ClusterSystemInbound, ClusterTcpAssociationRuntime,
    ClusterTcpPeerReconnectReport, ClusterTcpPeerReconnectSettings, ClusterTcpPeerReconnectState,
    ClusterTcpPeerRouteError, ClusterTcpPeerRouteReport, ClusterTcpPeerRoutes, CurrentClusterState,
    UniqueAddress,
};

#[derive(Debug)]
/// Failure while projecting cluster membership or applying its TCP route intent.
pub enum ClusterTcpPeerRuntimeError {
    /// A cluster member could not be converted to a remote peer target.
    Peer(ClusterAssociationPeerError),
    /// A derived TCP route operation failed.
    Route(ClusterTcpPeerRouteError),
}

impl Display for ClusterTcpPeerRuntimeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Peer(error) => write!(f, "{error}"),
            Self::Route(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for ClusterTcpPeerRuntimeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Peer(error) => Some(error),
            Self::Route(error) => Some(error),
        }
    }
}

impl From<ClusterAssociationPeerError> for ClusterTcpPeerRuntimeError {
    fn from(error: ClusterAssociationPeerError) -> Self {
        Self::Peer(error)
    }
}

impl From<ClusterTcpPeerRouteError> for ClusterTcpPeerRuntimeError {
    fn from(error: ClusterTcpPeerRouteError) -> Self {
        Self::Route(error)
    }
}

/// Result of applying membership or reconnect work to the TCP peer runtime.
pub type ClusterTcpPeerRuntimeResult<T> = Result<T, ClusterTcpPeerRuntimeError>;

#[derive(Debug, Clone, PartialEq, Eq)]
/// Cleanup completed while shutting down a membership-driven TCP peer runtime.
pub struct ClusterTcpPeerRuntimeShutdownReport {
    /// Managed peer routes removed before listener shutdown.
    pub peer_routes: ClusterTcpPeerRouteReport,
    /// Pending reconnect deadlines cleared before listener shutdown.
    pub pending_reconnects: ClusterTcpPeerReconnectReport,
    /// Accepted-association report returned by the underlying listener.
    pub listener: TcpAssociationListenerReport,
}

/// Synchronous composition of membership projection, TCP routes, and reconnect state.
///
/// The runtime does not subscribe to cluster events by itself. Callers feed snapshots and events,
/// and the runtime derives transport intent without treating associations as membership truth.
pub struct ClusterTcpPeerRuntime {
    runtime: ClusterTcpAssociationRuntime,
    peers: ClusterAssociationPeerState,
    routes: ClusterTcpPeerRoutes,
    reconnect: ClusterTcpPeerReconnectState,
}

impl ClusterTcpPeerRuntime {
    /// Binds a runtime with the default fixed-interval reconnect policy.
    pub fn bind(
        local_system: impl Into<String>,
        node_uid: u64,
        local_system_uid: u64,
        settings: RemoteSettings,
        inbound: impl FnOnce(UniqueAddress, RemoteAssociationCache) -> ClusterSystemInbound,
    ) -> RemoteResult<Self> {
        let runtime = ClusterTcpAssociationRuntime::bind(
            local_system,
            node_uid,
            local_system_uid,
            settings,
            inbound,
        )?;
        let peers = ClusterAssociationPeerState::new(runtime.self_node().clone());
        Ok(Self {
            runtime,
            peers,
            routes: ClusterTcpPeerRoutes::new(),
            reconnect: ClusterTcpPeerReconnectState::default(),
        })
    }

    /// Binds a runtime with an explicit fixed-interval reconnect policy.
    pub fn bind_with_reconnect(
        local_system: impl Into<String>,
        node_uid: u64,
        local_system_uid: u64,
        settings: RemoteSettings,
        reconnect_settings: ClusterTcpPeerReconnectSettings,
        inbound: impl FnOnce(UniqueAddress, RemoteAssociationCache) -> ClusterSystemInbound,
    ) -> RemoteResult<Self> {
        let runtime = ClusterTcpAssociationRuntime::bind(
            local_system,
            node_uid,
            local_system_uid,
            settings,
            inbound,
        )?;
        let peers = ClusterAssociationPeerState::new(runtime.self_node().clone());
        Ok(Self {
            runtime,
            peers,
            routes: ClusterTcpPeerRoutes::new(),
            reconnect: ClusterTcpPeerReconnectState::new(reconnect_settings),
        })
    }

    /// Returns the owned lower-level TCP association runtime.
    pub fn runtime(&self) -> &ClusterTcpAssociationRuntime {
        &self.runtime
    }

    /// Returns the canonical local cluster member identity.
    pub fn self_node(&self) -> &UniqueAddress {
        self.runtime.self_node()
    }

    /// Returns the canonical local transport address.
    pub fn local_address(&self) -> &RemoteAssociationAddress {
        self.runtime.local_address()
    }

    /// Returns the shared bidirectional association route cache.
    pub fn association_cache(&self) -> &RemoteAssociationCache {
        self.runtime.association_cache()
    }

    /// Returns identities learned from accepted remote associations.
    pub fn association_registry(&self) -> &RemoteAssociationRegistry {
        self.runtime.association_registry()
    }

    /// Returns the number of managed membership-derived route entries.
    pub fn peer_route_count(&self) -> usize {
        self.routes.route_count()
    }

    /// Returns managed peer targets in deterministic member order.
    pub fn active_peer_targets(&self) -> Vec<crate::ClusterAssociationPeerTarget> {
        self.routes.active_targets()
    }

    /// Returns the number of failed peer dials waiting for retry.
    pub fn pending_peer_reconnect_count(&self) -> usize {
        self.reconnect.pending_count()
    }

    /// Returns pending reconnects in deterministic member order.
    pub fn pending_peer_reconnects(&self) -> Vec<crate::ClusterTcpPeerReconnectPending> {
        self.reconnect.pending_reconnects()
    }

    /// Applies a full cluster snapshot using zero as the reconnect clock value.
    pub fn apply_snapshot(
        &mut self,
        snapshot: CurrentClusterState,
    ) -> ClusterTcpPeerRuntimeResult<ClusterTcpPeerRouteReport> {
        self.apply_snapshot_at(snapshot, Duration::ZERO)
    }

    /// Applies a full cluster snapshot and schedules failed dials relative to `now`.
    pub fn apply_snapshot_at(
        &mut self,
        snapshot: CurrentClusterState,
        now: Duration,
    ) -> ClusterTcpPeerRuntimeResult<ClusterTcpPeerRouteReport> {
        let changes = self.peers.apply_snapshot(snapshot)?;
        self.apply_route_changes(changes, now)
    }

    /// Applies one cluster-domain event using zero as the reconnect clock value.
    pub fn apply_event(
        &mut self,
        event: ClusterEvent,
    ) -> ClusterTcpPeerRuntimeResult<ClusterTcpPeerRouteReport> {
        self.apply_event_at(event, Duration::ZERO)
    }

    /// Applies one cluster-domain event and schedules failed dials relative to `now`.
    pub fn apply_event_at(
        &mut self,
        event: ClusterEvent,
        now: Duration,
    ) -> ClusterTcpPeerRuntimeResult<ClusterTcpPeerRouteReport> {
        let changes = self.peers.apply_event(event)?;
        self.apply_route_changes(changes, now)
    }

    /// Retries every failed peer dial whose deadline is at or before `now`.
    pub fn retry_due_peer_routes(
        &mut self,
        now: Duration,
    ) -> ClusterTcpPeerRuntimeResult<ClusterTcpPeerRouteReport> {
        let targets = self.reconnect.due_targets(now);
        self.apply_route_changes(
            targets.into_iter().map(ClusterAssociationPeerChange::Dial),
            now,
        )
    }

    /// Clears every pending reconnect deadline without removing active routes.
    pub fn clear_pending_peer_reconnects(&mut self) -> ClusterTcpPeerReconnectReport {
        ClusterTcpPeerReconnectReport {
            scheduled: Vec::new(),
            cleared: self.reconnect.clear_all(),
        }
    }

    /// Removes every managed peer route without clearing reconnect deadlines.
    pub fn clear_peer_routes(&mut self) -> ClusterTcpPeerRouteReport {
        self.routes.clear(&self.runtime)
    }

    /// Clears peer state and stops the TCP association runtime with its default policy.
    ///
    /// # Errors
    ///
    /// Returns the first route-close or listener failure, or
    /// [`kairo_remote::RemoteError::ShutdownTimeout`] when the default
    /// shutdown deadline expires.
    pub fn shutdown(self) -> RemoteResult<ClusterTcpPeerRuntimeShutdownReport> {
        self.shutdown_with_timeout(Duration::from_secs(1))
    }

    /// Clears reconnects and routes before stopping the TCP association runtime.
    ///
    /// `timeout` is forwarded to the lower-level runtime, which owns listener shutdown semantics.
    ///
    /// # Errors
    ///
    /// Returns the first route-close or listener failure, or
    /// [`kairo_remote::RemoteError::ShutdownTimeout`] when `timeout` expires.
    pub fn shutdown_with_timeout(
        mut self,
        timeout: Duration,
    ) -> RemoteResult<ClusterTcpPeerRuntimeShutdownReport> {
        let pending_reconnects = self.clear_pending_peer_reconnects();
        let peer_routes = self.clear_peer_routes();
        let listener = self.runtime.shutdown_with_timeout(timeout)?;
        Ok(ClusterTcpPeerRuntimeShutdownReport {
            peer_routes,
            pending_reconnects,
            listener,
        })
    }

    fn apply_route_changes(
        &mut self,
        changes: impl IntoIterator<Item = ClusterAssociationPeerChange>,
        now: Duration,
    ) -> ClusterTcpPeerRuntimeResult<ClusterTcpPeerRouteReport> {
        let mut report = ClusterTcpPeerRouteReport::default();
        for change in changes {
            match &change {
                ClusterAssociationPeerChange::Remove(target) => {
                    self.reconnect.clear_peer(target);
                }
                ClusterAssociationPeerChange::Dial(_) => {}
            }

            match self
                .routes
                .apply_changes(&self.runtime, std::iter::once(change))
            {
                Ok(next) => {
                    for target in next.dialed.iter().chain(next.skipped.iter()) {
                        self.reconnect.clear_peer(target);
                    }
                    for target in &next.removed {
                        self.reconnect.clear_peer(target);
                    }
                    merge_route_report(&mut report, next);
                }
                Err(error) => {
                    self.reconnect.record_failure(error.target().clone(), now);
                    return Err(error.into());
                }
            }
        }
        Ok(report)
    }
}

fn merge_route_report(into: &mut ClusterTcpPeerRouteReport, next: ClusterTcpPeerRouteReport) {
    into.dialed.extend(next.dialed);
    into.removed.extend(next.removed);
    into.skipped.extend(next.skipped);
}

#[cfg(test)]
mod tests;
