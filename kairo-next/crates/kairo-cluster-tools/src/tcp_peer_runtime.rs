#![deny(missing_docs)]

use std::fmt::{self, Display, Formatter};
use std::time::Duration;

use kairo_cluster::{
    ClusterAssociationPeerChange, ClusterAssociationPeerError, ClusterAssociationPeerState,
    ClusterEvent, CurrentClusterState, UniqueAddress,
};
use kairo_remote::{
    RemoteAssociationAddress, RemoteAssociationCache, RemoteAssociationRegistry, RemoteSettings,
    Result as RemoteResult, TcpAssociationListenerReport,
};
use kairo_serialization::RemoteMessage;

use crate::{
    ClusterToolsSystemInbound, ClusterToolsTcpAssociationRuntime,
    ClusterToolsTcpPeerReconnectReport, ClusterToolsTcpPeerReconnectSettings,
    ClusterToolsTcpPeerReconnectState, ClusterToolsTcpPeerRouteError,
    ClusterToolsTcpPeerRouteReport, ClusterToolsTcpPeerRoutes,
};

#[derive(Debug)]
/// Failure while projecting cluster membership or applying cluster-tools TCP route intent.
pub enum ClusterToolsTcpPeerRuntimeError {
    /// A cluster member could not be converted to a remote peer target.
    Peer(ClusterAssociationPeerError),
    /// A derived TCP route operation failed.
    Route(ClusterToolsTcpPeerRouteError),
}

impl Display for ClusterToolsTcpPeerRuntimeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Peer(error) => write!(f, "{error}"),
            Self::Route(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for ClusterToolsTcpPeerRuntimeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Peer(error) => Some(error),
            Self::Route(error) => Some(error),
        }
    }
}

impl From<ClusterAssociationPeerError> for ClusterToolsTcpPeerRuntimeError {
    fn from(error: ClusterAssociationPeerError) -> Self {
        Self::Peer(error)
    }
}

impl From<ClusterToolsTcpPeerRouteError> for ClusterToolsTcpPeerRuntimeError {
    fn from(error: ClusterToolsTcpPeerRouteError) -> Self {
        Self::Route(error)
    }
}

/// Result of applying membership or reconnect work to the TCP peer runtime.
pub type ClusterToolsTcpPeerRuntimeResult<T> = Result<T, ClusterToolsTcpPeerRuntimeError>;

#[derive(Debug, Clone, PartialEq, Eq)]
/// Cleanup completed while shutting down a membership-driven cluster-tools runtime.
pub struct ClusterToolsTcpPeerRuntimeShutdownReport {
    /// Managed peer routes removed before listener shutdown.
    pub peer_routes: ClusterToolsTcpPeerRouteReport,
    /// Pending reconnect deadlines cleared before listener shutdown.
    pub pending_reconnects: ClusterToolsTcpPeerReconnectReport,
    /// Accepted-association report returned by the underlying listener.
    pub listener: TcpAssociationListenerReport,
}

/// Synchronous composition of membership projection, TCP routes, and reconnect state.
///
/// The runtime does not subscribe to cluster events by itself. Callers feed
/// snapshots and events, and the runtime derives transport intent without
/// treating associations as membership truth.
pub struct ClusterToolsTcpPeerRuntime<M>
where
    M: RemoteMessage + Send + 'static,
{
    runtime: ClusterToolsTcpAssociationRuntime<M>,
    peers: ClusterAssociationPeerState,
    routes: ClusterToolsTcpPeerRoutes,
    reconnect: ClusterToolsTcpPeerReconnectState,
}

impl<M> ClusterToolsTcpPeerRuntime<M>
where
    M: RemoteMessage + Send + 'static,
{
    /// Binds a runtime with the default fixed-interval reconnect policy.
    ///
    /// # Errors
    ///
    /// Returns an error when the underlying cluster-tools association runtime
    /// cannot bind or start its listener.
    pub fn bind(
        local_system: impl Into<String>,
        node_uid: u64,
        local_system_uid: u64,
        settings: RemoteSettings,
        inbound: impl FnOnce(UniqueAddress) -> ClusterToolsSystemInbound<M>,
    ) -> RemoteResult<Self> {
        let runtime = ClusterToolsTcpAssociationRuntime::bind(
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
            routes: ClusterToolsTcpPeerRoutes::new(),
            reconnect: ClusterToolsTcpPeerReconnectState::default(),
        })
    }

    /// Binds a runtime with an explicit fixed-interval reconnect policy.
    ///
    /// # Errors
    ///
    /// Returns an error when the underlying cluster-tools association runtime
    /// cannot bind or start its listener.
    pub fn bind_with_reconnect(
        local_system: impl Into<String>,
        node_uid: u64,
        local_system_uid: u64,
        settings: RemoteSettings,
        reconnect_settings: ClusterToolsTcpPeerReconnectSettings,
        inbound: impl FnOnce(UniqueAddress) -> ClusterToolsSystemInbound<M>,
    ) -> RemoteResult<Self> {
        let runtime = ClusterToolsTcpAssociationRuntime::bind(
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
            routes: ClusterToolsTcpPeerRoutes::new(),
            reconnect: ClusterToolsTcpPeerReconnectState::new(reconnect_settings),
        })
    }

    /// Returns the owned lower-level TCP association runtime.
    pub fn runtime(&self) -> &ClusterToolsTcpAssociationRuntime<M> {
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
    pub fn active_peer_targets(&self) -> Vec<kairo_cluster::ClusterAssociationPeerTarget> {
        self.routes.active_targets()
    }

    /// Returns the number of failed peer dials waiting for retry.
    pub fn pending_peer_reconnect_count(&self) -> usize {
        self.reconnect.pending_count()
    }

    /// Returns pending reconnects in deterministic member order.
    pub fn pending_peer_reconnects(&self) -> Vec<crate::ClusterToolsTcpPeerReconnectPending> {
        self.reconnect.pending_reconnects()
    }

    /// Applies a full cluster snapshot using zero as the reconnect clock value.
    ///
    /// # Errors
    ///
    /// Returns an error when membership projection or a derived dial fails.
    pub fn apply_snapshot(
        &mut self,
        snapshot: CurrentClusterState,
    ) -> ClusterToolsTcpPeerRuntimeResult<ClusterToolsTcpPeerRouteReport> {
        self.apply_snapshot_at(snapshot, Duration::ZERO)
    }

    /// Applies a full cluster snapshot and schedules failed dials relative to `now`.
    ///
    /// # Errors
    ///
    /// Returns an error when membership projection or a derived dial fails.
    pub fn apply_snapshot_at(
        &mut self,
        snapshot: CurrentClusterState,
        now: Duration,
    ) -> ClusterToolsTcpPeerRuntimeResult<ClusterToolsTcpPeerRouteReport> {
        let changes = self.peers.apply_snapshot(snapshot)?;
        self.apply_route_changes(changes, now)
    }

    /// Applies one cluster-domain event using zero as the reconnect clock value.
    ///
    /// # Errors
    ///
    /// Returns an error when membership projection or a derived dial fails.
    pub fn apply_event(
        &mut self,
        event: ClusterEvent,
    ) -> ClusterToolsTcpPeerRuntimeResult<ClusterToolsTcpPeerRouteReport> {
        self.apply_event_at(event, Duration::ZERO)
    }

    /// Applies one cluster-domain event and schedules failed dials relative to `now`.
    ///
    /// # Errors
    ///
    /// Returns an error when membership projection or a derived dial fails.
    pub fn apply_event_at(
        &mut self,
        event: ClusterEvent,
        now: Duration,
    ) -> ClusterToolsTcpPeerRuntimeResult<ClusterToolsTcpPeerRouteReport> {
        let changes = self.peers.apply_event(event)?;
        self.apply_route_changes(changes, now)
    }

    /// Retries every failed peer dial whose deadline is at or before `now`.
    ///
    /// # Errors
    ///
    /// Returns the first derived route dial failure.
    pub fn retry_due_peer_routes(
        &mut self,
        now: Duration,
    ) -> ClusterToolsTcpPeerRuntimeResult<ClusterToolsTcpPeerRouteReport> {
        let targets = self.reconnect.due_targets(now);
        self.apply_route_changes(
            targets.into_iter().map(ClusterAssociationPeerChange::Dial),
            now,
        )
    }

    /// Clears every pending reconnect deadline without removing active routes.
    pub fn clear_pending_peer_reconnects(&mut self) -> ClusterToolsTcpPeerReconnectReport {
        ClusterToolsTcpPeerReconnectReport {
            scheduled: Vec::new(),
            cleared: self.reconnect.clear_all(),
        }
    }

    /// Removes every managed peer route without clearing reconnect deadlines.
    pub fn clear_peer_routes(&mut self) -> ClusterToolsTcpPeerRouteReport {
        self.routes.clear(&self.runtime)
    }

    /// Clears peer state and stops the TCP association runtime with its default policy.
    ///
    /// # Errors
    ///
    /// Returns the first route-close or listener failure.
    pub fn shutdown(self) -> RemoteResult<ClusterToolsTcpPeerRuntimeShutdownReport> {
        self.shutdown_with_timeout(Duration::from_secs(1))
    }

    /// Clears reconnects and routes before stopping the TCP association runtime.
    ///
    /// `timeout` is forwarded to the lower-level standalone runtime for API
    /// symmetry; that runtime currently does not enforce a shutdown deadline.
    ///
    /// # Errors
    ///
    /// Returns the first route-close or listener failure.
    pub fn shutdown_with_timeout(
        mut self,
        timeout: Duration,
    ) -> RemoteResult<ClusterToolsTcpPeerRuntimeShutdownReport> {
        let pending_reconnects = self.clear_pending_peer_reconnects();
        let peer_routes = self.clear_peer_routes();
        let listener = self.runtime.shutdown_with_timeout(timeout)?;
        Ok(ClusterToolsTcpPeerRuntimeShutdownReport {
            peer_routes,
            pending_reconnects,
            listener,
        })
    }

    fn apply_route_changes(
        &mut self,
        changes: impl IntoIterator<Item = ClusterAssociationPeerChange>,
        now: Duration,
    ) -> ClusterToolsTcpPeerRuntimeResult<ClusterToolsTcpPeerRouteReport> {
        let mut report = ClusterToolsTcpPeerRouteReport::default();
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

fn merge_route_report(
    into: &mut ClusterToolsTcpPeerRouteReport,
    next: ClusterToolsTcpPeerRouteReport,
) {
    into.dialed.extend(next.dialed);
    into.removed.extend(next.removed);
    into.skipped.extend(next.skipped);
}

#[cfg(test)]
mod tests;
