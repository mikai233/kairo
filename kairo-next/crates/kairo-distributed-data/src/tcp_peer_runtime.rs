#![deny(missing_docs)]

use std::fmt::{self, Display, Formatter};
use std::sync::Arc;
use std::time::Duration;

use kairo_actor::Address;
use kairo_cluster::{
    ClusterAssociationPeerChange, ClusterAssociationPeerError, ClusterAssociationPeerState,
    ClusterEvent, CurrentClusterState, UniqueAddress,
};
use kairo_remote::{
    RemoteAssociationAddress, RemoteAssociationCache, RemoteAssociationRegistry, RemoteSettings,
    Result as RemoteResult, TcpAssociationListenerReport,
};

use crate::{
    ReplicaId, ReplicatorRemoteReplyReceiver, ReplicatorRemoteRequestReceiver,
    ReplicatorTcpAssociationRuntime, ReplicatorTcpPeerReconnectReport,
    ReplicatorTcpPeerReconnectSettings, ReplicatorTcpPeerReconnectState,
    ReplicatorTcpPeerRouteError, ReplicatorTcpPeerRouteReport, ReplicatorTcpPeerRoutes,
};

#[derive(Debug)]
/// Failure while projecting cluster membership or applying distributed-data TCP route intent.
pub enum ReplicatorTcpPeerRuntimeError {
    /// A cluster member could not be converted to a remote peer target.
    Peer(ClusterAssociationPeerError),
    /// A derived distributed-data route operation failed.
    Route(ReplicatorTcpPeerRouteError),
}

impl Display for ReplicatorTcpPeerRuntimeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Peer(error) => write!(f, "{error}"),
            Self::Route(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for ReplicatorTcpPeerRuntimeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Peer(error) => Some(error),
            Self::Route(error) => Some(error),
        }
    }
}

impl From<ClusterAssociationPeerError> for ReplicatorTcpPeerRuntimeError {
    fn from(error: ClusterAssociationPeerError) -> Self {
        Self::Peer(error)
    }
}

impl From<ReplicatorTcpPeerRouteError> for ReplicatorTcpPeerRuntimeError {
    fn from(error: ReplicatorTcpPeerRouteError) -> Self {
        Self::Route(error)
    }
}

/// Result of applying membership or reconnect work to the distributed-data peer runtime.
pub type ReplicatorTcpPeerRuntimeResult<T> = Result<T, ReplicatorTcpPeerRuntimeError>;

#[derive(Debug, Clone, PartialEq, Eq)]
/// Listener and reconnect settings for a distributed-data TCP peer runtime.
pub struct ReplicatorTcpPeerRuntimeSettings {
    remote: RemoteSettings,
    reconnect: ReplicatorTcpPeerReconnectSettings,
}

impl ReplicatorTcpPeerRuntimeSettings {
    /// Creates runtime settings with the default fixed-interval reconnect policy.
    pub fn new(remote: RemoteSettings) -> Self {
        Self {
            remote,
            reconnect: ReplicatorTcpPeerReconnectSettings::default(),
        }
    }

    /// Replaces the fixed-interval reconnect policy.
    pub fn with_reconnect(mut self, reconnect: ReplicatorTcpPeerReconnectSettings) -> Self {
        self.reconnect = reconnect;
        self
    }

    /// Returns listener and outbound connection settings.
    pub fn remote(&self) -> &RemoteSettings {
        &self.remote
    }

    /// Returns the fixed-interval reconnect policy.
    pub fn reconnect(&self) -> &ReplicatorTcpPeerReconnectSettings {
        &self.reconnect
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Cleanup completed while shutting down a distributed-data TCP peer runtime.
pub struct ReplicatorTcpPeerRuntimeShutdownReport {
    /// Managed peer routes removed before listener shutdown.
    pub peer_routes: ReplicatorTcpPeerRouteReport,
    /// Pending reconnect deadlines cleared before listener shutdown.
    pub pending_reconnects: ReplicatorTcpPeerReconnectReport,
    /// Accepted-association report returned by the underlying listener.
    pub listener: TcpAssociationListenerReport,
}

/// Synchronous composition of membership projection, distributed-data routes, and reconnect state.
///
/// The runtime does not subscribe to cluster events by itself. Callers feed snapshots and events,
/// and the runtime derives transport intent without treating associations as membership truth.
pub struct ReplicatorTcpPeerRuntime {
    runtime: ReplicatorTcpAssociationRuntime,
    peers: ClusterAssociationPeerState,
    routes: ReplicatorTcpPeerRoutes,
    reconnect: ReplicatorTcpPeerReconnectState,
}

impl ReplicatorTcpPeerRuntime {
    /// Binds a runtime with the default fixed-interval reconnect policy.
    pub fn bind(
        local_system: impl Into<String>,
        node_uid: u64,
        local_system_uid: u64,
        remote_replica: ReplicaId,
        settings: RemoteSettings,
        requests: Arc<dyn ReplicatorRemoteRequestReceiver>,
        replies: Arc<dyn ReplicatorRemoteReplyReceiver>,
    ) -> RemoteResult<Self> {
        Self::bind_with_settings(
            local_system,
            node_uid,
            local_system_uid,
            remote_replica,
            ReplicatorTcpPeerRuntimeSettings::new(settings),
            requests,
            replies,
        )
    }

    /// Binds a runtime with explicit listener and reconnect settings.
    pub fn bind_with_settings(
        local_system: impl Into<String>,
        node_uid: u64,
        local_system_uid: u64,
        remote_replica: ReplicaId,
        settings: ReplicatorTcpPeerRuntimeSettings,
        requests: Arc<dyn ReplicatorRemoteRequestReceiver>,
        replies: Arc<dyn ReplicatorRemoteReplyReceiver>,
    ) -> RemoteResult<Self> {
        let local_system = local_system.into();
        let runtime = ReplicatorTcpAssociationRuntime::bind(
            local_system.clone(),
            ReplicaId::new(""),
            remote_replica,
            local_system_uid,
            settings.remote,
            requests,
            replies,
        )?;
        let self_node = UniqueAddress::new(
            Address::new(
                "kairo",
                local_system,
                Some(runtime.settings().canonical_hostname.clone()),
                Some(runtime.settings().canonical_port),
            ),
            node_uid,
        );
        let local_replica = ReplicaId::from(&self_node);
        let runtime = runtime.with_local_replica(local_replica);
        let peers = ClusterAssociationPeerState::new(self_node);
        Ok(Self {
            runtime,
            peers,
            routes: ReplicatorTcpPeerRoutes::new(),
            reconnect: ReplicatorTcpPeerReconnectState::new(settings.reconnect),
        })
    }

    /// Returns the owned lower-level distributed-data TCP association runtime.
    pub fn runtime(&self) -> &ReplicatorTcpAssociationRuntime {
        &self.runtime
    }

    /// Returns the canonical local cluster member identity.
    pub fn self_node(&self) -> &UniqueAddress {
        self.peers.self_node()
    }

    /// Returns the local replica identity derived from the cluster member incarnation.
    pub fn local_replica(&self) -> &ReplicaId {
        self.runtime.local_replica()
    }

    /// Returns the fallback remote replica used before a source mapping is available.
    pub fn remote_replica(&self) -> &ReplicaId {
        self.runtime.remote_replica()
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
    pub fn pending_peer_reconnects(&self) -> Vec<crate::ReplicatorTcpPeerReconnectPending> {
        self.reconnect.pending_reconnects()
    }

    /// Applies a full cluster snapshot using zero as the reconnect clock value.
    pub fn apply_snapshot(
        &mut self,
        snapshot: CurrentClusterState,
    ) -> ReplicatorTcpPeerRuntimeResult<ReplicatorTcpPeerRouteReport> {
        self.apply_snapshot_at(snapshot, Duration::ZERO)
    }

    /// Applies a full cluster snapshot and schedules failed dials relative to `now`.
    pub fn apply_snapshot_at(
        &mut self,
        snapshot: CurrentClusterState,
        now: Duration,
    ) -> ReplicatorTcpPeerRuntimeResult<ReplicatorTcpPeerRouteReport> {
        let changes = self.peers.apply_snapshot(snapshot)?;
        self.apply_route_changes(changes, now)
    }

    /// Applies one cluster-domain event using zero as the reconnect clock value.
    pub fn apply_event(
        &mut self,
        event: ClusterEvent,
    ) -> ReplicatorTcpPeerRuntimeResult<ReplicatorTcpPeerRouteReport> {
        self.apply_event_at(event, Duration::ZERO)
    }

    /// Applies one cluster-domain event and schedules failed dials relative to `now`.
    pub fn apply_event_at(
        &mut self,
        event: ClusterEvent,
        now: Duration,
    ) -> ReplicatorTcpPeerRuntimeResult<ReplicatorTcpPeerRouteReport> {
        let changes = self.peers.apply_event(event)?;
        self.apply_route_changes(changes, now)
    }

    /// Retries every failed peer dial whose deadline is at or before `now`.
    pub fn retry_due_peer_routes(
        &mut self,
        now: Duration,
    ) -> ReplicatorTcpPeerRuntimeResult<ReplicatorTcpPeerRouteReport> {
        let targets = self.reconnect.due_targets(now);
        self.apply_route_changes(
            targets.into_iter().map(ClusterAssociationPeerChange::Dial),
            now,
        )
    }

    /// Clears every pending reconnect deadline without removing active routes.
    pub fn clear_pending_peer_reconnects(&mut self) -> ReplicatorTcpPeerReconnectReport {
        ReplicatorTcpPeerReconnectReport {
            scheduled: Vec::new(),
            cleared: self.reconnect.clear_all(),
        }
    }

    /// Removes every managed peer route without clearing reconnect deadlines.
    pub fn clear_peer_routes(&mut self) -> ReplicatorTcpPeerRouteReport {
        self.routes.clear(&self.runtime)
    }

    /// Clears peer state and stops the TCP association runtime with its default policy.
    pub fn shutdown(self) -> RemoteResult<ReplicatorTcpPeerRuntimeShutdownReport> {
        self.shutdown_with_timeout(Duration::from_secs(1))
    }

    /// Clears reconnects and routes before stopping the TCP association runtime.
    ///
    /// `timeout` is forwarded to the lower-level runtime, which owns listener shutdown semantics.
    pub fn shutdown_with_timeout(
        mut self,
        timeout: Duration,
    ) -> RemoteResult<ReplicatorTcpPeerRuntimeShutdownReport> {
        let pending_reconnects = self.clear_pending_peer_reconnects();
        let peer_routes = self.clear_peer_routes();
        let listener = self.runtime.shutdown_with_timeout(timeout)?;
        Ok(ReplicatorTcpPeerRuntimeShutdownReport {
            peer_routes,
            pending_reconnects,
            listener,
        })
    }

    fn apply_route_changes(
        &mut self,
        changes: impl IntoIterator<Item = ClusterAssociationPeerChange>,
        now: Duration,
    ) -> ReplicatorTcpPeerRuntimeResult<ReplicatorTcpPeerRouteReport> {
        let mut report = ReplicatorTcpPeerRouteReport::default();
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

fn merge_route_report(into: &mut ReplicatorTcpPeerRouteReport, next: ReplicatorTcpPeerRouteReport) {
    into.dialed.extend(next.dialed);
    into.removed.extend(next.removed);
    into.skipped.extend(next.skipped);
}

#[cfg(test)]
mod tests;
