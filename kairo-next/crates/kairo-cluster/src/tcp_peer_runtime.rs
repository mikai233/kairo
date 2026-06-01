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
pub enum ClusterTcpPeerRuntimeError {
    Peer(ClusterAssociationPeerError),
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

pub type ClusterTcpPeerRuntimeResult<T> = Result<T, ClusterTcpPeerRuntimeError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterTcpPeerRuntimeShutdownReport {
    pub peer_routes: ClusterTcpPeerRouteReport,
    pub pending_reconnects: ClusterTcpPeerReconnectReport,
    pub listener: TcpAssociationListenerReport,
}

pub struct ClusterTcpPeerRuntime {
    runtime: ClusterTcpAssociationRuntime,
    peers: ClusterAssociationPeerState,
    routes: ClusterTcpPeerRoutes,
    reconnect: ClusterTcpPeerReconnectState,
}

impl ClusterTcpPeerRuntime {
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

    pub fn runtime(&self) -> &ClusterTcpAssociationRuntime {
        &self.runtime
    }

    pub fn self_node(&self) -> &UniqueAddress {
        self.runtime.self_node()
    }

    pub fn local_address(&self) -> &RemoteAssociationAddress {
        self.runtime.local_address()
    }

    pub fn association_cache(&self) -> &RemoteAssociationCache {
        self.runtime.association_cache()
    }

    pub fn association_registry(&self) -> &RemoteAssociationRegistry {
        self.runtime.association_registry()
    }

    pub fn peer_route_count(&self) -> usize {
        self.routes.route_count()
    }

    pub fn active_peer_targets(&self) -> Vec<crate::ClusterAssociationPeerTarget> {
        self.routes.active_targets()
    }

    pub fn pending_peer_reconnect_count(&self) -> usize {
        self.reconnect.pending_count()
    }

    pub fn pending_peer_reconnects(&self) -> Vec<crate::ClusterTcpPeerReconnectPending> {
        self.reconnect.pending_reconnects()
    }

    pub fn apply_snapshot(
        &mut self,
        snapshot: CurrentClusterState,
    ) -> ClusterTcpPeerRuntimeResult<ClusterTcpPeerRouteReport> {
        self.apply_snapshot_at(snapshot, Duration::ZERO)
    }

    pub fn apply_snapshot_at(
        &mut self,
        snapshot: CurrentClusterState,
        now: Duration,
    ) -> ClusterTcpPeerRuntimeResult<ClusterTcpPeerRouteReport> {
        let changes = self.peers.apply_snapshot(snapshot)?;
        self.apply_route_changes(changes, now)
    }

    pub fn apply_event(
        &mut self,
        event: ClusterEvent,
    ) -> ClusterTcpPeerRuntimeResult<ClusterTcpPeerRouteReport> {
        self.apply_event_at(event, Duration::ZERO)
    }

    pub fn apply_event_at(
        &mut self,
        event: ClusterEvent,
        now: Duration,
    ) -> ClusterTcpPeerRuntimeResult<ClusterTcpPeerRouteReport> {
        let changes = self.peers.apply_event(event)?;
        self.apply_route_changes(changes, now)
    }

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

    pub fn clear_pending_peer_reconnects(&mut self) -> ClusterTcpPeerReconnectReport {
        ClusterTcpPeerReconnectReport {
            scheduled: Vec::new(),
            cleared: self.reconnect.clear_all(),
        }
    }

    pub fn clear_peer_routes(&mut self) -> ClusterTcpPeerRouteReport {
        self.routes.clear(&self.runtime)
    }

    pub fn shutdown(self) -> RemoteResult<ClusterTcpPeerRuntimeShutdownReport> {
        self.shutdown_with_timeout(Duration::from_secs(1))
    }

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
