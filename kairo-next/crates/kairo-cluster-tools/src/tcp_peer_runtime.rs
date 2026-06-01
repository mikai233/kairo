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
pub enum ClusterToolsTcpPeerRuntimeError {
    Peer(ClusterAssociationPeerError),
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

pub type ClusterToolsTcpPeerRuntimeResult<T> = Result<T, ClusterToolsTcpPeerRuntimeError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterToolsTcpPeerRuntimeShutdownReport {
    pub peer_routes: ClusterToolsTcpPeerRouteReport,
    pub pending_reconnects: ClusterToolsTcpPeerReconnectReport,
    pub listener: TcpAssociationListenerReport,
}

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

    pub fn runtime(&self) -> &ClusterToolsTcpAssociationRuntime<M> {
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

    pub fn active_peer_targets(&self) -> Vec<kairo_cluster::ClusterAssociationPeerTarget> {
        self.routes.active_targets()
    }

    pub fn pending_peer_reconnect_count(&self) -> usize {
        self.reconnect.pending_count()
    }

    pub fn pending_peer_reconnects(&self) -> Vec<crate::ClusterToolsTcpPeerReconnectPending> {
        self.reconnect.pending_reconnects()
    }

    pub fn apply_snapshot(
        &mut self,
        snapshot: CurrentClusterState,
    ) -> ClusterToolsTcpPeerRuntimeResult<ClusterToolsTcpPeerRouteReport> {
        self.apply_snapshot_at(snapshot, Duration::ZERO)
    }

    pub fn apply_snapshot_at(
        &mut self,
        snapshot: CurrentClusterState,
        now: Duration,
    ) -> ClusterToolsTcpPeerRuntimeResult<ClusterToolsTcpPeerRouteReport> {
        let changes = self.peers.apply_snapshot(snapshot)?;
        self.apply_route_changes(changes, now)
    }

    pub fn apply_event(
        &mut self,
        event: ClusterEvent,
    ) -> ClusterToolsTcpPeerRuntimeResult<ClusterToolsTcpPeerRouteReport> {
        self.apply_event_at(event, Duration::ZERO)
    }

    pub fn apply_event_at(
        &mut self,
        event: ClusterEvent,
        now: Duration,
    ) -> ClusterToolsTcpPeerRuntimeResult<ClusterToolsTcpPeerRouteReport> {
        let changes = self.peers.apply_event(event)?;
        self.apply_route_changes(changes, now)
    }

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

    pub fn clear_pending_peer_reconnects(&mut self) -> ClusterToolsTcpPeerReconnectReport {
        ClusterToolsTcpPeerReconnectReport {
            scheduled: Vec::new(),
            cleared: self.reconnect.clear_all(),
        }
    }

    pub fn clear_peer_routes(&mut self) -> ClusterToolsTcpPeerRouteReport {
        self.routes.clear(&self.runtime)
    }

    pub fn shutdown(self) -> RemoteResult<ClusterToolsTcpPeerRuntimeShutdownReport> {
        self.shutdown_with_timeout(Duration::from_secs(1))
    }

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
