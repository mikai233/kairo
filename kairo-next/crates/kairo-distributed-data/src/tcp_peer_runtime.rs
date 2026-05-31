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
pub enum ReplicatorTcpPeerRuntimeError {
    Peer(ClusterAssociationPeerError),
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

pub type ReplicatorTcpPeerRuntimeResult<T> = Result<T, ReplicatorTcpPeerRuntimeError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorTcpPeerRuntimeSettings {
    remote: RemoteSettings,
    reconnect: ReplicatorTcpPeerReconnectSettings,
}

impl ReplicatorTcpPeerRuntimeSettings {
    pub fn new(remote: RemoteSettings) -> Self {
        Self {
            remote,
            reconnect: ReplicatorTcpPeerReconnectSettings::default(),
        }
    }

    pub fn with_reconnect(mut self, reconnect: ReplicatorTcpPeerReconnectSettings) -> Self {
        self.reconnect = reconnect;
        self
    }

    pub fn remote(&self) -> &RemoteSettings {
        &self.remote
    }

    pub fn reconnect(&self) -> &ReplicatorTcpPeerReconnectSettings {
        &self.reconnect
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorTcpPeerRuntimeShutdownReport {
    pub peer_routes: ReplicatorTcpPeerRouteReport,
    pub pending_reconnects: ReplicatorTcpPeerReconnectReport,
    pub listener: TcpAssociationListenerReport,
}

pub struct ReplicatorTcpPeerRuntime {
    runtime: ReplicatorTcpAssociationRuntime,
    peers: ClusterAssociationPeerState,
    routes: ReplicatorTcpPeerRoutes,
    reconnect: ReplicatorTcpPeerReconnectState,
}

impl ReplicatorTcpPeerRuntime {
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

    pub fn runtime(&self) -> &ReplicatorTcpAssociationRuntime {
        &self.runtime
    }

    pub fn self_node(&self) -> &UniqueAddress {
        self.peers.self_node()
    }

    pub fn local_replica(&self) -> &ReplicaId {
        self.runtime.local_replica()
    }

    pub fn remote_replica(&self) -> &ReplicaId {
        self.runtime.remote_replica()
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

    pub fn pending_peer_reconnects(&self) -> Vec<crate::ReplicatorTcpPeerReconnectPending> {
        self.reconnect.pending_reconnects()
    }

    pub fn apply_snapshot(
        &mut self,
        snapshot: CurrentClusterState,
    ) -> ReplicatorTcpPeerRuntimeResult<ReplicatorTcpPeerRouteReport> {
        self.apply_snapshot_at(snapshot, Duration::ZERO)
    }

    pub fn apply_snapshot_at(
        &mut self,
        snapshot: CurrentClusterState,
        now: Duration,
    ) -> ReplicatorTcpPeerRuntimeResult<ReplicatorTcpPeerRouteReport> {
        let changes = self.peers.apply_snapshot(snapshot)?;
        self.apply_route_changes(changes, now)
    }

    pub fn apply_event(
        &mut self,
        event: ClusterEvent,
    ) -> ReplicatorTcpPeerRuntimeResult<ReplicatorTcpPeerRouteReport> {
        self.apply_event_at(event, Duration::ZERO)
    }

    pub fn apply_event_at(
        &mut self,
        event: ClusterEvent,
        now: Duration,
    ) -> ReplicatorTcpPeerRuntimeResult<ReplicatorTcpPeerRouteReport> {
        let changes = self.peers.apply_event(event)?;
        self.apply_route_changes(changes, now)
    }

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

    pub fn clear_pending_peer_reconnects(&mut self) -> ReplicatorTcpPeerReconnectReport {
        ReplicatorTcpPeerReconnectReport {
            scheduled: Vec::new(),
            cleared: self.reconnect.clear_all(),
        }
    }

    pub fn clear_peer_routes(&mut self) -> ReplicatorTcpPeerRouteReport {
        self.routes.clear(&self.runtime)
    }

    pub fn shutdown(self) -> RemoteResult<ReplicatorTcpPeerRuntimeShutdownReport> {
        self.shutdown_with_timeout(Duration::from_secs(1))
    }

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
