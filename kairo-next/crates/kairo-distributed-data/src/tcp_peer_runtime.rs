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
mod route_tests {
    use std::net::TcpListener;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use kairo_actor::Address;
    use kairo_cluster::{
        CurrentClusterState, Member, MemberStatus, ReachabilityEvent, UniqueAddress,
    };
    use kairo_remote::RemoteSettings;
    use kairo_serialization::RemoteEnvelope;

    use super::*;
    use crate::{ReplicatorRemoteReplyError, ReplicatorRemoteRequestError};

    #[derive(Default)]
    struct IgnoreRequests;

    impl ReplicatorRemoteRequestReceiver for IgnoreRequests {
        fn receive_request_from(
            &self,
            _from: ReplicaId,
            _envelope: RemoteEnvelope,
        ) -> Result<(), ReplicatorRemoteRequestError> {
            Ok(())
        }
    }

    #[derive(Default)]
    struct IgnoreReplies;

    impl ReplicatorRemoteReplyReceiver for IgnoreReplies {
        fn receive_reply_from(
            &self,
            _from: ReplicaId,
            _envelope: RemoteEnvelope,
        ) -> Result<(), ReplicatorRemoteReplyError> {
            Ok(())
        }
    }

    fn replica(id: &str) -> ReplicaId {
        ReplicaId::new(id)
    }

    fn node(system: &str, port: u16, uid: u64) -> UniqueAddress {
        UniqueAddress::new(
            Address::new("kairo", system, Some("127.0.0.1".to_string()), Some(port)),
            uid,
        )
    }

    fn member(node: UniqueAddress) -> Member {
        Member::new(node, vec![]).with_status(MemberStatus::Up)
    }

    fn state(members: Vec<Member>, unreachable: Vec<Member>) -> CurrentClusterState {
        CurrentClusterState {
            members,
            unreachable,
            seen_by: std::collections::HashSet::new(),
            leader: None,
            role_leaders: std::collections::HashMap::new(),
            member_tombstones: std::collections::HashSet::new(),
        }
    }

    fn unused_port() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    }

    fn bind_peer_runtime(
        name: &str,
        node_uid: u64,
        system_uid: u64,
        settings: RemoteSettings,
        remote_replica: ReplicaId,
        retry_interval: Duration,
    ) -> ReplicatorTcpPeerRuntime {
        ReplicatorTcpPeerRuntime::bind_with_settings(
            name,
            node_uid,
            system_uid,
            remote_replica,
            ReplicatorTcpPeerRuntimeSettings::new(settings)
                .with_reconnect(ReplicatorTcpPeerReconnectSettings::new(retry_interval).unwrap()),
            Arc::new(IgnoreRequests) as Arc<dyn ReplicatorRemoteRequestReceiver>,
            Arc::new(IgnoreReplies) as Arc<dyn ReplicatorRemoteReplyReceiver>,
        )
        .unwrap()
    }

    fn bind_association_runtime_on_port(
        name: &str,
        local: ReplicaId,
        remote: ReplicaId,
        system_uid: u64,
        port: u16,
    ) -> ReplicatorTcpAssociationRuntime {
        ReplicatorTcpAssociationRuntime::bind(
            name,
            local,
            remote,
            system_uid,
            RemoteSettings::new("127.0.0.1", port),
            Arc::new(IgnoreRequests) as Arc<dyn ReplicatorRemoteRequestReceiver>,
            Arc::new(IgnoreReplies) as Arc<dyn ReplicatorRemoteReplyReceiver>,
        )
        .unwrap()
    }

    fn wait_for_reverse_route(runtime: &ReplicatorTcpAssociationRuntime) {
        let deadline = Instant::now() + Duration::from_secs(1);
        while runtime.association_cache().route_count() == 0 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(runtime.association_cache().route_count(), 1);
    }

    #[test]
    fn peer_runtime_applies_snapshot_and_reachability_event_to_live_routes() {
        let retry_interval = Duration::from_millis(25);
        let receiver_port = unused_port();
        let receiver_node = node("receiver", receiver_port, 2);
        let receiver = bind_association_runtime_on_port(
            "receiver",
            ReplicaId::from(&receiver_node),
            replica("sender"),
            22,
            receiver_port,
        );
        let mut sender = bind_peer_runtime(
            "sender",
            1,
            11,
            RemoteSettings::new("127.0.0.1", 0),
            ReplicaId::from(&receiver_node),
            retry_interval,
        );
        let sender_node = sender.self_node().clone();

        let report = sender
            .apply_snapshot(state(
                vec![member(sender_node.clone()), member(receiver_node.clone())],
                vec![],
            ))
            .unwrap();
        assert_eq!(report.dialed.len(), 1);
        assert_eq!(sender.peer_route_count(), 1);
        assert_eq!(sender.association_cache().route_count(), 1);
        wait_for_reverse_route(&receiver);

        let report = sender
            .apply_event(ClusterEvent::Reachability(ReachabilityEvent::Unreachable(
                member(receiver_node),
            )))
            .unwrap();
        assert_eq!(report.removed.len(), 1);
        assert_eq!(sender.peer_route_count(), 0);
        assert_eq!(sender.association_cache().route_count(), 0);

        let sender_report = sender.shutdown().unwrap();
        assert_eq!(sender_report.peer_routes.removed.len(), 0);
        assert_eq!(sender_report.listener.accepted_associations, 0);
        let receiver_report = receiver.shutdown().unwrap();
        assert_eq!(receiver_report.accepted_associations, 1);
    }

    #[test]
    fn peer_runtime_retries_failed_peer_dial_after_retry_interval() {
        let receiver_port = unused_port();
        let receiver_node = node("receiver", receiver_port, 2);
        let retry_interval = Duration::from_millis(25);
        let mut sender = bind_peer_runtime(
            "sender",
            1,
            11,
            RemoteSettings::new("127.0.0.1", 0),
            ReplicaId::from(&receiver_node),
            retry_interval,
        );
        let sender_node = sender.self_node().clone();

        let error = sender
            .apply_snapshot_at(
                state(
                    vec![member(sender_node.clone()), member(receiver_node.clone())],
                    vec![],
                ),
                Duration::ZERO,
            )
            .unwrap_err();

        assert!(matches!(error, ReplicatorTcpPeerRuntimeError::Route(_)));
        assert_eq!(sender.peer_route_count(), 0);
        assert_eq!(sender.pending_peer_reconnect_count(), 1);
        let pending = sender.pending_peer_reconnects();
        assert_eq!(pending[0].target.node(), &receiver_node);
        assert_eq!(pending[0].attempts, 1);
        assert_eq!(pending[0].next_retry_at, retry_interval);

        let report = sender
            .retry_due_peer_routes(retry_interval - Duration::from_millis(1))
            .unwrap();
        assert!(report.is_empty());
        assert_eq!(sender.pending_peer_reconnect_count(), 1);

        let receiver = bind_association_runtime_on_port(
            "receiver",
            ReplicaId::from(&receiver_node),
            ReplicaId::from(&sender_node),
            22,
            receiver_port,
        );
        let report = sender.retry_due_peer_routes(retry_interval).unwrap();

        assert_eq!(report.dialed.len(), 1);
        assert_eq!(sender.peer_route_count(), 1);
        assert_eq!(sender.pending_peer_reconnect_count(), 0);
        wait_for_reverse_route(&receiver);

        let sender_report = sender.shutdown().unwrap();
        assert_eq!(sender_report.peer_routes.removed.len(), 1);
        assert!(sender_report.pending_reconnects.is_empty());
        let receiver_report = receiver.shutdown().unwrap();
        assert_eq!(receiver_report.accepted_associations, 1);
    }

    #[test]
    fn peer_runtime_shutdown_clears_pending_reconnects_after_failed_dial() {
        let receiver_port = unused_port();
        let receiver_node = node("receiver", receiver_port, 2);
        let retry_interval = Duration::from_millis(25);
        let mut sender = bind_peer_runtime(
            "sender",
            1,
            11,
            RemoteSettings::new("127.0.0.1", 0),
            ReplicaId::from(&receiver_node),
            retry_interval,
        );
        let sender_node = sender.self_node().clone();

        sender
            .apply_snapshot_at(
                state(
                    vec![member(sender_node), member(receiver_node.clone())],
                    vec![],
                ),
                Duration::ZERO,
            )
            .unwrap_err();

        assert_eq!(sender.peer_route_count(), 0);
        assert_eq!(sender.pending_peer_reconnect_count(), 1);

        let report = sender.shutdown().unwrap();

        assert!(report.peer_routes.is_empty());
        assert_eq!(report.pending_reconnects.cleared.len(), 1);
        assert_eq!(report.pending_reconnects.cleared[0].node(), &receiver_node);
        assert!(report.pending_reconnects.scheduled.is_empty());
        assert_eq!(report.listener.accepted_associations, 0);
    }

    #[test]
    fn peer_runtime_clears_pending_reconnect_when_peer_is_removed() {
        let receiver_port = unused_port();
        let receiver_node = node("receiver", receiver_port, 2);
        let retry_interval = Duration::from_millis(25);
        let mut runtime = bind_peer_runtime(
            "sender",
            1,
            11,
            RemoteSettings::new("127.0.0.1", 0),
            ReplicaId::from(&receiver_node),
            retry_interval,
        );
        let sender_node = runtime.self_node().clone();

        runtime
            .apply_snapshot_at(
                state(
                    vec![member(sender_node), member(receiver_node.clone())],
                    vec![],
                ),
                Duration::ZERO,
            )
            .unwrap_err();
        assert_eq!(runtime.pending_peer_reconnect_count(), 1);

        let report = runtime
            .apply_event(ClusterEvent::Reachability(ReachabilityEvent::Unreachable(
                member(receiver_node),
            )))
            .unwrap();

        assert_eq!(report.skipped.len(), 1);
        assert_eq!(runtime.pending_peer_reconnect_count(), 0);
        runtime.shutdown().unwrap();
    }
}

#[cfg(test)]
mod basic_tests {
    use std::net::TcpListener;
    use std::time::Instant;

    use kairo_cluster::{Member, MemberStatus, ReachabilityEvent};
    use kairo_serialization::RemoteEnvelope;

    use super::*;
    use crate::{ReplicatorRemoteReplyError, ReplicatorRemoteRequestError};

    #[derive(Default)]
    struct IgnoreRequests;

    impl ReplicatorRemoteRequestReceiver for IgnoreRequests {
        fn receive_request_from(
            &self,
            _from: ReplicaId,
            _envelope: RemoteEnvelope,
        ) -> Result<(), ReplicatorRemoteRequestError> {
            Ok(())
        }
    }

    #[derive(Default)]
    struct IgnoreReplies;

    impl ReplicatorRemoteReplyReceiver for IgnoreReplies {
        fn receive_reply_from(
            &self,
            _from: ReplicaId,
            _envelope: RemoteEnvelope,
        ) -> Result<(), ReplicatorRemoteReplyError> {
            Ok(())
        }
    }

    fn replica(id: &str) -> ReplicaId {
        ReplicaId::new(id)
    }

    fn member(node: UniqueAddress) -> Member {
        Member::new(node, vec![]).with_status(MemberStatus::Up)
    }

    fn state(members: Vec<Member>, unreachable: Vec<Member>) -> CurrentClusterState {
        CurrentClusterState {
            members,
            unreachable,
            seen_by: std::collections::HashSet::new(),
            leader: None,
            role_leaders: std::collections::HashMap::new(),
            member_tombstones: std::collections::HashSet::new(),
        }
    }

    fn unused_port() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    }

    fn node(system: &str, port: u16, uid: u64) -> UniqueAddress {
        UniqueAddress::new(
            Address::new("kairo", system, Some("127.0.0.1".to_string()), Some(port)),
            uid,
        )
    }

    fn bind_peer_runtime(
        name: &str,
        node_uid: u64,
        system_uid: u64,
        remote: ReplicaId,
    ) -> ReplicatorTcpPeerRuntime {
        ReplicatorTcpPeerRuntime::bind(
            name,
            node_uid,
            system_uid,
            remote,
            RemoteSettings::new("127.0.0.1", 0),
            Arc::new(IgnoreRequests) as Arc<dyn ReplicatorRemoteRequestReceiver>,
            Arc::new(IgnoreReplies) as Arc<dyn ReplicatorRemoteReplyReceiver>,
        )
        .unwrap()
    }

    fn bind_peer_runtime_with_reconnect(
        name: &str,
        node_uid: u64,
        system_uid: u64,
        remote: ReplicaId,
        settings: RemoteSettings,
        reconnect_settings: ReplicatorTcpPeerReconnectSettings,
    ) -> ReplicatorTcpPeerRuntime {
        ReplicatorTcpPeerRuntime::bind_with_settings(
            name,
            node_uid,
            system_uid,
            remote,
            ReplicatorTcpPeerRuntimeSettings::new(settings).with_reconnect(reconnect_settings),
            Arc::new(IgnoreRequests) as Arc<dyn ReplicatorRemoteRequestReceiver>,
            Arc::new(IgnoreReplies) as Arc<dyn ReplicatorRemoteReplyReceiver>,
        )
        .unwrap()
    }

    fn bind_association_runtime(
        name: &str,
        local: ReplicaId,
        remote: ReplicaId,
        uid: u64,
    ) -> ReplicatorTcpAssociationRuntime {
        ReplicatorTcpAssociationRuntime::bind(
            name,
            local,
            remote,
            uid,
            RemoteSettings::new("127.0.0.1", 0),
            Arc::new(IgnoreRequests) as Arc<dyn ReplicatorRemoteRequestReceiver>,
            Arc::new(IgnoreReplies) as Arc<dyn ReplicatorRemoteReplyReceiver>,
        )
        .unwrap()
    }

    fn bind_association_runtime_on_port(
        name: &str,
        local: ReplicaId,
        remote: ReplicaId,
        uid: u64,
        port: u16,
    ) -> ReplicatorTcpAssociationRuntime {
        ReplicatorTcpAssociationRuntime::bind(
            name,
            local,
            remote,
            uid,
            RemoteSettings::new("127.0.0.1", port),
            Arc::new(IgnoreRequests) as Arc<dyn ReplicatorRemoteRequestReceiver>,
            Arc::new(IgnoreReplies) as Arc<dyn ReplicatorRemoteReplyReceiver>,
        )
        .unwrap()
    }

    fn wait_for_route(runtime: &ReplicatorTcpAssociationRuntime) {
        let deadline = Instant::now() + Duration::from_secs(1);
        while runtime.association_cache().route_count() == 0 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(runtime.association_cache().route_count(), 1);
    }

    #[test]
    fn peer_runtime_applies_snapshot_and_reachability_event_to_live_routes() {
        let mut sender = bind_peer_runtime("sender", 1, 11, replica("receiver"));
        let receiver =
            bind_association_runtime("receiver", replica("receiver"), replica("sender"), 22);

        assert_eq!(sender.local_replica(), &ReplicaId::from(sender.self_node()));
        let receiver_node = node("receiver", receiver.settings().canonical_port, 2);
        let report = sender
            .apply_snapshot(state(
                vec![
                    member(sender.self_node().clone()),
                    member(receiver_node.clone()),
                ],
                vec![],
            ))
            .unwrap();
        assert_eq!(report.dialed.len(), 1);
        assert_eq!(sender.peer_route_count(), 1);
        assert_eq!(sender.association_cache().route_count(), 1);
        wait_for_route(&receiver);

        let report = sender
            .apply_event(ClusterEvent::Reachability(ReachabilityEvent::Unreachable(
                member(receiver_node),
            )))
            .unwrap();
        assert_eq!(report.removed.len(), 1);
        assert_eq!(sender.peer_route_count(), 0);
        assert_eq!(sender.association_cache().route_count(), 0);

        let sender_report = sender.shutdown().unwrap();
        assert_eq!(sender_report.peer_routes.removed.len(), 0);
        assert_eq!(sender_report.listener.accepted_associations, 0);
        let receiver_report = receiver.shutdown().unwrap();
        assert_eq!(receiver_report.accepted_associations, 1);
    }

    #[test]
    fn peer_runtime_retries_failed_peer_dial_after_retry_interval() {
        let receiver_port = unused_port();
        let retry_interval = Duration::from_millis(25);
        let mut sender = bind_peer_runtime_with_reconnect(
            "sender",
            1,
            11,
            replica("receiver"),
            RemoteSettings::new("127.0.0.1", 0),
            ReplicatorTcpPeerReconnectSettings::new(retry_interval).unwrap(),
        );
        let receiver_node = node("receiver", receiver_port, 2);

        let error = sender
            .apply_snapshot_at(
                state(
                    vec![
                        member(sender.self_node().clone()),
                        member(receiver_node.clone()),
                    ],
                    vec![],
                ),
                Duration::ZERO,
            )
            .unwrap_err();

        assert!(matches!(error, ReplicatorTcpPeerRuntimeError::Route(_)));
        assert_eq!(sender.peer_route_count(), 0);
        assert_eq!(sender.pending_peer_reconnect_count(), 1);
        let pending = sender.pending_peer_reconnects();
        assert_eq!(pending[0].target.node(), &receiver_node);
        assert_eq!(pending[0].attempts, 1);
        assert_eq!(pending[0].next_retry_at, retry_interval);

        let report = sender
            .retry_due_peer_routes(retry_interval - Duration::from_millis(1))
            .unwrap();
        assert!(report.is_empty());
        assert_eq!(sender.pending_peer_reconnect_count(), 1);

        let receiver = bind_association_runtime_on_port(
            "receiver",
            replica("receiver"),
            replica("sender"),
            22,
            receiver_port,
        );
        let report = sender.retry_due_peer_routes(retry_interval).unwrap();

        assert_eq!(report.dialed.len(), 1);
        assert_eq!(sender.peer_route_count(), 1);
        assert_eq!(sender.pending_peer_reconnect_count(), 0);
        wait_for_route(&receiver);

        let sender_report = sender.shutdown().unwrap();
        assert_eq!(sender_report.peer_routes.removed.len(), 1);
        assert!(sender_report.pending_reconnects.is_empty());
        let receiver_report = receiver.shutdown().unwrap();
        assert_eq!(receiver_report.accepted_associations, 1);
    }

    #[test]
    fn peer_runtime_clears_pending_reconnect_when_peer_is_removed() {
        let receiver_port = unused_port();
        let mut runtime = bind_peer_runtime_with_reconnect(
            "sender",
            1,
            11,
            replica("receiver"),
            RemoteSettings::new("127.0.0.1", 0),
            ReplicatorTcpPeerReconnectSettings::new(Duration::from_millis(25)).unwrap(),
        );
        let receiver_node = node("receiver", receiver_port, 2);

        runtime
            .apply_snapshot_at(
                state(
                    vec![
                        member(runtime.self_node().clone()),
                        member(receiver_node.clone()),
                    ],
                    vec![],
                ),
                Duration::ZERO,
            )
            .unwrap_err();
        assert_eq!(runtime.pending_peer_reconnect_count(), 1);

        let report = runtime
            .apply_event(ClusterEvent::Reachability(ReachabilityEvent::Unreachable(
                member(receiver_node),
            )))
            .unwrap();

        assert_eq!(report.skipped.len(), 1);
        assert_eq!(runtime.pending_peer_reconnect_count(), 0);
        runtime.shutdown().unwrap();
    }
}
