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
mod tests {
    use std::net::TcpListener;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use bytes::Bytes;
    use kairo_actor::Address;
    use kairo_cluster::{
        CurrentClusterState, Member, MemberStatus, ReachabilityEvent, UniqueAddress,
    };
    use kairo_remote::RemoteSettings;
    use kairo_serialization::{MessageCodec, Registry, SerializationRegistry};
    use kairo_testkit::ActorSystemTestKit;

    use super::*;
    use crate::{
        ClusterToolsSystemInbound, DistributedPubSubMediatorMsg, PubSubGossipMsg,
        PubSubGossipWireInbound, PubSubRemoteDeliveryInbound, SingletonManagerMsg,
        SingletonManagerRemoteInbound, register_cluster_tools_protocol_codecs,
    };

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestMessage {
        value: u8,
    }

    impl RemoteMessage for TestMessage {
        const MANIFEST: &'static str = "kairo.cluster-tools.test.peer-runtime-message";
        const VERSION: u16 = 1;
    }

    #[derive(Debug, Clone, Copy)]
    struct TestMessageCodec;

    impl MessageCodec<TestMessage> for TestMessageCodec {
        fn serializer_id(&self) -> u32 {
            59_203
        }

        fn encode(&self, message: &TestMessage) -> kairo_serialization::Result<Bytes> {
            Ok(Bytes::from(vec![message.value]))
        }

        fn decode(
            &self,
            payload: Bytes,
            _version: u16,
        ) -> kairo_serialization::Result<TestMessage> {
            Ok(TestMessage { value: payload[0] })
        }
    }

    fn registry() -> Arc<Registry> {
        let mut registry = Registry::new();
        register_cluster_tools_protocol_codecs(&mut registry).unwrap();
        registry
            .register::<TestMessage, _>(TestMessageCodec)
            .unwrap();
        Arc::new(registry)
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

    fn wait_for_route(runtime: &ClusterToolsTcpAssociationRuntime<TestMessage>) {
        let deadline = Instant::now() + Duration::from_secs(1);
        while runtime.association_cache().route_count() == 0 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(runtime.association_cache().route_count(), 1);
    }

    fn inbound_for(
        name: &str,
        kit: &ActorSystemTestKit,
        registry: Arc<Registry>,
        self_node: UniqueAddress,
    ) -> ClusterToolsSystemInbound<TestMessage> {
        let gossip = kit
            .create_probe::<PubSubGossipMsg>(format!("{name}-gossip"))
            .unwrap();
        let mediator = kit
            .create_probe::<DistributedPubSubMediatorMsg<TestMessage>>(format!("{name}-mediator"))
            .unwrap();
        let manager = kit
            .create_probe::<SingletonManagerMsg>(format!("{name}-singleton-manager"))
            .unwrap();
        ClusterToolsSystemInbound::new(self_node.clone())
            .with_pubsub_gossip(PubSubGossipWireInbound::new(
                self_node.clone(),
                registry.clone(),
                gossip.actor_ref(),
            ))
            .with_pubsub_delivery(PubSubRemoteDeliveryInbound::new(
                self_node.clone(),
                registry.clone(),
                mediator.actor_ref(),
            ))
            .with_singleton_manager(SingletonManagerRemoteInbound::new(
                self_node,
                registry,
                manager.actor_ref(),
            ))
    }

    fn bind_peer_runtime(
        name: &str,
        uid: u64,
        system_uid: u64,
        kit: &ActorSystemTestKit,
        registry: Arc<Registry>,
    ) -> ClusterToolsTcpPeerRuntime<TestMessage> {
        ClusterToolsTcpPeerRuntime::bind(
            name,
            uid,
            system_uid,
            RemoteSettings::new("127.0.0.1", 0),
            move |self_node| inbound_for(name, kit, registry, self_node),
        )
        .unwrap()
    }

    fn bind_peer_runtime_with_reconnect(
        name: &str,
        uid: u64,
        system_uid: u64,
        settings: RemoteSettings,
        reconnect_settings: ClusterToolsTcpPeerReconnectSettings,
        kit: &ActorSystemTestKit,
        registry: Arc<Registry>,
    ) -> ClusterToolsTcpPeerRuntime<TestMessage> {
        ClusterToolsTcpPeerRuntime::bind_with_reconnect(
            name,
            uid,
            system_uid,
            settings,
            reconnect_settings,
            move |self_node| inbound_for(name, kit, registry, self_node),
        )
        .unwrap()
    }

    fn bind_association_runtime(
        name: &str,
        uid: u64,
        system_uid: u64,
        kit: &ActorSystemTestKit,
        registry: Arc<Registry>,
    ) -> ClusterToolsTcpAssociationRuntime<TestMessage> {
        ClusterToolsTcpAssociationRuntime::bind(
            name,
            uid,
            system_uid,
            RemoteSettings::new("127.0.0.1", 0),
            move |self_node| inbound_for(name, kit, registry, self_node),
        )
        .unwrap()
    }

    fn bind_association_runtime_on_port(
        name: &str,
        uid: u64,
        system_uid: u64,
        port: u16,
        kit: &ActorSystemTestKit,
        registry: Arc<Registry>,
    ) -> ClusterToolsTcpAssociationRuntime<TestMessage> {
        ClusterToolsTcpAssociationRuntime::bind(
            name,
            uid,
            system_uid,
            RemoteSettings::new("127.0.0.1", port),
            move |self_node| inbound_for(name, kit, registry, self_node),
        )
        .unwrap()
    }

    #[test]
    fn peer_runtime_applies_snapshot_and_reachability_event_to_live_routes() {
        let sender_kit = ActorSystemTestKit::new("cluster-tools-peer-runtime-sender").unwrap();
        let receiver_kit = ActorSystemTestKit::new("cluster-tools-peer-runtime-receiver").unwrap();
        let registry = registry();
        let mut sender = bind_peer_runtime("sender", 1, 11, &sender_kit, registry.clone());
        let receiver = bind_association_runtime("receiver", 2, 22, &receiver_kit, registry);

        let report = sender
            .apply_snapshot(state(
                vec![
                    member(sender.self_node().clone()),
                    member(receiver.self_node().clone()),
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
                member(receiver.self_node().clone()),
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
        sender_kit.shutdown(Duration::from_secs(1)).unwrap();
        receiver_kit.shutdown(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn peer_runtime_retries_failed_peer_dial_after_retry_interval() {
        let sender_kit =
            ActorSystemTestKit::new("cluster-tools-peer-runtime-retry-sender").unwrap();
        let receiver_kit =
            ActorSystemTestKit::new("cluster-tools-peer-runtime-retry-receiver").unwrap();
        let registry = registry();
        let receiver_port = unused_port();
        let retry_interval = Duration::from_millis(25);
        let mut sender = bind_peer_runtime_with_reconnect(
            "sender",
            1,
            11,
            RemoteSettings::new("127.0.0.1", 0),
            ClusterToolsTcpPeerReconnectSettings::new(retry_interval).unwrap(),
            &sender_kit,
            registry.clone(),
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

        assert!(matches!(error, ClusterToolsTcpPeerRuntimeError::Route(_)));
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
            2,
            22,
            receiver_port,
            &receiver_kit,
            registry,
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
        sender_kit.shutdown(Duration::from_secs(1)).unwrap();
        receiver_kit.shutdown(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn peer_runtime_clears_pending_reconnect_when_peer_is_removed() {
        let kit = ActorSystemTestKit::new("cluster-tools-peer-runtime-retry-removed").unwrap();
        let registry = registry();
        let receiver_port = unused_port();
        let mut runtime = bind_peer_runtime_with_reconnect(
            "sender",
            1,
            11,
            RemoteSettings::new("127.0.0.1", 0),
            ClusterToolsTcpPeerReconnectSettings::new(Duration::from_millis(25)).unwrap(),
            &kit,
            registry,
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
        kit.shutdown(Duration::from_secs(1)).unwrap();
    }
}
