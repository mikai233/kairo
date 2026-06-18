use std::collections::BTreeMap;
use std::fmt::{self, Display, Formatter};

use kairo_cluster::{ClusterAssociationPeerChange, ClusterAssociationPeerTarget};
use kairo_remote::{RemoteAssociationRouteRegistration, RemoteError};
use kairo_serialization::RemoteMessage;

use crate::ClusterToolsTcpAssociationRuntime;

#[derive(Debug)]
pub enum ClusterToolsTcpPeerRouteError {
    Dial {
        target: Box<ClusterAssociationPeerTarget>,
        source: Box<RemoteError>,
    },
}

impl Display for ClusterToolsTcpPeerRouteError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Dial { target, source } => write!(
                f,
                "cluster-tools tcp peer dial to {} failed: {source}",
                target.as_ref().node().ordering_key()
            ),
        }
    }
}

impl std::error::Error for ClusterToolsTcpPeerRouteError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Dial { source, .. } => Some(source.as_ref()),
        }
    }
}

impl ClusterToolsTcpPeerRouteError {
    pub fn target(&self) -> &ClusterAssociationPeerTarget {
        match self {
            Self::Dial { target, .. } => target.as_ref(),
        }
    }
}

pub type ClusterToolsTcpPeerRouteResult<T> = Result<T, ClusterToolsTcpPeerRouteError>;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ClusterToolsTcpPeerRouteReport {
    pub dialed: Vec<ClusterAssociationPeerTarget>,
    pub removed: Vec<ClusterAssociationPeerTarget>,
    pub skipped: Vec<ClusterAssociationPeerTarget>,
}

impl ClusterToolsTcpPeerRouteReport {
    pub fn is_empty(&self) -> bool {
        self.dialed.is_empty() && self.removed.is_empty() && self.skipped.is_empty()
    }
}

#[derive(Default)]
pub struct ClusterToolsTcpPeerRoutes {
    registrations: BTreeMap<String, ClusterToolsTcpPeerRouteEntry>,
}

struct ClusterToolsTcpPeerRouteEntry {
    target: ClusterAssociationPeerTarget,
    registration: Option<RemoteAssociationRouteRegistration>,
}

impl ClusterToolsTcpPeerRoutes {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn route_count(&self) -> usize {
        self.registrations.len()
    }

    pub fn contains_peer(&self, target: &ClusterAssociationPeerTarget) -> bool {
        self.registrations.contains_key(&peer_key(target))
    }

    pub fn active_targets(&self) -> Vec<ClusterAssociationPeerTarget> {
        self.registrations
            .values()
            .map(|entry| entry.target.clone())
            .collect()
    }

    pub fn apply_changes<M>(
        &mut self,
        runtime: &ClusterToolsTcpAssociationRuntime<M>,
        changes: impl IntoIterator<Item = ClusterAssociationPeerChange>,
    ) -> ClusterToolsTcpPeerRouteResult<ClusterToolsTcpPeerRouteReport>
    where
        M: RemoteMessage + Send + 'static,
    {
        let mut report = ClusterToolsTcpPeerRouteReport::default();
        for change in changes {
            match change {
                ClusterAssociationPeerChange::Remove(target) => {
                    self.remove(runtime, target, &mut report);
                }
                ClusterAssociationPeerChange::Dial(target) => {
                    self.dial(runtime, target, &mut report)?;
                }
            }
        }
        Ok(report)
    }

    pub fn clear<M>(
        &mut self,
        runtime: &ClusterToolsTcpAssociationRuntime<M>,
    ) -> ClusterToolsTcpPeerRouteReport
    where
        M: RemoteMessage + Send + 'static,
    {
        let targets = self.active_targets();
        let mut report = ClusterToolsTcpPeerRouteReport::default();
        for target in targets {
            self.remove(runtime, target, &mut report);
        }
        report
    }

    fn remove<M>(
        &mut self,
        runtime: &ClusterToolsTcpAssociationRuntime<M>,
        target: ClusterAssociationPeerTarget,
        report: &mut ClusterToolsTcpPeerRouteReport,
    ) where
        M: RemoteMessage + Send + 'static,
    {
        if let Some(entry) = self.registrations.remove(&peer_key(&target)) {
            let address = entry
                .registration
                .as_ref()
                .map(RemoteAssociationRouteRegistration::address)
                .unwrap_or_else(|| entry.target.association());
            runtime.remove_route_with_reason(address, "cluster-tools peer route removed");
            report.removed.push(target);
        } else if runtime
            .remove_route_with_reason(target.association(), "cluster-tools peer route removed")
        {
            report.removed.push(target);
        } else {
            report.skipped.push(target);
        }
    }

    fn dial<M>(
        &mut self,
        runtime: &ClusterToolsTcpAssociationRuntime<M>,
        target: ClusterAssociationPeerTarget,
        report: &mut ClusterToolsTcpPeerRouteReport,
    ) -> ClusterToolsTcpPeerRouteResult<()>
    where
        M: RemoteMessage + Send + 'static,
    {
        if self.contains_peer(&target) {
            report.skipped.push(target);
            return Ok(());
        }

        if runtime
            .association_cache()
            .contains_route(target.association())
        {
            self.registrations.insert(
                peer_key(&target),
                ClusterToolsTcpPeerRouteEntry {
                    target: target.clone(),
                    registration: None,
                },
            );
            report.skipped.push(target);
            return Ok(());
        }

        let registration = runtime
            .dial(target.association().clone())
            .map_err(|source| ClusterToolsTcpPeerRouteError::Dial {
                target: Box::new(target.clone()),
                source: Box::new(source),
            })?;
        self.registrations.insert(
            peer_key(&target),
            ClusterToolsTcpPeerRouteEntry {
                target: target.clone(),
                registration: Some(registration),
            },
        );
        report.dialed.push(target);
        Ok(())
    }
}

fn peer_key(target: &ClusterAssociationPeerTarget) -> String {
    target.node().ordering_key()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use kairo_actor::Address;
    use kairo_cluster::{
        ClusterAssociationPeerState, CurrentClusterState, Gossip, Member, MemberStatus,
        UniqueAddress,
    };
    use kairo_remote::RemoteSettings;
    use kairo_serialization::{MessageCodec, Registry, SerializationRegistry};
    use kairo_testkit::{ActorSystemTestKit, await_assert};

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
        const MANIFEST: &'static str = "kairo.cluster-tools.test.peer-route-message";
        const VERSION: u16 = 1;
    }

    #[derive(Debug, Clone, Copy)]
    struct TestMessageCodec;

    impl MessageCodec<TestMessage> for TestMessageCodec {
        fn serializer_id(&self) -> u32 {
            59_202
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

    fn wait_for_route(runtime: &ClusterToolsTcpAssociationRuntime<TestMessage>) {
        await_assert(Duration::from_secs(1), Duration::from_millis(1), || {
            let actual = runtime.association_cache().route_count();
            (actual == 1)
                .then_some(())
                .ok_or_else(|| format!("expected 1 route, got {actual}"))
        })
        .unwrap();
    }

    fn bind_runtime(
        name: &str,
        uid: u64,
        system_uid: u64,
        kit: &ActorSystemTestKit,
        registry: Arc<Registry>,
    ) -> ClusterToolsTcpAssociationRuntime<TestMessage> {
        let gossip = kit
            .create_probe::<PubSubGossipMsg>(format!("{name}-gossip"))
            .unwrap();
        let mediator = kit
            .create_probe::<DistributedPubSubMediatorMsg<TestMessage>>(format!("{name}-mediator"))
            .unwrap();
        let manager = kit
            .create_probe::<SingletonManagerMsg>(format!("{name}-singleton-manager"))
            .unwrap();
        let gossip_ref = gossip.actor_ref();
        let mediator_ref = mediator.actor_ref();
        let manager_ref = manager.actor_ref();
        ClusterToolsTcpAssociationRuntime::bind(
            name,
            uid,
            system_uid,
            RemoteSettings::new("127.0.0.1", 0),
            move |self_node| {
                ClusterToolsSystemInbound::new(self_node.clone())
                    .with_pubsub_gossip(PubSubGossipWireInbound::new(
                        self_node.clone(),
                        registry.clone(),
                        gossip_ref,
                    ))
                    .with_pubsub_delivery(PubSubRemoteDeliveryInbound::new(
                        self_node.clone(),
                        registry.clone(),
                        mediator_ref,
                    ))
                    .with_singleton_manager(SingletonManagerRemoteInbound::new(
                        self_node,
                        registry,
                        manager_ref,
                    ))
            },
        )
        .unwrap()
    }

    #[test]
    fn peer_routes_apply_cluster_planner_dial_and_remove_to_tools_tcp_runtime() {
        let sender_kit = ActorSystemTestKit::new("cluster-tools-peer-routes-sender").unwrap();
        let receiver_kit = ActorSystemTestKit::new("cluster-tools-peer-routes-receiver").unwrap();
        let registry = registry();
        let sender = bind_runtime("sender", 1, 11, &sender_kit, registry.clone());
        let receiver = bind_runtime("receiver", 2, 22, &receiver_kit, registry);
        let mut planner = ClusterAssociationPeerState::new(sender.self_node().clone());
        let mut routes = ClusterToolsTcpPeerRoutes::new();

        let changes = planner
            .apply_snapshot(state(
                vec![
                    member(sender.self_node().clone()),
                    member(receiver.self_node().clone()),
                ],
                vec![],
            ))
            .unwrap();
        let report = routes.apply_changes(&sender, changes).unwrap();
        assert_eq!(report.dialed.len(), 1);
        assert_eq!(routes.route_count(), 1);
        assert_eq!(sender.association_cache().route_count(), 1);
        wait_for_route(&receiver);

        let changes = planner
            .apply_snapshot(state(
                vec![
                    member(sender.self_node().clone()),
                    member(receiver.self_node().clone()),
                ],
                vec![member(receiver.self_node().clone())],
            ))
            .unwrap();
        let report = routes.apply_changes(&sender, changes).unwrap();
        assert_eq!(report.removed.len(), 1);
        assert_eq!(routes.route_count(), 0);
        assert_eq!(sender.association_cache().route_count(), 0);

        let sender_report = sender.shutdown().unwrap();
        assert_eq!(sender_report.accepted_associations, 0);
        let receiver_report = receiver.shutdown().unwrap();
        assert_eq!(receiver_report.accepted_associations, 1);
        sender_kit.shutdown(Duration::from_secs(1)).unwrap();
        receiver_kit.shutdown(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn peer_routes_adopt_existing_cluster_tools_tcp_runtime_route_and_clear_it() {
        let sender_kit =
            ActorSystemTestKit::new("cluster-tools-peer-routes-existing-sender").unwrap();
        let receiver_kit =
            ActorSystemTestKit::new("cluster-tools-peer-routes-existing-receiver").unwrap();
        let registry = registry();
        let sender = bind_runtime("existing-sender", 1, 11, &sender_kit, registry.clone());
        let receiver = bind_runtime("existing-receiver", 2, 22, &receiver_kit, registry);
        let mut planner = ClusterAssociationPeerState::new(sender.self_node().clone());
        let mut routes = ClusterToolsTcpPeerRoutes::new();
        sender.dial(receiver.local_address().clone()).unwrap();
        wait_for_route(&receiver);
        assert_eq!(sender.association_cache().route_count(), 1);

        let changes = planner
            .apply_snapshot(state(
                vec![
                    member(sender.self_node().clone()),
                    member(receiver.self_node().clone()),
                ],
                vec![],
            ))
            .unwrap();
        let report = routes.apply_changes(&sender, changes).unwrap();

        assert!(report.dialed.is_empty());
        assert_eq!(report.skipped.len(), 1);
        assert_eq!(report.skipped[0].node(), receiver.self_node());
        assert_eq!(routes.route_count(), 1);
        assert_eq!(sender.association_cache().route_count(), 1);

        let clear_report = routes.clear(&sender);

        assert_eq!(clear_report.removed.len(), 1);
        assert_eq!(clear_report.removed[0].node(), receiver.self_node());
        assert_eq!(routes.route_count(), 0);
        assert_eq!(sender.association_cache().route_count(), 0);

        let sender_report = sender.shutdown().unwrap();
        assert_eq!(sender_report.accepted_associations, 0);
        let receiver_report = receiver.shutdown().unwrap();
        assert_eq!(receiver_report.accepted_associations, 1);
        sender_kit.shutdown(Duration::from_secs(1)).unwrap();
        receiver_kit.shutdown(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn peer_routes_keep_remaining_tools_route_when_one_peer_is_removed() {
        let sender_kit =
            ActorSystemTestKit::new("cluster-tools-peer-routes-reduce-sender").unwrap();
        let second_kit =
            ActorSystemTestKit::new("cluster-tools-peer-routes-reduce-second").unwrap();
        let third_kit = ActorSystemTestKit::new("cluster-tools-peer-routes-reduce-third").unwrap();
        let registry = registry();
        let sender = bind_runtime("reduce-sender", 1, 11, &sender_kit, registry.clone());
        let second = bind_runtime("reduce-second", 2, 22, &second_kit, registry.clone());
        let third = bind_runtime("reduce-third", 3, 33, &third_kit, registry);
        let mut planner = ClusterAssociationPeerState::new(sender.self_node().clone());
        let mut routes = ClusterToolsTcpPeerRoutes::new();

        let changes = planner
            .apply_snapshot(state(
                vec![
                    member(sender.self_node().clone()),
                    member(second.self_node().clone()),
                    member(third.self_node().clone()),
                ],
                vec![],
            ))
            .unwrap();
        let report = routes.apply_changes(&sender, changes).unwrap();

        assert_eq!(report.dialed.len(), 2);
        assert_eq!(routes.route_count(), 2);
        assert_eq!(sender.association_cache().route_count(), 2);
        wait_for_route(&second);
        wait_for_route(&third);

        let changes = planner
            .apply_snapshot(state(
                vec![
                    member(sender.self_node().clone()),
                    member(second.self_node().clone()),
                ],
                vec![],
            ))
            .unwrap();
        let report = routes.apply_changes(&sender, changes).unwrap();

        assert_eq!(report.removed.len(), 1);
        assert_eq!(report.removed[0].node(), third.self_node());
        assert_eq!(routes.route_count(), 1);
        assert_eq!(sender.association_cache().route_count(), 1);
        assert!(
            routes
                .active_targets()
                .iter()
                .any(|target| target.node() == second.self_node())
        );

        let clear_report = routes.clear(&sender);
        assert_eq!(clear_report.removed.len(), 1);
        assert_eq!(routes.route_count(), 0);
        assert_eq!(sender.association_cache().route_count(), 0);

        let sender_report = sender.shutdown().unwrap();
        assert_eq!(sender_report.accepted_associations, 0);
        let second_report = second.shutdown().unwrap();
        assert_eq!(second_report.accepted_associations, 1);
        let third_report = third.shutdown().unwrap();
        assert_eq!(third_report.accepted_associations, 1);
        sender_kit.shutdown(Duration::from_secs(1)).unwrap();
        second_kit.shutdown(Duration::from_secs(1)).unwrap();
        third_kit.shutdown(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn peer_routes_keep_membership_state_out_of_tools_route_owner() {
        let target = ClusterAssociationPeerTarget::new(node("peer", 2552, 2)).unwrap();
        let routes = ClusterToolsTcpPeerRoutes::new();

        assert!(!routes.contains_peer(&target));
        assert!(ClusterToolsTcpPeerRouteReport::default().is_empty());
        assert!(Gossip::new().members().is_empty());
    }

    #[test]
    fn clear_without_routes_reports_no_work() {
        let kit = ActorSystemTestKit::new("cluster-tools-peer-routes-clear").unwrap();
        let registry = registry();
        let runtime = bind_runtime("clear", 1, 11, &kit, registry);
        let mut routes = ClusterToolsTcpPeerRoutes::new();

        let report = routes.clear(&runtime);

        assert!(report.is_empty());
        runtime.shutdown().unwrap();
        kit.shutdown(Duration::from_secs(1)).unwrap();
    }
}
