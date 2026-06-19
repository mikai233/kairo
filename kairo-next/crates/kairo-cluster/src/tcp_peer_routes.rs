use std::collections::BTreeMap;
use std::fmt::{self, Display, Formatter};

use kairo_remote::{RemoteAssociationRouteRegistration, RemoteError};

use crate::{
    ClusterAssociationPeerChange, ClusterAssociationPeerTarget, ClusterTcpAssociationRuntime,
};

#[derive(Debug)]
pub enum ClusterTcpPeerRouteError {
    Dial {
        target: Box<ClusterAssociationPeerTarget>,
        source: Box<RemoteError>,
    },
}

impl Display for ClusterTcpPeerRouteError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Dial { target, source } => {
                write!(
                    f,
                    "cluster tcp peer dial to {} failed: {source}",
                    target.as_ref().node().ordering_key()
                )
            }
        }
    }
}

impl std::error::Error for ClusterTcpPeerRouteError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Dial { source, .. } => Some(source.as_ref()),
        }
    }
}

impl ClusterTcpPeerRouteError {
    pub fn target(&self) -> &ClusterAssociationPeerTarget {
        match self {
            Self::Dial { target, .. } => target.as_ref(),
        }
    }
}

pub type ClusterTcpPeerRouteResult<T> = Result<T, ClusterTcpPeerRouteError>;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ClusterTcpPeerRouteReport {
    pub dialed: Vec<ClusterAssociationPeerTarget>,
    pub removed: Vec<ClusterAssociationPeerTarget>,
    pub skipped: Vec<ClusterAssociationPeerTarget>,
}

impl ClusterTcpPeerRouteReport {
    pub fn is_empty(&self) -> bool {
        self.dialed.is_empty() && self.removed.is_empty() && self.skipped.is_empty()
    }
}

#[derive(Default)]
pub struct ClusterTcpPeerRoutes {
    registrations: BTreeMap<String, ClusterTcpPeerRouteEntry>,
}

struct ClusterTcpPeerRouteEntry {
    target: ClusterAssociationPeerTarget,
    registration: Option<RemoteAssociationRouteRegistration>,
}

impl ClusterTcpPeerRoutes {
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

    pub fn apply_changes(
        &mut self,
        runtime: &ClusterTcpAssociationRuntime,
        changes: impl IntoIterator<Item = ClusterAssociationPeerChange>,
    ) -> ClusterTcpPeerRouteResult<ClusterTcpPeerRouteReport> {
        let mut report = ClusterTcpPeerRouteReport::default();
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

    pub fn clear(&mut self, runtime: &ClusterTcpAssociationRuntime) -> ClusterTcpPeerRouteReport {
        let targets = self.active_targets();
        let mut report = ClusterTcpPeerRouteReport::default();
        for target in targets {
            self.remove(runtime, target, &mut report);
        }
        report
    }

    fn remove(
        &mut self,
        runtime: &ClusterTcpAssociationRuntime,
        target: ClusterAssociationPeerTarget,
        report: &mut ClusterTcpPeerRouteReport,
    ) {
        if let Some(entry) = self.registrations.remove(&peer_key(&target)) {
            let address = entry
                .registration
                .as_ref()
                .map(RemoteAssociationRouteRegistration::address)
                .unwrap_or_else(|| entry.target.association());
            if let Some(registration) = &entry.registration {
                registration.close_owned_route("cluster peer route removed");
            }
            runtime.remove_route_with_reason(address, "cluster peer route removed");
            report.removed.push(target);
        } else if runtime
            .remove_route_with_reason(target.association(), "cluster peer route removed")
        {
            report.removed.push(target);
        } else {
            report.skipped.push(target);
        }
    }

    fn dial(
        &mut self,
        runtime: &ClusterTcpAssociationRuntime,
        target: ClusterAssociationPeerTarget,
        report: &mut ClusterTcpPeerRouteReport,
    ) -> ClusterTcpPeerRouteResult<()> {
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
                ClusterTcpPeerRouteEntry {
                    target: target.clone(),
                    registration: None,
                },
            );
            report.skipped.push(target);
            return Ok(());
        }

        let registration = runtime
            .dial(target.association().clone())
            .map_err(|source| ClusterTcpPeerRouteError::Dial {
                target: Box::new(target.clone()),
                source: Box::new(source),
            })?;
        self.registrations.insert(
            peer_key(&target),
            ClusterTcpPeerRouteEntry {
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

    use kairo_actor::Address;
    use kairo_remote::{RemoteOutbound, RemoteSettings};
    use kairo_serialization::{ActorRefWireData, Registry};
    use kairo_testkit::{ActorSystemTestKit, await_assert};

    use super::*;
    use crate::{
        ClusterAssociationPeerState, ClusterMembershipMsg, ClusterMembershipWireInbound,
        ClusterSystemInbound, CurrentClusterState, DEFAULT_CLUSTER_HEARTBEAT_RECEIVER_PATH,
        DEFAULT_CLUSTER_HEARTBEAT_SENDER_PATH, Gossip, HeartbeatRemoteReceiverInbound,
        HeartbeatRemoteResponseInbound, HeartbeatSenderMsg, Member, MemberStatus, UniqueAddress,
        register_cluster_protocol_codecs,
    };

    fn registry() -> Arc<Registry> {
        let mut registry = Registry::new();
        register_cluster_protocol_codecs(&mut registry).unwrap();
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

    fn wire_for(node: &UniqueAddress, path: &str) -> ActorRefWireData {
        ActorRefWireData::new(format!("{}{}", node.address, path)).unwrap()
    }

    fn wait_for_reverse_route(runtime: &ClusterTcpAssociationRuntime) {
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
    ) -> ClusterTcpAssociationRuntime {
        let membership = kit
            .create_probe::<ClusterMembershipMsg>(format!("{name}-membership"))
            .unwrap();
        let heartbeat_sender = kit
            .create_probe::<HeartbeatSenderMsg>(format!("{name}-heartbeat-sender"))
            .unwrap();
        let membership_ref = membership.actor_ref();
        let heartbeat_sender_ref = heartbeat_sender.actor_ref();
        ClusterTcpAssociationRuntime::bind(
            name,
            uid,
            system_uid,
            RemoteSettings::new("127.0.0.1", 0),
            move |self_node, cache| {
                ClusterSystemInbound::new(self_node.clone())
                    .with_membership(ClusterMembershipWireInbound::new(
                        self_node.clone(),
                        registry.clone(),
                        membership_ref,
                    ))
                    .with_heartbeat_receiver(
                        HeartbeatRemoteReceiverInbound::from_arc(
                            self_node.clone(),
                            registry.clone(),
                            Arc::new(cache.clone()) as Arc<dyn RemoteOutbound>,
                        )
                        .with_sender(Some(wire_for(
                            &self_node,
                            DEFAULT_CLUSTER_HEARTBEAT_RECEIVER_PATH,
                        ))),
                    )
                    .with_heartbeat_response(HeartbeatRemoteResponseInbound::new(
                        wire_for(&self_node, DEFAULT_CLUSTER_HEARTBEAT_SENDER_PATH),
                        registry,
                        heartbeat_sender_ref,
                    ))
            },
        )
        .unwrap()
    }

    #[test]
    fn peer_routes_apply_planner_dial_and_remove_to_tcp_runtime() {
        let sender_kit = ActorSystemTestKit::new("cluster-peer-routes-sender").unwrap();
        let receiver_kit = ActorSystemTestKit::new("cluster-peer-routes-receiver").unwrap();
        let registry = registry();
        let sender = bind_runtime("sender", 1, 11, &sender_kit, registry.clone());
        let receiver = bind_runtime("receiver", 2, 22, &receiver_kit, registry);
        let mut planner = ClusterAssociationPeerState::new(sender.self_node().clone());
        let mut routes = ClusterTcpPeerRoutes::new();

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
        wait_for_reverse_route(&receiver);

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
    fn peer_routes_adopt_existing_cluster_tcp_runtime_route_and_clear_it() {
        let sender_kit = ActorSystemTestKit::new("cluster-peer-routes-existing-sender").unwrap();
        let receiver_kit =
            ActorSystemTestKit::new("cluster-peer-routes-existing-receiver").unwrap();
        let registry = registry();
        let sender = bind_runtime("existing-sender", 1, 11, &sender_kit, registry.clone());
        let receiver = bind_runtime("existing-receiver", 2, 22, &receiver_kit, registry);
        let mut planner = ClusterAssociationPeerState::new(sender.self_node().clone());
        let mut routes = ClusterTcpPeerRoutes::new();
        sender.dial(receiver.local_address().clone()).unwrap();
        wait_for_reverse_route(&receiver);
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
    fn peer_routes_keep_remaining_cluster_route_when_one_peer_is_removed() {
        let sender_kit = ActorSystemTestKit::new("cluster-peer-routes-reduce-sender").unwrap();
        let second_kit = ActorSystemTestKit::new("cluster-peer-routes-reduce-second").unwrap();
        let third_kit = ActorSystemTestKit::new("cluster-peer-routes-reduce-third").unwrap();
        let registry = registry();
        let sender = bind_runtime("reduce-sender", 1, 11, &sender_kit, registry.clone());
        let second = bind_runtime("reduce-second", 2, 22, &second_kit, registry.clone());
        let third = bind_runtime("reduce-third", 3, 33, &third_kit, registry);
        let mut planner = ClusterAssociationPeerState::new(sender.self_node().clone());
        let mut routes = ClusterTcpPeerRoutes::new();

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
        wait_for_reverse_route(&second);
        wait_for_reverse_route(&third);

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
    fn peer_routes_skip_duplicate_dial_changes() {
        let target = ClusterAssociationPeerTarget::new(node("peer", 2552, 2)).unwrap();
        let routes = ClusterTcpPeerRoutes::new();

        assert!(!routes.contains_peer(&target));
        assert!(ClusterTcpPeerRouteReport::default().is_empty());
    }

    #[test]
    fn clear_without_routes_reports_no_work() {
        let kit = ActorSystemTestKit::new("cluster-peer-routes-clear").unwrap();
        let registry = registry();
        let runtime = bind_runtime("clear", 1, 11, &kit, registry);
        let mut routes = ClusterTcpPeerRoutes::new();

        let report = routes.clear(&runtime);

        assert!(report.is_empty());
        runtime.shutdown().unwrap();
        kit.shutdown(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn peer_routes_keep_membership_state_out_of_route_owner() {
        let target = ClusterAssociationPeerTarget::new(node("peer", 2552, 2)).unwrap();
        let routes = ClusterTcpPeerRoutes::new();

        assert!(!routes.contains_peer(&target));
        assert!(Gossip::new().members().is_empty());
    }
}
