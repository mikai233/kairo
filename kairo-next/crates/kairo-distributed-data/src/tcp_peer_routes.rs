use std::collections::BTreeMap;
use std::fmt::{self, Display, Formatter};

use kairo_cluster::{ClusterAssociationPeerChange, ClusterAssociationPeerTarget};
use kairo_remote::{RemoteAssociationRouteRegistration, RemoteError};

use crate::ReplicatorTcpAssociationRuntime;

#[derive(Debug)]
pub enum ReplicatorTcpPeerRouteError {
    Dial {
        target: Box<ClusterAssociationPeerTarget>,
        source: Box<RemoteError>,
    },
}

impl Display for ReplicatorTcpPeerRouteError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Dial { target, source } => write!(
                f,
                "distributed-data tcp peer dial to {} failed: {source}",
                target.as_ref().node().ordering_key()
            ),
        }
    }
}

impl std::error::Error for ReplicatorTcpPeerRouteError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Dial { source, .. } => Some(source.as_ref()),
        }
    }
}

impl ReplicatorTcpPeerRouteError {
    pub fn target(&self) -> &ClusterAssociationPeerTarget {
        match self {
            Self::Dial { target, .. } => target.as_ref(),
        }
    }
}

pub type ReplicatorTcpPeerRouteResult<T> = Result<T, ReplicatorTcpPeerRouteError>;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReplicatorTcpPeerRouteReport {
    pub dialed: Vec<ClusterAssociationPeerTarget>,
    pub removed: Vec<ClusterAssociationPeerTarget>,
    pub skipped: Vec<ClusterAssociationPeerTarget>,
}

impl ReplicatorTcpPeerRouteReport {
    pub fn is_empty(&self) -> bool {
        self.dialed.is_empty() && self.removed.is_empty() && self.skipped.is_empty()
    }
}

#[derive(Default)]
pub struct ReplicatorTcpPeerRoutes {
    registrations: BTreeMap<String, ReplicatorTcpPeerRouteEntry>,
}

struct ReplicatorTcpPeerRouteEntry {
    target: ClusterAssociationPeerTarget,
    registration: Option<RemoteAssociationRouteRegistration>,
}

impl ReplicatorTcpPeerRoutes {
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
        runtime: &ReplicatorTcpAssociationRuntime,
        changes: impl IntoIterator<Item = ClusterAssociationPeerChange>,
    ) -> ReplicatorTcpPeerRouteResult<ReplicatorTcpPeerRouteReport> {
        let mut report = ReplicatorTcpPeerRouteReport::default();
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

    pub fn clear(
        &mut self,
        runtime: &ReplicatorTcpAssociationRuntime,
    ) -> ReplicatorTcpPeerRouteReport {
        let targets = self.active_targets();
        let mut report = ReplicatorTcpPeerRouteReport::default();
        for target in targets {
            self.remove(runtime, target, &mut report);
        }
        report
    }

    fn remove(
        &mut self,
        runtime: &ReplicatorTcpAssociationRuntime,
        target: ClusterAssociationPeerTarget,
        report: &mut ReplicatorTcpPeerRouteReport,
    ) {
        if let Some(entry) = self.registrations.remove(&peer_key(&target)) {
            let address = entry
                .registration
                .as_ref()
                .map(RemoteAssociationRouteRegistration::address)
                .unwrap_or_else(|| entry.target.association());
            runtime.remove_route_with_reason(address, "distributed-data peer route removed");
            report.removed.push(target);
        } else if runtime
            .remove_route_with_reason(target.association(), "distributed-data peer route removed")
        {
            report.removed.push(target);
        } else {
            report.skipped.push(target);
        }
    }

    fn dial(
        &mut self,
        runtime: &ReplicatorTcpAssociationRuntime,
        target: ClusterAssociationPeerTarget,
        report: &mut ReplicatorTcpPeerRouteReport,
    ) -> ReplicatorTcpPeerRouteResult<()> {
        if self.contains_peer(&target) {
            report.skipped.push(target);
            return Ok(());
        }

        if runtime
            .association_cache()
            .contains_route(target.association())
        {
            runtime.register_source_replica(
                target.association().clone(),
                crate::ReplicaId::from(target.node()),
            );
            self.registrations.insert(
                peer_key(&target),
                ReplicatorTcpPeerRouteEntry {
                    target: target.clone(),
                    registration: None,
                },
            );
            report.skipped.push(target);
            return Ok(());
        }

        let registration = runtime
            .dial_peer(
                target.association().clone(),
                crate::ReplicaId::from(target.node()),
            )
            .map_err(|source| ReplicatorTcpPeerRouteError::Dial {
                target: Box::new(target.clone()),
                source: Box::new(source),
            })?;
        self.registrations.insert(
            peer_key(&target),
            ReplicatorTcpPeerRouteEntry {
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
    use std::time::{Duration, Instant};

    use kairo_actor::Address;
    use kairo_cluster::{
        ClusterAssociationPeerState, CurrentClusterState, Member, MemberStatus, ReachabilityEvent,
        UniqueAddress,
    };
    use kairo_remote::RemoteSettings;
    use kairo_serialization::RemoteEnvelope;

    use super::*;
    use crate::{
        ReplicaId, ReplicatorRemoteReplyError, ReplicatorRemoteReplyReceiver,
        ReplicatorRemoteRequestError, ReplicatorRemoteRequestReceiver,
        test_support::ddata_socket_test_lock,
    };

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

    fn bind_runtime(
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

    fn wait_for_reverse_route(runtime: &ReplicatorTcpAssociationRuntime) {
        let deadline = Instant::now() + Duration::from_secs(1);
        while runtime.association_cache().route_count() == 0 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(runtime.association_cache().route_count(), 1);
    }

    #[test]
    fn peer_routes_apply_cluster_planner_dial_and_remove_to_ddata_tcp_runtime() {
        let _guard = ddata_socket_test_lock();
        let sender = bind_runtime("sender", replica("sender"), replica("receiver"), 11);
        let receiver = bind_runtime("receiver", replica("receiver"), replica("sender"), 22);
        let sender_node = node("sender", sender.settings().canonical_port, 1);
        let receiver_node = node("receiver", receiver.settings().canonical_port, 2);
        let mut planner = ClusterAssociationPeerState::new(sender_node.clone());
        let mut routes = ReplicatorTcpPeerRoutes::new();

        let changes = planner
            .apply_snapshot(state(
                vec![member(sender_node.clone()), member(receiver_node.clone())],
                vec![],
            ))
            .unwrap();
        let report = routes.apply_changes(&sender, changes).unwrap();

        assert_eq!(report.dialed.len(), 1);
        assert_eq!(routes.route_count(), 1);
        assert_eq!(sender.association_cache().route_count(), 1);
        wait_for_reverse_route(&receiver);

        let changes = planner
            .apply_event(kairo_cluster::ClusterEvent::Reachability(
                ReachabilityEvent::Unreachable(member(receiver_node)),
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
    }

    #[test]
    fn peer_routes_keep_remaining_ddata_route_when_one_peer_is_removed() {
        let _guard = ddata_socket_test_lock();
        let sender = bind_runtime("reduce-sender", replica("sender"), replica("second"), 11);
        let second = bind_runtime("reduce-second", replica("second"), replica("sender"), 22);
        let third = bind_runtime("reduce-third", replica("third"), replica("sender"), 33);
        let sender_node = node("reduce-sender", sender.settings().canonical_port, 1);
        let second_node = node("reduce-second", second.settings().canonical_port, 2);
        let third_node = node("reduce-third", third.settings().canonical_port, 3);
        let mut planner = ClusterAssociationPeerState::new(sender_node.clone());
        let mut routes = ReplicatorTcpPeerRoutes::new();

        let changes = planner
            .apply_snapshot(state(
                vec![
                    member(sender_node.clone()),
                    member(second_node.clone()),
                    member(third_node.clone()),
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
                vec![member(sender_node), member(second_node.clone())],
                vec![],
            ))
            .unwrap();
        let report = routes.apply_changes(&sender, changes).unwrap();

        assert_eq!(report.removed.len(), 1);
        assert_eq!(report.removed[0].node(), &third_node);
        assert_eq!(routes.route_count(), 1);
        assert_eq!(sender.association_cache().route_count(), 1);
        assert!(
            routes
                .active_targets()
                .iter()
                .any(|target| target.node() == &second_node)
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
    }

    #[test]
    fn peer_routes_adopt_existing_ddata_tcp_runtime_route_and_clear_it() {
        let _guard = ddata_socket_test_lock();
        let sender = bind_runtime(
            "existing-sender",
            replica("sender"),
            replica("receiver"),
            11,
        );
        let receiver = bind_runtime(
            "existing-receiver",
            replica("receiver"),
            replica("sender"),
            22,
        );
        let sender_node = node("existing-sender", sender.settings().canonical_port, 1);
        let receiver_node = node("existing-receiver", receiver.settings().canonical_port, 2);
        let mut planner = ClusterAssociationPeerState::new(sender_node.clone());
        let mut routes = ReplicatorTcpPeerRoutes::new();
        sender
            .dial_peer(
                receiver.local_address().clone(),
                ReplicaId::from(&receiver_node),
            )
            .unwrap();
        wait_for_reverse_route(&receiver);
        assert_eq!(sender.association_cache().route_count(), 1);

        let changes = planner
            .apply_snapshot(state(
                vec![member(sender_node), member(receiver_node.clone())],
                vec![],
            ))
            .unwrap();
        let report = routes.apply_changes(&sender, changes).unwrap();

        assert!(report.dialed.is_empty());
        assert_eq!(report.skipped.len(), 1);
        assert_eq!(report.skipped[0].node(), &receiver_node);
        assert_eq!(routes.route_count(), 1);
        assert_eq!(sender.association_cache().route_count(), 1);

        let clear_report = routes.clear(&sender);

        assert_eq!(clear_report.removed.len(), 1);
        assert_eq!(clear_report.removed[0].node(), &receiver_node);
        assert_eq!(routes.route_count(), 0);
        assert_eq!(sender.association_cache().route_count(), 0);

        let sender_report = sender.shutdown().unwrap();
        assert_eq!(sender_report.accepted_associations, 0);
        let receiver_report = receiver.shutdown().unwrap();
        assert_eq!(receiver_report.accepted_associations, 1);
    }

    #[test]
    fn clear_without_routes_reports_no_work() {
        let _guard = ddata_socket_test_lock();
        let sender = bind_runtime("clear", replica("clear"), replica("peer"), 33);
        let mut routes = ReplicatorTcpPeerRoutes::new();

        let report = routes.clear(&sender);

        assert!(report.is_empty());
        assert_eq!(routes.route_count(), 0);
        sender.shutdown().unwrap();
    }
}
