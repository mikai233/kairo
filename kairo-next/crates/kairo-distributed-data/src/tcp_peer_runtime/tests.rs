use super::*;

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
    fn peer_runtime_keeps_remaining_route_when_one_peer_is_removed() {
        let retry_interval = Duration::from_millis(25);
        let second_port = unused_port();
        let third_port = unused_port();
        let second_node = node("reduce-second", second_port, 2);
        let third_node = node("reduce-third", third_port, 3);
        let mut sender = bind_peer_runtime(
            "reduce-sender",
            1,
            11,
            RemoteSettings::new("127.0.0.1", 0),
            ReplicaId::from(&second_node),
            retry_interval,
        );
        let sender_node = sender.self_node().clone();
        let second = bind_association_runtime_on_port(
            "reduce-second",
            ReplicaId::from(&second_node),
            ReplicaId::from(&sender_node),
            22,
            second_port,
        );
        let third = bind_association_runtime_on_port(
            "reduce-third",
            ReplicaId::from(&third_node),
            ReplicaId::from(&sender_node),
            33,
            third_port,
        );

        let report = sender
            .apply_snapshot(state(
                vec![
                    member(sender_node.clone()),
                    member(second_node.clone()),
                    member(third_node.clone()),
                ],
                vec![],
            ))
            .unwrap();
        assert_eq!(report.dialed.len(), 2);
        assert_eq!(sender.peer_route_count(), 2);
        assert_eq!(sender.association_cache().route_count(), 2);
        wait_for_reverse_route(&second);
        wait_for_reverse_route(&third);

        let report = sender
            .apply_snapshot(state(
                vec![member(sender_node), member(second_node.clone())],
                vec![],
            ))
            .unwrap();

        assert_eq!(report.removed.len(), 1);
        assert_eq!(report.removed[0].node(), &third_node);
        assert_eq!(sender.peer_route_count(), 1);
        assert_eq!(sender.association_cache().route_count(), 1);
        assert!(
            sender
                .active_peer_targets()
                .iter()
                .any(|target| target.node() == &second_node)
        );

        let sender_report = sender.shutdown().unwrap();
        assert_eq!(sender_report.peer_routes.removed.len(), 1);
        assert!(sender_report.pending_reconnects.is_empty());
        assert_eq!(sender_report.listener.accepted_associations, 0);
        let second_report = second.shutdown().unwrap();
        assert_eq!(second_report.accepted_associations, 1);
        let third_report = third.shutdown().unwrap();
        assert_eq!(third_report.accepted_associations, 1);
    }

    #[test]
    fn peer_runtime_shutdown_clears_active_peer_routes_before_listener_stop() {
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

        sender
            .apply_snapshot(state(
                vec![member(sender_node), member(receiver_node)],
                vec![],
            ))
            .unwrap();
        assert_eq!(sender.peer_route_count(), 1);
        assert_eq!(sender.association_cache().route_count(), 1);
        wait_for_reverse_route(&receiver);

        let sender_report = sender.shutdown().unwrap();

        assert_eq!(sender_report.peer_routes.removed.len(), 1);
        assert!(sender_report.pending_reconnects.is_empty());
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
