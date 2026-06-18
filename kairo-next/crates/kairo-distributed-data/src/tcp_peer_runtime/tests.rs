use super::*;

mod route_tests {
    use std::net::TcpListener;
    use std::sync::{Arc, Condvar, Mutex};
    use std::time::{Duration, Instant};

    use kairo_actor::Address;
    use kairo_cluster::{
        CurrentClusterState, Member, MemberEvent, MemberStatus, ReachabilityEvent, UniqueAddress,
    };
    use kairo_remote::{RemoteError, RemoteSettings};
    use kairo_serialization::{ActorRefWireData, Registry, RemoteEnvelope, RemoteMessage};
    use kairo_testkit::await_assert;

    use super::*;
    use crate::{
        ReplicatorRead, ReplicatorRemoteReplyError, ReplicatorRemoteRequestError,
        register_ddata_protocol_codecs, test_support::ddata_socket_test_lock,
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

    #[derive(Default)]
    struct RecordingRequests {
        received: Mutex<Vec<(ReplicaId, RemoteEnvelope)>>,
        changed: Condvar,
    }

    impl RecordingRequests {
        fn wait_for_len(&self, len: usize, timeout: Duration) -> Vec<(ReplicaId, RemoteEnvelope)> {
            let deadline = Instant::now() + timeout;
            let mut received = self.received.lock().expect("requests poisoned");
            while received.len() < len {
                let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                    break;
                };
                let (next_received, wait) = self
                    .changed
                    .wait_timeout(received, remaining)
                    .expect("requests poisoned");
                received = next_received;
                if wait.timed_out() {
                    break;
                }
            }
            received.clone()
        }
    }

    impl ReplicatorRemoteRequestReceiver for RecordingRequests {
        fn receive_request_from(
            &self,
            from: ReplicaId,
            envelope: RemoteEnvelope,
        ) -> Result<(), ReplicatorRemoteRequestError> {
            self.received
                .lock()
                .expect("requests poisoned")
                .push((from, envelope));
            self.changed.notify_all();
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
        bind_association_runtime_on_port_with_requests(
            name,
            local,
            remote,
            system_uid,
            port,
            Arc::new(IgnoreRequests) as Arc<dyn ReplicatorRemoteRequestReceiver>,
        )
    }

    fn bind_association_runtime_on_port_with_requests(
        name: &str,
        local: ReplicaId,
        remote: ReplicaId,
        system_uid: u64,
        port: u16,
        requests: Arc<dyn ReplicatorRemoteRequestReceiver>,
    ) -> ReplicatorTcpAssociationRuntime {
        ReplicatorTcpAssociationRuntime::bind(
            name,
            local,
            remote,
            system_uid,
            RemoteSettings::new("127.0.0.1", port),
            requests,
            Arc::new(IgnoreReplies) as Arc<dyn ReplicatorRemoteReplyReceiver>,
        )
        .unwrap()
    }

    fn registry() -> Registry {
        let mut registry = Registry::new();
        register_ddata_protocol_codecs(&mut registry).unwrap();
        registry
    }

    fn replicator_ref(system: &str, port: u16) -> ActorRefWireData {
        ActorRefWireData::new(format!(
            "kairo://{system}@127.0.0.1:{port}/system/replicator"
        ))
        .unwrap()
    }

    fn wait_for_reverse_route(runtime: &ReplicatorTcpAssociationRuntime) {
        await_assert(
            Duration::from_secs(1),
            Duration::from_millis(1),
            || -> Result<(), String> {
                let actual = runtime.association_cache().route_count();
                if actual == 1 {
                    Ok(())
                } else {
                    Err(format!("expected 1 association route, found {actual}"))
                }
            },
        )
        .unwrap();
    }

    #[test]
    fn peer_runtime_applies_snapshot_and_reachability_event_to_live_routes() {
        let _guard = ddata_socket_test_lock();
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
        let _guard = ddata_socket_test_lock();
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
    fn peer_runtime_keeps_remaining_route_delivering_after_one_peer_is_removed() {
        let _guard = ddata_socket_test_lock();
        let retry_interval = Duration::from_millis(25);
        let registry = registry();
        let second_port = unused_port();
        let third_port = unused_port();
        let second_node = node("deliver-second", second_port, 2);
        let third_node = node("deliver-third", third_port, 3);
        let second_requests = Arc::new(RecordingRequests::default());
        let third_requests = Arc::new(RecordingRequests::default());
        let mut sender = bind_peer_runtime(
            "deliver-sender",
            1,
            11,
            RemoteSettings::new("127.0.0.1", 0),
            ReplicaId::from(&second_node),
            retry_interval,
        );
        let sender_node = sender.self_node().clone();
        let second = bind_association_runtime_on_port_with_requests(
            "deliver-second",
            ReplicaId::from(&second_node),
            ReplicaId::from(&sender_node),
            22,
            second_port,
            second_requests.clone() as Arc<dyn ReplicatorRemoteRequestReceiver>,
        );
        let third = bind_association_runtime_on_port_with_requests(
            "deliver-third",
            ReplicaId::from(&third_node),
            ReplicaId::from(&sender_node),
            33,
            third_port,
            third_requests.clone() as Arc<dyn ReplicatorRemoteRequestReceiver>,
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
                vec![member(sender_node.clone()), member(second_node.clone())],
                vec![],
            ))
            .unwrap();
        assert_eq!(report.removed.len(), 1);
        assert_eq!(sender.peer_route_count(), 1);
        assert_eq!(sender.association_cache().route_count(), 1);

        let recipient = replicator_ref("deliver-second", second_port);
        let sender_ref = replicator_ref(
            sender_node.address.system(),
            sender_node.address.port().unwrap(),
        );
        let read = ReplicatorRead {
            key: "counter-after-route-reduction".to_string(),
            from: Some(ReplicaId::from(&sender_node)),
        };
        let envelope = RemoteEnvelope::new(
            recipient.clone(),
            Some(sender_ref.clone()),
            registry.serialize(&read).unwrap(),
        );

        sender
            .association_cache()
            .send_to_recipient(envelope)
            .unwrap();

        let received = second_requests.wait_for_len(1, Duration::from_secs(1));
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].0, ReplicaId::from(&sender_node));
        assert_eq!(received[0].1.recipient, recipient);
        assert_eq!(received[0].1.sender, Some(sender_ref.clone()));
        assert_eq!(
            received[0].1.message.manifest.as_str(),
            ReplicatorRead::MANIFEST
        );
        let decoded = registry
            .deserialize::<ReplicatorRead>(received[0].1.message.clone())
            .unwrap();
        assert_eq!(decoded, read);

        let removed_recipient = replicator_ref("deliver-third", third_port);
        let removed_read = ReplicatorRead {
            key: "counter-after-removed-route".to_string(),
            from: Some(ReplicaId::from(&sender_node)),
        };
        let removed_envelope = RemoteEnvelope::new(
            removed_recipient,
            Some(sender_ref),
            registry.serialize(&removed_read).unwrap(),
        );

        let error = sender
            .association_cache()
            .send_to_recipient(removed_envelope)
            .expect_err("removed peer route should reject later delivery");
        assert!(matches!(error, RemoteError::AssociationUnavailable { .. }));
        assert!(
            third_requests
                .wait_for_len(1, Duration::from_millis(50))
                .is_empty(),
            "removed peer must not receive a request after membership route reduction"
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
    fn peer_runtime_keeps_remaining_route_delivering_after_member_removed_event() {
        let _guard = ddata_socket_test_lock();
        let retry_interval = Duration::from_millis(25);
        let registry = registry();
        let second_port = unused_port();
        let third_port = unused_port();
        let second_node = node("event-remove-second", second_port, 2);
        let third_node = node("event-remove-third", third_port, 3);
        let second_requests = Arc::new(RecordingRequests::default());
        let third_requests = Arc::new(RecordingRequests::default());
        let mut sender = bind_peer_runtime(
            "event-remove-sender",
            1,
            11,
            RemoteSettings::new("127.0.0.1", 0),
            ReplicaId::from(&second_node),
            retry_interval,
        );
        let sender_node = sender.self_node().clone();
        let second = bind_association_runtime_on_port_with_requests(
            "event-remove-second",
            ReplicaId::from(&second_node),
            ReplicaId::from(&sender_node),
            22,
            second_port,
            second_requests.clone() as Arc<dyn ReplicatorRemoteRequestReceiver>,
        );
        let third = bind_association_runtime_on_port_with_requests(
            "event-remove-third",
            ReplicaId::from(&third_node),
            ReplicaId::from(&sender_node),
            33,
            third_port,
            third_requests.clone() as Arc<dyn ReplicatorRemoteRequestReceiver>,
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
            .apply_event(ClusterEvent::Member(MemberEvent::Removed {
                member: member(third_node.clone()).with_status(MemberStatus::Removed),
                previous_status: MemberStatus::Up,
            }))
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

        let recipient = replicator_ref("event-remove-second", second_port);
        let sender_ref = replicator_ref(
            sender_node.address.system(),
            sender_node.address.port().unwrap(),
        );
        let read = ReplicatorRead {
            key: "counter-after-member-removed-event".to_string(),
            from: Some(ReplicaId::from(&sender_node)),
        };
        sender
            .association_cache()
            .send_to_recipient(RemoteEnvelope::new(
                recipient.clone(),
                Some(sender_ref.clone()),
                registry.serialize(&read).unwrap(),
            ))
            .unwrap();

        let received = second_requests.wait_for_len(1, Duration::from_secs(1));
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].0, ReplicaId::from(&sender_node));
        assert_eq!(received[0].1.recipient, recipient);
        assert_eq!(received[0].1.sender, Some(sender_ref.clone()));
        let decoded = registry
            .deserialize::<ReplicatorRead>(received[0].1.message.clone())
            .unwrap();
        assert_eq!(decoded, read);

        let removed_read = ReplicatorRead {
            key: "counter-after-removed-member-event".to_string(),
            from: Some(ReplicaId::from(&sender_node)),
        };
        let removed_error = sender
            .association_cache()
            .send_to_recipient(RemoteEnvelope::new(
                replicator_ref("event-remove-third", third_port),
                Some(sender_ref),
                registry.serialize(&removed_read).unwrap(),
            ))
            .expect_err("removed member route should reject later delivery");
        assert!(matches!(
            removed_error,
            RemoteError::AssociationUnavailable { .. }
        ));
        assert!(
            third_requests
                .wait_for_len(1, Duration::from_millis(50))
                .is_empty(),
            "removed member must not receive a request after MemberRemoved"
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
    fn peer_runtime_clears_routes_when_self_member_is_removed() {
        let _guard = ddata_socket_test_lock();
        let retry_interval = Duration::from_millis(25);
        let registry = registry();
        let receiver_port = unused_port();
        let receiver_node = node("self-remove-receiver", receiver_port, 2);
        let receiver_requests = Arc::new(RecordingRequests::default());
        let mut sender = bind_peer_runtime(
            "self-remove-sender",
            1,
            11,
            RemoteSettings::new("127.0.0.1", 0),
            ReplicaId::from(&receiver_node),
            retry_interval,
        );
        let sender_node = sender.self_node().clone();
        let receiver = bind_association_runtime_on_port_with_requests(
            "self-remove-receiver",
            ReplicaId::from(&receiver_node),
            ReplicaId::from(&sender_node),
            22,
            receiver_port,
            receiver_requests.clone() as Arc<dyn ReplicatorRemoteRequestReceiver>,
        );

        sender
            .apply_snapshot(state(
                vec![member(sender_node.clone()), member(receiver_node.clone())],
                vec![],
            ))
            .unwrap();
        assert_eq!(sender.peer_route_count(), 1);
        assert_eq!(sender.association_cache().route_count(), 1);
        wait_for_reverse_route(&receiver);

        let recipient = replicator_ref("self-remove-receiver", receiver_port);
        let sender_ref = replicator_ref(
            sender_node.address.system(),
            sender_node.address.port().unwrap(),
        );
        let before_removal = ReplicatorRead {
            key: "counter-before-self-removal".to_string(),
            from: Some(ReplicaId::from(&sender_node)),
        };
        sender
            .association_cache()
            .send_to_recipient(RemoteEnvelope::new(
                recipient.clone(),
                Some(sender_ref.clone()),
                registry.serialize(&before_removal).unwrap(),
            ))
            .unwrap();
        assert_eq!(
            receiver_requests
                .wait_for_len(1, Duration::from_secs(1))
                .len(),
            1
        );

        let report = sender
            .apply_event(ClusterEvent::Member(MemberEvent::Removed {
                member: member(sender_node.clone()).with_status(MemberStatus::Removed),
                previous_status: MemberStatus::Up,
            }))
            .unwrap();

        assert_eq!(report.removed.len(), 1);
        assert_eq!(report.removed[0].node(), &receiver_node);
        assert_eq!(sender.peer_route_count(), 0);
        assert_eq!(sender.association_cache().route_count(), 0);

        let after_removal = ReplicatorRead {
            key: "counter-after-self-removal".to_string(),
            from: Some(ReplicaId::from(&sender_node)),
        };
        let error = sender
            .association_cache()
            .send_to_recipient(RemoteEnvelope::new(
                recipient,
                Some(sender_ref),
                registry.serialize(&after_removal).unwrap(),
            ))
            .expect_err("self-removed peer runtime should clear outbound routes");
        assert!(matches!(error, RemoteError::AssociationUnavailable { .. }));
        assert_eq!(
            receiver_requests
                .wait_for_len(2, Duration::from_millis(50))
                .len(),
            1,
            "self-removed runtime must not deliver after local removal"
        );

        let sender_report = sender.shutdown().unwrap();
        assert_eq!(sender_report.peer_routes.removed.len(), 0);
        assert!(sender_report.pending_reconnects.is_empty());
        let receiver_report = receiver.shutdown().unwrap();
        assert_eq!(receiver_report.accepted_associations, 1);
    }

    #[test]
    fn peer_runtime_shutdown_clears_active_peer_routes_before_listener_stop() {
        let _guard = ddata_socket_test_lock();
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
    fn peer_runtime_shutdown_clears_multiple_active_peer_routes() {
        let _guard = ddata_socket_test_lock();
        let retry_interval = Duration::from_millis(25);
        let second_port = unused_port();
        let third_port = unused_port();
        let second_node = node("multi-shutdown-second", second_port, 2);
        let third_node = node("multi-shutdown-third", third_port, 3);
        let second = bind_association_runtime_on_port(
            "multi-shutdown-second",
            ReplicaId::from(&second_node),
            replica("multi-shutdown-sender"),
            22,
            second_port,
        );
        let third = bind_association_runtime_on_port(
            "multi-shutdown-third",
            ReplicaId::from(&third_node),
            replica("multi-shutdown-sender"),
            33,
            third_port,
        );
        let mut sender = bind_peer_runtime(
            "multi-shutdown-sender",
            1,
            11,
            RemoteSettings::new("127.0.0.1", 0),
            ReplicaId::from(&second_node),
            retry_interval,
        );
        let sender_node = sender.self_node().clone();

        sender
            .apply_snapshot(state(
                vec![
                    member(sender_node),
                    member(second_node.clone()),
                    member(third_node.clone()),
                ],
                vec![],
            ))
            .unwrap();
        assert_eq!(sender.peer_route_count(), 2);
        assert_eq!(sender.association_cache().route_count(), 2);
        wait_for_reverse_route(&second);
        wait_for_reverse_route(&third);

        let sender_report = sender.shutdown().unwrap();

        assert_eq!(sender_report.peer_routes.removed.len(), 2);
        assert!(
            sender_report
                .peer_routes
                .removed
                .iter()
                .any(|target| target.node() == &second_node)
        );
        assert!(
            sender_report
                .peer_routes
                .removed
                .iter()
                .any(|target| target.node() == &third_node)
        );
        assert!(sender_report.pending_reconnects.is_empty());
        assert_eq!(sender_report.listener.accepted_associations, 0);
        let second_report = second.shutdown().unwrap();
        assert_eq!(second_report.accepted_associations, 1);
        let third_report = third.shutdown().unwrap();
        assert_eq!(third_report.accepted_associations, 1);
    }

    #[test]
    fn peer_runtime_adopts_existing_ddata_route_and_clears_it_on_shutdown() {
        let _guard = ddata_socket_test_lock();
        let retry_interval = Duration::from_millis(25);
        let receiver_port = unused_port();
        let receiver_node = node("adopted-receiver", receiver_port, 2);
        let receiver = bind_association_runtime_on_port(
            "adopted-receiver",
            ReplicaId::from(&receiver_node),
            replica("adopted-sender"),
            22,
            receiver_port,
        );
        let mut sender = bind_peer_runtime(
            "adopted-sender",
            1,
            11,
            RemoteSettings::new("127.0.0.1", 0),
            ReplicaId::from(&receiver_node),
            retry_interval,
        );
        let sender_node = sender.self_node().clone();

        sender
            .runtime()
            .dial_peer(
                receiver.local_address().clone(),
                ReplicaId::from(&receiver_node),
            )
            .unwrap();
        wait_for_reverse_route(&receiver);
        assert_eq!(sender.association_cache().route_count(), 1);
        assert_eq!(sender.peer_route_count(), 0);

        let report = sender
            .apply_snapshot(state(
                vec![member(sender_node), member(receiver_node.clone())],
                vec![],
            ))
            .unwrap();

        assert!(report.dialed.is_empty());
        assert_eq!(report.skipped.len(), 1);
        assert_eq!(report.skipped[0].node(), &receiver_node);
        assert_eq!(sender.peer_route_count(), 1);
        assert_eq!(sender.association_cache().route_count(), 1);

        let sender_report = sender.shutdown().unwrap();

        assert_eq!(sender_report.peer_routes.removed.len(), 1);
        assert_eq!(sender_report.peer_routes.removed[0].node(), &receiver_node);
        assert!(sender_report.pending_reconnects.is_empty());
        assert_eq!(sender_report.listener.accepted_associations, 0);
        let receiver_report = receiver.shutdown().unwrap();
        assert_eq!(receiver_report.accepted_associations, 1);
    }

    #[test]
    fn peer_runtime_retries_failed_peer_dial_after_retry_interval() {
        let _guard = ddata_socket_test_lock();
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
    fn peer_runtime_preserves_successful_routes_when_later_snapshot_dial_fails() {
        let _guard = ddata_socket_test_lock();
        let bound_port = unused_port();
        let missing_port = unused_port();
        let bound_node = node("partial-bound", bound_port, 2);
        let missing_node = node("partial-missing", missing_port, 3);
        let retry_interval = Duration::from_millis(25);
        let mut sender = bind_peer_runtime(
            "partial-sender",
            1,
            11,
            RemoteSettings::new("127.0.0.1", 0),
            ReplicaId::from(&bound_node),
            retry_interval,
        );
        let sender_node = sender.self_node().clone();
        let bound = bind_association_runtime_on_port(
            "partial-bound",
            ReplicaId::from(&bound_node),
            ReplicaId::from(&sender_node),
            22,
            bound_port,
        );

        let error = sender
            .apply_snapshot_at(
                state(
                    vec![
                        member(sender_node.clone()),
                        member(bound_node.clone()),
                        member(missing_node.clone()),
                    ],
                    vec![],
                ),
                Duration::ZERO,
            )
            .unwrap_err();

        assert!(matches!(error, ReplicatorTcpPeerRuntimeError::Route(_)));
        assert_eq!(sender.peer_route_count(), 1);
        assert_eq!(sender.association_cache().route_count(), 1);
        wait_for_reverse_route(&bound);
        let pending = sender.pending_peer_reconnects();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].target.node(), &missing_node);
        assert_eq!(pending[0].attempts, 1);
        assert_eq!(pending[0].next_retry_at, retry_interval);

        let missing = bind_association_runtime_on_port(
            "partial-missing",
            ReplicaId::from(&missing_node),
            ReplicaId::from(&sender_node),
            33,
            missing_port,
        );
        let report = sender.retry_due_peer_routes(retry_interval).unwrap();

        assert_eq!(report.dialed.len(), 1);
        assert_eq!(report.dialed[0].node(), &missing_node);
        assert_eq!(sender.peer_route_count(), 2);
        assert_eq!(sender.association_cache().route_count(), 2);
        assert_eq!(sender.pending_peer_reconnect_count(), 0);
        wait_for_reverse_route(&missing);

        let sender_report = sender.shutdown().unwrap();
        assert_eq!(sender_report.peer_routes.removed.len(), 2);
        assert!(sender_report.pending_reconnects.is_empty());
        let bound_report = bound.shutdown().unwrap();
        assert_eq!(bound_report.accepted_associations, 1);
        let missing_report = missing.shutdown().unwrap();
        assert_eq!(missing_report.accepted_associations, 1);
    }

    #[test]
    fn peer_runtime_shutdown_clears_pending_reconnects_after_failed_dial() {
        let _guard = ddata_socket_test_lock();
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
        let _guard = ddata_socket_test_lock();
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

    use kairo_cluster::{Member, MemberStatus, ReachabilityEvent};
    use kairo_serialization::RemoteEnvelope;
    use kairo_testkit::await_assert;

    use super::*;
    use crate::{
        ReplicatorRemoteReplyError, ReplicatorRemoteRequestError,
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
        await_assert(
            Duration::from_secs(1),
            Duration::from_millis(1),
            || -> Result<(), String> {
                let actual = runtime.association_cache().route_count();
                if actual == 1 {
                    Ok(())
                } else {
                    Err(format!("expected 1 association route, found {actual}"))
                }
            },
        )
        .unwrap();
    }

    #[test]
    fn peer_runtime_applies_snapshot_and_reachability_event_to_live_routes() {
        let _guard = ddata_socket_test_lock();
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
        let _guard = ddata_socket_test_lock();
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
        let _guard = ddata_socket_test_lock();
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
