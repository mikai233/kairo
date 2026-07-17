#![deny(missing_docs)]

use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use kairo_actor::Address;
use kairo_remote::{
    AssociationOutboundPipeline, RemoteAssociationAddress, RemoteAssociationCache,
    RemoteAssociationRegistry, RemoteAssociationRouteInstaller, RemoteAssociationRouteRegistration,
    RemoteError, RemoteLaneClassifier, RemoteSettings, Result as RemoteResult,
    TcpAssociationDialer, TcpAssociationIdentity, TcpAssociationListener,
    TcpAssociationListenerHandle, TcpAssociationListenerReport, TcpAssociationReaderHandle,
    TcpAssociationStreamReader,
};
use kairo_serialization::RemoteMessage;

use crate::{
    ClusterSystemInbound, GossipEnvelope, Heartbeat, HeartbeatRsp, InitJoin, InitJoinAck,
    InitJoinNack, Join, UniqueAddress, Welcome,
};

const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);
const CLUSTER_TCP_SHUTDOWN_REASON: &str = "cluster tcp association runtime shutdown";

/// Standalone TCP association runtime for cluster control traffic.
///
/// The runtime owns one listener, a shared bidirectional route cache, accepted-association
/// identities, and every outbound pipeline/reader it creates. It is the lower-level transport
/// used by the cluster TCP peer route components; gossip remains the source of peer intent.
pub struct ClusterTcpAssociationRuntime {
    self_node: UniqueAddress,
    local_address: RemoteAssociationAddress,
    settings: RemoteSettings,
    association_cache: RemoteAssociationCache,
    association_registry: RemoteAssociationRegistry,
    dialer: TcpAssociationDialer,
    outbound_reader: TcpAssociationStreamReader,
    outbound_readers: Arc<Mutex<Vec<TcpAssociationReaderHandle>>>,
    outbound_pipelines: Arc<Mutex<Vec<AssociationOutboundPipeline>>>,
    listener: TcpAssociationListenerHandle,
}

impl ClusterTcpAssociationRuntime {
    /// Binds a cluster TCP listener and constructs its manifest-aware inbound router.
    ///
    /// A configured port of zero is replaced with the listener's effective port before the local
    /// cluster and association identities are exposed. `node_uid` identifies the cluster member
    /// incarnation, while `local_system_uid` identifies the remoting ActorSystem incarnation.
    pub fn bind(
        local_system: impl Into<String>,
        node_uid: u64,
        local_system_uid: u64,
        settings: RemoteSettings,
        inbound: impl FnOnce(UniqueAddress, RemoteAssociationCache) -> ClusterSystemInbound,
    ) -> RemoteResult<Self> {
        let local_system = local_system.into();
        let listener = TcpListener::bind((
            settings.canonical_hostname.as_str(),
            settings.canonical_port,
        ))
        .map_err(|error| RemoteError::Inbound(format!("tcp bind failed: {error}")))?;
        let local_addr = listener
            .local_addr()
            .map_err(|error| RemoteError::Inbound(format!("tcp local address failed: {error}")))?;
        let effective_settings = RemoteSettings {
            canonical_hostname: settings.canonical_hostname.clone(),
            canonical_port: if settings.canonical_port == 0 {
                local_addr.port()
            } else {
                settings.canonical_port
            },
            connect_timeout: settings.connect_timeout,
        };
        let self_node = UniqueAddress::new(
            Address::new(
                "kairo",
                local_system.clone(),
                Some(effective_settings.canonical_hostname.clone()),
                Some(effective_settings.canonical_port),
            ),
            node_uid,
        );
        let local_address = RemoteAssociationAddress::new(
            "kairo",
            local_system,
            effective_settings.canonical_hostname.clone(),
            Some(effective_settings.canonical_port),
        )?;
        let association_cache = RemoteAssociationCache::new();
        let association_registry = RemoteAssociationRegistry::new();
        let installer = RemoteAssociationRouteInstaller::new(association_cache.clone())
            .with_classifier(cluster_lane_classifier());
        let inbound = Arc::new(inbound(self_node.clone(), association_cache.clone()));
        let outbound_reader = TcpAssociationStreamReader::new(inbound.clone());
        let listener = TcpAssociationListener::from_listener(listener, inbound)
            .with_local_address(local_address.clone())
            .with_association_registry(association_registry.clone())
            .with_route_installer(installer.clone())
            .spawn_accept_loop()?;
        let dialer = TcpAssociationDialer::new(installer)
            .with_local_identity(local_address.clone(), local_system_uid)
            .with_connect_timeout(effective_settings.connect_timeout_or_default());

        Ok(Self {
            self_node,
            local_address,
            settings: effective_settings,
            association_cache,
            association_registry,
            dialer,
            outbound_reader,
            outbound_readers: Arc::new(Mutex::new(Vec::new())),
            outbound_pipelines: Arc::new(Mutex::new(Vec::new())),
            listener,
        })
    }

    /// Returns the canonical local cluster member identity.
    pub fn self_node(&self) -> &UniqueAddress {
        &self.self_node
    }

    /// Returns the canonical transport address advertised during association handshakes.
    pub fn local_address(&self) -> &RemoteAssociationAddress {
        &self.local_address
    }

    /// Returns the effective remote settings, including an ephemeral port selected by bind.
    pub fn settings(&self) -> &RemoteSettings {
        &self.settings
    }

    /// Returns the shared bidirectional association route cache.
    pub fn association_cache(&self) -> &RemoteAssociationCache {
        &self.association_cache
    }

    /// Returns the registry of accepted remote association identities.
    pub fn association_registry(&self) -> &RemoteAssociationRegistry {
        &self.association_registry
    }

    /// Establishes an outbound association and retains ownership of its pipeline and reader.
    pub fn dial(
        &self,
        address: RemoteAssociationAddress,
    ) -> RemoteResult<RemoteAssociationRouteRegistration> {
        let (registration, reader_handle) = self
            .dialer
            .dial_with_reader(address, self.outbound_reader.clone())?;
        self.outbound_pipelines
            .lock()
            .expect("cluster tcp outbound pipelines lock poisoned")
            .push(registration.pipeline().clone());
        self.outbound_readers
            .lock()
            .expect("cluster tcp outbound readers lock poisoned")
            .push(reader_handle);
        Ok(registration)
    }

    /// Removes and closes the cached route for `address` with the default cluster reason.
    ///
    /// Returns whether a route was present.
    pub fn remove_route(&self, address: &RemoteAssociationAddress) -> bool {
        self.remove_route_with_reason(address, "cluster tcp association route removed")
    }

    /// Removes and closes the cached route for `address` with an explicit diagnostic `reason`.
    ///
    /// Returns whether a route was present.
    pub fn remove_route_with_reason(
        &self,
        address: &RemoteAssociationAddress,
        reason: &str,
    ) -> bool {
        self.association_cache
            .remove_route_and_close(address, reason)
            .is_some()
    }

    /// Stops the runtime using the default shutdown timeout policy.
    ///
    /// # Errors
    ///
    /// Returns the first route-close or listener failure, or
    /// [`RemoteError::ShutdownTimeout`] when the default shutdown deadline
    /// expires.
    pub fn shutdown(self) -> RemoteResult<TcpAssociationListenerReport> {
        self.shutdown_with_timeout(DEFAULT_SHUTDOWN_TIMEOUT)
    }

    /// Closes cached routes, stops owned readers and the listener, and reports accepted peers.
    ///
    /// The cache is cleared again after the listener joins so routes registered concurrently by a
    /// closing association cannot escape shutdown. One deadline bounds outbound-reader and listener
    /// joins; expiration returns [`RemoteError::ShutdownTimeout`] after forceful transport close.
    ///
    /// # Errors
    ///
    /// Returns the first route-close or listener failure, or
    /// [`RemoteError::ShutdownTimeout`] when `timeout` expires.
    pub fn shutdown_with_timeout(
        self,
        timeout: Duration,
    ) -> RemoteResult<TcpAssociationListenerReport> {
        let deadline = Instant::now() + timeout;
        let mut first_error = None;
        for result in self
            .association_cache
            .clear_routes_and_close(CLUSTER_TCP_SHUTDOWN_REASON)
        {
            if let Err(error) = result {
                first_error.get_or_insert(error);
            }
        }
        self.listener.stop();
        self.outbound_pipelines
            .lock()
            .expect("cluster tcp outbound pipelines lock poisoned")
            .clear();
        let outbound_readers = self
            .outbound_readers
            .lock()
            .expect("cluster tcp outbound readers lock poisoned")
            .drain(..)
            .collect::<Vec<_>>();
        let mut readers_stopped = true;
        for reader in outbound_readers {
            readers_stopped &= reader.join_after_stop_until(deadline).is_some();
        }
        let listener_report = self.listener.join_until(deadline);
        for result in self
            .association_cache
            .clear_routes_and_close(CLUSTER_TCP_SHUTDOWN_REASON)
        {
            if let Err(error) = result {
                first_error.get_or_insert(error);
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        if !readers_stopped || listener_report.is_none() {
            return Err(RemoteError::ShutdownTimeout { timeout });
        }
        listener_report.expect("listener completion checked above")
    }
}

/// Builds the lane classifier that prioritizes seed, membership, gossip, and heartbeat traffic.
pub fn cluster_lane_classifier() -> RemoteLaneClassifier {
    let mut classifier = RemoteLaneClassifier::default();
    classifier.add_control_manifest(InitJoin::MANIFEST);
    classifier.add_control_manifest(InitJoinAck::MANIFEST);
    classifier.add_control_manifest(InitJoinNack::MANIFEST);
    classifier.add_control_manifest(Join::MANIFEST);
    classifier.add_control_manifest(Welcome::MANIFEST);
    classifier.add_control_manifest(GossipEnvelope::MANIFEST);
    classifier.add_control_manifest(Heartbeat::MANIFEST);
    classifier.add_control_manifest(HeartbeatRsp::MANIFEST);
    classifier
}

/// Builds the remoting handshake identity for a named cluster ActorSystem incarnation.
pub fn cluster_association_identity_for(
    system: &str,
    settings: &RemoteSettings,
    uid: u64,
) -> RemoteResult<TcpAssociationIdentity> {
    Ok(TcpAssociationIdentity::new(
        RemoteAssociationAddress::new(
            "kairo",
            system,
            settings.canonical_hostname.clone(),
            Some(settings.canonical_port),
        )?,
        uid,
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use kairo_remote::RemoteOutbound;
    use kairo_serialization::{ActorRefWireData, Registry, RemoteEnvelope};
    use kairo_testkit::{ActorSystemTestKit, await_assert};

    use super::*;
    use crate::{
        ClusterMembershipMsg, ClusterMembershipRemoteEnvelopeOutbound,
        ClusterMembershipWireInbound, ClusterMembershipWireOutbound,
        DEFAULT_CLUSTER_HEARTBEAT_RECEIVER_PATH, DEFAULT_CLUSTER_HEARTBEAT_SENDER_PATH, Gossip,
        HeartbeatRemoteReceiverInbound, HeartbeatRemoteReceiverOutbound,
        HeartbeatRemoteResponseInbound, HeartbeatSenderMsg, Member,
        register_cluster_protocol_codecs, test_support::cluster_socket_test_lock,
    };

    fn registry() -> Arc<Registry> {
        let mut registry = Registry::new();
        register_cluster_protocol_codecs(&mut registry).unwrap();
        Arc::new(registry)
    }

    fn wait_for_route(runtime: &ClusterTcpAssociationRuntime) {
        await_assert(Duration::from_secs(1), Duration::from_millis(1), || {
            let actual = runtime.association_cache().route_count();
            (actual == 1)
                .then_some(())
                .ok_or_else(|| format!("expected 1 route, got {actual}"))
        })
        .unwrap();
    }

    fn wire_for(node: &UniqueAddress, path: &str) -> ActorRefWireData {
        ActorRefWireData::new(format!("{}{}", node.address, path)).unwrap()
    }

    #[derive(Default)]
    struct RecordingOutbound {
        close_reasons: Mutex<Vec<String>>,
    }

    impl RecordingOutbound {
        fn close_reasons(&self) -> Vec<String> {
            self.close_reasons
                .lock()
                .expect("recording outbound lock poisoned")
                .clone()
        }
    }

    impl RemoteOutbound for RecordingOutbound {
        fn send(&self, _envelope: RemoteEnvelope) -> RemoteResult<()> {
            Ok(())
        }

        fn close(&self, reason: &str) -> RemoteResult<()> {
            self.close_reasons
                .lock()
                .expect("recording outbound lock poisoned")
                .push(reason.to_string());
            Ok(())
        }
    }

    #[derive(Default)]
    struct NoopOutbound;

    impl RemoteOutbound for NoopOutbound {
        fn send(&self, _envelope: RemoteEnvelope) -> RemoteResult<()> {
            Ok(())
        }
    }

    struct LateRouteOnClose {
        cache: RemoteAssociationCache,
        late_address: RemoteAssociationAddress,
    }

    impl RemoteOutbound for LateRouteOnClose {
        fn send(&self, _envelope: RemoteEnvelope) -> RemoteResult<()> {
            Ok(())
        }

        fn close(&self, _reason: &str) -> RemoteResult<()> {
            self.cache
                .insert_route(self.late_address.clone(), Arc::new(NoopOutbound));
            Ok(())
        }
    }

    fn bind_runtime(
        name: &str,
        uid: u64,
        system_uid: u64,
        kit: &ActorSystemTestKit,
        registry: Arc<Registry>,
    ) -> (
        ClusterTcpAssociationRuntime,
        kairo_testkit::TestProbe<ClusterMembershipMsg>,
        kairo_testkit::TestProbe<HeartbeatSenderMsg>,
    ) {
        let membership = kit
            .create_probe::<ClusterMembershipMsg>("membership")
            .unwrap();
        let heartbeat_sender = kit
            .create_probe::<HeartbeatSenderMsg>("heartbeat-sender")
            .unwrap();
        let membership_ref = membership.actor_ref();
        let heartbeat_sender_ref = heartbeat_sender.actor_ref();
        let runtime = ClusterTcpAssociationRuntime::bind(
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
        .unwrap();
        (runtime, membership, heartbeat_sender)
    }

    #[test]
    fn tcp_runtime_routes_membership_and_heartbeat_over_bidirectional_association() {
        let _guard = cluster_socket_test_lock();
        let sender_kit = ActorSystemTestKit::new("cluster-tcp-sender").unwrap();
        let receiver_kit = ActorSystemTestKit::new("cluster-tcp-receiver").unwrap();
        let registry = registry();
        let (sender, _sender_membership, sender_heartbeat) =
            bind_runtime("sender", 1, 11, &sender_kit, registry.clone());
        let (receiver, receiver_membership, _receiver_heartbeat) =
            bind_runtime("receiver", 2, 22, &receiver_kit, registry.clone());
        let registration = sender.dial(receiver.local_address().clone()).unwrap();
        wait_for_route(&receiver);
        assert!(
            receiver
                .association_registry()
                .association_by_uid(11)
                .is_some()
        );

        let membership_outbound = ClusterMembershipWireOutbound::new(
            receiver.self_node().clone(),
            registry.clone(),
            ClusterMembershipRemoteEnvelopeOutbound::from_arc(Arc::new(
                sender.association_cache().clone(),
            )
                as Arc<dyn RemoteOutbound>),
        );
        membership_outbound
            .send_membership(ClusterMembershipMsg::Join {
                join: Join {
                    node: sender.self_node().clone(),
                    roles: vec!["backend".to_string()],
                    app_version: crate::ApplicationVersion::default(),
                },
                reply_to: None,
            })
            .unwrap();
        match receiver_membership
            .expect_msg(Duration::from_secs(1))
            .unwrap()
        {
            ClusterMembershipMsg::Join { join, reply_to } => {
                assert_eq!(join.node, *sender.self_node());
                assert_eq!(join.roles, vec!["backend".to_string()]);
                assert!(reply_to.is_none());
            }
            _ => panic!("expected cluster join"),
        }

        let heartbeat_outbound = HeartbeatRemoteReceiverOutbound::from_arc(
            receiver.self_node().clone(),
            registry.clone(),
            wire_for(sender.self_node(), DEFAULT_CLUSTER_HEARTBEAT_SENDER_PATH),
            Arc::new(sender.association_cache().clone()) as Arc<dyn RemoteOutbound>,
        );
        heartbeat_outbound
            .send_heartbeat(Heartbeat {
                from: sender.self_node().clone(),
                sequence_nr: 7,
                creation_time_nanos: 99,
            })
            .unwrap();
        match sender_heartbeat.expect_msg(Duration::from_secs(1)).unwrap() {
            HeartbeatSenderMsg::HeartbeatResponse(response) => {
                assert_eq!(response.from, *receiver.self_node());
                assert_eq!(response.sequence_nr, 7);
                assert_eq!(response.creation_time_nanos, 99);
            }
            _ => panic!("expected heartbeat response"),
        }

        assert!(sender.remove_route(receiver.local_address()));
        assert!(!sender.remove_route(receiver.local_address()));
        assert_eq!(sender.association_cache().route_count(), 0);

        let expected_sender_identity =
            cluster_association_identity_for("sender", sender.settings(), 11).unwrap();
        let sender_report = sender.shutdown().unwrap();
        assert_eq!(sender_report.accepted_associations, 0);
        assert_eq!(registration.address(), receiver.local_address());
        let receiver_report = receiver.shutdown().unwrap();
        assert_eq!(receiver_report.accepted_associations, 1);
        assert_eq!(
            receiver_report.remote_identities,
            vec![expected_sender_identity]
        );
        sender_kit.shutdown(Duration::from_secs(1)).unwrap();
        receiver_kit.shutdown(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn tcp_runtime_shutdown_closes_cached_routes_with_live_reverse_route() {
        let _guard = cluster_socket_test_lock();
        let sender_kit = ActorSystemTestKit::new("cluster-tcp-shutdown-sender").unwrap();
        let receiver_kit = ActorSystemTestKit::new("cluster-tcp-shutdown-receiver").unwrap();
        let registry = registry();
        let (sender, _sender_membership, _sender_heartbeat) =
            bind_runtime("sender", 1, 11, &sender_kit, registry.clone());
        let (receiver, _receiver_membership, _receiver_heartbeat) =
            bind_runtime("receiver", 2, 22, &receiver_kit, registry);
        let receiver_address = receiver.local_address().clone();
        let registration = sender.dial(receiver_address.clone()).unwrap();
        wait_for_route(&receiver);
        assert!(
            receiver
                .association_registry()
                .association_by_uid(11)
                .is_some()
        );
        let recording_route = Arc::new(RecordingOutbound::default());
        receiver.association_cache().insert_route(
            RemoteAssociationAddress::new("kairo", "recorded", "127.0.0.1", Some(2552)).unwrap(),
            recording_route.clone() as Arc<dyn RemoteOutbound>,
        );
        assert_eq!(receiver.association_cache().route_count(), 2);

        let receiver_report = receiver.shutdown().unwrap();

        assert_eq!(registration.address(), &receiver_address);
        assert_eq!(receiver_report.accepted_associations, 1);
        assert_eq!(
            recording_route.close_reasons(),
            vec![CLUSTER_TCP_SHUTDOWN_REASON.to_string()]
        );
        let sender_report = sender.shutdown().unwrap();
        assert_eq!(sender_report.accepted_associations, 0);
        sender_kit.shutdown(Duration::from_secs(1)).unwrap();
        receiver_kit.shutdown(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn tcp_runtime_shutdown_clears_late_routes_registered_during_shutdown() {
        let _guard = cluster_socket_test_lock();
        let kit = ActorSystemTestKit::new("cluster-tcp-late-route").unwrap();
        let registry = registry();
        let (runtime, _membership, _heartbeat) = bind_runtime("late-route", 1, 11, &kit, registry);
        let cache = runtime.association_cache().clone();
        let initial_address =
            RemoteAssociationAddress::new("kairo", "initial", "127.0.0.1", Some(2552)).unwrap();
        let late_address =
            RemoteAssociationAddress::new("kairo", "late", "127.0.0.1", Some(2553)).unwrap();
        cache.insert_route(
            initial_address,
            Arc::new(LateRouteOnClose {
                cache: cache.clone(),
                late_address,
            }),
        );
        assert_eq!(cache.route_count(), 1);

        let report = runtime.shutdown().unwrap();

        assert_eq!(report.accepted_associations, 0);
        assert_eq!(cache.route_count(), 0);
        kit.shutdown(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn cluster_classifier_routes_system_manifests_to_control_lane() {
        let classifier = cluster_lane_classifier();
        let registry = registry();
        let target = UniqueAddress::new(
            Address::new(
                "kairo",
                "receiver",
                Some("127.0.0.1".to_string()),
                Some(2552),
            ),
            2,
        );
        let envelope = RemoteEnvelope::new(
            wire_for(&target, crate::DEFAULT_CLUSTER_MEMBERSHIP_REMOTE_PATH),
            None,
            registry
                .serialize(&Welcome {
                    from: UniqueAddress::new(Address::local("sender"), 1),
                    gossip: Gossip::from_members([Member::new(target, vec![])]),
                })
                .unwrap(),
        );

        assert_eq!(
            classifier.classify(&envelope, 128),
            kairo_remote::RemoteStreamId::Control
        );
        let seed_contact = RemoteEnvelope::new(
            ActorRefWireData::new("kairo://receiver@127.0.0.1:2552/system/cluster/core/daemon")
                .unwrap(),
            None,
            registry
                .serialize(&InitJoin {
                    joining_config_digest: bytes::Bytes::new(),
                })
                .unwrap(),
        );
        assert_eq!(
            classifier.classify(&seed_contact, 128),
            kairo_remote::RemoteStreamId::Control
        );
    }
}
