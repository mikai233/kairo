#![deny(missing_docs)]

//! Standalone TCP association ownership for cluster-tools protocols.
//!
//! [`ClusterToolsTcpAssociationRuntime`] binds one listener, installs inbound
//! pubsub and singleton routing, and owns outbound routes dialed through that
//! listener. Applications that already use the ActorSystem-owned remoting
//! runtime should compose cluster tools through the shared registration APIs;
//! this runtime is the cluster-tools-only transport boundary.

use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use kairo_actor::Address;
use kairo_cluster::UniqueAddress;
use kairo_remote::{
    AssociationOutboundPipeline, RemoteAssociationAddress, RemoteAssociationCache,
    RemoteAssociationRegistry, RemoteAssociationRouteInstaller, RemoteAssociationRouteRegistration,
    RemoteError, RemoteLaneClassifier, RemoteSettings, Result as RemoteResult,
    TcpAssociationDialer, TcpAssociationIdentity, TcpAssociationListener,
    TcpAssociationListenerHandle, TcpAssociationListenerReport, TcpAssociationReaderHandle,
    TcpAssociationStreamReader,
};
use kairo_serialization::RemoteMessage;

use crate::{CLUSTER_TOOLS_SYSTEM_MANIFESTS, ClusterToolsSystemInbound};

const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);
const CLUSTER_TOOLS_TCP_SHUTDOWN_REASON: &str = "cluster-tools tcp association runtime shutdown";

/// Owns a cluster-tools-only TCP listener and its outbound peer associations.
pub struct ClusterToolsTcpAssociationRuntime<M>
where
    M: RemoteMessage + Send + 'static,
{
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
    _message: std::marker::PhantomData<fn(M)>,
}

impl<M> ClusterToolsTcpAssociationRuntime<M>
where
    M: RemoteMessage + Send + 'static,
{
    /// Binds a listener and installs the cluster-tools system inbound router.
    ///
    /// A canonical port of zero selects an ephemeral port and the runtime's
    /// effective [`RemoteSettings`] and addresses expose the assigned port.
    ///
    /// # Errors
    ///
    /// Returns an error when the listener cannot bind, its local address cannot
    /// be read, the association identity is invalid, or the accept loop cannot
    /// start.
    pub fn bind(
        local_system: impl Into<String>,
        node_uid: u64,
        local_system_uid: u64,
        settings: RemoteSettings,
        inbound: impl FnOnce(UniqueAddress) -> ClusterToolsSystemInbound<M>,
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
            .with_classifier(cluster_tools_lane_classifier());
        let inbound = Arc::new(inbound(self_node.clone()));
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
            _message: std::marker::PhantomData,
        })
    }

    /// Returns the effective local cluster node identity.
    pub fn self_node(&self) -> &UniqueAddress {
        &self.self_node
    }

    /// Returns the effective local remoting association address.
    pub fn local_address(&self) -> &RemoteAssociationAddress {
        &self.local_address
    }

    /// Returns the effective remoting settings, including an assigned port.
    pub fn settings(&self) -> &RemoteSettings {
        &self.settings
    }

    /// Returns the cache used to route outbound messages to active peers.
    pub fn association_cache(&self) -> &RemoteAssociationCache {
        &self.association_cache
    }

    /// Returns the registry of accepted remote association identities.
    pub fn association_registry(&self) -> &RemoteAssociationRegistry {
        &self.association_registry
    }

    /// Dials `address` and installs the resulting outbound route.
    ///
    /// # Errors
    ///
    /// Returns an error when the TCP association cannot connect, negotiate its
    /// lane streams, or install its route.
    pub fn dial(
        &self,
        address: RemoteAssociationAddress,
    ) -> RemoteResult<RemoteAssociationRouteRegistration> {
        let (registration, reader_handle) = self
            .dialer
            .dial_with_reader(address, self.outbound_reader.clone())?;
        self.outbound_pipelines
            .lock()
            .expect("cluster-tools tcp outbound pipelines lock poisoned")
            .push(registration.pipeline().clone());
        self.outbound_readers
            .lock()
            .expect("cluster-tools tcp outbound readers lock poisoned")
            .push(reader_handle);
        Ok(registration)
    }

    /// Removes and closes the route to `address` with the default reason.
    ///
    /// Returns `true` when an active route was removed.
    pub fn remove_route(&self, address: &RemoteAssociationAddress) -> bool {
        self.remove_route_with_reason(address, "cluster-tools tcp association route removed")
    }

    /// Removes and closes the route to `address` with `reason`.
    ///
    /// Returns `true` when an active route was removed.
    pub fn remove_route_with_reason(
        &self,
        address: &RemoteAssociationAddress,
        reason: &str,
    ) -> bool {
        self.association_cache
            .remove_route_and_close(address, reason)
            .is_some()
    }

    /// Stops the runtime using the default shutdown policy.
    ///
    /// # Errors
    ///
    /// Returns the first route-close or listener failure.
    pub fn shutdown(self) -> RemoteResult<TcpAssociationListenerReport> {
        self.shutdown_with_timeout(DEFAULT_SHUTDOWN_TIMEOUT)
    }

    /// Closes cached routes, stops owned readers and the listener, and reports accepted peers.
    ///
    /// The cache is cleared again after the listener joins so routes registered
    /// concurrently by a closing association cannot escape shutdown. The
    /// supplied deadline bounds outbound-reader and listener joins; expiration
    /// returns [`RemoteError::ShutdownTimeout`] after forceful transport close.
    ///
    /// # Errors
    ///
    /// Returns the first route-close, listener, or timeout failure.
    pub fn shutdown_with_timeout(
        self,
        timeout: Duration,
    ) -> RemoteResult<TcpAssociationListenerReport> {
        let deadline = Instant::now() + timeout;
        let mut first_error = None;
        for result in self
            .association_cache
            .clear_routes_and_close(CLUSTER_TOOLS_TCP_SHUTDOWN_REASON)
        {
            if let Err(error) = result {
                first_error.get_or_insert(error);
            }
        }
        self.listener.stop();
        self.outbound_pipelines
            .lock()
            .expect("cluster-tools tcp outbound pipelines lock poisoned")
            .clear();
        let outbound_readers = self
            .outbound_readers
            .lock()
            .expect("cluster-tools tcp outbound readers lock poisoned")
            .drain(..)
            .collect::<Vec<_>>();
        let mut readers_stopped = true;
        for reader in outbound_readers {
            readers_stopped &= reader.join_after_stop_until(deadline).is_some();
        }
        let listener_report = self.listener.join_until(deadline);
        for result in self
            .association_cache
            .clear_routes_and_close(CLUSTER_TOOLS_TCP_SHUTDOWN_REASON)
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

/// Builds the lane classifier for cluster-tools control-plane protocols.
///
/// Every manifest in [`CLUSTER_TOOLS_SYSTEM_MANIFESTS`] uses the ordered
/// control lane. Unlisted business messages retain the ordinary lane.
pub fn cluster_tools_lane_classifier() -> RemoteLaneClassifier {
    let mut classifier = RemoteLaneClassifier::default();
    for manifest in CLUSTER_TOOLS_SYSTEM_MANIFESTS {
        classifier.add_control_manifest(manifest);
    }
    classifier
}

/// Builds the local association identity for a cluster-tools TCP runtime.
///
/// # Errors
///
/// Returns an error when the configured system or canonical address cannot be
/// represented as a remote association address.
pub fn cluster_tools_association_identity_for(
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
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use kairo_actor::Recipient;
    use kairo_remote::{AssociationState, RemoteOutbound, RemoteStreamId};
    use kairo_serialization::{
        Manifest, MessageCodec, Registry, RemoteMessage, SerializationRegistry, SerializedMessage,
    };
    use kairo_testkit::{ActorSystemTestKit, await_assert};

    use super::*;
    use crate::{
        DistributedPubSubMediatorMsg, LocalPubSubMsg, PubSubGossipMsg, PubSubGossipWireInbound,
        PubSubRemoteDeliveryInbound, PubSubRemoteDeliveryOutbound, PubSubRemoteEnvelopeOutbound,
        PubSubSerializedGossip, PubSubStatus, SingletonManagerEffect, SingletonManagerMsg,
        SingletonManagerRemoteInbound, SingletonManagerRemoteOutbound, TopicName, TopicPublishMode,
        register_cluster_tools_protocol_codecs, test_support::cluster_tools_socket_test_lock,
    };

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestMessage {
        value: u8,
    }

    impl RemoteMessage for TestMessage {
        const MANIFEST: &'static str = "kairo.cluster-tools.test.tcp-message";
        const VERSION: u16 = 1;
    }

    #[derive(Debug, Clone, Copy)]
    struct TestMessageCodec;

    impl MessageCodec<TestMessage> for TestMessageCodec {
        fn serializer_id(&self) -> u32 {
            59_201
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

    fn wait_for_route<M>(runtime: &ClusterToolsTcpAssociationRuntime<M>)
    where
        M: RemoteMessage + Send + 'static,
    {
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
    ) -> (
        ClusterToolsTcpAssociationRuntime<TestMessage>,
        kairo_testkit::TestProbe<PubSubGossipMsg>,
        kairo_testkit::TestProbe<DistributedPubSubMediatorMsg<TestMessage>>,
        kairo_testkit::TestProbe<SingletonManagerMsg>,
    ) {
        let gossip = kit.create_probe::<PubSubGossipMsg>("gossip").unwrap();
        let mediator = kit
            .create_probe::<DistributedPubSubMediatorMsg<TestMessage>>("mediator")
            .unwrap();
        let manager = kit
            .create_probe::<SingletonManagerMsg>("singleton-manager")
            .unwrap();
        let gossip_ref = gossip.actor_ref();
        let mediator_ref = mediator.actor_ref();
        let manager_ref = manager.actor_ref();
        let runtime = ClusterToolsTcpAssociationRuntime::bind(
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
        .unwrap();
        (runtime, gossip, mediator, manager)
    }

    #[derive(Default)]
    struct NoopOutbound;

    impl RemoteOutbound for NoopOutbound {
        fn send(&self, _envelope: kairo_serialization::RemoteEnvelope) -> RemoteResult<()> {
            Ok(())
        }
    }

    struct LateRouteOnClose {
        cache: RemoteAssociationCache,
        late_address: RemoteAssociationAddress,
    }

    impl RemoteOutbound for LateRouteOnClose {
        fn send(&self, _envelope: kairo_serialization::RemoteEnvelope) -> RemoteResult<()> {
            Ok(())
        }

        fn close(&self, _reason: &str) -> RemoteResult<()> {
            self.cache
                .insert_route(self.late_address.clone(), Arc::new(NoopOutbound));
            Ok(())
        }
    }

    #[test]
    fn tcp_runtime_routes_pubsub_and_singleton_system_messages_bidirectionally() {
        let _guard = cluster_tools_socket_test_lock();
        let sender_kit = ActorSystemTestKit::new("cluster-tools-tcp-sender").unwrap();
        let receiver_kit = ActorSystemTestKit::new("cluster-tools-tcp-receiver").unwrap();
        let registry = registry();
        let (sender, sender_gossip, _sender_mediator, sender_manager) =
            bind_runtime("sender", 1, 11, &sender_kit, registry.clone());
        let (receiver, receiver_gossip, receiver_mediator, _receiver_manager) =
            bind_runtime("receiver", 2, 22, &receiver_kit, registry.clone());
        let registration = sender.dial(receiver.local_address().clone()).unwrap();
        wait_for_route(&receiver);
        assert!(
            receiver
                .association_registry()
                .association_by_uid(11)
                .is_some()
        );

        let pubsub_gossip_outbound = PubSubRemoteEnvelopeOutbound::from_arc(Arc::new(
            sender.association_cache().clone(),
        )
            as Arc<dyn RemoteOutbound>);
        pubsub_gossip_outbound
            .tell(PubSubSerializedGossip::new(
                receiver.self_node().clone(),
                registry
                    .serialize(&PubSubStatus {
                        from: sender.self_node().clone(),
                        versions: BTreeMap::from([(sender.self_node().ordering_key(), 7)]),
                        reply: true,
                    })
                    .unwrap(),
            ))
            .unwrap();
        match receiver_gossip.expect_msg(Duration::from_secs(1)).unwrap() {
            PubSubGossipMsg::Status {
                from,
                versions,
                reply,
            } => {
                assert_eq!(from, *sender.self_node());
                assert_eq!(versions[&sender.self_node().ordering_key()], 7);
                assert!(reply);
            }
            _ => panic!("expected pubsub status"),
        }

        let pubsub_delivery_outbound = PubSubRemoteDeliveryOutbound::<TestMessage>::from_arc(
            receiver.self_node().clone(),
            registry.clone(),
            Arc::new(sender.association_cache().clone()) as Arc<dyn RemoteOutbound>,
        );
        pubsub_delivery_outbound
            .tell(LocalPubSubMsg::Publish {
                topic: TopicName::new("orders"),
                message: TestMessage { value: 42 },
                mode: TopicPublishMode::Broadcast,
                reply_to: None,
            })
            .unwrap();
        match receiver_mediator
            .expect_msg(Duration::from_secs(1))
            .unwrap()
        {
            DistributedPubSubMediatorMsg::LocalDelivery(LocalPubSubMsg::Publish {
                topic,
                message,
                mode,
                reply_to,
            }) => {
                assert_eq!(topic, TopicName::new("orders"));
                assert_eq!(message, TestMessage { value: 42 });
                assert_eq!(mode, TopicPublishMode::Broadcast);
                assert!(reply_to.is_none());
            }
            _ => panic!("expected pubsub local delivery"),
        }
        pubsub_delivery_outbound
            .tell(LocalPubSubMsg::Send {
                path: "/user/worker".to_string(),
                message: TestMessage { value: 43 },
                reply_to: None,
            })
            .unwrap();
        match receiver_mediator
            .expect_msg(Duration::from_secs(1))
            .unwrap()
        {
            DistributedPubSubMediatorMsg::LocalDelivery(LocalPubSubMsg::Send {
                path,
                message,
                reply_to,
            }) => {
                assert_eq!(path, "/user/worker");
                assert_eq!(message, TestMessage { value: 43 });
                assert!(reply_to.is_none());
            }
            _ => panic!("expected pubsub path delivery"),
        }

        let singleton_outbound = SingletonManagerRemoteOutbound::from_arc(
            receiver.self_node().clone(),
            registry,
            Arc::new(receiver.association_cache().clone()) as Arc<dyn RemoteOutbound>,
        );
        singleton_outbound
            .tell(vec![SingletonManagerEffect::SendHandOverToMe {
                to: sender.self_node().clone(),
            }])
            .unwrap();
        match sender_manager.expect_msg(Duration::from_secs(1)).unwrap() {
            SingletonManagerMsg::HandOverToMe { from, reply_to } => {
                assert_eq!(from, *receiver.self_node());
                assert!(reply_to.is_none());
            }
            _ => panic!("expected singleton handover"),
        }
        sender_gossip
            .expect_no_msg(Duration::from_millis(50))
            .unwrap();

        let expected_sender_identity =
            cluster_tools_association_identity_for("sender", sender.settings(), 11).unwrap();
        let sender_report = sender.shutdown().unwrap();
        assert_eq!(sender_report.accepted_associations, 0);
        assert_eq!(registration.address(), receiver.local_address());
        assert!(matches!(
            registration
                .pipeline()
                .association()
                .lock()
                .expect("association mutex poisoned")
                .state(),
            AssociationState::Closed { reason }
                if reason == CLUSTER_TOOLS_TCP_SHUTDOWN_REASON
        ));
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
    fn tcp_runtime_shutdown_clears_late_routes_registered_during_shutdown() {
        let _guard = cluster_tools_socket_test_lock();
        let kit = ActorSystemTestKit::new("cluster-tools-tcp-late-route").unwrap();
        let registry = registry();
        let (runtime, _gossip, _mediator, _manager) =
            bind_runtime("late-route", 1, 11, &kit, registry);
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
    fn cluster_tools_classifier_routes_system_manifests_to_control_lane() {
        let classifier = cluster_tools_lane_classifier();
        let recipient = kairo_serialization::ActorRefWireData::new(
            "kairo://receiver@127.0.0.1:2552/system/pubsub",
        )
        .unwrap();
        for manifest in CLUSTER_TOOLS_SYSTEM_MANIFESTS {
            let envelope = kairo_serialization::RemoteEnvelope::new(
                recipient.clone(),
                None,
                SerializedMessage::new(1, Manifest::new(manifest), 1, Bytes::new()),
            );
            assert_eq!(
                classifier.classify(&envelope, 128),
                RemoteStreamId::Control,
                "{manifest} must use the control lane"
            );
        }

        let business = kairo_serialization::RemoteEnvelope::new(
            recipient,
            None,
            SerializedMessage::new(
                1,
                Manifest::new(TestMessage::MANIFEST),
                TestMessage::VERSION,
                Bytes::new(),
            ),
        );
        assert_eq!(
            classifier.classify(&business, 128),
            RemoteStreamId::Ordinary
        );
    }
}
