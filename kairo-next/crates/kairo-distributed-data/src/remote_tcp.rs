#![deny(missing_docs)]

use std::collections::BTreeMap;
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use kairo_remote::{
    AssociationOutboundPipeline, RemoteAssociationAddress, RemoteAssociationCache,
    RemoteAssociationRegistry, RemoteAssociationRouteInstaller, RemoteAssociationRouteRegistration,
    RemoteError, RemoteFrameHandler, RemoteSettings, Result as RemoteResult, TcpAssociationDialer,
    TcpAssociationIdentity, TcpAssociationListener, TcpAssociationListenerHandle,
    TcpAssociationListenerReport, TcpAssociationReaderHandle, TcpAssociationStreamReader,
};

use crate::{
    ReplicaId, ReplicatorRemoteAssociationInbound, ReplicatorRemoteReplyReceiver,
    ReplicatorRemoteRequestReceiver, ReplicatorRemoteSourceMap,
};

const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);
const REPLICATOR_TCP_SHUTDOWN_REASON: &str = "replicator tcp association runtime shutdown";

/// Standalone TCP association runtime for distributed-data system traffic.
///
/// The runtime owns one listener, a shared bidirectional route cache, accepted-association
/// identities, source-replica mappings, and every outbound pipeline and reader it creates.
/// Cluster membership remains the source of peer intent; this transport only realizes routes.
pub struct ReplicatorTcpAssociationRuntime {
    local_replica: ReplicaId,
    remote_replica: ReplicaId,
    local_address: RemoteAssociationAddress,
    settings: RemoteSettings,
    requests: Arc<dyn ReplicatorRemoteRequestReceiver>,
    replies: Arc<dyn ReplicatorRemoteReplyReceiver>,
    association_cache: RemoteAssociationCache,
    association_registry: RemoteAssociationRegistry,
    source_replicas: ReplicatorRemoteSourceMap,
    dialer: TcpAssociationDialer,
    outbound_reader: TcpAssociationStreamReader,
    outbound_readers: Arc<Mutex<Vec<TcpAssociationReaderHandle>>>,
    outbound_pipelines: Arc<Mutex<Vec<AssociationOutboundPipeline>>>,
    listener: TcpAssociationListenerHandle,
}

impl ReplicatorTcpAssociationRuntime {
    /// Binds a distributed-data listener and constructs its request/reply inbound router.
    ///
    /// A configured port of zero is replaced with the listener's effective port. The
    /// `local_system_uid` identifies the remoting ActorSystem incarnation, while the replica
    /// identifiers provide local identity and an inbound fallback when no source mapping exists.
    pub fn bind(
        local_system: impl Into<String>,
        local_replica: ReplicaId,
        remote_replica: ReplicaId,
        local_system_uid: u64,
        settings: RemoteSettings,
        requests: Arc<dyn ReplicatorRemoteRequestReceiver>,
        replies: Arc<dyn ReplicatorRemoteReplyReceiver>,
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
        let local_address = RemoteAssociationAddress::new(
            "kairo",
            local_system,
            effective_settings.canonical_hostname.clone(),
            Some(effective_settings.canonical_port),
        )?;
        let association_cache = RemoteAssociationCache::new();
        let association_registry = RemoteAssociationRegistry::new();
        let source_replicas = Arc::new(Mutex::new(BTreeMap::new()));
        let installer = RemoteAssociationRouteInstaller::new(association_cache.clone());
        let listener_requests = Arc::clone(&requests);
        let listener_replies = Arc::clone(&replies);
        let listener_fallback_replica = remote_replica.clone();
        let listener_source_replicas = Arc::clone(&source_replicas);
        let handler_factory = Arc::new(
            move |identity: Option<&TcpAssociationIdentity>| -> Arc<dyn RemoteFrameHandler> {
                match identity {
                    Some(identity) => Arc::new(ReplicatorRemoteAssociationInbound::from_address(
                        identity.address().clone(),
                        Arc::clone(&listener_source_replicas),
                        listener_fallback_replica.clone(),
                        Arc::clone(&listener_requests),
                        Arc::clone(&listener_replies),
                    )) as Arc<dyn RemoteFrameHandler>,
                    None => Arc::new(ReplicatorRemoteAssociationInbound::new(
                        listener_fallback_replica.clone(),
                        Arc::clone(&listener_requests),
                        Arc::clone(&listener_replies),
                    )) as Arc<dyn RemoteFrameHandler>,
                }
            },
        );
        let inbound = Arc::new(ReplicatorRemoteAssociationInbound::new(
            remote_replica.clone(),
            Arc::clone(&requests),
            Arc::clone(&replies),
        ));
        let outbound_reader = TcpAssociationStreamReader::new(inbound.clone());
        let listener = TcpAssociationListener::from_listener(listener, inbound)
            .with_local_address(local_address.clone())
            .with_association_registry(association_registry.clone())
            .with_route_installer(installer.clone())
            .with_handler_factory(handler_factory)
            .spawn_accept_loop()?;
        let dialer = TcpAssociationDialer::new(installer)
            .with_local_identity(local_address.clone(), local_system_uid)
            .with_connect_timeout(effective_settings.connect_timeout_or_default());

        Ok(Self {
            local_replica,
            remote_replica,
            local_address,
            settings: effective_settings,
            requests,
            replies,
            association_cache,
            association_registry,
            source_replicas,
            dialer,
            outbound_reader,
            outbound_readers: Arc::new(Mutex::new(Vec::new())),
            outbound_pipelines: Arc::new(Mutex::new(Vec::new())),
            listener,
        })
    }

    /// Returns the local distributed-data replica identity.
    pub fn local_replica(&self) -> &ReplicaId {
        &self.local_replica
    }

    pub(crate) fn with_local_replica(mut self, local_replica: ReplicaId) -> Self {
        self.local_replica = local_replica;
        self
    }

    /// Returns the fallback replica identity used for inbound traffic without a source mapping.
    pub fn remote_replica(&self) -> &ReplicaId {
        &self.remote_replica
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

    /// Establishes a route using the runtime's fallback inbound replica identity.
    ///
    /// The runtime retains ownership of the resulting pipeline and reader.
    pub fn dial(
        &self,
        address: RemoteAssociationAddress,
    ) -> RemoteResult<RemoteAssociationRouteRegistration> {
        let (registration, reader_handle) = self
            .dialer
            .dial_with_reader(address, self.outbound_reader.clone())?;
        self.outbound_pipelines
            .lock()
            .expect("replicator tcp outbound pipelines lock poisoned")
            .push(registration.pipeline().clone());
        self.outbound_readers
            .lock()
            .expect("replicator tcp outbound readers lock poisoned")
            .push(reader_handle);
        Ok(registration)
    }

    /// Establishes a route whose inbound replies and requests are attributed to `replica`.
    ///
    /// The source mapping is removed if dialing fails.
    pub fn dial_peer(
        &self,
        address: RemoteAssociationAddress,
        replica: ReplicaId,
    ) -> RemoteResult<RemoteAssociationRouteRegistration> {
        self.register_source_replica(address.clone(), replica.clone());
        let inbound = Arc::new(ReplicatorRemoteAssociationInbound::new(
            replica,
            Arc::clone(&self.requests),
            Arc::clone(&self.replies),
        ));
        let reader = TcpAssociationStreamReader::new(inbound);
        match self.dial_with_reader(address.clone(), reader) {
            Ok(registration) => Ok(registration),
            Err(error) => {
                self.unregister_source_replica(&address);
                Err(error)
            }
        }
    }

    fn dial_with_reader(
        &self,
        address: RemoteAssociationAddress,
        reader: TcpAssociationStreamReader,
    ) -> RemoteResult<RemoteAssociationRouteRegistration> {
        let (registration, reader_handle) = self.dialer.dial_with_reader(address, reader)?;
        self.outbound_pipelines
            .lock()
            .expect("replicator tcp outbound pipelines lock poisoned")
            .push(registration.pipeline().clone());
        self.outbound_readers
            .lock()
            .expect("replicator tcp outbound readers lock poisoned")
            .push(reader_handle);
        Ok(registration)
    }

    /// Associates transport traffic from `address` with a distributed-data replica identity.
    pub fn register_source_replica(&self, address: RemoteAssociationAddress, replica: ReplicaId) {
        self.source_replicas
            .lock()
            .expect("replicator remote source map lock poisoned")
            .insert(address, replica);
    }

    /// Removes and returns the source-replica mapping for `address`, if present.
    pub fn unregister_source_replica(
        &self,
        address: &RemoteAssociationAddress,
    ) -> Option<ReplicaId> {
        self.source_replicas
            .lock()
            .expect("replicator remote source map lock poisoned")
            .remove(address)
    }

    /// Removes the source mapping and closes the cached route with the default reason.
    ///
    /// Returns whether a route was present.
    pub fn remove_route(&self, address: &RemoteAssociationAddress) -> bool {
        self.remove_route_with_reason(address, "replicator tcp association route removed")
    }

    /// Removes the source mapping and closes the cached route with an explicit diagnostic reason.
    ///
    /// Returns whether a route was present.
    pub fn remove_route_with_reason(
        &self,
        address: &RemoteAssociationAddress,
        reason: &str,
    ) -> bool {
        self.unregister_source_replica(address);
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
            .clear_routes_and_close(REPLICATOR_TCP_SHUTDOWN_REASON)
        {
            if let Err(error) = result {
                first_error.get_or_insert(error);
            }
        }
        self.listener.stop();
        self.outbound_pipelines
            .lock()
            .expect("replicator tcp outbound pipelines lock poisoned")
            .clear();
        let outbound_readers = self
            .outbound_readers
            .lock()
            .expect("replicator tcp outbound readers lock poisoned")
            .drain(..)
            .collect::<Vec<_>>();
        let mut readers_stopped = true;
        for reader in outbound_readers {
            readers_stopped &= reader.join_after_stop_until(deadline).is_some();
        }
        let listener_report = self.listener.join_until(deadline);
        for result in self
            .association_cache
            .clear_routes_and_close(REPLICATOR_TCP_SHUTDOWN_REASON)
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

/// Builds the canonical `/system/ddata` wire actor reference for an ActorSystem.
pub fn replicator_actor_ref_for(
    system: &str,
    settings: &RemoteSettings,
) -> RemoteResult<kairo_serialization::ActorRefWireData> {
    kairo_serialization::ActorRefWireData::new(format!(
        "kairo://{}@{}:{}/system/ddata",
        system, settings.canonical_hostname, settings.canonical_port
    ))
    .map_err(RemoteError::from)
}

/// Builds the remoting handshake identity for a named ActorSystem incarnation.
pub fn tcp_association_identity_for(
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
    use std::sync::{Condvar, Mutex};
    use std::time::{Duration, Instant};

    use kairo_actor::Recipient;
    use kairo_remote::{AssociationState, RemoteOutbound};
    use kairo_serialization::{Registry, RemoteEnvelope, RemoteMessage};
    use kairo_testkit::await_assert;

    use super::*;
    use crate::{
        ReplicatorRead, ReplicatorReadResult, ReplicatorRemoteAssociationCacheOutbound,
        ReplicatorRemoteEnvelopeOutbound, ReplicatorRemoteReplyError, ReplicatorRemoteRequestError,
        ReplicatorRemoteTarget, register_ddata_protocol_codecs,
        test_support::ddata_socket_test_lock,
    };

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

    #[derive(Default)]
    struct RecordingReplies {
        received: Mutex<Vec<(ReplicaId, RemoteEnvelope)>>,
        changed: Condvar,
    }

    impl RecordingReplies {
        fn wait_for_len(&self, len: usize, timeout: Duration) -> Vec<(ReplicaId, RemoteEnvelope)> {
            let deadline = Instant::now() + timeout;
            let mut received = self.received.lock().expect("replies poisoned");
            while received.len() < len {
                let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                    break;
                };
                let (next_received, wait) = self
                    .changed
                    .wait_timeout(received, remaining)
                    .expect("replies poisoned");
                received = next_received;
                if wait.timed_out() {
                    break;
                }
            }
            received.clone()
        }
    }

    impl ReplicatorRemoteReplyReceiver for RecordingReplies {
        fn receive_reply_from(
            &self,
            from: ReplicaId,
            envelope: RemoteEnvelope,
        ) -> Result<(), ReplicatorRemoteReplyError> {
            self.received
                .lock()
                .expect("replies poisoned")
                .push((from, envelope));
            self.changed.notify_all();
            Ok(())
        }
    }

    fn registry() -> Arc<Registry> {
        let mut registry = Registry::new();
        register_ddata_protocol_codecs(&mut registry).unwrap();
        Arc::new(registry)
    }

    fn replica(id: &str) -> ReplicaId {
        ReplicaId::new(id)
    }

    fn wait_for_route(runtime: &ReplicatorTcpAssociationRuntime) {
        await_assert(Duration::from_secs(1), Duration::from_millis(1), || {
            let actual = runtime.association_cache().route_count();
            (actual == 1)
                .then_some(())
                .ok_or_else(|| format!("expected 1 route, got {actual}"))
        })
        .unwrap();
    }

    fn outbound(
        target: ReplicaId,
        recipient: kairo_serialization::ActorRefWireData,
        sender: kairo_serialization::ActorRefWireData,
        registry: Arc<Registry>,
        cache: RemoteAssociationCache,
    ) -> ReplicatorRemoteEnvelopeOutbound {
        ReplicatorRemoteEnvelopeOutbound::new(
            ReplicatorRemoteTarget::new(target, recipient),
            Some(sender),
            registry,
            ReplicatorRemoteAssociationCacheOutbound::new(cache),
        )
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

    #[test]
    fn tcp_runtime_routes_replicator_requests_and_replies_over_bidirectional_association() {
        let _guard = ddata_socket_test_lock();
        let registry = registry();
        let receiver_requests = Arc::new(RecordingRequests::default());
        let receiver_replies = Arc::new(RecordingReplies::default());
        let sender_requests = Arc::new(RecordingRequests::default());
        let sender_replies = Arc::new(RecordingReplies::default());
        let receiver = ReplicatorTcpAssociationRuntime::bind(
            "receiver",
            replica("receiver"),
            replica("sender"),
            11,
            RemoteSettings::new("127.0.0.1", 0),
            receiver_requests.clone() as Arc<dyn ReplicatorRemoteRequestReceiver>,
            receiver_replies.clone() as Arc<dyn ReplicatorRemoteReplyReceiver>,
        )
        .unwrap();
        let sender = ReplicatorTcpAssociationRuntime::bind(
            "sender",
            replica("sender"),
            replica("receiver"),
            22,
            RemoteSettings::new("127.0.0.1", 0),
            sender_requests.clone() as Arc<dyn ReplicatorRemoteRequestReceiver>,
            sender_replies.clone() as Arc<dyn ReplicatorRemoteReplyReceiver>,
        )
        .unwrap();
        let sender_identity =
            tcp_association_identity_for("sender", sender.settings(), 22).unwrap();
        let registration = sender.dial(receiver.local_address().clone()).unwrap();
        wait_for_route(&receiver);
        assert!(
            receiver
                .association_registry()
                .association_by_uid(22)
                .is_some()
        );
        let sender_ref = replicator_actor_ref_for("sender", sender.settings()).unwrap();
        let receiver_ref = replicator_actor_ref_for("receiver", receiver.settings()).unwrap();

        let sender_outbound = outbound(
            replica("receiver"),
            receiver_ref.clone(),
            sender_ref.clone(),
            registry.clone(),
            sender.association_cache().clone(),
        );
        sender_outbound
            .tell(ReplicatorRead {
                key: "counter".to_string(),
                from: Some(replica("sender")),
            })
            .unwrap();

        let received = receiver_requests.wait_for_len(1, Duration::from_secs(1));
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].0, replica("sender"));
        assert_eq!(
            received[0].1.message.manifest.as_str(),
            ReplicatorRead::MANIFEST
        );
        assert_eq!(received[0].1.recipient, receiver_ref);
        assert_eq!(received[0].1.sender, Some(sender_ref.clone()));

        let receiver_outbound = outbound(
            replica("sender"),
            sender_ref.clone(),
            receiver_ref.clone(),
            registry.clone(),
            receiver.association_cache().clone(),
        );
        receiver_outbound
            .tell(ReplicatorReadResult { envelope: None })
            .unwrap();

        let replies = sender_replies.wait_for_len(1, Duration::from_secs(1));
        assert_eq!(replies.len(), 1);
        assert_eq!(replies[0].0, replica("receiver"));
        assert_eq!(
            replies[0].1.message.manifest.as_str(),
            ReplicatorReadResult::MANIFEST
        );
        assert_eq!(replies[0].1.recipient, sender_ref);
        assert_eq!(replies[0].1.sender, Some(receiver_ref.clone()));

        let receiver_request_outbound = outbound(
            replica("sender"),
            sender_ref.clone(),
            receiver_ref.clone(),
            registry,
            receiver.association_cache().clone(),
        );
        receiver_request_outbound
            .tell(ReplicatorRead {
                key: "reverse-counter".to_string(),
                from: Some(replica("receiver")),
            })
            .unwrap();
        let reverse_requests = sender_requests.wait_for_len(1, Duration::from_secs(1));
        assert_eq!(reverse_requests.len(), 1);
        assert_eq!(reverse_requests[0].0, replica("receiver"));
        assert_eq!(
            reverse_requests[0].1.message.manifest.as_str(),
            ReplicatorRead::MANIFEST
        );
        assert_eq!(reverse_requests[0].1.recipient, sender_ref);
        assert_eq!(reverse_requests[0].1.sender, Some(receiver_ref));

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
                if reason == REPLICATOR_TCP_SHUTDOWN_REASON
        ));
        let receiver_report = receiver.shutdown().unwrap();
        assert_eq!(receiver_report.accepted_associations, 1);
        assert_eq!(receiver_report.remote_identities, vec![sender_identity]);
    }

    #[test]
    fn tcp_runtime_shutdown_clears_late_routes_registered_during_shutdown() {
        let _guard = ddata_socket_test_lock();
        let requests = Arc::new(RecordingRequests::default());
        let replies = Arc::new(RecordingReplies::default());
        let runtime = ReplicatorTcpAssociationRuntime::bind(
            "late-route",
            replica("local"),
            replica("remote"),
            11,
            RemoteSettings::new("127.0.0.1", 0),
            requests as Arc<dyn ReplicatorRemoteRequestReceiver>,
            replies as Arc<dyn ReplicatorRemoteReplyReceiver>,
        )
        .unwrap();
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
    }
}
