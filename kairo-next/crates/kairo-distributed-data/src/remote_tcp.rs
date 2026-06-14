use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use kairo_remote::{
    AssociationOutboundPipeline, RemoteAssociationAddress, RemoteAssociationCache,
    RemoteAssociationRegistry, RemoteAssociationRouteInstaller, RemoteAssociationRouteRegistration,
    RemoteError, RemoteSettings, Result as RemoteResult, TcpAssociationDialer,
    TcpAssociationIdentity, TcpAssociationListener, TcpAssociationListenerHandle,
    TcpAssociationListenerReport, TcpAssociationReaderHandle, TcpAssociationStreamReader,
};

use crate::{
    ReplicaId, ReplicatorRemoteAssociationInbound, ReplicatorRemoteReplyReceiver,
    ReplicatorRemoteRequestReceiver,
};

const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);

pub struct ReplicatorTcpAssociationRuntime {
    local_replica: ReplicaId,
    remote_replica: ReplicaId,
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

impl ReplicatorTcpAssociationRuntime {
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
        let effective_settings = RemoteSettings::new(
            settings.canonical_hostname.clone(),
            if settings.canonical_port == 0 {
                local_addr.port()
            } else {
                settings.canonical_port
            },
        );
        let local_address = RemoteAssociationAddress::new(
            "kairo",
            local_system,
            effective_settings.canonical_hostname.clone(),
            Some(effective_settings.canonical_port),
        )?;
        let association_cache = RemoteAssociationCache::new();
        let association_registry = RemoteAssociationRegistry::new();
        let installer = RemoteAssociationRouteInstaller::new(association_cache.clone());
        let inbound = Arc::new(ReplicatorRemoteAssociationInbound::new(
            remote_replica.clone(),
            requests,
            replies,
        ));
        let outbound_reader = TcpAssociationStreamReader::new(inbound.clone());
        let listener = TcpAssociationListener::from_listener(listener, inbound)
            .with_local_address(local_address.clone())
            .with_association_registry(association_registry.clone())
            .with_route_installer(installer.clone())
            .spawn_accept_loop()?;
        let dialer = TcpAssociationDialer::new(installer)
            .with_local_identity(local_address.clone(), local_system_uid)
            .with_connect_timeout(Duration::from_secs(1));

        Ok(Self {
            local_replica,
            remote_replica,
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

    pub fn local_replica(&self) -> &ReplicaId {
        &self.local_replica
    }

    pub(crate) fn with_local_replica(mut self, local_replica: ReplicaId) -> Self {
        self.local_replica = local_replica;
        self
    }

    pub fn remote_replica(&self) -> &ReplicaId {
        &self.remote_replica
    }

    pub fn local_address(&self) -> &RemoteAssociationAddress {
        &self.local_address
    }

    pub fn settings(&self) -> &RemoteSettings {
        &self.settings
    }

    pub fn association_cache(&self) -> &RemoteAssociationCache {
        &self.association_cache
    }

    pub fn association_registry(&self) -> &RemoteAssociationRegistry {
        &self.association_registry
    }

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

    pub fn remove_route(&self, address: &RemoteAssociationAddress) -> bool {
        self.association_cache.remove_route(address).is_some()
    }

    pub fn shutdown(self) -> RemoteResult<TcpAssociationListenerReport> {
        self.shutdown_with_timeout(DEFAULT_SHUTDOWN_TIMEOUT)
    }

    pub fn shutdown_with_timeout(
        self,
        _timeout: Duration,
    ) -> RemoteResult<TcpAssociationListenerReport> {
        self.association_cache.clear_routes();
        self.listener.stop();
        let outbound_pipelines = self
            .outbound_pipelines
            .lock()
            .expect("replicator tcp outbound pipelines lock poisoned")
            .drain(..)
            .collect::<Vec<_>>();
        for pipeline in outbound_pipelines {
            let _ = pipeline.close("replicator tcp association runtime shutdown");
        }
        let outbound_readers = self
            .outbound_readers
            .lock()
            .expect("replicator tcp outbound readers lock poisoned")
            .drain(..)
            .collect::<Vec<_>>();
        for reader in outbound_readers {
            reader.join()?;
        }
        self.listener.join()
    }
}

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
    use std::time::Instant;

    use kairo_actor::Recipient;
    use kairo_serialization::{Registry, RemoteEnvelope, RemoteMessage};

    use super::*;
    use crate::{
        ReplicatorRead, ReplicatorReadResult, ReplicatorRemoteAssociationCacheOutbound,
        ReplicatorRemoteEnvelopeOutbound, ReplicatorRemoteReplyError, ReplicatorRemoteRequestError,
        ReplicatorRemoteTarget, register_ddata_protocol_codecs,
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
        let deadline = Instant::now() + Duration::from_secs(1);
        while runtime.association_cache().route_count() == 0 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(runtime.association_cache().route_count(), 1);
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

    #[test]
    fn tcp_runtime_routes_replicator_requests_and_replies_over_bidirectional_association() {
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
            sender_ref,
            receiver_ref,
            registry,
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

        let sender_report = sender.shutdown().unwrap();
        assert_eq!(sender_report.accepted_associations, 0);
        assert_eq!(registration.address(), receiver.local_address());
        let receiver_report = receiver.shutdown().unwrap();
        assert_eq!(receiver_report.accepted_associations, 1);
        assert_eq!(receiver_report.remote_identities, vec![sender_identity]);
    }
}
