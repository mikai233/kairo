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
    ClusterSystemInbound, GossipEnvelope, Heartbeat, HeartbeatRsp, Join, UniqueAddress, Welcome,
};

const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);

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

    pub fn self_node(&self) -> &UniqueAddress {
        &self.self_node
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
            .expect("cluster tcp outbound pipelines lock poisoned")
            .push(registration.pipeline().clone());
        self.outbound_readers
            .lock()
            .expect("cluster tcp outbound readers lock poisoned")
            .push(reader_handle);
        Ok(registration)
    }

    pub fn remove_route(&self, address: &RemoteAssociationAddress) -> bool {
        self.remove_route_with_reason(address, "cluster tcp association route removed")
    }

    pub fn remove_route_with_reason(
        &self,
        address: &RemoteAssociationAddress,
        reason: &str,
    ) -> bool {
        self.association_cache
            .remove_route_and_close(address, reason)
            .is_some()
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
            .expect("cluster tcp outbound pipelines lock poisoned")
            .drain(..)
            .collect::<Vec<_>>();
        for pipeline in outbound_pipelines {
            let _ = pipeline.close("cluster tcp association runtime shutdown");
        }
        let outbound_readers = self
            .outbound_readers
            .lock()
            .expect("cluster tcp outbound readers lock poisoned")
            .drain(..)
            .collect::<Vec<_>>();
        for reader in outbound_readers {
            let _ = reader.join_after_stop_until(Instant::now());
        }
        self.listener.join()
    }
}

pub fn cluster_lane_classifier() -> RemoteLaneClassifier {
    let mut classifier = RemoteLaneClassifier::default();
    classifier.add_control_manifest(Join::MANIFEST);
    classifier.add_control_manifest(Welcome::MANIFEST);
    classifier.add_control_manifest(GossipEnvelope::MANIFEST);
    classifier.add_control_manifest(Heartbeat::MANIFEST);
    classifier.add_control_manifest(HeartbeatRsp::MANIFEST);
    classifier
}

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
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use kairo_remote::RemoteOutbound;
    use kairo_serialization::{ActorRefWireData, Registry, RemoteEnvelope};
    use kairo_testkit::ActorSystemTestKit;

    use super::*;
    use crate::{
        ClusterMembershipMsg, ClusterMembershipRemoteEnvelopeOutbound,
        ClusterMembershipWireInbound, ClusterMembershipWireOutbound,
        DEFAULT_CLUSTER_HEARTBEAT_RECEIVER_PATH, DEFAULT_CLUSTER_HEARTBEAT_SENDER_PATH, Gossip,
        HeartbeatRemoteReceiverInbound, HeartbeatRemoteReceiverOutbound,
        HeartbeatRemoteResponseInbound, HeartbeatSenderMsg, Member,
        register_cluster_protocol_codecs,
    };

    fn registry() -> Arc<Registry> {
        let mut registry = Registry::new();
        register_cluster_protocol_codecs(&mut registry).unwrap();
        Arc::new(registry)
    }

    fn wait_for_route(runtime: &ClusterTcpAssociationRuntime) {
        let deadline = Instant::now() + Duration::from_secs(1);
        while runtime.association_cache().route_count() == 0 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(runtime.association_cache().route_count(), 1);
    }

    fn wire_for(node: &UniqueAddress, path: &str) -> ActorRefWireData {
        ActorRefWireData::new(format!("{}{}", node.address, path)).unwrap()
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
    }
}
