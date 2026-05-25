use std::time::Duration;

use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, Context};

use crate::{
    Cluster, ClusterSubscriptionEvent, ClusterSubscriptionInitialState,
    ClusterTcpPeerReconnectPending, ClusterTcpPeerRouteReport, ClusterTcpPeerRuntime,
    UniqueAddress,
};

pub struct ClusterTcpPeerConnector {
    cluster: Cluster,
    runtime: Option<ClusterTcpPeerRuntime>,
    subscription: Option<ActorRef<ClusterSubscriptionEvent>>,
    last_report: Option<ClusterTcpPeerRouteReport>,
    last_error: Option<String>,
}

impl ClusterTcpPeerConnector {
    pub fn new(cluster: Cluster, runtime: ClusterTcpPeerRuntime) -> Self {
        Self {
            cluster,
            runtime: Some(runtime),
            subscription: None,
            last_report: None,
            last_error: None,
        }
    }

    fn snapshot(&self) -> ClusterTcpPeerConnectorSnapshot {
        let runtime = self.runtime.as_ref();
        ClusterTcpPeerConnectorSnapshot {
            self_node: runtime.map(|runtime| runtime.self_node().clone()),
            active_targets: runtime
                .map(ClusterTcpPeerRuntime::active_peer_targets)
                .unwrap_or_default(),
            route_count: runtime.map_or(0, ClusterTcpPeerRuntime::peer_route_count),
            pending_reconnects: runtime
                .map(ClusterTcpPeerRuntime::pending_peer_reconnects)
                .unwrap_or_default(),
            last_report: self.last_report.clone(),
            last_error: self.last_error.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum ClusterTcpPeerConnectorMsg {
    Cluster(ClusterSubscriptionEvent),
    RetryDuePeerRoutes {
        now: Duration,
    },
    ClearRoutes,
    Snapshot {
        reply_to: ActorRef<ClusterTcpPeerConnectorSnapshot>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterTcpPeerConnectorSnapshot {
    pub self_node: Option<UniqueAddress>,
    pub active_targets: Vec<crate::ClusterAssociationPeerTarget>,
    pub route_count: usize,
    pub pending_reconnects: Vec<ClusterTcpPeerReconnectPending>,
    pub last_report: Option<ClusterTcpPeerRouteReport>,
    pub last_error: Option<String>,
}

impl Actor for ClusterTcpPeerConnector {
    type Msg = ClusterTcpPeerConnectorMsg;

    fn started(&mut self, ctx: &mut Context<Self::Msg>) -> ActorResult {
        let subscription = ctx.message_adapter(ClusterTcpPeerConnectorMsg::Cluster)?;
        self.cluster
            .subscribe_with_initial_state(
                subscription.clone(),
                ClusterSubscriptionInitialState::Snapshot,
            )
            .map_err(|error| ActorError::Message(error.to_string()))?;
        self.subscription = Some(subscription);
        Ok(())
    }

    fn stopped(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        if let Some(subscription) = self.subscription.take() {
            let _ = self.cluster.unsubscribe(subscription);
        }
        if let Some(runtime) = self.runtime.take() {
            let _ = runtime.shutdown();
        }
        Ok(())
    }

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            ClusterTcpPeerConnectorMsg::Cluster(event) => self.apply_cluster_event(event),
            ClusterTcpPeerConnectorMsg::RetryDuePeerRoutes { now } => self.retry_due(now),
            ClusterTcpPeerConnectorMsg::ClearRoutes => self.clear_routes(),
            ClusterTcpPeerConnectorMsg::Snapshot { reply_to } => reply_to
                .tell(self.snapshot())
                .map_err(|error| ActorError::Message(error.reason().to_string())),
        }
    }
}

impl ClusterTcpPeerConnector {
    fn apply_cluster_event(&mut self, event: ClusterSubscriptionEvent) -> ActorResult {
        let Some(runtime) = self.runtime.as_mut() else {
            return Err(ActorError::Message(
                "cluster tcp peer connector runtime is stopped".to_string(),
            ));
        };
        let result = match event {
            ClusterSubscriptionEvent::CurrentState(state) => runtime.apply_snapshot(state),
            ClusterSubscriptionEvent::Event(event) => runtime.apply_event(event),
        };
        self.record_route_result(result);
        Ok(())
    }

    fn retry_due(&mut self, now: Duration) -> ActorResult {
        let Some(runtime) = self.runtime.as_mut() else {
            return Err(ActorError::Message(
                "cluster tcp peer connector runtime is stopped".to_string(),
            ));
        };
        let result = runtime.retry_due_peer_routes(now);
        self.record_route_result(result);
        Ok(())
    }

    fn clear_routes(&mut self) -> ActorResult {
        let Some(runtime) = self.runtime.as_mut() else {
            return Err(ActorError::Message(
                "cluster tcp peer connector runtime is stopped".to_string(),
            ));
        };
        self.last_report = Some(runtime.clear_peer_routes());
        self.last_error = None;
        Ok(())
    }

    fn record_route_result(
        &mut self,
        result: crate::ClusterTcpPeerRuntimeResult<ClusterTcpPeerRouteReport>,
    ) {
        match result {
            Ok(report) => {
                self.last_report = Some(report);
                self.last_error = None;
            }
            Err(error) => {
                self.last_error = Some(error.to_string());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use kairo_actor::{Address, Props};
    use kairo_remote::{RemoteAssociationCache, RemoteOutbound, RemoteSettings};
    use kairo_serialization::{ActorRefWireData, Registry};
    use kairo_testkit::{ActorSystemTestKit, TestProbe};

    use super::*;
    use crate::{
        ClusterEventPublisher, ClusterEventPublisherMsg, ClusterMembershipMsg,
        ClusterMembershipWireInbound, ClusterSystemInbound, ClusterTcpAssociationRuntime,
        ClusterTcpPeerReconnectSettings, DEFAULT_CLUSTER_HEARTBEAT_RECEIVER_PATH,
        DEFAULT_CLUSTER_HEARTBEAT_SENDER_PATH, Gossip, HeartbeatRemoteReceiverInbound,
        HeartbeatRemoteResponseInbound, HeartbeatSenderMsg, Member, MemberStatus, Reachability,
        register_cluster_protocol_codecs,
    };

    fn registry() -> Arc<Registry> {
        let mut registry = Registry::new();
        register_cluster_protocol_codecs(&mut registry).unwrap();
        Arc::new(registry)
    }

    fn member(node: UniqueAddress) -> Member {
        Member::new(node, vec![]).with_status(MemberStatus::Up)
    }

    fn node(system: &str, port: u16, uid: u64) -> UniqueAddress {
        UniqueAddress::new(
            Address::new("kairo", system, Some("127.0.0.1".to_string()), Some(port)),
            uid,
        )
    }

    fn unused_port() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    }

    fn wire_for(node: &UniqueAddress, path: &str) -> ActorRefWireData {
        ActorRefWireData::new(format!("{}{}", node.address, path)).unwrap()
    }

    fn bind_peer_runtime(
        name: &str,
        uid: u64,
        system_uid: u64,
        settings: RemoteSettings,
        retry_interval: Duration,
        kit: &ActorSystemTestKit,
        registry: Arc<Registry>,
    ) -> ClusterTcpPeerRuntime {
        ClusterTcpPeerRuntime::bind_with_reconnect(
            name,
            uid,
            system_uid,
            settings,
            ClusterTcpPeerReconnectSettings::new(retry_interval).unwrap(),
            move |self_node, cache| inbound_for(name, kit, registry, self_node, cache),
        )
        .unwrap()
    }

    fn bind_association_runtime_on_port(
        name: &str,
        uid: u64,
        system_uid: u64,
        port: u16,
        kit: &ActorSystemTestKit,
        registry: Arc<Registry>,
    ) -> ClusterTcpAssociationRuntime {
        ClusterTcpAssociationRuntime::bind(
            name,
            uid,
            system_uid,
            RemoteSettings::new("127.0.0.1", port),
            move |self_node, cache| inbound_for(name, kit, registry, self_node, cache),
        )
        .unwrap()
    }

    fn inbound_for(
        name: &str,
        kit: &ActorSystemTestKit,
        registry: Arc<Registry>,
        self_node: UniqueAddress,
        cache: RemoteAssociationCache,
    ) -> ClusterSystemInbound {
        let membership = kit
            .create_probe::<ClusterMembershipMsg>(format!("{name}-membership"))
            .unwrap();
        let heartbeat_sender = kit
            .create_probe::<HeartbeatSenderMsg>(format!("{name}-heartbeat-sender"))
            .unwrap();
        ClusterSystemInbound::new(self_node.clone())
            .with_membership(ClusterMembershipWireInbound::new(
                self_node.clone(),
                registry.clone(),
                membership.actor_ref(),
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
                heartbeat_sender.actor_ref(),
            ))
    }

    fn spawn_publisher(
        kit: &ActorSystemTestKit,
        self_node: UniqueAddress,
    ) -> ActorRef<ClusterEventPublisherMsg> {
        kit.system()
            .spawn(
                "publisher",
                Props::new(move || ClusterEventPublisher::new(self_node.clone())),
            )
            .unwrap()
    }

    fn expect_snapshot(
        connector: &ActorRef<ClusterTcpPeerConnectorMsg>,
        probe: &TestProbe<ClusterTcpPeerConnectorSnapshot>,
    ) -> ClusterTcpPeerConnectorSnapshot {
        connector
            .tell(ClusterTcpPeerConnectorMsg::Snapshot {
                reply_to: probe.actor_ref(),
            })
            .unwrap();
        probe.expect_msg(Duration::from_secs(1)).unwrap()
    }

    fn eventually_snapshot(
        connector: &ActorRef<ClusterTcpPeerConnectorMsg>,
        probe: &TestProbe<ClusterTcpPeerConnectorSnapshot>,
        predicate: impl Fn(&ClusterTcpPeerConnectorSnapshot) -> bool,
    ) -> ClusterTcpPeerConnectorSnapshot {
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            let snapshot = expect_snapshot(connector, probe);
            if predicate(&snapshot) {
                return snapshot;
            }
            assert!(Instant::now() < deadline, "timed out waiting for snapshot");
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    #[test]
    fn connector_subscribes_to_cluster_and_applies_tcp_peer_routes() {
        let sender_kit = ActorSystemTestKit::new("cluster-tcp-peer-connector-sender").unwrap();
        let receiver_kit = ActorSystemTestKit::new("cluster-tcp-peer-connector-receiver").unwrap();
        let registry = registry();
        let retry_interval = Duration::from_millis(25);
        let sender_runtime = bind_peer_runtime(
            "sender",
            1,
            11,
            RemoteSettings::new("127.0.0.1", 0),
            retry_interval,
            &sender_kit,
            registry.clone(),
        );
        let receiver_port = unused_port();
        let sender_node = sender_runtime.self_node().clone();
        let receiver_node = node("receiver", receiver_port, 2);
        let publisher = spawn_publisher(&sender_kit, sender_node.clone());
        let cluster = Cluster::new(publisher.clone());
        let snapshots = sender_kit
            .create_probe::<ClusterTcpPeerConnectorSnapshot>("snapshots")
            .unwrap();

        publisher
            .tell(ClusterEventPublisherMsg::PublishChanges(
                Gossip::from_members([member(sender_node.clone()), member(receiver_node.clone())]),
            ))
            .unwrap();
        let connector = sender_kit
            .system()
            .spawn(
                "tcp-peer-connector",
                Props::new(move || ClusterTcpPeerConnector::new(cluster, sender_runtime)),
            )
            .unwrap();

        let snapshot = eventually_snapshot(&connector, &snapshots, |snapshot| {
            snapshot.pending_reconnects.len() == 1
        });
        assert_eq!(snapshot.route_count, 0);
        assert!(snapshot.last_error.is_some());
        assert_eq!(snapshot.pending_reconnects[0].target.node(), &receiver_node);

        let receiver_runtime = bind_association_runtime_on_port(
            "receiver",
            2,
            22,
            receiver_port,
            &receiver_kit,
            registry,
        );
        connector
            .tell(ClusterTcpPeerConnectorMsg::RetryDuePeerRoutes {
                now: retry_interval,
            })
            .unwrap();
        let snapshot =
            eventually_snapshot(&connector, &snapshots, |snapshot| snapshot.route_count == 1);
        assert_eq!(snapshot.active_targets[0].node(), &receiver_node);
        assert!(snapshot.pending_reconnects.is_empty());

        publisher
            .tell(ClusterEventPublisherMsg::PublishChanges(
                Gossip::from_members([member(sender_node.clone()), member(receiver_node.clone())])
                    .with_reachability(
                        Reachability::new().unreachable(sender_node.clone(), receiver_node.clone()),
                    ),
            ))
            .unwrap();
        let snapshot =
            eventually_snapshot(&connector, &snapshots, |snapshot| snapshot.route_count == 0);
        assert!(snapshot.active_targets.is_empty());
        assert!(snapshot.last_error.is_none());

        sender_kit.system().stop(&connector);
        assert!(connector.wait_for_stop(Duration::from_secs(1)));
        receiver_runtime.shutdown().unwrap();
        sender_kit.shutdown(Duration::from_secs(1)).unwrap();
        receiver_kit.shutdown(Duration::from_secs(1)).unwrap();
    }
}
