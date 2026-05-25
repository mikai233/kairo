use std::time::Duration;

use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, Context};
use kairo_cluster::{
    Cluster, ClusterAssociationPeerTarget, ClusterSubscriptionEvent,
    ClusterSubscriptionInitialState, UniqueAddress,
};

use crate::{
    ReplicatorTcpPeerReconnectPending, ReplicatorTcpPeerRouteReport, ReplicatorTcpPeerRuntime,
    ReplicatorTcpPeerRuntimeResult,
};

const TCP_PEER_RETRY_TIMER_KEY: &str = "ddata-tcp-peer-retry";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicatorTcpPeerConnectorSettingsError {
    ZeroRetryInterval,
}

impl std::fmt::Display for ReplicatorTcpPeerConnectorSettingsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroRetryInterval => {
                write!(
                    f,
                    "distributed-data tcp peer connector retry interval must be non-zero"
                )
            }
        }
    }
}

impl std::error::Error for ReplicatorTcpPeerConnectorSettingsError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorTcpPeerConnectorSettings {
    retry_interval: Duration,
    initial_retry_delay: Duration,
    automatic_retry_ticks: bool,
}

impl ReplicatorTcpPeerConnectorSettings {
    pub fn new(retry_interval: Duration) -> Result<Self, ReplicatorTcpPeerConnectorSettingsError> {
        if retry_interval.is_zero() {
            return Err(ReplicatorTcpPeerConnectorSettingsError::ZeroRetryInterval);
        }
        Ok(Self {
            retry_interval,
            initial_retry_delay: retry_interval,
            automatic_retry_ticks: true,
        })
    }

    pub fn with_initial_retry_delay(mut self, delay: Duration) -> Self {
        self.initial_retry_delay = delay;
        self
    }

    pub fn with_automatic_retry_ticks(mut self, automatic: bool) -> Self {
        self.automatic_retry_ticks = automatic;
        self
    }

    pub fn retry_interval(&self) -> Duration {
        self.retry_interval
    }
}

impl Default for ReplicatorTcpPeerConnectorSettings {
    fn default() -> Self {
        Self {
            retry_interval: Duration::from_secs(1),
            initial_retry_delay: Duration::from_secs(1),
            automatic_retry_ticks: true,
        }
    }
}

pub struct ReplicatorTcpPeerConnector {
    cluster: Cluster,
    runtime: Option<ReplicatorTcpPeerRuntime>,
    settings: ReplicatorTcpPeerConnectorSettings,
    subscription: Option<ActorRef<ClusterSubscriptionEvent>>,
    last_report: Option<ReplicatorTcpPeerRouteReport>,
    last_error: Option<String>,
    retry_clock: Duration,
}

impl ReplicatorTcpPeerConnector {
    pub fn new(cluster: Cluster, runtime: ReplicatorTcpPeerRuntime) -> Self {
        Self::with_settings(
            cluster,
            runtime,
            ReplicatorTcpPeerConnectorSettings::default(),
        )
    }

    pub fn with_settings(
        cluster: Cluster,
        runtime: ReplicatorTcpPeerRuntime,
        settings: ReplicatorTcpPeerConnectorSettings,
    ) -> Self {
        Self {
            cluster,
            runtime: Some(runtime),
            settings,
            subscription: None,
            last_report: None,
            last_error: None,
            retry_clock: Duration::ZERO,
        }
    }

    fn snapshot(&self) -> ReplicatorTcpPeerConnectorSnapshot {
        let runtime = self.runtime.as_ref();
        ReplicatorTcpPeerConnectorSnapshot {
            self_node: runtime.map(|runtime| runtime.self_node().clone()),
            active_targets: runtime
                .map(ReplicatorTcpPeerRuntime::active_peer_targets)
                .unwrap_or_default(),
            route_count: runtime.map_or(0, ReplicatorTcpPeerRuntime::peer_route_count),
            pending_reconnects: runtime
                .map(ReplicatorTcpPeerRuntime::pending_peer_reconnects)
                .unwrap_or_default(),
            last_report: self.last_report.clone(),
            last_error: self.last_error.clone(),
        }
    }

    fn apply_cluster_event(&mut self, event: ClusterSubscriptionEvent) -> ActorResult {
        let Some(runtime) = self.runtime.as_mut() else {
            return Err(ActorError::Message(
                "distributed-data tcp peer connector runtime is stopped".to_string(),
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
                "distributed-data tcp peer connector runtime is stopped".to_string(),
            ));
        };
        let result = runtime.retry_due_peer_routes(now);
        self.record_route_result(result);
        Ok(())
    }

    fn clear_routes(&mut self) -> ActorResult {
        let Some(runtime) = self.runtime.as_mut() else {
            return Err(ActorError::Message(
                "distributed-data tcp peer connector runtime is stopped".to_string(),
            ));
        };
        self.last_report = Some(runtime.clear_peer_routes());
        self.last_error = None;
        Ok(())
    }

    fn record_route_result(
        &mut self,
        result: ReplicatorTcpPeerRuntimeResult<ReplicatorTcpPeerRouteReport>,
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

#[derive(Debug, Clone)]
pub enum ReplicatorTcpPeerConnectorMsg {
    Cluster(ClusterSubscriptionEvent),
    RetryDuePeerRoutes {
        now: Duration,
    },
    RetryTimerTick,
    ClearRoutes,
    Snapshot {
        reply_to: ActorRef<ReplicatorTcpPeerConnectorSnapshot>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorTcpPeerConnectorSnapshot {
    pub self_node: Option<UniqueAddress>,
    pub active_targets: Vec<ClusterAssociationPeerTarget>,
    pub route_count: usize,
    pub pending_reconnects: Vec<ReplicatorTcpPeerReconnectPending>,
    pub last_report: Option<ReplicatorTcpPeerRouteReport>,
    pub last_error: Option<String>,
}

impl Actor for ReplicatorTcpPeerConnector {
    type Msg = ReplicatorTcpPeerConnectorMsg;

    fn started(&mut self, ctx: &mut Context<Self::Msg>) -> ActorResult {
        let subscription = ctx.message_adapter(ReplicatorTcpPeerConnectorMsg::Cluster)?;
        self.cluster
            .subscribe_with_initial_state(
                subscription.clone(),
                ClusterSubscriptionInitialState::Snapshot,
            )
            .map_err(|error| ActorError::Message(error.to_string()))?;
        self.subscription = Some(subscription);
        if self.settings.automatic_retry_ticks {
            ctx.start_timer_with_fixed_delay(
                TCP_PEER_RETRY_TIMER_KEY,
                self.settings.initial_retry_delay,
                self.settings.retry_interval,
                ReplicatorTcpPeerConnectorMsg::RetryTimerTick,
            );
        }
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
            ReplicatorTcpPeerConnectorMsg::Cluster(event) => self.apply_cluster_event(event),
            ReplicatorTcpPeerConnectorMsg::RetryDuePeerRoutes { now } => self.retry_due(now),
            ReplicatorTcpPeerConnectorMsg::RetryTimerTick => {
                self.retry_clock = self
                    .retry_clock
                    .saturating_add(self.settings.retry_interval);
                self.retry_due(self.retry_clock)
            }
            ReplicatorTcpPeerConnectorMsg::ClearRoutes => self.clear_routes(),
            ReplicatorTcpPeerConnectorMsg::Snapshot { reply_to } => reply_to
                .tell(self.snapshot())
                .map_err(|error| ActorError::Message(error.reason().to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use kairo_actor::{Address, Props};
    use kairo_cluster::{
        ClusterEventPublisher, ClusterEventPublisherMsg, Gossip, Member, MemberStatus, Reachability,
    };
    use kairo_remote::RemoteSettings;
    use kairo_serialization::RemoteEnvelope;
    use kairo_testkit::{ActorSystemTestKit, TestProbe};

    use super::*;
    use crate::{
        ReplicaId, ReplicatorRemoteReplyError, ReplicatorRemoteReplyReceiver,
        ReplicatorRemoteRequestError, ReplicatorRemoteRequestReceiver,
        ReplicatorTcpAssociationRuntime, ReplicatorTcpPeerReconnectSettings,
        ReplicatorTcpPeerRuntimeSettings,
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

    fn bind_peer_runtime(
        name: &str,
        node_uid: u64,
        system_uid: u64,
        remote_replica: ReplicaId,
        retry_interval: Duration,
    ) -> ReplicatorTcpPeerRuntime {
        ReplicatorTcpPeerRuntime::bind_with_settings(
            name,
            node_uid,
            system_uid,
            remote_replica,
            ReplicatorTcpPeerRuntimeSettings::new(RemoteSettings::new("127.0.0.1", 0))
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
        connector: &ActorRef<ReplicatorTcpPeerConnectorMsg>,
        probe: &TestProbe<ReplicatorTcpPeerConnectorSnapshot>,
    ) -> ReplicatorTcpPeerConnectorSnapshot {
        connector
            .tell(ReplicatorTcpPeerConnectorMsg::Snapshot {
                reply_to: probe.actor_ref(),
            })
            .unwrap();
        probe.expect_msg(Duration::from_secs(1)).unwrap()
    }

    fn eventually_snapshot(
        connector: &ActorRef<ReplicatorTcpPeerConnectorMsg>,
        probe: &TestProbe<ReplicatorTcpPeerConnectorSnapshot>,
        predicate: impl Fn(&ReplicatorTcpPeerConnectorSnapshot) -> bool,
    ) -> ReplicatorTcpPeerConnectorSnapshot {
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
        let sender_kit = ActorSystemTestKit::new("ddata-tcp-peer-connector-sender").unwrap();
        let receiver_kit = ActorSystemTestKit::new("ddata-tcp-peer-connector-receiver").unwrap();
        let retry_interval = Duration::from_millis(25);
        let receiver_port = unused_port();
        let receiver_node = node("receiver", receiver_port, 2);
        let sender_runtime = bind_peer_runtime(
            "sender",
            1,
            11,
            ReplicaId::from(&receiver_node),
            retry_interval,
        );
        let sender_node = sender_runtime.self_node().clone();
        let publisher = spawn_publisher(&sender_kit, sender_node.clone());
        let cluster = Cluster::new(publisher.clone());
        let snapshots = sender_kit
            .create_probe::<ReplicatorTcpPeerConnectorSnapshot>("snapshots")
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
                Props::new(move || {
                    ReplicatorTcpPeerConnector::with_settings(
                        cluster,
                        sender_runtime,
                        ReplicatorTcpPeerConnectorSettings::new(retry_interval)
                            .unwrap()
                            .with_automatic_retry_ticks(false),
                    )
                }),
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
            ReplicaId::from(&receiver_node),
            ReplicaId::from(&sender_node),
            22,
            receiver_port,
        );
        connector
            .tell(ReplicatorTcpPeerConnectorMsg::RetryDuePeerRoutes {
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

    #[test]
    fn connector_automatic_retry_timer_drives_due_peer_routes() {
        assert_eq!(
            ReplicatorTcpPeerConnectorSettings::new(Duration::ZERO).unwrap_err(),
            ReplicatorTcpPeerConnectorSettingsError::ZeroRetryInterval
        );

        let (sender_kit, time) =
            ActorSystemTestKit::with_manual_time("ddata-tcp-peer-connector-timer").unwrap();
        let receiver_kit =
            ActorSystemTestKit::new("ddata-tcp-peer-connector-timer-receiver").unwrap();
        let retry_interval = Duration::from_millis(25);
        let receiver_port = unused_port();
        let receiver_node = node("receiver", receiver_port, 2);
        let sender_runtime = bind_peer_runtime(
            "sender",
            1,
            11,
            ReplicaId::from(&receiver_node),
            retry_interval,
        );
        let sender_node = sender_runtime.self_node().clone();
        let publisher = spawn_publisher(&sender_kit, sender_node.clone());
        let cluster = Cluster::new(publisher.clone());
        let snapshots = sender_kit
            .create_probe::<ReplicatorTcpPeerConnectorSnapshot>("timer-snapshots")
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
                Props::new(move || {
                    ReplicatorTcpPeerConnector::with_settings(
                        cluster,
                        sender_runtime,
                        ReplicatorTcpPeerConnectorSettings::new(retry_interval).unwrap(),
                    )
                }),
            )
            .unwrap();
        eventually_snapshot(&connector, &snapshots, |snapshot| {
            snapshot.pending_reconnects.len() == 1
        });

        let receiver_runtime = bind_association_runtime_on_port(
            "receiver",
            ReplicaId::from(&receiver_node),
            ReplicaId::from(&sender_node),
            22,
            receiver_port,
        );
        time.advance(retry_interval);

        let snapshot =
            eventually_snapshot(&connector, &snapshots, |snapshot| snapshot.route_count == 1);
        assert_eq!(snapshot.active_targets[0].node(), &receiver_node);
        assert!(snapshot.pending_reconnects.is_empty());
        assert!(snapshot.last_error.is_none());

        sender_kit.system().stop(&connector);
        assert!(connector.wait_for_stop(Duration::from_secs(1)));
        receiver_runtime.shutdown().unwrap();
        sender_kit.shutdown(Duration::from_secs(1)).unwrap();
        receiver_kit.shutdown(Duration::from_secs(1)).unwrap();
    }
}
