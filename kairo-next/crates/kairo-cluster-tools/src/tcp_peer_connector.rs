use std::time::Duration;

use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, Context};
use kairo_cluster::{
    Cluster, ClusterAssociationPeerTarget, ClusterSubscriptionEvent,
    ClusterSubscriptionInitialState, UniqueAddress,
};
use kairo_serialization::RemoteMessage;

use crate::{
    ClusterToolsTcpPeerReconnectPending, ClusterToolsTcpPeerRouteReport,
    ClusterToolsTcpPeerRuntime, ClusterToolsTcpPeerRuntimeResult,
};

const TCP_PEER_RETRY_TIMER_KEY: &str = "cluster-tools-tcp-peer-retry";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterToolsTcpPeerConnectorSettingsError {
    ZeroRetryInterval,
}

impl std::fmt::Display for ClusterToolsTcpPeerConnectorSettingsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroRetryInterval => {
                write!(
                    f,
                    "cluster-tools tcp peer connector retry interval must be non-zero"
                )
            }
        }
    }
}

impl std::error::Error for ClusterToolsTcpPeerConnectorSettingsError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterToolsTcpPeerConnectorSettings {
    retry_interval: Duration,
    initial_retry_delay: Duration,
    automatic_retry_ticks: bool,
}

impl ClusterToolsTcpPeerConnectorSettings {
    pub fn new(
        retry_interval: Duration,
    ) -> Result<Self, ClusterToolsTcpPeerConnectorSettingsError> {
        if retry_interval.is_zero() {
            return Err(ClusterToolsTcpPeerConnectorSettingsError::ZeroRetryInterval);
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

impl Default for ClusterToolsTcpPeerConnectorSettings {
    fn default() -> Self {
        Self {
            retry_interval: Duration::from_secs(1),
            initial_retry_delay: Duration::from_secs(1),
            automatic_retry_ticks: true,
        }
    }
}

pub struct ClusterToolsTcpPeerConnector<M>
where
    M: RemoteMessage + Send + 'static,
{
    cluster: Cluster,
    runtime: Option<ClusterToolsTcpPeerRuntime<M>>,
    settings: ClusterToolsTcpPeerConnectorSettings,
    subscription: Option<ActorRef<ClusterSubscriptionEvent>>,
    last_report: Option<ClusterToolsTcpPeerRouteReport>,
    last_error: Option<String>,
    retry_clock: Duration,
}

impl<M> ClusterToolsTcpPeerConnector<M>
where
    M: RemoteMessage + Send + 'static,
{
    pub fn new(cluster: Cluster, runtime: ClusterToolsTcpPeerRuntime<M>) -> Self {
        Self::with_settings(
            cluster,
            runtime,
            ClusterToolsTcpPeerConnectorSettings::default(),
        )
    }

    pub fn with_settings(
        cluster: Cluster,
        runtime: ClusterToolsTcpPeerRuntime<M>,
        settings: ClusterToolsTcpPeerConnectorSettings,
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

    fn snapshot(&self) -> ClusterToolsTcpPeerConnectorSnapshot {
        let runtime = self.runtime.as_ref();
        ClusterToolsTcpPeerConnectorSnapshot {
            self_node: runtime.map(|runtime| runtime.self_node().clone()),
            active_targets: runtime
                .map(ClusterToolsTcpPeerRuntime::active_peer_targets)
                .unwrap_or_default(),
            route_count: runtime.map_or(0, ClusterToolsTcpPeerRuntime::peer_route_count),
            pending_reconnects: runtime
                .map(ClusterToolsTcpPeerRuntime::pending_peer_reconnects)
                .unwrap_or_default(),
            last_report: self.last_report.clone(),
            last_error: self.last_error.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum ClusterToolsTcpPeerConnectorMsg {
    Cluster(ClusterSubscriptionEvent),
    RetryDuePeerRoutes {
        now: Duration,
    },
    RetryTimerTick,
    ClearRoutes,
    Snapshot {
        reply_to: ActorRef<ClusterToolsTcpPeerConnectorSnapshot>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterToolsTcpPeerConnectorSnapshot {
    pub self_node: Option<UniqueAddress>,
    pub active_targets: Vec<ClusterAssociationPeerTarget>,
    pub route_count: usize,
    pub pending_reconnects: Vec<ClusterToolsTcpPeerReconnectPending>,
    pub last_report: Option<ClusterToolsTcpPeerRouteReport>,
    pub last_error: Option<String>,
}

impl<M> Actor for ClusterToolsTcpPeerConnector<M>
where
    M: RemoteMessage + Send + 'static,
{
    type Msg = ClusterToolsTcpPeerConnectorMsg;

    fn started(&mut self, ctx: &mut Context<Self::Msg>) -> ActorResult {
        let subscription = ctx.message_adapter(ClusterToolsTcpPeerConnectorMsg::Cluster)?;
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
                ClusterToolsTcpPeerConnectorMsg::RetryTimerTick,
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
            ClusterToolsTcpPeerConnectorMsg::Cluster(event) => self.apply_cluster_event(event),
            ClusterToolsTcpPeerConnectorMsg::RetryDuePeerRoutes { now } => self.retry_due(now),
            ClusterToolsTcpPeerConnectorMsg::RetryTimerTick => {
                self.retry_clock = self
                    .retry_clock
                    .saturating_add(self.settings.retry_interval);
                self.retry_due(self.retry_clock)
            }
            ClusterToolsTcpPeerConnectorMsg::ClearRoutes => self.clear_routes(),
            ClusterToolsTcpPeerConnectorMsg::Snapshot { reply_to } => reply_to
                .tell(self.snapshot())
                .map_err(|error| ActorError::Message(error.reason().to_string())),
        }
    }
}

impl<M> ClusterToolsTcpPeerConnector<M>
where
    M: RemoteMessage + Send + 'static,
{
    fn apply_cluster_event(&mut self, event: ClusterSubscriptionEvent) -> ActorResult {
        let Some(runtime) = self.runtime.as_mut() else {
            return Err(ActorError::Message(
                "cluster-tools tcp peer connector runtime is stopped".to_string(),
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
                "cluster-tools tcp peer connector runtime is stopped".to_string(),
            ));
        };
        let result = runtime.retry_due_peer_routes(now);
        self.record_route_result(result);
        Ok(())
    }

    fn clear_routes(&mut self) -> ActorResult {
        let Some(runtime) = self.runtime.as_mut() else {
            return Err(ActorError::Message(
                "cluster-tools tcp peer connector runtime is stopped".to_string(),
            ));
        };
        self.last_report = Some(runtime.clear_peer_routes());
        self.last_error = None;
        Ok(())
    }

    fn record_route_result(
        &mut self,
        result: ClusterToolsTcpPeerRuntimeResult<ClusterToolsTcpPeerRouteReport>,
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

    use bytes::Bytes;
    use kairo_actor::{Address, Props};
    use kairo_cluster::{
        ClusterEventPublisher, ClusterEventPublisherMsg, Gossip, Member, MemberStatus,
        Reachability, UniqueAddress,
    };
    use kairo_remote::RemoteSettings;
    use kairo_serialization::{MessageCodec, Registry, SerializationRegistry};
    use kairo_testkit::{ActorSystemTestKit, TestProbe};

    use super::*;
    use crate::{
        ClusterToolsSystemInbound, ClusterToolsTcpAssociationRuntime,
        ClusterToolsTcpPeerReconnectSettings, DistributedPubSubMediatorMsg, PubSubGossipMsg,
        PubSubGossipWireInbound, PubSubRemoteDeliveryInbound, SingletonManagerMsg,
        SingletonManagerRemoteInbound, register_cluster_tools_protocol_codecs,
    };

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestMessage {
        value: u8,
    }

    impl RemoteMessage for TestMessage {
        const MANIFEST: &'static str = "kairo.cluster-tools.test.peer-connector-message";
        const VERSION: u16 = 1;
    }

    #[derive(Debug, Clone, Copy)]
    struct TestMessageCodec;

    impl MessageCodec<TestMessage> for TestMessageCodec {
        fn serializer_id(&self) -> u32 {
            59_204
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
        uid: u64,
        system_uid: u64,
        settings: RemoteSettings,
        retry_interval: Duration,
        kit: &ActorSystemTestKit,
        registry: Arc<Registry>,
    ) -> ClusterToolsTcpPeerRuntime<TestMessage> {
        ClusterToolsTcpPeerRuntime::bind_with_reconnect(
            name,
            uid,
            system_uid,
            settings,
            ClusterToolsTcpPeerReconnectSettings::new(retry_interval).unwrap(),
            move |self_node| inbound_for(name, kit, registry, self_node),
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
    ) -> ClusterToolsTcpAssociationRuntime<TestMessage> {
        ClusterToolsTcpAssociationRuntime::bind(
            name,
            uid,
            system_uid,
            RemoteSettings::new("127.0.0.1", port),
            move |self_node| inbound_for(name, kit, registry, self_node),
        )
        .unwrap()
    }

    fn inbound_for(
        name: &str,
        kit: &ActorSystemTestKit,
        registry: Arc<Registry>,
        self_node: UniqueAddress,
    ) -> ClusterToolsSystemInbound<TestMessage> {
        let gossip = kit
            .create_probe::<PubSubGossipMsg>(format!("{name}-gossip"))
            .unwrap();
        let mediator = kit
            .create_probe::<DistributedPubSubMediatorMsg<TestMessage>>(format!("{name}-mediator"))
            .unwrap();
        let manager = kit
            .create_probe::<SingletonManagerMsg>(format!("{name}-singleton-manager"))
            .unwrap();
        ClusterToolsSystemInbound::new(self_node.clone())
            .with_pubsub_gossip(PubSubGossipWireInbound::new(
                self_node.clone(),
                registry.clone(),
                gossip.actor_ref(),
            ))
            .with_pubsub_delivery(PubSubRemoteDeliveryInbound::new(
                self_node.clone(),
                registry.clone(),
                mediator.actor_ref(),
            ))
            .with_singleton_manager(SingletonManagerRemoteInbound::new(
                self_node,
                registry,
                manager.actor_ref(),
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
        connector: &ActorRef<ClusterToolsTcpPeerConnectorMsg>,
        probe: &TestProbe<ClusterToolsTcpPeerConnectorSnapshot>,
    ) -> ClusterToolsTcpPeerConnectorSnapshot {
        connector
            .tell(ClusterToolsTcpPeerConnectorMsg::Snapshot {
                reply_to: probe.actor_ref(),
            })
            .unwrap();
        probe.expect_msg(Duration::from_secs(1)).unwrap()
    }

    fn eventually_snapshot(
        connector: &ActorRef<ClusterToolsTcpPeerConnectorMsg>,
        probe: &TestProbe<ClusterToolsTcpPeerConnectorSnapshot>,
        predicate: impl Fn(&ClusterToolsTcpPeerConnectorSnapshot) -> bool,
    ) -> ClusterToolsTcpPeerConnectorSnapshot {
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
        let sender_kit =
            ActorSystemTestKit::new("cluster-tools-tcp-peer-connector-sender").unwrap();
        let receiver_kit =
            ActorSystemTestKit::new("cluster-tools-tcp-peer-connector-receiver").unwrap();
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
            .create_probe::<ClusterToolsTcpPeerConnectorSnapshot>("snapshots")
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
                    ClusterToolsTcpPeerConnector::with_settings(
                        cluster,
                        sender_runtime,
                        ClusterToolsTcpPeerConnectorSettings::new(retry_interval)
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
            2,
            22,
            receiver_port,
            &receiver_kit,
            registry,
        );
        connector
            .tell(ClusterToolsTcpPeerConnectorMsg::RetryDuePeerRoutes {
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
            ClusterToolsTcpPeerConnectorSettings::new(Duration::ZERO).unwrap_err(),
            ClusterToolsTcpPeerConnectorSettingsError::ZeroRetryInterval
        );

        let (sender_kit, time) =
            ActorSystemTestKit::with_manual_time("cluster-tools-tcp-peer-connector-timer").unwrap();
        let receiver_kit =
            ActorSystemTestKit::new("cluster-tools-tcp-peer-connector-timer-receiver").unwrap();
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
            .create_probe::<ClusterToolsTcpPeerConnectorSnapshot>("timer-snapshots")
            .unwrap();

        publisher
            .tell(ClusterEventPublisherMsg::PublishChanges(
                Gossip::from_members([member(sender_node), member(receiver_node.clone())]),
            ))
            .unwrap();
        let connector = sender_kit
            .system()
            .spawn(
                "tcp-peer-connector",
                Props::new(move || {
                    ClusterToolsTcpPeerConnector::with_settings(
                        cluster,
                        sender_runtime,
                        ClusterToolsTcpPeerConnectorSettings::new(retry_interval).unwrap(),
                    )
                }),
            )
            .unwrap();
        eventually_snapshot(&connector, &snapshots, |snapshot| {
            snapshot.pending_reconnects.len() == 1
        });

        let receiver_runtime = bind_association_runtime_on_port(
            "receiver",
            2,
            22,
            receiver_port,
            &receiver_kit,
            registry,
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
