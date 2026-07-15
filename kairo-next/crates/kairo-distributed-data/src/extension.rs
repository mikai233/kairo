use std::fmt::{self, Display, Formatter};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use kairo_actor::{ActorError, ActorRef, ActorSystem, PHASE_CLUSTER_SHUTDOWN, Props};
use kairo_cluster::{ClusterDaemonRegistration, ClusterExtension, UniqueAddress};
use kairo_remote::{RemoteError, TcpRemoteActorRuntime, TcpRemoteActorRuntimeBuilder};
use kairo_serialization::{ActorRefWireData, RemoteMessage};

use crate::{
    AggregationTargetRegistry, AggregationTransport, DEFAULT_REPLICATOR_REMOTE_PATH,
    DeltaPropagationLoop, DeltaPropagationTargetRegistry, DeltaPropagationTransport,
    DeltaReplicatedData, RemovedNodePruning, ReplicaId, ReplicatorActor, ReplicatorActorMsg,
    ReplicatorAggregation, ReplicatorClusterConnector, ReplicatorClusterConnectorMsg,
    ReplicatorDeltaAck, ReplicatorDeltaNack, ReplicatorDeltaPropagation, ReplicatorGossip,
    ReplicatorGossipStatus, ReplicatorGossipTargetRegistry, ReplicatorGossipTransport,
    ReplicatorRead, ReplicatorReadResult, ReplicatorRemoteAssociationCacheOutbound,
    ReplicatorRemoteReplyInbound, ReplicatorRemoteRequestInbound, ReplicatorRemoteRouteTargets,
    ReplicatorRemoteSourceMap, ReplicatorRemoteSystemInbound, ReplicatorWireCodecs,
    ReplicatorWrite, ReplicatorWriteAck, ReplicatorWriteNack,
};

const DELTA_GOSSIP_INTERVAL_DIVISOR: u32 = 5;
const MIN_DERIVED_DELTA_PROPAGATION_INTERVAL: Duration = Duration::from_millis(200);

pub const DDATA_SYSTEM_MANIFESTS: [&str; 10] = [
    ReplicatorDeltaPropagation::MANIFEST,
    ReplicatorDeltaAck::MANIFEST,
    ReplicatorDeltaNack::MANIFEST,
    ReplicatorWrite::MANIFEST,
    ReplicatorWriteAck::MANIFEST,
    ReplicatorWriteNack::MANIFEST,
    ReplicatorRead::MANIFEST,
    ReplicatorReadResult::MANIFEST,
    ReplicatorGossipStatus::MANIFEST,
    ReplicatorGossip::MANIFEST,
];

pub struct DistributedDataSettings<D>
where
    D: DeltaReplicatedData + Send + 'static,
    D::Delta: Send + 'static,
{
    codecs: ReplicatorWireCodecs<D>,
    gossip_interval: Duration,
    delta_propagation_interval: Option<Duration>,
    shutdown_timeout: Duration,
}

impl<D> Clone for DistributedDataSettings<D>
where
    D: DeltaReplicatedData + Send + 'static,
    D::Delta: Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            codecs: self.codecs.clone(),
            gossip_interval: self.gossip_interval,
            delta_propagation_interval: self.delta_propagation_interval,
            shutdown_timeout: self.shutdown_timeout,
        }
    }
}

impl<D> DistributedDataSettings<D>
where
    D: DeltaReplicatedData + Send + 'static,
    D::Delta: Send + 'static,
{
    pub fn new(codecs: ReplicatorWireCodecs<D>) -> Self {
        Self {
            codecs,
            gossip_interval: Duration::from_secs(2),
            delta_propagation_interval: None,
            shutdown_timeout: Duration::from_secs(3),
        }
    }

    pub fn with_gossip_interval(mut self, value: Duration) -> Self {
        self.gossip_interval = value;
        self
    }

    pub fn with_delta_propagation_interval(mut self, value: Duration) -> Self {
        self.delta_propagation_interval = Some(value);
        self
    }

    pub fn with_shutdown_timeout(mut self, value: Duration) -> Self {
        self.shutdown_timeout = value;
        self
    }

    fn validate(&self) -> Result<(), DistributedDataBootstrapError> {
        if self.gossip_interval.is_zero() {
            return Err(DistributedDataBootstrapError::InvalidSettings {
                reason: "gossip interval must be greater than zero",
            });
        }
        if self
            .delta_propagation_interval
            .is_some_and(|value| value.is_zero())
        {
            return Err(DistributedDataBootstrapError::InvalidSettings {
                reason: "delta propagation interval must be greater than zero",
            });
        }
        if self.shutdown_timeout.is_zero() {
            return Err(DistributedDataBootstrapError::InvalidSettings {
                reason: "shutdown timeout must be greater than zero",
            });
        }
        Ok(())
    }

    fn effective_delta_propagation_interval(&self) -> Duration {
        self.delta_propagation_interval.unwrap_or_else(|| {
            (self.gossip_interval / DELTA_GOSSIP_INTERVAL_DIVISOR)
                .max(MIN_DERIVED_DELTA_PROPAGATION_INTERVAL)
        })
    }
}

pub struct DistributedDataHandle<D>
where
    D: DeltaReplicatedData + Send + 'static,
    D::Delta: Send + 'static,
{
    self_node: UniqueAddress,
    replicator: ActorRef<ReplicatorActorMsg<D>>,
    connector: ActorRef<ReplicatorClusterConnectorMsg>,
}

impl<D> Clone for DistributedDataHandle<D>
where
    D: DeltaReplicatedData + Send + 'static,
    D::Delta: Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            self_node: self.self_node.clone(),
            replicator: self.replicator.clone(),
            connector: self.connector.clone(),
        }
    }
}

impl<D> DistributedDataHandle<D>
where
    D: DeltaReplicatedData + Send + 'static,
    D::Delta: Send + 'static,
{
    pub fn self_node(&self) -> &UniqueAddress {
        &self.self_node
    }

    pub fn self_replica(&self) -> ReplicaId {
        ReplicaId::from(&self.self_node)
    }

    pub fn replicator(&self) -> &ActorRef<ReplicatorActorMsg<D>> {
        &self.replicator
    }

    pub fn connector(&self) -> &ActorRef<ReplicatorClusterConnectorMsg> {
        &self.connector
    }
}

pub struct DistributedDataExtension<D>
where
    D: DeltaReplicatedData + Send + 'static,
    D::Delta: Send + 'static,
{
    handle: DistributedDataHandle<D>,
}

impl<D> DistributedDataExtension<D>
where
    D: DeltaReplicatedData + Send + 'static,
    D::Delta: Send + 'static,
{
    fn new(handle: DistributedDataHandle<D>) -> Self {
        Self { handle }
    }

    pub fn get(system: &ActorSystem) -> Result<Arc<Self>, ActorError> {
        system.extension::<Self>()
    }

    pub fn self_replica(&self) -> ReplicaId {
        self.handle.self_replica()
    }

    pub fn replicator(&self) -> &ActorRef<ReplicatorActorMsg<D>> {
        self.handle.replicator()
    }

    pub fn connector(&self) -> &ActorRef<ReplicatorClusterConnectorMsg> {
        self.handle.connector()
    }
}

pub struct DistributedDataRegistration<D>
where
    D: DeltaReplicatedData + Send + 'static,
    D::Delta: Send + 'static,
{
    settings: DistributedDataSettings<D>,
    handle: Arc<Mutex<Option<DistributedDataHandle<D>>>>,
    activated: Arc<Mutex<bool>>,
}

impl<D> Clone for DistributedDataRegistration<D>
where
    D: DeltaReplicatedData + Send + 'static,
    D::Delta: Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            settings: self.settings.clone(),
            handle: Arc::clone(&self.handle),
            activated: Arc::clone(&self.activated),
        }
    }
}

impl<D> DistributedDataRegistration<D>
where
    D: DeltaReplicatedData + RemovedNodePruning + Send + 'static,
    D::Delta: Send + 'static,
{
    pub fn handle(&self) -> Option<DistributedDataHandle<D>> {
        self.handle
            .lock()
            .expect("distributed-data handle poisoned")
            .clone()
    }

    pub fn activate(
        &self,
        runtime: &TcpRemoteActorRuntime,
    ) -> Result<DistributedDataHandle<D>, DistributedDataBootstrapError> {
        let handle = self
            .handle()
            .ok_or(DistributedDataBootstrapError::NotMaterialized)?;
        ClusterExtension::get(runtime.system())?;
        let mut activated = self
            .activated
            .lock()
            .expect("distributed-data activation poisoned");
        if !*activated {
            let timeout = self.settings.shutdown_timeout;
            add_forced_actor_stop_task(
                runtime.system(),
                "ddata-cluster-connector-stop",
                &handle.connector,
                timeout,
            )?;
            add_forced_actor_stop_task(
                runtime.system(),
                "ddata-replicator-stop",
                &handle.replicator,
                timeout,
            )?;
            let extension_handle = handle.clone();
            runtime
                .system()
                .register_extension(move |_| DistributedDataExtension::new(extension_handle));
            *activated = true;
        }
        Ok(handle)
    }
}

fn add_forced_actor_stop_task<M>(
    system: &ActorSystem,
    task_name: &'static str,
    actor: &ActorRef<M>,
    timeout: Duration,
) -> Result<(), ActorError>
where
    M: Send + 'static,
{
    let actor = actor.clone();
    let stop_system = system.clone();
    system
        .coordinated_shutdown()
        .add_task(PHASE_CLUSTER_SHUTDOWN, task_name, move || {
            stop_system.stop(&actor);
            if actor.wait_for_stop(timeout) {
                Ok(())
            } else {
                Err(ActorError::ShutdownTaskFailed(
                    "actor termination task timed out".to_string(),
                ))
            }
        })
}

#[derive(Debug)]
pub enum DistributedDataBootstrapError {
    Actor(ActorError),
    InvalidSettings { reason: &'static str },
    NotMaterialized,
    Remote(RemoteError),
}

impl Display for DistributedDataBootstrapError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Actor(error) => write!(f, "{error}"),
            Self::InvalidSettings { reason } => {
                write!(f, "invalid distributed-data settings: {reason}")
            }
            Self::NotMaterialized => {
                write!(f, "distributed data has not been materialized by bind")
            }
            Self::Remote(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for DistributedDataBootstrapError {}

impl From<ActorError> for DistributedDataBootstrapError {
    fn from(error: ActorError) -> Self {
        Self::Actor(error)
    }
}

impl From<RemoteError> for DistributedDataBootstrapError {
    fn from(error: RemoteError) -> Self {
        Self::Remote(error)
    }
}

pub fn register_distributed_data<D>(
    builder: &mut TcpRemoteActorRuntimeBuilder,
    cluster: ClusterDaemonRegistration,
    settings: DistributedDataSettings<D>,
) -> Result<DistributedDataRegistration<D>, DistributedDataBootstrapError>
where
    D: DeltaReplicatedData + RemovedNodePruning + Send + 'static,
    D::Delta: Send + 'static,
{
    settings.validate()?;
    let handle = Arc::new(Mutex::new(None));
    let factory_handle = Arc::clone(&handle);
    let factory_settings = settings.clone();
    builder.register_ordinary_handler(&DDATA_SYSTEM_MANIFESTS, move |context| {
        let cluster = cluster.handle().ok_or_else(|| {
            RemoteError::Inbound(
                "cluster daemon must be registered before distributed data".to_string(),
            )
        })?;
        let self_node = cluster.self_node().clone();
        let registry = context.registry().clone();
        let outbound =
            ReplicatorRemoteAssociationCacheOutbound::new(context.association_cache().clone());
        let local_sender = ActorRefWireData::new(format!(
            "{}{}",
            self_node.address, DEFAULT_REPLICATOR_REMOTE_PATH
        ))
        .map_err(|error| RemoteError::Inbound(error.to_string()))?;
        let gossip_targets = ReplicatorGossipTargetRegistry::new();
        let gossip_transport =
            ReplicatorGossipTransport::with_target_registry(gossip_targets.clone());
        let actor_gossip_transport = gossip_transport.clone();
        let actor_data_codec = factory_settings.codecs.data_codec();
        let delta_targets = DeltaPropagationTargetRegistry::new();
        let delta_transport = DeltaPropagationTransport::with_target_registry(
            ReplicaId::from(&self_node),
            factory_settings.codecs.delta_codec(),
            delta_targets.clone(),
        );
        let actor_delta_loop = DeltaPropagationLoop::new(delta_transport);
        let delta_propagation_interval = factory_settings.effective_delta_propagation_interval();
        let aggregation_targets = AggregationTargetRegistry::new();
        let aggregation_transport = AggregationTransport::with_target_registry(
            ReplicaId::from(&self_node),
            factory_settings.codecs.data_codec(),
            aggregation_targets.clone(),
        );
        let actor_aggregation = ReplicatorAggregation::with_sender_remote_settings(
            aggregation_transport,
            factory_settings.codecs.data_codec(),
            context.settings().clone(),
        );
        let gossip_interval = factory_settings.gossip_interval;
        let local_system_uid = context.local_system_uid();
        let replicator = context
            .system()
            .spawn_system(
                "ddata",
                Props::new(move || {
                    ReplicatorActor::<D>::with_gossip_interval(
                        actor_gossip_transport.clone(),
                        actor_data_codec.clone(),
                        gossip_interval,
                    )
                    .enable_delta_propagation(actor_delta_loop.clone(), delta_propagation_interval)
                    .enable_aggregation(actor_aggregation.clone())
                    .with_self_system_uid(local_system_uid)
                }),
            )
            .map_err(|error| RemoteError::Inbound(error.to_string()))?;
        let source_replicas = ReplicatorRemoteSourceMap::default();
        let remote_targets = ReplicatorRemoteRouteTargets::new(registry.clone(), outbound.clone())
            .with_sender(Some(local_sender.clone()));
        let connector_replicator = replicator.clone();
        let connector_cluster = cluster.cluster().clone();
        let connector_self_node = self_node.clone();
        let connector_sources = Arc::clone(&source_replicas);
        let connector = match context.system().spawn_system(
            "ddata-cluster",
            Props::new(move || {
                ReplicatorClusterConnector::new(
                    connector_cluster.clone(),
                    connector_self_node.clone(),
                    connector_replicator.clone(),
                )
                .with_remote_route_targets(
                    remote_targets.clone(),
                    Some(delta_targets.clone()),
                    Some(aggregation_targets.clone()),
                    Some(gossip_targets.clone()),
                )
                .with_remote_source_replicas(Arc::clone(&connector_sources))
            }),
        ) {
            Ok(connector) => connector,
            Err(error) => {
                context.system().stop(&replicator);
                return Err(RemoteError::Inbound(error.to_string()));
            }
        };
        let requests = Arc::new(ReplicatorRemoteRequestInbound::new(
            context.system().clone(),
            local_sender.clone(),
            Some(local_sender),
            registry.clone(),
            replicator.clone(),
            factory_settings.codecs.clone(),
            outbound,
        ));
        let replies = Arc::new(ReplicatorRemoteReplyInbound::with_remote_settings(
            context.system().clone(),
            context.settings().clone(),
            registry,
        ));
        *factory_handle
            .lock()
            .expect("distributed-data handle poisoned") = Some(DistributedDataHandle {
            self_node,
            replicator,
            connector,
        });
        Ok(ReplicatorRemoteSystemInbound::new(
            source_replicas,
            requests,
            replies,
        ))
    })?;
    Ok(DistributedDataRegistration {
        settings,
        handle,
        activated: Arc::new(Mutex::new(false)),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use bytes::Bytes;
    use kairo_cluster::{
        ClusterDaemonBootstrapSettings, ClusterGossipProcessSettings, ClusterMembershipMsg,
        DeadlineFailureDetectorSettings, Gossip, HeartbeatSenderSettings, MemberStatus,
        ReachabilityStatus, register_cluster_daemon, register_cluster_protocol_codecs,
    };
    use kairo_remote::{
        RemoteSettings, TcpRemoteActorRuntime, TcpRemoteReconnectSettings,
        register_remote_protocol_codecs,
    };
    use kairo_serialization::Registry;
    use kairo_testkit::{ActorSystemTestKit, TestProbe, await_assert};

    use super::*;
    use crate::{
        GCounter, GCounterCodec, GetResponse, ReadConsistency, ReplicatorClusterConnectorSnapshot,
        ReplicatorKey, UpdateResponse, WriteConsistency, register_ddata_protocol_codecs,
    };

    static PROBE_ID: AtomicU64 = AtomicU64::new(0);

    struct ComposedDdataNode {
        kit: ActorSystemTestKit,
        runtime: TcpRemoteActorRuntime,
        cluster: kairo_cluster::ClusterDaemonHandle,
        ddata: DistributedDataHandle<GCounter>,
        gossip_probe: TestProbe<Gossip>,
        connector_probe: TestProbe<ReplicatorClusterConnectorSnapshot>,
    }

    impl ComposedDdataNode {
        fn start(
            system: &str,
            node_uid: u64,
            remote_uid: u64,
            seed_nodes: Vec<kairo_actor::Address>,
            registry: Arc<Registry>,
            gossip_interval: Duration,
            delta_propagation_interval: Option<Duration>,
        ) -> Self {
            let kit = ActorSystemTestKit::new(system).unwrap();
            let mut builder = TcpRemoteActorRuntime::builder(
                kit.system().clone(),
                registry,
                RemoteSettings::new("127.0.0.1", 0),
                remote_uid,
            )
            .with_reconnect_settings(
                TcpRemoteReconnectSettings::new(
                    Duration::from_millis(100),
                    Duration::from_millis(300),
                )
                .unwrap(),
            );
            let cluster_registration = register_cluster_daemon(
                &mut builder,
                ClusterDaemonBootstrapSettings::new(node_uid)
                    .with_seed_nodes(seed_nodes)
                    .with_config_digest(Some(Bytes::from_static(b"ddata-cluster")))
                    .with_gossip_process_settings(
                        ClusterGossipProcessSettings::new(Duration::from_millis(15)).unwrap(),
                    )
                    .with_heartbeat_sender_settings(
                        HeartbeatSenderSettings::new(
                            5,
                            DeadlineFailureDetectorSettings::new(
                                Duration::from_millis(100),
                                Duration::from_secs(2),
                            )
                            .unwrap(),
                        )
                        .with_heartbeat_expected_response_after(Duration::from_millis(500)),
                    ),
            )
            .unwrap();
            let mut ddata_settings = DistributedDataSettings::new(ReplicatorWireCodecs::new(
                Arc::new(GCounterCodec),
                Arc::new(GCounterCodec),
            ))
            .with_gossip_interval(gossip_interval);
            if let Some(interval) = delta_propagation_interval {
                ddata_settings = ddata_settings.with_delta_propagation_interval(interval);
            }
            let ddata_registration = register_distributed_data(
                &mut builder,
                cluster_registration.clone(),
                ddata_settings,
            )
            .unwrap();
            let runtime = builder.bind().unwrap();
            let cluster = cluster_registration.activate(&runtime).unwrap();
            let ddata = ddata_registration.activate(&runtime).unwrap();
            assert!(
                kit.system()
                    .has_extension::<DistributedDataExtension<GCounter>>()
            );
            Self {
                gossip_probe: kit.create_probe("cluster-gossip").unwrap(),
                connector_probe: kit.create_probe("ddata-connector").unwrap(),
                kit,
                runtime,
                cluster,
                ddata,
            }
        }

        fn gossip(&self) -> Gossip {
            self.cluster
                .membership()
                .tell(ClusterMembershipMsg::SendCurrentGossip {
                    reply_to: self.gossip_probe.actor_ref(),
                })
                .unwrap();
            self.gossip_probe
                .expect_msg(Duration::from_secs(1))
                .unwrap()
        }

        fn connector(&self) -> ReplicatorClusterConnectorSnapshot {
            self.ddata
                .connector()
                .tell(ReplicatorClusterConnectorMsg::Snapshot {
                    reply_to: self.connector_probe.actor_ref(),
                })
                .unwrap();
            self.connector_probe
                .expect_msg(Duration::from_secs(1))
                .unwrap()
        }

        fn update_counter(&self, key: ReplicatorKey, amount: u128) {
            let updates = self
                .kit
                .create_probe::<UpdateResponse<GCounter>>("updates")
                .unwrap();
            let self_replica = self.ddata.self_replica();
            self.ddata
                .replicator()
                .tell(ReplicatorActorMsg::Update {
                    key,
                    initial: GCounter::new(),
                    consistency: WriteConsistency::local(),
                    modify: Box::new(move |counter| {
                        counter
                            .increment(self_replica, amount)
                            .map_err(|error| error.to_string())
                    }),
                    reply_to: updates.actor_ref(),
                })
                .unwrap();
            assert!(matches!(
                updates.expect_msg(Duration::from_secs(1)).unwrap(),
                UpdateResponse::Success(_)
            ));
        }

        fn await_counter(&self, key: ReplicatorKey, expected: u128) {
            let id = PROBE_ID.fetch_add(1, Ordering::Relaxed);
            let reads = self
                .kit
                .create_probe::<GetResponse<GCounter>>(format!("reads-{id}"))
                .unwrap();
            await_assert(Duration::from_secs(3), Duration::from_millis(20), || {
                self.ddata
                    .replicator()
                    .tell(ReplicatorActorMsg::Get {
                        key: key.clone(),
                        consistency: ReadConsistency::local(),
                        reply_to: reads.actor_ref(),
                    })
                    .map_err(|error| error.reason().to_string())?;
                match reads
                    .expect_msg(Duration::from_millis(100))
                    .map_err(|error| error.to_string())?
                {
                    GetResponse::Success { data, .. } if data.value().unwrap() == expected => {
                        Ok(())
                    }
                    response => Err(format!("counter has not converged: {response:?}")),
                }
            })
            .unwrap();
        }

        fn shutdown(self) {
            self.kit.system().stop(self.cluster.root());
            self.runtime.shutdown().unwrap();
            self.kit.shutdown(Duration::from_secs(1)).unwrap();
        }

        fn crash(self) {
            self.runtime.shutdown().unwrap();
            self.kit.shutdown(Duration::from_secs(1)).unwrap();
        }
    }

    fn registry() -> Arc<Registry> {
        let mut registry = Registry::new();
        register_remote_protocol_codecs(&mut registry).unwrap();
        register_cluster_protocol_codecs(&mut registry).unwrap();
        register_ddata_protocol_codecs(&mut registry).unwrap();
        Arc::new(registry)
    }

    #[test]
    fn settings_reject_zero_runtime_intervals() {
        let settings = || {
            DistributedDataSettings::new(ReplicatorWireCodecs::new(
                Arc::new(GCounterCodec),
                Arc::new(GCounterCodec),
            ))
        };

        assert!(matches!(
            settings().with_gossip_interval(Duration::ZERO).validate(),
            Err(DistributedDataBootstrapError::InvalidSettings { .. })
        ));
        assert!(matches!(
            settings().with_shutdown_timeout(Duration::ZERO).validate(),
            Err(DistributedDataBootstrapError::InvalidSettings { .. })
        ));
        assert!(matches!(
            settings()
                .with_delta_propagation_interval(Duration::ZERO)
                .validate(),
            Err(DistributedDataBootstrapError::InvalidSettings { .. })
        ));
    }

    #[test]
    fn settings_derive_pekko_delta_interval_unless_overridden() {
        let settings = || {
            DistributedDataSettings::new(ReplicatorWireCodecs::new(
                Arc::new(GCounterCodec),
                Arc::new(GCounterCodec),
            ))
        };

        assert_eq!(
            settings().effective_delta_propagation_interval(),
            Duration::from_millis(400)
        );
        assert_eq!(
            settings()
                .with_gossip_interval(Duration::from_millis(500))
                .effective_delta_propagation_interval(),
            Duration::from_millis(200)
        );
        assert_eq!(
            settings()
                .with_delta_propagation_interval(Duration::from_millis(25))
                .effective_delta_propagation_interval(),
            Duration::from_millis(25)
        );
    }

    #[test]
    fn composed_extension_gossips_counter_from_real_cluster_membership() {
        let registry = registry();
        let seed = ComposedDdataNode::start(
            "ddata-extension-seed",
            1,
            101,
            vec![],
            registry.clone(),
            Duration::from_millis(20),
            None,
        );
        await_assert(Duration::from_secs(2), Duration::from_millis(5), || {
            (seed
                .gossip()
                .member(seed.cluster.self_node())
                .map(|member| member.status)
                == Some(MemberStatus::Up))
            .then_some(())
            .ok_or_else(|| "ddata seed has not formed".to_string())
        })
        .unwrap();
        let peer = ComposedDdataNode::start(
            "ddata-extension-peer",
            2,
            102,
            vec![seed.cluster.self_node().address.clone()],
            registry,
            Duration::from_millis(20),
            None,
        );
        await_assert(Duration::from_secs(3), Duration::from_millis(10), || {
            let seed_routes = seed.connector();
            let peer_routes = peer.connector();
            (seed_routes.remote_replicas == vec![ReplicaId::from(peer.cluster.self_node())]
                && peer_routes.remote_replicas == vec![ReplicaId::from(seed.cluster.self_node())])
            .then_some(())
            .ok_or_else(|| "ddata connectors have not derived cluster replicas".to_string())
        })
        .unwrap();

        let key = ReplicatorKey::new("visits");
        seed.update_counter(key.clone(), 3);
        peer.await_counter(key, 3);

        peer.shutdown();
        seed.shutdown();
    }

    #[test]
    fn composed_extension_propagates_delta_before_full_state_gossip() {
        let registry = registry();
        let seed = ComposedDdataNode::start(
            "ddata-delta-seed",
            1,
            201,
            vec![],
            registry.clone(),
            Duration::from_secs(30),
            Some(Duration::from_millis(20)),
        );
        await_assert(Duration::from_secs(2), Duration::from_millis(5), || {
            (seed
                .gossip()
                .member(seed.cluster.self_node())
                .map(|member| member.status)
                == Some(MemberStatus::Up))
            .then_some(())
            .ok_or_else(|| "ddata delta seed has not formed".to_string())
        })
        .unwrap();
        let peer = ComposedDdataNode::start(
            "ddata-delta-peer",
            2,
            202,
            vec![seed.cluster.self_node().address.clone()],
            registry,
            Duration::from_secs(30),
            Some(Duration::from_millis(20)),
        );
        await_assert(Duration::from_secs(3), Duration::from_millis(10), || {
            let seed_routes = seed.connector();
            let peer_routes = peer.connector();
            let seed_delta_targets = seed_routes
                .last_target_registration
                .as_ref()
                .map(|report| report.delta_registered());
            let peer_delta_targets = peer_routes
                .last_target_registration
                .as_ref()
                .map(|report| report.delta_registered());
            (seed_delta_targets == Some(&[ReplicaId::from(peer.cluster.self_node())][..])
                && peer_delta_targets == Some(&[ReplicaId::from(seed.cluster.self_node())][..]))
            .then_some(())
            .ok_or_else(|| "ddata delta targets have not followed membership".to_string())
        })
        .unwrap();

        let key = ReplicatorKey::new("delta-visits");
        seed.update_counter(key.clone(), 7);
        peer.await_counter(key, 7);

        peer.shutdown();
        seed.shutdown();
    }

    #[test]
    fn composed_extension_completes_remote_majority_write_and_read() {
        let registry = registry();
        let seed = ComposedDdataNode::start(
            "ddata-consistency-seed",
            1,
            301,
            vec![],
            registry.clone(),
            Duration::from_secs(30),
            Some(Duration::from_secs(30)),
        );
        await_assert(Duration::from_secs(2), Duration::from_millis(5), || {
            (seed
                .gossip()
                .member(seed.cluster.self_node())
                .map(|member| member.status)
                == Some(MemberStatus::Up))
            .then_some(())
            .ok_or_else(|| "ddata consistency seed has not formed".to_string())
        })
        .unwrap();
        let peer = ComposedDdataNode::start(
            "ddata-consistency-peer",
            2,
            302,
            vec![seed.cluster.self_node().address.clone()],
            registry,
            Duration::from_secs(30),
            Some(Duration::from_secs(30)),
        );
        await_assert(Duration::from_secs(3), Duration::from_millis(10), || {
            let seed_routes = seed.connector();
            let peer_routes = peer.connector();
            let seed_targets = seed_routes
                .last_target_registration
                .as_ref()
                .map(|report| report.aggregation_registered());
            let peer_targets = peer_routes
                .last_target_registration
                .as_ref()
                .map(|report| report.aggregation_registered());
            (seed_targets == Some(&[ReplicaId::from(peer.cluster.self_node())][..])
                && peer_targets == Some(&[ReplicaId::from(seed.cluster.self_node())][..]))
            .then_some(())
            .ok_or_else(|| "ddata aggregation targets have not followed membership".to_string())
        })
        .unwrap();

        let write_key = ReplicatorKey::new("majority-write");
        let writes = seed
            .kit
            .create_probe::<UpdateResponse<GCounter>>("majority-writes")
            .unwrap();
        let seed_replica = seed.ddata.self_replica();
        seed.ddata
            .replicator()
            .tell(ReplicatorActorMsg::Update {
                key: write_key.clone(),
                initial: GCounter::new(),
                consistency: WriteConsistency::majority(Duration::from_secs(2)),
                modify: Box::new(move |counter| {
                    counter
                        .increment(seed_replica, 11)
                        .map_err(|error| error.to_string())
                }),
                reply_to: writes.actor_ref(),
            })
            .unwrap();
        assert!(matches!(
            writes.expect_msg(Duration::from_secs(3)).unwrap(),
            UpdateResponse::Success(_)
        ));
        peer.await_counter(write_key, 11);

        let read_key = ReplicatorKey::new("majority-read");
        seed.update_counter(read_key.clone(), 13);
        let reads = peer
            .kit
            .create_probe::<GetResponse<GCounter>>("majority-reads")
            .unwrap();
        peer.ddata
            .replicator()
            .tell(ReplicatorActorMsg::Get {
                key: read_key.clone(),
                consistency: ReadConsistency::majority(Duration::from_secs(2)),
                reply_to: reads.actor_ref(),
            })
            .unwrap();
        let GetResponse::Success { key, data } = reads.expect_msg(Duration::from_secs(3)).unwrap()
        else {
            panic!("remote majority read did not succeed");
        };
        assert_eq!(key, read_key);
        assert_eq!(data.value().unwrap(), 13);

        peer.shutdown();
        seed.shutdown();
    }

    #[test]
    fn composed_extension_survivors_reform_quorum_after_replica_crash() {
        let registry = registry();
        let seed = ComposedDdataNode::start(
            "ddata-fault-seed",
            1,
            401,
            vec![],
            registry.clone(),
            Duration::from_millis(20),
            Some(Duration::from_millis(20)),
        );
        await_assert(Duration::from_secs(2), Duration::from_millis(5), || {
            (seed
                .gossip()
                .member(seed.cluster.self_node())
                .map(|member| member.status)
                == Some(MemberStatus::Up))
            .then_some(())
            .ok_or_else(|| "ddata fault seed has not formed".to_string())
        })
        .unwrap();
        let peer = ComposedDdataNode::start(
            "ddata-fault-peer",
            2,
            402,
            vec![seed.cluster.self_node().address.clone()],
            registry.clone(),
            Duration::from_millis(20),
            Some(Duration::from_millis(20)),
        );
        let crashed = ComposedDdataNode::start(
            "ddata-fault-crashed",
            3,
            403,
            vec![seed.cluster.self_node().address.clone()],
            registry,
            Duration::from_millis(20),
            Some(Duration::from_millis(20)),
        );
        let crashed_node = crashed.cluster.self_node().clone();
        let crashed_replica = ReplicaId::from(&crashed_node);
        let seed_replica = ReplicaId::from(seed.cluster.self_node());
        let peer_replica = ReplicaId::from(peer.cluster.self_node());

        await_assert(Duration::from_secs(5), Duration::from_millis(10), || {
            let seed_routes = seed.connector();
            let peer_routes = peer.connector();
            let crashed_routes = crashed.connector();
            (seed_routes.remote_replicas.len() == 2
                && peer_routes.remote_replicas.len() == 2
                && crashed_routes.remote_replicas.len() == 2)
                .then_some(())
                .ok_or_else(|| {
                    format!(
                        "ddata fault topology has not formed: seed={seed_routes:?}, peer={peer_routes:?}, crashed={crashed_routes:?}"
                    )
                })
        })
        .unwrap();

        let key = ReplicatorKey::new("crash-survivor-counter");
        crashed.update_counter(key.clone(), 5);
        seed.await_counter(key.clone(), 5);
        peer.await_counter(key.clone(), 5);

        crashed.crash();
        await_assert(Duration::from_secs(6), Duration::from_millis(25), || {
            let gossip = seed.gossip();
            (gossip
                .reachability()
                .status(seed.cluster.self_node(), &crashed_node)
                == ReachabilityStatus::Unreachable)
                .then_some(())
                .ok_or_else(|| "crashed ddata replica is not yet unreachable".to_string())
        })
        .unwrap();

        seed.cluster
            .cluster()
            .down(crashed_node.address.clone())
            .unwrap();
        await_assert(Duration::from_secs(5), Duration::from_millis(20), || {
            let seed_gossip = seed.gossip();
            let peer_gossip = peer.gossip();
            (seed_gossip.member(&crashed_node).is_none()
                && peer_gossip.member(&crashed_node).is_none()
                && seed_gossip.tombstones().contains_key(&crashed_node)
                && peer_gossip.tombstones().contains_key(&crashed_node))
            .then_some(())
            .ok_or_else(|| "crashed ddata replica has not been removed".to_string())
        })
        .unwrap();
        await_assert(Duration::from_secs(3), Duration::from_millis(20), || {
            let seed_routes = seed.connector();
            let peer_routes = peer.connector();
            (seed_routes.remote_replicas == vec![peer_replica.clone()]
                && peer_routes.remote_replicas == vec![seed_replica.clone()]
                && !seed_routes.remote_replicas.contains(&crashed_replica)
                && !peer_routes.remote_replicas.contains(&crashed_replica))
                .then_some(())
                .ok_or_else(|| {
                    format!(
                        "ddata routes still include crashed replica: seed={seed_routes:?}, peer={peer_routes:?}"
                    )
                })
        })
        .unwrap();

        let writes = seed
            .kit
            .create_probe::<UpdateResponse<GCounter>>("post-crash-majority-writes")
            .unwrap();
        let local_replica = seed.ddata.self_replica();
        seed.ddata
            .replicator()
            .tell(ReplicatorActorMsg::Update {
                key: key.clone(),
                initial: GCounter::new(),
                consistency: WriteConsistency::majority(Duration::from_secs(2)),
                modify: Box::new(move |counter| {
                    counter
                        .increment(local_replica, 3)
                        .map_err(|error| error.to_string())
                }),
                reply_to: writes.actor_ref(),
            })
            .unwrap();
        assert!(matches!(
            writes.expect_msg(Duration::from_secs(3)).unwrap(),
            UpdateResponse::Success(_)
        ));
        peer.await_counter(key, 8);

        let read_key = ReplicatorKey::new("post-crash-majority-read");
        seed.update_counter(read_key.clone(), 13);
        let reads = peer
            .kit
            .create_probe::<GetResponse<GCounter>>("post-crash-majority-reads")
            .unwrap();
        peer.ddata
            .replicator()
            .tell(ReplicatorActorMsg::Get {
                key: read_key.clone(),
                consistency: ReadConsistency::majority(Duration::from_secs(2)),
                reply_to: reads.actor_ref(),
            })
            .unwrap();
        let GetResponse::Success { key, data } = reads.expect_msg(Duration::from_secs(3)).unwrap()
        else {
            panic!("post-crash majority read did not succeed");
        };
        assert_eq!(key, read_key);
        assert_eq!(data.value().unwrap(), 13);

        peer.shutdown();
        seed.shutdown();
    }
}
