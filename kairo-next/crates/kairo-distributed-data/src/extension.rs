use std::any::{TypeId, type_name};
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::fmt::{self, Display, Formatter};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use kairo_actor::{ActorError, ActorRef, ActorSystem, PHASE_CLUSTER_SHUTDOWN, Props};
use kairo_cluster::{
    ClusterDaemonHandle, ClusterDaemonRegistration, ClusterExtension, UniqueAddress,
};
use kairo_remote::{
    RemoteError, TcpRemoteActorRuntime, TcpRemoteActorRuntimeBuilder, TcpRemoteActorRuntimeContext,
};
use kairo_serialization::{ActorRefWireData, RemoteMessage};

use crate::{
    AggregationTargetRegistry, AggregationTransport, DeltaPropagationLoop,
    DeltaPropagationTargetRegistry, DeltaPropagationTransport, DeltaReplicatedData,
    RemovedNodePruning, ReplicaId, ReplicatorActor, ReplicatorActorMsg, ReplicatorAggregation,
    ReplicatorClusterConnector, ReplicatorClusterConnectorMsg,
    ReplicatorClusterConnectorTimingSettings, ReplicatorClusterPruningSettings, ReplicatorDeltaAck,
    ReplicatorDeltaNack, ReplicatorDeltaPropagation, ReplicatorGossip, ReplicatorGossipStatus,
    ReplicatorGossipTargetRegistry, ReplicatorGossipTransport, ReplicatorRead,
    ReplicatorReadResult, ReplicatorRemoteAssociationCacheOutbound, ReplicatorRemoteReplyInbound,
    ReplicatorRemoteRequestInbound, ReplicatorRemoteRequestReceiver,
    ReplicatorRemoteRequestRegistry, ReplicatorRemoteRouteTargets, ReplicatorRemoteSourceMap,
    ReplicatorRemoteSystemInbound, ReplicatorRemoteTargetError, ReplicatorWireCodecs,
    ReplicatorWrite, ReplicatorWriteAck, ReplicatorWriteNack, replicator_remote_path_for_manifest,
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
    pruning_interval: Option<Duration>,
    max_pruning_dissemination: Duration,
    pruning_marker_ttl: Duration,
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
            pruning_interval: self.pruning_interval,
            max_pruning_dissemination: self.max_pruning_dissemination,
            pruning_marker_ttl: self.pruning_marker_ttl,
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
            pruning_interval: Some(Duration::from_secs(120)),
            max_pruning_dissemination: Duration::from_secs(300),
            pruning_marker_ttl: Duration::from_secs(6 * 60 * 60),
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

    pub fn with_pruning_interval(mut self, value: Option<Duration>) -> Self {
        self.pruning_interval = value;
        self
    }

    pub fn with_max_pruning_dissemination(mut self, value: Duration) -> Self {
        self.max_pruning_dissemination = value;
        self
    }

    pub fn with_pruning_marker_ttl(mut self, value: Duration) -> Self {
        self.pruning_marker_ttl = value;
        self
    }

    pub fn with_shutdown_timeout(mut self, value: Duration) -> Self {
        self.shutdown_timeout = value;
        self
    }

    pub fn pruning_interval(&self) -> Option<Duration> {
        self.pruning_interval
    }

    pub fn max_pruning_dissemination(&self) -> Duration {
        self.max_pruning_dissemination
    }

    pub fn pruning_marker_ttl(&self) -> Duration {
        self.pruning_marker_ttl
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
        if self.pruning_interval.is_some_and(|value| value.is_zero()) {
            return Err(DistributedDataBootstrapError::InvalidSettings {
                reason: "pruning interval must be greater than zero when enabled",
            });
        }
        if self.max_pruning_dissemination.is_zero() {
            return Err(DistributedDataBootstrapError::InvalidSettings {
                reason: "max pruning dissemination must be greater than zero",
            });
        }
        if self.pruning_marker_ttl.is_zero() {
            return Err(DistributedDataBootstrapError::InvalidSettings {
                reason: "pruning marker ttl must be greater than zero",
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
    actor_name: String,
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
            actor_name: self.actor_name.clone(),
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
                format!("{}-cluster-connector-stop", self.actor_name),
                &handle.connector,
                timeout,
            )?;
            add_forced_actor_stop_task(
                runtime.system(),
                format!("{}-replicator-stop", self.actor_name),
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
    task_name: impl Into<String>,
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
    DuplicateDataManifest {
        manifest: String,
    },
    DuplicateDataType {
        type_name: &'static str,
    },
    FamilyPathCollision {
        path: String,
        registered_manifest: String,
        requested_manifest: String,
    },
    InvalidSettings {
        reason: &'static str,
    },
    NotMaterialized,
    Remote(RemoteError),
    Target(ReplicatorRemoteTargetError),
}

impl Display for DistributedDataBootstrapError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Actor(error) => write!(f, "{error}"),
            Self::DuplicateDataManifest { manifest } => write!(
                f,
                "distributed-data CRDT manifest `{manifest}` is already registered"
            ),
            Self::DuplicateDataType { type_name } => write!(
                f,
                "distributed-data Rust type `{type_name}` is already registered"
            ),
            Self::FamilyPathCollision {
                path,
                registered_manifest,
                requested_manifest,
            } => write!(
                f,
                "distributed-data family path `{path}` for `{requested_manifest}` is already owned by `{registered_manifest}`"
            ),
            Self::InvalidSettings { reason } => {
                write!(f, "invalid distributed-data settings: {reason}")
            }
            Self::NotMaterialized => {
                write!(f, "distributed data has not been materialized by bind")
            }
            Self::Remote(error) => write!(f, "{error}"),
            Self::Target(error) => write!(f, "{error}"),
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

impl From<ReplicatorRemoteTargetError> for DistributedDataBootstrapError {
    fn from(error: ReplicatorRemoteTargetError) -> Self {
        Self::Target(error)
    }
}

trait DistributedDataFamilyFactory: Send {
    fn materialize(
        self: Box<Self>,
        context: &TcpRemoteActorRuntimeContext,
        cluster: &ClusterDaemonHandle,
        source_replicas: ReplicatorRemoteSourceMap,
        requests: &ReplicatorRemoteRequestRegistry,
    ) -> Result<(), RemoteError>;
}

struct TypedDistributedDataFamilyFactory<D>
where
    D: DeltaReplicatedData + Send + 'static,
    D::Delta: Send + 'static,
{
    manifest: String,
    remote_path: String,
    actor_name: String,
    settings: DistributedDataSettings<D>,
    handle: Arc<Mutex<Option<DistributedDataHandle<D>>>>,
}

impl<D> DistributedDataFamilyFactory for TypedDistributedDataFamilyFactory<D>
where
    D: DeltaReplicatedData + RemovedNodePruning + Send + 'static,
    D::Delta: Send + 'static,
{
    fn materialize(
        self: Box<Self>,
        context: &TcpRemoteActorRuntimeContext,
        cluster: &ClusterDaemonHandle,
        source_replicas: ReplicatorRemoteSourceMap,
        requests: &ReplicatorRemoteRequestRegistry,
    ) -> Result<(), RemoteError> {
        materialize_distributed_data_family(context, cluster, source_replicas, requests, *self)
    }
}

/// Collects typed CRDT families before installing one shared remote handler.
pub struct DistributedDataRuntimeBuilder {
    cluster: ClusterDaemonRegistration,
    families: Vec<Box<dyn DistributedDataFamilyFactory>>,
    manifests_by_path: BTreeMap<String, String>,
    data_types: HashSet<TypeId>,
}

impl DistributedDataRuntimeBuilder {
    pub fn new(cluster: ClusterDaemonRegistration) -> Self {
        Self {
            cluster,
            families: Vec::new(),
            manifests_by_path: BTreeMap::new(),
            data_types: HashSet::new(),
        }
    }

    pub fn register<D>(
        &mut self,
        settings: DistributedDataSettings<D>,
    ) -> Result<DistributedDataRegistration<D>, DistributedDataBootstrapError>
    where
        D: DeltaReplicatedData + RemovedNodePruning + Send + 'static,
        D::Delta: Send + 'static,
    {
        settings.validate()?;
        let manifest = settings.codecs.data_codec().manifest().to_string();
        let remote_path = replicator_remote_path_for_manifest(&manifest)?;
        if self
            .manifests_by_path
            .values()
            .any(|registered| registered == &manifest)
        {
            return Err(DistributedDataBootstrapError::DuplicateDataManifest { manifest });
        }
        if let Some(registered_manifest) = self.manifests_by_path.get(&remote_path) {
            return Err(DistributedDataBootstrapError::FamilyPathCollision {
                path: remote_path,
                registered_manifest: registered_manifest.clone(),
                requested_manifest: manifest,
            });
        }
        if self.data_types.contains(&TypeId::of::<D>()) {
            return Err(DistributedDataBootstrapError::DuplicateDataType {
                type_name: type_name::<D>(),
            });
        }
        let actor_name = remote_path
            .strip_prefix("/system/")
            .expect("replicator family path must be a system path")
            .to_string();
        let handle = Arc::new(Mutex::new(None));
        self.manifests_by_path
            .insert(remote_path.clone(), manifest.clone());
        self.data_types.insert(TypeId::of::<D>());
        self.families
            .push(Box::new(TypedDistributedDataFamilyFactory {
                manifest,
                remote_path,
                actor_name: actor_name.clone(),
                settings: settings.clone(),
                handle: Arc::clone(&handle),
            }));
        Ok(DistributedDataRegistration {
            settings,
            actor_name,
            handle,
            activated: Arc::new(Mutex::new(false)),
        })
    }

    pub fn install(
        self,
        builder: &mut TcpRemoteActorRuntimeBuilder,
    ) -> Result<(), DistributedDataBootstrapError> {
        if self.families.is_empty() {
            return Err(DistributedDataBootstrapError::InvalidSettings {
                reason: "at least one CRDT family must be registered",
            });
        }
        let cluster = self.cluster;
        let families = self.families;
        builder.register_ordinary_handler(&DDATA_SYSTEM_MANIFESTS, move |context| {
            let cluster = cluster.handle().ok_or_else(|| {
                RemoteError::Inbound(
                    "cluster daemon must be registered before distributed data".to_string(),
                )
            })?;
            let source_replicas = ReplicatorRemoteSourceMap::default();
            let requests = ReplicatorRemoteRequestRegistry::default();
            for family in families {
                family.materialize(context, &cluster, Arc::clone(&source_replicas), &requests)?;
            }
            let replies = Arc::new(ReplicatorRemoteReplyInbound::with_remote_settings(
                context.system().clone(),
                context.settings().clone(),
                context.registry().clone(),
            ));
            Ok(ReplicatorRemoteSystemInbound::new(
                source_replicas,
                Arc::new(requests),
                replies,
            ))
        })?;
        Ok(())
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
    let mut runtime = DistributedDataRuntimeBuilder::new(cluster);
    let registration = runtime.register(settings)?;
    runtime.install(builder)?;
    Ok(registration)
}

fn materialize_distributed_data_family<D>(
    context: &TcpRemoteActorRuntimeContext,
    cluster: &ClusterDaemonHandle,
    source_replicas: ReplicatorRemoteSourceMap,
    requests: &ReplicatorRemoteRequestRegistry,
    family: TypedDistributedDataFamilyFactory<D>,
) -> Result<(), RemoteError>
where
    D: DeltaReplicatedData + RemovedNodePruning + Send + 'static,
    D::Delta: Send + 'static,
{
    let self_node = cluster.self_node().clone();
    let registry = context.registry().clone();
    let outbound =
        ReplicatorRemoteAssociationCacheOutbound::new(context.association_cache().clone());
    let local_sender =
        ActorRefWireData::new(format!("{}{}", self_node.address, family.remote_path))
            .map_err(|error| RemoteError::Inbound(error.to_string()))?;
    let gossip_targets = ReplicatorGossipTargetRegistry::new();
    let gossip_transport = ReplicatorGossipTransport::with_target_registry(gossip_targets.clone());
    let actor_gossip_transport = gossip_transport.clone();
    let actor_data_codec = family.settings.codecs.data_codec();
    let delta_targets = DeltaPropagationTargetRegistry::new();
    let delta_transport = DeltaPropagationTransport::with_target_registry(
        ReplicaId::from(&self_node),
        family.settings.codecs.delta_codec(),
        delta_targets.clone(),
    );
    let actor_delta_loop = DeltaPropagationLoop::new(delta_transport);
    let delta_propagation_interval = family.settings.effective_delta_propagation_interval();
    let aggregation_targets = AggregationTargetRegistry::new();
    let aggregation_transport = AggregationTransport::with_target_registry(
        ReplicaId::from(&self_node),
        family.settings.codecs.data_codec(),
        aggregation_targets.clone(),
    );
    let actor_aggregation = ReplicatorAggregation::with_sender_remote_settings(
        aggregation_transport,
        family.settings.codecs.data_codec(),
        context.settings().clone(),
    );
    let gossip_interval = family.settings.gossip_interval;
    let local_system_uid = context.local_system_uid();
    let actor_self_replica = ReplicaId::from(&self_node);
    let replicator = context
        .system()
        .spawn_system(
            family.actor_name.clone(),
            Props::new(move || {
                ReplicatorActor::<D>::with_gossip_interval(
                    actor_gossip_transport.clone(),
                    actor_data_codec.clone(),
                    gossip_interval,
                )
                .enable_delta_propagation(actor_delta_loop.clone(), delta_propagation_interval)
                .enable_aggregation(actor_aggregation.clone())
                .with_self_system_uid(local_system_uid)
                .with_self_replica(actor_self_replica.clone())
            }),
        )
        .map_err(|error| RemoteError::Inbound(error.to_string()))?;
    let remote_targets = ReplicatorRemoteRouteTargets::new(registry.clone(), outbound.clone())
        .with_recipient_path(family.remote_path.clone())
        .with_sender(Some(local_sender.clone()));
    let connector_replicator = replicator.clone();
    let connector_cluster = cluster.cluster().clone();
    let connector_self_node = self_node.clone();
    let connector_sources = Arc::clone(&source_replicas);
    let pruning_settings = ReplicatorClusterPruningSettings::new(
        duration_as_u64_nanos(family.settings.max_pruning_dissemination),
        duration_as_u64_millis(family.settings.pruning_marker_ttl),
    );
    let timing_settings = ReplicatorClusterConnectorTimingSettings::disabled()
        .with_clock_interval(Some(family.settings.gossip_interval))
        .with_pruning_interval(family.settings.pruning_interval);
    let connector = match context.system().spawn_system(
        format!("{}-cluster", family.actor_name),
        Props::new(move || {
            ReplicatorClusterConnector::new(
                connector_cluster.clone(),
                connector_self_node.clone(),
                connector_replicator.clone(),
            )
            .with_pruning_settings(pruning_settings)
            .with_timing_settings(timing_settings.clone())
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
    let request_inbound = Arc::new(
        ReplicatorRemoteRequestInbound::new(
            context.system().clone(),
            local_sender.clone(),
            Some(local_sender),
            registry.clone(),
            replicator.clone(),
            family.settings.codecs.clone(),
            outbound,
        )
        .with_reply_actor_prefix(format!("{}-remote", family.actor_name)),
    );
    let request_recipient = request_inbound.recipient().path().to_string();
    requests
        .register(
            family.manifest,
            request_recipient,
            request_inbound.clone() as Arc<dyn ReplicatorRemoteRequestReceiver>,
        )
        .map_err(|error| RemoteError::Inbound(error.to_string()))?;
    *family
        .handle
        .lock()
        .expect("distributed-data handle poisoned") = Some(DistributedDataHandle {
        self_node,
        replicator,
        connector,
    });
    Ok(())
}

fn duration_as_u64_nanos(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

fn duration_as_u64_millis(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use bytes::Bytes;
    use kairo_cluster::{
        ClusterAssociationPeerTarget, ClusterDaemonBootstrapSettings, ClusterGossipProcessSettings,
        ClusterMembershipMsg, DeadlineFailureDetectorSettings, Gossip, HeartbeatSenderSettings,
        MemberStatus, ReachabilityStatus, register_cluster_daemon,
        register_cluster_protocol_codecs,
    };
    use kairo_remote::{
        RemoteSettings, TcpRemoteActorRuntime, TcpRemoteReconnectSettings,
        register_remote_protocol_codecs,
    };
    use kairo_serialization::Registry;
    use kairo_testkit::{ActorSystemTestKit, TestProbe, await_assert};

    use super::*;
    use crate::{
        GCounter, GCounterCodec, GSet, GSetStringCodec, GSetStringDeltaCodec, GetResponse,
        ReadConsistency, ReplicatorClusterConnectorSnapshot, ReplicatorKey, UpdateResponse,
        WriteConsistency, register_ddata_protocol_codecs,
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

    struct MultiFamilyDdataNode {
        kit: ActorSystemTestKit,
        runtime: TcpRemoteActorRuntime,
        cluster: kairo_cluster::ClusterDaemonHandle,
        counters: DistributedDataHandle<GCounter>,
        sets: DistributedDataHandle<GSet<String>>,
        gossip_probe: TestProbe<Gossip>,
        connector_probe: TestProbe<ReplicatorClusterConnectorSnapshot>,
    }

    impl MultiFamilyDdataNode {
        fn start(
            system: &str,
            node_uid: u64,
            remote_uid: u64,
            seed_nodes: Vec<kairo_actor::Address>,
            registry: Arc<Registry>,
        ) -> Self {
            let kit = ActorSystemTestKit::new(system).unwrap();
            let mut builder = TcpRemoteActorRuntime::builder(
                kit.system().clone(),
                registry,
                RemoteSettings::new("127.0.0.1", 0),
                remote_uid,
            );
            let cluster_registration = register_cluster_daemon(
                &mut builder,
                ClusterDaemonBootstrapSettings::new(node_uid)
                    .with_seed_nodes(seed_nodes)
                    .with_config_digest(Some(Bytes::from_static(b"ddata-multi-family")))
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
            let mut ddata = DistributedDataRuntimeBuilder::new(cluster_registration.clone());
            let counters = ddata
                .register(
                    DistributedDataSettings::new(ReplicatorWireCodecs::new(
                        Arc::new(GCounterCodec),
                        Arc::new(GCounterCodec),
                    ))
                    .with_gossip_interval(Duration::from_millis(20))
                    .with_delta_propagation_interval(Duration::from_millis(20)),
                )
                .unwrap();
            let sets = ddata
                .register(
                    DistributedDataSettings::new(ReplicatorWireCodecs::new(
                        Arc::new(GSetStringDeltaCodec),
                        Arc::new(GSetStringCodec),
                    ))
                    .with_gossip_interval(Duration::from_millis(20))
                    .with_delta_propagation_interval(Duration::from_millis(20)),
                )
                .unwrap();
            ddata.install(&mut builder).unwrap();
            let runtime = builder.bind().unwrap();
            let cluster = cluster_registration.activate(&runtime).unwrap();
            let counters = counters.activate(&runtime).unwrap();
            let sets = sets.activate(&runtime).unwrap();
            assert!(
                kit.system()
                    .has_extension::<DistributedDataExtension<GCounter>>()
            );
            assert!(
                kit.system()
                    .has_extension::<DistributedDataExtension<GSet<String>>>()
            );
            Self {
                gossip_probe: kit.create_probe("multi-family-gossip").unwrap(),
                connector_probe: kit.create_probe("multi-family-connector").unwrap(),
                kit,
                runtime,
                cluster,
                counters,
                sets,
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

        fn connector(
            &self,
            connector: &ActorRef<ReplicatorClusterConnectorMsg>,
        ) -> ReplicatorClusterConnectorSnapshot {
            connector
                .tell(ReplicatorClusterConnectorMsg::Snapshot {
                    reply_to: self.connector_probe.actor_ref(),
                })
                .unwrap();
            self.connector_probe
                .expect_msg(Duration::from_secs(1))
                .unwrap()
        }

        fn update_both(&self, key: ReplicatorKey) {
            let counter_updates = self
                .kit
                .create_probe::<UpdateResponse<GCounter>>("multi-counter-updates")
                .unwrap();
            let self_replica = self.counters.self_replica();
            self.counters
                .replicator()
                .tell(ReplicatorActorMsg::Update {
                    key: key.clone(),
                    initial: GCounter::new(),
                    consistency: WriteConsistency::local(),
                    modify: Box::new(move |counter| {
                        counter
                            .increment(self_replica, 7)
                            .map_err(|error| error.to_string())
                    }),
                    reply_to: counter_updates.actor_ref(),
                })
                .unwrap();
            assert!(matches!(
                counter_updates.expect_msg(Duration::from_secs(1)).unwrap(),
                UpdateResponse::Success(_)
            ));

            let set_updates = self
                .kit
                .create_probe::<UpdateResponse<GSet<String>>>("multi-set-updates")
                .unwrap();
            self.sets
                .replicator()
                .tell(ReplicatorActorMsg::Update {
                    key,
                    initial: GSet::new(),
                    consistency: WriteConsistency::local(),
                    modify: Box::new(|set| Ok(set.add("blue".to_string()))),
                    reply_to: set_updates.actor_ref(),
                })
                .unwrap();
            assert!(matches!(
                set_updates.expect_msg(Duration::from_secs(1)).unwrap(),
                UpdateResponse::Success(_)
            ));
        }

        fn await_both(&self, key: ReplicatorKey) {
            let counter_reads = self
                .kit
                .create_probe::<GetResponse<GCounter>>("multi-counter-reads")
                .unwrap();
            let set_reads = self
                .kit
                .create_probe::<GetResponse<GSet<String>>>("multi-set-reads")
                .unwrap();
            await_assert(Duration::from_secs(4), Duration::from_millis(20), || {
                self.counters
                    .replicator()
                    .tell(ReplicatorActorMsg::Get {
                        key: key.clone(),
                        consistency: ReadConsistency::local(),
                        reply_to: counter_reads.actor_ref(),
                    })
                    .map_err(|error| error.reason().to_string())?;
                match counter_reads
                    .expect_msg(Duration::from_millis(100))
                    .map_err(|error| error.to_string())?
                {
                    GetResponse::Success { data, .. } if data.value().unwrap() == 7 => {}
                    response => return Err(format!("counter has not converged: {response:?}")),
                }
                self.sets
                    .replicator()
                    .tell(ReplicatorActorMsg::Get {
                        key: key.clone(),
                        consistency: ReadConsistency::local(),
                        reply_to: set_reads.actor_ref(),
                    })
                    .map_err(|error| error.reason().to_string())?;
                match set_reads
                    .expect_msg(Duration::from_millis(100))
                    .map_err(|error| error.to_string())?
                {
                    GetResponse::Success { data, .. } if data.contains(&"blue".to_string()) => {
                        Ok(())
                    }
                    response => Err(format!("set has not converged: {response:?}")),
                }
            })
            .unwrap();
        }

        fn shutdown(self) {
            self.kit.system().stop(self.cluster.root());
            self.runtime.shutdown().unwrap();
            self.kit.shutdown(Duration::from_secs(1)).unwrap();
        }
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
            Self::start_configured(
                system,
                node_uid,
                remote_uid,
                seed_nodes,
                registry,
                gossip_interval,
                delta_propagation_interval,
                None,
            )
        }

        #[allow(clippy::too_many_arguments)]
        fn start_with_pruning(
            system: &str,
            node_uid: u64,
            remote_uid: u64,
            seed_nodes: Vec<kairo_actor::Address>,
            registry: Arc<Registry>,
            gossip_interval: Duration,
            pruning_interval: Duration,
            max_pruning_dissemination: Duration,
            pruning_marker_ttl: Duration,
        ) -> Self {
            Self::start_configured(
                system,
                node_uid,
                remote_uid,
                seed_nodes,
                registry,
                gossip_interval,
                Some(gossip_interval),
                Some((
                    pruning_interval,
                    max_pruning_dissemination,
                    pruning_marker_ttl,
                )),
            )
        }

        #[allow(clippy::too_many_arguments)]
        fn start_configured(
            system: &str,
            node_uid: u64,
            remote_uid: u64,
            seed_nodes: Vec<kairo_actor::Address>,
            registry: Arc<Registry>,
            gossip_interval: Duration,
            delta_propagation_interval: Option<Duration>,
            pruning: Option<(Duration, Duration, Duration)>,
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
            if let Some((interval, dissemination, marker_ttl)) = pruning {
                ddata_settings = ddata_settings
                    .with_pruning_interval(Some(interval))
                    .with_max_pruning_dissemination(dissemination)
                    .with_pruning_marker_ttl(marker_ttl);
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

        fn await_counter_pruned(&self, key: ReplicatorKey, removed: &ReplicaId, expected: u128) {
            let id = PROBE_ID.fetch_add(1, Ordering::Relaxed);
            let reads = self
                .kit
                .create_probe::<GetResponse<GCounter>>(format!("pruned-reads-{id}"))
                .unwrap();
            await_assert(Duration::from_secs(5), Duration::from_millis(20), || {
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
                    GetResponse::Success { data, .. }
                        if data.value().unwrap() == expected
                            && !data.need_pruning_from(removed) =>
                    {
                        Ok(())
                    }
                    response => Err(format!("counter has not been pruned: {response:?}")),
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
        assert!(matches!(
            settings()
                .with_pruning_interval(Some(Duration::ZERO))
                .validate(),
            Err(DistributedDataBootstrapError::InvalidSettings { .. })
        ));
        assert!(matches!(
            settings()
                .with_max_pruning_dissemination(Duration::ZERO)
                .validate(),
            Err(DistributedDataBootstrapError::InvalidSettings { .. })
        ));
        assert!(matches!(
            settings()
                .with_pruning_marker_ttl(Duration::ZERO)
                .validate(),
            Err(DistributedDataBootstrapError::InvalidSettings { .. })
        ));
    }

    #[test]
    fn settings_use_pekko_aligned_removed_node_pruning_defaults() {
        let settings = DistributedDataSettings::new(ReplicatorWireCodecs::new(
            Arc::new(GCounterCodec),
            Arc::new(GCounterCodec),
        ));

        assert_eq!(settings.pruning_interval(), Some(Duration::from_secs(120)));
        assert_eq!(
            settings.max_pruning_dissemination(),
            Duration::from_secs(300)
        );
        assert_eq!(
            settings.pruning_marker_ttl(),
            Duration::from_secs(6 * 60 * 60)
        );
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
    fn composed_runtime_routes_two_typed_families_with_the_same_key() {
        let registry = registry();
        let seed = MultiFamilyDdataNode::start(
            "ddata-multi-family-seed",
            1,
            151,
            vec![],
            registry.clone(),
        );
        await_assert(Duration::from_secs(2), Duration::from_millis(5), || {
            (seed
                .gossip()
                .member(seed.cluster.self_node())
                .map(|member| member.status)
                == Some(MemberStatus::Up))
            .then_some(())
            .ok_or_else(|| "multi-family seed has not formed".to_string())
        })
        .unwrap();
        let peer = MultiFamilyDdataNode::start(
            "ddata-multi-family-peer",
            2,
            152,
            vec![seed.cluster.self_node().address.clone()],
            registry,
        );
        await_assert(Duration::from_secs(3), Duration::from_millis(10), || {
            let expected_seed = ReplicaId::from(seed.cluster.self_node());
            let expected_peer = ReplicaId::from(peer.cluster.self_node());
            let seed_counter_routes = seed.connector(seed.counters.connector());
            let seed_set_routes = seed.connector(seed.sets.connector());
            let peer_counter_routes = peer.connector(peer.counters.connector());
            let peer_set_routes = peer.connector(peer.sets.connector());
            (seed_counter_routes.remote_replicas == vec![expected_peer.clone()]
                && seed_set_routes.remote_replicas == vec![expected_peer]
                && peer_counter_routes.remote_replicas == vec![expected_seed.clone()]
                && peer_set_routes.remote_replicas == vec![expected_seed])
            .then_some(())
            .ok_or_else(|| "typed family connectors have not derived cluster routes".to_string())
        })
        .unwrap();

        let shared_key = ReplicatorKey::new("shared-key");
        seed.update_both(shared_key.clone());
        peer.await_both(shared_key);

        peer.shutdown();
        seed.shutdown();
    }

    #[test]
    fn composed_replicas_merge_divergent_updates_after_partition_heals() {
        let registry = registry();
        let seed = ComposedDdataNode::start(
            "ddata-partition-seed",
            1,
            161,
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
            .ok_or_else(|| "partition seed has not formed".to_string())
        })
        .unwrap();
        let peer = ComposedDdataNode::start(
            "ddata-partition-peer",
            2,
            162,
            vec![seed.cluster.self_node().address.clone()],
            registry,
            Duration::from_millis(20),
            Some(Duration::from_millis(20)),
        );
        let seed_node = seed.cluster.self_node().clone();
        let peer_node = peer.cluster.self_node().clone();
        await_assert(Duration::from_secs(3), Duration::from_millis(10), || {
            let seed_routes = seed.connector();
            let peer_routes = peer.connector();
            (seed_routes.remote_replicas == vec![ReplicaId::from(&peer_node)]
                && peer_routes.remote_replicas == vec![ReplicaId::from(&seed_node)])
            .then_some(())
            .ok_or_else(|| "partition pair has not established ddata routes".to_string())
        })
        .unwrap();

        let peer_address = ClusterAssociationPeerTarget::new(peer_node.clone())
            .unwrap()
            .association()
            .clone();
        let seed_address = ClusterAssociationPeerTarget::new(seed_node.clone())
            .unwrap()
            .association()
            .clone();
        seed.runtime
            .peer_manager()
            .disconnect(&peer_address, "ddata partition test")
            .unwrap();
        peer.runtime
            .peer_manager()
            .disconnect(&seed_address, "ddata partition test")
            .unwrap();

        await_assert(Duration::from_secs(3), Duration::from_millis(10), || {
            let seed_gossip = seed.gossip();
            let peer_gossip = peer.gossip();
            let seed_routes = seed.connector();
            let peer_routes = peer.connector();
            let seed_status = seed_gossip.reachability().status(&seed_node, &peer_node);
            let peer_status = peer_gossip.reachability().status(&peer_node, &seed_node);
            (seed_status == ReachabilityStatus::Unreachable
                && peer_status == ReachabilityStatus::Unreachable
                && seed_routes
                    .unreachable_replicas
                    .contains(&ReplicaId::from(&peer_node))
                && peer_routes
                    .unreachable_replicas
                    .contains(&ReplicaId::from(&seed_node)))
            .then_some(())
            .ok_or_else(|| {
                format!(
                    "partition not observed: statuses={seed_status:?}/{peer_status:?}, unreachable={:?}/{:?}",
                    seed_routes.unreachable_replicas, peer_routes.unreachable_replicas
                )
            })
        })
        .unwrap();

        let key = ReplicatorKey::new("partition-counter");
        seed.update_counter(key.clone(), 3);
        peer.update_counter(key.clone(), 5);
        seed.await_counter(key.clone(), 3);
        peer.await_counter(key.clone(), 5);

        seed.runtime.peer_manager().connect(peer_address).unwrap();
        await_assert(Duration::from_secs(4), Duration::from_millis(10), || {
            let seed_gossip = seed.gossip();
            let peer_gossip = peer.gossip();
            let seed_routes = seed.connector();
            let peer_routes = peer.connector();
            (seed_gossip.reachability().status(&seed_node, &peer_node)
                == ReachabilityStatus::Reachable
                && peer_gossip.reachability().status(&peer_node, &seed_node)
                    == ReachabilityStatus::Reachable
                && seed_routes.unreachable_replicas.is_empty()
                && peer_routes.unreachable_replicas.is_empty()
                && seed_routes.remote_replicas == vec![ReplicaId::from(&peer_node)]
                && peer_routes.remote_replicas == vec![ReplicaId::from(&seed_node)])
            .then_some(())
            .ok_or_else(|| "healed partition has not restored both ddata routes".to_string())
        })
        .unwrap();
        seed.await_counter(key.clone(), 8);
        peer.await_counter(key, 8);

        peer.shutdown();
        seed.shutdown();
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

        let repaired = peer
            .kit
            .create_probe::<GetResponse<GCounter>>("repaired-local-reads")
            .unwrap();
        peer.ddata
            .replicator()
            .tell(ReplicatorActorMsg::Get {
                key: read_key.clone(),
                consistency: ReadConsistency::local(),
                reply_to: repaired.actor_ref(),
            })
            .unwrap();
        let GetResponse::Success { data, .. } =
            repaired.expect_msg(Duration::from_secs(1)).unwrap()
        else {
            panic!("majority read replied before repairing local state");
        };
        assert_eq!(data.value().unwrap(), 13);

        peer.shutdown();
        seed.shutdown();
    }

    #[test]
    fn composed_extension_survivors_reform_quorum_after_replica_crash() {
        let registry = registry();
        let seed = ComposedDdataNode::start_with_pruning(
            "ddata-fault-seed",
            1,
            401,
            vec![],
            registry.clone(),
            Duration::from_millis(20),
            Duration::from_millis(20),
            Duration::from_millis(100),
            Duration::from_secs(5),
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
        let peer = ComposedDdataNode::start_with_pruning(
            "ddata-fault-peer",
            2,
            402,
            vec![seed.cluster.self_node().address.clone()],
            registry.clone(),
            Duration::from_millis(20),
            Duration::from_millis(20),
            Duration::from_millis(100),
            Duration::from_secs(5),
        );
        await_assert(Duration::from_secs(4), Duration::from_millis(10), || {
            let seed_routes = seed.connector();
            let peer_routes = peer.connector();
            (seed_routes.remote_replicas == vec![ReplicaId::from(peer.cluster.self_node())]
                && peer_routes.remote_replicas == vec![ReplicaId::from(seed.cluster.self_node())])
            .then_some(())
            .ok_or_else(|| "ddata pruning survivors have not formed before third join".to_string())
        })
        .unwrap();
        let crashed = ComposedDdataNode::start_with_pruning(
            "ddata-fault-crashed",
            3,
            403,
            vec![seed.cluster.self_node().address.clone()],
            registry,
            Duration::from_millis(20),
            Duration::from_millis(20),
            Duration::from_millis(100),
            Duration::from_secs(5),
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
        await_assert(Duration::from_secs(2), Duration::from_millis(10), || {
            seed.connector()
                .last_pruning_report
                .filter(|report| report.skipped_unreachable)
                .map(|_| ())
                .ok_or_else(|| {
                    "removed-node pruning did not pause for unreachable replica".to_string()
                })
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
        seed.await_counter_pruned(key.clone(), &crashed_replica, 5);
        peer.await_counter_pruned(key.clone(), &crashed_replica, 5);

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
