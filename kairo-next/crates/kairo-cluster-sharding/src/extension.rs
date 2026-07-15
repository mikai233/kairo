use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::fmt::{self, Display, Formatter};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use kairo_actor::{
    Actor, ActorError, ActorRef, ActorResult, ActorSystem, Context, PHASE_BEFORE_CLUSTER_SHUTDOWN,
    Props, Recipient,
};
use kairo_cluster::{
    Cluster, ClusterDaemonRegistration, ClusterEvent, ClusterExtension, ClusterSubscriptionEvent,
    ClusterSubscriptionInitialState, Member, MemberEvent, UniqueAddress,
};
use kairo_cluster_tools::{
    ClusterSingleton, ClusterSingletonBootstrapError, ClusterSingletonRef,
    ClusterSingletonRegistration, ClusterSingletonSettings, Singleton, register_cluster_singleton,
};
use kairo_distributed_data::{ORSet, ReplicaId, ReplicatorActorMsg};
use kairo_remote::{
    RemoteEnvelopeHandler, RemoteError, RemoteOutboundRecipient, TcpRemoteActorRuntime,
    TcpRemoteActorRuntimeBuilder, TcpRemoteActorRuntimeContext,
};
use kairo_serialization::{ActorRefWireData, Registry, RemoteEnvelope, RemoteMessage};

use crate::{
    BeginHandOff, BeginHandOffAck, CoordinatorDiscoverySettings, CoordinatorState,
    DEFAULT_SHARD_COUNT, EntityActorFactory, EntityRef, EntityTypeKey, GetShardHome,
    GracefulShutdownReq, HandOff, HostShard, LeastShardAllocationStrategy, RegionLocalRoutePlan,
    RegionRemoteCoordinatorTransport, RegionStopped, Register, RegisterAck,
    RememberCoordinatorDDataStoreMsg, RememberCoordinatorStoreMsg, RoutedShardEnvelope,
    ShardCoordinatorActor, ShardCoordinatorMsg, ShardCoordinatorRemoteHomeInbound,
    ShardCoordinatorRemoteRegistrationInbound, ShardCoordinatorRemoteTarget, ShardDeliverPlan,
    ShardHome, ShardRegionActor, ShardRegionMsg, ShardRegionRemoteControlInbound,
    ShardRegionRemoteInbound, ShardRegionRemoteOutbound, ShardRegionSystemInbound, ShardStarted,
    ShardStopped, ShardingEnvelope, ShardingEnvelopeRouter, remote_region_id,
};

pub const SHARDING_ORDINARY_MANIFESTS: [&str; 1] = [RoutedShardEnvelope::MANIFEST];

pub const SHARDING_CONTROL_MANIFESTS: [&str; 12] = [
    Register::MANIFEST,
    RegisterAck::MANIFEST,
    GetShardHome::MANIFEST,
    ShardHome::MANIFEST,
    HostShard::MANIFEST,
    GracefulShutdownReq::MANIFEST,
    RegionStopped::MANIFEST,
    ShardStarted::MANIFEST,
    BeginHandOff::MANIFEST,
    BeginHandOffAck::MANIFEST,
    HandOff::MANIFEST,
    ShardStopped::MANIFEST,
];

#[derive(Debug, Clone)]
pub struct ClusterShardingSettings {
    region_buffer_capacity: usize,
    shard_buffer_capacity: usize,
    registration_retry_interval: Duration,
    handoff_timeout: Duration,
    shutdown_timeout: Duration,
}

impl Default for ClusterShardingSettings {
    fn default() -> Self {
        Self {
            region_buffer_capacity: 1_000,
            shard_buffer_capacity: 1_000,
            registration_retry_interval: Duration::from_secs(1),
            handoff_timeout: Duration::from_secs(10),
            shutdown_timeout: Duration::from_secs(3),
        }
    }
}

impl ClusterShardingSettings {
    pub fn with_region_buffer_capacity(mut self, capacity: usize) -> Self {
        self.region_buffer_capacity = capacity;
        self
    }

    pub fn with_shard_buffer_capacity(mut self, capacity: usize) -> Self {
        self.shard_buffer_capacity = capacity;
        self
    }

    pub fn with_registration_retry_interval(mut self, interval: Duration) -> Self {
        self.registration_retry_interval = interval;
        self
    }

    pub fn with_handoff_timeout(mut self, timeout: Duration) -> Self {
        self.handoff_timeout = timeout;
        self
    }

    pub fn with_shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    fn validate(&self) -> Result<(), ClusterShardingBootstrapError> {
        if self.region_buffer_capacity == 0 {
            return Err(ClusterShardingBootstrapError::InvalidSettings(
                "region buffer capacity must be greater than zero",
            ));
        }
        if self.shard_buffer_capacity == 0 {
            return Err(ClusterShardingBootstrapError::InvalidSettings(
                "shard buffer capacity must be greater than zero",
            ));
        }
        if self.registration_retry_interval.is_zero() {
            return Err(ClusterShardingBootstrapError::InvalidSettings(
                "registration retry interval must be greater than zero",
            ));
        }
        if self.handoff_timeout.is_zero() {
            return Err(ClusterShardingBootstrapError::InvalidSettings(
                "handoff timeout must be greater than zero",
            ));
        }
        if self.shutdown_timeout.is_zero() {
            return Err(ClusterShardingBootstrapError::InvalidSettings(
                "shutdown timeout must be greater than zero",
            ));
        }
        Ok(())
    }
}

pub struct Entity<M>
where
    M: Clone + Send + 'static,
{
    type_key: EntityTypeKey<M>,
    factory: EntityActorFactory<M>,
    shard_count: u64,
    coordinator_role: Option<String>,
    coordinator_remember_store: Option<CoordinatorRememberStoreSettings>,
    ddata_remember_entities: Option<DDataRememberEntitiesSettings>,
    stop_message: Option<M>,
}

#[derive(Clone)]
struct CoordinatorRememberStoreSettings {
    target: CoordinatorRememberStoreTargetSettings,
    timeout: Duration,
}

#[derive(Clone)]
enum CoordinatorRememberStoreTargetSettings {
    Actor(ActorRef<RememberCoordinatorStoreMsg>),
    DistributedData(ActorRef<RememberCoordinatorDDataStoreMsg>),
}

#[derive(Clone)]
pub struct DDataRememberEntitiesSettings {
    coordinator_store: ActorRef<RememberCoordinatorDDataStoreMsg>,
    shard_replica_id: ReplicaId,
    shard_replicator: ActorRef<ReplicatorActorMsg<ORSet<String>>>,
    timeout: Duration,
}

impl DDataRememberEntitiesSettings {
    pub fn new(
        coordinator_store: ActorRef<RememberCoordinatorDDataStoreMsg>,
        shard_replica_id: impl Into<ReplicaId>,
        shard_replicator: ActorRef<ReplicatorActorMsg<ORSet<String>>>,
        timeout: Duration,
    ) -> Self {
        Self {
            coordinator_store,
            shard_replica_id: shard_replica_id.into(),
            shard_replicator,
            timeout,
        }
    }
}

impl<M> Entity<M>
where
    M: Clone + Send + 'static,
{
    pub fn new<A, F>(type_key: EntityTypeKey<M>, factory: F) -> Self
    where
        A: Actor<Msg = M>,
        F: Fn(String) -> A + Send + Sync + 'static,
    {
        Self {
            type_key,
            factory: EntityActorFactory::new(factory),
            shard_count: DEFAULT_SHARD_COUNT,
            coordinator_role: None,
            coordinator_remember_store: None,
            ddata_remember_entities: None,
            stop_message: None,
        }
    }

    pub fn of<A, F>(type_key: EntityTypeKey<M>, factory: F) -> Self
    where
        A: Actor<Msg = M>,
        F: Fn(String) -> A + Send + Sync + 'static,
    {
        Self::new(type_key, factory)
    }

    pub fn with_shard_count(mut self, shard_count: u64) -> Self {
        self.shard_count = shard_count;
        self
    }

    pub fn with_coordinator_role(mut self, role: impl Into<String>) -> Self {
        self.coordinator_role = Some(role.into());
        self
    }

    pub fn with_coordinator_remember_store(
        mut self,
        store: ActorRef<RememberCoordinatorStoreMsg>,
        timeout: Duration,
    ) -> Self {
        self.coordinator_remember_store = Some(CoordinatorRememberStoreSettings {
            target: CoordinatorRememberStoreTargetSettings::Actor(store),
            timeout,
        });
        self
    }

    pub fn with_coordinator_ddata_remember_store(
        mut self,
        store: ActorRef<RememberCoordinatorDDataStoreMsg>,
        timeout: Duration,
    ) -> Self {
        self.coordinator_remember_store = Some(CoordinatorRememberStoreSettings {
            target: CoordinatorRememberStoreTargetSettings::DistributedData(store),
            timeout,
        });
        self
    }

    pub fn with_ddata_remember_entities(mut self, settings: DDataRememberEntitiesSettings) -> Self {
        self.coordinator_remember_store = Some(CoordinatorRememberStoreSettings {
            target: CoordinatorRememberStoreTargetSettings::DistributedData(
                settings.coordinator_store.clone(),
            ),
            timeout: settings.timeout,
        });
        self.ddata_remember_entities = Some(settings);
        self
    }

    pub fn with_stop_message(mut self, stop_message: M) -> Self {
        self.stop_message = Some(stop_message);
        self
    }
}

#[derive(Debug)]
pub enum ClusterShardingBootstrapError {
    Actor(ActorError),
    InvalidEntity {
        type_name: String,
        reason: &'static str,
    },
    InvalidSettings(&'static str),
    NotMaterialized,
    Remote(RemoteError),
    Singleton(ClusterSingletonBootstrapError),
    TypeMismatch {
        type_name: String,
    },
    WirePath {
        type_name: String,
        reason: String,
    },
}

impl Display for ClusterShardingBootstrapError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Actor(error) => write!(f, "{error}"),
            Self::InvalidEntity { type_name, reason } => {
                write!(f, "invalid sharded entity type `{type_name}`: {reason}")
            }
            Self::InvalidSettings(reason) => {
                write!(f, "invalid cluster-sharding settings: {reason}")
            }
            Self::NotMaterialized => {
                write!(f, "cluster sharding has not been materialized by bind")
            }
            Self::Remote(error) => write!(f, "{error}"),
            Self::Singleton(error) => write!(f, "{error}"),
            Self::TypeMismatch { type_name } => write!(
                f,
                "sharded entity type `{type_name}` was initialized with a different message type"
            ),
            Self::WirePath { type_name, reason } => {
                write!(
                    f,
                    "sharded entity type `{type_name}` has an invalid wire path: {reason}"
                )
            }
        }
    }
}

impl std::error::Error for ClusterShardingBootstrapError {}

impl From<ActorError> for ClusterShardingBootstrapError {
    fn from(error: ActorError) -> Self {
        Self::Actor(error)
    }
}

impl From<RemoteError> for ClusterShardingBootstrapError {
    fn from(error: RemoteError) -> Self {
        Self::Remote(error)
    }
}

impl From<ClusterSingletonBootstrapError> for ClusterShardingBootstrapError {
    fn from(error: ClusterSingletonBootstrapError) -> Self {
        Self::Singleton(error)
    }
}

type InboundRoute = dyn Fn(RemoteEnvelope) -> Result<(), RemoteError> + Send + Sync;

#[derive(Clone, Default)]
struct ShardingInboundRouter {
    routes: Arc<RwLock<HashMap<String, Arc<InboundRoute>>>>,
}

impl ShardingInboundRouter {
    fn insert(
        &self,
        path: String,
        route: Arc<InboundRoute>,
    ) -> Result<(), ClusterShardingBootstrapError> {
        let mut routes = self
            .routes
            .write()
            .expect("sharding inbound routes poisoned");
        if routes.insert(path.clone(), route).is_some() {
            return Err(ClusterShardingBootstrapError::WirePath {
                type_name: path,
                reason: "recipient path is already registered".to_string(),
            });
        }
        Ok(())
    }

    fn remove(&self, path: &str) {
        self.routes
            .write()
            .expect("sharding inbound routes poisoned")
            .remove(path);
    }
}

impl RemoteEnvelopeHandler for ShardingInboundRouter {
    fn receive(&self, envelope: RemoteEnvelope) -> Result<(), RemoteError> {
        let path = envelope.recipient.path().to_string();
        let route = self
            .routes
            .read()
            .expect("sharding inbound routes poisoned")
            .get(&path)
            .cloned()
            .ok_or_else(|| {
                RemoteError::Inbound(format!(
                    "no cluster-sharding recipient is registered for `{path}`"
                ))
            })?;
        route(envelope)
    }
}

#[derive(Clone)]
struct ShardingRuntimeResources {
    system: ActorSystem,
    cluster: Cluster,
    self_node: UniqueAddress,
    registry: Arc<Registry>,
    outbound: Arc<dyn Recipient<RemoteEnvelope> + Send + Sync>,
    inbound: ShardingInboundRouter,
}

pub struct ClusterShardingRegistration {
    settings: ClusterShardingSettings,
    resources: Arc<Mutex<Option<ShardingRuntimeResources>>>,
    singleton: Option<ClusterSingletonRegistration>,
}

impl Clone for ClusterShardingRegistration {
    fn clone(&self) -> Self {
        Self {
            settings: self.settings.clone(),
            resources: Arc::clone(&self.resources),
            singleton: self.singleton.clone(),
        }
    }
}

impl ClusterShardingRegistration {
    pub fn activate(
        &self,
        runtime: &TcpRemoteActorRuntime,
    ) -> Result<Arc<ClusterSharding>, ClusterShardingBootstrapError> {
        ClusterExtension::get(runtime.system())?;
        if let Some(singleton) = self.singleton.as_ref() {
            singleton.activate(runtime)?;
        }
        let resources = self
            .resources
            .lock()
            .expect("cluster-sharding resources poisoned")
            .clone()
            .ok_or(ClusterShardingBootstrapError::NotMaterialized)?;
        let settings = self.settings.clone();
        let extension = runtime
            .system()
            .register_extension(move |_| ClusterSharding {
                resources,
                settings,
                entities: Mutex::new(HashMap::new()),
            });
        Ok(extension)
    }
}

pub fn register_cluster_sharding(
    builder: &mut TcpRemoteActorRuntimeBuilder,
    cluster: ClusterDaemonRegistration,
    settings: ClusterShardingSettings,
) -> Result<ClusterShardingRegistration, ClusterShardingBootstrapError> {
    settings.validate()?;
    let resources = Arc::new(Mutex::new(None));
    let ordinary_resources = Arc::clone(&resources);
    let ordinary_cluster = cluster.clone();
    builder.register_ordinary_handler(&SHARDING_ORDINARY_MANIFESTS, move |context| {
        materialize_resources(context, &ordinary_cluster, &ordinary_resources)
    })?;
    let control_resources = Arc::clone(&resources);
    builder.register_reliable_control_handler(&SHARDING_CONTROL_MANIFESTS, move |context| {
        materialize_resources(context, &cluster, &control_resources)
    })?;
    Ok(ClusterShardingRegistration {
        settings,
        resources,
        singleton: None,
    })
}

pub fn register_cluster_sharding_with_singleton(
    builder: &mut TcpRemoteActorRuntimeBuilder,
    cluster: ClusterDaemonRegistration,
    settings: ClusterShardingSettings,
    singleton_settings: ClusterSingletonSettings,
) -> Result<ClusterShardingRegistration, ClusterShardingBootstrapError> {
    let singleton = register_cluster_singleton(builder, cluster.clone(), singleton_settings)?;
    let mut sharding = register_cluster_sharding(builder, cluster, settings)?;
    sharding.singleton = Some(singleton);
    Ok(sharding)
}

fn materialize_resources(
    context: &TcpRemoteActorRuntimeContext,
    cluster: &ClusterDaemonRegistration,
    slot: &Arc<Mutex<Option<ShardingRuntimeResources>>>,
) -> Result<ShardingInboundRouter, RemoteError> {
    let mut slot = slot.lock().expect("cluster-sharding resources poisoned");
    if let Some(resources) = slot.as_ref() {
        return Ok(resources.inbound.clone());
    }
    let cluster = cluster.handle().ok_or_else(|| {
        RemoteError::Inbound("cluster daemon must be registered before cluster sharding".into())
    })?;
    let inbound = ShardingInboundRouter::default();
    *slot = Some(ShardingRuntimeResources {
        system: context.system().clone(),
        cluster: cluster.cluster().clone(),
        self_node: cluster.self_node().clone(),
        registry: context.registry().clone(),
        outbound: Arc::new(RemoteOutboundRecipient::from_arc(
            context.outbound().clone(),
        )),
        inbound: inbound.clone(),
    });
    Ok(inbound)
}

struct InitializedEntity<M>
where
    M: Clone + Send + 'static,
{
    router: ActorRef<ShardingEnvelope<M>>,
    _region: ActorRef<ShardRegionMsg<M>>,
    _coordinator: ActorRef<ShardCoordinatorMsg<M>>,
    _connector: ActorRef<ClusterShardingConnectorMsg>,
}

struct SingletonCoordinatorEndpoint<M>
where
    M: Clone + Send + 'static,
{
    singleton: ClusterSingletonRef<ShardCoordinatorMsg<M>>,
}

impl<M> Actor for SingletonCoordinatorEndpoint<M>
where
    M: Clone + Send + 'static,
{
    type Msg = ShardCoordinatorMsg<M>;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, message: Self::Msg) -> ActorResult {
        self.singleton
            .tell(message)
            .map_err(|error| ActorError::Message(error.reason().to_string()))
    }
}

pub struct ClusterSharding {
    resources: ShardingRuntimeResources,
    settings: ClusterShardingSettings,
    entities: Mutex<HashMap<String, Box<dyn Any + Send>>>,
}

impl ClusterSharding {
    pub fn get(system: &ActorSystem) -> Result<Arc<Self>, ActorError> {
        system.extension::<Self>()
    }

    pub fn init<M>(
        &self,
        entity: Entity<M>,
    ) -> Result<ActorRef<ShardingEnvelope<M>>, ClusterShardingBootstrapError>
    where
        M: Clone + RemoteMessage + Send + 'static,
    {
        let type_name = entity.type_key.name().to_string();
        validate_entity(
            &type_name,
            entity.shard_count,
            entity.coordinator_role.as_deref(),
        )?;
        let stop_message =
            entity
                .stop_message
                .ok_or_else(|| ClusterShardingBootstrapError::InvalidEntity {
                    type_name: type_name.clone(),
                    reason: "a stop message is required for handoff",
                })?;
        if entity
            .coordinator_remember_store
            .as_ref()
            .is_some_and(|settings| settings.timeout.is_zero())
        {
            return Err(ClusterShardingBootstrapError::InvalidEntity {
                type_name,
                reason: "coordinator remember-store timeout must be greater than zero",
            });
        }
        let mut entities = self.entities.lock().expect("sharded entities poisoned");
        if let Some(existing) = entities.get(&type_name) {
            return existing
                .downcast_ref::<InitializedEntity<M>>()
                .map(|initialized| initialized.router.clone())
                .ok_or(ClusterShardingBootstrapError::TypeMismatch { type_name });
        }

        let names = EntitySystemNames::new(&type_name);
        let region_wire = wire_ref(&self.resources.self_node, &names.region_path, &type_name)?;
        let coordinator_wire = wire_ref(
            &self.resources.self_node,
            &names.coordinator_path,
            &type_name,
        )?;
        let discovery_settings = match entity.coordinator_role.as_ref() {
            Some(role) => CoordinatorDiscoverySettings::default().with_required_role(role.clone()),
            None => CoordinatorDiscoverySettings::default(),
        };
        let discovery = crate::RegionCoordinatorDiscoveryConfig::new(
            discovery_settings,
            self.settings.registration_retry_interval,
        );
        let coordinator = match ClusterSingleton::get(&self.resources.system) {
            Ok(singletons) => {
                let coordinator_stop = Arc::new(Mutex::new(stop_message.clone()));
                let coordinator_remember_store = entity.coordinator_remember_store.clone();
                let handoff_timeout = self.settings.handoff_timeout;
                let stash_capacity = self.settings.region_buffer_capacity;
                let mut singleton = Singleton::new(
                    format!("sharding-coordinator-{type_name}"),
                    move || {
                        coordinator_props(
                            coordinator_stop
                                .lock()
                                .expect("sharding coordinator stop message poisoned")
                                .clone(),
                            handoff_timeout,
                            coordinator_remember_store.clone(),
                            stash_capacity,
                        )
                    },
                    ShardCoordinatorMsg::Terminate,
                );
                if let Some(role) = entity.coordinator_role.as_ref() {
                    singleton = singleton.with_role(role.clone());
                }
                let singleton = singletons.init_local(singleton)?;
                self.resources.system.spawn_system(
                    names.coordinator_name.clone(),
                    Props::new(move || SingletonCoordinatorEndpoint {
                        singleton: singleton.clone(),
                    }),
                )?
            }
            Err(ActorError::ExtensionNotRegistered(_)) => self.resources.system.spawn_system(
                names.coordinator_name.clone(),
                coordinator_props(
                    stop_message.clone(),
                    self.settings.handoff_timeout,
                    entity.coordinator_remember_store.clone(),
                    self.settings.region_buffer_capacity,
                ),
            )?,
            Err(error) => return Err(error.into()),
        };
        let discovery =
            discovery.with_local_coordinator(self.resources.self_node.clone(), coordinator.clone());
        let remote_coordinator_transport = RegionRemoteCoordinatorTransport::from_arc(
            region_wire.clone(),
            self.resources.registry.clone(),
            self.resources.outbound.clone(),
        );
        let handoff_timeout = self.settings.handoff_timeout;
        let region = match self.resources.system.spawn_system(
            names.region_name.clone(),
            Props::new({
                let self_region = region_wire.path().to_string();
                let factory = entity.factory.clone();
                let discovery = discovery.clone();
                let transport = remote_coordinator_transport.clone();
                let region_capacity = self.settings.region_buffer_capacity;
                let shard_capacity = self.settings.shard_buffer_capacity;
                let entity_type_name = type_name.clone();
                let ddata_remember_entities = entity.ddata_remember_entities.clone();
                move || {
                    let handoff_stop = Arc::new(Mutex::new(stop_message.clone()));
                    let region = match &ddata_remember_entities {
                        Some(settings) => ShardRegionActor::new_with_ddata_remember_entity_shards(
                            self_region.clone(),
                            entity_type_name.clone(),
                            region_capacity,
                            shard_capacity,
                            factory.clone(),
                            settings.shard_replica_id.clone(),
                            settings.shard_replicator.clone(),
                            settings.timeout,
                        ),
                        None => ShardRegionActor::new_with_local_entity_shards(
                            self_region.clone(),
                            region_capacity,
                            shard_capacity,
                            factory.clone(),
                        ),
                    };
                    region
                        .with_coordinator_discovery(discovery.clone())
                        .with_remote_coordinator_transport(transport.clone())
                        .with_region_route_transport(crate::RegionRouteTransport::new())
                        .with_remote_handoff_stop_message_factory(
                            move || {
                                handoff_stop
                                    .lock()
                                    .expect("sharding handoff stop message poisoned")
                                    .clone()
                            },
                            handoff_timeout,
                        )
                }
            }),
        ) {
            Ok(region) => region,
            Err(error) => {
                self.resources.system.stop(&coordinator);
                return Err(error.into());
            }
        };
        let router = match self.resources.system.spawn_system(
            names.router_name.clone(),
            ShardingEnvelopeRouter::props(region.clone(), entity.shard_count),
        ) {
            Ok(router) => router,
            Err(error) => {
                self.resources.system.stop(&region);
                self.resources.system.stop(&coordinator);
                return Err(error.into());
            }
        };
        let route_sink = match self.resources.system.spawn_system(
            names.route_sink_name.clone(),
            IgnoreActor::<RegionLocalRoutePlan<M>>::props(),
        ) {
            Ok(route_sink) => route_sink,
            Err(error) => {
                self.resources.system.stop(&router);
                self.resources.system.stop(&region);
                self.resources.system.stop(&coordinator);
                return Err(error.into());
            }
        };
        let delivery_sink = match self.resources.system.spawn_system(
            names.delivery_sink_name.clone(),
            IgnoreActor::<ShardDeliverPlan<M>>::props(),
        ) {
            Ok(delivery_sink) => delivery_sink,
            Err(error) => {
                self.resources.system.stop(&router);
                self.resources.system.stop(&region);
                self.resources.system.stop(&coordinator);
                self.resources.system.stop(&route_sink);
                return Err(error.into());
            }
        };

        let region_inbound = ShardRegionSystemInbound::new(region.clone())
            .with_routes(
                ShardRegionRemoteInbound::new(
                    self.resources.self_node.clone(),
                    self.resources.registry.clone(),
                    region.clone(),
                    route_sink.clone(),
                    delivery_sink.clone(),
                )
                .with_recipient_path(names.region_path.clone()),
            )
            .with_registration(ShardCoordinatorRemoteRegistrationInbound::new(
                region_wire.clone(),
                self.resources.registry.clone(),
            ))
            .with_shard_home(ShardCoordinatorRemoteHomeInbound::new(
                region_wire.clone(),
                self.resources.registry.clone(),
            ))
            .with_control(ShardRegionRemoteControlInbound::from_arc(
                region_wire.clone(),
                self.resources.registry.clone(),
                self.resources.outbound.clone(),
            ));
        let coordinator_inbound = crate::ShardCoordinatorSystemInbound::from_arc(
            coordinator.clone(),
            coordinator_wire.clone(),
            self.resources.registry.clone(),
            self.resources.outbound.clone(),
        );
        let region_path = region_wire.path().to_string();
        let coordinator_path = coordinator_wire.path().to_string();
        let region_route: Arc<InboundRoute> = Arc::new(move |envelope| {
            region_inbound
                .receive(envelope)
                .map_err(|error| RemoteError::Inbound(error.to_string()))
        });
        let coordinator_route: Arc<InboundRoute> = Arc::new(move |envelope| {
            coordinator_inbound
                .receive(envelope)
                .map_err(|error| RemoteError::Inbound(error.to_string()))
        });
        if let Err(error) = self
            .resources
            .inbound
            .insert(region_path.clone(), region_route)
        {
            self.stop_initialized_parts(
                &router,
                &region,
                &coordinator,
                &route_sink,
                &delivery_sink,
            );
            return Err(error);
        }
        if let Err(error) = self
            .resources
            .inbound
            .insert(coordinator_path.clone(), coordinator_route)
        {
            self.resources.inbound.remove(&region_path);
            self.stop_initialized_parts(
                &router,
                &region,
                &coordinator,
                &route_sink,
                &delivery_sink,
            );
            return Err(error);
        }

        let connector = match self.resources.system.spawn_system(
            names.connector_name.clone(),
            ClusterShardingConnector::<M>::props(ClusterShardingConnectorConfig {
                cluster: self.resources.cluster.clone(),
                self_node: self.resources.self_node.clone(),
                coordinator: coordinator.clone(),
                region: region.clone(),
                region_wire,
                coordinator_path: names.coordinator_path.clone(),
                region_path: names.region_path.clone(),
                registry: self.resources.registry.clone(),
                outbound: self.resources.outbound.clone(),
            }),
        ) {
            Ok(connector) => connector,
            Err(error) => {
                self.resources.inbound.remove(&region_path);
                self.resources.inbound.remove(&coordinator_path);
                self.stop_initialized_parts(
                    &router,
                    &region,
                    &coordinator,
                    &route_sink,
                    &delivery_sink,
                );
                return Err(error.into());
            }
        };
        if let Err(error) =
            self.register_shutdown_tasks(&names, &router, &region, &coordinator, &connector)
        {
            self.resources.inbound.remove(&region_path);
            self.resources.inbound.remove(&coordinator_path);
            self.resources.system.stop(&connector);
            self.stop_initialized_parts(
                &router,
                &region,
                &coordinator,
                &route_sink,
                &delivery_sink,
            );
            return Err(error.into());
        }
        entities.insert(
            type_name,
            Box::new(InitializedEntity {
                router: router.clone(),
                _region: region,
                _coordinator: coordinator,
                _connector: connector,
            }),
        );
        Ok(router)
    }

    pub fn entity_ref_for<M>(
        &self,
        type_key: EntityTypeKey<M>,
        entity_id: impl Into<String>,
    ) -> Result<EntityRef<M>, ClusterShardingBootstrapError>
    where
        M: Clone + RemoteMessage + Send + 'static,
    {
        let type_name = type_key.name().to_string();
        let entities = self.entities.lock().expect("sharded entities poisoned");
        let initialized = entities
            .get(&type_name)
            .ok_or_else(|| ClusterShardingBootstrapError::InvalidEntity {
                type_name: type_name.clone(),
                reason: "entity type is not initialized",
            })?
            .downcast_ref::<InitializedEntity<M>>()
            .ok_or_else(|| ClusterShardingBootstrapError::TypeMismatch {
                type_name: type_name.clone(),
            })?;
        Ok(EntityRef::new(entity_id, initialized.router.clone()))
    }

    fn stop_initialized_parts<M>(
        &self,
        router: &ActorRef<ShardingEnvelope<M>>,
        region: &ActorRef<ShardRegionMsg<M>>,
        coordinator: &ActorRef<ShardCoordinatorMsg<M>>,
        route_sink: &ActorRef<RegionLocalRoutePlan<M>>,
        delivery_sink: &ActorRef<ShardDeliverPlan<M>>,
    ) where
        M: Clone + Send + 'static,
    {
        self.resources.system.stop(router);
        self.resources.system.stop(region);
        self.resources.system.stop(coordinator);
        self.resources.system.stop(route_sink);
        self.resources.system.stop(delivery_sink);
    }

    fn register_shutdown_tasks<M>(
        &self,
        names: &EntitySystemNames,
        router: &ActorRef<ShardingEnvelope<M>>,
        region: &ActorRef<ShardRegionMsg<M>>,
        coordinator: &ActorRef<ShardCoordinatorMsg<M>>,
        connector: &ActorRef<ClusterShardingConnectorMsg>,
    ) -> Result<(), ActorError>
    where
        M: Clone + Send + 'static,
    {
        let shutdown = self.resources.system.coordinated_shutdown();
        let timeout = self.settings.shutdown_timeout;
        shutdown.add_actor_termination_task(
            PHASE_BEFORE_CLUSTER_SHUTDOWN,
            format!("{}-connector-stop", names.base),
            connector.clone(),
            None,
            timeout,
        )?;
        shutdown.add_actor_termination_task(
            PHASE_BEFORE_CLUSTER_SHUTDOWN,
            format!("{}-router-stop", names.base),
            router.clone(),
            None,
            timeout,
        )?;
        shutdown.add_actor_termination_task(
            PHASE_BEFORE_CLUSTER_SHUTDOWN,
            format!("{}-region-stop", names.base),
            region.clone(),
            None,
            timeout,
        )?;
        shutdown.add_actor_termination_task(
            PHASE_BEFORE_CLUSTER_SHUTDOWN,
            format!("{}-coordinator-stop", names.base),
            coordinator.clone(),
            None,
            timeout,
        )?;
        Ok(())
    }
}

fn coordinator_props<M>(
    stop_message: M,
    handoff_timeout: Duration,
    remember_store: Option<CoordinatorRememberStoreSettings>,
    stash_capacity: usize,
) -> Props<ShardCoordinatorActor<M>>
where
    M: Clone + Send + 'static,
{
    match remember_store {
        Some(CoordinatorRememberStoreSettings {
            target: CoordinatorRememberStoreTargetSettings::Actor(store),
            timeout,
        }) => ShardCoordinatorActor::props_with_remember_store_and_handoff(
            CoordinatorState::new(),
            LeastShardAllocationStrategy::default(),
            store,
            timeout,
            stop_message,
            handoff_timeout,
            crate::HandoffTransport::new(),
            stash_capacity,
        ),
        Some(CoordinatorRememberStoreSettings {
            target: CoordinatorRememberStoreTargetSettings::DistributedData(store),
            timeout,
        }) => ShardCoordinatorActor::props_with_ddata_remember_store_and_handoff(
            CoordinatorState::new(),
            LeastShardAllocationStrategy::default(),
            store,
            timeout,
            stop_message,
            handoff_timeout,
            crate::HandoffTransport::new(),
            stash_capacity,
        ),
        None => ShardCoordinatorActor::props_with_handoff(
            CoordinatorState::new(),
            LeastShardAllocationStrategy::default(),
            stop_message,
            handoff_timeout,
            crate::HandoffTransport::new(),
        ),
    }
}

fn validate_entity(
    type_name: &str,
    shard_count: u64,
    role: Option<&str>,
) -> Result<(), ClusterShardingBootstrapError> {
    if type_name.is_empty() {
        return Err(ClusterShardingBootstrapError::InvalidEntity {
            type_name: type_name.to_string(),
            reason: "type name must not be empty",
        });
    }
    if shard_count == 0 {
        return Err(ClusterShardingBootstrapError::InvalidEntity {
            type_name: type_name.to_string(),
            reason: "shard count must be greater than zero",
        });
    }
    if role.is_some_and(str::is_empty) {
        return Err(ClusterShardingBootstrapError::InvalidEntity {
            type_name: type_name.to_string(),
            reason: "coordinator role must not be empty",
        });
    }
    Ok(())
}

struct EntitySystemNames {
    base: String,
    coordinator_name: String,
    coordinator_path: String,
    region_name: String,
    region_path: String,
    router_name: String,
    connector_name: String,
    route_sink_name: String,
    delivery_sink_name: String,
}

impl EntitySystemNames {
    fn new(type_name: &str) -> Self {
        let encoded = type_name
            .as_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let base = format!("sharding-{encoded}");
        let coordinator_name = format!("{base}-coordinator");
        let region_name = format!("{base}-region");
        Self {
            coordinator_path: format!("/system/{coordinator_name}"),
            region_path: format!("/system/{region_name}"),
            router_name: format!("{base}-router"),
            connector_name: format!("{base}-cluster"),
            route_sink_name: format!("{base}-route-sink"),
            delivery_sink_name: format!("{base}-delivery-sink"),
            coordinator_name,
            region_name,
            base,
        }
    }
}

fn wire_ref(
    node: &UniqueAddress,
    path: &str,
    type_name: &str,
) -> Result<ActorRefWireData, ClusterShardingBootstrapError> {
    ActorRefWireData::new(format!("{}{path}", node.address)).map_err(|error| {
        ClusterShardingBootstrapError::WirePath {
            type_name: type_name.to_string(),
            reason: error.to_string(),
        }
    })
}

struct IgnoreActor<M> {
    _message: std::marker::PhantomData<fn(M)>,
}

impl<M> IgnoreActor<M>
where
    M: Send + 'static,
{
    fn props() -> Props<Self> {
        Props::new(|| Self {
            _message: std::marker::PhantomData,
        })
    }
}

impl<M> Actor for IgnoreActor<M>
where
    M: Send + 'static,
{
    type Msg = M;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, _msg: Self::Msg) -> ActorResult {
        Ok(())
    }
}

mod connector;

use connector::{
    ClusterShardingConnector, ClusterShardingConnectorConfig, ClusterShardingConnectorMsg,
};

#[cfg(test)]
mod tests;
