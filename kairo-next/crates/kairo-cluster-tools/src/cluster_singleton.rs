#![deny(missing_docs)]

use std::any::Any;
use std::collections::{HashMap, VecDeque, hash_map::Entry};
use std::fmt::{self, Display, Formatter};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use kairo_actor::{
    Actor, ActorError, ActorRef, ActorSystem, PHASE_CLUSTER_SHUTDOWN, Props, SendError,
};
use kairo_cluster::{Cluster, ClusterDaemonRegistration, ClusterExtension, UniqueAddress};
use kairo_remote::{
    RemoteError, RemoteOutbound, TcpRemoteActorRuntime, TcpRemoteActorRuntimeBuilder,
};
use kairo_serialization::{Registry, RemoteEnvelope, RemoteMessage, SerializedMessage};

use crate::{
    LocalSingletonManagerActor, LocalSingletonManagerRemoteInbound, SingletonHandOverDone,
    SingletonHandOverInProgress, SingletonHandOverToMe, SingletonManagerRemoteOutbound,
    SingletonManagerSettings, SingletonMessageEnvelope, SingletonProxyActor, SingletonProxyMsg,
    SingletonProxySettings, SingletonScope, SingletonTakeOverFromMe,
};

mod connector;

use connector::{
    ClusterSingletonConnector, ClusterSingletonConnectorConfig, SingletonRemoteTargetFactory,
    singleton_remote_target_factory,
};
pub use connector::{ClusterSingletonConnectorMsg, ClusterSingletonConnectorSnapshot};

/// Stable manifests carried on the reliable control lane for singleton handover.
pub const SINGLETON_SYSTEM_MANIFESTS: [&str; 4] = [
    SingletonHandOverToMe::MANIFEST,
    SingletonHandOverInProgress::MANIFEST,
    SingletonHandOverDone::MANIFEST,
    SingletonTakeOverFromMe::MANIFEST,
];

/// Stable manifests carried on the ordinary lane for singleton business messages.
pub const SINGLETON_MESSAGE_MANIFESTS: [&str; 1] = [SingletonMessageEnvelope::MANIFEST];

pub(crate) enum SingletonDeliveryMsg<M: Send + 'static> {
    Update(Option<ActorRef<M>>),
    Deliver(M),
}

pub(crate) struct SingletonDeliveryActor<M>
where
    M: Send + 'static,
{
    singleton: Option<ActorRef<M>>,
    buffer: VecDeque<M>,
    buffer_size: usize,
}

impl<M> Actor for SingletonDeliveryActor<M>
where
    M: Send + 'static,
{
    type Msg = SingletonDeliveryMsg<M>;

    fn receive(
        &mut self,
        _ctx: &mut kairo_actor::Context<Self::Msg>,
        msg: Self::Msg,
    ) -> kairo_actor::ActorResult {
        match msg {
            SingletonDeliveryMsg::Update(singleton) => {
                self.singleton = singleton;
                if let Some(singleton) = &self.singleton {
                    while let Some(message) = self.buffer.pop_front() {
                        let _ = singleton.tell(message);
                    }
                }
            }
            SingletonDeliveryMsg::Deliver(message) => {
                if let Some(singleton) = &self.singleton {
                    let _ = singleton.tell(message);
                } else if self.buffer_size > 0 {
                    if self.buffer.len() == self.buffer_size {
                        self.buffer.pop_front();
                    }
                    self.buffer.push_back(message);
                }
            }
        }
        Ok(())
    }
}

type InboundHandler =
    Arc<dyn Fn(RemoteEnvelope) -> Result<(), RemoteError> + Send + Sync + 'static>;
type MessageDecoder<M> =
    Arc<dyn Fn(SerializedMessage) -> Result<M, RemoteError> + Send + Sync + 'static>;

#[derive(Default)]
struct SingletonInboundRegistry {
    handlers: Mutex<HashMap<String, InboundHandler>>,
}

impl SingletonInboundRegistry {
    fn register(&self, recipient: String, handler: InboundHandler) -> Result<(), RemoteError> {
        let mut handlers = self
            .handlers
            .lock()
            .expect("cluster singleton inbound registry poisoned");
        match handlers.entry(recipient.clone()) {
            Entry::Vacant(entry) => {
                entry.insert(handler);
            }
            Entry::Occupied(_) => {
                return Err(RemoteError::Inbound(format!(
                    "cluster singleton inbound recipient `{recipient}` is already registered"
                )));
            }
        }
        Ok(())
    }

    fn unregister(&self, recipient: &str) {
        self.handlers
            .lock()
            .expect("cluster singleton inbound registry poisoned")
            .remove(recipient);
    }

    fn receive(&self, envelope: RemoteEnvelope) -> Result<(), RemoteError> {
        let recipient = envelope.recipient.path().to_string();
        let handler = self
            .handlers
            .lock()
            .expect("cluster singleton inbound registry poisoned")
            .get(&recipient)
            .cloned()
            .ok_or_else(|| {
                RemoteError::Inbound(format!(
                    "no cluster singleton manager is registered for `{recipient}`"
                ))
            })?;
        handler(envelope)
    }
}

#[derive(Debug, Clone)]
/// Settings shared by every singleton initialized through one extension.
///
/// Manager and proxy settings control handover and buffering. Route refreshes
/// reconcile the local singleton child with the proxy, while the shutdown
/// timeout bounds each actor-stop task installed in the cluster shutdown phase.
pub struct ClusterSingletonSettings {
    manager: SingletonManagerSettings,
    proxy: SingletonProxySettings,
    route_refresh_interval: Duration,
    shutdown_timeout: Duration,
}

impl Default for ClusterSingletonSettings {
    fn default() -> Self {
        Self {
            manager: SingletonManagerSettings::default(),
            proxy: SingletonProxySettings::default(),
            route_refresh_interval: Duration::from_millis(100),
            shutdown_timeout: Duration::from_secs(3),
        }
    }
}

impl ClusterSingletonSettings {
    /// Replaces the manager handover settings.
    pub fn with_manager_settings(mut self, settings: SingletonManagerSettings) -> Self {
        self.manager = settings;
        self
    }

    /// Replaces the proxy routing and buffering settings.
    pub fn with_proxy_settings(mut self, settings: SingletonProxySettings) -> Self {
        self.proxy = settings;
        self
    }

    /// Sets how often the membership connector refreshes the local route.
    pub fn with_route_refresh_interval(mut self, interval: Duration) -> Self {
        self.route_refresh_interval = interval;
        self
    }

    /// Sets the maximum wait for each singleton actor to stop during shutdown.
    pub fn with_shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    /// Returns the manager handover settings.
    pub fn manager_settings(&self) -> &SingletonManagerSettings {
        &self.manager
    }

    /// Returns the copyable proxy routing settings.
    pub fn proxy_settings(&self) -> SingletonProxySettings {
        self.proxy
    }

    /// Returns the local-route refresh interval.
    pub fn route_refresh_interval(&self) -> Duration {
        self.route_refresh_interval
    }

    /// Returns the per-actor coordinated-shutdown timeout.
    pub fn shutdown_timeout(&self) -> Duration {
        self.shutdown_timeout
    }

    fn validate(&self) -> Result<(), ClusterSingletonBootstrapError> {
        if self.route_refresh_interval.is_zero() {
            return Err(ClusterSingletonBootstrapError::InvalidSettings(
                "route refresh interval must be greater than zero",
            ));
        }
        if self.shutdown_timeout.is_zero() {
            return Err(ClusterSingletonBootstrapError::InvalidSettings(
                "shutdown timeout must be greater than zero",
            ));
        }
        Ok(())
    }
}

/// Definition of one named cluster-wide singleton actor.
///
/// The actor factory is invoked only on the oldest eligible member. The
/// termination message is sent before a graceful handover stops that local
/// instance. Names identify singleton registrations within an actor system.
pub struct Singleton<A>
where
    A: Actor,
{
    name: String,
    props: Arc<dyn Fn() -> Props<A> + Send + Sync>,
    termination_message: A::Msg,
    scope: SingletonScope,
}

impl<A> Singleton<A>
where
    A: Actor,
{
    /// Creates an all-members singleton definition.
    ///
    /// `props` must create a fresh actor for each ownership period.
    pub fn new<F>(name: impl Into<String>, props: F, termination_message: A::Msg) -> Self
    where
        F: Fn() -> Props<A> + Send + Sync + 'static,
    {
        Self {
            name: name.into(),
            props: Arc::new(props),
            termination_message,
            scope: SingletonScope::all(),
        }
    }

    /// Restricts ownership to cluster members carrying `role`.
    pub fn with_role(mut self, role: impl Into<String>) -> Self {
        self.scope = SingletonScope::for_role(role);
        self
    }

    /// Returns the logical singleton name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the role scope used for oldest-member selection.
    pub fn scope(&self) -> &SingletonScope {
        &self.scope
    }
}

/// Typed location-transparent reference to a named cluster singleton.
///
/// Messages enter a local proxy, which routes to the current owner or applies
/// its configured buffering policy while ownership is unavailable.
pub struct ClusterSingletonRef<M>
where
    M: Send + 'static,
{
    proxy: ActorRef<SingletonProxyMsg<M>>,
}

impl<M> Clone for ClusterSingletonRef<M>
where
    M: Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            proxy: self.proxy.clone(),
        }
    }
}

impl<M> ClusterSingletonRef<M>
where
    M: Send + 'static,
{
    /// Enqueues a message on the local singleton proxy.
    ///
    /// Success means the proxy accepted the message, not that a remote owner
    /// has processed it. A stopped or unavailable proxy returns the original
    /// message in [`SendError`].
    pub fn tell(&self, message: M) -> Result<(), SendError<M>> {
        self.proxy
            .tell(SingletonProxyMsg::Route(message))
            .map_err(|error| {
                let reason = error.reason().to_string();
                match error.into_message() {
                    SingletonProxyMsg::Route(message) => SendError::new(message, reason),
                    _ => unreachable!("cluster singleton ref only sends route messages"),
                }
            })
    }

    /// Returns the underlying proxy actor reference for low-level integration.
    pub fn proxy(&self) -> &ActorRef<SingletonProxyMsg<M>> {
        &self.proxy
    }
}

struct ClusterSingletonRuntime {
    system: ActorSystem,
    cluster: Cluster,
    self_node: UniqueAddress,
    registry: Arc<Registry>,
    outbound: Arc<dyn RemoteOutbound>,
    inbound: Arc<SingletonInboundRegistry>,
}

/// Actor-system extension that owns named cluster singleton registrations.
///
/// Install it with [`register_cluster_singleton`], bind the shared remote
/// runtime, and then call [`ClusterSingletonRegistration::activate`] before
/// retrieving or initializing singleton definitions.
pub struct ClusterSingleton {
    runtime: ClusterSingletonRuntime,
    settings: ClusterSingletonSettings,
    singletons: Mutex<HashMap<String, Box<dyn Any + Send + Sync>>>,
}

struct InitializedSingleton<M>
where
    M: Send + 'static,
{
    singleton_ref: ClusterSingletonRef<M>,
    remote_enabled: bool,
}

impl ClusterSingleton {
    /// Retrieves the activated singleton extension from `system`.
    pub fn get(system: &ActorSystem) -> Result<Arc<Self>, ActorError> {
        system.extension::<Self>()
    }

    /// Initializes a remotely routable singleton and returns its typed proxy.
    ///
    /// The message codec for `A::Msg` must already be present in the shared
    /// serialization registry. Repeating the same name and message type is
    /// idempotent; reusing the name with another type is rejected.
    pub fn init<A>(
        &self,
        singleton: Singleton<A>,
    ) -> Result<ClusterSingletonRef<A::Msg>, ClusterSingletonBootstrapError>
    where
        A: Actor,
        A::Msg: Clone + RemoteMessage,
    {
        let registry = self.runtime.registry.clone();
        let decoder: MessageDecoder<A::Msg> = Arc::new(move |message| {
            registry
                .deserialize::<A::Msg>(message)
                .map_err(|error| RemoteError::Inbound(error.to_string()))
        });
        let manager_path = manager_path_for(&singleton.name);
        let remote_targets = singleton_remote_target_factory(
            manager_path,
            self.runtime.registry.clone(),
            self.runtime.outbound.clone(),
        );
        self.init_with(singleton, Some(decoder), Some(remote_targets), true)
    }

    /// Runs a cluster-wide singleton whose local actor protocol is not a wire contract.
    ///
    /// Handover still uses the stable singleton control protocol. The returned
    /// proxy is useful to local integration actors; remote application traffic
    /// must use the owning subsystem's explicit wire adapter.
    pub fn init_local<A>(
        &self,
        singleton: Singleton<A>,
    ) -> Result<ClusterSingletonRef<A::Msg>, ClusterSingletonBootstrapError>
    where
        A: Actor,
        A::Msg: Clone,
    {
        self.init_with(singleton, None, None, false)
    }

    fn init_with<A>(
        &self,
        singleton: Singleton<A>,
        decoder: Option<MessageDecoder<A::Msg>>,
        remote_target_factory: Option<SingletonRemoteTargetFactory<A::Msg>>,
        remote_enabled: bool,
    ) -> Result<ClusterSingletonRef<A::Msg>, ClusterSingletonBootstrapError>
    where
        A: Actor,
        A::Msg: Clone,
    {
        if singleton.name.trim().is_empty() {
            return Err(ClusterSingletonBootstrapError::InvalidSingletonName);
        }
        if singleton
            .scope
            .role()
            .is_some_and(|role| role.trim().is_empty())
        {
            return Err(ClusterSingletonBootstrapError::InvalidSingletonRole);
        }
        let mut singletons = self
            .singletons
            .lock()
            .expect("cluster singleton extension poisoned");
        if let Some(existing) = singletons.get(&singleton.name) {
            let existing = existing
                .downcast_ref::<InitializedSingleton<A::Msg>>()
                .ok_or_else(|| ClusterSingletonBootstrapError::NameTypeConflict {
                    name: singleton.name.clone(),
                })?;
            if remote_enabled && !existing.remote_enabled {
                return Err(ClusterSingletonBootstrapError::RemoteModeConflict {
                    name: singleton.name.clone(),
                });
            }
            return Ok(existing.singleton_ref.clone());
        }

        let token = stable_name_token(&singleton.name);
        let manager_name = format!("singleton-{token:016x}-manager");
        let proxy_name = format!("singleton-{token:016x}-proxy");
        let connector_name = format!("singleton-{token:016x}-cluster");
        let delivery_name = format!("singleton-{token:016x}-delivery");
        let manager_path = manager_path_for(&singleton.name);
        let recipient = format!("{}{}", self.runtime.self_node.address, manager_path);

        let remote_effects = SingletonManagerRemoteOutbound::from_arc(
            self.runtime.self_node.clone(),
            self.runtime.registry.clone(),
            self.runtime.outbound.clone(),
        )
        .with_recipient_path(manager_path.clone());
        let props = Arc::clone(&singleton.props);
        let manager = self.runtime.system.spawn_system(
            &manager_name,
            LocalSingletonManagerActor::props_with_remote_effect_sink(
                self.runtime.self_node.clone(),
                "singleton",
                move || props(),
                singleton.termination_message,
                self.settings.manager.clone(),
                remote_effects,
            ),
        )?;

        let delivery = match self.runtime.system.spawn_system(
            &delivery_name,
            Props::new({
                let buffer_size = self.settings.proxy.buffer_size();
                move || SingletonDeliveryActor {
                    singleton: None,
                    buffer: VecDeque::new(),
                    buffer_size,
                }
            }),
        ) {
            Ok(delivery) => delivery,
            Err(error) => {
                self.runtime.system.stop(&manager);
                return Err(error.into());
            }
        };

        let inbound = LocalSingletonManagerRemoteInbound::new(
            self.runtime.self_node.clone(),
            self.runtime.registry.clone(),
            manager.clone(),
        )
        .with_recipient_path(manager_path.clone());
        if let Err(error) = self.runtime.inbound.register(
            recipient.clone(),
            Arc::new({
                let registry = self.runtime.registry.clone();
                let delivery = delivery.clone();
                let decoder = decoder.clone();
                move |envelope| {
                    if envelope.message.manifest.as_str() == SingletonMessageEnvelope::MANIFEST {
                        let decoder = decoder.as_ref().ok_or_else(|| {
                            RemoteError::Inbound(
                                "local-protocol cluster singleton rejects remote business messages"
                                    .to_string(),
                            )
                        })?;
                        let envelope = registry
                            .deserialize::<SingletonMessageEnvelope>(envelope.message)
                            .map_err(|error| RemoteError::Inbound(error.to_string()))?;
                        let message = decoder(envelope.message)?;
                        delivery
                            .tell(SingletonDeliveryMsg::Deliver(message))
                            .map_err(|error| RemoteError::Inbound(error.reason().to_string()))
                    } else {
                        inbound
                            .receive(envelope)
                            .map_err(|error| RemoteError::Inbound(error.to_string()))
                    }
                }
            }),
        ) {
            self.runtime.system.stop(&delivery);
            self.runtime.system.stop(&manager);
            return Err(error.into());
        }

        let proxy = match self
            .runtime
            .system
            .spawn_system(&proxy_name, SingletonProxyActor::props(self.settings.proxy))
        {
            Ok(proxy) => proxy,
            Err(error) => {
                self.runtime.inbound.unregister(&recipient);
                self.runtime.system.stop(&delivery);
                self.runtime.system.stop(&manager);
                return Err(error.into());
            }
        };
        let connector = match self.runtime.system.spawn_system(
            &connector_name,
            Props::new({
                let config = ClusterSingletonConnectorConfig {
                    cluster: self.runtime.cluster.clone(),
                    self_node: self.runtime.self_node.clone(),
                    scope: singleton.scope,
                    manager: manager.clone(),
                    proxy: proxy.clone(),
                    delivery: delivery.clone(),
                    remote_target_factory,
                    route_refresh_interval: self.settings.route_refresh_interval,
                };
                move || ClusterSingletonConnector::new(config.clone())
            }),
        ) {
            Ok(connector) => connector,
            Err(error) => {
                self.runtime.inbound.unregister(&recipient);
                self.runtime.system.stop(&delivery);
                self.runtime.system.stop(&proxy);
                self.runtime.system.stop(&manager);
                return Err(error.into());
            }
        };

        let timeout = self.settings.shutdown_timeout;
        add_forced_actor_stop_task(
            &self.runtime.system,
            format!("{connector_name}-stop"),
            &connector,
            timeout,
        )?;
        add_forced_actor_stop_task(
            &self.runtime.system,
            format!("{delivery_name}-stop"),
            &delivery,
            timeout,
        )?;
        add_forced_actor_stop_task(
            &self.runtime.system,
            format!("{proxy_name}-stop"),
            &proxy,
            timeout,
        )?;
        add_forced_actor_stop_task(
            &self.runtime.system,
            format!("{manager_name}-stop"),
            &manager,
            timeout,
        )?;

        let singleton_ref = ClusterSingletonRef { proxy };
        singletons.insert(
            singleton.name,
            Box::new(InitializedSingleton {
                singleton_ref: singleton_ref.clone(),
                remote_enabled,
            }),
        );
        Ok(singleton_ref)
    }
}

fn add_forced_actor_stop_task<M>(
    system: &ActorSystem,
    task_name: String,
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

/// Pre-bind registration token for the cluster singleton extension.
///
/// Clones share the same one-shot materialization and activation state.
pub struct ClusterSingletonRegistration {
    settings: ClusterSingletonSettings,
    runtime: Arc<Mutex<Option<ClusterSingletonRuntime>>>,
    activated: Arc<Mutex<bool>>,
}

impl Clone for ClusterSingletonRegistration {
    fn clone(&self) -> Self {
        Self {
            settings: self.settings.clone(),
            runtime: self.runtime.clone(),
            activated: self.activated.clone(),
        }
    }
}

impl ClusterSingletonRegistration {
    /// Activates the extension after the shared remote runtime has bound.
    ///
    /// Activation requires the cluster extension to be active on the same
    /// actor system. Repeated calls return the existing extension.
    pub fn activate(
        &self,
        runtime: &TcpRemoteActorRuntime,
    ) -> Result<Arc<ClusterSingleton>, ClusterSingletonBootstrapError> {
        ClusterExtension::get(runtime.system())?;
        let mut activated = self
            .activated
            .lock()
            .expect("cluster singleton activation poisoned");
        if !*activated {
            let materialized = self
                .runtime
                .lock()
                .expect("cluster singleton runtime poisoned")
                .take()
                .ok_or(ClusterSingletonBootstrapError::NotMaterialized)?;
            let settings = self.settings.clone();
            runtime
                .system()
                .register_extension(move |_| ClusterSingleton {
                    runtime: materialized,
                    settings,
                    singletons: Mutex::new(HashMap::new()),
                });
            *activated = true;
        }
        ClusterSingleton::get(runtime.system()).map_err(Into::into)
    }
}

#[derive(Debug)]
/// Failure returned while registering, activating, or initializing a singleton.
pub enum ClusterSingletonBootstrapError {
    /// The actor runtime rejected an extension, child, or shutdown task.
    Actor(ActorError),
    /// A shared extension setting was invalid.
    InvalidSettings(&'static str),
    /// The logical singleton name was empty or whitespace-only.
    InvalidSingletonName,
    /// The configured role was empty or whitespace-only.
    InvalidSingletonRole,
    /// A singleton name was already bound to another message type.
    NameTypeConflict {
        /// Conflicting logical singleton name.
        name: String,
    },
    /// A remotely routable initialization followed an existing local-only one.
    RemoteModeConflict {
        /// Conflicting logical singleton name.
        name: String,
    },
    /// Activation ran before the remote runtime materialized the registration.
    NotMaterialized,
    /// The shared remoting builder or inbound boundary rejected registration.
    Remote(RemoteError),
}

impl Display for ClusterSingletonBootstrapError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Actor(error) => write!(f, "{error}"),
            Self::InvalidSettings(reason) => {
                write!(f, "invalid cluster singleton settings: {reason}")
            }
            Self::InvalidSingletonName => write!(f, "cluster singleton name must not be blank"),
            Self::InvalidSingletonRole => write!(f, "cluster singleton role must not be blank"),
            Self::NameTypeConflict { name } => write!(
                f,
                "cluster singleton `{name}` was already initialized with another message type"
            ),
            Self::RemoteModeConflict { name } => write!(
                f,
                "cluster singleton `{name}` was initialized with a local-only protocol"
            ),
            Self::NotMaterialized => {
                write!(f, "cluster singleton runtime has not been materialized")
            }
            Self::Remote(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for ClusterSingletonBootstrapError {}

impl From<ActorError> for ClusterSingletonBootstrapError {
    fn from(error: ActorError) -> Self {
        Self::Actor(error)
    }
}

impl From<RemoteError> for ClusterSingletonBootstrapError {
    fn from(error: RemoteError) -> Self {
        Self::Remote(error)
    }
}

/// Registers singleton control and business-message handlers before TCP bind.
///
/// `cluster` must belong to the same remote runtime builder and must have been
/// registered first. Call [`ClusterSingletonRegistration::activate`] after the
/// builder binds to install the actor-system extension.
pub fn register_cluster_singleton(
    builder: &mut TcpRemoteActorRuntimeBuilder,
    cluster: ClusterDaemonRegistration,
    settings: ClusterSingletonSettings,
) -> Result<ClusterSingletonRegistration, ClusterSingletonBootstrapError> {
    settings.validate()?;
    let runtime = Arc::new(Mutex::new(None));
    let runtime_slot = runtime.clone();
    builder.register_reliable_control_handler(&SINGLETON_SYSTEM_MANIFESTS, move |context| {
        let cluster = cluster.handle().ok_or_else(|| {
            RemoteError::Inbound(
                "cluster daemon must be registered before cluster singleton".to_string(),
            )
        })?;
        let inbound = Arc::new(SingletonInboundRegistry::default());
        *runtime_slot
            .lock()
            .expect("cluster singleton runtime poisoned") = Some(ClusterSingletonRuntime {
            system: context.system().clone(),
            cluster: cluster.cluster().clone(),
            self_node: cluster.self_node().clone(),
            registry: context.registry().clone(),
            outbound: context.outbound().clone(),
            inbound: inbound.clone(),
        });
        Ok(move |envelope| inbound.receive(envelope))
    })?;
    let message_inbound = {
        let runtime = runtime.clone();
        move |_context: &kairo_remote::TcpRemoteActorRuntimeContext| {
            let inbound = runtime
                .lock()
                .expect("cluster singleton runtime poisoned")
                .as_ref()
                .map(|runtime| runtime.inbound.clone())
                .ok_or_else(|| {
                    RemoteError::Inbound(
                        "cluster singleton control runtime was not materialized".to_string(),
                    )
                })?;
            Ok(move |envelope| inbound.receive(envelope))
        }
    };
    builder.register_ordinary_handler(&SINGLETON_MESSAGE_MANIFESTS, message_inbound)?;
    Ok(ClusterSingletonRegistration {
        settings,
        runtime,
        activated: Arc::new(Mutex::new(false)),
    })
}

fn stable_name_token(name: &str) -> u64 {
    const OFFSET: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;
    name.as_bytes().iter().fold(OFFSET, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(PRIME)
    })
}

fn manager_path_for(name: &str) -> String {
    format!("/system/singleton-{:016x}-manager", stable_name_token(name))
}

#[cfg(test)]
mod tests;
