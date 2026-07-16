#![deny(missing_docs)]

use std::fmt::{self, Display, Formatter};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use kairo_actor::{ActorError, ActorRef, ActorSystem, PHASE_BEFORE_CLUSTER_SHUTDOWN, Props};
use kairo_cluster::{ClusterDaemonRegistration, ClusterExtension, UniqueAddress};
use kairo_remote::{RemoteError, TcpRemoteActorRuntime, TcpRemoteActorRuntimeBuilder};
use kairo_serialization::RemoteMessage;

use crate::{
    ClusterToolsSystemInbound, DistributedPubSubMediatorActor, DistributedPubSubMediatorMsg,
    PubSubDelta, PubSubGossipActor, PubSubGossipMsg, PubSubGossipWireInbound, PubSubPathEnvelope,
    PubSubPublishEnvelope, PubSubRemoteDeliveryInbound, PubSubStatus,
};

mod connector;

use connector::{DistributedPubSubConnector, DistributedPubSubConnectorConfig};
pub use connector::{DistributedPubSubConnectorMsg, DistributedPubSubConnectorSnapshot};

/// Stable manifests handled by the composed distributed-pubsub endpoint.
pub const PUBSUB_SYSTEM_MANIFESTS: [&str; 4] = [
    PubSubStatus::MANIFEST,
    PubSubDelta::MANIFEST,
    PubSubPublishEnvelope::MANIFEST,
    PubSubPathEnvelope::MANIFEST,
];

/// Settings for one typed distributed-pubsub extension.
///
/// The role filters participating members, the gossip interval drives periodic
/// status exchange, and the delta limit bounds registry entries per response.
/// The shutdown timeout bounds each actor termination task installed before
/// cluster shutdown.
#[derive(Debug, Clone)]
pub struct DistributedPubSubSettings {
    gossip_interval: Duration,
    max_delta_entries: usize,
    role: Option<String>,
    shutdown_timeout: Duration,
}

impl Default for DistributedPubSubSettings {
    fn default() -> Self {
        Self {
            gossip_interval: Duration::from_secs(1),
            max_delta_entries: 1_000,
            role: None,
            shutdown_timeout: Duration::from_secs(3),
        }
    }
}

impl DistributedPubSubSettings {
    /// Sets the fixed-delay interval between pubsub gossip ticks.
    pub fn with_gossip_interval(mut self, interval: Duration) -> Self {
        self.gossip_interval = interval;
        self
    }

    /// Sets the maximum registry entries emitted in one gossip delta.
    pub fn with_max_delta_entries(mut self, max: usize) -> Self {
        self.max_delta_entries = max;
        self
    }

    /// Restricts peer participation to members carrying `role`.
    pub fn with_role(mut self, role: impl Into<String>) -> Self {
        self.role = Some(role.into());
        self
    }

    /// Sets the maximum wait for each pubsub actor to stop during shutdown.
    pub fn with_shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    /// Returns the fixed-delay gossip interval.
    pub fn gossip_interval(&self) -> Duration {
        self.gossip_interval
    }

    /// Returns the maximum number of entries in one outgoing delta.
    pub fn max_delta_entries(&self) -> usize {
        self.max_delta_entries
    }

    /// Returns the required cluster role, if configured.
    pub fn role(&self) -> Option<&str> {
        self.role.as_deref()
    }

    /// Returns the per-actor coordinated-shutdown timeout.
    pub fn shutdown_timeout(&self) -> Duration {
        self.shutdown_timeout
    }

    fn validate(&self) -> Result<(), DistributedPubSubBootstrapError> {
        if self.gossip_interval.is_zero() {
            return Err(DistributedPubSubBootstrapError::InvalidSettings(
                "gossip interval must be greater than zero",
            ));
        }
        if self.max_delta_entries == 0 {
            return Err(DistributedPubSubBootstrapError::InvalidSettings(
                "max delta entries must be greater than zero",
            ));
        }
        if self
            .role
            .as_ref()
            .is_some_and(|role| role.trim().is_empty())
        {
            return Err(DistributedPubSubBootstrapError::InvalidSettings(
                "role must not be blank",
            ));
        }
        if self.shutdown_timeout.is_zero() {
            return Err(DistributedPubSubBootstrapError::InvalidSettings(
                "shutdown timeout must be greater than zero",
            ));
        }
        Ok(())
    }
}

/// Typed handles materialized for one distributed-pubsub protocol.
///
/// The actors are created when the shared remote runtime binds. Activation
/// installs coordinated-shutdown tasks and the actor-system extension but does
/// not replace these references.
pub struct DistributedPubSubHandle<M>
where
    M: Send + 'static,
{
    self_node: UniqueAddress,
    mediator: ActorRef<DistributedPubSubMediatorMsg<M>>,
    gossip: ActorRef<PubSubGossipMsg>,
    connector: ActorRef<DistributedPubSubConnectorMsg>,
}

impl<M> Clone for DistributedPubSubHandle<M>
where
    M: Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            self_node: self.self_node.clone(),
            mediator: self.mediator.clone(),
            gossip: self.gossip.clone(),
            connector: self.connector.clone(),
        }
    }
}

impl<M> DistributedPubSubHandle<M>
where
    M: Send + 'static,
{
    /// Returns the effective local cluster incarnation used by pubsub gossip.
    pub fn self_node(&self) -> &UniqueAddress {
        &self.self_node
    }

    /// Returns the typed mediator used for subscriptions and delivery.
    pub fn mediator(&self) -> &ActorRef<DistributedPubSubMediatorMsg<M>> {
        &self.mediator
    }

    /// Returns the registry-gossip actor.
    pub fn gossip(&self) -> &ActorRef<PubSubGossipMsg> {
        &self.gossip
    }

    /// Returns the membership connector for diagnostics and explicit snapshots.
    pub fn connector(&self) -> &ActorRef<DistributedPubSubConnectorMsg> {
        &self.connector
    }
}

/// Actor-system extension for one remote-capable pubsub message protocol.
///
/// The generic parameter keeps mediator traffic typed; it is not erased into a
/// global application-message enum at the remote boundary.
pub struct DistributedPubSubExtension<M>
where
    M: Send + 'static,
{
    handle: DistributedPubSubHandle<M>,
}

impl<M> DistributedPubSubExtension<M>
where
    M: Send + 'static,
{
    /// Retrieves the activated extension for `M` from `system`.
    pub fn get(system: &ActorSystem) -> Result<Arc<Self>, ActorError> {
        system.extension::<Self>()
    }

    /// Returns the typed distributed-pubsub mediator.
    pub fn mediator(&self) -> &ActorRef<DistributedPubSubMediatorMsg<M>> {
        self.handle.mediator()
    }

    /// Returns the effective local cluster incarnation.
    pub fn self_node(&self) -> &UniqueAddress {
        self.handle.self_node()
    }

    /// Returns the registry-gossip actor.
    pub fn gossip(&self) -> &ActorRef<PubSubGossipMsg> {
        self.handle.gossip()
    }

    /// Returns the membership connector for diagnostics and snapshots.
    pub fn connector(&self) -> &ActorRef<DistributedPubSubConnectorMsg> {
        self.handle.connector()
    }
}

/// Pre-bind registration token for one typed distributed-pubsub extension.
///
/// Clones share materialization and one-shot activation state. Only one
/// registration may own the stable pubsub manifests and `/system/pubsub` path
/// in a remote runtime builder.
pub struct DistributedPubSubRegistration<M>
where
    M: Send + 'static,
{
    settings: DistributedPubSubSettings,
    handle: Arc<Mutex<Option<DistributedPubSubHandle<M>>>>,
    activated: Arc<Mutex<bool>>,
}

impl<M> Clone for DistributedPubSubRegistration<M>
where
    M: Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            settings: self.settings.clone(),
            handle: Arc::clone(&self.handle),
            activated: Arc::clone(&self.activated),
        }
    }
}

impl<M> DistributedPubSubRegistration<M>
where
    M: Clone + RemoteMessage + Send + 'static,
{
    /// Returns the bind-time actor handles once the runtime has materialized.
    ///
    /// This is `None` before a successful remote runtime bind.
    pub fn handle(&self) -> Option<DistributedPubSubHandle<M>> {
        self.handle
            .lock()
            .expect("distributed-pubsub handle poisoned")
            .clone()
    }

    /// Installs shutdown ownership and the actor-system extension after bind.
    ///
    /// The cluster extension must already be active on the same actor system.
    /// Repeated calls are idempotent and return the same materialized handles.
    pub fn activate(
        &self,
        runtime: &TcpRemoteActorRuntime,
    ) -> Result<DistributedPubSubHandle<M>, DistributedPubSubBootstrapError> {
        ClusterExtension::get(runtime.system())?;
        let handle = self
            .handle()
            .ok_or(DistributedPubSubBootstrapError::NotMaterialized)?;
        let mut activated = self
            .activated
            .lock()
            .expect("distributed-pubsub activation poisoned");
        if !*activated {
            let shutdown = runtime.system().coordinated_shutdown();
            let timeout = self.settings.shutdown_timeout;
            shutdown.add_actor_termination_task(
                PHASE_BEFORE_CLUSTER_SHUTDOWN,
                "distributed-pubsub-connector-stop",
                handle.connector.clone(),
                None,
                timeout,
            )?;
            shutdown.add_actor_termination_task(
                PHASE_BEFORE_CLUSTER_SHUTDOWN,
                "distributed-pubsub-mediator-stop",
                handle.mediator.clone(),
                None,
                timeout,
            )?;
            shutdown.add_actor_termination_task(
                PHASE_BEFORE_CLUSTER_SHUTDOWN,
                "distributed-pubsub-gossip-stop",
                handle.gossip.clone(),
                None,
                timeout,
            )?;
            let extension_handle = handle.clone();
            runtime
                .system()
                .register_extension(move |_| DistributedPubSubExtension {
                    handle: extension_handle,
                });
            *activated = true;
        }
        Ok(handle)
    }
}

/// Failure returned while registering or activating distributed pubsub.
#[derive(Debug)]
pub enum DistributedPubSubBootstrapError {
    /// The actor runtime rejected a child, extension, or shutdown task.
    Actor(ActorError),
    /// A composed pubsub setting was invalid.
    InvalidSettings(&'static str),
    /// Activation ran before the remote runtime materialized the actor graph.
    NotMaterialized,
    /// The shared remoting builder or inbound boundary rejected registration.
    Remote(RemoteError),
}

impl Display for DistributedPubSubBootstrapError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Actor(error) => write!(f, "{error}"),
            Self::InvalidSettings(reason) => {
                write!(f, "invalid distributed-pubsub settings: {reason}")
            }
            Self::NotMaterialized => write!(f, "distributed pubsub has not been materialized"),
            Self::Remote(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for DistributedPubSubBootstrapError {}

impl From<ActorError> for DistributedPubSubBootstrapError {
    fn from(error: ActorError) -> Self {
        Self::Actor(error)
    }
}

impl From<RemoteError> for DistributedPubSubBootstrapError {
    fn from(error: RemoteError) -> Self {
        Self::Remote(error)
    }
}

/// Registers a typed pubsub endpoint on the shared remote runtime before bind.
///
/// The cluster daemon registration must belong to the same builder and must be
/// registered first. Bind materializes the mediator, gossip actor, membership
/// connector, and stable inbound routes; call
/// [`DistributedPubSubRegistration::activate`] afterward to install extension
/// and coordinated-shutdown ownership.
pub fn register_distributed_pubsub<M>(
    builder: &mut TcpRemoteActorRuntimeBuilder,
    cluster: ClusterDaemonRegistration,
    settings: DistributedPubSubSettings,
) -> Result<DistributedPubSubRegistration<M>, DistributedPubSubBootstrapError>
where
    M: Clone + RemoteMessage + Send + 'static,
{
    settings.validate()?;
    let handle = Arc::new(Mutex::new(None));
    let factory_handle = Arc::clone(&handle);
    let factory_settings = settings.clone();
    builder.register_control_handler(&PUBSUB_SYSTEM_MANIFESTS, move |context| {
        let cluster = cluster.handle().ok_or_else(|| {
            RemoteError::Inbound(
                "cluster daemon must be registered before distributed pubsub".to_string(),
            )
        })?;
        let self_node = cluster.self_node().clone();
        let gossip = context
            .system()
            .spawn_system(
                "pubsub-gossip",
                Props::new({
                    let self_node = self_node.clone();
                    let max = factory_settings.max_delta_entries;
                    move || PubSubGossipActor::new(self_node.clone()).with_max_delta_entries(max)
                }),
            )
            .map_err(|error| RemoteError::Inbound(error.to_string()))?;
        let mediator = match context.system().spawn_system(
            "pubsub",
            Props::new({
                let self_node = self_node.clone();
                let gossip = gossip.clone();
                move || {
                    DistributedPubSubMediatorActor::new(self_node.clone())
                        .with_gossip(gossip.clone())
                }
            }),
        ) {
            Ok(mediator) => mediator,
            Err(error) => {
                context.system().stop(&gossip);
                return Err(RemoteError::Inbound(error.to_string()));
            }
        };
        let connector = match context.system().spawn_system(
            "pubsub-cluster",
            Props::new({
                let config = DistributedPubSubConnectorConfig {
                    cluster: cluster.cluster().clone(),
                    self_node: self_node.clone(),
                    role: factory_settings.role.clone(),
                    gossip_interval: factory_settings.gossip_interval,
                    registry: context.registry().clone(),
                    outbound: context.outbound().clone(),
                    gossip: gossip.clone(),
                    mediator: mediator.clone(),
                };
                move || DistributedPubSubConnector::new(config.clone())
            }),
        ) {
            Ok(connector) => connector,
            Err(error) => {
                context.system().stop(&mediator);
                context.system().stop(&gossip);
                return Err(RemoteError::Inbound(error.to_string()));
            }
        };
        *factory_handle
            .lock()
            .expect("distributed-pubsub handle poisoned") = Some(DistributedPubSubHandle {
            self_node: self_node.clone(),
            mediator: mediator.clone(),
            gossip: gossip.clone(),
            connector,
        });
        let inbound = ClusterToolsSystemInbound::new(self_node.clone())
            .with_pubsub_gossip(PubSubGossipWireInbound::new(
                self_node.clone(),
                context.registry().clone(),
                gossip,
            ))
            .with_pubsub_delivery(PubSubRemoteDeliveryInbound::new(
                self_node,
                context.registry().clone(),
                mediator,
            ));
        Ok(move |envelope| {
            inbound
                .receive(envelope)
                .map_err(|error| RemoteError::Inbound(error.to_string()))
        })
    })?;
    Ok(DistributedPubSubRegistration {
        settings,
        handle,
        activated: Arc::new(Mutex::new(false)),
    })
}

impl<M> Clone for DistributedPubSubConnectorConfig<M>
where
    M: Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            cluster: self.cluster.clone(),
            self_node: self.self_node.clone(),
            role: self.role.clone(),
            gossip_interval: self.gossip_interval,
            registry: self.registry.clone(),
            outbound: self.outbound.clone(),
            gossip: self.gossip.clone(),
            mediator: self.mediator.clone(),
        }
    }
}

#[cfg(test)]
mod tests;
