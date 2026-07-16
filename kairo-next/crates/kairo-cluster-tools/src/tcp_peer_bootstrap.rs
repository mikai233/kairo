#![deny(missing_docs)]

use std::fmt::{self, Display, Formatter};
use std::time::Duration;

use kairo_actor::{ActorError, ActorRef, ActorSystem, PHASE_BEFORE_CLUSTER_SHUTDOWN, Props};
use kairo_cluster::{Cluster, UniqueAddress};
use kairo_remote::{RemoteAssociationAddress, RemoteError, RemoteSettings};
use kairo_serialization::RemoteMessage;

use crate::{
    ClusterToolsSystemInbound, ClusterToolsTcpPeerConnector, ClusterToolsTcpPeerConnectorMsg,
    ClusterToolsTcpPeerConnectorSettings, ClusterToolsTcpPeerRuntime,
};

const DEFAULT_CONNECTOR_NAME: &str = "cluster-tools-tcp-peer-connector";
const DEFAULT_SHUTDOWN_TASK_NAME: &str = "cluster-tools-tcp-peer-connector-stop";
const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Debug)]
/// Failure while binding the cluster-tools TCP runtime or installing its connector actor.
pub enum ClusterToolsTcpPeerBootstrapError {
    /// Listener bind or remote-runtime setup failed.
    Remote(RemoteError),
    /// Connector spawn or coordinated-shutdown registration failed.
    Actor(ActorError),
}

impl Display for ClusterToolsTcpPeerBootstrapError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Remote(error) => write!(f, "{error}"),
            Self::Actor(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for ClusterToolsTcpPeerBootstrapError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Remote(error) => Some(error),
            Self::Actor(error) => Some(error),
        }
    }
}

impl From<RemoteError> for ClusterToolsTcpPeerBootstrapError {
    fn from(error: RemoteError) -> Self {
        Self::Remote(error)
    }
}

impl From<ActorError> for ClusterToolsTcpPeerBootstrapError {
    fn from(error: ActorError) -> Self {
        Self::Actor(error)
    }
}

/// Result of composing a membership-driven cluster-tools TCP peer runtime.
pub type ClusterToolsTcpPeerBootstrapResult<T> = Result<T, ClusterToolsTcpPeerBootstrapError>;

#[derive(Debug, Clone, PartialEq, Eq)]
/// Connector and coordinated-shutdown settings for cluster-tools TCP peer composition.
pub struct ClusterToolsTcpPeerBootstrapSettings {
    connector_name: String,
    connector_settings: ClusterToolsTcpPeerConnectorSettings,
    shutdown_phase: String,
    shutdown_task_name: String,
    shutdown_timeout: Duration,
}

impl ClusterToolsTcpPeerBootstrapSettings {
    /// Creates settings with the default connector name, retry policy, and shutdown task.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the system-actor name used for the TCP peer connector.
    pub fn with_connector_name(mut self, name: impl Into<String>) -> Self {
        self.connector_name = name.into();
        self
    }

    /// Sets retry scheduling for the TCP peer connector.
    pub fn with_connector_settings(
        mut self,
        settings: ClusterToolsTcpPeerConnectorSettings,
    ) -> Self {
        self.connector_settings = settings;
        self
    }

    /// Sets the coordinated-shutdown phase that stops the connector and owned runtime.
    pub fn with_shutdown_phase(mut self, phase: impl Into<String>) -> Self {
        self.shutdown_phase = phase.into();
        self
    }

    /// Sets the coordinated-shutdown task name.
    pub fn with_shutdown_task_name(mut self, task_name: impl Into<String>) -> Self {
        self.shutdown_task_name = task_name.into();
        self
    }

    /// Sets how long coordinated shutdown waits for the connector actor to stop.
    pub fn with_shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    /// Returns the system-actor name used for the connector.
    pub fn connector_name(&self) -> &str {
        &self.connector_name
    }

    /// Returns the connector retry policy.
    pub fn connector_settings(&self) -> &ClusterToolsTcpPeerConnectorSettings {
        &self.connector_settings
    }

    /// Returns the coordinated-shutdown phase that owns connector stop.
    pub fn shutdown_phase(&self) -> &str {
        &self.shutdown_phase
    }

    /// Returns the coordinated-shutdown task name.
    pub fn shutdown_task_name(&self) -> &str {
        &self.shutdown_task_name
    }

    /// Returns the connector stop deadline used by coordinated shutdown.
    pub fn shutdown_timeout(&self) -> Duration {
        self.shutdown_timeout
    }
}

impl Default for ClusterToolsTcpPeerBootstrapSettings {
    fn default() -> Self {
        Self {
            connector_name: DEFAULT_CONNECTOR_NAME.to_string(),
            connector_settings: ClusterToolsTcpPeerConnectorSettings::default(),
            shutdown_phase: PHASE_BEFORE_CLUSTER_SHUTDOWN.to_string(),
            shutdown_task_name: DEFAULT_SHUTDOWN_TASK_NAME.to_string(),
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
        }
    }
}

/// Handle to a spawned membership-driven cluster-tools connector and bound identity.
///
/// The connector owns the runtime after spawn. Stopping it unsubscribes from
/// cluster events, clears pending work, closes managed routes, and shuts down
/// the listener.
pub struct ClusterToolsTcpPeerBootstrap<M>
where
    M: RemoteMessage + Send + 'static,
{
    connector: ActorRef<ClusterToolsTcpPeerConnectorMsg>,
    self_node: UniqueAddress,
    local_address: RemoteAssociationAddress,
    _message: std::marker::PhantomData<M>,
}

impl<M> ClusterToolsTcpPeerBootstrap<M>
where
    M: RemoteMessage + Send + 'static,
{
    /// Binds a TCP peer runtime, spawns its cluster connector, and registers shutdown.
    ///
    /// # Errors
    ///
    /// Returns an error when transport binding, connector spawn, or
    /// coordinated-shutdown registration fails.
    pub fn bind_and_spawn(
        system: &ActorSystem,
        cluster: Cluster,
        node_uid: u64,
        local_system_uid: u64,
        remote_settings: RemoteSettings,
        settings: ClusterToolsTcpPeerBootstrapSettings,
        inbound: impl FnOnce(UniqueAddress) -> ClusterToolsSystemInbound<M>,
    ) -> ClusterToolsTcpPeerBootstrapResult<Self> {
        let runtime = ClusterToolsTcpPeerRuntime::bind(
            system.name().to_string(),
            node_uid,
            local_system_uid,
            remote_settings,
            inbound,
        )?;
        Self::spawn_with_runtime(system, cluster, runtime, settings)
    }

    /// Spawns a connector around an already-bound runtime and registers shutdown.
    ///
    /// If shutdown registration fails, the newly spawned connector is stopped
    /// and allowed to release its owned runtime before the error is returned.
    ///
    /// # Errors
    ///
    /// Returns an error when connector spawn or coordinated-shutdown
    /// registration fails.
    pub fn spawn_with_runtime(
        system: &ActorSystem,
        cluster: Cluster,
        runtime: ClusterToolsTcpPeerRuntime<M>,
        settings: ClusterToolsTcpPeerBootstrapSettings,
    ) -> ClusterToolsTcpPeerBootstrapResult<Self> {
        let self_node = runtime.self_node().clone();
        let local_address = runtime.local_address().clone();
        let connector_name = settings.connector_name().to_string();
        let connector_settings = settings.connector_settings().clone();
        let shutdown_phase = settings.shutdown_phase().to_string();
        let shutdown_task_name = settings.shutdown_task_name().to_string();
        let shutdown_timeout = settings.shutdown_timeout();
        let connector = system.spawn_system(
            connector_name,
            Props::new(move || {
                ClusterToolsTcpPeerConnector::with_settings(cluster, runtime, connector_settings)
            }),
        )?;
        if let Err(error) = register_connector_shutdown(
            system,
            &connector,
            &shutdown_phase,
            &shutdown_task_name,
            shutdown_timeout,
        ) {
            system.stop(&connector);
            let _ = connector.wait_for_stop(shutdown_timeout);
            return Err(error.into());
        }
        Ok(Self {
            connector,
            self_node,
            local_address,
            _message: std::marker::PhantomData,
        })
    }

    /// Returns the system actor that owns membership subscription and the runtime.
    pub fn connector(&self) -> &ActorRef<ClusterToolsTcpPeerConnectorMsg> {
        &self.connector
    }

    /// Returns the canonical local cluster identity derived during bind.
    pub fn self_node(&self) -> &UniqueAddress {
        &self.self_node
    }

    /// Returns the canonical local association address derived during bind.
    pub fn local_address(&self) -> &RemoteAssociationAddress {
        &self.local_address
    }
}

fn register_connector_shutdown(
    system: &ActorSystem,
    connector: &ActorRef<ClusterToolsTcpPeerConnectorMsg>,
    phase: &str,
    task_name: &str,
    timeout: Duration,
) -> Result<(), ActorError> {
    let system = system.clone();
    let connector = connector.clone();
    system
        .coordinated_shutdown()
        .add_task(phase, task_name, move || {
            system.stop(&connector);
            if connector.wait_for_stop(timeout) {
                Ok(())
            } else {
                Err(ActorError::ShutdownTaskFailed(
                    "cluster-tools tcp peer connector shutdown timed out".to_string(),
                ))
            }
        })
}

#[cfg(test)]
mod tests;
