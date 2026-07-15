#![deny(missing_docs)]

use std::fmt::{self, Display, Formatter};
use std::time::Duration;

use kairo_actor::{ActorError, ActorRef, ActorSystem, PHASE_BEFORE_CLUSTER_SHUTDOWN, Props};
use kairo_remote::{RemoteAssociationAddress, RemoteAssociationCache, RemoteError, RemoteSettings};

use crate::{
    Cluster, ClusterSystemInbound, ClusterTcpPeerConnector, ClusterTcpPeerConnectorMsg,
    ClusterTcpPeerConnectorSettings, ClusterTcpPeerRuntime, UniqueAddress,
};

const DEFAULT_CONNECTOR_NAME: &str = "cluster-tcp-peer-connector";
const DEFAULT_SHUTDOWN_TASK_NAME: &str = "cluster-tcp-peer-connector-stop";
const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Debug)]
/// Failure while binding the TCP peer runtime or installing its connector actor.
pub enum ClusterTcpPeerBootstrapError {
    /// Listener bind or remote-runtime setup failed.
    Remote(RemoteError),
    /// Connector spawn or coordinated-shutdown registration failed.
    Actor(ActorError),
}

impl Display for ClusterTcpPeerBootstrapError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Remote(error) => write!(f, "{error}"),
            Self::Actor(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for ClusterTcpPeerBootstrapError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Remote(error) => Some(error),
            Self::Actor(error) => Some(error),
        }
    }
}

impl From<RemoteError> for ClusterTcpPeerBootstrapError {
    fn from(error: RemoteError) -> Self {
        Self::Remote(error)
    }
}

impl From<ActorError> for ClusterTcpPeerBootstrapError {
    fn from(error: ActorError) -> Self {
        Self::Actor(error)
    }
}

/// Result of composing a membership-driven TCP peer runtime.
pub type ClusterTcpPeerBootstrapResult<T> = Result<T, ClusterTcpPeerBootstrapError>;

#[derive(Debug, Clone, PartialEq, Eq)]
/// Distinct cluster-member and remoting-system incarnation identifiers used at bind.
pub struct ClusterTcpPeerBootstrapIdentity {
    node_uid: u64,
    local_system_uid: u64,
}

impl ClusterTcpPeerBootstrapIdentity {
    /// Creates bootstrap identity from the cluster node UID and local remoting system UID.
    pub fn new(node_uid: u64, local_system_uid: u64) -> Self {
        Self {
            node_uid,
            local_system_uid,
        }
    }

    /// Returns the UID embedded in the local cluster `UniqueAddress`.
    pub fn node_uid(&self) -> u64 {
        self.node_uid
    }

    /// Returns the UID advertised by the local remoting association handshake.
    pub fn local_system_uid(&self) -> u64 {
        self.local_system_uid
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Transport, connector, and coordinated-shutdown settings for TCP peer composition.
pub struct ClusterTcpPeerBootstrapSettings {
    remote_settings: RemoteSettings,
    connector_name: String,
    connector_settings: ClusterTcpPeerConnectorSettings,
    shutdown_phase: String,
    shutdown_task_name: String,
    shutdown_timeout: Duration,
}

impl ClusterTcpPeerBootstrapSettings {
    /// Creates settings with the default connector name, retry policy, and shutdown task.
    pub fn new(remote_settings: RemoteSettings) -> Self {
        Self {
            remote_settings,
            connector_name: DEFAULT_CONNECTOR_NAME.to_string(),
            connector_settings: ClusterTcpPeerConnectorSettings::default(),
            shutdown_phase: PHASE_BEFORE_CLUSTER_SHUTDOWN.to_string(),
            shutdown_task_name: DEFAULT_SHUTDOWN_TASK_NAME.to_string(),
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
        }
    }

    /// Sets the system-actor name used for the TCP peer connector.
    pub fn with_connector_name(mut self, name: impl Into<String>) -> Self {
        self.connector_name = name.into();
        self
    }

    /// Sets retry scheduling for the TCP peer connector.
    pub fn with_connector_settings(mut self, settings: ClusterTcpPeerConnectorSettings) -> Self {
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

    /// Returns listener and outbound connection settings.
    pub fn remote_settings(&self) -> &RemoteSettings {
        &self.remote_settings
    }

    /// Returns the system-actor name used for the connector.
    pub fn connector_name(&self) -> &str {
        &self.connector_name
    }

    /// Returns the connector retry policy.
    pub fn connector_settings(&self) -> &ClusterTcpPeerConnectorSettings {
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

/// Handle to a spawned membership-driven TCP peer connector and its bound identity.
///
/// The connector owns the runtime after spawn. Stopping it unsubscribes from cluster events,
/// clears pending work, closes managed routes, and shuts down the listener.
pub struct ClusterTcpPeerBootstrap {
    connector: ActorRef<ClusterTcpPeerConnectorMsg>,
    self_node: UniqueAddress,
    local_address: RemoteAssociationAddress,
}

impl ClusterTcpPeerBootstrap {
    /// Binds a TCP peer runtime, spawns its cluster connector, and registers coordinated shutdown.
    pub fn bind_and_spawn(
        system: &ActorSystem,
        cluster: Cluster,
        identity: ClusterTcpPeerBootstrapIdentity,
        settings: ClusterTcpPeerBootstrapSettings,
        inbound: impl FnOnce(UniqueAddress, RemoteAssociationCache) -> ClusterSystemInbound,
    ) -> ClusterTcpPeerBootstrapResult<Self> {
        let runtime = ClusterTcpPeerRuntime::bind(
            system.name().to_string(),
            identity.node_uid(),
            identity.local_system_uid(),
            settings.remote_settings().clone(),
            inbound,
        )?;
        Self::spawn_with_runtime(system, cluster, runtime, settings)
    }

    /// Spawns a connector around an already-bound runtime and registers coordinated shutdown.
    ///
    /// If shutdown registration fails, the newly spawned connector is stopped and allowed to
    /// release its owned runtime before the error is returned.
    pub fn spawn_with_runtime(
        system: &ActorSystem,
        cluster: Cluster,
        runtime: ClusterTcpPeerRuntime,
        settings: ClusterTcpPeerBootstrapSettings,
    ) -> ClusterTcpPeerBootstrapResult<Self> {
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
                ClusterTcpPeerConnector::with_settings(cluster, runtime, connector_settings)
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
        })
    }

    /// Returns the connector actor that owns membership subscription and runtime commands.
    pub fn connector(&self) -> &ActorRef<ClusterTcpPeerConnectorMsg> {
        &self.connector
    }

    /// Returns the canonical local cluster member identity captured at bind.
    pub fn self_node(&self) -> &UniqueAddress {
        &self.self_node
    }

    /// Returns the canonical local transport address captured at bind.
    pub fn local_address(&self) -> &RemoteAssociationAddress {
        &self.local_address
    }
}

fn register_connector_shutdown(
    system: &ActorSystem,
    connector: &ActorRef<ClusterTcpPeerConnectorMsg>,
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
                    "cluster tcp peer connector shutdown timed out".to_string(),
                ))
            }
        })
}

#[cfg(test)]
mod tests;
