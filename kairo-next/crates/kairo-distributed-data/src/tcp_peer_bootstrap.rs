use std::fmt::{self, Display, Formatter};
use std::sync::Arc;
use std::time::Duration;

use kairo_actor::{ActorError, ActorRef, ActorSystem, PHASE_BEFORE_CLUSTER_SHUTDOWN, Props};
use kairo_cluster::{Cluster, UniqueAddress};
use kairo_remote::{RemoteAssociationAddress, RemoteError, RemoteSettings};

use crate::{
    ReplicaId, ReplicatorRemoteReplyReceiver, ReplicatorRemoteRequestReceiver,
    ReplicatorTcpPeerConnector, ReplicatorTcpPeerConnectorMsg, ReplicatorTcpPeerConnectorSettings,
    ReplicatorTcpPeerRuntime, ReplicatorTcpPeerRuntimeSettings,
};

const DEFAULT_CONNECTOR_NAME: &str = "ddata-tcp-peer-connector";
const DEFAULT_SHUTDOWN_TASK_NAME: &str = "ddata-tcp-peer-connector-stop";
const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Debug)]
pub enum ReplicatorTcpPeerBootstrapError {
    Remote(RemoteError),
    Actor(ActorError),
}

impl Display for ReplicatorTcpPeerBootstrapError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Remote(error) => write!(f, "{error}"),
            Self::Actor(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for ReplicatorTcpPeerBootstrapError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Remote(error) => Some(error),
            Self::Actor(error) => Some(error),
        }
    }
}

impl From<RemoteError> for ReplicatorTcpPeerBootstrapError {
    fn from(error: RemoteError) -> Self {
        Self::Remote(error)
    }
}

impl From<ActorError> for ReplicatorTcpPeerBootstrapError {
    fn from(error: ActorError) -> Self {
        Self::Actor(error)
    }
}

pub type ReplicatorTcpPeerBootstrapResult<T> = Result<T, ReplicatorTcpPeerBootstrapError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorTcpPeerBootstrapIdentity {
    node_uid: u64,
    local_system_uid: u64,
    remote_replica: ReplicaId,
}

impl ReplicatorTcpPeerBootstrapIdentity {
    pub fn new(node_uid: u64, local_system_uid: u64, remote_replica: ReplicaId) -> Self {
        Self {
            node_uid,
            local_system_uid,
            remote_replica,
        }
    }

    pub fn node_uid(&self) -> u64 {
        self.node_uid
    }

    pub fn local_system_uid(&self) -> u64 {
        self.local_system_uid
    }

    pub fn remote_replica(&self) -> &ReplicaId {
        &self.remote_replica
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorTcpPeerBootstrapSettings {
    runtime_settings: ReplicatorTcpPeerRuntimeSettings,
    connector_name: String,
    connector_settings: ReplicatorTcpPeerConnectorSettings,
    shutdown_phase: String,
    shutdown_task_name: String,
    shutdown_timeout: Duration,
}

impl ReplicatorTcpPeerBootstrapSettings {
    pub fn new(remote: RemoteSettings) -> Self {
        Self {
            runtime_settings: ReplicatorTcpPeerRuntimeSettings::new(remote),
            connector_name: DEFAULT_CONNECTOR_NAME.to_string(),
            connector_settings: ReplicatorTcpPeerConnectorSettings::default(),
            shutdown_phase: PHASE_BEFORE_CLUSTER_SHUTDOWN.to_string(),
            shutdown_task_name: DEFAULT_SHUTDOWN_TASK_NAME.to_string(),
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
        }
    }

    pub fn with_runtime_settings(mut self, settings: ReplicatorTcpPeerRuntimeSettings) -> Self {
        self.runtime_settings = settings;
        self
    }

    pub fn with_connector_name(mut self, name: impl Into<String>) -> Self {
        self.connector_name = name.into();
        self
    }

    pub fn with_connector_settings(mut self, settings: ReplicatorTcpPeerConnectorSettings) -> Self {
        self.connector_settings = settings;
        self
    }

    pub fn with_shutdown_phase(mut self, phase: impl Into<String>) -> Self {
        self.shutdown_phase = phase.into();
        self
    }

    pub fn with_shutdown_task_name(mut self, task_name: impl Into<String>) -> Self {
        self.shutdown_task_name = task_name.into();
        self
    }

    pub fn with_shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    pub fn runtime_settings(&self) -> &ReplicatorTcpPeerRuntimeSettings {
        &self.runtime_settings
    }

    pub fn connector_name(&self) -> &str {
        &self.connector_name
    }

    pub fn connector_settings(&self) -> &ReplicatorTcpPeerConnectorSettings {
        &self.connector_settings
    }

    pub fn shutdown_phase(&self) -> &str {
        &self.shutdown_phase
    }

    pub fn shutdown_task_name(&self) -> &str {
        &self.shutdown_task_name
    }

    pub fn shutdown_timeout(&self) -> Duration {
        self.shutdown_timeout
    }
}

pub struct ReplicatorTcpPeerBootstrap {
    connector: ActorRef<ReplicatorTcpPeerConnectorMsg>,
    self_node: UniqueAddress,
    local_address: RemoteAssociationAddress,
}

impl ReplicatorTcpPeerBootstrap {
    pub fn bind_and_spawn(
        system: &ActorSystem,
        cluster: Cluster,
        identity: ReplicatorTcpPeerBootstrapIdentity,
        settings: ReplicatorTcpPeerBootstrapSettings,
        requests: Arc<dyn ReplicatorRemoteRequestReceiver>,
        replies: Arc<dyn ReplicatorRemoteReplyReceiver>,
    ) -> ReplicatorTcpPeerBootstrapResult<Self> {
        let runtime = ReplicatorTcpPeerRuntime::bind_with_settings(
            system.name().to_string(),
            identity.node_uid,
            identity.local_system_uid,
            identity.remote_replica,
            settings.runtime_settings().clone(),
            requests,
            replies,
        )?;
        Self::spawn_with_runtime(system, cluster, runtime, settings)
    }

    pub fn spawn_with_runtime(
        system: &ActorSystem,
        cluster: Cluster,
        runtime: ReplicatorTcpPeerRuntime,
        settings: ReplicatorTcpPeerBootstrapSettings,
    ) -> ReplicatorTcpPeerBootstrapResult<Self> {
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
                ReplicatorTcpPeerConnector::with_settings(cluster, runtime, connector_settings)
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

    pub fn connector(&self) -> &ActorRef<ReplicatorTcpPeerConnectorMsg> {
        &self.connector
    }

    pub fn self_node(&self) -> &UniqueAddress {
        &self.self_node
    }

    pub fn local_address(&self) -> &RemoteAssociationAddress {
        &self.local_address
    }
}

fn register_connector_shutdown(
    system: &ActorSystem,
    connector: &ActorRef<ReplicatorTcpPeerConnectorMsg>,
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
                    "distributed-data tcp peer connector shutdown timed out".to_string(),
                ))
            }
        })
}

#[cfg(test)]
mod tests;
