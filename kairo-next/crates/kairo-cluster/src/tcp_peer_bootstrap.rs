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
pub enum ClusterTcpPeerBootstrapError {
    Remote(RemoteError),
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

pub type ClusterTcpPeerBootstrapResult<T> = Result<T, ClusterTcpPeerBootstrapError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterTcpPeerBootstrapIdentity {
    node_uid: u64,
    local_system_uid: u64,
}

impl ClusterTcpPeerBootstrapIdentity {
    pub fn new(node_uid: u64, local_system_uid: u64) -> Self {
        Self {
            node_uid,
            local_system_uid,
        }
    }

    pub fn node_uid(&self) -> u64 {
        self.node_uid
    }

    pub fn local_system_uid(&self) -> u64 {
        self.local_system_uid
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterTcpPeerBootstrapSettings {
    remote_settings: RemoteSettings,
    connector_name: String,
    connector_settings: ClusterTcpPeerConnectorSettings,
    shutdown_phase: String,
    shutdown_task_name: String,
    shutdown_timeout: Duration,
}

impl ClusterTcpPeerBootstrapSettings {
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

    pub fn with_connector_name(mut self, name: impl Into<String>) -> Self {
        self.connector_name = name.into();
        self
    }

    pub fn with_connector_settings(mut self, settings: ClusterTcpPeerConnectorSettings) -> Self {
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

    pub fn remote_settings(&self) -> &RemoteSettings {
        &self.remote_settings
    }

    pub fn connector_name(&self) -> &str {
        &self.connector_name
    }

    pub fn connector_settings(&self) -> &ClusterTcpPeerConnectorSettings {
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

pub struct ClusterTcpPeerBootstrap {
    connector: ActorRef<ClusterTcpPeerConnectorMsg>,
    self_node: UniqueAddress,
    local_address: RemoteAssociationAddress,
}

impl ClusterTcpPeerBootstrap {
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
        let connector = system.spawn(
            connector_name,
            Props::new(move || {
                ClusterTcpPeerConnector::with_settings(cluster, runtime, connector_settings)
            }),
        )?;
        register_connector_shutdown(
            system,
            &connector,
            &shutdown_phase,
            &shutdown_task_name,
            shutdown_timeout,
        )?;
        Ok(Self {
            connector,
            self_node,
            local_address,
        })
    }

    pub fn connector(&self) -> &ActorRef<ClusterTcpPeerConnectorMsg> {
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
    connector: &ActorRef<ClusterTcpPeerConnectorMsg>,
    phase: &str,
    task_name: &str,
    timeout: Duration,
) -> Result<(), ActorError> {
    system.coordinated_shutdown().add_actor_termination_task(
        phase,
        task_name,
        connector.clone(),
        None,
        timeout,
    )
}

#[cfg(test)]
mod tests {
    use kairo_actor::{Address, PHASE_BEFORE_CLUSTER_SHUTDOWN};
    use kairo_testkit::{ActorSystemTestKit, await_assert};

    use super::*;
    use crate::{
        ClusterEventPublisher, ClusterEventPublisherMsg, ClusterTcpPeerConnectorSettings,
        ClusterTcpPeerConnectorSnapshot, Gossip, Member, MemberStatus,
    };

    #[test]
    fn bootstrap_binds_connector_and_registers_coordinated_shutdown_stop() {
        let kit = ActorSystemTestKit::new("cluster-peer-bootstrap").unwrap();
        let publisher_node = UniqueAddress::new(Address::local("cluster-peer-bootstrap"), 1);
        let publisher = kit
            .system()
            .spawn(
                "publisher",
                Props::new(move || ClusterEventPublisher::new(publisher_node.clone())),
            )
            .unwrap();
        let cluster = Cluster::new(publisher);
        let settings = ClusterTcpPeerBootstrapSettings::new(RemoteSettings::new("127.0.0.1", 0))
            .with_connector_name("cluster-peer")
            .with_connector_settings(
                ClusterTcpPeerConnectorSettings::new(Duration::from_millis(25)).unwrap(),
            )
            .with_shutdown_timeout(Duration::from_secs(1));
        let identity = ClusterTcpPeerBootstrapIdentity::new(1, 11);

        let bootstrap = ClusterTcpPeerBootstrap::bind_and_spawn(
            kit.system(),
            cluster,
            identity,
            settings,
            |self_node, _cache| ClusterSystemInbound::new(self_node),
        )
        .unwrap();

        assert_eq!(bootstrap.self_node().uid, 1);
        assert_eq!(bootstrap.local_address().system(), "cluster-peer-bootstrap");
        assert!(!bootstrap.connector().is_stopped());

        kit.system()
            .coordinated_shutdown()
            .run_from("test", Some(PHASE_BEFORE_CLUSTER_SHUTDOWN))
            .unwrap();

        assert!(bootstrap.connector().wait_for_stop(Duration::from_secs(1)));
        kit.shutdown(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn bootstrap_two_nodes_install_peer_routes_from_cluster_membership() {
        let sender_kit = ActorSystemTestKit::new("cluster-bootstrap-sender").unwrap();
        let receiver_kit = ActorSystemTestKit::new("cluster-bootstrap-receiver").unwrap();
        let sender_runtime = bind_runtime("cluster-bootstrap-sender", 1, 11);
        let receiver_runtime = bind_runtime("cluster-bootstrap-receiver", 2, 22);
        let sender_node = sender_runtime.self_node().clone();
        let receiver_node = receiver_runtime.self_node().clone();
        let sender_publisher =
            spawn_publisher(&sender_kit, "sender-publisher", sender_node.clone());
        let receiver_publisher =
            spawn_publisher(&receiver_kit, "receiver-publisher", receiver_node.clone());
        let sender_cluster = Cluster::new(sender_publisher.clone());
        let receiver_cluster = Cluster::new(receiver_publisher.clone());
        let settings = ClusterTcpPeerBootstrapSettings::new(RemoteSettings::new("127.0.0.1", 0))
            .with_connector_settings(
                ClusterTcpPeerConnectorSettings::new(Duration::from_millis(25))
                    .unwrap()
                    .with_automatic_retry_ticks(false),
            );

        let sender_bootstrap = ClusterTcpPeerBootstrap::spawn_with_runtime(
            sender_kit.system(),
            sender_cluster,
            sender_runtime,
            settings.clone().with_connector_name("sender-cluster-peer"),
        )
        .unwrap();
        let receiver_bootstrap = ClusterTcpPeerBootstrap::spawn_with_runtime(
            receiver_kit.system(),
            receiver_cluster,
            receiver_runtime,
            settings.with_connector_name("receiver-cluster-peer"),
        )
        .unwrap();
        let sender_snapshots = sender_kit
            .create_probe::<ClusterTcpPeerConnectorSnapshot>("sender-snapshots")
            .unwrap();
        let receiver_snapshots = receiver_kit
            .create_probe::<ClusterTcpPeerConnectorSnapshot>("receiver-snapshots")
            .unwrap();

        let gossip = Gossip::from_members([
            Member::new(sender_node.clone(), Vec::new()).with_status(MemberStatus::Up),
            Member::new(receiver_node.clone(), Vec::new()).with_status(MemberStatus::Up),
        ]);
        sender_publisher
            .tell(ClusterEventPublisherMsg::PublishChanges(gossip.clone()))
            .unwrap();
        receiver_publisher
            .tell(ClusterEventPublisherMsg::PublishChanges(gossip))
            .unwrap();

        await_connector_route(
            sender_bootstrap.connector(),
            &sender_snapshots,
            &receiver_node,
        );
        await_connector_route(
            receiver_bootstrap.connector(),
            &receiver_snapshots,
            &sender_node,
        );

        sender_kit.shutdown(Duration::from_secs(1)).unwrap();
        receiver_kit.shutdown(Duration::from_secs(1)).unwrap();
    }

    fn bind_runtime(system: &str, node_uid: u64, system_uid: u64) -> ClusterTcpPeerRuntime {
        ClusterTcpPeerRuntime::bind(
            system,
            node_uid,
            system_uid,
            RemoteSettings::new("127.0.0.1", 0),
            |self_node, _cache| ClusterSystemInbound::new(self_node),
        )
        .unwrap()
    }

    fn spawn_publisher(
        kit: &ActorSystemTestKit,
        name: &str,
        self_node: UniqueAddress,
    ) -> ActorRef<ClusterEventPublisherMsg> {
        kit.system()
            .spawn(
                name,
                Props::new(move || ClusterEventPublisher::new(self_node.clone())),
            )
            .unwrap()
    }

    fn await_connector_route(
        connector: &ActorRef<ClusterTcpPeerConnectorMsg>,
        snapshots: &kairo_testkit::TestProbe<ClusterTcpPeerConnectorSnapshot>,
        expected_peer: &UniqueAddress,
    ) {
        await_assert(
            Duration::from_secs(1),
            Duration::from_millis(10),
            || -> Result<(), String> {
                connector
                    .tell(ClusterTcpPeerConnectorMsg::Snapshot {
                        reply_to: snapshots.actor_ref(),
                    })
                    .map_err(|error| error.reason().to_string())?;
                let snapshot = snapshots
                    .expect_msg(Duration::from_millis(100))
                    .map_err(|error| error.to_string())?;
                if snapshot.route_count == 1
                    && snapshot
                        .active_targets
                        .iter()
                        .any(|target| target.node() == expected_peer)
                {
                    Ok(())
                } else {
                    Err(format!("unexpected connector snapshot: {snapshot:?}"))
                }
            },
        )
        .unwrap();
    }
}
