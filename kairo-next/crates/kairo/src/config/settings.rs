use std::collections::BTreeMap;
use std::time::Duration;

/// Format-neutral root settings for a Kairo actor system and optional
/// distributed subsystems.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct KairoSettings {
    /// Local actor-system settings.
    pub actor: ActorConfig,
    /// Remoting settings used when the `remote` feature is enabled.
    pub remote: RemoteConfig,
    /// Cluster, sharding, and cluster-tools settings.
    pub cluster: ClusterConfig,
    /// Diagnostic and observability settings.
    pub observability: ObservabilityConfig,
}

/// Local actor runtime settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActorConfig {
    /// Named dispatcher settings keyed by dispatcher id.
    pub dispatchers: BTreeMap<String, DispatcherConfig>,
    /// Named mailbox settings keyed by mailbox id.
    pub mailboxes: BTreeMap<String, MailboxConfig>,
    /// Executor used by actor-owned blocking helper tasks.
    pub task_executor: TaskExecutorConfig,
}

impl Default for ActorConfig {
    fn default() -> Self {
        Self {
            dispatchers: BTreeMap::from([("default".to_string(), DispatcherConfig::default())]),
            mailboxes: BTreeMap::from([("default".to_string(), MailboxConfig::default())]),
            task_executor: TaskExecutorConfig::default(),
        }
    }
}

/// Dispatcher throughput settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatcherConfig {
    /// Maximum user messages an actor worker processes before yielding.
    pub throughput: usize,
    /// Optional fixed worker count; absent uses the runtime default.
    pub workers: Option<usize>,
}

impl Default for DispatcherConfig {
    fn default() -> Self {
        Self {
            throughput: 5,
            workers: None,
        }
    }
}

/// Fixed worker-pool settings for actor-owned helper tasks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskExecutorConfig {
    /// Optional fixed worker count; absent uses the runtime default.
    pub workers: Option<usize>,
    /// Maximum number of accepted tasks waiting for a worker.
    pub queue_capacity: usize,
}

impl Default for TaskExecutorConfig {
    fn default() -> Self {
        Self {
            workers: None,
            queue_capacity: 1_024,
        }
    }
}

/// Mailbox capacity settings.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MailboxConfig {
    /// Optional bounded capacity for the user-message lane.
    pub capacity: Option<usize>,
}

/// Remoting settings.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RemoteConfig {
    /// Transport-level address and connection settings.
    pub transport: RemoteTransportConfig,
}

/// TCP remoting transport settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteTransportConfig {
    /// Hostname or IP address advertised as this node's canonical remote host.
    pub canonical_hostname: String,
    /// TCP port advertised as this node's canonical remote port.
    pub canonical_port: u16,
    /// Optional timeout for outbound TCP association attempts.
    pub connect_timeout: Option<std::time::Duration>,
}

impl Default for RemoteTransportConfig {
    fn default() -> Self {
        Self {
            canonical_hostname: "127.0.0.1".to_string(),
            canonical_port: 25520,
            connect_timeout: None,
        }
    }
}

/// Cluster-family settings grouped under one format-neutral root.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ClusterConfig {
    /// Seed/contact points used to discover existing cluster members.
    pub seed: ClusterSeedConfig,
    /// Failure-detector and heartbeat timing settings.
    pub heartbeat: ClusterHeartbeatConfig,
    /// Downing strategy settings.
    pub downing: ClusterDowningConfig,
    /// Cluster sharding settings.
    pub sharding: ClusterShardingConfig,
    /// Cluster singleton and pubsub settings.
    pub tools: ClusterToolsConfig,
}

/// Cluster seed/contact point settings.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ClusterSeedConfig {
    /// Remote actor-system addresses used as contact points.
    pub nodes: Vec<String>,
}

/// Cluster heartbeat and failure-detector timing settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterHeartbeatConfig {
    /// Number of members each node should monitor.
    pub monitored_by_nr_of_members: usize,
    /// Heartbeat send interval.
    pub interval: Duration,
    /// Pause tolerated by the failure detector before suspicion.
    pub acceptable_pause: Duration,
    /// Expected response window used by heartbeat senders.
    pub expected_response_after: Duration,
}

impl Default for ClusterHeartbeatConfig {
    fn default() -> Self {
        Self {
            monitored_by_nr_of_members: 5,
            interval: Duration::from_secs(1),
            acceptable_pause: Duration::from_secs(3),
            expected_response_after: Duration::from_secs(1),
        }
    }
}

/// Cluster downing settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterDowningConfig {
    /// Strategy used to convert unreachable observations into downing actions.
    pub strategy: ClusterDowningStrategyConfig,
    /// Stability window before the configured strategy may down members.
    pub stable_after: Duration,
}

impl Default for ClusterDowningConfig {
    fn default() -> Self {
        Self {
            strategy: ClusterDowningStrategyConfig::None,
            stable_after: Duration::from_secs(20),
        }
    }
}

/// Format-neutral cluster downing strategy selection.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum ClusterDowningStrategyConfig {
    /// Disable automatic downing.
    #[default]
    None,
    /// Down all observed unreachable members after the stability window.
    DownAll,
    /// Keep the majority side, optionally restricted to a role.
    KeepMajority {
        /// Optional role used when calculating majority.
        role: Option<String>,
    },
    /// Keep the oldest member side, optionally restricted to a role.
    KeepOldest {
        /// Optional role used when selecting the oldest side.
        role: Option<String>,
        /// Whether the oldest member may down itself when isolated alone.
        down_if_alone: bool,
    },
    /// Keep the side that can acquire an external majority lease.
    LeaseMajority {
        /// Lease name supplied to the caller-provided lease implementation.
        lease_name: String,
        /// Optional role used when calculating lease majority.
        role: Option<String>,
        /// Delay before the minority side attempts to acquire the lease.
        acquire_lease_delay_for_minority: Duration,
        /// Delay before releasing a previously acquired lease.
        release_after: Duration,
    },
}

/// Cluster sharding settings.
#[derive(Debug, Clone, PartialEq)]
pub struct ClusterShardingConfig {
    /// Total number of logical shards for each entity type.
    pub number_of_shards: u64,
    /// Whether remembered entity recovery is enabled.
    pub remember_entities: bool,
    /// Least-shard allocation and rebalance limits.
    pub least_shard_allocation: ClusterShardingAllocationConfig,
    /// Retry interval for coordinator and region operations.
    pub retry_interval: Duration,
    /// Timeout for shard handoff operations.
    pub handoff_timeout: Duration,
    /// Backoff applied before restarting failed shard actors.
    pub shard_failure_backoff: Duration,
    /// Interval between periodic rebalance attempts.
    pub rebalance_interval: Duration,
    /// Timeout for shard-region query operations.
    pub shard_region_query_timeout: Duration,
}

impl Default for ClusterShardingConfig {
    fn default() -> Self {
        Self {
            number_of_shards: 100,
            remember_entities: false,
            least_shard_allocation: ClusterShardingAllocationConfig::default(),
            retry_interval: Duration::from_secs(2),
            handoff_timeout: Duration::from_secs(60),
            shard_failure_backoff: Duration::from_secs(10),
            rebalance_interval: Duration::from_secs(10),
            shard_region_query_timeout: Duration::from_secs(3),
        }
    }
}

/// Least-shard allocation strategy settings.
#[derive(Debug, Clone, PartialEq)]
pub struct ClusterShardingAllocationConfig {
    /// Maximum number of shards to rebalance in one decision.
    pub rebalance_absolute_limit: usize,
    /// Maximum rebalance fraction relative to the current shard count.
    pub rebalance_relative_limit: f64,
}

impl Default for ClusterShardingAllocationConfig {
    fn default() -> Self {
        Self {
            rebalance_absolute_limit: 10,
            rebalance_relative_limit: 0.1,
        }
    }
}

/// Cluster singleton and pubsub settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterToolsConfig {
    /// Optional role that may host the singleton.
    pub singleton_role: Option<String>,
    /// Singleton handover retry interval while a node is becoming oldest.
    pub singleton_hand_over_retry_interval: Duration,
    /// Pubsub gossip tick interval.
    pub pubsub_gossip_interval: Duration,
    /// Maximum pubsub delta entries sent in one gossip update.
    pub pubsub_max_delta_entries: usize,
}

impl Default for ClusterToolsConfig {
    fn default() -> Self {
        Self {
            singleton_role: None,
            singleton_hand_over_retry_interval: Duration::from_secs(1),
            pubsub_gossip_interval: Duration::from_secs(1),
            pubsub_max_delta_entries: 1000,
        }
    }
}

/// Observability settings for backend-neutral diagnostics.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ObservabilityConfig {
    /// Diagnostic category switches.
    pub diagnostics: DiagnosticsConfig,
}

/// Backend-neutral diagnostic category switches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiagnosticsConfig {
    /// Publish local dead letters to the actor-system event stream.
    pub dead_letters: bool,
    /// Record remote delivery failures.
    pub remote_delivery_failures: bool,
    /// Record remote serialization failures.
    pub serialization_failures: bool,
    /// Record association quarantine events.
    pub quarantine_events: bool,
    /// Record association close and shutdown events.
    pub association_close_events: bool,
    /// Record cluster gossip state changes.
    pub gossip_state_changes: bool,
}

impl Default for DiagnosticsConfig {
    fn default() -> Self {
        Self {
            dead_letters: true,
            remote_delivery_failures: true,
            serialization_failures: true,
            quarantine_events: true,
            association_close_events: true,
            gossip_state_changes: true,
        }
    }
}
