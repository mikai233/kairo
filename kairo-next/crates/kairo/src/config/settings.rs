use std::collections::BTreeMap;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct KairoSettings {
    pub actor: ActorConfig,
    pub remote: RemoteConfig,
    pub cluster: ClusterConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActorConfig {
    pub dispatchers: BTreeMap<String, DispatcherConfig>,
    pub mailboxes: BTreeMap<String, MailboxConfig>,
}

impl Default for ActorConfig {
    fn default() -> Self {
        Self {
            dispatchers: BTreeMap::from([("default".to_string(), DispatcherConfig::default())]),
            mailboxes: BTreeMap::from([("default".to_string(), MailboxConfig::default())]),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatcherConfig {
    pub throughput: usize,
}

impl Default for DispatcherConfig {
    fn default() -> Self {
        Self { throughput: 5 }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MailboxConfig {
    pub capacity: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RemoteConfig {
    pub transport: RemoteTransportConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteTransportConfig {
    pub canonical_hostname: String,
    pub canonical_port: u16,
}

impl Default for RemoteTransportConfig {
    fn default() -> Self {
        Self {
            canonical_hostname: "127.0.0.1".to_string(),
            canonical_port: 25520,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ClusterConfig {
    pub seed: ClusterSeedConfig,
    pub heartbeat: ClusterHeartbeatConfig,
    pub downing: ClusterDowningConfig,
    pub sharding: ClusterShardingConfig,
    pub tools: ClusterToolsConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ClusterSeedConfig {
    pub nodes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterHeartbeatConfig {
    pub monitored_by_nr_of_members: usize,
    pub interval: Duration,
    pub acceptable_pause: Duration,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterDowningConfig {
    pub strategy: ClusterDowningStrategyConfig,
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

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum ClusterDowningStrategyConfig {
    #[default]
    None,
    DownAll,
    KeepMajority {
        role: Option<String>,
    },
    KeepOldest {
        role: Option<String>,
        down_if_alone: bool,
    },
    LeaseMajority {
        lease_name: String,
        role: Option<String>,
        acquire_lease_delay_for_minority: Duration,
        release_after: Duration,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterShardingConfig {
    pub number_of_shards: u64,
    pub rebalance_interval: Duration,
}

impl Default for ClusterShardingConfig {
    fn default() -> Self {
        Self {
            number_of_shards: 100,
            rebalance_interval: Duration::from_secs(10),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterToolsConfig {
    pub singleton_role: Option<String>,
    pub pubsub_gossip_interval: Duration,
    pub pubsub_max_delta_entries: usize,
}

impl Default for ClusterToolsConfig {
    fn default() -> Self {
        Self {
            singleton_role: None,
            pubsub_gossip_interval: Duration::from_secs(1),
            pubsub_max_delta_entries: 1000,
        }
    }
}
