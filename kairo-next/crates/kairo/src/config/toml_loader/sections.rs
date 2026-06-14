use std::collections::BTreeMap;
use std::time::Duration;

use toml::Value;

use crate::config::{
    ActorConfig, ClusterConfig, ClusterDowningConfig, ClusterDowningStrategyConfig,
    ClusterHeartbeatConfig, ClusterSeedConfig, ClusterShardingConfig, ClusterToolsConfig,
    ConfigError, DispatcherConfig, MailboxConfig, RemoteConfig, RemoteTransportConfig,
};

use super::primitives::{
    expect_table, optional_bool, optional_duration, optional_non_empty_string, parse_duration,
    parse_string, parse_string_array, parse_u64, parse_usize, reject_unknown, reject_zero_duration,
};

pub(super) fn parse_actor(value: &Value) -> Result<ActorConfig, ConfigError> {
    let table = expect_table(value, "actor")?;
    reject_unknown(table, "actor", &["dispatchers", "mailboxes"])?;
    let mut config = ActorConfig::default();
    if let Some(dispatchers) = table.get("dispatchers") {
        config.dispatchers = parse_dispatchers(dispatchers)?;
    }
    if let Some(mailboxes) = table.get("mailboxes") {
        config.mailboxes = parse_mailboxes(mailboxes)?;
    }
    Ok(config)
}

pub(super) fn parse_remote(value: &Value) -> Result<RemoteConfig, ConfigError> {
    let table = expect_table(value, "remote")?;
    reject_unknown(table, "remote", &["transport"])?;
    let mut config = RemoteConfig::default();
    if let Some(transport) = table.get("transport") {
        config.transport = parse_remote_transport(transport)?;
    }
    Ok(config)
}

pub(super) fn parse_cluster(value: &Value) -> Result<ClusterConfig, ConfigError> {
    let table = expect_table(value, "cluster")?;
    reject_unknown(
        table,
        "cluster",
        &["seed", "heartbeat", "downing", "sharding", "tools"],
    )?;
    let mut config = ClusterConfig::default();
    if let Some(seed) = table.get("seed") {
        config.seed = parse_cluster_seed(seed)?;
    }
    if let Some(heartbeat) = table.get("heartbeat") {
        config.heartbeat = parse_cluster_heartbeat(heartbeat)?;
    }
    if let Some(downing) = table.get("downing") {
        config.downing = parse_cluster_downing(downing)?;
    }
    if let Some(sharding) = table.get("sharding") {
        config.sharding = parse_cluster_sharding(sharding)?;
    }
    if let Some(tools) = table.get("tools") {
        config.tools = parse_cluster_tools(tools)?;
    }
    Ok(config)
}

fn parse_dispatchers(value: &Value) -> Result<BTreeMap<String, DispatcherConfig>, ConfigError> {
    let table = expect_table(value, "actor.dispatchers")?;
    let mut dispatchers = BTreeMap::new();
    for (name, value) in table {
        let path = format!("actor.dispatchers.{name}");
        let table = expect_table(value, &path)?;
        reject_unknown(table, &path, &["throughput"])?;
        let mut dispatcher = DispatcherConfig::default();
        if let Some(throughput) = table.get("throughput") {
            dispatcher.throughput = parse_usize(throughput, &format!("{path}.throughput"))?;
            if dispatcher.throughput == 0 {
                return Err(ConfigError::InvalidValue {
                    path: format!("{path}.throughput"),
                    reason: "must be greater than zero".to_string(),
                });
            }
        }
        dispatchers.insert(name.clone(), dispatcher);
    }
    if dispatchers.is_empty() {
        dispatchers.insert("default".to_string(), DispatcherConfig::default());
    }
    Ok(dispatchers)
}

fn parse_mailboxes(value: &Value) -> Result<BTreeMap<String, MailboxConfig>, ConfigError> {
    let table = expect_table(value, "actor.mailboxes")?;
    let mut mailboxes = BTreeMap::new();
    for (name, value) in table {
        let path = format!("actor.mailboxes.{name}");
        let table = expect_table(value, &path)?;
        reject_unknown(table, &path, &["capacity"])?;
        let mut mailbox = MailboxConfig::default();
        if let Some(capacity) = table.get("capacity") {
            mailbox.capacity = Some(parse_usize(capacity, &format!("{path}.capacity"))?);
            mailbox.validated_capacity(format!("{path}.capacity"))?;
        }
        mailboxes.insert(name.clone(), mailbox);
    }
    if mailboxes.is_empty() {
        mailboxes.insert("default".to_string(), MailboxConfig::default());
    }
    Ok(mailboxes)
}

fn parse_remote_transport(value: &Value) -> Result<RemoteTransportConfig, ConfigError> {
    let table = expect_table(value, "remote.transport")?;
    reject_unknown(
        table,
        "remote.transport",
        &["canonical_hostname", "canonical_port"],
    )?;
    let mut config = RemoteTransportConfig::default();
    if let Some(hostname) = table.get("canonical_hostname") {
        config.canonical_hostname = parse_string(hostname, "remote.transport.canonical_hostname")?;
        if config.canonical_hostname.is_empty() {
            return Err(ConfigError::InvalidValue {
                path: "remote.transport.canonical_hostname".to_string(),
                reason: "must not be empty".to_string(),
            });
        }
    }
    if let Some(port) = table.get("canonical_port") {
        let port = parse_u64(port, "remote.transport.canonical_port")?;
        config.canonical_port = u16::try_from(port).map_err(|_| ConfigError::InvalidValue {
            path: "remote.transport.canonical_port".to_string(),
            reason: "must fit in a u16 port".to_string(),
        })?;
    }
    Ok(config)
}

fn parse_cluster_seed(value: &Value) -> Result<ClusterSeedConfig, ConfigError> {
    let table = expect_table(value, "cluster.seed")?;
    reject_unknown(table, "cluster.seed", &["nodes"])?;
    let mut config = ClusterSeedConfig::default();
    if let Some(nodes) = table.get("nodes") {
        config.nodes = parse_string_array(nodes, "cluster.seed.nodes")?;
    }
    Ok(config)
}

fn parse_cluster_heartbeat(value: &Value) -> Result<ClusterHeartbeatConfig, ConfigError> {
    let table = expect_table(value, "cluster.heartbeat")?;
    reject_unknown(
        table,
        "cluster.heartbeat",
        &[
            "monitored_by_nr_of_members",
            "interval",
            "acceptable_pause",
            "expected_response_after",
        ],
    )?;
    let mut config = ClusterHeartbeatConfig::default();
    if let Some(count) = table.get("monitored_by_nr_of_members") {
        config.monitored_by_nr_of_members =
            parse_usize(count, "cluster.heartbeat.monitored_by_nr_of_members")?;
        if config.monitored_by_nr_of_members == 0 {
            return Err(ConfigError::InvalidValue {
                path: "cluster.heartbeat.monitored_by_nr_of_members".to_string(),
                reason: "must be greater than zero".to_string(),
            });
        }
    }
    if let Some(interval) = table.get("interval") {
        config.interval = parse_duration(interval, "cluster.heartbeat.interval")?;
        reject_zero_duration(config.interval, "cluster.heartbeat.interval")?;
    }
    if let Some(pause) = table.get("acceptable_pause") {
        config.acceptable_pause = parse_duration(pause, "cluster.heartbeat.acceptable_pause")?;
    }
    if let Some(delay) = table.get("expected_response_after") {
        config.expected_response_after =
            parse_duration(delay, "cluster.heartbeat.expected_response_after")?;
        reject_zero_duration(
            config.expected_response_after,
            "cluster.heartbeat.expected_response_after",
        )?;
    }
    Ok(config)
}

fn parse_cluster_downing(value: &Value) -> Result<ClusterDowningConfig, ConfigError> {
    let table = expect_table(value, "cluster.downing")?;
    reject_unknown(
        table,
        "cluster.downing",
        &[
            "strategy",
            "stable_after",
            "role",
            "down_if_alone",
            "lease_name",
            "acquire_lease_delay_for_minority",
            "release_after",
        ],
    )?;
    let mut config = ClusterDowningConfig::default();
    if let Some(strategy) = table.get("strategy") {
        config.strategy = parse_downing_strategy(table, strategy)?;
    }
    if let Some(stable_after) = table.get("stable_after") {
        config.stable_after = parse_duration(stable_after, "cluster.downing.stable_after")?;
        reject_zero_duration(config.stable_after, "cluster.downing.stable_after")?;
    }
    Ok(config)
}

fn parse_downing_strategy(
    table: &toml::map::Map<String, Value>,
    value: &Value,
) -> Result<ClusterDowningStrategyConfig, ConfigError> {
    let strategy = parse_string(value, "cluster.downing.strategy")?;
    match strategy.as_str() {
        "none" => {
            reject_downing_strategy_options(table, "none", &[])?;
            Ok(ClusterDowningStrategyConfig::None)
        }
        "down-all" => {
            reject_downing_strategy_options(table, "down-all", &[])?;
            Ok(ClusterDowningStrategyConfig::DownAll)
        }
        "keep-majority" => {
            reject_downing_strategy_options(table, "keep-majority", &["role"])?;
            let role = optional_non_empty_string(table, "role", "cluster.downing.role")?;
            Ok(ClusterDowningStrategyConfig::KeepMajority { role })
        }
        "keep-oldest" => {
            reject_downing_strategy_options(table, "keep-oldest", &["role", "down_if_alone"])?;
            let role = optional_non_empty_string(table, "role", "cluster.downing.role")?;
            Ok(ClusterDowningStrategyConfig::KeepOldest {
                role,
                down_if_alone: optional_bool(
                    table,
                    "down_if_alone",
                    "cluster.downing.down_if_alone",
                )?
                .unwrap_or(false),
            })
        }
        "lease-majority" => parse_lease_majority_strategy(table),
        _ => Err(ConfigError::InvalidValue {
            path: "cluster.downing.strategy".to_string(),
            reason: "expected none, down-all, keep-majority, keep-oldest, or lease-majority"
                .to_string(),
        }),
    }
}

fn parse_lease_majority_strategy(
    table: &toml::map::Map<String, Value>,
) -> Result<ClusterDowningStrategyConfig, ConfigError> {
    reject_downing_strategy_options(
        table,
        "lease-majority",
        &[
            "role",
            "lease_name",
            "acquire_lease_delay_for_minority",
            "release_after",
        ],
    )?;
    let role = optional_non_empty_string(table, "role", "cluster.downing.role")?;
    let lease_name = optional_non_empty_string(table, "lease_name", "cluster.downing.lease_name")?
        .ok_or_else(|| ConfigError::InvalidValue {
            path: "cluster.downing.lease_name".to_string(),
            reason: "must be set for lease-majority".to_string(),
        })?;
    let acquire_lease_delay_for_minority = optional_duration(
        table,
        "acquire_lease_delay_for_minority",
        "cluster.downing.acquire_lease_delay_for_minority",
    )?
    .unwrap_or(Duration::ZERO);
    let release_after = optional_duration(table, "release_after", "cluster.downing.release_after")?
        .unwrap_or(Duration::from_secs(20));
    reject_zero_duration(release_after, "cluster.downing.release_after")?;
    Ok(ClusterDowningStrategyConfig::LeaseMajority {
        lease_name,
        role,
        acquire_lease_delay_for_minority,
        release_after,
    })
}

fn reject_downing_strategy_options(
    table: &toml::map::Map<String, Value>,
    strategy: &str,
    allowed: &[&str],
) -> Result<(), ConfigError> {
    for key in [
        "role",
        "down_if_alone",
        "lease_name",
        "acquire_lease_delay_for_minority",
        "release_after",
    ] {
        if table.contains_key(key) && !allowed.contains(&key) {
            return Err(ConfigError::InvalidValue {
                path: format!("cluster.downing.{key}"),
                reason: format!("is not valid for {strategy}"),
            });
        }
    }
    Ok(())
}

fn parse_cluster_sharding(value: &Value) -> Result<ClusterShardingConfig, ConfigError> {
    let table = expect_table(value, "cluster.sharding")?;
    reject_unknown(
        table,
        "cluster.sharding",
        &["number_of_shards", "rebalance_interval"],
    )?;
    let mut config = ClusterShardingConfig::default();
    if let Some(shards) = table.get("number_of_shards") {
        config.number_of_shards = parse_u64(shards, "cluster.sharding.number_of_shards")?;
        if config.number_of_shards == 0 {
            return Err(ConfigError::InvalidValue {
                path: "cluster.sharding.number_of_shards".to_string(),
                reason: "must be greater than zero".to_string(),
            });
        }
    }
    if let Some(interval) = table.get("rebalance_interval") {
        config.rebalance_interval =
            parse_duration(interval, "cluster.sharding.rebalance_interval")?;
        reject_zero_duration(
            config.rebalance_interval,
            "cluster.sharding.rebalance_interval",
        )?;
    }
    Ok(config)
}

fn parse_cluster_tools(value: &Value) -> Result<ClusterToolsConfig, ConfigError> {
    let table = expect_table(value, "cluster.tools")?;
    reject_unknown(table, "cluster.tools", &["singleton", "pubsub"])?;
    let mut config = ClusterToolsConfig::default();
    if let Some(singleton) = table.get("singleton") {
        parse_cluster_singleton_tool(singleton, &mut config)?;
    }
    if let Some(pubsub) = table.get("pubsub") {
        parse_cluster_pubsub_tool(pubsub, &mut config)?;
    }
    Ok(config)
}

fn parse_cluster_singleton_tool(
    value: &Value,
    config: &mut ClusterToolsConfig,
) -> Result<(), ConfigError> {
    let singleton = expect_table(value, "cluster.tools.singleton")?;
    reject_unknown(singleton, "cluster.tools.singleton", &["role"])?;
    if let Some(role) = singleton.get("role") {
        let role = parse_string(role, "cluster.tools.singleton.role")?;
        config.singleton_role = (!role.is_empty()).then_some(role);
    }
    Ok(())
}

fn parse_cluster_pubsub_tool(
    value: &Value,
    config: &mut ClusterToolsConfig,
) -> Result<(), ConfigError> {
    let pubsub = expect_table(value, "cluster.tools.pubsub")?;
    reject_unknown(
        pubsub,
        "cluster.tools.pubsub",
        &["gossip_interval", "max_delta_entries"],
    )?;
    if let Some(interval) = pubsub.get("gossip_interval") {
        config.pubsub_gossip_interval =
            parse_duration(interval, "cluster.tools.pubsub.gossip_interval")?;
        reject_zero_duration(
            config.pubsub_gossip_interval,
            "cluster.tools.pubsub.gossip_interval",
        )?;
    }
    if let Some(max_delta_entries) = pubsub.get("max_delta_entries") {
        config.pubsub_max_delta_entries =
            parse_usize(max_delta_entries, "cluster.tools.pubsub.max_delta_entries")?;
        if config.pubsub_max_delta_entries == 0 {
            return Err(ConfigError::InvalidValue {
                path: "cluster.tools.pubsub.max_delta_entries".to_string(),
                reason: "must be greater than zero".to_string(),
            });
        }
    }
    Ok(())
}
