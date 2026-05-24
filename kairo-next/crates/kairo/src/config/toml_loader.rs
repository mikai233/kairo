use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;
use std::time::Duration;

use toml::Value;

use super::error::ConfigError;
use super::settings::{
    ActorConfig, ClusterConfig, ClusterDowningConfig, ClusterHeartbeatConfig, ClusterSeedConfig,
    ClusterShardingConfig, ClusterToolsConfig, DispatcherConfig, KairoSettings, RemoteConfig,
    RemoteTransportConfig,
};

pub fn load_toml_file(path: impl AsRef<Path>) -> Result<KairoSettings, ConfigError> {
    let path = path.as_ref();
    let contents = fs::read_to_string(path).map_err(|error| ConfigError::ReadFailed {
        path: path.to_path_buf(),
        reason: error.to_string(),
    })?;
    parse_toml_str(&contents)
}

pub fn parse_toml_str(input: &str) -> Result<KairoSettings, ConfigError> {
    let value = input
        .parse::<Value>()
        .map_err(|error| ConfigError::ParseFailed {
            reason: error.to_string(),
        })?;
    let table = value.as_table().ok_or_else(|| ConfigError::InvalidType {
        path: "<root>".to_string(),
        expected: "a TOML table".to_string(),
    })?;
    reject_unknown(table, "", &["actor", "remote", "cluster"])?;

    let mut settings = KairoSettings::default();
    if let Some(actor) = table.get("actor") {
        settings.actor = parse_actor(actor)?;
    }
    if let Some(remote) = table.get("remote") {
        settings.remote = parse_remote(remote)?;
    }
    if let Some(cluster) = table.get("cluster") {
        settings.cluster = parse_cluster(cluster)?;
    }
    Ok(settings)
}

fn parse_actor(value: &Value) -> Result<ActorConfig, ConfigError> {
    let table = expect_table(value, "actor")?;
    reject_unknown(table, "actor", &["dispatchers"])?;
    let mut config = ActorConfig::default();
    if let Some(dispatchers) = table.get("dispatchers") {
        config.dispatchers = parse_dispatchers(dispatchers)?;
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

fn parse_remote(value: &Value) -> Result<RemoteConfig, ConfigError> {
    let table = expect_table(value, "remote")?;
    reject_unknown(table, "remote", &["transport"])?;
    let mut config = RemoteConfig::default();
    if let Some(transport) = table.get("transport") {
        config.transport = parse_remote_transport(transport)?;
    }
    Ok(config)
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

fn parse_cluster(value: &Value) -> Result<ClusterConfig, ConfigError> {
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
    reject_unknown(table, "cluster.downing", &["strategy", "stable_after"])?;
    let mut config = ClusterDowningConfig::default();
    if let Some(strategy) = table.get("strategy") {
        config.strategy = parse_string(strategy, "cluster.downing.strategy")?;
    }
    if let Some(stable_after) = table.get("stable_after") {
        config.stable_after = parse_duration(stable_after, "cluster.downing.stable_after")?;
    }
    Ok(config)
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
    }
    Ok(config)
}

fn parse_cluster_tools(value: &Value) -> Result<ClusterToolsConfig, ConfigError> {
    let table = expect_table(value, "cluster.tools")?;
    reject_unknown(table, "cluster.tools", &["singleton", "pubsub"])?;
    let mut config = ClusterToolsConfig::default();
    if let Some(singleton) = table.get("singleton") {
        let singleton = expect_table(singleton, "cluster.tools.singleton")?;
        reject_unknown(singleton, "cluster.tools.singleton", &["role"])?;
        if let Some(role) = singleton.get("role") {
            let role = parse_string(role, "cluster.tools.singleton.role")?;
            config.singleton_role = (!role.is_empty()).then_some(role);
        }
    }
    if let Some(pubsub) = table.get("pubsub") {
        let pubsub = expect_table(pubsub, "cluster.tools.pubsub")?;
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
    }
    Ok(config)
}

fn expect_table<'a>(
    value: &'a Value,
    path: &str,
) -> Result<&'a toml::map::Map<String, Value>, ConfigError> {
    value.as_table().ok_or_else(|| ConfigError::InvalidType {
        path: path.to_string(),
        expected: "a table".to_string(),
    })
}

fn reject_unknown(
    table: &toml::map::Map<String, Value>,
    path: &str,
    allowed: &[&str],
) -> Result<(), ConfigError> {
    let allowed: BTreeSet<_> = allowed.iter().copied().collect();
    for key in table.keys() {
        if !allowed.contains(key.as_str()) {
            let path = if path.is_empty() {
                key.clone()
            } else {
                format!("{path}.{key}")
            };
            return Err(ConfigError::UnknownKey { path });
        }
    }
    Ok(())
}

fn parse_string(value: &Value, path: &str) -> Result<String, ConfigError> {
    value
        .as_str()
        .map(ToString::to_string)
        .ok_or_else(|| ConfigError::InvalidType {
            path: path.to_string(),
            expected: "a string".to_string(),
        })
}

fn parse_string_array(value: &Value, path: &str) -> Result<Vec<String>, ConfigError> {
    let array = value.as_array().ok_or_else(|| ConfigError::InvalidType {
        path: path.to_string(),
        expected: "an array of strings".to_string(),
    })?;
    array
        .iter()
        .enumerate()
        .map(|(index, item)| parse_string(item, &format!("{path}[{index}]")))
        .collect()
}

fn parse_usize(value: &Value, path: &str) -> Result<usize, ConfigError> {
    let value = parse_u64(value, path)?;
    usize::try_from(value).map_err(|_| ConfigError::InvalidValue {
        path: path.to_string(),
        reason: "must fit in usize".to_string(),
    })
}

fn parse_u64(value: &Value, path: &str) -> Result<u64, ConfigError> {
    let value = value.as_integer().ok_or_else(|| ConfigError::InvalidType {
        path: path.to_string(),
        expected: "an integer".to_string(),
    })?;
    u64::try_from(value).map_err(|_| ConfigError::InvalidValue {
        path: path.to_string(),
        reason: "must not be negative".to_string(),
    })
}

fn parse_duration(value: &Value, path: &str) -> Result<Duration, ConfigError> {
    match value {
        Value::Integer(_) => Ok(Duration::from_millis(parse_u64(value, path)?)),
        Value::String(input) => parse_duration_string(input, path),
        _ => Err(ConfigError::InvalidType {
            path: path.to_string(),
            expected: "a duration string or integer milliseconds".to_string(),
        }),
    }
}

fn parse_duration_string(input: &str, path: &str) -> Result<Duration, ConfigError> {
    let Some((number, multiplier)) = duration_parts(input.trim()) else {
        return Err(ConfigError::InvalidValue {
            path: path.to_string(),
            reason: "use integer milliseconds or a string with ms, s, m, or h suffix".to_string(),
        });
    };
    let value = number
        .parse::<u64>()
        .map_err(|_| ConfigError::InvalidValue {
            path: path.to_string(),
            reason: "duration amount must be an unsigned integer".to_string(),
        })?;
    value
        .checked_mul(multiplier)
        .map(Duration::from_millis)
        .ok_or_else(|| ConfigError::InvalidValue {
            path: path.to_string(),
            reason: "duration is too large".to_string(),
        })
}

fn duration_parts(input: &str) -> Option<(&str, u64)> {
    for (suffix, multiplier) in [("ms", 1), ("s", 1_000), ("m", 60_000), ("h", 3_600_000)] {
        if let Some(number) = input.strip_suffix(suffix) {
            return Some((number.trim(), multiplier));
        }
    }
    None
}

fn reject_zero_duration(duration: Duration, path: &str) -> Result<(), ConfigError> {
    if duration.is_zero() {
        Err(ConfigError::InvalidValue {
            path: path.to_string(),
            reason: "must be greater than zero".to_string(),
        })
    } else {
        Ok(())
    }
}
