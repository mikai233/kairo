use std::fs;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::{ConfigError, KairoSettings, load_toml_file, parse_toml_str};

#[test]
fn toml_config_parses_structured_runtime_settings() {
    let settings = parse_toml_str(
        r#"
[actor.dispatchers.default]
throughput = 32

[actor.dispatchers.blocking]
throughput = 1

[remote.transport]
canonical_hostname = "10.0.0.12"
canonical_port = 25521

[cluster.seed]
nodes = [
  "kairo://kairo@10.0.0.10:25520",
  "kairo://kairo@10.0.0.11:25520",
]

[cluster.heartbeat]
monitored_by_nr_of_members = 7
interval = "2s"
acceptable_pause = "5s"
expected_response_after = 750

[cluster.downing]
strategy = "keep-majority"
stable_after = "15s"

[cluster.sharding]
number_of_shards = 128
rebalance_interval = "30s"

[cluster.tools.singleton]
role = "backend"

[cluster.tools.pubsub]
gossip_interval = "500ms"
max_delta_entries = 250
"#,
    )
    .unwrap();

    assert_eq!(settings.actor.dispatchers["default"].throughput, 32);
    assert_eq!(settings.actor.dispatchers["blocking"].throughput, 1);
    assert_eq!(settings.remote.transport.canonical_hostname, "10.0.0.12");
    assert_eq!(settings.remote.transport.canonical_port, 25521);
    assert_eq!(settings.cluster.seed.nodes.len(), 2);
    assert_eq!(settings.cluster.heartbeat.monitored_by_nr_of_members, 7);
    assert_eq!(settings.cluster.heartbeat.interval, Duration::from_secs(2));
    assert_eq!(
        settings.cluster.heartbeat.acceptable_pause,
        Duration::from_secs(5)
    );
    assert_eq!(
        settings.cluster.heartbeat.expected_response_after,
        Duration::from_millis(750)
    );
    assert_eq!(settings.cluster.downing.strategy, "keep-majority");
    assert_eq!(
        settings.cluster.downing.stable_after,
        Duration::from_secs(15)
    );
    assert_eq!(settings.cluster.sharding.number_of_shards, 128);
    assert_eq!(
        settings.cluster.sharding.rebalance_interval,
        Duration::from_secs(30)
    );
    assert_eq!(
        settings.cluster.tools.singleton_role.as_deref(),
        Some("backend")
    );
    assert_eq!(
        settings.cluster.tools.pubsub_gossip_interval,
        Duration::from_millis(500)
    );
    assert_eq!(settings.cluster.tools.pubsub_max_delta_entries, 250);
}

#[test]
fn toml_config_defaults_missing_sections_without_toml_specific_state() {
    let settings = parse_toml_str("").unwrap();

    assert_eq!(settings, KairoSettings::default());
    assert_eq!(settings.actor.dispatchers["default"].throughput, 5);
    assert_eq!(settings.remote.transport.canonical_port, 25520);
    assert_eq!(settings.cluster.sharding.number_of_shards, 100);
}

#[test]
fn toml_config_loads_from_file() {
    let mut path = std::env::temp_dir();
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    path.push(format!("kairo-config-test-{nonce}.toml"));
    fs::write(
        &path,
        r#"
[remote.transport]
canonical_port = 26666
"#,
    )
    .unwrap();

    let settings = load_toml_file(&path).unwrap();
    fs::remove_file(path).unwrap();

    assert_eq!(settings.remote.transport.canonical_port, 26666);
}

#[test]
fn toml_config_rejects_unknown_keys() {
    let error = parse_toml_str(
        r#"
[cluster]
membership_store = "etcd"
"#,
    )
    .unwrap_err();

    assert_eq!(
        error,
        ConfigError::UnknownKey {
            path: "cluster.membership_store".to_string(),
        }
    );
}

#[test]
fn toml_config_rejects_invalid_values() {
    let error = parse_toml_str(
        r#"
[cluster.sharding]
number_of_shards = 0
"#,
    )
    .unwrap_err();

    assert_eq!(
        error,
        ConfigError::InvalidValue {
            path: "cluster.sharding.number_of_shards".to_string(),
            reason: "must be greater than zero".to_string(),
        }
    );
}
