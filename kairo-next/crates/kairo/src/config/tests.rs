use std::collections::BTreeMap;
use std::fs;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::{
    ActorConfig, ConfigError, DispatcherConfig, KairoSettings, load_toml_file, parse_toml_str,
};

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

#[test]
fn config_converts_actor_settings_to_builder() {
    let settings = parse_toml_str(
        r#"
[actor.dispatchers.default]
throughput = 17
"#,
    )
    .unwrap();

    let system = settings
        .actor
        .actor_system_builder("configured-actor")
        .unwrap()
        .build()
        .unwrap();

    assert_eq!(system.dispatcher_settings().throughput(), 17);
}

#[test]
fn config_converts_remote_and_cluster_settings() {
    let settings = parse_toml_str(
        r#"
[remote.transport]
canonical_hostname = "127.0.0.42"
canonical_port = 26666

[cluster.heartbeat]
monitored_by_nr_of_members = 3
interval = "2s"
acceptable_pause = "4s"
expected_response_after = "750ms"
"#,
    )
    .unwrap();

    let remote = settings.remote.transport.to_remote_settings().unwrap();
    assert_eq!(remote.canonical_hostname, "127.0.0.42");
    assert_eq!(remote.canonical_port, 26666);

    let failure_detector = settings
        .cluster
        .heartbeat
        .to_failure_detector_settings()
        .unwrap();
    assert_eq!(
        failure_detector.heartbeat_interval(),
        Duration::from_secs(2)
    );
    assert_eq!(
        failure_detector.acceptable_heartbeat_pause(),
        Duration::from_secs(4)
    );
    let heartbeat = settings
        .cluster
        .heartbeat
        .to_heartbeat_sender_settings()
        .unwrap();
    assert_eq!(heartbeat.monitored_by_nr_of_members, 3);
    assert_eq!(
        heartbeat.heartbeat_expected_response_after,
        Duration::from_millis(750)
    );
}

#[test]
fn config_runtime_helpers_validate_directly_constructed_settings() {
    let actor = ActorConfig {
        dispatchers: BTreeMap::from([("other".to_string(), DispatcherConfig { throughput: 1 })]),
    };
    assert_eq!(
        actor.default_dispatcher().unwrap_err(),
        ConfigError::InvalidValue {
            path: "actor.dispatchers.default".to_string(),
            reason: "default dispatcher settings are required".to_string(),
        }
    );

    let settings = KairoSettings {
        actor: ActorConfig {
            dispatchers: BTreeMap::from([(
                "default".to_string(),
                DispatcherConfig { throughput: 0 },
            )]),
        },
        ..KairoSettings::default()
    };
    assert_eq!(
        settings
            .actor
            .actor_system_builder("invalid-throughput")
            .unwrap_err(),
        ConfigError::InvalidValue {
            path: "actor.dispatchers.default.throughput".to_string(),
            reason: "must be greater than zero".to_string(),
        }
    );
}

#[test]
fn config_validate_checks_all_format_neutral_sections() {
    let settings = KairoSettings {
        actor: ActorConfig {
            dispatchers: BTreeMap::from([
                ("default".to_string(), DispatcherConfig { throughput: 5 }),
                ("blocking".to_string(), DispatcherConfig { throughput: 0 }),
            ]),
        },
        ..KairoSettings::default()
    };
    assert_eq!(
        settings.validate().unwrap_err(),
        ConfigError::InvalidValue {
            path: "actor.dispatchers.blocking.throughput".to_string(),
            reason: "must be greater than zero".to_string(),
        }
    );

    let settings = KairoSettings {
        cluster: super::ClusterConfig {
            downing: super::ClusterDowningConfig {
                strategy: String::new(),
                ..Default::default()
            },
            ..Default::default()
        },
        ..KairoSettings::default()
    };
    assert_eq!(
        settings.validate().unwrap_err(),
        ConfigError::InvalidValue {
            path: "cluster.downing.strategy".to_string(),
            reason: "must not be empty".to_string(),
        }
    );

    assert!(KairoSettings::default().validate().is_ok());
}
