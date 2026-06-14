use std::collections::BTreeMap;
use std::fs;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::{
    ActorConfig, ClusterDowningStrategyConfig, ConfigError, DispatcherConfig, KairoSettings,
    MailboxConfig, load_toml_file, parse_toml_str,
};

#[test]
fn toml_config_parses_structured_runtime_settings() {
    let settings = parse_toml_str(
        r#"
[actor.dispatchers.default]
throughput = 32

[actor.dispatchers.blocking]
throughput = 1

[actor.mailboxes.default]
capacity = 64

[actor.mailboxes.control]
capacity = 8

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
role = "backend"

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
    assert_eq!(settings.actor.mailboxes["default"].capacity, Some(64));
    assert_eq!(settings.actor.mailboxes["control"].capacity, Some(8));
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
    assert_eq!(
        settings.cluster.downing.strategy,
        ClusterDowningStrategyConfig::KeepMajority {
            role: Some("backend".to_string())
        }
    );
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
    assert_eq!(settings.actor.mailboxes["default"].capacity, None);
    assert_eq!(settings.remote.transport.canonical_port, 25520);
    assert_eq!(settings.cluster.sharding.number_of_shards, 100);
}

#[test]
fn toml_config_parses_lease_majority_downing_settings() {
    let settings = parse_toml_str(
        r#"
[cluster.downing]
strategy = "lease-majority"
stable_after = "10s"
role = "backend"
lease_name = "cluster-sbr"
acquire_lease_delay_for_minority = "3s"
release_after = "30s"
"#,
    )
    .unwrap();

    assert_eq!(
        settings.cluster.downing.strategy,
        ClusterDowningStrategyConfig::LeaseMajority {
            lease_name: "cluster-sbr".to_string(),
            role: Some("backend".to_string()),
            acquire_lease_delay_for_minority: Duration::from_secs(3),
            release_after: Duration::from_secs(30),
        }
    );
    assert_eq!(
        settings.cluster.downing.stable_after,
        Duration::from_secs(10)
    );
}

#[test]
fn toml_config_rejects_invalid_downing_strategy() {
    let error = parse_toml_str(
        r#"
[cluster.downing]
strategy = "split-everything"
"#,
    )
    .unwrap_err();

    assert_eq!(
        error,
        ConfigError::InvalidValue {
            path: "cluster.downing.strategy".to_string(),
            reason: "expected none, down-all, keep-majority, keep-oldest, or lease-majority"
                .to_string(),
        }
    );
}

#[test]
fn toml_config_rejects_lease_majority_without_lease_name() {
    let error = parse_toml_str(
        r#"
[cluster.downing]
strategy = "lease-majority"
"#,
    )
    .unwrap_err();

    assert_eq!(
        error,
        ConfigError::InvalidValue {
            path: "cluster.downing.lease_name".to_string(),
            reason: "must be set for lease-majority".to_string(),
        }
    );
}

#[test]
fn toml_config_rejects_strategy_specific_downing_options() {
    let error = parse_toml_str(
        r#"
[cluster.downing]
strategy = "keep-majority"
down_if_alone = true
"#,
    )
    .unwrap_err();

    assert_eq!(
        error,
        ConfigError::InvalidValue {
            path: "cluster.downing.down_if_alone".to_string(),
            reason: "is not valid for keep-majority".to_string(),
        }
    );
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
fn toml_config_rejects_zero_sharding_rebalance_interval() {
    let error = parse_toml_str(
        r#"
[cluster.sharding]
rebalance_interval = "0ms"
"#,
    )
    .unwrap_err();

    assert_eq!(
        error,
        ConfigError::InvalidValue {
            path: "cluster.sharding.rebalance_interval".to_string(),
            reason: "must be greater than zero".to_string(),
        }
    );
}

#[test]
fn toml_config_rejects_zero_mailbox_capacity() {
    let error = parse_toml_str(
        r#"
[actor.mailboxes.default]
capacity = 0
"#,
    )
    .unwrap_err();

    assert_eq!(
        error,
        ConfigError::InvalidValue {
            path: "actor.mailboxes.default.capacity".to_string(),
            reason: "must be greater than zero when set".to_string(),
        }
    );
}

#[test]
#[cfg(feature = "actor")]
fn config_converts_actor_settings_to_builder() {
    let settings = parse_toml_str(
        r#"
[actor.dispatchers.default]
throughput = 17

[actor.mailboxes.default]
capacity = 3
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
    assert_eq!(system.mailbox_settings().user_capacity(), Some(3));
}

#[test]
#[cfg(all(feature = "remote", feature = "cluster"))]
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
#[cfg(feature = "actor")]
fn config_runtime_helpers_validate_directly_constructed_settings() {
    let actor = ActorConfig {
        dispatchers: BTreeMap::from([("other".to_string(), DispatcherConfig { throughput: 1 })]),
        ..Default::default()
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
            ..Default::default()
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
            ..Default::default()
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
        actor: ActorConfig {
            mailboxes: BTreeMap::from([(
                "default".to_string(),
                MailboxConfig { capacity: Some(0) },
            )]),
            ..Default::default()
        },
        ..KairoSettings::default()
    };
    assert_eq!(
        settings.validate().unwrap_err(),
        ConfigError::InvalidValue {
            path: "actor.mailboxes.default.capacity".to_string(),
            reason: "must be greater than zero when set".to_string(),
        }
    );

    let settings = KairoSettings {
        cluster: super::ClusterConfig {
            downing: super::ClusterDowningConfig {
                strategy: ClusterDowningStrategyConfig::LeaseMajority {
                    lease_name: String::new(),
                    role: None,
                    acquire_lease_delay_for_minority: Duration::ZERO,
                    release_after: Duration::from_secs(1),
                },
                ..Default::default()
            },
            ..Default::default()
        },
        ..KairoSettings::default()
    };
    assert_eq!(
        settings.validate().unwrap_err(),
        ConfigError::InvalidValue {
            path: "cluster.downing.lease_name".to_string(),
            reason: "must not be empty for lease-majority".to_string(),
        }
    );

    let settings = KairoSettings {
        cluster: super::ClusterConfig {
            sharding: super::ClusterShardingConfig {
                rebalance_interval: Duration::ZERO,
                ..Default::default()
            },
            ..Default::default()
        },
        ..KairoSettings::default()
    };
    assert_eq!(
        settings.validate().unwrap_err(),
        ConfigError::InvalidValue {
            path: "cluster.sharding.rebalance_interval".to_string(),
            reason: "must be greater than zero".to_string(),
        }
    );

    assert!(KairoSettings::default().validate().is_ok());
}
