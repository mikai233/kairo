use std::collections::BTreeMap;
use std::fs;
#[cfg(feature = "actor")]
use std::sync::mpsc;
#[cfg(any(feature = "cluster", feature = "remote"))]
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::{
    ActorConfig, ClusterDowningStrategyConfig, ClusterShardingAllocationConfig, ConfigError,
    DiagnosticsConfig, DispatcherConfig, KairoSettings, MailboxConfig, STANDARD_TOML_FILES,
    find_standard_toml_files, load_standard_toml_files, load_toml_file, load_toml_files,
    parse_toml_str,
};

#[cfg(feature = "remote")]
#[derive(Default)]
struct CollectingRemoteDiagnostics {
    records: Mutex<Vec<kairo_remote::RemoteInboundDiagnostic>>,
}

#[cfg(feature = "remote")]
impl CollectingRemoteDiagnostics {
    fn records(&self) -> Vec<kairo_remote::RemoteInboundDiagnostic> {
        self.records
            .lock()
            .expect("remote diagnostics poisoned")
            .clone()
    }
}

#[cfg(feature = "remote")]
impl kairo_remote::RemoteInboundDiagnostics for CollectingRemoteDiagnostics {
    fn record(&self, diagnostic: kairo_remote::RemoteInboundDiagnostic) {
        self.records
            .lock()
            .expect("remote diagnostics poisoned")
            .push(diagnostic);
    }
}

#[cfg(feature = "remote")]
#[derive(Default)]
struct CollectingAssociationDiagnostics {
    records: Mutex<Vec<kairo_remote::RemoteAssociationDiagnostic>>,
}

#[cfg(feature = "remote")]
impl CollectingAssociationDiagnostics {
    fn records(&self) -> Vec<kairo_remote::RemoteAssociationDiagnostic> {
        self.records
            .lock()
            .expect("association diagnostics poisoned")
            .clone()
    }
}

#[cfg(feature = "remote")]
impl kairo_remote::RemoteAssociationDiagnostics for CollectingAssociationDiagnostics {
    fn record(&self, diagnostic: kairo_remote::RemoteAssociationDiagnostic) {
        self.records
            .lock()
            .expect("association diagnostics poisoned")
            .push(diagnostic);
    }
}

#[cfg(feature = "remote")]
fn remote_diagnostic_recipient() -> kairo_serialization::ActorRefWireData {
    kairo_serialization::ActorRefWireData::new("kairo://receiver/user/target").unwrap()
}

#[cfg(feature = "cluster")]
#[derive(Default)]
struct CollectingClusterDiagnostics {
    records: Mutex<Vec<kairo_cluster::ClusterDiagnostic>>,
}

#[cfg(feature = "cluster")]
impl CollectingClusterDiagnostics {
    fn records(&self) -> Vec<kairo_cluster::ClusterDiagnostic> {
        self.records
            .lock()
            .expect("cluster diagnostics poisoned")
            .clone()
    }
}

#[cfg(feature = "cluster")]
impl kairo_cluster::ClusterDiagnostics for CollectingClusterDiagnostics {
    fn record(&self, diagnostic: kairo_cluster::ClusterDiagnostic) {
        self.records
            .lock()
            .expect("cluster diagnostics poisoned")
            .push(diagnostic);
    }
}

#[test]
fn config_dependencies_remain_toml_first_without_hocon() -> Result<(), Box<dyn std::error::Error>> {
    let crate_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let repo_root = crate_dir
        .ancestors()
        .nth(3)
        .ok_or("kairo crate should live under kairo-next/crates/kairo")?;
    let root_manifest = fs::read_to_string(repo_root.join("Cargo.toml"))?;
    let facade_manifest = fs::read_to_string(crate_dir.join("Cargo.toml"))?;
    let lockfile = fs::read_to_string(repo_root.join("Cargo.lock"))?;

    assert!(
        root_manifest.contains("\ntoml = "),
        "workspace should keep TOML as the selected config parser"
    );
    assert!(
        facade_manifest.contains("toml = { workspace = true, optional = true }"),
        "facade config parser dependency should stay optional"
    );
    assert!(
        facade_manifest.contains("config = [\"dep:toml\"]"),
        "config feature should opt into TOML explicitly"
    );
    for (name, contents) in [
        ("root Cargo.toml", root_manifest.as_str()),
        ("kairo Cargo.toml", facade_manifest.as_str()),
        ("Cargo.lock", lockfile.as_str()),
    ] {
        assert!(
            !contents.to_ascii_lowercase().contains("hocon"),
            "{name} must not introduce HOCON before that parser is intentionally selected"
        );
    }

    Ok(())
}

#[test]
fn toml_config_parses_structured_runtime_settings() {
    let settings = parse_toml_str(
        r#"
[actor.dispatchers.default]
throughput = 32
workers = 4

[actor.dispatchers.blocking]
throughput = 1

[actor.mailboxes.default]
capacity = 64

[actor.mailboxes.control]
capacity = 8

[actor.task_executor]
workers = 3
queue_capacity = 256

[remote.transport]
canonical_hostname = "10.0.0.12"
canonical_port = 25521
connect_timeout = "250ms"

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
remember_entities = true
retry_interval = "3s"
handoff_timeout = "45s"
shard_failure_backoff = "12s"
rebalance_interval = "30s"
shard_region_query_timeout = "4s"

[cluster.sharding.least_shard_allocation]
rebalance_absolute_limit = 4
rebalance_relative_limit = 0.25

[cluster.tools.singleton]
role = "backend"
hand_over_retry_interval = "750ms"

[cluster.tools.pubsub]
gossip_interval = "500ms"
max_delta_entries = 250

[observability.diagnostics]
dead_letters = true
remote_delivery_failures = false
serialization_failures = true
quarantine_events = false
association_close_events = false
gossip_state_changes = true
"#,
    )
    .unwrap();

    assert_eq!(settings.actor.dispatchers["default"].throughput, 32);
    assert_eq!(settings.actor.dispatchers["default"].workers, Some(4));
    assert_eq!(settings.actor.dispatchers["blocking"].throughput, 1);
    assert_eq!(settings.actor.mailboxes["default"].capacity, Some(64));
    assert_eq!(settings.actor.mailboxes["control"].capacity, Some(8));
    assert_eq!(settings.actor.task_executor.workers, Some(3));
    assert_eq!(settings.actor.task_executor.queue_capacity, 256);
    assert_eq!(settings.remote.transport.canonical_hostname, "10.0.0.12");
    assert_eq!(settings.remote.transport.canonical_port, 25521);
    assert_eq!(
        settings.remote.transport.connect_timeout,
        Some(Duration::from_millis(250))
    );
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
    assert!(settings.cluster.sharding.remember_entities);
    assert_eq!(
        settings.cluster.sharding.retry_interval,
        Duration::from_secs(3)
    );
    assert_eq!(
        settings.cluster.sharding.handoff_timeout,
        Duration::from_secs(45)
    );
    assert_eq!(
        settings.cluster.sharding.shard_failure_backoff,
        Duration::from_secs(12)
    );
    assert_eq!(
        settings.cluster.sharding.rebalance_interval,
        Duration::from_secs(30)
    );
    assert_eq!(
        settings.cluster.sharding.shard_region_query_timeout,
        Duration::from_secs(4)
    );
    assert_eq!(
        settings
            .cluster
            .sharding
            .least_shard_allocation
            .rebalance_absolute_limit,
        4
    );
    assert_eq!(
        settings
            .cluster
            .sharding
            .least_shard_allocation
            .rebalance_relative_limit,
        0.25
    );
    assert_eq!(
        settings.cluster.tools.singleton_role.as_deref(),
        Some("backend")
    );
    assert_eq!(
        settings.cluster.tools.singleton_hand_over_retry_interval,
        Duration::from_millis(750)
    );
    assert_eq!(
        settings.cluster.tools.pubsub_gossip_interval,
        Duration::from_millis(500)
    );
    assert_eq!(settings.cluster.tools.pubsub_max_delta_entries, 250);
    assert!(settings.observability.diagnostics.dead_letters);
    assert!(!settings.observability.diagnostics.remote_delivery_failures);
    assert!(settings.observability.diagnostics.serialization_failures);
    assert!(!settings.observability.diagnostics.quarantine_events);
    assert!(!settings.observability.diagnostics.association_close_events);
    assert!(settings.observability.diagnostics.gossip_state_changes);
}

#[test]
fn toml_config_defaults_missing_sections_without_toml_specific_state() {
    let settings = parse_toml_str("").unwrap();

    assert_eq!(settings, KairoSettings::default());
    assert_eq!(settings.actor.dispatchers["default"].throughput, 5);
    assert_eq!(settings.actor.mailboxes["default"].capacity, None);
    assert_eq!(settings.remote.transport.canonical_port, 25520);
    assert_eq!(settings.cluster.sharding.number_of_shards, 100);
    assert!(!settings.cluster.sharding.remember_entities);
    assert_eq!(
        settings.cluster.sharding.retry_interval,
        Duration::from_secs(2)
    );
    assert_eq!(
        settings.cluster.sharding.handoff_timeout,
        Duration::from_secs(60)
    );
    assert_eq!(
        settings.cluster.sharding.shard_failure_backoff,
        Duration::from_secs(10)
    );
    assert_eq!(
        settings
            .cluster
            .sharding
            .least_shard_allocation
            .rebalance_absolute_limit,
        10
    );
    assert_eq!(
        settings
            .cluster
            .sharding
            .least_shard_allocation
            .rebalance_relative_limit,
        0.1
    );
    assert_eq!(
        settings.cluster.sharding.shard_region_query_timeout,
        Duration::from_secs(3)
    );
    assert!(settings.observability.diagnostics.dead_letters);
    assert!(
        settings
            .observability
            .diagnostics
            .publishes_runtime_failures()
    );
}

#[test]
fn toml_config_parses_observability_diagnostics() {
    let settings = parse_toml_str(
        r#"
[observability.diagnostics]
dead_letters = false
remote_delivery_failures = false
serialization_failures = false
quarantine_events = false
association_close_events = false
gossip_state_changes = false
"#,
    )
    .unwrap();

    assert_eq!(
        settings.observability.diagnostics,
        DiagnosticsConfig {
            dead_letters: false,
            remote_delivery_failures: false,
            serialization_failures: false,
            quarantine_events: false,
            association_close_events: false,
            gossip_state_changes: false,
        }
    );
    assert!(
        !settings
            .observability
            .diagnostics
            .publishes_runtime_failures()
    );
}

#[cfg(feature = "remote")]
#[test]
fn diagnostics_config_filters_remote_inbound_categories() {
    let settings = parse_toml_str(
        r#"
[observability.diagnostics]
remote_delivery_failures = false
serialization_failures = true
"#,
    )
    .unwrap();
    let diagnostics = Arc::new(CollectingRemoteDiagnostics::default());
    let observer = settings
        .observability
        .diagnostics
        .remote_inbound_diagnostics(
            diagnostics.clone() as Arc<dyn kairo_remote::RemoteInboundDiagnostics>
        )
        .expect("serialization diagnostics should install observer");

    observer.record(
        kairo_remote::RemoteInboundDiagnostic::SerializationFailure {
            recipient: remote_diagnostic_recipient(),
            sender: None,
            serializer_id: 17,
            manifest: "example.Manifest".to_string(),
            version: 1,
            reason: "decode failed".to_string(),
        },
    );
    observer.record(kairo_remote::RemoteInboundDiagnostic::DeliveryFailure {
        recipient: remote_diagnostic_recipient(),
        sender: None,
        reason: "delivery failed".to_string(),
    });

    assert_eq!(
        diagnostics.records(),
        vec![
            kairo_remote::RemoteInboundDiagnostic::SerializationFailure {
                recipient: remote_diagnostic_recipient(),
                sender: None,
                serializer_id: 17,
                manifest: "example.Manifest".to_string(),
                version: 1,
                reason: "decode failed".to_string(),
            }
        ]
    );
}

#[cfg(feature = "remote")]
#[test]
fn diagnostics_config_omits_remote_inbound_observer_when_disabled() {
    let settings = parse_toml_str(
        r#"
[observability.diagnostics]
remote_delivery_failures = false
serialization_failures = false
"#,
    )
    .unwrap();
    let diagnostics = Arc::new(CollectingRemoteDiagnostics::default());

    assert!(
        settings
            .observability
            .diagnostics
            .remote_inbound_diagnostics(
                diagnostics as Arc<dyn kairo_remote::RemoteInboundDiagnostics>
            )
            .is_none()
    );
}

#[cfg(feature = "remote")]
#[test]
fn diagnostics_config_filters_remote_association_categories() {
    let settings = parse_toml_str(
        r#"
[observability.diagnostics]
quarantine_events = true
association_close_events = false
"#,
    )
    .unwrap();
    let diagnostics = Arc::new(CollectingAssociationDiagnostics::default());
    let observer = settings
        .observability
        .diagnostics
        .remote_association_diagnostics(
            diagnostics.clone() as Arc<dyn kairo_remote::RemoteAssociationDiagnostics>
        )
        .expect("association diagnostics should install observer");

    observer.record(kairo_remote::RemoteAssociationDiagnostic::Quarantined {
        remote: "kairo://remote@127.0.0.1:25520".to_string(),
        remote_uid: Some(12),
        reason: "uid mismatch".to_string(),
    });
    observer.record(kairo_remote::RemoteAssociationDiagnostic::Closed {
        remote: "kairo://remote@127.0.0.1:25520".to_string(),
        reason: "transport stopped".to_string(),
    });

    assert_eq!(
        diagnostics.records(),
        vec![kairo_remote::RemoteAssociationDiagnostic::Quarantined {
            remote: "kairo://remote@127.0.0.1:25520".to_string(),
            remote_uid: Some(12),
            reason: "uid mismatch".to_string(),
        }]
    );
}

#[cfg(feature = "remote")]
#[test]
fn diagnostics_config_omits_remote_association_observer_when_disabled() {
    let settings = parse_toml_str(
        r#"
[observability.diagnostics]
quarantine_events = false
association_close_events = false
"#,
    )
    .unwrap();
    let diagnostics = Arc::new(CollectingAssociationDiagnostics::default());

    assert!(
        settings
            .observability
            .diagnostics
            .remote_association_diagnostics(
                diagnostics as Arc<dyn kairo_remote::RemoteAssociationDiagnostics>
            )
            .is_none()
    );
}

#[cfg(feature = "cluster")]
#[test]
fn diagnostics_config_filters_cluster_gossip_state_changes() {
    let settings = parse_toml_str(
        r#"
[observability.diagnostics]
gossip_state_changes = true
"#,
    )
    .unwrap();
    let diagnostics = Arc::new(CollectingClusterDiagnostics::default());
    let observer = settings
        .observability
        .diagnostics
        .cluster_diagnostics(diagnostics.clone() as Arc<dyn kairo_cluster::ClusterDiagnostics>)
        .expect("gossip diagnostics should install observer");

    observer.record(kairo_cluster::ClusterDiagnostic::GossipStateChanged {
        previous: kairo_cluster::Gossip::new(),
        current: kairo_cluster::Gossip::new(),
        events: Vec::new(),
    });

    assert_eq!(diagnostics.records().len(), 1);
}

#[cfg(feature = "cluster")]
#[test]
fn diagnostics_config_omits_cluster_observer_when_gossip_diagnostics_disabled() {
    let settings = parse_toml_str(
        r#"
[observability.diagnostics]
gossip_state_changes = false
"#,
    )
    .unwrap();
    let diagnostics = Arc::new(CollectingClusterDiagnostics::default());

    assert!(
        settings
            .observability
            .diagnostics
            .cluster_diagnostics(diagnostics as Arc<dyn kairo_cluster::ClusterDiagnostics>)
            .is_none()
    );
}

#[test]
fn toml_config_rejects_invalid_observability_diagnostics_type() {
    let error = parse_toml_str(
        r#"
[observability.diagnostics]
dead_letters = "yes"
"#,
    )
    .unwrap_err();

    assert_eq!(
        error,
        ConfigError::InvalidType {
            path: "observability.diagnostics.dead_letters".to_string(),
            expected: "a boolean".to_string(),
        }
    );
}

#[test]
fn toml_config_rejects_unknown_observability_keys() {
    let error = parse_toml_str(
        r#"
[observability]
logging = true
"#,
    )
    .unwrap_err();

    assert_eq!(
        error,
        ConfigError::UnknownKey {
            path: "observability.logging".to_string(),
        }
    );
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
fn toml_config_rejects_empty_seed_node() {
    let error = parse_toml_str(
        r#"
[cluster.seed]
nodes = [""]
"#,
    )
    .unwrap_err();

    assert_eq!(
        error,
        ConfigError::InvalidValue {
            path: "cluster.seed.nodes[0]".to_string(),
            reason: "must not be empty".to_string(),
        }
    );
}

#[test]
fn toml_config_reports_parse_failure() {
    let error = parse_toml_str("[actor.dispatchers.default").unwrap_err();

    match error {
        ConfigError::ParseFailed { reason } => {
            assert!(!reason.is_empty());
        }
        other => panic!("expected parse failure, got {other:?}"),
    }
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
fn toml_config_load_file_reports_read_failure_path() {
    let mut path = std::env::temp_dir();
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    path.push(format!("kairo-config-missing-{nonce}.toml"));

    let error = load_toml_file(&path).unwrap_err();

    match error {
        ConfigError::ReadFailed {
            path: error_path,
            reason,
        } => {
            assert_eq!(error_path, path);
            assert!(!reason.is_empty());
        }
        other => panic!("expected read failure, got {other:?}"),
    }
}

#[test]
fn toml_config_loads_layered_files_with_later_overrides() {
    let mut base_path = std::env::temp_dir();
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    base_path.push(format!("kairo-config-base-{nonce}.toml"));
    let mut local_path = std::env::temp_dir();
    local_path.push(format!("kairo-config-local-{nonce}.toml"));

    fs::write(
        &base_path,
        r#"
[actor.dispatchers.default]
throughput = 5

[actor.mailboxes.default]
capacity = 16

[remote.transport]
canonical_hostname = "10.0.0.10"
canonical_port = 25520

[cluster.seed]
nodes = ["kairo://app@10.0.0.1:25520"]
"#,
    )
    .unwrap();
    fs::write(
        &local_path,
        r#"
[actor.dispatchers.default]
throughput = 9

[remote.transport]
canonical_port = 26666

[cluster.seed]
nodes = ["kairo://app@127.0.0.1:26666"]
"#,
    )
    .unwrap();

    let settings = load_toml_files([base_path.as_path(), local_path.as_path()]).unwrap();
    fs::remove_file(base_path).unwrap();
    fs::remove_file(local_path).unwrap();

    assert_eq!(
        settings.actor.dispatchers["default"],
        DispatcherConfig {
            throughput: 9,
            ..Default::default()
        }
    );
    assert_eq!(
        settings.actor.mailboxes["default"],
        MailboxConfig { capacity: Some(16) }
    );
    assert_eq!(settings.remote.transport.canonical_hostname, "10.0.0.10");
    assert_eq!(settings.remote.transport.canonical_port, 26666);
    assert_eq!(
        settings.cluster.seed.nodes,
        vec!["kairo://app@127.0.0.1:26666".to_string()]
    );
}

#[test]
fn toml_config_layered_files_defaults_empty_iterator() {
    let settings = load_toml_files(std::iter::empty::<&str>()).unwrap();

    assert_eq!(settings, KairoSettings::default());
}

#[test]
fn toml_config_standard_files_default_when_absent() {
    let mut dir = std::env::temp_dir();
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    dir.push(format!("kairo-config-standard-empty-{nonce}"));
    fs::create_dir(&dir).unwrap();

    let paths = find_standard_toml_files(&dir);
    let settings = load_standard_toml_files(&dir).unwrap();
    fs::remove_dir(&dir).unwrap();

    assert!(paths.is_empty());
    assert_eq!(settings, KairoSettings::default());
    assert_eq!(STANDARD_TOML_FILES, ["kairo.toml", "kairo.local.toml"]);
}

#[test]
fn toml_config_standard_files_loads_base_only() {
    let mut dir = std::env::temp_dir();
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    dir.push(format!("kairo-config-standard-base-{nonce}"));
    fs::create_dir(&dir).unwrap();
    let base_path = dir.join("kairo.toml");

    fs::write(
        &base_path,
        r#"
[remote.transport]
canonical_port = 26666
"#,
    )
    .unwrap();

    let paths = find_standard_toml_files(&dir);
    let settings = load_standard_toml_files(&dir).unwrap();
    fs::remove_file(&base_path).unwrap();
    fs::remove_dir(&dir).unwrap();

    assert_eq!(paths, vec![base_path]);
    assert_eq!(settings.remote.transport.canonical_port, 26666);
}

#[test]
fn toml_config_standard_files_loads_local_override_after_base() {
    let mut dir = std::env::temp_dir();
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    dir.push(format!("kairo-config-standard-layered-{nonce}"));
    fs::create_dir(&dir).unwrap();
    let base_path = dir.join("kairo.toml");
    let local_path = dir.join("kairo.local.toml");

    fs::write(
        &base_path,
        r#"
[actor.dispatchers.default]
throughput = 5

[remote.transport]
canonical_hostname = "10.0.0.10"
canonical_port = 25520
"#,
    )
    .unwrap();
    fs::write(
        &local_path,
        r#"
[actor.dispatchers.default]
throughput = 11

[remote.transport]
canonical_port = 27777
"#,
    )
    .unwrap();

    let paths = find_standard_toml_files(&dir);
    let settings = load_standard_toml_files(&dir).unwrap();
    fs::remove_file(&base_path).unwrap();
    fs::remove_file(&local_path).unwrap();
    fs::remove_dir(&dir).unwrap();

    assert_eq!(paths, vec![base_path, local_path]);
    assert_eq!(settings.actor.default_dispatcher().unwrap().throughput, 11);
    assert_eq!(settings.remote.transport.canonical_hostname, "10.0.0.10");
    assert_eq!(settings.remote.transport.canonical_port, 27777);
}

#[test]
fn toml_config_layered_files_reports_missing_layer_path() {
    let mut base_path = std::env::temp_dir();
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    base_path.push(format!("kairo-config-present-{nonce}.toml"));
    let mut missing_path = std::env::temp_dir();
    missing_path.push(format!("kairo-config-missing-layer-{nonce}.toml"));

    fs::write(
        &base_path,
        r#"
[actor.dispatchers.default]
throughput = 9
"#,
    )
    .unwrap();

    let error = load_toml_files([base_path.as_path(), missing_path.as_path()]).unwrap_err();
    fs::remove_file(base_path).unwrap();

    match error {
        ConfigError::ReadFailed { path, reason } => {
            assert_eq!(path, missing_path);
            assert!(!reason.is_empty());
        }
        other => panic!("expected missing layer read failure, got {other:?}"),
    }
}

#[test]
fn toml_config_layered_files_merge_nested_tables_recursively() {
    let mut base_path = std::env::temp_dir();
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    base_path.push(format!("kairo-config-nested-base-{nonce}.toml"));
    let mut local_path = std::env::temp_dir();
    local_path.push(format!("kairo-config-nested-local-{nonce}.toml"));

    fs::write(
        &base_path,
        r#"
[cluster.sharding]
number_of_shards = 64
remember_entities = false
retry_interval = "2s"
handoff_timeout = "60s"
shard_failure_backoff = "10s"
rebalance_interval = "10s"
shard_region_query_timeout = "3s"

[cluster.sharding.least_shard_allocation]
rebalance_absolute_limit = 10
rebalance_relative_limit = 0.1

[observability.diagnostics]
dead_letters = true
remote_delivery_failures = true
serialization_failures = true
quarantine_events = true
association_close_events = true
gossip_state_changes = true
"#,
    )
    .unwrap();
    fs::write(
        &local_path,
        r#"
[cluster.sharding]
number_of_shards = 128
remember_entities = true

[cluster.sharding.least_shard_allocation]
rebalance_absolute_limit = 4

[observability.diagnostics]
dead_letters = false
quarantine_events = false
"#,
    )
    .unwrap();

    let settings = load_toml_files([base_path.as_path(), local_path.as_path()]).unwrap();
    fs::remove_file(base_path).unwrap();
    fs::remove_file(local_path).unwrap();

    assert_eq!(settings.cluster.sharding.number_of_shards, 128);
    assert!(settings.cluster.sharding.remember_entities);
    assert_eq!(
        settings.cluster.sharding.retry_interval,
        Duration::from_secs(2)
    );
    assert_eq!(
        settings.cluster.sharding.handoff_timeout,
        Duration::from_secs(60)
    );
    assert_eq!(
        settings.cluster.sharding.shard_failure_backoff,
        Duration::from_secs(10)
    );
    assert_eq!(
        settings.cluster.sharding.rebalance_interval,
        Duration::from_secs(10)
    );
    assert_eq!(
        settings.cluster.sharding.shard_region_query_timeout,
        Duration::from_secs(3)
    );
    assert_eq!(
        settings
            .cluster
            .sharding
            .least_shard_allocation
            .rebalance_absolute_limit,
        4
    );
    assert_eq!(
        settings
            .cluster
            .sharding
            .least_shard_allocation
            .rebalance_relative_limit,
        0.1
    );
    assert!(!settings.observability.diagnostics.dead_letters);
    assert!(settings.observability.diagnostics.remote_delivery_failures);
    assert!(settings.observability.diagnostics.serialization_failures);
    assert!(!settings.observability.diagnostics.quarantine_events);
    assert!(settings.observability.diagnostics.association_close_events);
    assert!(settings.observability.diagnostics.gossip_state_changes);
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
fn toml_config_rejects_blank_downing_role() {
    let error = parse_toml_str(
        r#"
[cluster.downing]
strategy = "keep-majority"
role = "   "
"#,
    )
    .unwrap_err();

    assert_eq!(
        error,
        ConfigError::InvalidValue {
            path: "cluster.downing.role".to_string(),
            reason: "must not be empty when set".to_string(),
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
fn toml_config_rejects_zero_remote_connect_timeout() {
    let error = parse_toml_str(
        r#"
[remote.transport]
connect_timeout = "0ms"
"#,
    )
    .unwrap_err();

    assert_eq!(
        error,
        ConfigError::InvalidValue {
            path: "remote.transport.connect_timeout".to_string(),
            reason: "must be greater than zero".to_string(),
        }
    );
}

#[test]
fn toml_config_rejects_blank_remote_hostname() {
    let error = parse_toml_str(
        r#"
[remote.transport]
canonical_hostname = "   "
"#,
    )
    .unwrap_err();

    assert_eq!(
        error,
        ConfigError::InvalidValue {
            path: "remote.transport.canonical_hostname".to_string(),
            reason: "must not be empty".to_string(),
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
fn toml_config_rejects_invalid_least_shard_allocation_limits() {
    for (toml, path, reason) in [
        (
            r#"
[cluster.sharding.least_shard_allocation]
rebalance_absolute_limit = 0
"#,
            "cluster.sharding.least_shard_allocation.rebalance_absolute_limit",
            "must be greater than zero",
        ),
        (
            r#"
[cluster.sharding.least_shard_allocation]
rebalance_relative_limit = 0.0
"#,
            "cluster.sharding.least_shard_allocation.rebalance_relative_limit",
            "must be finite and greater than zero",
        ),
        (
            r#"
[cluster.sharding.least_shard_allocation]
rebalance_relative_limit = -0.1
"#,
            "cluster.sharding.least_shard_allocation.rebalance_relative_limit",
            "must be finite and greater than zero",
        ),
    ] {
        let error = parse_toml_str(toml).unwrap_err();

        assert_eq!(
            error,
            ConfigError::InvalidValue {
                path: path.to_string(),
                reason: reason.to_string(),
            }
        );
    }
}

#[test]
fn toml_config_rejects_zero_sharding_runtime_durations() {
    for (key, path) in [
        ("retry_interval", "cluster.sharding.retry_interval"),
        ("handoff_timeout", "cluster.sharding.handoff_timeout"),
        (
            "shard_failure_backoff",
            "cluster.sharding.shard_failure_backoff",
        ),
        (
            "shard_region_query_timeout",
            "cluster.sharding.shard_region_query_timeout",
        ),
    ] {
        let error = parse_toml_str(&format!(
            r#"
[cluster.sharding]
{key} = "0ms"
"#
        ))
        .unwrap_err();

        assert_eq!(
            error,
            ConfigError::InvalidValue {
                path: path.to_string(),
                reason: "must be greater than zero".to_string(),
            }
        );
    }
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
fn toml_config_rejects_zero_executor_counts() {
    for (toml, path, reason) in [
        (
            "[actor.dispatchers.default]\nworkers = 0\n",
            "actor.dispatchers.default.workers",
            "must be greater than zero when set",
        ),
        (
            "[actor.task_executor]\nworkers = 0\n",
            "actor.task_executor.workers",
            "must be greater than zero when set",
        ),
        (
            "[actor.task_executor]\nqueue_capacity = 0\n",
            "actor.task_executor.queue_capacity",
            "must be greater than zero",
        ),
    ] {
        assert_eq!(
            parse_toml_str(toml).unwrap_err(),
            ConfigError::InvalidValue {
                path: path.to_string(),
                reason: reason.to_string(),
            }
        );
    }
}

#[test]
fn toml_config_rejects_empty_singleton_role() {
    let error = parse_toml_str(
        r#"
[cluster.tools.singleton]
role = ""
"#,
    )
    .unwrap_err();

    assert_eq!(
        error,
        ConfigError::InvalidValue {
            path: "cluster.tools.singleton.role".to_string(),
            reason: "must not be empty when set".to_string(),
        }
    );
}

#[test]
fn toml_config_rejects_zero_singleton_handover_retry_interval() {
    let error = parse_toml_str(
        r#"
[cluster.tools.singleton]
hand_over_retry_interval = "0ms"
"#,
    )
    .unwrap_err();

    assert_eq!(
        error,
        ConfigError::InvalidValue {
            path: "cluster.tools.singleton.hand_over_retry_interval".to_string(),
            reason: "must be greater than zero".to_string(),
        }
    );
}

#[test]
fn toml_config_rejects_invalid_pubsub_runtime_values() {
    for (key, path) in [
        (
            "gossip_interval = \"0ms\"",
            "cluster.tools.pubsub.gossip_interval",
        ),
        (
            "max_delta_entries = 0",
            "cluster.tools.pubsub.max_delta_entries",
        ),
    ] {
        let error = parse_toml_str(&format!(
            r#"
[cluster.tools.pubsub]
{key}
"#
        ))
        .unwrap_err();

        assert_eq!(
            error,
            ConfigError::InvalidValue {
                path: path.to_string(),
                reason: "must be greater than zero".to_string(),
            }
        );
    }
}

#[test]
fn toml_config_rejects_blank_singleton_role_after_projection() {
    let error = parse_toml_str(
        r#"
[cluster.tools.singleton]
role = "   "
"#,
    )
    .unwrap_err();

    assert_eq!(
        error,
        ConfigError::InvalidValue {
            path: "cluster.tools.singleton.role".to_string(),
            reason: "must not be empty when set".to_string(),
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
workers = 2

[actor.mailboxes.default]
capacity = 3

[actor.task_executor]
workers = 3
queue_capacity = 19
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
    assert_eq!(system.dispatcher_settings().workers(), 2);
    assert_eq!(system.mailbox_settings().user_capacity(), Some(3));
    assert_eq!(system.task_executor_settings().workers(), 3);
    assert_eq!(system.task_executor_settings().queue_capacity(), 19);
}

#[test]
#[cfg(feature = "actor")]
fn settings_actor_system_builder_applies_dead_letter_diagnostics() {
    use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, Context, DeadLetter, Props};

    struct DeadLetterForwarder {
        observed: mpsc::Sender<DeadLetter>,
    }

    impl Actor for DeadLetterForwarder {
        type Msg = DeadLetter;

        fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
            self.observed
                .send(msg)
                .map_err(|error| ActorError::Message(error.to_string()))
        }
    }

    let settings = parse_toml_str(
        r#"
[observability.diagnostics]
dead_letters = false
"#,
    )
    .unwrap();
    let system = settings
        .actor_system_builder("configured-dead-letter-diagnostics")
        .unwrap()
        .build()
        .unwrap();
    let (dead_letter_tx, dead_letter_rx) = mpsc::channel();
    let subscriber = system
        .spawn(
            "dead-letter-subscriber",
            Props::new(move || DeadLetterForwarder {
                observed: dead_letter_tx,
            }),
        )
        .unwrap();
    assert!(system.event_stream().subscribe(subscriber));
    let missing: ActorRef<u8> =
        system.missing_ref("kairo://configured-dead-letter-diagnostics/user/missing#404");

    missing.tell(7).unwrap_err();

    assert!(
        system
            .dead_letters()
            .wait_for_len(1, Duration::from_secs(1))
    );
    assert_eq!(
        system.dead_letters().records()[0].recipient(),
        missing.path()
    );
    assert!(
        dead_letter_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err()
    );
    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
#[cfg(all(feature = "remote", feature = "cluster"))]
fn config_converts_remote_and_cluster_settings() {
    let settings = parse_toml_str(
        r#"
[remote.transport]
canonical_hostname = "127.0.0.42"
canonical_port = 26666
connect_timeout = "1500ms"

[cluster.seed]
nodes = [
  "kairo://cluster@seed-a.example.test:25520",
  "kairo://cluster@seed-b.example.test",
]

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
    assert_eq!(remote.connect_timeout, Some(Duration::from_millis(1500)));
    let seeds = settings
        .cluster
        .seed
        .to_remote_association_addresses()
        .unwrap();
    assert_eq!(seeds.len(), 2);
    assert_eq!(seeds[0].system(), "cluster");
    assert_eq!(seeds[0].host(), "seed-a.example.test");
    assert_eq!(seeds[0].port(), Some(25520));
    assert_eq!(seeds[1].host(), "seed-b.example.test");
    assert_eq!(seeds[1].port(), None);

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
#[cfg(all(feature = "remote", feature = "cluster"))]
fn config_converts_downing_strategy_to_runtime_hook() {
    use crate::cluster::{
        DowningDecision, DowningHook, DowningPlan, Gossip, Member, MemberStatus, Reachability,
        UniqueAddress,
    };
    use kairo_actor::Address;

    let settings = parse_toml_str(
        r#"
[cluster.downing]
strategy = "keep-majority"
role = "backend"
"#,
    )
    .unwrap();
    let hook = settings.cluster.downing.to_downing_hook().unwrap();
    let self_node = node("a", 1);
    let peer = node("b", 2);
    let unreachable = node("c", 3);
    let gossip = Gossip::from_members([
        Member::new(self_node.clone(), vec!["backend".to_string()]).with_status(MemberStatus::Up),
        Member::new(peer, vec!["frontend".to_string()]).with_status(MemberStatus::Up),
        Member::new(unreachable.clone(), vec!["backend".to_string()]).with_status(MemberStatus::Up),
    ])
    .with_reachability(Reachability::new().unreachable(self_node.clone(), unreachable.clone()));

    let plan = DowningPlan::from_hook(&hook, &gossip, &self_node);

    assert_eq!(
        hook.decide(&gossip, &self_node),
        DowningDecision::DownUnreachable
    );
    assert_eq!(plan.nodes_to_down(), &[unreachable]);

    fn node(name: &str, uid: u64) -> UniqueAddress {
        UniqueAddress::new(
            Address::new(
                "kairo",
                name,
                Some(format!("{name}.example.test")),
                Some(2552),
            ),
            uid,
        )
    }
}

#[test]
#[cfg(all(feature = "remote", feature = "cluster"))]
fn config_converts_lease_majority_with_explicit_lease() {
    use crate::cluster::{
        DowningDecision, DowningHook, DowningPlan, Gossip, LeaseMajorityLease, Member,
        MemberStatus, Reachability, UniqueAddress,
    };
    use kairo_actor::Address;

    struct AlwaysAcquire;

    impl LeaseMajorityLease for AlwaysAcquire {
        fn acquire(&self, lease_name: &str) -> bool {
            lease_name == "cluster-sbr"
        }
    }

    let settings = parse_toml_str(
        r#"
[cluster.downing]
strategy = "lease-majority"
lease_name = "cluster-sbr"
role = "backend"
acquire_lease_delay_for_minority = "3s"
release_after = "30s"
"#,
    )
    .unwrap();

    assert!(matches!(
        settings.cluster.downing.to_downing_hook().unwrap_err(),
        ConfigError::InvalidValue { .. }
    ));
    let lease_settings = settings
        .cluster
        .downing
        .to_lease_majority_settings()
        .unwrap();
    assert_eq!(lease_settings.lease_name(), "cluster-sbr");
    assert_eq!(lease_settings.role(), Some("backend"));
    assert_eq!(
        lease_settings.acquire_lease_delay_for_minority(),
        Duration::from_secs(3)
    );
    let hook = settings
        .cluster
        .downing
        .to_lease_majority_hook(AlwaysAcquire)
        .unwrap();
    let self_node = node("a", 1);
    let unreachable = node("b", 2);
    let gossip = Gossip::from_members([
        Member::new(self_node.clone(), vec!["backend".to_string()]).with_status(MemberStatus::Up),
        Member::new(unreachable.clone(), vec!["backend".to_string()]).with_status(MemberStatus::Up),
    ])
    .with_reachability(Reachability::new().unreachable(self_node.clone(), unreachable.clone()));

    let plan = DowningPlan::from_hook(&hook, &gossip, &self_node);

    assert_eq!(
        hook.decide(&gossip, &self_node),
        DowningDecision::DownUnreachable
    );
    assert_eq!(plan.nodes_to_down(), &[unreachable]);

    fn node(name: &str, uid: u64) -> UniqueAddress {
        UniqueAddress::new(
            Address::new(
                "kairo",
                name,
                Some(format!("{name}.example.test")),
                Some(2552),
            ),
            uid,
        )
    }
}

#[test]
#[cfg(feature = "cluster-sharding")]
fn config_converts_sharding_settings_to_runtime_helpers() {
    let settings = parse_toml_str(
        r#"
[cluster.sharding]
number_of_shards = 128
remember_entities = true
retry_interval = "3s"
handoff_timeout = "45s"
shard_failure_backoff = "12s"
rebalance_interval = "30s"
shard_region_query_timeout = "4s"

[cluster.sharding.least_shard_allocation]
rebalance_absolute_limit = 4
rebalance_relative_limit = 0.25
"#,
    )
    .unwrap();

    assert_eq!(settings.cluster.sharding.to_shard_count().unwrap(), 128);
    assert!(settings.cluster.sharding.remember_entities_enabled());
    assert_eq!(
        settings.cluster.sharding.to_retry_interval().unwrap(),
        Duration::from_secs(3)
    );
    assert_eq!(
        settings.cluster.sharding.to_handoff_timeout().unwrap(),
        Duration::from_secs(45)
    );
    assert_eq!(
        settings
            .cluster
            .sharding
            .to_shard_failure_backoff()
            .unwrap(),
        Duration::from_secs(12)
    );
    assert_eq!(
        settings.cluster.sharding.to_rebalance_interval().unwrap(),
        Duration::from_secs(30)
    );
    assert_eq!(
        settings
            .cluster
            .sharding
            .to_shard_region_query_timeout()
            .unwrap(),
        Duration::from_secs(4)
    );
    assert!(
        !settings
            .cluster
            .sharding
            .default_shard_count_matches_runtime()
    );
    let strategy = settings
        .cluster
        .sharding
        .to_least_shard_allocation_strategy()
        .unwrap();
    assert_eq!(strategy.absolute_limit(), 4);
    assert_eq!(strategy.relative_limit(), 0.25);
    assert_eq!(
        settings.cluster.sharding.shard_id_for("counter-1").unwrap(),
        crate::cluster_sharding::shard_id_for("counter-1", 128).unwrap()
    );
}

#[test]
#[cfg(all(feature = "cluster", feature = "cluster-tools"))]
fn config_converts_cluster_tools_settings_to_runtime_helpers() {
    use crate::cluster::{Member, UniqueAddress};
    use kairo_actor::Address;

    let settings = parse_toml_str(
        r#"
[cluster.tools.singleton]
role = "backend"
hand_over_retry_interval = "125ms"

[cluster.tools.pubsub]
gossip_interval = "250ms"
max_delta_entries = 7
"#,
    )
    .unwrap();

    let scope = settings.cluster.tools.to_singleton_scope().unwrap();
    let self_node = node("tools", 1);
    let backend = Member::new(self_node.clone(), vec!["backend".to_string()]);
    let frontend = Member::new(node("frontend", 2), vec!["frontend".to_string()]);

    assert_eq!(scope.role(), Some("backend"));
    assert!(scope.includes(&backend));
    assert!(!scope.includes(&frontend));
    assert_eq!(
        settings
            .cluster
            .tools
            .to_singleton_hand_over_retry_interval()
            .unwrap(),
        Duration::from_millis(125)
    );
    let manager_settings = settings
        .cluster
        .tools
        .to_singleton_manager_settings()
        .unwrap();
    assert_eq!(
        manager_settings.hand_over_retry_interval(),
        Duration::from_millis(125)
    );
    assert!(manager_settings.automatic_hand_over_retries());
    assert_eq!(
        settings.cluster.tools.to_pubsub_gossip_interval().unwrap(),
        Duration::from_millis(250)
    );
    assert_eq!(
        settings
            .cluster
            .tools
            .to_pubsub_max_delta_entries()
            .unwrap(),
        7
    );

    let gossip = settings
        .cluster
        .tools
        .to_pubsub_gossip_actor(self_node.clone())
        .unwrap();
    assert_eq!(gossip.registry().self_node(), &self_node);
    assert_eq!(gossip.max_delta_entries(), 7);

    fn node(name: &str, uid: u64) -> UniqueAddress {
        UniqueAddress::new(
            Address::new(
                "kairo",
                name,
                Some(format!("{name}.example.test")),
                Some(2552),
            ),
            uid,
        )
    }
}

#[test]
#[cfg(feature = "actor")]
fn config_runtime_helpers_validate_directly_constructed_settings() {
    let actor = ActorConfig {
        dispatchers: BTreeMap::from([(
            "other".to_string(),
            DispatcherConfig {
                throughput: 1,
                ..Default::default()
            },
        )]),
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
                DispatcherConfig {
                    throughput: 0,
                    ..Default::default()
                },
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
                (
                    "default".to_string(),
                    DispatcherConfig {
                        throughput: 5,
                        ..Default::default()
                    },
                ),
                (
                    "blocking".to_string(),
                    DispatcherConfig {
                        throughput: 0,
                        ..Default::default()
                    },
                ),
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
            downing: super::ClusterDowningConfig {
                strategy: ClusterDowningStrategyConfig::KeepOldest {
                    role: Some("   ".to_string()),
                    down_if_alone: false,
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
            path: "cluster.downing.role".to_string(),
            reason: "must not be empty when set".to_string(),
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

    for (config, path) in [
        (
            super::ClusterShardingConfig {
                retry_interval: Duration::ZERO,
                ..Default::default()
            },
            "cluster.sharding.retry_interval",
        ),
        (
            super::ClusterShardingConfig {
                handoff_timeout: Duration::ZERO,
                ..Default::default()
            },
            "cluster.sharding.handoff_timeout",
        ),
        (
            super::ClusterShardingConfig {
                shard_failure_backoff: Duration::ZERO,
                ..Default::default()
            },
            "cluster.sharding.shard_failure_backoff",
        ),
        (
            super::ClusterShardingConfig {
                shard_region_query_timeout: Duration::ZERO,
                ..Default::default()
            },
            "cluster.sharding.shard_region_query_timeout",
        ),
    ] {
        let settings = KairoSettings {
            cluster: super::ClusterConfig {
                sharding: config,
                ..Default::default()
            },
            ..KairoSettings::default()
        };
        assert_eq!(
            settings.validate().unwrap_err(),
            ConfigError::InvalidValue {
                path: path.to_string(),
                reason: "must be greater than zero".to_string(),
            }
        );
    }

    for (allocation, path) in [
        (
            ClusterShardingAllocationConfig {
                rebalance_absolute_limit: 0,
                ..Default::default()
            },
            "cluster.sharding.least_shard_allocation.rebalance_absolute_limit",
        ),
        (
            ClusterShardingAllocationConfig {
                rebalance_relative_limit: 0.0,
                ..Default::default()
            },
            "cluster.sharding.least_shard_allocation.rebalance_relative_limit",
        ),
    ] {
        let settings = KairoSettings {
            cluster: super::ClusterConfig {
                sharding: super::ClusterShardingConfig {
                    least_shard_allocation: allocation,
                    ..Default::default()
                },
                ..Default::default()
            },
            ..KairoSettings::default()
        };
        assert!(matches!(
            settings.validate().unwrap_err(),
            ConfigError::InvalidValue { path: actual, .. } if actual == path
        ));
    }

    let settings = KairoSettings {
        cluster: super::ClusterConfig {
            seed: super::ClusterSeedConfig {
                nodes: vec![String::new()],
            },
            ..Default::default()
        },
        ..KairoSettings::default()
    };
    assert_eq!(
        settings.validate().unwrap_err(),
        ConfigError::InvalidValue {
            path: "cluster.seed.nodes[0]".to_string(),
            reason: "must not be empty".to_string(),
        }
    );

    let settings = KairoSettings {
        remote: super::RemoteConfig {
            transport: super::RemoteTransportConfig {
                canonical_hostname: "   ".to_string(),
                ..Default::default()
            },
        },
        ..KairoSettings::default()
    };
    assert_eq!(
        settings.validate().unwrap_err(),
        ConfigError::InvalidValue {
            path: "remote.transport.canonical_hostname".to_string(),
            reason: "must not be empty".to_string(),
        }
    );

    let settings = KairoSettings {
        remote: super::RemoteConfig {
            transport: super::RemoteTransportConfig {
                connect_timeout: Some(Duration::ZERO),
                ..Default::default()
            },
        },
        ..KairoSettings::default()
    };
    assert_eq!(
        settings.validate().unwrap_err(),
        ConfigError::InvalidValue {
            path: "remote.transport.connect_timeout".to_string(),
            reason: "must be greater than zero".to_string(),
        }
    );

    for (heartbeat, path) in [
        (
            super::ClusterHeartbeatConfig {
                monitored_by_nr_of_members: 0,
                ..Default::default()
            },
            "cluster.heartbeat.monitored_by_nr_of_members",
        ),
        (
            super::ClusterHeartbeatConfig {
                interval: Duration::ZERO,
                ..Default::default()
            },
            "cluster.heartbeat.interval",
        ),
        (
            super::ClusterHeartbeatConfig {
                expected_response_after: Duration::ZERO,
                ..Default::default()
            },
            "cluster.heartbeat.expected_response_after",
        ),
    ] {
        let settings = KairoSettings {
            cluster: super::ClusterConfig {
                heartbeat,
                ..Default::default()
            },
            ..KairoSettings::default()
        };
        assert_eq!(
            settings.validate().unwrap_err(),
            ConfigError::InvalidValue {
                path: path.to_string(),
                reason: "must be greater than zero".to_string(),
            }
        );
    }

    let settings = KairoSettings {
        cluster: super::ClusterConfig {
            downing: super::ClusterDowningConfig {
                stable_after: Duration::ZERO,
                ..Default::default()
            },
            ..Default::default()
        },
        ..KairoSettings::default()
    };
    assert_eq!(
        settings.validate().unwrap_err(),
        ConfigError::InvalidValue {
            path: "cluster.downing.stable_after".to_string(),
            reason: "must be greater than zero".to_string(),
        }
    );

    let settings = KairoSettings {
        cluster: super::ClusterConfig {
            downing: super::ClusterDowningConfig {
                strategy: ClusterDowningStrategyConfig::LeaseMajority {
                    lease_name: "cluster-sbr".to_string(),
                    role: None,
                    acquire_lease_delay_for_minority: Duration::ZERO,
                    release_after: Duration::ZERO,
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
            path: "cluster.downing.release_after".to_string(),
            reason: "must be greater than zero".to_string(),
        }
    );

    let settings = KairoSettings {
        cluster: super::ClusterConfig {
            sharding: super::ClusterShardingConfig {
                number_of_shards: 0,
                ..Default::default()
            },
            ..Default::default()
        },
        ..KairoSettings::default()
    };
    assert_eq!(
        settings.validate().unwrap_err(),
        ConfigError::InvalidValue {
            path: "cluster.sharding.number_of_shards".to_string(),
            reason: "must be greater than zero".to_string(),
        }
    );

    let settings = KairoSettings {
        cluster: super::ClusterConfig {
            tools: super::ClusterToolsConfig {
                singleton_role: Some(String::new()),
                ..Default::default()
            },
            ..Default::default()
        },
        ..KairoSettings::default()
    };
    assert_eq!(
        settings.validate().unwrap_err(),
        ConfigError::InvalidValue {
            path: "cluster.tools.singleton.role".to_string(),
            reason: "must not be empty when set".to_string(),
        }
    );

    for (tools, path) in [
        (
            super::ClusterToolsConfig {
                singleton_hand_over_retry_interval: Duration::ZERO,
                ..Default::default()
            },
            "cluster.tools.singleton.hand_over_retry_interval",
        ),
        (
            super::ClusterToolsConfig {
                pubsub_gossip_interval: Duration::ZERO,
                ..Default::default()
            },
            "cluster.tools.pubsub.gossip_interval",
        ),
        (
            super::ClusterToolsConfig {
                pubsub_max_delta_entries: 0,
                ..Default::default()
            },
            "cluster.tools.pubsub.max_delta_entries",
        ),
    ] {
        let settings = KairoSettings {
            cluster: super::ClusterConfig {
                tools,
                ..Default::default()
            },
            ..KairoSettings::default()
        };
        assert_eq!(
            settings.validate().unwrap_err(),
            ConfigError::InvalidValue {
                path: path.to_string(),
                reason: "must be greater than zero".to_string(),
            }
        );
    }

    assert!(KairoSettings::default().validate().is_ok());
}
