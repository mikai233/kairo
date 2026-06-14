use std::collections::BTreeMap;
use std::fs;
#[cfg(feature = "actor")]
use std::sync::mpsc;
#[cfg(any(feature = "cluster", feature = "remote"))]
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::{
    ActorConfig, ClusterDowningStrategyConfig, ConfigError, DiagnosticsConfig, DispatcherConfig,
    KairoSettings, MailboxConfig, load_toml_file, parse_toml_str,
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

[observability.diagnostics]
dead_letters = true
remote_delivery_failures = false
serialization_failures = true
quarantine_events = false
gossip_state_changes = true
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
    assert!(settings.observability.diagnostics.dead_letters);
    assert!(!settings.observability.diagnostics.remote_delivery_failures);
    assert!(settings.observability.diagnostics.serialization_failures);
    assert!(!settings.observability.diagnostics.quarantine_events);
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
rebalance_interval = "30s"
"#,
    )
    .unwrap();

    assert_eq!(settings.cluster.sharding.to_shard_count().unwrap(), 128);
    assert_eq!(
        settings.cluster.sharding.to_rebalance_interval().unwrap(),
        Duration::from_secs(30)
    );
    assert!(
        !settings
            .cluster
            .sharding
            .default_shard_count_matches_runtime()
    );
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

    assert!(KairoSettings::default().validate().is_ok());
}
