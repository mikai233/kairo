#[cfg(feature = "remote")]
#[derive(Debug)]
struct PreludeRemoteMsg;

fn repo_root() -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
    let crate_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    Ok(crate_dir
        .ancestors()
        .nth(3)
        .ok_or("kairo crate should live under kairo-next/crates/kairo")?
        .to_path_buf())
}

#[test]
fn root_workspace_members_stay_on_kairo_next() -> Result<(), Box<dyn std::error::Error>> {
    let repo_root = repo_root()?;
    let root_manifest = std::fs::read_to_string(repo_root.join("Cargo.toml"))?;
    let root_manifest = root_manifest.replace("\r\n", "\n");

    assert!(
        root_manifest.contains("[workspace]\n"),
        "root Cargo.toml must define the active workspace"
    );
    assert!(
        root_manifest.contains("members = [\"kairo-next/crates/*\"]"),
        "normal workspace builds must include only kairo-next crates"
    );
    assert!(
        !root_manifest.contains("\"crates/"),
        "legacy crates/ must remain reference-only, not workspace members"
    );
    assert!(
        !root_manifest.contains("path = \"crates/"),
        "workspace dependencies must not point at legacy crates/"
    );

    Ok(())
}

#[test]
fn next_crate_manifests_do_not_depend_on_legacy_crates() -> Result<(), Box<dyn std::error::Error>> {
    let repo_root = repo_root()?;
    let next_crates = repo_root.join("kairo-next").join("crates");
    let legacy_path_patterns = [
        "path = \"crates/",
        "path = \"../crates/",
        "path = \"../../crates/",
        "path = \"../../../crates/",
        "path = \"../../../../crates/",
    ];

    for entry in std::fs::read_dir(next_crates)? {
        let entry = entry?;
        let manifest_path = entry.path().join("Cargo.toml");
        if !manifest_path.is_file() {
            continue;
        }

        let manifest = std::fs::read_to_string(&manifest_path)?
            .replace("\r\n", "\n")
            .replace('\\', "/");

        for pattern in legacy_path_patterns {
            assert!(
                !manifest.contains(pattern),
                "{} must not depend on legacy crates/ with `{pattern}`",
                manifest_path.display()
            );
        }
    }

    Ok(())
}

#[test]
fn active_manifests_do_not_introduce_hocon() -> Result<(), Box<dyn std::error::Error>> {
    let repo_root = repo_root()?;
    let next_crates = repo_root.join("kairo-next").join("crates");
    let mut manifest_paths = vec![repo_root.join("Cargo.toml")];

    for entry in std::fs::read_dir(next_crates)? {
        let entry = entry?;
        let manifest_path = entry.path().join("Cargo.toml");
        if manifest_path.is_file() {
            manifest_paths.push(manifest_path);
        }
    }

    for manifest_path in manifest_paths {
        let manifest = std::fs::read_to_string(&manifest_path)?.to_ascii_lowercase();
        assert!(
            !manifest.contains("hocon"),
            "{} must keep TOML as the first config file format and must not add HOCON or hocon-rs before that parser is intentionally selected",
            manifest_path.display()
        );
    }

    Ok(())
}

#[test]
fn foundational_crates_keep_architecture_dependency_boundaries()
-> Result<(), Box<dyn std::error::Error>> {
    let repo_root = repo_root()?;
    let next_crates = repo_root.join("kairo-next").join("crates");
    let forbidden_dependencies = [
        (
            "kairo-actor",
            [
                "kairo-serialization",
                "kairo-remote",
                "kairo-cluster",
                "kairo-distributed-data",
                "kairo-cluster-sharding",
                "kairo-cluster-tools",
            ],
            "kairo-actor must remain the local runtime and know nothing about serialization, remoting, or cluster membership",
        ),
        (
            "kairo-serialization",
            [
                "kairo-actor",
                "kairo-remote",
                "kairo-cluster",
                "kairo-distributed-data",
                "kairo-cluster-sharding",
                "kairo-cluster-tools",
            ],
            "kairo-serialization must own wire metadata/codecs without depending on actors, transports, or cluster crates",
        ),
    ];

    for (crate_name, forbidden, reason) in forbidden_dependencies {
        let manifest_path = next_crates.join(crate_name).join("Cargo.toml");
        let manifest = std::fs::read_to_string(&manifest_path)?;
        for dependency in forbidden {
            assert!(
                !manifest.contains(dependency),
                "{} must not depend on `{dependency}`: {reason}",
                manifest_path.display()
            );
        }
    }

    Ok(())
}

#[test]
fn core_serialization_crate_stays_format_neutral() -> Result<(), Box<dyn std::error::Error>> {
    let repo_root = repo_root()?;
    let manifest_path = repo_root
        .join("kairo-next")
        .join("crates")
        .join("kairo-serialization")
        .join("Cargo.toml");
    let manifest = std::fs::read_to_string(&manifest_path)?.to_ascii_lowercase();
    let forbidden_format_dependencies = [
        "serde",
        "serde_json",
        "serde_cbor",
        "bincode",
        "prost",
        "postcard",
        "rmp-serde",
        "ciborium",
    ];

    for dependency in forbidden_format_dependencies {
        assert!(
            !manifest.contains(dependency),
            "{} must keep core serialization format-neutral; codec dependencies like `{dependency}` belong in optional helper crates or intentionally selected features outside the core crate",
            manifest_path.display()
        );
    }

    Ok(())
}

#[test]
fn distributed_crates_keep_architecture_dependency_boundaries()
-> Result<(), Box<dyn std::error::Error>> {
    let repo_root = repo_root()?;
    let next_crates = repo_root.join("kairo-next").join("crates");
    let forbidden_dependencies: [(&str, &[&str], &str); 4] = [
        (
            "kairo-remote",
            &[
                "kairo-cluster",
                "kairo-distributed-data",
                "kairo-cluster-sharding",
                "kairo-cluster-tools",
            ],
            "kairo-remote must provide remoting without depending on cluster, ddata, sharding, or tools layers",
        ),
        (
            "kairo-cluster",
            &[
                "kairo-distributed-data",
                "kairo-cluster-sharding",
                "kairo-cluster-tools",
            ],
            "kairo-cluster must own membership without depending on ddata, sharding, or tools layers",
        ),
        (
            "kairo-distributed-data",
            &["kairo-cluster-sharding", "kairo-cluster-tools"],
            "kairo-distributed-data may consume cluster and remote routes but must not depend on sharding or tools",
        ),
        (
            "kairo-cluster-sharding",
            &["kairo-cluster-tools"],
            "kairo-cluster-sharding must not depend on cluster tools private shortcuts",
        ),
    ];

    for (crate_name, forbidden, reason) in forbidden_dependencies {
        let manifest_path = next_crates.join(crate_name).join("Cargo.toml");
        let manifest = std::fs::read_to_string(&manifest_path)?;
        for dependency in forbidden {
            assert!(
                !manifest.contains(dependency),
                "{} must not depend on `{dependency}`: {reason}",
                manifest_path.display()
            );
        }
    }

    Ok(())
}

#[test]
fn support_crates_remain_leaf_facade_consumers() -> Result<(), Box<dyn std::error::Error>> {
    let repo_root = repo_root()?;
    let next_crates = repo_root.join("kairo-next").join("crates");
    let support_crates = ["kairo-examples", "kairo-benchmarks"];
    let runtime_crates = [
        "kairo",
        "kairo-actor",
        "kairo-actor-macros",
        "kairo-serialization",
        "kairo-remote",
        "kairo-cluster",
        "kairo-distributed-data",
        "kairo-cluster-sharding",
        "kairo-cluster-tools",
        "kairo-testkit",
    ];

    for support_crate in support_crates {
        let manifest_path = next_crates.join(support_crate).join("Cargo.toml");
        let manifest = std::fs::read_to_string(&manifest_path)?;
        assert!(
            manifest.contains("kairo = { path = \"../kairo\""),
            "{} must validate public workflows through the user-facing `kairo` facade",
            manifest_path.display()
        );
    }

    for runtime_crate in runtime_crates {
        let manifest_path = next_crates.join(runtime_crate).join("Cargo.toml");
        let manifest = std::fs::read_to_string(&manifest_path)?;
        for support_crate in support_crates {
            assert!(
                !manifest.contains(support_crate),
                "{} must not depend on leaf support crate `{support_crate}`",
                manifest_path.display()
            );
        }
    }

    Ok(())
}

#[test]
fn distributed_layers_do_not_introduce_authoritative_membership_store()
-> Result<(), Box<dyn std::error::Error>> {
    let repo_root = repo_root()?;
    let next_crates = repo_root.join("kairo-next").join("crates");
    let distributed_layers = [
        "kairo-cluster",
        "kairo-distributed-data",
        "kairo-cluster-sharding",
        "kairo-cluster-tools",
    ];
    let forbidden_terms = [
        concat!("et", "cd"),
        concat!("kuber", "netes"),
        "membership_store",
        "membershipstore",
        "centralmembershipstore",
    ];

    for crate_name in distributed_layers {
        let src = next_crates.join(crate_name).join("src");
        let mut files = Vec::new();
        collect_active_rs_files(&src, &mut files)?;

        for file in files {
            if file.file_name().and_then(|name| name.to_str()) == Some("lib.rs") {
                continue;
            }

            let source = std::fs::read_to_string(&file)?.replace("\r\n", "\n");
            for (line_index, line) in source.lines().enumerate() {
                let trimmed = line.trim_start();
                if trimmed.starts_with("//") {
                    continue;
                }

                let normalized_line = line.to_ascii_lowercase();
                for term in forbidden_terms {
                    assert!(
                        !normalized_line.contains(term),
                        "{}:{} must keep cluster membership gossip-based; distributed layers may consume cluster events but must not introduce a central membership authority",
                        file.display(),
                        line_index + 1
                    );
                }
            }
        }
    }

    Ok(())
}

#[test]
fn next_sources_do_not_expose_dyn_message_primary_api() -> Result<(), Box<dyn std::error::Error>> {
    let repo_root = repo_root()?;
    let next_crates = repo_root.join("kairo-next").join("crates");
    let forbidden_declarations = [
        "pub enum DynMessage",
        "enum DynMessage",
        "pub trait DynMessage",
        "trait DynMessage",
        "pub struct DynMessage",
        "struct DynMessage",
        "pub type DynMessage",
        "type DynMessage",
        "pub enum GlobalMessage",
        "enum GlobalMessage",
        "pub trait GlobalMessage",
        "trait GlobalMessage",
        "pub struct GlobalMessage",
        "struct GlobalMessage",
        "pub type GlobalMessage",
        "type GlobalMessage",
    ];

    let mut files = Vec::new();
    for entry in std::fs::read_dir(next_crates)? {
        let entry = entry?;
        let src = entry.path().join("src");
        if src.is_dir() {
            collect_active_rs_files(&src, &mut files)?;
        }
    }

    for file in files {
        let source = std::fs::read_to_string(&file)?.replace("\r\n", "\n");
        for declaration in forbidden_declarations {
            assert!(
                !source.contains(declaration),
                "{} must keep typed ActorRef<M> protocols as the primary user API; do not expose `{declaration}`",
                file.display()
            );
        }
    }

    Ok(())
}

fn collect_active_rs_files(
    directory: &std::path::Path,
    files: &mut Vec<std::path::PathBuf>,
) -> Result<(), Box<dyn std::error::Error>> {
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        let file_name = path.file_name().and_then(|name| name.to_str());
        if path.is_dir() {
            if file_name == Some("tests") {
                continue;
            }
            collect_active_rs_files(&path, files)?;
        } else if path.extension().and_then(|extension| extension.to_str()) == Some("rs")
            && !file_name.is_some_and(|name| name == "tests.rs" || name.contains("test"))
        {
            files.push(path);
        }
    }

    Ok(())
}

#[cfg(feature = "remote")]
impl crate::prelude::RemoteMessage for PreludeRemoteMsg {
    const MANIFEST: &'static str = "kairo.facade.test.PreludeRemoteMsg";
    const VERSION: u16 = 1;
}

#[cfg(feature = "remote")]
#[test]
fn prelude_exposes_remote_entry_points() {
    use crate::prelude::*;

    struct PreludeSink {
        received: std::sync::mpsc::Sender<()>,
    }

    impl Actor for PreludeSink {
        type Msg = PreludeRemoteMsg;

        fn receive(&mut self, _ctx: &mut Context<Self::Msg>, _msg: Self::Msg) -> ActorResult {
            self.received
                .send(())
                .map_err(|error| ActorError::Message(error.to_string()))
        }
    }

    fn assert_remote_outbound<T: RemoteOutbound + ?Sized>() {}

    let settings = RemoteSettings::new("127.0.0.1", 25520);
    assert_eq!(settings.canonical_hostname, "127.0.0.1");
    assert_eq!(settings.canonical_port, 25520);
    assert_remote_outbound::<dyn RemoteOutbound>();
    let system = ActorSystem::builder("facade-remote-prelude")
        .build()
        .unwrap();
    let (received_tx, received_rx) = std::sync::mpsc::channel();
    let local = system
        .spawn(
            "sink",
            Props::new(move || PreludeSink {
                received: received_tx,
            }),
        )
        .unwrap();
    let resolved = ResolvedActorRef::Local(local.clone());

    let _ = std::mem::size_of::<Option<RemoteActorRef<PreludeRemoteMsg>>>();
    let _ = std::mem::size_of::<Option<RemoteActorRefResolver<PreludeRemoteMsg>>>();
    let _ = std::mem::size_of::<Option<RemoteActorRefProvider>>();
    let _ = std::mem::size_of::<Option<ResolvedActorRef<PreludeRemoteMsg>>>();
    let _ = std::mem::size_of::<Option<TcpRemoteActorSystem<PreludeRemoteMsg>>>();
    assert!(resolved.is_local());
    assert_eq!(resolved.path(), local.path());
    resolved.tell(PreludeRemoteMsg).unwrap();
    received_rx
        .recv_timeout(std::time::Duration::from_secs(1))
        .unwrap();
    let error = RemoteError::Outbound("send failed".to_string());
    assert!(error.to_string().contains("send failed"));
}

#[cfg(feature = "distributed-data")]
#[test]
fn prelude_exposes_distributed_data_entry_points() {
    use crate::prelude::*;

    let replica = ReplicaId::new("node-a");
    let key = ReplicatorKey::new("counters.requests");
    let mut state = ReplicatorState::<GCounter>::new();
    let outcome = state
        .update_local(key.clone(), GCounter::new(), |counter| {
            counter.increment(replica, 2)
        })
        .expect("counter update should succeed");

    assert!(outcome.changed());
    assert!(matches!(state.get_local(&key), GetResponse::Success { .. }));
    let _ = std::mem::size_of::<Option<ReplicatorActor<GCounter>>>();
    let _ = std::mem::size_of::<Option<ReplicatorActorMsg<GCounter>>>();
    let _ = std::mem::size_of::<Option<UpdateResponse<<GCounter as DeltaReplicatedData>::Delta>>>();
    let _ = std::mem::size_of::<Option<GSet<String>>>();
    let _ = std::mem::size_of::<Option<ORSet<String>>>();
    let _ = std::mem::size_of::<Option<PNCounter>>();
    let _ = ReadConsistency::Local;
    let _ = WriteConsistency::Local;
}

#[cfg(feature = "cluster-sharding")]
#[test]
fn prelude_exposes_sharding_entry_points() {
    use crate::prelude::*;

    let envelope = ShardingEnvelope::new("entity-1", "credit".to_string());
    let (entity_id, message) = envelope.into_parts();
    assert_eq!(entity_id, "entity-1");
    assert_eq!(message, "credit");
    assert_eq!(
        shard_id_for("entity-1", DEFAULT_SHARD_COUNT).expect("valid shard count"),
        default_shard_id_for("entity-1")
    );
    assert_ne!(stable_hash_entity_id("entity-1"), 0);
    let _ = EntityTypeKey::<String>::new("Account");
    let _ = std::mem::size_of::<Option<EntityRef<String>>>();
    let _ = std::mem::size_of::<Option<ShardingEnvelopeRouter<String>>>();
    let _ = std::mem::size_of::<Option<ShardRegionActor<String>>>();
    let _ = std::mem::size_of::<Option<ShardRegionMsg<String>>>();
    let _ = ShardingError::InvalidShardCount;
}

#[cfg(feature = "cluster-tools")]
#[test]
fn prelude_exposes_cluster_tools_entry_points() {
    use crate::prelude::*;

    struct NoopSingleton;

    impl Actor for NoopSingleton {
        type Msg = String;

        fn receive(&mut self, _ctx: &mut Context<Self::Msg>, _msg: Self::Msg) -> ActorResult {
            Ok(())
        }
    }

    let topic = TopicName::new("events");
    assert_eq!(topic.as_str(), "events");
    assert_eq!(SingletonScope::for_role("backend").role(), Some("backend"));
    let _ = TopicPublishMode::Broadcast;
    let _ = std::mem::size_of::<Option<LocalPubSub<String>>>();
    let _ = std::mem::size_of::<Option<DistributedPubSubMediatorActor<String>>>();
    let _ = std::mem::size_of::<Option<DistributedPubSubMediatorMsg<String>>>();
    let _ = std::mem::size_of::<Option<LocalSingletonManagerActor<NoopSingleton>>>();
    let _ = std::mem::size_of::<Option<LocalSingletonManagerMsg<String>>>();
    let _ = std::mem::size_of::<Option<SingletonProxyActor<String>>>();
    let _ = std::mem::size_of::<Option<SingletonProxyMsg<String>>>();
}

#[cfg(feature = "testkit")]
#[test]
fn prelude_exposes_testkit_entry_points() -> Result<(), Box<dyn std::error::Error>> {
    use crate::prelude::*;

    let (kit, manual_time) = ActorSystemTestKit::with_manual_time("facade-testkit-prelude")?;
    let probe = kit.create_probe::<&'static str>("probe")?;
    let handle = manual_time.schedule_once(
        std::time::Duration::from_millis(1),
        probe.actor_ref(),
        "tick",
    );
    assert!(handle.cancel());
    let _ = std::mem::size_of::<Option<ManualTimeHandle>>();
    let _ = std::mem::size_of::<Option<ActorSystemTestKit>>();
    let _ = std::mem::size_of::<Option<MultiNode>>();
    let _ = std::mem::size_of::<Option<MultiNodeError>>();
    let _ = std::mem::size_of::<Option<MultiNodeResult<()>>>();
    let multi_node = MultiNodeTestKit::new(["facade-node-a", "facade-node-b"])?;
    assert_eq!(
        multi_node.node_names().collect::<Vec<_>>(),
        vec!["facade-node-a", "facade-node-b"]
    );
    let _ = std::mem::size_of::<Option<TestProbe<String>>>();
    let _ = std::mem::size_of::<Option<FishingOutcome>>();
    let _ = await_assert(
        std::time::Duration::from_millis(1),
        std::time::Duration::from_millis(1),
        || Ok::<_, &'static str>(()),
    );
    multi_node.shutdown(std::time::Duration::from_secs(1))?;
    kit.shutdown(std::time::Duration::from_secs(1))?;
    Ok(())
}
