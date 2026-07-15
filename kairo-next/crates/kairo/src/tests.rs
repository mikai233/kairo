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

const FACADE_FEATURE_EXPECTATIONS: [(&str, &str, &str); 8] = [
    (
        "default = [\"actor\", \"macros\", \"config\"]",
        "| `default` | `actor`, `macros`, `config` |",
        "the default facade must stay local/config-only",
    ),
    (
        "remote = [\"actor\", \"serialization\", \"dep:kairo-remote\"]",
        "| `remote` | `actor`, `serialization`, remote refs and associations |",
        "remoting must opt into local actors and stable serialization metadata",
    ),
    (
        "cluster = [\"remote\", \"dep:kairo-cluster\"]",
        "| `cluster` | `remote`, gossip membership and downing hooks |",
        "cluster must build on remoting instead of bypassing the layer boundary",
    ),
    (
        "distributed-data = [\"cluster\", \"dep:kairo-distributed-data\"]",
        "| `distributed-data` | `cluster`, CRDT replication |",
        "distributed data must build on cluster membership",
    ),
    (
        "cluster-sharding = [\"cluster\", \"distributed-data\", \"cluster-tools\", \"dep:kairo-cluster-sharding\"]",
        "| `cluster-sharding` | `cluster`, `distributed-data`, `cluster-tools`, entity routing |",
        "cluster sharding must build on cluster, distributed-data, and singleton support",
    ),
    (
        "cluster-tools = [\"cluster\", \"distributed-data\", \"dep:kairo-cluster-tools\"]",
        "| `cluster-tools` | `cluster`, `distributed-data`, singleton and pubsub |",
        "cluster tools must build on cluster and distributed-data support",
    ),
    (
        "testkit = [\"actor\", \"dep:kairo-testkit\"]",
        "| `testkit` | local actor test utilities without distributed runtime layers |",
        "testkit support should not pull in distributed runtime layers",
    ),
    (
        "full = [\"actor\", \"macros\", \"config\", \"serialization\", \"remote\", \"cluster\", \"distributed-data\", \"cluster-sharding\", \"cluster-tools\", \"testkit\"]",
        "| `full` | every public facade feature for integration checks |",
        "the explicit full feature should remain the all-surface integration bundle",
    ),
];

const M13_VALIDATION_GATE_EXPECTATIONS: [(&str, &str); 10] = [
    (
        "cargo fmt --all -- --check",
        "formatting must remain a release-readiness gate",
    ),
    (
        "cargo clippy --workspace --all-targets --all-features -- -D warnings",
        "workspace clippy with warnings denied must remain a release-readiness gate",
    ),
    (
        "cargo test --workspace --all-targets --all-features",
        "full workspace tests must remain a release-readiness gate",
    ),
    (
        "cargo test -p kairo-examples --all-targets --all-features",
        "public example workflows must remain in CI",
    ),
    (
        "cargo test --doc --workspace --all-features",
        "public API doctests must remain compile-tested across the workspace",
    ),
    (
        "cargo test -p kairo-examples --doc --all-features",
        "example rustdoc snippets must remain checked",
    ),
    (
        "cargo test -p kairo-testkit multi_node --all-targets --all-features",
        "deterministic multi-node testkit coverage must remain in CI",
    ),
    (
        "RUSTDOCFLAGS=\"-D warnings\" cargo doc --workspace --all-features --no-deps",
        "workspace rustdoc warnings must remain denied",
    ),
    (
        "cargo package --workspace --all-features --exclude kairo-examples --exclude kairo-benchmarks",
        "every public crate must assemble and verify from its release archive",
    ),
    (
        "KAIRO_BENCH_ITERS=100 cargo run -p kairo-benchmarks --release -- all",
        "M13 benchmark smoke coverage must exercise optimized release builds in CI",
    ),
];

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
fn user_facing_crates_deny_missing_public_documentation() -> Result<(), Box<dyn std::error::Error>>
{
    let repo_root = repo_root()?;
    let user_facing_crates = [
        "kairo-next/crates/kairo/src/lib.rs",
        "kairo-next/crates/kairo-actor/src/lib.rs",
        "kairo-next/crates/kairo-actor-macros/src/lib.rs",
        "kairo-next/crates/kairo-serialization/src/lib.rs",
        "kairo-next/crates/kairo-testkit/src/lib.rs",
    ];

    for relative_path in user_facing_crates {
        let source = std::fs::read_to_string(repo_root.join(relative_path))?.replace("\r\n", "\n");
        assert!(
            source.starts_with("#![deny(missing_docs)]\n"),
            "{relative_path} must keep missing public documentation as a hard error"
        );
    }

    Ok(())
}

#[test]
fn remote_boundary_modules_deny_missing_docs() -> Result<(), Box<dyn std::error::Error>> {
    let repo_root = repo_root()?;
    let documented_modules = [
        "kairo-next/crates/kairo-remote/src/association.rs",
        "kairo-next/crates/kairo-remote/src/association_cache.rs",
        "kairo-next/crates/kairo-remote/src/association_inbound.rs",
        "kairo-next/crates/kairo-remote/src/association_outbound.rs",
        "kairo-next/crates/kairo-remote/src/association_pipeline.rs",
        "kairo-next/crates/kairo-remote/src/association_registry.rs",
        "kairo-next/crates/kairo-remote/src/association_routes.rs",
        "kairo-next/crates/kairo-remote/src/codec.rs",
        "kairo-next/crates/kairo-remote/src/error.rs",
        "kairo-next/crates/kairo-remote/src/frame.rs",
        "kairo-next/crates/kairo-remote/src/inbound.rs",
        "kairo-next/crates/kairo-remote/src/inbound_router.rs",
        "kairo-next/crates/kairo-remote/src/lanes.rs",
        "kairo-next/crates/kairo-remote/src/local_address.rs",
        "kairo-next/crates/kairo-remote/src/local_delivery.rs",
        "kairo-next/crates/kairo-remote/src/outbound.rs",
        "kairo-next/crates/kairo-remote/src/protocol.rs",
        "kairo-next/crates/kairo-remote/src/provider.rs",
        "kairo-next/crates/kairo-remote/src/reliable_delivery.rs",
        "kairo-next/crates/kairo-remote/src/reliable_runtime.rs",
        "kairo-next/crates/kairo-remote/src/remote_ref.rs",
        "kairo-next/crates/kairo-remote/src/resolved_ref.rs",
        "kairo-next/crates/kairo-remote/src/settings.rs",
        "kairo-next/crates/kairo-remote/src/stream.rs",
        "kairo-next/crates/kairo-remote/src/stream_inbound.rs",
        "kairo-next/crates/kairo-remote/src/stream_sink.rs",
        "kairo-next/crates/kairo-remote/src/system_inbound.rs",
        "kairo-next/crates/kairo-remote/src/transport.rs",
    ];

    for relative_path in documented_modules {
        let source = std::fs::read_to_string(repo_root.join(relative_path))?.replace("\r\n", "\n");
        assert!(
            source.starts_with("#![deny(missing_docs)]\n"),
            "{relative_path} must keep missing public documentation as a hard error"
        );
    }

    Ok(())
}

#[test]
fn publishable_workspace_dependencies_keep_registry_versions()
-> Result<(), Box<dyn std::error::Error>> {
    let repo_root = repo_root()?;
    let root_manifest =
        std::fs::read_to_string(repo_root.join("Cargo.toml"))?.replace("\r\n", "\n");
    let publishable_dependencies = [
        "kairo-actor",
        "kairo-actor-macros",
        "kairo-serialization",
        "kairo-remote",
        "kairo-cluster",
        "kairo-distributed-data",
        "kairo-cluster-tools",
        "kairo-cluster-sharding",
        "kairo-testkit",
    ];

    for crate_name in publishable_dependencies {
        let dependency = format!(
            "{crate_name} = {{ version = \"0.1.0\", path = \"kairo-next/crates/{crate_name}\" }}"
        );
        assert!(
            root_manifest.contains(&dependency),
            "workspace dependency `{crate_name}` must combine a registry version with its local path so public crates can be packaged together"
        );
    }

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
fn active_crates_inherit_workspace_release_metadata_and_support_crates_stay_private()
-> Result<(), Box<dyn std::error::Error>> {
    let repo_root = repo_root()?;
    let root_manifest = std::fs::read_to_string(repo_root.join("Cargo.toml"))?;
    let next_crates = repo_root.join("kairo-next").join("crates");
    let support_crates = ["kairo-examples", "kairo-benchmarks"];

    assert!(
        root_manifest.contains("license = \"MIT\""),
        "workspace package metadata must keep the audited MIT license"
    );
    assert!(
        root_manifest.contains("rust-version = \"1.88\""),
        "workspace package metadata must declare the CI-verified minimum Rust version"
    );

    for entry in std::fs::read_dir(next_crates)? {
        let entry = entry?;
        let crate_name = entry.file_name().to_string_lossy().into_owned();
        let manifest_path = entry.path().join("Cargo.toml");
        if !manifest_path.is_file() {
            continue;
        }

        let manifest = std::fs::read_to_string(&manifest_path)?;
        assert!(
            manifest.contains("license.workspace = true"),
            "{} must inherit the workspace license metadata recorded by the M13 audit",
            manifest_path.display()
        );
        assert!(
            manifest.contains("rust-version.workspace = true"),
            "{} must inherit the CI-verified minimum Rust version",
            manifest_path.display()
        );
        assert!(
            !manifest.contains("license = \""),
            "{} must not override the audited workspace license locally",
            manifest_path.display()
        );

        if support_crates.contains(&crate_name.as_str()) {
            assert!(
                manifest.contains("publish = false"),
                "{} must remain a private support crate, not a published runtime surface",
                manifest_path.display()
            );
        } else {
            assert!(
                manifest.contains("description = \""),
                "{} must describe its public package surface",
                manifest_path.display()
            );
            assert!(
                !manifest.contains("publish = false"),
                "{} must remain available to the workspace package gate",
                manifest_path.display()
            );
        }
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
            "kairo-cluster-tools",
            &["kairo-cluster-sharding"],
            "kairo-cluster-tools must remain below sharding so singleton integration does not form a cycle",
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

    let sharding_manifest = std::fs::read_to_string(
        next_crates
            .join("kairo-cluster-sharding")
            .join("Cargo.toml"),
    )?;
    assert!(
        sharding_manifest.contains("kairo-cluster-tools = { workspace = true }"),
        "cluster sharding must use the public cluster-tools crate for coordinator singleton placement"
    );

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
fn facade_feature_graph_keeps_distributed_layers_opt_in() -> Result<(), Box<dyn std::error::Error>>
{
    let repo_root = repo_root()?;
    let manifest_path = repo_root
        .join("kairo-next")
        .join("crates")
        .join("kairo")
        .join("Cargo.toml");
    let manifest = std::fs::read_to_string(&manifest_path)?.replace("\r\n", "\n");

    for (feature_line, _, reason) in FACADE_FEATURE_EXPECTATIONS {
        assert!(
            manifest.contains(feature_line),
            "{} must contain `{feature_line}`: {reason}",
            manifest_path.display()
        );
    }

    Ok(())
}

#[test]
fn public_docs_keep_facade_feature_map_aligned() -> Result<(), Box<dyn std::error::Error>> {
    let repo_root = repo_root()?;
    let docs = [
        repo_root.join("README.md"),
        repo_root.join("kairo-next").join("README.md"),
        repo_root.join("docs").join("migration.md"),
    ];

    for doc_path in docs {
        let doc = std::fs::read_to_string(&doc_path)?.replace("\r\n", "\n");
        assert!(
            doc.contains("The `kairo` facade"),
            "{} must present the facade as the normal user entry point",
            doc_path.display()
        );

        for (_, feature_row, reason) in FACADE_FEATURE_EXPECTATIONS {
            assert!(
                doc.contains(feature_row),
                "{} must document facade feature row `{feature_row}`: {reason}",
                doc_path.display()
            );
        }
        for helper in ["DiagnosticCounters", "DiagnosticTextSink"] {
            assert!(
                doc.contains(helper),
                "{} must document facade observability helper `{helper}`",
                doc_path.display()
            );
        }
    }

    Ok(())
}

#[test]
fn implementation_status_docs_do_not_mark_region_bootstrap_as_future_work()
-> Result<(), Box<dyn std::error::Error>> {
    let repo_root = repo_root()?;
    let docs = [
        repo_root.join("docs").join("progress.md"),
        repo_root.join("docs").join("decisions.md"),
    ];
    let stale_phrases = [
        "higher-level region bootstrap helper as future work",
        "future higher-level region bootstrap helper",
    ];

    for doc_path in docs {
        let doc = std::fs::read_to_string(&doc_path)?.replace("\r\n", "\n");
        assert!(
            doc.contains("ShardRegionBootstrap"),
            "{} must mention the implemented sharding region bootstrap helper",
            doc_path.display()
        );
        for phrase in stale_phrases {
            assert!(
                !doc.contains(phrase),
                "{} must not describe the implemented ShardRegionBootstrap helper as future work",
                doc_path.display()
            );
        }
    }

    Ok(())
}

#[test]
fn implementation_status_docs_do_not_mark_region_discovery_wiring_as_future_work()
-> Result<(), Box<dyn std::error::Error>> {
    let repo_root = repo_root()?;
    let decisions =
        std::fs::read_to_string(repo_root.join("docs").join("decisions.md"))?.replace("\r\n", "\n");
    let progress =
        std::fs::read_to_string(repo_root.join("docs").join("progress.md"))?.replace("\r\n", "\n");

    assert!(
        !decisions.contains("The region actor still needs to react to cluster\nsnapshots/events"),
        "decisions must not describe implemented region discovery message wiring as future work"
    );
    for phrase in [
        "The region actor reacts to cluster snapshots/events\nthrough focused discovery messages",
        "The\nregion actor accepts discovery snapshots/events and refreshes its existing\nregistration boundary from that bridge.",
    ] {
        assert!(
            decisions.contains(phrase),
            "decisions must describe implemented region discovery wiring: {phrase}"
        );
    }
    for phrase in [
        "`kairo-cluster-sharding` shard region actors can now accept coordinator\n  discovery snapshots/events",
        "Shard-region discovery subscriber coverage now validates coordinator\n  movement",
    ] {
        assert!(
            progress.contains(phrase),
            "progress must mention implemented region discovery coverage: {phrase}"
        );
    }

    Ok(())
}

#[test]
fn implementation_status_docs_do_not_mark_remote_sharding_registration_as_future_work()
-> Result<(), Box<dyn std::error::Error>> {
    let repo_root = repo_root()?;
    let decisions =
        std::fs::read_to_string(repo_root.join("docs").join("decisions.md"))?.replace("\r\n", "\n");
    let progress =
        std::fs::read_to_string(repo_root.join("docs").join("progress.md"))?.replace("\r\n", "\n");
    let stale_phrases = [
        "stable wire recipient for a future remote registration bridge",
        "future\n  remote registration",
        "The actual remote registration outbound/reply bridge remains a separate\n  transport-facing module",
        "Outbound retry scheduling for remote registration and shard-home requests\n  remains a follow-up",
    ];

    for phrase in stale_phrases {
        assert!(
            !decisions.contains(phrase) && !progress.contains(phrase),
            "status docs must not describe implemented remote sharding registration as future work"
        );
    }
    for phrase in [
        "ShardCoordinatorRemoteRegistrationOutbound",
        "RegionRemoteCoordinatorTransport",
        "/system/sharding/coordinator",
    ] {
        assert!(
            decisions.contains(phrase) && progress.contains(phrase),
            "remote sharding registration status docs must mention implemented boundary `{phrase}`"
        );
    }
    assert!(
        progress.contains(
            "sending\n  stable `Register` envelopes after remote coordinator discovery/retry"
        ),
        "progress must mention region-driven remote registration retry sends"
    );

    Ok(())
}

#[test]
fn implementation_status_docs_do_not_mark_remote_region_control_inbound_as_future_work()
-> Result<(), Box<dyn std::error::Error>> {
    let repo_root = repo_root()?;
    let decisions =
        std::fs::read_to_string(repo_root.join("docs").join("decisions.md"))?.replace("\r\n", "\n");
    let progress =
        std::fs::read_to_string(repo_root.join("docs").join("progress.md"))?.replace("\r\n", "\n");

    assert!(
        !decisions.contains(
            "Region-side inbound execution of remote `HostShard`, `BeginHandOff`, and\n  `HandOff` commands remains a follow-up"
        ),
        "decisions must not describe implemented remote region control inbound as future work"
    );
    for phrase in [
        "ShardRegionSystemInbound",
        "ShardRegionRemoteControlReplyTarget",
    ] {
        assert!(
            decisions.contains(phrase) && progress.contains(phrase),
            "remote region control inbound status docs must mention implemented boundary `{phrase}`"
        );
    }
    for phrase in ["remote `HostShard`", "`BeginHandOff`", "`HandOff` commands"] {
        assert!(
            decisions.contains(phrase) && progress.contains(phrase),
            "remote region control inbound status docs must mention stable command fragment `{phrase}`"
        );
    }
    for phrase in [
        "emits stable",
        "`ShardStarted`",
        "`BeginHandOffAck`",
        "immediate `ShardStopped` replies",
    ] {
        assert!(
            progress.contains(phrase),
            "progress must mention stable remote control replies generated by region actors: {phrase}"
        );
    }

    Ok(())
}

#[test]
fn implementation_status_docs_mark_actor_tree_lifecycle_audit_complete()
-> Result<(), Box<dyn std::error::Error>> {
    let repo_root = repo_root()?;
    let progress =
        std::fs::read_to_string(repo_root.join("docs").join("progress.md"))?.replace("\r\n", "\n");
    let stale_phrases = [
        "Full actor tree lifecycle semantics beyond recursive local stop, recursive\n  restart-time child handling, restart-time child watch cleanup, and\n  terminating-child name reservation.",
        "terminating-child name reservation remain future work",
        "terminating-parent\n  queued child-spawn drain coverage for direct parent and actor-system stop,\n  actor-system recursive child mailbox drain coverage",
        "actor-system recursive child mailbox drain coverage, and actor-system\n  stashed-message/message-adapter/async-helper/ask-temp-ref/timer and",
    ];

    for phrase in stale_phrases {
        assert!(
            !progress.contains(phrase),
            "progress must not mark implemented actor-tree lifecycle coverage as future work"
        );
    }
    for implemented_phrase in [
        "final actor-tree audit complete",
        "late parent watch could lose a child's failure cause",
        "Remaining work is tuning, compatibility depth, documentation, and release\nhardening rather than foundational redesign.",
    ] {
        assert!(
            progress.contains(implemented_phrase),
            "progress must mention implemented actor-tree lifecycle coverage: {implemented_phrase}"
        );
    }
    assert!(!progress.contains("Full actor tree lifecycle semantic audit beyond the current"));

    Ok(())
}

#[test]
fn implementation_status_docs_do_not_mark_ddata_bootstrap_shrink_cleanup_as_future_work()
-> Result<(), Box<dyn std::error::Error>> {
    let repo_root = repo_root()?;
    let progress =
        std::fs::read_to_string(repo_root.join("docs").join("progress.md"))?.replace("\r\n", "\n");
    let stale_phrases = [
        "including removed-peer route cleanup in the three-node\n  bootstrap shrink path",
        "removed-peer route cleanup in the three-node bootstrap shrink path remains future work",
    ];

    for phrase in stale_phrases {
        assert!(
            !progress.contains(phrase),
            "progress must not mark implemented distributed-data bootstrap shrink cleanup as future work"
        );
    }
    for implemented_phrase in [
        "`kairo-distributed-data` three-node TCP bootstrap coverage now feeds reduced\n  gossip to the removed peer as well as the survivors",
        "`kairo-distributed-data` and `kairo-cluster-tools` three-node TCP bootstrap\n  coverage now mirror the cluster route-cache shrink checks",
        "Current three-node bootstrap shrink coverage already feeds\n  reduced gossip to the removed peer",
    ] {
        assert!(
            progress.contains(implemented_phrase),
            "progress must mention distributed-data bootstrap shrink cleanup coverage: {implemented_phrase}"
        );
    }

    Ok(())
}

#[test]
fn implementation_status_docs_list_all_tcp_peer_runtime_local_only_rejection_coverage()
-> Result<(), Box<dyn std::error::Error>> {
    let repo_root = repo_root()?;
    let progress =
        std::fs::read_to_string(repo_root.join("docs").join("progress.md"))?.replace("\r\n", "\n");

    assert!(
        !progress.contains("distributed-data and cluster-tools peer-runtime local-only snapshot\n  rejection coverage"),
        "progress must not omit cluster from implemented TCP peer-runtime local-only rejection coverage"
    );
    assert!(
        progress.contains(
            "cluster, distributed-data, and cluster-tools peer-runtime local-only\n  snapshot rejection coverage"
        ),
        "progress must mention local-only snapshot rejection coverage for all TCP peer runtimes"
    );

    Ok(())
}

#[test]
fn implementation_status_docs_do_not_mark_cluster_remote_envelope_boundary_as_future_work()
-> Result<(), Box<dyn std::error::Error>> {
    let repo_root = repo_root()?;
    let decisions =
        std::fs::read_to_string(repo_root.join("docs").join("decisions.md"))?.replace("\r\n", "\n");
    let progress =
        std::fs::read_to_string(repo_root.join("docs").join("progress.md"))?.replace("\r\n", "\n");

    let stale_phrases = [
        "but still needs a\nshared remote association boundary",
        "Socket-backed cluster transport and heartbeat receiver routing remain\n  separate integration steps.",
    ];

    for phrase in stale_phrases {
        assert!(
            !decisions.contains(phrase),
            "decisions must not describe implemented cluster remote-envelope wiring as future work"
        );
    }
    for phrase in [
        "ClusterMembershipRemoteEnvelopeOutbound",
        "/system/cluster/core/daemon",
        "RemoteAssociationCache",
    ] {
        assert!(
            decisions.contains(phrase) && progress.contains(phrase),
            "cluster remote-envelope status docs must mention implemented boundary `{phrase}`"
        );
    }
    for phrase in [
        "routes\n  join/welcome/gossip and heartbeat request/response envelopes through live",
        "socket associations, and keeps cluster membership truth in gossip",
    ] {
        assert!(
            progress.contains(phrase),
            "progress must mention implemented cluster socket association routing: {phrase}"
        );
    }

    Ok(())
}

#[test]
fn implementation_status_docs_do_not_mark_lease_majority_as_future_work()
-> Result<(), Box<dyn std::error::Error>> {
    let repo_root = repo_root()?;
    let architecture =
        std::fs::read_to_string(repo_root.join("kairo-next").join("ARCHITECTURE.md"))?
            .replace("\r\n", "\n");
    let decisions =
        std::fs::read_to_string(repo_root.join("docs").join("decisions.md"))?.replace("\r\n", "\n");
    let progress =
        std::fs::read_to_string(repo_root.join("docs").join("progress.md"))?.replace("\r\n", "\n");
    let stale_phrases = [
        "lease-majority support was still pending",
        "Lease-majority and broader data-center-aware policy coverage remain later\n  work.",
    ];

    for phrase in stale_phrases {
        assert!(
            !architecture.contains(phrase)
                && !decisions.contains(phrase)
                && !progress.contains(phrase),
            "status docs must not describe implemented lease-majority support as future work"
        );
    }
    for phrase in ["LeaseMajorityHook", "lease-majority", "membership truth"] {
        assert!(
            architecture.contains(phrase)
                && decisions.contains(phrase)
                && progress.contains(phrase),
            "lease-majority status docs must mention implemented boundary `{phrase}`"
        );
    }

    Ok(())
}

#[test]
fn implementation_status_docs_do_not_mark_reader_supervision_as_future_work()
-> Result<(), Box<dyn std::error::Error>> {
    let repo_root = repo_root()?;
    let docs = [
        repo_root.join("docs").join("progress.md"),
        repo_root.join("docs").join("decisions.md"),
    ];
    let stale_phrases = [
        "Reader supervision and reconnect/backoff policy remain future work",
        "reader supervision and reconnect/backoff policy remain future work",
    ];

    for doc_path in docs {
        let doc = std::fs::read_to_string(&doc_path)?.replace("\r\n", "\n");
        assert!(
            doc.contains("TcpAssociationReaderSupervisor"),
            "{} must mention the implemented TCP reader supervision state machine",
            doc_path.display()
        );
        for phrase in stale_phrases {
            assert!(
                !doc.contains(phrase),
                "{} must not describe the implemented TCP reader supervision state machine as future work",
                doc_path.display()
            );
        }
    }

    Ok(())
}

#[test]
fn implementation_status_docs_do_not_mark_public_api_docs_as_unreviewed()
-> Result<(), Box<dyn std::error::Error>> {
    let repo_root = repo_root()?;
    let progress =
        std::fs::read_to_string(repo_root.join("docs").join("progress.md"))?.replace("\r\n", "\n");

    assert!(
        !progress.contains("public API documentation needs to be reviewed"),
        "progress status must not describe compile-tested public API docs as unreviewed"
    );
    assert!(
        progress.contains("workspace doctests")
            && progress.contains("rustdoc warning gates")
            && progress.contains("compile-tested public API snippets"),
        "progress status must mention the implemented public API documentation gates"
    );

    Ok(())
}

#[test]
fn implementation_status_docs_do_not_mark_observability_facade_wiring_as_future_work()
-> Result<(), Box<dyn std::error::Error>> {
    let repo_root = repo_root()?;
    let decisions =
        std::fs::read_to_string(repo_root.join("docs").join("decisions.md"))?.replace("\r\n", "\n");
    let progress =
        std::fs::read_to_string(repo_root.join("docs").join("progress.md"))?.replace("\r\n", "\n");

    assert!(
        !decisions.contains("Future facade wiring can map observability settings"),
        "decisions must not describe implemented observability facade helpers as future work"
    );
    assert!(
        !progress.contains("M11 configuration and observability: partially complete"),
        "progress must not mark implemented M11 observability helpers as only partially complete"
    );
    assert!(
        !progress.contains("concrete logging/metrics adapters and operator polish remain"),
        "progress must not describe implemented diagnostic counters as missing metrics adapters"
    );
    for helper in [
        "DiagnosticsConfig::remote_inbound_diagnostics",
        "DiagnosticsConfig::remote_association_diagnostics",
        "DiagnosticsConfig::cluster_diagnostics",
        "DiagnosticCounters",
        "DiagnosticTextSink",
    ] {
        assert!(
            decisions.contains(helper) && progress.contains(helper),
            "observability status docs must mention implemented helper `{helper}`"
        );
    }

    Ok(())
}

#[test]
fn migration_notes_pin_legacy_removal_gates() -> Result<(), Box<dyn std::error::Error>> {
    let repo_root = repo_root()?;
    let migration_path = repo_root.join("docs").join("migration.md");
    let migration = std::fs::read_to_string(&migration_path)?.replace("\r\n", "\n");

    let required_legacy_section_phrases = [
        "The old `crates/` tree is reference material only.",
        "intentionally excluded",
        "from the root workspace",
        "normal validation, runnable examples",
        "implementation work",
        "The legacy tree can be removed after these release-hardening gates are met:",
        "the `kairo` facade is the documented entry point for normal users",
        "examples cover the local actor, configuration, remote, cluster",
        "full workspace CI runs formatting, clippy with warnings denied, and tests",
        "workspace and active crate manifests do not depend on `crates/`",
        "remaining migration gaps are tracked as release issues",
        "Removal should happen as a separate `chore` or `docs` checkpoint",
    ];

    for phrase in required_legacy_section_phrases {
        assert!(
            migration.contains(phrase),
            "{} must keep legacy-removal gate `{phrase}` documented",
            migration_path.display()
        );
    }

    Ok(())
}

#[test]
fn public_readmes_list_current_workspace_crates() -> Result<(), Box<dyn std::error::Error>> {
    let repo_root = repo_root()?;
    let crate_names = active_workspace_crate_names(&repo_root.join("kairo-next/crates"))?;
    assert!(
        !crate_names.is_empty(),
        "kairo-next/crates must contain active workspace crates"
    );

    let readmes = [
        repo_root.join("README.md"),
        repo_root.join("kairo-next/README.md"),
    ];
    for readme_path in readmes {
        let readme = std::fs::read_to_string(&readme_path)?.replace("\r\n", "\n");
        for crate_name in &crate_names {
            let bullet = format!("- `{crate_name}`:");
            assert!(
                readme.contains(&bullet),
                "{} must list active workspace crate `{crate_name}`",
                readme_path.display()
            );
        }
        for line in readme.lines() {
            let Some(crate_name) = documented_workspace_crate_name(line) else {
                continue;
            };
            assert!(
                crate_names.contains(crate_name),
                "{} documents missing workspace crate `{crate_name}`",
                readme_path.display()
            );
        }
    }

    Ok(())
}

#[test]
fn architecture_lists_current_workspace_crates() -> Result<(), Box<dyn std::error::Error>> {
    let repo_root = repo_root()?;
    let crate_names = active_workspace_crate_names(&repo_root.join("kairo-next/crates"))?;
    let architecture_path = repo_root.join("kairo-next/ARCHITECTURE.md");
    let architecture = std::fs::read_to_string(&architecture_path)?.replace("\r\n", "\n");
    let documented_crates = architecture_workspace_crate_names(&architecture)?;

    assert_eq!(
        documented_crates,
        crate_names,
        "{} workspace crate list must match active kairo-next package manifests",
        architecture_path.display()
    );

    Ok(())
}

#[test]
fn architecture_dependency_direction_matches_active_manifests()
-> Result<(), Box<dyn std::error::Error>> {
    let repo_root = repo_root()?;
    let crates_dir = repo_root.join("kairo-next/crates");
    let crate_names = active_workspace_crate_names(&crates_dir)?;
    let manifest_edges = active_workspace_dependency_edges(&crates_dir, &crate_names)?;
    let architecture_path = repo_root.join("kairo-next/ARCHITECTURE.md");
    let architecture = std::fs::read_to_string(&architecture_path)?.replace("\r\n", "\n");
    let architecture_edges = architecture_dependency_edges(&architecture)?;

    assert_eq!(
        architecture_edges,
        manifest_edges,
        "{} dependency direction block must match active kairo-next package manifests",
        architecture_path.display()
    );

    Ok(())
}

fn active_workspace_crate_names(
    crates_dir: &std::path::Path,
) -> Result<std::collections::BTreeSet<String>, Box<dyn std::error::Error>> {
    let mut crate_names = std::collections::BTreeSet::new();

    for entry in std::fs::read_dir(crates_dir)? {
        let entry = entry?;
        let manifest_path = entry.path().join("Cargo.toml");
        if !manifest_path.is_file() {
            continue;
        }

        let manifest = std::fs::read_to_string(&manifest_path)?;
        let name = manifest
            .lines()
            .find_map(|line| line.strip_prefix("name = "))
            .map(unquote_toml_string)
            .ok_or_else(|| format!("{} must declare package name", manifest_path.display()))?;
        crate_names.insert(name);
    }

    Ok(crate_names)
}

fn active_workspace_dependency_edges(
    crates_dir: &std::path::Path,
    crate_names: &std::collections::BTreeSet<String>,
) -> Result<
    std::collections::BTreeMap<String, std::collections::BTreeSet<String>>,
    Box<dyn std::error::Error>,
> {
    let mut edges = std::collections::BTreeMap::new();

    for entry in std::fs::read_dir(crates_dir)? {
        let entry = entry?;
        let manifest_path = entry.path().join("Cargo.toml");
        if !manifest_path.is_file() {
            continue;
        }

        let manifest = std::fs::read_to_string(&manifest_path)?;
        let package = manifest
            .lines()
            .find_map(|line| line.strip_prefix("name = "))
            .map(unquote_toml_string)
            .ok_or_else(|| format!("{} must declare package name", manifest_path.display()))?;
        let mut dependencies = std::collections::BTreeSet::new();
        let mut in_dependencies = false;

        for line in manifest.lines() {
            if line == "[dependencies]" {
                in_dependencies = true;
                continue;
            }
            if in_dependencies && line.starts_with('[') {
                break;
            }
            if !in_dependencies {
                continue;
            }

            let Some((dependency, _)) = line.split_once(" = ") else {
                continue;
            };
            if crate_names.contains(dependency) {
                dependencies.insert(dependency.to_string());
            }
        }

        if !dependencies.is_empty() {
            edges.insert(package, dependencies);
        }
    }

    Ok(edges)
}

fn architecture_workspace_crate_names(
    architecture: &str,
) -> Result<std::collections::BTreeSet<String>, Box<dyn std::error::Error>> {
    let mut crate_names = std::collections::BTreeSet::new();
    let mut in_workspace_block = false;

    for line in architecture.lines() {
        if line == "kairo-next/crates/" {
            in_workspace_block = true;
            continue;
        }
        if in_workspace_block && line == "```" {
            break;
        }
        if !in_workspace_block {
            continue;
        }

        let crate_name = line.trim();
        if crate_name.starts_with("kairo") {
            crate_names.insert(crate_name.to_string());
        }
    }

    if crate_names.is_empty() {
        return Err("ARCHITECTURE.md must list active crates under kairo-next/crates/".into());
    }

    Ok(crate_names)
}

fn architecture_dependency_edges(
    architecture: &str,
) -> Result<
    std::collections::BTreeMap<String, std::collections::BTreeSet<String>>,
    Box<dyn std::error::Error>,
> {
    let mut edges: std::collections::BTreeMap<String, std::collections::BTreeSet<String>> =
        std::collections::BTreeMap::new();
    let mut stack: Vec<(usize, String)> = Vec::new();
    let mut after_heading = false;
    let mut in_block = false;

    for line in architecture.lines() {
        if line == "Dependency direction:" {
            after_heading = true;
            continue;
        }
        if after_heading && line == "```text" {
            in_block = true;
            continue;
        }
        if in_block && line == "```" {
            break;
        }
        if !in_block {
            continue;
        }

        let trimmed = line.trim_start();
        if trimmed.is_empty() {
            continue;
        }

        let indent = line.len() - trimmed.len();
        if !trimmed.starts_with("-> ") {
            stack.clear();
            stack.push((indent, trimmed.to_string()));
            continue;
        }

        let dependency = trimmed
            .strip_prefix("-> ")
            .expect("dependency line starts with arrow")
            .to_string();
        while stack
            .last()
            .is_some_and(|(parent_indent, _)| *parent_indent >= indent)
        {
            stack.pop();
        }
        let Some((_, parent)) = stack.last() else {
            return Err(format!("dependency `{dependency}` has no documented parent").into());
        };
        edges
            .entry(parent.clone())
            .or_default()
            .insert(dependency.clone());
        stack.push((indent, dependency));
    }

    if edges.is_empty() {
        return Err("ARCHITECTURE.md must document dependency direction edges".into());
    }

    Ok(edges)
}

fn documented_workspace_crate_name(line: &str) -> Option<&str> {
    let crate_name = line.trim_start().strip_prefix("- `")?.split_once("`:")?.0;
    crate_name.starts_with("kairo").then_some(crate_name)
}

#[test]
fn public_readmes_list_current_example_binaries() -> Result<(), Box<dyn std::error::Error>> {
    let repo_root = repo_root()?;
    let examples_dir = repo_root
        .join("kairo-next")
        .join("crates")
        .join("kairo-examples")
        .join("examples");
    let mut example_names = std::collections::BTreeSet::new();

    for entry in std::fs::read_dir(&examples_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|extension| extension.to_str()) != Some("rs") {
            continue;
        }
        let name = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .ok_or_else(|| format!("example path has no UTF-8 stem: {}", path.display()))?
            .to_string();
        example_names.insert(name);
    }

    let required_docs = [
        repo_root.join("README.md"),
        repo_root.join("kairo-next/README.md"),
    ];
    for doc_path in required_docs {
        let doc = std::fs::read_to_string(&doc_path)?.replace("\r\n", "\n");
        for example_name in &example_names {
            let command = format!("cargo run -p kairo-examples --example {example_name}");
            assert!(
                doc.contains(&command),
                "{} must list runnable example command `{command}`",
                doc_path.display()
            );
        }
    }

    let docs_with_example_commands = [
        repo_root.join("README.md"),
        repo_root.join("kairo-next/README.md"),
        repo_root.join("docs/migration.md"),
    ];
    for doc_path in docs_with_example_commands {
        let doc = std::fs::read_to_string(&doc_path)?.replace("\r\n", "\n");
        for line in doc.lines() {
            let Some(example_name) = line.strip_prefix("cargo run -p kairo-examples --example ")
            else {
                continue;
            };
            assert!(
                example_names.contains(example_name),
                "{} documents missing kairo-examples binary `{example_name}`",
                doc_path.display()
            );
        }
    }

    Ok(())
}

#[test]
fn public_docs_list_current_benchmark_scenarios() -> Result<(), Box<dyn std::error::Error>> {
    let repo_root = repo_root()?;
    let benchmark_source_path = repo_root
        .join("kairo-next")
        .join("crates")
        .join("kairo-benchmarks")
        .join("src")
        .join("main.rs");
    let benchmark_source = std::fs::read_to_string(&benchmark_source_path)?.replace("\r\n", "\n");
    let benchmark_scenarios = benchmark_scenarios_from_source(&benchmark_source)?;
    assert!(
        !benchmark_scenarios.is_empty(),
        "{} must expose at least one benchmark scenario",
        benchmark_source_path.display()
    );

    let public_docs = [
        repo_root.join("README.md"),
        repo_root.join("kairo-next/README.md"),
        repo_root.join("docs/migration.md"),
    ];
    for doc_path in public_docs {
        let doc = std::fs::read_to_string(&doc_path)?.replace("\r\n", "\n");
        assert!(
            doc.contains("cargo run -p kairo-benchmarks -- --help"),
            "{} must document the benchmark help command",
            doc_path.display()
        );
        for scenario in &benchmark_scenarios {
            let command = format!("cargo run -p kairo-benchmarks --release -- {scenario}");
            assert!(
                doc.contains(&command),
                "{} must document benchmark scenario command `{command}`",
                doc_path.display()
            );
        }
        for line in doc.lines() {
            let Some(scenario) = documented_benchmark_scenario(line) else {
                continue;
            };
            if scenario == "--help" {
                continue;
            }
            assert!(
                benchmark_scenarios.contains(scenario),
                "{} documents missing kairo-benchmarks scenario `{scenario}`",
                doc_path.display()
            );
        }
    }

    Ok(())
}

fn benchmark_scenarios_from_source(
    source: &str,
) -> Result<std::collections::BTreeSet<String>, Box<dyn std::error::Error>> {
    let mut scenarios = std::collections::BTreeSet::new();

    for line in source.lines() {
        let trimmed = line.trim();
        if !trimmed.contains("=> Ok(Self::") {
            continue;
        }

        let Some(after_quote) = trimmed.strip_prefix('"') else {
            continue;
        };
        let Some((scenario, _)) = after_quote.split_once('"') else {
            return Err(format!("malformed benchmark command parser row: {trimmed}").into());
        };
        if scenario == "help" || scenario.starts_with('-') {
            continue;
        }
        scenarios.insert(scenario.to_string());
    }

    Ok(scenarios)
}

fn documented_benchmark_scenario(line: &str) -> Option<&str> {
    let command_start = line.find("cargo run -p kairo-benchmarks")?;
    let command = line[command_start..].trim();
    let (_, scenario) = command.rsplit_once(" -- ")?;
    scenario.split_whitespace().next()
}

#[test]
fn public_docs_document_m13_validation_gates() -> Result<(), Box<dyn std::error::Error>> {
    let repo_root = repo_root()?;
    let public_docs = [
        repo_root.join("README.md"),
        repo_root.join("kairo-next/README.md"),
        repo_root.join("docs/migration.md"),
    ];

    for doc_path in public_docs {
        let doc = std::fs::read_to_string(&doc_path)?.replace("\r\n", "\n");
        for (command, reason) in M13_VALIDATION_GATE_EXPECTATIONS {
            assert!(
                doc.contains(command),
                "{} must document validation command `{command}`: {reason}",
                doc_path.display()
            );
        }
    }

    Ok(())
}

#[test]
fn public_docs_use_repository_root_for_workspace_commands() -> Result<(), Box<dyn std::error::Error>>
{
    let repo_root = repo_root()?;
    assert!(
        repo_root.join("Cargo.toml").is_file(),
        "the active workspace manifest must live at the repository root"
    );
    assert!(
        !repo_root.join("kairo-next/Cargo.toml").exists(),
        "kairo-next is not a standalone Cargo workspace"
    );

    let public_docs = [
        repo_root.join("README.md"),
        repo_root.join("kairo-next/README.md"),
        repo_root.join("docs/migration.md"),
    ];
    let stale_workspace_hints = [
        "cd kairo-next",
        "From `kairo-next`",
        "from `kairo-next`",
        "locally from `kairo-next`",
    ];

    for doc_path in public_docs {
        let doc = std::fs::read_to_string(&doc_path)?.replace("\r\n", "\n");
        for stale_hint in stale_workspace_hints {
            assert!(
                !doc.contains(stale_hint),
                "{} must not imply cargo workspace commands run from kairo-next; use the repository root instead",
                doc_path.display()
            );
        }
    }

    Ok(())
}

#[test]
fn rust_ci_keeps_m13_release_readiness_gates() -> Result<(), Box<dyn std::error::Error>> {
    let repo_root = repo_root()?;
    let workflow_path = repo_root
        .join(".github")
        .join("workflows")
        .join("rust-ci.yml");
    let workflow = std::fs::read_to_string(&workflow_path)?.replace("\r\n", "\n");

    for (command, reason) in M13_VALIDATION_GATE_EXPECTATIONS {
        assert!(
            workflow.contains(command),
            "{} must contain `{command}`: {reason}",
            workflow_path.display()
        );
    }

    assert!(
        workflow.contains("dtolnay/rust-toolchain@1.88.0"),
        "{} must check the declared Rust 1.88 MSRV",
        workflow_path.display()
    );
    for platform in ["ubuntu-latest", "windows-latest", "macos-latest"] {
        assert!(
            workflow.contains(platform),
            "{} must retain stable test coverage for `{platform}`",
            workflow_path.display()
        );
    }

    Ok(())
}

#[test]
fn resolved_workspace_lockfile_excludes_deferred_dependency_families()
-> Result<(), Box<dyn std::error::Error>> {
    let repo_root = repo_root()?;
    let lockfile = std::fs::read_to_string(repo_root.join("Cargo.lock"))?.to_ascii_lowercase();
    let forbidden_packages = [
        (
            "hocon",
            "TOML is the first configuration file format; do not resolve HOCON support yet",
        ),
        (
            "hocon-rs",
            "TOML is the first configuration file format; do not resolve HOCON support yet",
        ),
        (
            "etcd-client",
            "cluster membership must remain gossip-based without a central membership store",
        ),
        (
            "kube",
            "cluster membership must not depend on Kubernetes as an authority",
        ),
        (
            "k8s-openapi",
            "cluster membership must not depend on Kubernetes as an authority",
        ),
        (
            "tokio",
            "the initial local actor runtime must not introduce an async runtime dependency",
        ),
        (
            "async-std",
            "the initial local actor runtime must not introduce an async runtime dependency",
        ),
        (
            "smol",
            "the initial local actor runtime must not introduce an async runtime dependency",
        ),
        (
            "serde_json",
            "public remote wire contracts must stay manifest/codec based, not serde-format based",
        ),
        (
            "bincode",
            "public remote wire contracts must stay manifest/codec based, not serde-format based",
        ),
        (
            "prost",
            "public remote wire contracts must stay manifest/codec based, not protobuf based",
        ),
        (
            "criterion",
            "the M13 benchmark runner is intentionally dependency-light",
        ),
        (
            "iai",
            "the M13 benchmark runner is intentionally dependency-light",
        ),
        (
            "divan",
            "the M13 benchmark runner is intentionally dependency-light",
        ),
        (
            "bencher",
            "the M13 benchmark runner is intentionally dependency-light",
        ),
    ];

    for (package, reason) in forbidden_packages {
        assert!(
            !lockfile.contains(&format!("name = \"{package}\"")),
            "Cargo.lock must not resolve `{package}`: {reason}"
        );
    }

    Ok(())
}

#[test]
fn dependency_audit_matches_resolved_external_lockfile_packages()
-> Result<(), Box<dyn std::error::Error>> {
    let repo_root = repo_root()?;
    let lockfile = std::fs::read_to_string(repo_root.join("Cargo.lock"))?.replace("\r\n", "\n");
    let audit_path = repo_root.join("docs").join("dependency-audit.md");
    let audit = std::fs::read_to_string(&audit_path)?.replace("\r\n", "\n");
    let lockfile_packages = resolved_external_lockfile_packages(&lockfile);
    let audit_packages = dependency_audit_resolved_packages(&audit)?;

    assert_eq!(
        audit_packages,
        lockfile_packages,
        "{} resolved external package table must match current Cargo.lock registry packages",
        audit_path.display()
    );

    Ok(())
}

#[test]
fn dependency_audit_lists_current_workspace_members() -> Result<(), Box<dyn std::error::Error>> {
    let repo_root = repo_root()?;
    let audit_path = repo_root.join("docs").join("dependency-audit.md");
    let audit = std::fs::read_to_string(&audit_path)?.replace("\r\n", "\n");
    let crate_names = active_workspace_crate_names(&repo_root.join("kairo-next/crates"))?;
    let audit_members = dependency_audit_workspace_members(&audit)?;

    assert_eq!(
        audit_members,
        crate_names,
        "{} active workspace member list must match current kairo-next package manifests",
        audit_path.display()
    );

    Ok(())
}

fn resolved_external_lockfile_packages(
    lockfile: &str,
) -> std::collections::BTreeMap<String, String> {
    let mut packages = std::collections::BTreeMap::new();
    let mut name: Option<String> = None;
    let mut version: Option<String> = None;
    let mut source: Option<String> = None;

    for line in lockfile.lines().chain(std::iter::once("[[package]]")) {
        if line == "[[package]]" {
            if source
                .as_deref()
                .is_some_and(|source| source.starts_with("registry+"))
                && let (Some(name), Some(version)) = (name.take(), version.take())
            {
                packages.insert(name, version);
            }
            name = None;
            version = None;
            source = None;
            continue;
        }

        if let Some(value) = line.strip_prefix("name = ") {
            name = Some(unquote_toml_string(value));
        } else if let Some(value) = line.strip_prefix("version = ") {
            version = Some(unquote_toml_string(value));
        } else if let Some(value) = line.strip_prefix("source = ") {
            source = Some(unquote_toml_string(value));
        }
    }

    packages
}

fn dependency_audit_resolved_packages(
    audit: &str,
) -> Result<std::collections::BTreeMap<String, String>, Box<dyn std::error::Error>> {
    let mut packages = std::collections::BTreeMap::new();
    let mut in_table = false;

    for line in audit.lines() {
        if line == "## Resolved External Licenses" {
            in_table = true;
            continue;
        }
        if in_table && line.starts_with("## ") {
            break;
        }
        if !in_table || !line.starts_with("| `") {
            continue;
        }

        let columns: Vec<_> = line.split('|').map(str::trim).collect();
        if columns.len() < 4 {
            return Err(format!("malformed dependency-audit row: {line}").into());
        }
        let name = columns[1]
            .strip_prefix('`')
            .and_then(|value| value.strip_suffix('`'))
            .ok_or_else(|| format!("dependency-audit package name must be backticked: {line}"))?;
        packages.insert(name.to_string(), columns[2].to_string());
    }

    Ok(packages)
}

fn dependency_audit_workspace_members(
    audit: &str,
) -> Result<std::collections::BTreeSet<String>, Box<dyn std::error::Error>> {
    let mut members = std::collections::BTreeSet::new();
    let mut after_heading = false;
    let mut in_block = false;

    for line in audit.lines() {
        if line == "Active workspace members:" {
            after_heading = true;
            continue;
        }
        if after_heading && line == "```text" {
            in_block = true;
            continue;
        }
        if in_block && line == "```" {
            break;
        }
        if in_block && !line.trim().is_empty() {
            members.insert(line.trim().to_string());
        }
    }

    if members.is_empty() {
        return Err("dependency-audit.md must list active workspace members".into());
    }

    Ok(members)
}

fn unquote_toml_string(value: &str) -> String {
    value.trim().trim_matches('"').to_string()
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
    let queue_settings = RemoteOutboundQueueSettings::new(8, 32, 2).unwrap();
    assert_eq!(queue_settings.control_capacity(), 8);
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
    let _ = std::mem::size_of::<Option<TcpRemoteActorRuntime>>();
    let _ = std::mem::size_of::<Option<TcpRemoteActorRuntimeBuilder>>();
    let _ = std::mem::size_of::<Option<TcpRemoteActorRuntimeContext>>();
    let _ = std::mem::size_of::<Option<ReliableSystemEnvelope>>();
    let _ = std::mem::size_of::<Option<ReliableSystemAck>>();
    let _ = std::mem::size_of::<Option<ReliableSystemNack>>();
    let _ = std::mem::size_of::<Option<ReliableSystemSender>>();
    let _ = std::mem::size_of::<Option<ReliableSystemReceiver>>();
    let _ = std::mem::size_of::<Option<ReliableSystemReceiveOutcome>>();
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

#[cfg(feature = "remote")]
#[test]
fn diagnostic_counters_record_remote_categories() {
    use crate::prelude::*;

    let counters = DiagnosticCounters::new();
    let recipient =
        kairo_serialization::ActorRefWireData::new("kairo://facade@127.0.0.1:25520/user/sink#1")
            .unwrap();

    RemoteInboundDiagnostics::record(
        &counters,
        RemoteInboundDiagnostic::SerializationFailure {
            recipient: recipient.clone(),
            sender: None,
            serializer_id: 1201,
            manifest: "kairo.facade.Ping".to_string(),
            version: 1,
            reason: "decode failed".to_string(),
        },
    );
    RemoteInboundDiagnostics::record(
        &counters,
        RemoteInboundDiagnostic::DeliveryFailure {
            recipient,
            sender: None,
            reason: "missing actor".to_string(),
        },
    );
    RemoteAssociationDiagnostics::record(
        &counters,
        RemoteAssociationDiagnostic::Quarantined {
            remote: "kairo://peer@127.0.0.1:25521".to_string(),
            remote_uid: Some(42),
            reason: "uid changed".to_string(),
        },
    );
    RemoteAssociationDiagnostics::record(
        &counters,
        RemoteAssociationDiagnostic::Closed {
            remote: "kairo://peer@127.0.0.1:25521".to_string(),
            reason: "shutdown".to_string(),
        },
    );

    assert_eq!(
        counters.snapshot(),
        DiagnosticCounterSnapshot {
            remote_serialization_failures: 1,
            remote_delivery_failures: 1,
            association_quarantine_events: 1,
            association_close_events: 1,
            cluster_gossip_state_changes: 0,
        }
    );
}

#[cfg(feature = "remote")]
#[test]
fn diagnostic_text_sink_exports_remote_lines() {
    use crate::prelude::*;
    use std::sync::{Arc, Mutex};

    let lines = Arc::new(Mutex::new(Vec::new()));
    let sink = DiagnosticTextSink::new({
        let lines = lines.clone();
        move |line| lines.lock().expect("diagnostic lines poisoned").push(line)
    });
    let recipient =
        kairo_serialization::ActorRefWireData::new("kairo://facade@127.0.0.1:25520/user/sink#1")
            .unwrap();

    RemoteInboundDiagnostics::record(
        &sink,
        RemoteInboundDiagnostic::SerializationFailure {
            recipient: recipient.clone(),
            sender: None,
            serializer_id: 1201,
            manifest: "kairo.facade.Ping".to_string(),
            version: 1,
            reason: "decode failed".to_string(),
        },
    );
    RemoteInboundDiagnostics::record(
        &sink,
        RemoteInboundDiagnostic::DeliveryFailure {
            recipient,
            sender: None,
            reason: "missing actor".to_string(),
        },
    );
    RemoteAssociationDiagnostics::record(
        &sink,
        RemoteAssociationDiagnostic::Quarantined {
            remote: "kairo://peer@127.0.0.1:25521".to_string(),
            remote_uid: Some(42),
            reason: "uid changed".to_string(),
        },
    );
    RemoteAssociationDiagnostics::record(
        &sink,
        RemoteAssociationDiagnostic::Closed {
            remote: "kairo://peer@127.0.0.1:25521".to_string(),
            reason: "shutdown".to_string(),
        },
    );

    assert_eq!(
        *lines.lock().expect("diagnostic lines poisoned"),
        vec![
            "remote.serialization_failure recipient=kairo://facade@127.0.0.1:25520/user/sink#1 sender=- serializer_id=1201 manifest=kairo.facade.Ping version=1 reason=decode failed",
            "remote.delivery_failure recipient=kairo://facade@127.0.0.1:25520/user/sink#1 sender=- reason=missing actor",
            "remote.association_quarantined remote=kairo://peer@127.0.0.1:25521 remote_uid=42 reason=uid changed",
            "remote.association_closed remote=kairo://peer@127.0.0.1:25521 reason=shutdown",
        ]
    );
}

#[cfg(feature = "cluster")]
#[test]
fn facade_cluster_module_exposes_tcp_bootstrap_surface() {
    use std::time::Duration;

    let settings = crate::remote::RemoteSettings::new("127.0.0.1", 0);
    let connector_settings =
        crate::cluster::ClusterTcpPeerConnectorSettings::new(Duration::from_millis(25)).unwrap();
    let bootstrap_settings = crate::cluster::ClusterTcpPeerBootstrapSettings::new(settings)
        .with_connector_settings(connector_settings)
        .with_connector_name("facade-cluster-peer");
    let identity = crate::cluster::ClusterTcpPeerBootstrapIdentity::new(1, 11);

    assert_eq!(bootstrap_settings.connector_name(), "facade-cluster-peer");
    assert_eq!(identity.node_uid(), 1);
    assert_eq!(identity.local_system_uid(), 11);
    assert_eq!(crate::cluster::CLUSTER_SYSTEM_MANIFESTS.len(), 12);
    assert!(crate::cluster::CLUSTER_SYSTEM_MANIFESTS.contains(&"kairo.cluster.init-join"));
    assert!(crate::cluster::CLUSTER_SYSTEM_MANIFESTS.contains(&"kairo.cluster.leave"));
    assert!(crate::cluster::CLUSTER_SYSTEM_MANIFESTS.contains(&"kairo.cluster.exiting-confirmed"));
    let _ = crate::cluster::ClusterDaemonBootstrapSettings::new(1);
    let _ = std::mem::size_of::<Option<crate::cluster::ClusterDaemonHandle>>();
    let _ = std::mem::size_of::<Option<crate::cluster::ClusterDaemonRegistration>>();
    let _ = std::mem::size_of::<Option<crate::cluster::ClusterExtension>>();
    let _ = std::mem::size_of::<Option<crate::prelude::ClusterExtension>>();
    let _ = std::mem::size_of::<Option<crate::cluster::ClusterTcpPeerBootstrap>>();
    let _ = std::mem::size_of::<Option<crate::cluster::ClusterTcpPeerBootstrapError>>();
    let _ = std::mem::size_of::<crate::cluster::ClusterTcpPeerBootstrapResult<()>>();
    let _ = std::mem::size_of::<Option<crate::cluster::ClusterTcpPeerConnector>>();
    let _ = std::mem::size_of::<Option<crate::cluster::ClusterTcpPeerConnectorMsg>>();
    let _ = std::mem::size_of::<Option<crate::cluster::ClusterTcpPeerConnectorSnapshot>>();
    let _ = std::mem::size_of::<Option<crate::cluster::ClusterRemotePeerConnector>>();
    let _ = std::mem::size_of::<Option<crate::cluster::ClusterRemotePeerConnectorMsg>>();
    let _ = std::mem::size_of::<Option<crate::cluster::ClusterRemotePeerConnectorSnapshot>>();
    let _ = std::mem::size_of::<Option<crate::cluster::ClusterHeartbeatConnector>>();
    let _ = std::mem::size_of::<Option<crate::cluster::ClusterHeartbeatConnectorMsg>>();
    let _ = std::mem::size_of::<Option<crate::remote::TcpRemotePeerManager>>();
}

#[cfg(feature = "distributed-data")]
#[test]
fn facade_distributed_data_module_exposes_tcp_bootstrap_surface() {
    use std::time::Duration;

    let settings = crate::remote::RemoteSettings::new("127.0.0.1", 0);
    let connector_settings =
        crate::distributed_data::ReplicatorTcpPeerConnectorSettings::new(Duration::from_millis(25))
            .unwrap();
    let bootstrap_settings =
        crate::distributed_data::ReplicatorTcpPeerBootstrapSettings::new(settings)
            .with_connector_settings(connector_settings)
            .with_connector_name("facade-ddata-peer");
    let identity = crate::distributed_data::ReplicatorTcpPeerBootstrapIdentity::new(
        1,
        11,
        crate::distributed_data::ReplicaId::new("facade"),
    );

    assert_eq!(bootstrap_settings.connector_name(), "facade-ddata-peer");
    assert_eq!(identity.node_uid(), 1);
    assert_eq!(identity.local_system_uid(), 11);
    assert_eq!(
        identity.remote_replica(),
        &crate::distributed_data::ReplicaId::new("facade")
    );
    let _ = std::mem::size_of::<Option<crate::distributed_data::ReplicatorTcpPeerBootstrap>>();
    let _ = std::mem::size_of::<Option<crate::distributed_data::ReplicatorTcpPeerBootstrapError>>();
    let _ = std::mem::size_of::<crate::distributed_data::ReplicatorTcpPeerBootstrapResult<()>>();
    let _ = std::mem::size_of::<Option<crate::distributed_data::ReplicatorTcpPeerConnector>>();
    let _ = std::mem::size_of::<Option<crate::distributed_data::ReplicatorTcpPeerConnectorMsg>>();
    let _ =
        std::mem::size_of::<Option<crate::distributed_data::ReplicatorTcpPeerConnectorSnapshot>>();
}

#[cfg(feature = "cluster-tools")]
#[test]
fn facade_cluster_tools_module_exposes_tcp_bootstrap_surface() {
    use std::time::Duration;

    let connector_settings =
        crate::cluster_tools::ClusterToolsTcpPeerConnectorSettings::new(Duration::from_millis(25))
            .unwrap();
    let bootstrap_settings = crate::cluster_tools::ClusterToolsTcpPeerBootstrapSettings::new()
        .with_connector_settings(connector_settings)
        .with_connector_name("facade-tools-peer");

    assert_eq!(bootstrap_settings.connector_name(), "facade-tools-peer");
    let _ = std::mem::size_of::<
        Option<crate::cluster_tools::ClusterToolsTcpPeerBootstrap<PreludeRemoteMsg>>,
    >();
    let _ = std::mem::size_of::<Option<crate::cluster_tools::ClusterToolsTcpPeerBootstrapError>>();
    let _ = std::mem::size_of::<crate::cluster_tools::ClusterToolsTcpPeerBootstrapResult<()>>();
    let _ = std::mem::size_of::<
        Option<crate::cluster_tools::ClusterToolsTcpPeerConnector<PreludeRemoteMsg>>,
    >();
    let _ = std::mem::size_of::<Option<crate::cluster_tools::ClusterToolsTcpPeerConnectorMsg>>();
    let _ =
        std::mem::size_of::<Option<crate::cluster_tools::ClusterToolsTcpPeerConnectorSnapshot>>();
}

#[cfg(feature = "cluster")]
#[test]
fn diagnostic_counters_record_cluster_categories() {
    use crate::prelude::*;

    let counters = DiagnosticCounters::new();
    ClusterDiagnostics::record(
        &counters,
        ClusterDiagnostic::GossipStateChanged {
            previous: kairo_cluster::Gossip::new(),
            current: kairo_cluster::Gossip::new(),
            events: Vec::new(),
        },
    );

    assert_eq!(
        counters.snapshot(),
        DiagnosticCounterSnapshot {
            remote_serialization_failures: 0,
            remote_delivery_failures: 0,
            association_quarantine_events: 0,
            association_close_events: 0,
            cluster_gossip_state_changes: 1,
        }
    );
}

#[cfg(feature = "cluster")]
#[test]
fn diagnostic_text_sink_exports_cluster_lines() {
    use crate::prelude::*;
    use std::sync::{Arc, Mutex};

    let lines = Arc::new(Mutex::new(Vec::new()));
    let sink = DiagnosticTextSink::new({
        let lines = lines.clone();
        move |line| lines.lock().expect("diagnostic lines poisoned").push(line)
    });
    let local = UniqueAddress::new(kairo_actor::Address::local("facade-cluster"), 1);
    let current = kairo_cluster::Gossip::from_members([
        Member::new(local, vec![]).with_status(MemberStatus::Up)
    ]);

    ClusterDiagnostics::record(
        &sink,
        ClusterDiagnostic::GossipStateChanged {
            previous: kairo_cluster::Gossip::new(),
            current,
            events: vec![ClusterEvent::LeaderChanged { leader: None }],
        },
    );

    assert_eq!(
        *lines.lock().expect("diagnostic lines poisoned"),
        vec!["cluster.gossip_state_changed previous_members=0 current_members=1 events=1"]
    );
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
    let _ = std::mem::size_of::<Option<DistributedDataExtension<GCounter>>>();
    let _ = std::mem::size_of::<Option<DistributedDataHandle<GCounter>>>();
    let _ = std::mem::size_of::<Option<DistributedDataRegistration<GCounter>>>();
    let _ = std::mem::size_of::<Option<DistributedDataSettings<GCounter>>>();
    let _ = std::mem::size_of::<Option<DistributedDataBootstrapError>>();
    let _ = DDATA_SYSTEM_MANIFESTS;
    let _ = register_distributed_data::<GCounter>;
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

    struct Account;

    impl Actor for Account {
        type Msg = String;

        fn receive(&mut self, _ctx: &mut Context<Self::Msg>, _msg: Self::Msg) -> ActorResult {
            Ok(())
        }
    }

    let envelope = ShardingEnvelope::new("entity-1", "credit".to_string());
    let (entity_id, message) = envelope.into_parts();
    assert_eq!(entity_id, "entity-1");
    assert_eq!(message, "credit");
    assert_eq!(
        shard_id_for("entity-1", DEFAULT_SHARD_COUNT).expect("valid shard count"),
        default_shard_id_for("entity-1")
    );
    assert_ne!(stable_hash_entity_id("entity-1"), 0);
    let key = EntityTypeKey::<String>::new("Account");
    let _ = Entity::new(key, |_| Account).with_stop_message("stop".to_string());
    let _ = ClusterShardingSettings::default();
    let _ = std::mem::size_of::<Option<ClusterSharding>>();
    let _ = std::mem::size_of::<Option<ClusterShardingRegistration>>();
    let _ = register_cluster_sharding;
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
    assert_eq!(CLUSTER_TOOLS_SYSTEM_MANIFESTS.len(), 8);
    assert_eq!(PUBSUB_SYSTEM_MANIFESTS.len(), 4);
    assert_eq!(SINGLETON_SYSTEM_MANIFESTS.len(), 4);
    assert_eq!(SINGLETON_MESSAGE_MANIFESTS.len(), 1);
    assert_eq!(
        DistributedPubSubSettings::default().gossip_interval(),
        std::time::Duration::from_secs(1)
    );
    let _ = TopicPublishMode::Broadcast;
    let _ = std::mem::size_of::<Option<LocalPubSub<String>>>();
    let _ = std::mem::size_of::<Option<DistributedPubSubMediatorActor<String>>>();
    let _ = std::mem::size_of::<Option<DistributedPubSubMediatorMsg<String>>>();
    let _ = std::mem::size_of::<Option<DistributedPubSubExtension<PreludeRemoteMsg>>>();
    let _ = std::mem::size_of::<Option<DistributedPubSubHandle<PreludeRemoteMsg>>>();
    let _ = std::mem::size_of::<Option<DistributedPubSubRegistration<PreludeRemoteMsg>>>();
    let _ = std::mem::size_of::<Option<LocalSingletonManagerActor<NoopSingleton>>>();
    let _ = std::mem::size_of::<Option<LocalSingletonManagerMsg<String>>>();
    let _ = std::mem::size_of::<Option<ClusterSingleton>>();
    let _ = std::mem::size_of::<Option<ClusterSingletonRegistration>>();
    let _ = std::mem::size_of::<Option<ClusterSingletonRef<PreludeRemoteMsg>>>();
    let _ = std::mem::size_of::<Option<ClusterSingletonConnectorMsg<PreludeRemoteMsg>>>();
    let _ = std::mem::size_of::<Option<Singleton<NoopSingleton>>>();
    assert_eq!(
        ClusterSingletonSettings::default()
            .manager_settings()
            .hand_over_retry_interval(),
        std::time::Duration::from_secs(1)
    );
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
