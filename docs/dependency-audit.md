# Dependency And License Audit

This audit covers the active `kairo-next` workspace with all features enabled.
The old `crates/` implementation is reference material only and is not part of
the normal build surface.

Regenerate the dependency list with:

```bash
cargo tree --workspace --all-features --edges normal,build,dev
cargo metadata --format-version 1 | jq -r '.packages[] | select(.source != null) | [.name, .version, (.license // "license-file:" + (.license_file // "unknown"))] | @tsv' | sort -u
```

## Workspace Packages

All active Kairo crates inherit the workspace `MIT` license. The examples and
benchmark crates are marked `publish = false`; they still inherit the same
license metadata for local builds and documentation.

Active workspace members:

```text
kairo
kairo-actor
kairo-actor-macros
kairo-serialization
kairo-remote
kairo-cluster
kairo-distributed-data
kairo-cluster-sharding
kairo-cluster-tools
kairo-testkit
kairo-examples
kairo-benchmarks
```

## Direct External Dependencies

| Crate | Why it is present |
| --- | --- |
| `bytes` | Stable wire buffers and frame payloads for serialization, remoting, cluster, distributed-data, sharding, tools, and examples. |
| `sha1` | Pekko-compatible, non-security digest of the cluster gossip seen table for status negotiation. |
| `thiserror` | Explicit error enums without hand-written `Display` boilerplate in actor, serialization, and remote crates. |
| `proc-macro2`, `quote`, `syn` | Derive macro implementation for stable remote-message metadata. |
| `toml` | Optional facade configuration feature for the initial TOML loader. |

No external async runtime, HOCON parser, central membership store client,
persistence backend, metrics backend, or benchmarking framework is part of the
active dependency surface.

## Resolved External Licenses

| Crate | Version | License |
| --- | ---: | --- |
| `block-buffer` | 0.10.4 | MIT OR Apache-2.0 |
| `bytes` | 1.11.1 | MIT |
| `cfg-if` | 1.0.4 | MIT OR Apache-2.0 |
| `cpufeatures` | 0.2.17 | MIT OR Apache-2.0 |
| `crypto-common` | 0.1.7 | MIT OR Apache-2.0 |
| `digest` | 0.10.7 | MIT OR Apache-2.0 |
| `equivalent` | 1.0.2 | Apache-2.0 OR MIT |
| `generic-array` | 0.14.7 | MIT |
| `hashbrown` | 0.17.1 | MIT OR Apache-2.0 |
| `indexmap` | 2.14.0 | Apache-2.0 OR MIT |
| `libc` | 0.2.186 | MIT OR Apache-2.0 |
| `proc-macro2` | 1.0.106 | MIT OR Apache-2.0 |
| `quote` | 1.0.45 | MIT OR Apache-2.0 |
| `serde_core` | 1.0.228 | MIT OR Apache-2.0 |
| `serde_derive` | 1.0.228 | MIT OR Apache-2.0 |
| `serde_spanned` | 1.1.1 | MIT OR Apache-2.0 |
| `sha1` | 0.10.7 | MIT OR Apache-2.0 |
| `syn` | 2.0.117 | MIT OR Apache-2.0 |
| `thiserror` | 2.0.18 | MIT OR Apache-2.0 |
| `thiserror-impl` | 2.0.18 | MIT OR Apache-2.0 |
| `toml` | 1.1.2+spec-1.1.0 | MIT OR Apache-2.0 |
| `toml_datetime` | 1.1.1+spec-1.1.0 | MIT OR Apache-2.0 |
| `toml_parser` | 1.1.2+spec-1.1.0 | MIT OR Apache-2.0 |
| `toml_writer` | 1.1.1+spec-1.1.0 | MIT OR Apache-2.0 |
| `typenum` | 1.20.1 | MIT OR Apache-2.0 |
| `unicode-ident` | 1.0.19 | (MIT OR Apache-2.0) AND Unicode-3.0 |
| `version_check` | 0.9.5 | MIT OR Apache-2.0 |
| `winnow` | 1.0.3 | MIT |

## Policy Check

- Dependency set is permissive-license only for the active all-feature
  workspace build.
- TOML is the only configuration parser. No `hocon-rs` or HOCON loader is
  present.
- Cluster membership has no etcd, Kubernetes lease, database, or central-store
  dependency.
- Local actor runtime has no async runtime dependency and still uses
  synchronous `Actor::receive`.
- Remote wire compatibility remains metadata and codec based; no serde,
  bincode, prost, or type-layout serialization dependency is used for public
  remote message contracts.
- The M13 benchmark runner uses the standard library timing APIs instead of a
  new benchmarking dependency.
