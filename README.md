# Kairo

Kairo is being rewritten as a Rust-first actor framework inspired by Apache
Pekko/Akka. The active implementation lives in [`kairo-next`](kairo-next/).
The old crates under [`crates`](crates/) are retained as reference material and
are not part of the normal workspace build.

The rewrite is intentionally typed and modular:

- local actors use `ActorRef<M>` and synchronous `Actor::receive` turns;
- local-only messages do not require serialization;
- remote messages use stable manifests, versions, serializer ids, and
  registered codecs;
- cluster membership is gossip plus local failure-detector observations, not
  etcd or another central store;
- sharding routes business messages through `EntityRef<M>` and
  `ShardingEnvelope<M>` so entity ids do not need to be embedded in every
  business message.

See [`docs/goal.md`](docs/goal.md) for the product roadmap,
[`kairo-next/ARCHITECTURE.md`](kairo-next/ARCHITECTURE.md) for the technical
contract, and [`docs/progress.md`](docs/progress.md) for current
implementation status. Migration guidance from the old reference crates to the
new facade lives in [`docs/migration.md`](docs/migration.md).

## Current Workspace

The normal workspace is under `kairo-next/crates/*` and includes:

- `kairo`: facade crate for common users and feature-gated subsystem entry
  points.
- `kairo-actor`: typed local actor runtime, lifecycle, supervision, timers,
  adapters, ask, event stream, receptionist, and coordinated shutdown.
- `kairo-actor-macros`: derive and attribute macros for stable remote-message
  manifests and ergonomic protocol declarations.
- `kairo-serialization`: stable remote message metadata and codec registry.
- `kairo-remote`: remote actor refs, associations, TCP framing, and remote
  death watch.
- `kairo-cluster`: gossip membership, vector clocks, reachability, failure
  detection, convergence, leader actions, and downing hooks.
- `kairo-distributed-data`: CRDT replication, delta/full-state propagation,
  pruning, and TCP peer bootstrap.
- `kairo-cluster-sharding`: entity refs, shard regions, coordinators,
  allocation, handoff, passivation, remember-entity storage, and routed remote
  region envelopes.
- `kairo-cluster-tools`: singleton and distributed pubsub tools.
- `kairo-testkit`: deterministic probes and manual-time test support.
- `kairo-examples`: runnable vertical slices for local actors, configuration,
  remoting, cluster membership, distributed data, sharding, and cluster tools.
- `kairo-benchmarks`: dependency-light M13 benchmark runner for actor tell,
  remote send, gossip merge, and sharding route throughput.

## Facade Features

The `kairo` facade is the recommended user entry point. Its default feature set
enables local actors, macros, and TOML configuration loading. Distributed
runtime layers are opt-in and preserve the architecture dependency order:

| Feature | Enables |
| --- | --- |
| `default` | `actor`, `macros`, `config` |
| `serialization` | stable remote-message metadata and codec registry |
| `remote` | `actor`, `serialization`, remote refs and associations |
| `cluster` | `remote`, gossip membership and downing hooks |
| `distributed-data` | `cluster`, CRDT replication |
| `cluster-sharding` | `cluster`, `distributed-data`, `cluster-tools`, entity routing |
| `cluster-tools` | `cluster`, `distributed-data`, singleton and pubsub |
| `testkit` | local actor test utilities without distributed runtime layers |
| `full` | every public facade feature for integration checks |

## Running Examples

From the repository root:

```bash
cargo run -p kairo-examples --example local_counter
cargo run -p kairo-examples --example configured_counter
cargo run -p kairo-examples --example ask_pipe_to_self
cargo run -p kairo-examples --example remote_ping_pong
cargo run -p kairo-examples --example ddata_counter
cargo run -p kairo-examples --example cluster_membership
cargo run -p kairo-examples --example cluster_tools_local
cargo run -p kairo-examples --example cluster_tools_singleton
cargo run -p kairo-examples --example cluster_tools_distributed
cargo run -p kairo-examples --example cluster_sharding_local
cargo run -p kairo-examples --example cluster_tcp_peer_bootstrap
cargo run -p kairo-examples --example ddata_tcp_peer_bootstrap
cargo run -p kairo-examples --example cluster_tools_tcp_peer_bootstrap
```

The `ask_pipe_to_self` example demonstrates local request/reply through
`Context::ask` and external work returning to the actor through
`Context::pipe_to_self`.

The `configured_counter` example demonstrates TOML-first facade settings
discovered from the standard `examples/kairo.toml` plus
`examples/kairo.local.toml` pair, including actor dispatcher settings,
dead-letter diagnostics, remote transport settings, sharding timing helpers,
and least-shard allocation limits, while keeping runtime configuration in
format-neutral structs.

The `remote_ping_pong` example demonstrates two local TCP remoting actor
systems exchanging typed messages through stable remote manifests and an
explicit registered codec.

The `ddata_counter` example demonstrates a local distributed-data
`ReplicatorActor<GCounter>` update, change notification, and readback.

The `cluster_membership` example demonstrates cluster-event subscription,
initial snapshot delivery, member-up publication, member removal, and
current-state request through the public cluster facade.

The `cluster_tools_local` example demonstrates local cluster-tools workflows:
pubsub subscribe/publish/topic listing and singleton manager startup with
typed access to the running singleton child.

The `cluster_tools_singleton` example demonstrates a two-manager singleton
handover workflow: the new oldest requests handover, the previous oldest stops
its singleton child, and the new oldest starts its replacement only after the
previous child has stopped.

The `cluster_tools_distributed` example demonstrates two distributed pubsub
mediators exchanging registry deltas, remote topic publish delivery, and
one-message-per-group routing across local and remote groups.

The `cluster_tools_tcp_peer_bootstrap` example demonstrates loopback TCP routes
for cluster-tools system traffic, including distributed pubsub publish and
registered-path `Send`/`SendToAll` delivery through stable remote envelopes.
The cluster, distributed-data, and cluster-tools TCP bootstrap binaries also
print their coordinated-shutdown observation, including the before/after route
counts that prove bootstrap-owned association routes are cleared before
shutdown reports success.

The `cluster_sharding_local` example demonstrates:

```text
EntityRef<String> -> ShardingEnvelope<String> -> ShardRegionActor
  -> EntityShardActor -> typed entity child
```

It also exercises entity passivation and restart through the same
`EntityRef<String>`, then runs a two-region graceful local shard movement that
rehosts a remembered entity on the surviving region.

## Configuration

Kairo starts with TOML file loading while keeping runtime settings
format-neutral. Applications can discover the standard `kairo.toml` plus
`kairo.local.toml` pair, load one explicit file, layer explicit base and local
overrides, or parse inline configuration text before converting the result into
runtime builders:

```rust
use kairo::prelude::{
    load_standard_toml_files, load_toml_file, load_toml_files, parse_toml_str,
};

let standard_settings = load_standard_toml_files(".")?;
let file_settings = load_toml_file("kairo.local.toml")?;
let layered_settings = load_toml_files(["kairo.toml", "kairo.local.toml"])?;
let inline_settings = parse_toml_str("[actor.dispatchers.default]\nthroughput = 8")?;

let system = layered_settings.actor_system_builder("app")?.build()?;
```

`KairoSettings` stores actor, remote, cluster, sharding, cluster-tools, and
diagnostics settings without exposing TOML-specific concepts, so future
configuration loaders can project into the same runtime model.
For dependency-free observability bridges, the facade also exposes
`DiagnosticCounters` for metrics-style category counts and `DiagnosticTextSink`
for stable single-line diagnostic records that applications can forward into
their own logging or tracing stack.

## Validation

The GitHub Actions validation matrix is intentionally small and mirrors the
normal next-workspace development surface:

```text
Format: cargo fmt --all -- --check
Clippy: cargo clippy --workspace --all-targets --all-features -- -D warnings
Test: cargo test --workspace --all-targets --all-features
Examples and Multi-Node:
  cargo test -p kairo-examples --all-targets --all-features
  cargo test --doc --workspace --all-features
  cargo test -p kairo-examples --doc --all-features
  cargo test -p kairo-testkit multi_node --all-targets --all-features
Rustdoc: RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
Benchmark Smoke: KAIRO_BENCH_ITERS=100 cargo run -p kairo-benchmarks -- all
```

Run the same default full validation target locally from the repository root:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets --all-features
```

Focused development usually runs the relevant crate target first, for example:

```bash
cargo test -p kairo-cluster-sharding --all-targets --all-features
cargo clippy -p kairo-cluster-sharding --all-targets --all-features -- -D warnings
```

Examples and local multi-node harness coverage can be validated directly:

```bash
cargo test -p kairo-examples --all-targets --all-features
cargo test --doc --workspace --all-features
cargo test -p kairo-examples --doc --all-features
cargo test -p kairo-testkit multi_node --all-targets --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
KAIRO_BENCH_ITERS=100 cargo run -p kairo-benchmarks -- all
```

## Benchmarks

The `kairo-benchmarks` crate provides a dependency-light baseline for the M13
performance surface: actor tell throughput, remote outbound send overhead,
gossip merge cost, and sharding route throughput.

```bash
cargo run -p kairo-benchmarks -- --help
cargo run -p kairo-benchmarks --release -- all
KAIRO_BENCH_ITERS=10000 cargo run -p kairo-benchmarks --release -- actor-tell
KAIRO_BENCH_ITERS=10000 cargo run -p kairo-benchmarks --release -- remote-send
KAIRO_BENCH_ITERS=10000 cargo run -p kairo-benchmarks --release -- gossip-merge
KAIRO_BENCH_ITERS=10000 cargo run -p kairo-benchmarks --release -- sharding-route
```

The current M13 dependency and license audit is tracked in
[docs/dependency-audit.md](docs/dependency-audit.md).
