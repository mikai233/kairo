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

- `kairo-actor`: typed local actor runtime, lifecycle, supervision, timers,
  adapters, ask, event stream, receptionist, and coordinated shutdown.
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

## Running Examples

From `kairo-next`:

```bash
cargo run -p kairo-examples --example local_counter
cargo run -p kairo-examples --example configured_counter
cargo run -p kairo-examples --example ask_pipe_to_self
cargo run -p kairo-examples --example remote_ping_pong
cargo run -p kairo-examples --example ddata_counter
cargo run -p kairo-examples --example cluster_membership
cargo run -p kairo-examples --example cluster_tools_local
cargo run -p kairo-examples --example cluster_sharding_local
cargo run -p kairo-examples --example cluster_tcp_peer_bootstrap
cargo run -p kairo-examples --example ddata_tcp_peer_bootstrap
cargo run -p kairo-examples --example cluster_tools_tcp_peer_bootstrap
```

The `ask_pipe_to_self` example demonstrates local request/reply through
`Context::ask` and external work returning to the actor through
`Context::pipe_to_self`.

The `configured_counter` example demonstrates TOML-first facade settings loaded
from `examples/kairo.toml` plus `examples/kairo.local.toml`, including actor
dispatcher settings, dead-letter diagnostics, remote transport settings,
sharding timing helpers, and least-shard allocation limits, while keeping
runtime configuration in format-neutral structs.

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

The `cluster_sharding_local` example demonstrates:

```text
EntityRef<String> -> ShardingEnvelope<String> -> ShardRegionActor
  -> EntityShardActor -> typed entity child
```

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

## Validation

Default full validation target:

```bash
cd kairo-next
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets --all-features
```

Focused development usually runs the relevant crate target first, for example:

```bash
cargo test -p kairo-cluster-sharding --all-targets --all-features
cargo clippy -p kairo-cluster-sharding --all-targets --all-features -- -D warnings
```
