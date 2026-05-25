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
implementation status.

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
cargo run -p kairo-examples --example cluster_sharding_local
cargo run -p kairo-examples --example ddata_tcp_peer_bootstrap
cargo run -p kairo-examples --example cluster_tools_tcp_peer_bootstrap
```

The `cluster_sharding_local` example demonstrates:

```text
EntityRef<String> -> ShardingEnvelope<String> -> ShardRegionActor
  -> EntityShardActor -> typed entity child
```

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
