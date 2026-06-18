# kairo-next

`kairo-next` is the rewrite workspace for Kairo. The old implementation under
`crates/` is intentionally kept as reference code but is no longer part of the
root Cargo workspace.

The rewrite starts with final crate boundaries instead of a temporary single
crate so remote, cluster, sharding, and tooling can evolve without a later
workspace split.

## Crates

- `kairo`: facade crate for common users.
- `kairo-actor`: typed local actor runtime, actor tree, supervision, mailbox,
  scheduler, receptionist, event stream, and coordinated shutdown.
- `kairo-actor-macros`: derive and attribute macros for message manifests and
  ergonomic protocol declarations.
- `kairo-serialization`: serializer registry, manifests, serializer ids, and
  rolling-update friendly wire payloads.
- `kairo-remote`: remote actor references, associations, transports, and remote
  death watch.
- `kairo-cluster`: gossip membership, reachability, failure detector, cluster
  events, and split-brain/downing hooks.
- `kairo-distributed-data`: CRDT-based replicated data built on cluster gossip,
  used by sharding state stores and higher-level cluster tools.
- `kairo-cluster-sharding`: entity refs, shard regions, coordinators,
  allocation, handoff, rebalancing, and passivation.
- `kairo-cluster-tools`: singleton, distributed pubsub/topic, and higher-level
  cluster utilities.
- `kairo-testkit`: probes, manual time, actor system test harnesses, and
  multi-node test helpers.
- `kairo-examples`: runnable vertical slices that validate public facade
  workflows across local and distributed features.
- `kairo-benchmarks`: dependency-light M13 benchmark runner for actor tell,
  remote send, gossip merge, and sharding route throughput.

See `ARCHITECTURE.md` for the planned public model and implementation order.
For migration guidance from the old reference crates, see
`../docs/migration.md`.

`kairo-examples` and `kairo-benchmarks` are leaf support crates. Runtime crates
must not depend on them.

## Facade Features

The `kairo` facade is the recommended dependency for application code. The
default feature set stays local and configuration focused, while distributed
layers remain explicit opt-ins:

| Feature | Enables |
| --- | --- |
| `default` | `actor`, `macros`, `config` |
| `serialization` | stable remote-message metadata and codec registry |
| `remote` | `actor`, `serialization`, remote refs and associations |
| `cluster` | `remote`, gossip membership and downing hooks |
| `distributed-data` | `cluster`, CRDT replication |
| `cluster-sharding` | `cluster`, `distributed-data`, entity routing |
| `cluster-tools` | `cluster`, `distributed-data`, singleton and pubsub |
| `testkit` | local actor test utilities without distributed runtime layers |
| `full` | every public facade feature for integration checks |

## Core User Model

Local actor protocols are plain Rust message types sent through `ActorRef<M>`.
They do not need manifests, serializer ids, codecs, or any other wire metadata
unless the protocol crosses a remote boundary.

Remote-capable messages are explicit wire contracts. A remote message must
implement `RemoteMessage` with a stable manifest and version, and it must have
a registered `MessageCodec`. Do not rely on Rust enum discriminants, type
names, memory layout, or compiler-generated details as the remote protocol.

`Actor::receive` is synchronous by design. Actor state changes happen in one
mailbox turn at a time, which keeps ownership and failure behavior explicit.
Async work should run outside the actor turn and return through `tell`,
`Context::ask`, `Context::pipe_to_self`, timers, or adapters.

Cluster membership is gossip plus local failure-detector observations. Seed and
discovery settings may provide contact addresses, but they are not membership
truth, and Kairo does not use etcd or another central authoritative membership
store.

Cluster sharding keeps routing metadata at the sharding boundary. Use
`EntityRef<M>` when a caller already knows the entity id, or send
`ShardingEnvelope<M>` to a region. Entity actors receive business messages `M`;
the entity id does not need to be embedded in every business protocol message.

## Examples

Runnable examples live in the `kairo-examples` crate:

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

The `local_counter` example demonstrates the first Rust-first actor workflow:
spawn a typed actor, send local messages without serialization, request a value
through an explicit reply channel, and stop the actor.

The `configured_counter` example discovers the standard
`kairo-next/crates/kairo-examples/examples/kairo.toml` plus
`kairo-next/crates/kairo-examples/examples/kairo.local.toml` pair, maps the
layered format-neutral settings into an `ActorSystemBuilder`, validates
diagnostics, remote transport, sharding timing, and least-shard allocation
helpers through the facade, and runs the same typed counter protocol with the
configured dispatcher throughput.

The `ask_pipe_to_self` example keeps a calculation service and a pattern
coordinator in focused modules while demonstrating `Context::ask` and
`Context::pipe_to_self` as mailbox-returning local actor patterns.

The `remote_ping_pong` example binds two TCP remoting actor systems, registers
an explicit stable codec for a typed ping/pong protocol, sends a remote ping,
and returns a remote pong through the sender's canonical actor ref.

The `ddata_counter` example runs a local distributed-data
`ReplicatorActor<GCounter>`, subscribes to a key, applies a local increment,
flushes the change notification, and reads the CRDT value back.

The `cluster_membership` example subscribes to cluster state, observes the
initial snapshot, publishes a gossip view that marks a peer `Up`, publishes a
removal, and requests the final current state through the cluster facade.

The `cluster_tools_local` example runs a local pubsub actor, verifies
subscribe/publish/current-topics behavior, starts a singleton through the local
singleton manager, and sends a typed message to the running singleton child.

The `cluster_tools_singleton` example drives two local singleton managers
through the previous-oldest to new-oldest handover workflow and verifies the
replacement singleton starts only after the previous singleton has stopped.

The `cluster_tools_distributed` example starts two distributed pubsub
mediators, merges one mediator's registry delta into the other, publishes to a
remote topic subscriber, and validates one-message-per-group delivery across
local and remote groups.

The `cluster_sharding_local` example wires a local shard coordinator, typed
shard region, stable `ShardingEnvelopeRouter`, `EntityRef<String>`, and
entity-backed shard actor. Business messages reach a typed entity child without
embedding the entity id in the business message. It also exercises entity
passivation/restart through the same `EntityRef<String>` and a two-region
graceful local shard movement that rehosts a remembered entity.

The TCP peer bootstrap examples demonstrate the current cluster,
distributed-data, and cluster-tools route setup around the shared remote
association primitives. The cluster-tools TCP example covers distributed
pubsub publish delivery plus registered-path `Send` and `SendToAll` delivery
over stable remote envelopes. Each TCP bootstrap binary also prints a
coordinated-shutdown observation with before/after route counts so the runnable
example surface shows that bootstrap-owned association routes are cleared
before shutdown reports success.

## Validation

The root CI workflow runs the normal next-workspace validation matrix:

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

Run the default full validation target locally with:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets --all-features
```

Runnable examples and the local multi-node test harness can be checked
directly while developing integration workflows:

```bash
cargo test -p kairo-examples --all-targets --all-features
cargo test --doc --workspace --all-features
cargo test -p kairo-examples --doc --all-features
cargo test -p kairo-testkit multi_node --all-targets --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
KAIRO_BENCH_ITERS=100 cargo run -p kairo-benchmarks -- all
```

## Benchmarks

`kairo-benchmarks` is the initial M13 benchmark suite. It uses the standard
library plus Kairo's public APIs to measure actor tell throughput, remote
outbound send overhead, gossip merge cost, and sharding route throughput.
Set `KAIRO_BENCH_ITERS` to a positive integer to override the default
iteration count.

```bash
cargo run -p kairo-benchmarks -- --help
cargo run -p kairo-benchmarks --release -- all
KAIRO_BENCH_ITERS=10000 cargo run -p kairo-benchmarks --release -- actor-tell
KAIRO_BENCH_ITERS=10000 cargo run -p kairo-benchmarks --release -- remote-send
KAIRO_BENCH_ITERS=10000 cargo run -p kairo-benchmarks --release -- gossip-merge
KAIRO_BENCH_ITERS=10000 cargo run -p kairo-benchmarks --release -- sharding-route
```

The M13 dependency and license audit lives in
[`../docs/dependency-audit.md`](../docs/dependency-audit.md).
