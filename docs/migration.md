# Migration Notes

These notes describe the current path from the old `crates/` implementation to
the Rust-first rewrite under `kairo-next/`. The old crates remain reference
material only; normal development, examples, and validation should use the
`kairo-next` workspace.

## Recommended Entry Point

The `kairo` facade is the recommended migration entry point:

```rust
use kairo::prelude::*;
```

The facade keeps the implementation split across focused crates while exposing
common actor, configuration, serialization, remote, cluster, distributed-data,
sharding, cluster-tools, and testkit entry points behind feature flags.

Default facade features enable typed local actors, macros, and TOML
configuration loading. Distributed layers are opt-in:

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

For subsystem internals or advanced tests, import the focused crates directly:

```rust
use kairo_actor::{Actor, ActorSystem, Context, Props};
use kairo_cluster_sharding::{EntityRef, ShardingEnvelope};
```

## Legacy Crates Status And Removal Plan

The old `crates/` tree is reference material only. It is intentionally excluded
from the root workspace, normal validation, runnable examples, and new
implementation work.

Do not add new features, fixes, or examples to `crates/` unless the change is
strictly needed to preserve reference material for the rewrite. New user-facing
work belongs under `kairo-next/`, and migration examples should use the `kairo`
facade or focused `kairo-next/crates/*` crates.

The legacy tree can be removed after these release-hardening gates are met:

- the `kairo` facade is the documented entry point for normal users;
- examples cover the local actor, configuration, remote, cluster,
  distributed-data, sharding, singleton, local pubsub, and distributed pubsub
  workflows;
- full workspace CI runs formatting, clippy with warnings denied, and tests for
  `kairo-next/crates/*`;
- workspace and active crate manifests do not depend on `crates/`;
- remaining migration gaps are tracked as release issues rather than requiring
  the old implementation for normal builds or examples.

Removal should happen as a separate `chore` or `docs` checkpoint so it can be
reviewed independently from behavior changes. That checkpoint should update the
root README, migration notes, progress log, and any CI or packaging references
that still mention the legacy tree.

## Actor Protocols

Old dynamic or erased message paths should be replaced with typed protocols and
typed refs:

```rust
enum CounterCmd {
    Increment,
    Get { reply_to: ActorRef<i64> },
}
```

`Actor::receive` is synchronous. Async work should return to the actor through
messages by using `Context::pipe_to_self`, `Context::ask`, timers, or message
adapters. Do not introduce `AsyncActor` for the initial rewrite model.

Local-only messages do not need serialization metadata. Add remote metadata only
when a message crosses a remote boundary.

The runnable `ask_pipe_to_self` example shows local request/reply and external
work returning through the actor mailbox without borrowing actor state across
an await point:

```bash
cargo run -p kairo-examples --example ask_pipe_to_self
```

## Actor-System Extensions

Use extensions for shared actor-system services that must be created once per
system and retrieved type-safely:

```rust
use std::sync::Arc;

let created = system.register_extension(|system| MyExtension::new(system.name()));
let looked_up = system.extension::<MyExtension>()?;
assert!(Arc::ptr_eq(&created, &looked_up));
```

Extensions are keyed by their Rust type and scoped to one `ActorSystem`.
Mutable actor-like behavior should usually remain in actors, with the extension
holding typed refs, handles, or other thread-safe service state.

## Configuration

Use TOML for file-backed configuration:

```toml
[actor.dispatchers.default]
throughput = 16
workers = 4

[actor.task_executor]
workers = 4
queue_capacity = 1024

[cluster.seed]
nodes = ["kairo://cluster@seed-a.example.test:25520"]

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
hand_over_retry_interval = "1s"

[cluster.tools.pubsub]
gossip_interval = "500ms"
max_delta_entries = 250

[observability.diagnostics]
dead_letters = true
remote_delivery_failures = true
serialization_failures = true
quarantine_events = true
gossip_state_changes = true
```

Load the standard `kairo.toml` plus `kairo.local.toml` pair, one explicit file,
or a layered stack through the facade, then map the format-neutral settings
into builders:

```rust
let standard = kairo::prelude::load_standard_toml_files(".")?;
let standard_system = standard.actor_system_builder("app-standard")?.build()?;

let settings = kairo::prelude::load_toml_file("kairo.local.toml")?;
let system = settings.actor_system_builder("app")?.build()?;

let layered = kairo::prelude::load_toml_files(["kairo.toml", "kairo.local.toml"])?;
let layered_system = layered.actor_system_builder("app-local")?.build()?;
```

Seed nodes are contact addresses only, not membership truth. When remoting is
enabled, convert them into remote association addresses for dial/bootstrap
code:

```rust
let seed_contacts = settings.cluster.seed.to_remote_association_addresses()?;
```

Cluster downing settings can be mapped into runtime hooks for `none`,
`down-all`, `keep-majority`, and `keep-oldest`:

```rust
let downing = settings.cluster.downing.to_downing_hook()?;
```

`lease-majority` requires an explicit lease implementation supplied by the
application:

```rust
let lease_hook = settings.cluster.downing.to_lease_majority_hook(my_lease)?;
```

Sharding and cluster-tools settings also expose runtime helpers:

```rust
let shard = settings.cluster.sharding.shard_id_for("account-42")?;
let remember_entities = settings.cluster.sharding.remember_entities_enabled();
let retry_every = settings.cluster.sharding.to_retry_interval()?;
let handoff_timeout = settings.cluster.sharding.to_handoff_timeout()?;
let shard_failure_backoff = settings.cluster.sharding.to_shard_failure_backoff()?;
let rebalance_every = settings.cluster.sharding.to_rebalance_interval()?;
let query_timeout = settings.cluster.sharding.to_shard_region_query_timeout()?;
let allocation = settings
    .cluster
    .sharding
    .to_least_shard_allocation_strategy()?;
let singleton_scope = settings.cluster.tools.to_singleton_scope()?;
let gossip_every = settings.cluster.tools.to_pubsub_gossip_interval()?;
```

These settings are format-neutral Rust values after loading. The current
`configured_counter` example uses standard discovery for
`examples/kairo.toml` plus `examples/kairo.local.toml` to validate
base-and-local layering, actor-system builder configuration, dead-letter
diagnostics, remote transport settings, sharding timing values, and
least-shard allocation settings through the facade without making TOML syntax
part of the runtime API.

Observability settings are backend-neutral. Use
`settings.observability.diagnostics` to decide which diagnostic categories an
application or runtime integration should publish. `KairoSettings` applies the
dead-letter diagnostic flag when building an actor system:

```rust
let diagnostics = &settings.observability.diagnostics;
assert!(diagnostics.dead_letters);
assert!(diagnostics.publishes_runtime_failures());
```

Do not add HOCON or `hocon-rs` until that parser is intentionally adopted.

## Remote Messages

Remote-capable messages must declare stable wire metadata:

```rust
#[derive(Debug, KairoRemoteMessage)]
#[kairo(manifest = "example.CounterCommand")]
#[kairo(version = 1)]
struct CounterCommand;
```

Wire compatibility is based on manifests, versions, serializer ids, and
registered codecs. Do not rely on Rust type names, enum discriminants, memory
layout, or compiler-generated details as the wire contract.

Remote inbound paths can attach backend-neutral diagnostics for decode and
delivery failures:

```rust
let inbound = inbound.with_diagnostics(diagnostics);
```

Actor-system inbound composition has matching constructors:

```rust
let inbound = ActorSystemRemoteInbound::<CounterCommand>::with_diagnostics(
    system,
    registry,
    remote_watch,
    local_system_uid,
    diagnostics,
);
```

Facade settings can filter a caller-provided observer before installation:

```rust
if let Some(diagnostics) = settings
    .observability
    .diagnostics
    .remote_inbound_diagnostics(diagnostics)
{
    inbound = inbound.with_diagnostics(diagnostics);
}
```

The diagnostic sink receives structured recipient, optional sender, manifest,
version, serializer id, and reason data, so applications can route failures to
logs, metrics, tests, or event streams without changing the wire contract.
The `kairo` facade includes dependency-free adapters for common cases:
`DiagnosticCounters` records per-category atomic counts for metrics export, and
`DiagnosticTextSink` turns the same structured events into stable single-line
records that can be forwarded to `log`, `tracing`, stderr, files, or tests
without adding a Kairo logging dependency.

The runnable `remote_ping_pong` example shows the same remote-message contract
end to end with a registered codec and two loopback TCP remoting actor systems:

```bash
cargo run -p kairo-examples --example remote_ping_pong
```

The runnable `ddata_counter` example shows local distributed-data usage with a
`ReplicatorActor<GCounter>`, key subscription, update, change notification, and
readback:

```bash
cargo run -p kairo-examples --example ddata_counter
```

Remote associations can report quarantine transitions through the same
backend-neutral observer style:

```rust
if let Some(diagnostics) = settings
    .observability
    .diagnostics
    .remote_association_diagnostics(diagnostics)
{
    association = association.with_diagnostics(diagnostics);
}
```

The observer receives the remote address, optional remote UID, and quarantine
reason after the association enters the quarantined state.

## Sharding

Prefer routing through `EntityRef<M>` or `ShardingEnvelope<M>`:

```rust
let account: EntityRef<AccountCmd> = sharding.entity_ref("account-42");
account.tell(AccountCmd::Credit(10))?;
```

Business messages should not need embedded entity ids by default. Shard ids use
the documented stable FNV-1a hash exposed by `stable_hash_entity_id` and
`shard_id_for`; do not use Rust `DefaultHasher` for cross-node routing.

The runnable `cluster_sharding_local` example wires a local coordinator,
shard region, `ShardingEnvelopeRouter`, and `EntityRef<String>` through the
current actor-backed sharding path. It also demonstrates passivation/restart
and graceful local shard movement:

```bash
cargo run -p kairo-examples --example cluster_sharding_local
```

For the composed distributed lifecycle, `cluster_sharding_tcp` starts three
cluster-daemon nodes, uses the singleton coordinator and ORSet-backed remember
entities, rebalances an existing shard, removes the oldest node through
coordinated shutdown, and verifies entity recovery before the next command:

```bash
cargo run -p kairo-examples --example cluster_sharding_tcp
```

## Cluster Membership

Cluster truth is gossip membership plus local failure-detector observations.
Discovery may provide seed or contact addresses only. Do not migrate old
membership flows to etcd, Kubernetes leases, or another central authoritative
store.

Cluster gossip diagnostics use the same backend-neutral observer style:

```rust
if let Some(diagnostics) = settings
    .observability
    .diagnostics
    .cluster_diagnostics(diagnostics)
{
    let publisher = ClusterEventPublisher::new(self_node)
        .with_diagnostics(diagnostics);
}
```

The observer receives previous gossip, current gossip, and the computed
cluster-event diff for each real gossip state change.

The runnable `cluster_membership` example shows the public cluster facade
subscription model end to end: initial state snapshot, `MemberUp` event,
`MemberRemoved` event, and current-state request:

```bash
cargo run -p kairo-examples --example cluster_membership
```

The TCP peer bootstrap examples exercise the current cluster-derived route
owners for cluster membership, distributed-data, and cluster-tools system
traffic over loopback remoting. The cluster-tools TCP example includes
distributed pubsub publish delivery and registered-path `Send`/`SendToAll`
delivery through stable remote envelopes:

```bash
cargo run -p kairo-examples --example cluster_tcp_peer_bootstrap
cargo run -p kairo-examples --example ddata_tcp_peer_bootstrap
cargo run -p kairo-examples --example cluster_tools_tcp_peer_bootstrap
```

The runnable `cluster_tools_local` example shows local cluster-tools usage:
pubsub subscribe/publish/topic listing plus singleton manager startup and typed
access to the singleton child:

```bash
cargo run -p kairo-examples --example cluster_tools_local
```

The runnable `cluster_tools_singleton` example focuses on singleton handover:
two local singleton managers model the previous-oldest to new-oldest protocol,
the previous owner stops its singleton child, and the new owner starts the
replacement only after that stop is observed:

```bash
cargo run -p kairo-examples --example cluster_tools_singleton
```

The runnable `cluster_tools_distributed` example shows distributed pubsub
usage without relying on the legacy crates: two mediators exchange registry
deltas, a publish from one mediator reaches a remote topic subscriber, and
one-message-per-group delivery reaches one local group and one remote group:

```bash
cargo run -p kairo-examples --example cluster_tools_distributed
```

## Validation

Run focused validation for the area you touch first, then widen as needed:

```bash
cargo fmt --all -- --check
cargo test -p kairo-examples --all-targets --all-features
cargo test --doc --workspace --all-features
cargo test -p kairo-examples --doc --all-features
cargo test -p kairo-testkit multi_node --all-targets --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
cargo clippy -p kairo-examples --all-targets --all-features -- -D warnings
```

The initial M13 benchmark suite is optional for everyday edits but should be
run when changing actor dispatch, remote outbound delivery, gossip merge, or
sharding route behavior:

```bash
cargo run -p kairo-benchmarks -- --help
KAIRO_BENCH_ITERS=100 cargo run -p kairo-benchmarks -- all
cargo run -p kairo-benchmarks --release -- all
KAIRO_BENCH_ITERS=10000 cargo run -p kairo-benchmarks --release -- actor-tell
KAIRO_BENCH_ITERS=10000 cargo run -p kairo-benchmarks --release -- remote-send
KAIRO_BENCH_ITERS=10000 cargo run -p kairo-benchmarks --release -- gossip-merge
KAIRO_BENCH_ITERS=10000 cargo run -p kairo-benchmarks --release -- sharding-route
```

The full workspace target is:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets --all-features
```
