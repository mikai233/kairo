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

Kairo declares Rust 1.88 as its minimum supported Rust version. CI checks that
version across all workspace targets and features, while the stable test matrix
runs on Ubuntu, Windows, and macOS. A separate facade matrix compiles the empty,
default, and every advertised individual feature set so optional subsystem
boundaries do not depend accidentally on the unified `full` graph.

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

For rolling remote-message upgrades, keep the serializer id and manifest
stable, increment `RemoteMessage::VERSION`, and teach `MessageCodec::decode` to
read each schema version that can coexist. Compatibility is explicit in both
directions: a newer codec can migrate an older payload, while an older codec
must deliberately accept a newer wire version if forward-compatible traffic is
required. Remoting carries the declared version unchanged and does not infer a
schema from Rust types or negotiate an automatic downgrade.

The canonical remote-envelope frame is independently versioned from business
message schemas. Frame version 1 is pinned by
`kairo-next/crates/kairo-remote/tests/fixtures/remote-envelope-frame-v1.hex`;
wire-layout changes require a new frame version and an explicit compatibility
path rather than changing the version-1 bytes in place.

Reliable system delivery has a separate stable message contract above that
envelope body. The checked
`kairo-next/crates/kairo-remote/tests/fixtures/reliable-system-envelope-v1.hex`
pins sender/receiver incarnation UIDs, the ordered sequence number, and a
nested remote-watch envelope. The companion `reliable-system-reply-v1.hex`
pins the shared cumulative ACK/NACK payload while the serializer id and
manifest keep those reply meanings distinct. Both fixtures must decode and be
reproduced exactly; incompatible changes require new reliable-message versions.

The TCP lane-association handshake has its own versioned contract. Handshake
version 2 is pinned by
`kairo-next/crates/kairo-remote/tests/fixtures/tcp-association-handshake-v2.hex`
in both encode and decode directions; address, incarnation UID, or lane-layout
changes likewise require an explicit handshake-version compatibility path.

Cluster membership payloads are versioned system protocols rather than Rust
data-layout dumps. The full `GossipEnvelope` version-1 payload is pinned by
`kairo-next/crates/kairo-cluster/tests/fixtures/gossip-envelope-v1.hex` in both
directions, including sender and target incarnations, member ordering, seen
state, observer-versioned reachability, vector clocks, and tombstones. Change
that schema only by introducing a new message version whose codec explicitly
retains every rolling-upgrade direction that deployed nodes require.

Distributed-data delta propagation demonstrates that rule across two wire
schema generations. The checked
`kairo-next/crates/kairo-distributed-data/tests/fixtures/delta-propagation-v1.hex`
must continue to decode without pruning metadata, while the version-2 fixture
in the same directory pins current encode/decode bytes including initialized
and performed removed-replica pruning state. A future delta schema must keep
every required older decode path rather than reinterpreting either fixture.

Cluster-tools pubsub registry convergence has the same explicit boundary. The
checked
`kairo-next/crates/kairo-cluster-tools/tests/fixtures/pubsub-delta-v1.hex`
pins `PubSubDelta` version 1 in both directions, including source and bucket
incarnations, bucket and entry versions, topic/group/path key tags, and removal
tombstones. Change that payload only through a new message version with the
required rolling-upgrade decode paths.

Cluster-singleton handover and business delivery are now pinned at the same
boundary. The checked `singleton-handover-v1.hex` fixture in the cluster-tools
fixture directory fixes the explicit sender incarnation payload shared by the
four distinct handover controls. The companion `singleton-message-v1.hex`
fixture fixes the nested business serializer id, manifest, version, and bytes.
Both fixtures must decode and be reproduced exactly; incompatible changes
require a new singleton protocol version with deliberate rolling-upgrade
handling rather than reinterpretation of version 1.

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

For compact hand-written formats, register the payload functions directly
without defining a one-off codec type:

```rust
let mut registry = Registry::new();
registry.register_with::<CounterCommand, _, _>(
    12_001,
    encode_counter_command,
    decode_counter_command,
)?;
```

The decode function receives the wire version so it can retain every schema
generation needed during a rolling upgrade. Named `MessageCodec`
implementations remain available when a codec owns reusable configuration or
state; `register_with` adds no implicit format and does not choose metadata.

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

Sharding ownership and forwarded entity traffic are stable system protocols.
Version-1 fixtures under
`kairo-next/crates/kairo-cluster-sharding/tests/fixtures/` pin `ShardHome`
region ownership and `RoutedShardEnvelope` shard/entity routing with nested
business serializer metadata in both encode and decode directions. Change
either payload only through a new message version and an explicit
rolling-upgrade path; never derive these bytes from Rust type names or layout.

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
cargo check -p kairo --all-targets --no-default-features
cargo check -p kairo --all-targets
for feature in actor macros config serialization remote cluster distributed-data cluster-tools cluster-sharding testkit full; do
  cargo check -p kairo --all-targets --no-default-features --features "$feature"
done
cargo test -p kairo-examples --all-targets --all-features
cargo test --doc --workspace --all-features
cargo test -p kairo-examples --doc --all-features
cargo test -p kairo-testkit multi_node --all-targets --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
cargo package --workspace --all-features --exclude kairo-examples --exclude kairo-benchmarks
cargo clippy -p kairo-examples --all-targets --all-features -- -D warnings
```

The initial M13 benchmark suite is optional for everyday edits but should be
run when changing actor dispatch, remote outbound delivery, gossip merge,
sharding route behavior, or passivation buffering:

```bash
cargo run -p kairo-benchmarks -- --help
KAIRO_BENCH_ITERS=100 cargo run -p kairo-benchmarks --release -- all
cargo run -p kairo-benchmarks --release -- all
KAIRO_BENCH_ITERS=10000 cargo run -p kairo-benchmarks --release -- actor-tell
KAIRO_BENCH_ITERS=10000 cargo run -p kairo-benchmarks --release -- remote-send
KAIRO_BENCH_ITERS=10000 cargo run -p kairo-benchmarks --release -- gossip-merge
KAIRO_BENCH_ITERS=10000 cargo run -p kairo-benchmarks --release -- sharding-route
KAIRO_BENCH_ITERS=10000 cargo run -p kairo-benchmarks --release -- shard-passivation
```

The full workspace target is:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets --all-features
```
