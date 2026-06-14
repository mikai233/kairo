# Migration Notes

These notes describe the current path from the old `crates/` implementation to
the Rust-first rewrite under `kairo-next/`. The old crates remain reference
material only; normal development, examples, and validation should use the
`kairo-next` workspace.

## Recommended Entry Point

Prefer the facade crate:

```rust
use kairo::prelude::*;
```

The facade keeps the implementation split across focused crates while exposing
common actor, configuration, serialization, remote, cluster, distributed-data,
sharding, cluster-tools, and testkit entry points behind feature flags.

For subsystem internals or advanced tests, import the focused crates directly:

```rust
use kairo_actor::{Actor, ActorSystem, Context, Props};
use kairo_cluster_sharding::{EntityRef, ShardingEnvelope};
```

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

[cluster.seed]
nodes = ["kairo://cluster@seed-a.example.test:25520"]

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
remote_delivery_failures = true
serialization_failures = true
quarantine_events = true
gossip_state_changes = true
```

Load it through the facade and map the format-neutral settings into builders:

```rust
let settings = kairo::prelude::load_toml_file("kairo.local.toml")?;
let system = settings.actor_system_builder("app")?.build()?;
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
let rebalance_every = settings.cluster.sharding.to_rebalance_interval()?;
let singleton_scope = settings.cluster.tools.to_singleton_scope()?;
let gossip_every = settings.cluster.tools.to_pubsub_gossip_interval()?;
```

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

## Validation

Run focused validation for the area you touch first, then widen as needed:

```bash
cd kairo-next
cargo fmt --all -- --check
cargo test -p kairo-examples --all-targets --all-features
cargo clippy -p kairo-examples --all-targets --all-features -- -D warnings
```

The full workspace target is:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets --all-features
```
