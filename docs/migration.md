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
```

Load it through the facade and map the format-neutral settings into builders:

```rust
let settings = kairo::prelude::load_toml_file("kairo.local.toml")?;
let system = settings.actor.actor_system_builder("app")?.build()?;
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
