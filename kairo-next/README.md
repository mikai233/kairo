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

See `ARCHITECTURE.md` for the planned public model and implementation order.
For migration guidance from the old reference crates, see
`../docs/migration.md`.

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
cargo run -p kairo-examples --example cluster_sharding_local
cargo run -p kairo-examples --example cluster_tcp_peer_bootstrap
cargo run -p kairo-examples --example ddata_tcp_peer_bootstrap
cargo run -p kairo-examples --example cluster_tools_tcp_peer_bootstrap
```

The `local_counter` example demonstrates the first Rust-first actor workflow:
spawn a typed actor, send local messages without serialization, request a value
through an explicit reply channel, and stop the actor.

The `configured_counter` example loads
`kairo-next/crates/kairo-examples/examples/kairo.local.toml`, maps the
format-neutral actor settings into an `ActorSystemBuilder`, validates the
configured sharding timing and least-shard allocation helpers through the
facade, and runs the same typed counter protocol with the configured dispatcher
throughput.

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

The `cluster_sharding_local` example wires a local shard coordinator, typed
shard region, stable `ShardingEnvelopeRouter`, `EntityRef<String>`, and
entity-backed shard actor. Business messages reach a typed entity child without
embedding the entity id in the business message.

The TCP peer bootstrap examples demonstrate the current cluster,
distributed-data, and cluster-tools route setup around the shared remote
association primitives.
