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
