# Goal

Build a Rust-first actor framework inspired by Apache Pekko/Akka. Kairo should
provide typed local actors, remoting, gossip-based clustering, distributed data,
cluster sharding, cluster tools, and test utilities without copying Pekko's
Scala API shape.

The rewrite is a replacement architecture, not an incremental cleanup of the
old code. The old implementation under `crates/` remains useful as reference
material, but the active implementation lives under `kairo-next/`.

By the end of the milestone plan below, Kairo should be close to a complete
refactor: old workspace code is no longer needed for normal builds, the new
public API is coherent, local and distributed actor workflows are runnable, and
the remaining work is release hardening rather than foundational redesign.

## Product Goals

Kairo should provide:

1. Typed actor references and actor protocols that feel natural in Rust.
2. Synchronous actor message turns, with async work returning through mailbox
   messages via `pipe_to_self`, `ask`, timers, or adapters.
3. Actor tree semantics: guardians, paths, lifecycle, supervision, death watch,
   dead letters, event stream, scheduler, receptionist, and coordinated
   shutdown.
4. Local-only messages that do not require serialization.
5. Remote messaging through stable manifests, versions, serializer IDs, and
   registered codecs.
6. Location transparency through local and remote `ActorRef<M>` values without
   making erased dynamic messages the primary user model.
7. Cluster membership implemented with gossip, vector clocks, reachability,
   heartbeat/failure detector observations, convergence, and leader actions.
8. Cluster sharding with `EntityRef<M>`, `ShardingEnvelope<M>`, stable shard
   allocation, passivation, handoff, rebalancing, and coordinator state.
9. Distributed data and cluster tools that build on cluster membership without
   becoming the membership authority.
10. TOML-based configuration first, with a format-neutral settings model that
    can later support HOCON after `hocon-rs` is ready.
11. Test utilities that make actor, remoting, cluster, and sharding behavior
    deterministic enough to verify.

## Non-Goals

The rewrite does not include:

- A direct port of Pekko's Scala DSL, class hierarchy, or implicit-heavy API.
- A single-crate MVP that must be split later.
- Etcd or any central store as the authoritative cluster membership model.
- Mandatory serialization for local-only actor messages.
- `AsyncActor` as the initial actor model.
- Remote actor deployment before local actors, remoting, and cluster semantics
  are stable.
- A global message enum or `DynMessage` as the primary user protocol.
- Sharded entity IDs embedded into every business message as the default API.
- Persistence, streams, typed event sourcing, or durable delivery in the first
  complete rewrite.

## Design Principles

```text
Rust-first API, Pekko-compatible semantics where the semantics matter.
Typed local protocols, stable dynamic boundaries at remote and system edges.
Actor state changes happen in short synchronous message turns.
Async work is external work; its result must re-enter through the mailbox.
Cluster membership is gossip, not a central registry.
Remote wire compatibility uses manifests and versions, not Rust internals.
Configuration starts with TOML, but settings structs stay format-neutral.
Sharding routes by envelope/entity ref; entities receive business messages.
Every milestone must be runnable, tested, and leave the workspace cleaner.
```

Engineering principles:

1. Every behavior feature must have focused tests.
2. Every milestone must leave the workspace compiling with all features.
3. Build the local actor runtime before remoting.
4. Build remoting before cluster runtime.
5. Build gossip membership before sharding depends on it.
6. Treat system protocols as public wire contracts once remoting exists.
7. Keep dependencies minimal and justified by implemented code.
8. Prefer deterministic tests over larger timeouts.

## Pekko Reference

The local Pekko checkout for implementation reference is:

```text
~/IdeaProjects/pekko
```

Use this source tree to preserve behavior, not API shape. Before implementing a
behavior-sensitive subsystem, inspect the corresponding Pekko files:

```text
actor-typed/.../ActorRef.scala
actor-typed/.../Behavior.scala
actor/.../ActorCell.scala
actor/.../dispatch/Mailbox.scala
actor/.../dungeon/DeathWatch.scala
remote/.../RemoteActorRefProvider.scala
remote/.../RemoteWatcher.scala
remote/.../MessageSerializer.scala
cluster/.../Gossip.scala
cluster/.../MembershipState.scala
cluster/.../Reachability.scala
cluster/.../VectorClock.scala
cluster/.../ClusterDaemon.scala
cluster/.../ClusterHeartbeat.scala
distributed-data/.../Replicator.scala
cluster-sharding/.../ShardRegion.scala
cluster-sharding/.../Shard.scala
cluster-sharding/.../ShardCoordinator.scala
cluster-sharding-typed/.../ClusterSharding.scala
```

The expected workflow is:

```text
read Pekko source -> extract observable semantics and state transitions ->
design Rust ownership/API shape -> implement focused Rust modules -> test
against the intended behavior
```

Do not hard-port Scala classes, inheritance, implicits, DSL builders, or
JVM-specific mechanisms. Kairo should use Rust traits, enums, structs, owned
state, explicit builders, typed refs, explicit errors, and crate boundaries.
When Rust intentionally differs from Pekko internals, record the decision in
`docs/decisions.md`.

## Long-Term Codex Goal

The following goal can be used as a persistent implementation target:

```text
/goal Treat docs/goal.md as the authoritative product roadmap, kairo-next/ARCHITECTURE.md as the technical contract, and docs/progress.md as the current implementation status when it exists. Use ~/IdeaProjects/pekko as the local Pekko semantic reference before implementing behavior-sensitive actor, remote, cluster, distributed-data, sharding, or cluster-tools logic; preserve observable semantics and state transitions, but express them with Rust ownership, modules, traits, enums, structs, explicit builders, typed refs, explicit errors, and crate boundaries instead of copying Scala inheritance, implicits, DSLs, or JVM-specific mechanisms. Continue implementing the Kairo rewrite until the old crates/ implementation is no longer needed for normal builds and kairo-next provides a coherent Rust-first actor framework inspired by Pekko: typed local actors with synchronous receive turns, actor tree lifecycle, supervision, death watch, timers, ask, adapters, event stream, receptionist, coordinated shutdown, stable serialization metadata and codec registration for remote messages, remote actor refs and associations, remote death watch, gossip-based cluster membership with vector clocks, reachability, failure detection, convergence, leader actions, and downing hooks, distributed data CRDT replication, cluster sharding with EntityRef and ShardingEnvelope routing, coordinator allocation, handoff, rebalancing, passivation, and remember-entity storage, cluster singleton and pubsub tools, TOML-based initial configuration with format-neutral settings, deterministic testkit support, examples, and user-facing documentation. Maintain these constraints throughout implementation: new code lives under kairo-next; keep the multi-crate workspace; do not use etcd or any central authoritative membership store; do not add AsyncActor in the initial design; local messages need no serialization; remote messages require stable RemoteMessage manifest/version metadata and registered codecs; do not rely on Rust enum discriminants, type names, or memory layout for wire compatibility; shard IDs use a documented stable hash; sharded business messages do not need embedded entity IDs by default; use TOML as the first configuration file format and do not add HOCON or hocon-rs until intentionally adopted later; and do not add broad third-party dependencies before code needs them. Every milestone must be runnable, tested, documented in docs/progress.md, and validated by the relevant subset of cargo fmt --all -- --check, cargo clippy --workspace --all-targets --all-features -- -D warnings, cargo test --workspace --all-targets --all-features, focused crate tests, examples, and multi-node tests once those exist. Commit appropriate verified checkpoints using Conventional Commit messages when the goal run is authorized to commit.
```

## Milestones

The milestone list is intentionally complete enough that finishing all M stages
puts the project near a finished rewrite, not merely an early prototype.

### M0: Workspace, Contracts, And Roadmap

Goal: the rewrite workspace is isolated from old code and has stable design
contracts for goal-mode implementation.

Scope:

```text
root workspace includes only kairo-next/crates/*
old crates/ remains reference-only
minimal external dependencies
kairo-next crate boundaries established
kairo-next/ARCHITECTURE.md documents module design
docs/goal.md and AGENTS.md guide Codex goal mode
local Pekko reference path is documented
serialization, actor, remote, cluster, ddata, sharding, tools, testkit skeletons
```

Acceptance:

```text
cargo check --workspace --all-targets --all-features passes
old crates/ are not workspace members
architecture explicitly rejects etcd membership
architecture explicitly uses synchronous Actor::receive
architecture defines remote message metadata and codec registration
architecture defines EntityRef/ShardingEnvelope entity-id routing
AGENTS.md and docs/goal.md require consulting ~/IdeaProjects/pekko
before implementing Pekko-sensitive behavior
```

### M1: Local Actor Runtime Core

Goal: local typed actors can be spawned, messaged, stopped, and observed through
the core mailbox loop.

Scope:

```text
ActorSystem internals and guardians
ActorRef<M>, AnyActorRef, Recipient<M>, DeadLetters, IgnoreRef
ActorPath and Address with uid/incarnation rules
Props and actor factory lifecycle
mailbox queues with system/user lanes
dispatcher scheduling and throughput
synchronous Actor::started, receive, stopped, signal
Context::myself, system, spawn, spawn_anonymous, stop
dead-letter delivery for stopped or missing actors
```

Acceptance:

```text
spawned actors receive messages in tell order from the same sender
receive is called one message at a time
stop prevents later user-message delivery
remaining stopped messages go to dead letters
child paths and unique incarnations are stable
kairo-actor tests cover spawn, tell, stop, dead letters, and ordering
```

### M2: Lifecycle, Supervision, Patterns, And Testkit

Goal: local actors have the lifecycle behavior needed by larger subsystems and
tests can exercise it deterministically.

Scope:

```text
watch, watch_with, unwatch, Terminated
parent/child stop ordering
failure reporting and supervision directives
restart, resume, stop, escalate
timers and scheduler
Context::ask, adapter, pipe_to_self, spawn_task
stash support if retained by architecture
event stream
local receptionist
coordinated shutdown basics
kairo-testkit probes and manual time
```

Acceptance:

```text
watchers receive termination exactly once
restart preserves actor ref incarnation semantics where intended
pipe_to_self does not borrow actor state across await
ask creates and cleans temp refs
timers can be advanced deterministically in tests
supervision tests cover stop, restart, and resume
```

### M3: Serialization And Message Metadata

Goal: remote-capable messages can be encoded through stable metadata while
local-only messages stay simple.

Scope:

```text
RemoteMessage trait with MANIFEST and VERSION
KairoRemoteMessage derive macro
MessageCodec<M> and DynCodec bridge
SerializationRegistry implementation
SerializedMessage and RemoteEnvelope data
actor-ref serialization through provider resolution
system manifests for remote, cluster, ddata, and sharding protocols
optional serde/json/cbor/prost codec crates or features when needed
rolling-version compatibility tests
```

Acceptance:

```text
local ActorRef<LocalMsg> works without RemoteMessage
remote resolution or remote send requires M: RemoteMessage
duplicate serializer IDs or manifests are rejected
wire payload includes serializer_id, manifest, version, and bytes
derive macro emits stable metadata only, not a codec choice
tests prove Rust type names and enum discriminants are not wire contracts
```

### M4: Remote Actor References And Transport

Goal: actor refs can communicate across processes using a framed transport and
remote death watch.

Scope:

```text
RemoteActorRefProvider composed with local provider
remote path resolution
RemoteActorRef<M>
transport abstraction and first TCP implementation
association state machine
outbound lanes for control and ordinary messages
inbound decode, deserialize, resolve target, deliver
remote watch/unwatch protocol
address termination and quarantine
backpressure and send failure mapping
```

Acceptance:

```text
two actor systems can exchange typed remote messages
remote sends serialize through the registry automatically
unknown remote target routes to dead letters or equivalent diagnostics
remote watch reports address termination
association close prevents silent message loss
focused remote integration tests run without cluster
```

### M5: Cluster Gossip Data Model

Goal: cluster membership state has a tested immutable model before runtime
actors depend on it.

Scope:

```text
UniqueAddress
Member and MemberStatus
VectorClock
Gossip
GossipOverview
Reachability
failure-detector observations as local inputs
gossip merge semantics
convergence calculation
leader selection
cluster event diffing
```

Acceptance:

```text
vector clock dominance and concurrency are covered by tests
gossip merge is associative and idempotent for membership facts
reachability merge preserves newest observations
convergence excludes unreachable or non-converged views as designed
leader selection is deterministic across nodes with same gossip
no etcd or central membership dependency exists
```

### M6: Cluster Runtime And Membership Protocol

Goal: multiple nodes can form and maintain a cluster through Pekko-style gossip
membership.

Scope:

```text
Cluster extension and daemon actors
seed/contact node join flow
InitJoin, InitJoinAck/Nack, Join, Welcome
GossipEnvelope and GossipStatus
periodic gossip target selection
heartbeat sender/receiver and phi/accrual or equivalent detector
leader actions for Joining->Up, Leaving->Exiting->Removed
downing provider hooks
cluster domain events and subscriptions
coordinated shutdown leave
```

Acceptance:

```text
three local test nodes can join and converge to Up
member leave converges to Removed
unreachable observations emit cluster events
leader actions happen only when convergence rules allow
seed discovery is contact-only, not authoritative membership
cluster tests cover join, gossip merge, leave, unreachable, and downing hooks
```

### M7: Distributed Data

Goal: CRDT replicated data is available for cluster tools and sharding stores
without becoming the cluster membership source.

Scope:

```text
Replicator actor
CRDT traits and delta/full-state replication
GCounter, PNCounter, ORSet, ORMap, LWWRegister where useful
read/write consistency levels
pruning for removed nodes
serialization manifests for CRDT operations
subscription and change notifications
test replicator over local and remote transports
```

Acceptance:

```text
two or more nodes converge replicated CRDT values
delta and full-state paths both preserve merge semantics
removed-node pruning is deterministic
ddata does not feed cluster membership decisions
cluster-sharding can use ddata as a coordinator/remember-entities store
```

### M8: Cluster Sharding Core

Goal: sharded entities can be addressed by entity ID and routed to local or
remote shards.

Scope:

```text
EntityTypeKey<M>
Entity<M> and EntityContext
EntityRef<M>
ShardingEnvelope<M>
optional EntityMessageExtractor<In, M>
stable shard-id strategy
ShardRegion
Shard
ShardCoordinator
allocation strategy
region buffers and retry timers
entity start/stop and passivation protocol
```

Acceptance:

```text
EntityRef<M> sends M without requiring M to contain entity_id
ShardingEnvelope<M> routes by explicit entity_id
default shard allocation uses a stable documented hash
local-only sharding test starts entities on demand
cluster-aware sharding test routes to remote shard homes
passivation stops idle entities and later restarts on demand
```

### M9: Sharding Rebalancing, Handoff, And Remember Entities

Goal: sharding behaves correctly during cluster changes and coordinator
recovery.

Scope:

```text
coordinator state store abstraction
memory store for tests
distributed-data store
remember entities
rebalance worker
BeginHandOff, HandOff, ShardStopped
buffering during handoff
coordinator failover through singleton or equivalent tool
allocation state recovery
graceful region shutdown
```

Acceptance:

```text
rebalance moves shards without losing accepted messages
handoff buffers or rejects according to documented rules
remembered entities restart after coordinator recovery
coordinator state survives selected failover scenarios
no etcd-backed coordinator or membership store is used
```

### M10: Cluster Tools

Goal: higher-level cluster utilities needed by users and sharding are available.

Scope:

```text
cluster singleton manager and proxy
singleton handover during leaving/downing
distributed pubsub mediator
topic registration and publish/send/send-to-all
cluster receptionist if retained
integration with distributed data where appropriate
configuration and typed public APIs
```

Acceptance:

```text
one singleton instance is active per role/scope
singleton handover works when the oldest node leaves
pubsub subscriptions converge across cluster nodes
published messages reach expected local and remote subscribers
sharding coordinator can use cluster tools without private shortcuts
```

### M11: Configuration, Extensions, Observability, And Shutdown

Goal: users can configure and operate actor systems without depending on test
hooks or hard-coded defaults.

Scope:

```text
configuration model and builders
extension registry
dispatcher and mailbox configuration
remote and cluster settings
TOML file loader for initial configuration
format-neutral typed settings structs
builder overrides merged with file settings
HOCON loader deferred until hocon-rs is intentionally adopted
roles and data-center metadata if retained
structured logging/tracing integration
metrics hooks where useful
coordinated shutdown phases
graceful remote/cluster/sharding shutdown
runtime diagnostics for dead letters, quarantine, serialization, gossip
```

Acceptance:

```text
common settings are configurable through builders and config files
TOML config files can configure actor, remote, cluster, and sharding settings
settings structs do not expose TOML-specific concepts
no HOCON dependency is introduced in the first config implementation
extensions can be loaded once and retrieved type-safely
coordinated shutdown leaves cluster before terminating remoting
diagnostics identify serialization and remote delivery failures
operator-visible cluster state can be queried
```

### M12: Integration Tests, Examples, And User Documentation

Goal: the framework is proven through realistic user workflows and documented
APIs.

Scope:

```text
local counter example
ask/pipe_to_self example
remote ping-pong example
cluster join/leave example
cluster sharding counter example
distributed data example
singleton and pubsub examples
multi-node test harness
API docs for actor, remote, cluster, ddata, sharding, tools
migration notes from old Kairo APIs
```

Acceptance:

```text
all examples compile and run
docs explain local vs remote serialization requirements
docs explain why Actor::receive is synchronous
docs explain gossip membership and why etcd is not used
docs explain entity ID routing through EntityRef/ShardingEnvelope
multi-node tests run in CI or documented local validation
```

### M13: Hardening, Performance, And Refactor Completion

Goal: the rewrite is close to complete: behavior is tested, public APIs are
coherent, and remaining work is release hardening rather than architecture
replacement.

Scope:

```text
public API review and feature gates
crate docs and examples
benchmark suite for actor tell, remote send, gossip merge, sharding route
mailbox and dispatcher tuning
remote batching/backpressure tuning
gossip target selection tuning
sharding allocation and passivation tuning
dependency/license audit
CI validation matrix
old README/status update
deprecation or removal plan for old crates/
```

Acceptance:

```text
cargo fmt, clippy, tests, examples, and benchmarks all pass where applicable
new kairo facade is the recommended user entrypoint
old crates/ are not needed for normal development or examples
public APIs have docs and compile-tested examples
cluster sharding demo survives node join, leave, rebalance, and entity restart
known remaining gaps are release issues, not foundational architecture gaps
```

## Remaining Task List

The original component-scaffolding order has largely been executed. Remaining
work is now gated by runnable vertical slices, not by the number of individual
state machines or focused tests present. Detailed current status and exit gates
live in `docs/progress.md`.

1. Replace the initial worker-thread baseline with the production dispatcher,
   task-executor, and scheduler ownership model while preserving synchronous
   typed actor semantics.
2. Compose remoting into one ActorSystem-owned lifecycle that supports
   heterogeneous registered business protocols and all system manifests over a
   shared canonical transport address, with bounded outbound lanes and reliable
   system-message delivery.
3. Complete the M6 cluster daemon: seed contact, init/join/welcome, periodic
   gossip status/full gossip, heartbeat, convergence-gated leader actions,
   leave/removal, and coordinated shutdown.
4. Integrate distributed data, sharding, and cluster tools with that real
   cluster/remoting lifecycle and expose cohesive ActorSystem extensions.
5. Pass the three-node final acceptance demo, including join, leave, rebalance,
   handoff, entity restart, and remember-store recovery without manually
   injected membership state.
6. Enter M13 release hardening only after items 1-5 no longer require
   foundational architecture replacement; then complete performance tuning,
   fault testing, API review, platform CI, documentation, and legacy removal
   planning.

## Roadmap Maintenance Files

`docs/progress.md`:

````md
# Progress

## Current Milestone

M0 - Workspace, contracts, and roadmap

## Completed

- Root workspace points at `kairo-next/crates/*`.
- Initial crate skeletons exist.
- Architecture document exists.

## Next

- [ ] Implement the next focused milestone task.

## Validation

```bash
cargo fmt --all -- --check
cargo check --workspace --all-targets --all-features
```
````

`docs/decisions.md`:

```md
# Architecture Decisions

## ADR-0001: Cluster membership uses gossip, not etcd

Status: Accepted

Context:
The old implementation used etcd as a temporary membership authority.

Decision:
Membership state is represented by gossip, vector clocks, reachability, and
failure-detector observations. Discovery provides contact points only.

Consequences:
- Cluster behavior follows Pekko semantics more closely.
- There is no central membership store.
- Split-brain and downing behavior must be explicit and tested.
```

`docs/blocked.md`:

```md
# Blocked Items

No blockers currently.
```

## Key Risks

### Actor API Drift

Risk: the Rust API becomes a direct Scala/Pekko copy or regresses into the old
dynamic message model.

Control:

```text
Keep ActorRef<M> as the primary boundary.
Keep Actor::receive synchronous.
Use adapters and ports for large protocols.
Keep erased refs and dynamic codecs at runtime boundaries.
```

### Cluster Membership Regression

Risk: a later implementation reintroduces etcd or another central store to make
cluster membership easier.

Control:

```text
Cluster membership must be implemented and tested through gossip.
Discovery may only provide contact points.
Coordinator state stores may use ddata or memory, but not membership authority.
```

### Wire Compatibility Instability

Risk: remote messages accidentally depend on Rust implementation details.

Control:

```text
Every remote/system message uses manifest, version, and serializer id.
Rolling-upgrade tests cover versioned manifests.
Do not use type names, enum discriminants, or memory layout as protocol data.
```

### Sharding Message Pollution

Risk: every entity message is forced to carry routing IDs.

Control:

```text
EntityRef<M> binds entity_id.
ShardingEnvelope<M> carries routing metadata at the region boundary.
Entity actors receive business M only.
Extractors are optional compatibility adapters.
```

### Over-Broad Dependencies

Risk: the rewrite imports dependencies before the architecture needs them.

Control:

```text
Keep workspace dependencies minimal.
Add codec, transport, metrics, or config dependencies only with implementing code.
Prefer optional crates or features for format-specific integrations.
```

## Final Acceptance Demo

Local actor:

```rust
use kairo::prelude::*;

enum CounterCmd {
    Increment,
    Get { reply_to: ActorRef<CounterValue> },
}

struct CounterValue(i64);
struct Counter { value: i64 }

impl Actor for Counter {
    type Msg = CounterCmd;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            CounterCmd::Increment => self.value += 1,
            CounterCmd::Get { reply_to } => reply_to.tell(CounterValue(self.value))?,
        }
        Ok(())
    }
}
```

Cluster sharding:

```rust
#[derive(Debug, Clone, KairoRemoteMessage)]
#[kairo(manifest = "example.counter.CounterCmd", version = 1)]
enum CounterCmd {
    Increment,
    Get { reply_to: ActorRef<CounterValue> },
}

let sharding = ClusterSharding::get(&system);
let counters = sharding.init(Entity::of(
    EntityTypeKey::<CounterCmd>::new("counter"),
    |ctx| Counter { value: 0 },
))?;

let counter = counters.entity_ref_for("counter-42");
counter.tell(CounterCmd::Increment)?;
```

Acceptance workflow:

1. Start three local test nodes.
2. Nodes join through seed contact points and converge to `Up` through gossip.
3. Register a sharded counter entity type.
4. Send messages through `EntityRef<CounterCmd>` without embedding entity IDs
   in `CounterCmd`.
5. Add a node and trigger rebalance.
6. Remove a node and complete handoff without losing accepted messages.
7. Stop and restart coordinator state using the selected store.
8. Validate remote serialization manifests, cluster events, and entity state
   through deterministic tests.
