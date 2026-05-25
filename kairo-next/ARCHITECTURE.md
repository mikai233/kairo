# Kairo Next Architecture

This document is the implementation blueprint for the Kairo rewrite. It uses
Apache Pekko as the semantic reference, but the API and internal shape are
Rust-first.

The old implementation under `crates/` is reference material only. New code
must live under `kairo-next/`.

## Non Goals

- Do not port Pekko's Scala DSL or class hierarchy directly.
- Do not model cluster membership through etcd or any central configuration
  store.
- Do not make sharded `EntityRef<M>` watchable as if it were a normal
  `ActorRef<M>`.
- Do not require serialization for local-only actor messages.
- Do not implement remote actor deployment before local actors, remoting, and
  cluster membership are stable.

## Pekko Source Map

The local Pekko checkout used for this design is
`/Users/mikai/IdeaProjects/pekko`.

- Typed public surface: `actor-typed/.../ActorRef.scala`,
  `Behavior.scala`, `ActorSystem.scala`, `Receptionist.scala`.
- Classic runtime machinery still used under typed actors:
  `actor/.../ActorCell.scala`, `dispatch/Mailbox.scala`,
  `actor/dungeon/DeathWatch.scala`.
- Remoting: `remote/.../RemoteActorRefProvider.scala`,
  `RemoteWatcher.scala`, `MessageSerializer.scala`,
  `remote/artery/Association.scala`.
- Cluster gossip and membership: `cluster/.../Gossip.scala`,
  `MembershipState.scala`, `Reachability.scala`, `VectorClock.scala`,
  `ClusterDaemon.scala`, `ClusterHeartbeat.scala`.
- Sharding: `cluster-sharding/.../ShardRegion.scala`, `Shard.scala`,
  `ShardCoordinator.scala`, and typed API in
  `cluster-sharding-typed/.../ClusterSharding.scala`.

These files define the semantics to preserve. The Kairo implementation should
not copy their inheritance structure.

Reference discipline:

- Before implementing behavior-sensitive actor, remote, cluster, distributed
  data, sharding, or cluster-tools logic, inspect the matching Pekko source in
  `/Users/mikai/IdeaProjects/pekko`.
- Preserve externally observable semantics, state transitions, ordering rules,
  failure behavior, and wire-compatibility concepts.
- Do not translate Scala inheritance, implicit APIs, builders, or DSL shape into
  Rust. Re-express the logic with Rust modules, traits, enums, structs, owned
  data, typed refs, explicit builders, and explicit error types.
- If Pekko internals depend on JVM or Scala-specific mechanisms, document the
  Rust replacement in `docs/decisions.md` before implementing the divergent
  design.

## Workspace

```text
kairo-next/crates/
  kairo
  kairo-actor
  kairo-actor-macros
  kairo-serialization
  kairo-remote
  kairo-cluster
  kairo-distributed-data
  kairo-cluster-sharding
  kairo-cluster-tools
  kairo-testkit
```

Dependency direction:

```text
kairo
  -> kairo-actor
  -> kairo-actor-macros
  -> kairo-serialization
  -> kairo-remote
       -> kairo-actor
       -> kairo-serialization
  -> kairo-cluster
       -> kairo-actor
       -> kairo-remote
  -> kairo-distributed-data
       -> kairo-actor
       -> kairo-cluster
       -> kairo-remote
       -> kairo-serialization
  -> kairo-cluster-sharding
       -> kairo-actor
       -> kairo-cluster
       -> kairo-distributed-data
  -> kairo-cluster-tools
       -> kairo-actor
       -> kairo-cluster
       -> kairo-distributed-data
  -> kairo-testkit
       -> kairo-actor
```

Rules:

- `kairo-actor` knows nothing about remoting or cluster membership.
- `kairo-serialization` knows nothing about actors, transports, or cluster.
- `kairo-remote` knows how to resolve remote actor refs, but not cluster
  membership decisions.
- `kairo-cluster` owns membership, reachability, gossip, leader actions, and
  cluster events.
- `kairo-distributed-data` owns CRDT replication; sharding may use it as a
  store, but cluster membership must not depend on it.
- `kairo-distributed-data` may use `kairo-remote` outbound association
  boundaries to carry already-addressed replicator envelopes; it must not use
  remoting as a source of cluster membership truth.
- `kairo-cluster-sharding` consumes cluster events and actor refs; it must not
  mutate cluster membership directly.

## Configuration

The first supported configuration file format is TOML.

Rationale:

- TOML is simple enough for the first rewrite implementation and Rust tooling.
- HOCON expresses layered application configuration better, but it should wait
  until `hocon-rs` is available in the shape needed by this project.
- The runtime configuration model should not depend on TOML-specific syntax.
  File parsing maps into typed settings structs, and users may still configure
  systems through builders.

Rules:

- Do not add HOCON support in the initial implementation.
- Do not add a `hocon-rs` dependency until the project intentionally switches
  or adds a HOCON loader.
- Do not add the `toml` crate until configuration parsing code actually needs
  it.
- Keep settings structs format-neutral so a future HOCON loader can reuse the
  same model.

Suggested initial file names:

```text
kairo.toml
kairo.local.toml
```

Suggested top-level sections:

```toml
[actor]
[actor.dispatchers.default]
[actor.mailboxes.default]

[remote]
[remote.transport]

[cluster]
[cluster.seed]
[cluster.downing]

[cluster.sharding]
[cluster.tools.singleton]
[cluster.tools.pubsub]
```

## Public API Shape

Users define protocols as Rust enums or structs and actors as stateful Rust
types.

```rust
use kairo::prelude::*;

#[derive(Debug)]
enum CounterCmd {
    Increment,
    Get { reply_to: ActorRef<CounterValue> },
    Stop,
}

#[derive(Debug)]
struct CounterValue(i64);

struct Counter {
    value: i64,
}

impl Actor for Counter {
    type Msg = CounterCmd;

    fn receive(
        &mut self,
        ctx: &mut Context<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorResult {
        match msg {
            CounterCmd::Increment => self.value += 1,
            CounterCmd::Get { reply_to } => reply_to.tell(CounterValue(self.value))?,
            CounterCmd::Stop => ctx.stop(ctx.myself())?,
        }
        Ok(())
    }
}
```

Decisions:

- `ActorRef<M>` is the typed boundary. It accepts only `M`.
- The normal request-response model is explicit `reply_to`, not implicit
  sender.
- The runtime may keep an internal sender in envelopes for forwarding,
  dead-letters, system protocols, and compatibility helpers, but it is not the
  primary user model.
- Local messages require `Send + 'static`.
- Remote messages require stable serialization metadata.
- Dynamic downcasting is kept at runtime boundaries, not in user message
  handlers.
- `Actor::receive` is synchronous by design. Actor state changes should happen
  in a short message-processing turn.
- Async work is started from the context and its result is sent back to the
  actor as another message through `pipe_to_self`, `ask`, timers, or adapters.
- Do not provide `AsyncActor` in the initial design. It weakens the default
  model by encouraging `&mut self` to live across `.await`, which blocks the
  actor turn and complicates cancellation and supervision semantics.

### Message Protocols

`Actor::Msg` is the actor's protocol boundary. It may be an enum, a struct, or
another user-defined type. The framework should not require a global message
enum.

Small actors usually use a single enum:

```rust
enum CounterCmd {
    Increment,
    Get { reply_to: ActorRef<CounterValue> },
}
```

Large protocols should be split by capability rather than grown into one
unbounded enum:

```rust
enum UserMsg {
    Account(AccountCmd),
    Session(SessionCmd),
    Profile(ProfileCmd),
}
```

An actor may expose multiple typed ports by using adapters:

```rust
struct UserPorts {
    account: ActorRef<AccountCmd>,
    session: ActorRef<SessionCmd>,
    profile: ActorRef<ProfileCmd>,
}
```

Each port maps its input into the actor's private `UserMsg` mailbox. This keeps
the actor state machine single-threaded while avoiding a large public enum.

Remote serialization is attached to the messages that cross a remote boundary,
not to every local actor message.

## `kairo-actor`

`kairo-actor` is the local actor runtime.

Suggested source layout:

```text
src/
  lib.rs
  actor.rs
  actor_ref.rs
  any_ref.rs
  path.rs
  address.rs
  props.rs
  system.rs
  provider.rs
  guardian.rs
  cell.rs
  context.rs
  envelope.rs
  mailbox.rs
  dispatcher.rs
  system_message.rs
  lifecycle.rs
  supervision.rs
  death_watch.rs
  scheduler.rs
  timers.rs
  event_stream.rs
  receptionist.rs
  routing/
  patterns/
  coordinated_shutdown.rs
```

### Core Types

`Actor`:

```rust
pub trait Actor: Send + 'static {
    type Msg: Send + 'static;

    fn started(&mut self, ctx: &mut Context<Self::Msg>) -> ActorResult { Ok(()) }
    fn stopped(&mut self, ctx: &mut Context<Self::Msg>) -> ActorResult { Ok(()) }
    fn signal(&mut self, ctx: &mut Context<Self::Msg>, signal: Signal) -> ActorResult { Ok(()) }
    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult;
}
```

The actor trait intentionally does not accept async handlers. Futures may run
outside the actor turn, but their result must re-enter through the mailbox:

```rust
match msg {
    CounterCmd::LoadFromStore { key } => {
        let store = self.store.clone();
        ctx.pipe_to_self(
            async move { store.load(key).await },
            |result| CounterCmd::Loaded(result),
        );
    }
    CounterCmd::Loaded(result) => {
        self.value = result?;
    }
    _ => {}
}
```

The core rule is that actor-owned state is not mutated across `.await`.

Typed refs:

- `ActorRef<M>`: public typed reference; stores path plus an `Arc<dyn
  Recipient<M>>`.
- `Recipient<M>`: `tell(M)`, `tell_with_sender(M, AnyActorRef)`, `narrow`.
- `AnyActorRef`: erased reference for child maps, death watch, event stream,
  remote resolution, and diagnostics.
- `SystemRef`: system-message-only handle used by death watch and lifecycle
  protocols.
- `DeadLetters`: special recipient for undeliverable user messages.
- `IgnoreRef`: no-op recipient for internal fire-and-forget responses.
- `TempRef<M>`: short-lived ref for `ask`.

Paths and identity:

- `Address { protocol, system, host, port }`.
- `ActorPath { address, elements, uid }`.
- Ref equality uses logical path plus incarnation `uid`.
- Path equality may ignore `uid` when used as a lookup key.
- Top-level scopes: `/`, `/user`, `/system`, `/temp`, `/deadLetters`, and
  `/remote`.

### ActorSystem and Provider

`ActorSystem` owns:

- system name and local address,
- root, user, system, temp, and dead-letter guardians,
- extension registry,
- scheduler,
- dispatchers,
- event stream,
- receptionist,
- coordinated shutdown,
- local provider.

`ActorRefProvider` should be a trait:

```rust
pub trait ActorRefProvider: Send + Sync {
    fn resolve(&self, path: &ActorPath) -> ResolveResult;
    fn root_guardian(&self) -> AnyActorRef;
    fn user_guardian(&self) -> AnyActorRef;
    fn system_guardian(&self) -> AnyActorRef;
    fn dead_letters(&self) -> AnyActorRef;
}
```

`kairo-remote` can wrap or extend this provider later. The actor crate should
only define the provider interface and local provider.

### ActorCell

`ActorCell<A>` is the private runtime owner for one actor incarnation.

State:

- `actor: A`,
- `context: Context<A::Msg>`,
- `mailbox: Mailbox<A::Msg>`,
- `path`,
- `uid`,
- `parent`,
- `children: HashMap<String, AnyActorRef>`,
- `watching: HashMap<AnyActorRef, TerminationMessage>`,
- `watched_by: HashSet<AnyActorRef>`,
- `state: CellState`,
- `supervision: SupervisionStrategy`,
- `restart_count`,
- cancellable child tasks/timers.

Cell states:

```text
Init
Starting
Running
Suspended
Restarting
Stopping
Terminated
```

Processing rules:

1. System/control messages are processed before user messages.
2. User messages are processed up to dispatcher throughput.
3. `Actor::receive` is called synchronously for one envelope at a time.
4. Futures launched by an actor do not hold `&mut actor`; they report back by
   sending a message.
5. A stopped mailbox drains remaining user messages to dead letters.
6. Child termination is observed before parent termination completes.
7. Stopping parent stops children first, then notifies watchers.
8. Restart keeps the same `ActorRef` incarnation and path uid, matching Pekko
   semantics.

### Mailbox

`Mailbox<M>`:

- user queue: `MessageQueue<Envelope<M>>`,
- system queue: `SystemMessageQueue`,
- status bits: open, closed, scheduled, suspended,
- throughput and optional throughput deadline,
- bounded overflow policy.

Queue implementations:

- `UnboundedMailbox`: default MPSC queue.
- `BoundedMailbox`: non-blocking bounded queue; overflow goes to dead letters.
- `ControlAwareMailbox`: separate control lane before user lane.
- `PriorityMailbox`: later, after core semantics are stable.

System messages:

```text
Start
Stop
Suspend
Resume
Restart
Watch
Unwatch
DeathWatchNotification
ChildFailed
TerminateChild
Timer
```

System messages must not require user message serialization.

### Context

`Context<M>` exposes:

- `myself() -> &ActorRef<M>`,
- `system()`,
- `spawn`, `spawn_anonymous`, `stop`,
- `watch`, `watch_with`, `unwatch`,
- `children`, `child`, `parent`,
- `adapter`,
- `spawn_task`,
- `schedule_once`, `schedule_fixed_delay`,
- `ask`, `pipe_to_self`,
- `stash`, `unstash`, `unstash_all`,
- `event_stream`, `receptionist`.

`adapter<T>` returns an `ActorRef<T>` that maps `T` into `M` and sends it to
self. The adapter is a local child-like function ref with lifecycle cleanup.

`spawn_task` starts a detached future owned by the actor context. It is
cancelled when the actor stops or restarts, unless explicitly detached from the
actor lifecycle.

`pipe_to_self` starts a future and maps its completion into `M`. The future is
not allowed to borrow actor state; it receives owned data cloned or moved out
before the call.

### Supervision

Default: stop on unexpected failure, matching Pekko Typed.

Directives:

```text
Stop
Restart
Resume
Escalate
```

Strategy configuration:

- failure class/category match,
- max restarts within time window,
- backoff min/max/random factor,
- stop children on restart by default,
- optional keep-children mode only if it is defensible in Rust.

Failure sources:

- `ActorResult::Err`,
- panic caught at the runtime boundary where possible,
- startup failure,
- child failure notification.

### Death Watch

Maintain two maps per cell:

- `watching`: subjects this actor watches.
- `watched_by`: watchers of this actor.

Rules:

- Watching self is invalid.
- `watch` is idempotent only for the same termination message.
- `watch_with` changes require `unwatch` first.
- On termination, notify remote watchers before local watchers to preserve
  remoting shutdown ordering.
- Remote/non-local refs subscribe the actor to address termination events.
- `AddressTerminated` produces `DeathWatchNotification` with
  `address_terminated = true`.

### Receptionist

Types:

- `ServiceKey<M> { id, manifest/type_id }`,
- `ReceptionistCommand`,
- `Listing<M>`,
- `Registered<M>`.

Local implementation:

- `Register(key, ActorRef<M>, ack?)`,
- `Deregister(key, ActorRef<M>, ack?)`,
- `Find(key, reply_to)`,
- `Subscribe(key, subscriber)`.

Cluster receptionist can be added later through `kairo-cluster-tools`; the
actor crate only needs local receptionist.

## `kairo-serialization`

Owns stable wire payloads.

The user path should be derive metadata plus register a codec once:

```rust
use kairo::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, KairoRemoteMessage)]
#[kairo(manifest = "example.counter.CounterCmd", version = 1)]
enum CounterCmd {
    Increment,
    Get { reply_to: ActorRef<CounterValue> },
}

#[derive(Debug, Clone, Serialize, Deserialize, KairoRemoteMessage)]
#[kairo(manifest = "example.counter.CounterValue", version = 1)]
struct CounterValue(i64);

let system = ActorSystem::builder("app")
    .serialization(|registry| {
        registry.register_serde_cbor::<CounterCmd>()?;
        registry.register_serde_cbor::<CounterValue>()?;
        Ok(())
    })
    .build()?;
```

After registration, remote sends use normal typed refs:

```rust
let counter: ActorRef<CounterCmd> = system
    .remote()
    .resolve("kairo://app@127.0.0.1:2552/user/counter")?;

counter.tell(CounterCmd::Increment)?;
```

Local-only messages do not need serialization. Remote actor refs and remote
delivery paths require `M: RemoteMessage`.

Wire envelope:

```text
target_path: actor path
sender_path: optional actor path
serializer_id: u32
manifest: string
version: u16
payload: bytes
```

Modules:

```text
src/
  lib.rs
  manifest.rs
  message.rs
  codec.rs
  registry.rs
  envelope.rs
  actor_ref.rs
  errors.rs
```

Traits:

```rust
pub trait RemoteMessage: Send + 'static {
    const MANIFEST: &'static str;
    const VERSION: u16;
}

pub trait MessageCodec<M>: Send + Sync + 'static
where
    M: RemoteMessage,
{
    fn serializer_id(&self) -> SerializerId;
    fn encode(&self, message: &M) -> Result<Bytes>;
    fn decode(&self, payload: Bytes, version: u16) -> Result<M>;
}

pub trait DynCodec: Send + Sync + 'static {
    fn serializer_id(&self) -> SerializerId;
    fn manifest(&self) -> &'static str;
    fn message_type_id(&self) -> TypeId;
    fn encode_dyn(&self, value: &dyn Any) -> Result<Bytes>;
    fn decode_dyn(&self, payload: Bytes, version: u16) -> Result<Box<dyn Any + Send>>;
}

pub trait SerializationRegistry {
    fn register<M, C>(&mut self, codec: C) -> Result<()>
    where
        M: RemoteMessage,
        C: MessageCodec<M>;

    fn codec_for_type<M: RemoteMessage>(&self) -> Result<&dyn DynCodec>;
    fn codec_for_wire(&self, serializer_id: SerializerId, manifest: &Manifest)
        -> Result<&dyn DynCodec>;
}
```

Registry:

- maps `(serializer id, manifest)` to a codec,
- maps Rust `TypeId` to a codec for outbound messages,
- rejects duplicate serializer ids,
- rejects duplicate manifests for the same serializer id unless an explicit
  migration rule exists,
- supports system serializers and user serializers,
- exposes explicit registration APIs without requiring macros,
- exposes helper registration APIs for optional codec crates.

The derive macro only supplies stable metadata. It must not choose the wire
format:

```rust
#[derive(KairoRemoteMessage)]
#[kairo(manifest = "example.counter.CounterCmd", version = 1)]
enum CounterCmd {
    Increment,
}
```

Codec crates can add ergonomic helpers:

```rust
registry.register_serde_json::<CounterCmd>()?;
registry.register_serde_cbor::<CounterCmd>()?;
registry.register_prost::<CounterEvent>()?;
```

These helpers belong in optional crates or optional features, not in
`kairo-actor` or `kairo-remote`:

```text
kairo-serialization-serde
kairo-serialization-json
kairo-serialization-cbor
kairo-serialization-prost
```

The core serialization crate should not depend on serde, bincode, prost, or a
specific binary format. It owns manifests, registry rules, typed/dynamic codec
bridging, actor-ref serialization hooks, and system wire envelopes.

System manifests must be stable and documented because remote, cluster, and
sharding protocols depend on rolling upgrades. Do not rely on Rust enum
discriminants, Rust type names, memory layout, or compiler-generated details.

Actor refs serialize as path strings plus protocol/address data. Deserialization
uses the current `ActorSystem` provider to resolve local or remote refs.

## `kairo-remote`

Owns location transparency and remote transport.

Suggested source layout:

```text
src/
  lib.rs
  provider.rs
  remote_ref.rs
  resolver.rs
  transport.rs
  association.rs
  outbound.rs
  inbound.rs
  protocol.rs
  watch.rs
  quarantine.rs
  settings.rs
  system_delivery.rs
```

### Provider and Refs

`RemoteActorRefProvider` composes local provider behavior and remote resolution.

Resolution:

- local address -> local ref,
- unknown local path -> empty/dead-letter ref that keeps the path,
- remote address -> `RemoteActorRef<M>`.

`RemoteActorRef<M>`:

- keeps target path and remote address,
- serializes messages through `kairo-serialization`,
- enqueues outbound payloads into an association,
- intercepts `Watch`/`Unwatch` system messages into remote watch protocol.

### Transport

Transport abstraction:

```rust
pub trait Transport: Send + Sync {
    async fn bind(&self, local: Address) -> Result<BoundTransport>;
}

pub trait Association: Send + Sync {
    fn remote_address(&self) -> &Address;
    fn send(&self, envelope: OutboundEnvelope) -> SendResult;
    async fn close(&self, reason: CloseReason);
}
```

Initial transport should be TCP. QUIC can be added later if it provides a clear
benefit. Transport is not part of the actor API.

Outbound lanes:

- control/system lane,
- ordinary lane,
- optional large-message lane later.

Inbound pipeline:

1. decode frame,
2. verify association and remote uid,
3. deserialize payload,
4. resolve target ref,
5. enqueue to target or dead letters.

### System Message Delivery

Death watch relies on system messages. Ordinary user messages are at-most-once,
but system watch/unwatch/termination notifications need stronger practical
delivery:

- sequence system messages per association,
- ack received system messages,
- retry until ack or quarantine,
- preserve ordering within the system-message lane.

### Remote Watch

`RemoteWatcher` maintains:

- `watching: watchee -> watchers`,
- `watchee_by_address: address -> watchees`,
- `failure_detector_by_address`,
- `address_uid_by_address`,
- `unreachable`.

Protocol:

- `WatchRemote`,
- `UnwatchRemote`,
- `RemoteHeartbeat`,
- `RemoteHeartbeatAck { uid }`,
- `AddressTerminated`.

If heartbeat failure detector marks an address unavailable:

1. quarantine or mark association failed,
2. publish `AddressTerminated`,
3. local actors watching refs on that address receive termination messages.

## `kairo-cluster`

`kairo-cluster` owns cluster membership. The state model is pure gossip plus
local failure-detector observations. There is no etcd-backed membership,
central registry, or authoritative store.

Discovery may provide contact addresses for joining, but discovery must never
store or decide membership state.

Suggested source layout:

```text
src/
  lib.rs
  api.rs
  settings.rs
  member.rs
  unique_address.rs
  vector_clock.rs
  reachability.rs
  gossip.rs
  membership_state.rs
  protocol.rs
  daemon.rs
  seed.rs
  heartbeat.rs
  failure_detector.rs
  downing.rs
  events.rs
  subscriptions.rs
  serialization.rs
```

### Public API

`Cluster` extension:

- `self_member()`,
- `self_unique_address()`,
- `state()`,
- `manager() -> ActorRef<ClusterCommand>`,
- `subscriptions() -> ActorRef<ClusterSubscriptionCommand>`,
- `join(address)`,
- `leave(address)`,
- `down(address)`,
- `subscribe(subscriber, event_class)`,
- `unsubscribe(subscriber)`.

Events:

```text
CurrentClusterState
MemberJoined
MemberWeaklyUp
MemberUp
MemberLeft
MemberExited
MemberDowned
MemberRemoved
UnreachableMember
ReachableMember
LeaderChanged
RoleLeaderChanged
```

### Membership Data

`UniqueAddress`:

```rust
pub struct UniqueAddress {
    pub address: Address,
    pub uid: u64,
}
```

`Member`:

```rust
pub struct Member {
    pub unique_address: UniqueAddress,
    pub status: MemberStatus,
    pub roles: BTreeSet<String>,
    pub up_number: u32,
    pub app_version: Version,
    pub data_center: DataCenter,
}
```

Statuses:

```text
Joining
WeaklyUp
Up
Leaving
Exiting
Down
Removed
```

`PreparingForShutdown` and `ReadyForShutdown` can be added with coordinated
full-cluster shutdown, but they are not needed for the first gossip
implementation.

### Gossip

`Gossip`:

```rust
pub struct Gossip {
    members: BTreeSet<Member>,
    overview: GossipOverview,
    version: VectorClock,
    tombstones: BTreeMap<UniqueAddress, Timestamp>,
}
```

`GossipOverview`:

```rust
pub struct GossipOverview {
    seen: BTreeSet<UniqueAddress>,
    reachability: Reachability,
}
```

Operations:

- `seen(self_unique_address)`,
- `only_seen(self_unique_address)`,
- `clear_seen()`,
- `merge(other)`,
- `mark_down(member)`,
- `remove(unique_address, timestamp)`,
- `prune_tombstones(before)`,
- `seen_digest()`.

Merge rules:

1. Merge tombstones first.
2. Merge vector clocks and prune tombstoned nodes.
3. Merge members by highest status priority.
4. Merge reachability by observer record version.
5. Clear `seen`, because merged gossip is a new view.

Vector-clock comparison:

```text
Same
Before
After
Concurrent
```

Concurrent gossip must be merged, not overwritten.

### Reachability

`Reachability` is immutable:

```rust
pub struct Reachability {
    records: Vec<Record>,
    versions: BTreeMap<UniqueAddress, u64>,
}

pub struct Record {
    observer: UniqueAddress,
    subject: UniqueAddress,
    status: ReachabilityStatus,
    version: u64,
}
```

Statuses:

```text
Reachable
Unreachable
Terminated
```

Only an observer may update its own row. Merging chooses the row with the
highest observer version. If an observer has no negative records, all subjects
are considered reachable from that observer.

Aggregated subject status:

1. `Terminated` if any observer says terminated.
2. `Unreachable` if any observer says unreachable.
3. `Reachable` otherwise.

### Failure Detector and Heartbeat

Implement Phi Accrual failure detector:

- threshold,
- max sample size,
- min standard deviation,
- acceptable heartbeat pause,
- first heartbeat estimate.

Cluster heartbeat actors:

- `HeartbeatReceiver`: replies to heartbeat.
- `HeartbeatSender`: selects monitored peers, sends heartbeat, records replies.
- `FailureDetectorReaper`: periodically converts detector verdicts into
  reachability records.

Heartbeat must feed reachability only. It does not directly remove members.
Removal is a leader action after gossip convergence.

### Protocol

Cluster protocol messages:

```text
InitJoin { joining_config_digest }
InitJoinAck { address, config_check }
InitJoinNack { address }
Join { node, roles, app_version }
Welcome { from, gossip }
GossipEnvelope { from, to, gossip }
GossipStatus { from, version, seen_digest }
Leave { address }
Down { address }
ExitingConfirmed { node }
```

`to` in `GossipEnvelope` must include `UniqueAddress`, not only host/port, so a
new actor-system incarnation can ignore gossip intended for an old uid.

### Cluster Core State Machine

Node states:

```text
Uninitialized
TryingToJoin { target, deadline }
Initialized
Leaving
Terminated
```

Startup:

1. Create cluster daemon under `/system/cluster/core/daemon`.
2. If seed nodes are configured, contact them with `InitJoin`.
3. First seed replying with `InitJoinAck` receives `Join`.
4. Existing member handles `Join` by adding a `Joining` member, bumping vector
   clock, marking self as seen, and replying `Welcome`.
5. Joining node adopts `Welcome.gossip`, marks itself seen, starts heartbeat and
   periodic gossip.

Gossip tick:

1. Select a peer from reachable members.
2. Send `GossipStatus` if the peer probably has the same seen digest or is in
   another data center.
3. Send full `GossipEnvelope` when the peer needs the full state.
4. Schedule speedup ticks if less than half the members have seen the state or
   any member is down.

Receiving `GossipStatus`:

1. Ignore unknown or unreachable sender.
2. Compare seen digest.
3. If digest differs, send full gossip.
4. Else compare vector clock.
5. If remote is newer, send status back; if local is newer or concurrent, send
   full gossip.

Receiving `GossipEnvelope`:

1. Validate `to == self_unique_address`.
2. Ignore unknown or unreachable sender.
3. Ignore gossip that does not contain self.
4. Compare vector clocks.
5. Same: merge seen tables.
6. Before: keep local, talk back.
7. After: accept remote, mark self seen.
8. Concurrent: prune removed down/exiting nodes, merge gossips, mark self seen.
9. Publish membership/reachability changes.
10. If self becomes `Down`, start coordinated shutdown after gossip spreads.

Leader selection:

- Leader is the oldest reachable member in the local data center whose status
  is not `Down`.
- Role leader is the oldest reachable member with that role.
- Multiple leaders may temporarily exist during partitions; downing strategy
  decides how to resolve that.

Convergence:

- Required for `Joining -> Up`.
- Required for `Leaving -> Exiting`.
- Required for removing `Down` or `Exiting` members.
- Unreachable members with status `Down` or `Exiting` can be ignored for
  convergence.

Leader actions after convergence:

1. Move `Joining` or `WeaklyUp` to `Up` when minimum-member constraints pass.
2. Move `Leaving` to `Exiting`.
3. Remove unreachable `Down` or `Exiting` members.
4. Remove confirmed exiting members.
5. Assign monotonically increasing `up_number`.
6. Bump vector clock, clear seen, mark self seen, publish events.

Downing:

- `ManualDowning`: only explicit `Down(address)`.
- `AutoDownUnreachableAfter`: simple timeout strategy for development.
- `SplitBrainResolver`: later, role-aware and data-center-aware.

### Seed and Discovery

Seed providers are contact-point providers only:

```rust
pub trait SeedProvider {
    async fn seed_nodes(&self) -> Result<Vec<Address>>;
}
```

Allowed implementations:

- static config,
- DNS/service discovery later.

Disallowed:

- writing membership to an external store,
- reading membership from an external store,
- treating any discovery backend as authoritative cluster state.

## `kairo-distributed-data`

This crate is for CRDT replication on top of cluster membership. It is not used
to decide membership.

Suggested source layout:

```text
src/
  lib.rs
  replicator.rs
  key.rs
  crdt/
    gcounter.rs
    pncounter.rs
    orset.rs
    ormap.rs
    lww_register.rs
  delta.rs
  pruning.rs
  remote_association.rs
  remote_association_inbound.rs
  durable.rs
  protocol.rs
```

Replicator API:

- `Get`,
- `Update`,
- `Subscribe`,
- `Unsubscribe`,
- `Changed`,
- `Delete`,
- consistency levels: local, read/write majority, all.

CRDT requirements:

- monotonic merge,
- delta support where practical,
- pruning removed cluster nodes,
- serializer manifests for all internal messages.

Sharding uses this later for coordinator state and remember entities. MVP
sharding may start with an in-memory coordinator store to prove the routing
model before CRDT storage is implemented.

## `kairo-cluster-sharding`

Owns entity distribution. It consumes cluster membership and optionally
distributed data; it does not own cluster state.

Suggested source layout:

```text
src/
  lib.rs
  api.rs
  entity.rs
  entity_ref.rs
  envelope.rs
  extractor.rs
  region.rs
  shard.rs
  coordinator.rs
  allocation.rs
  handoff.rs
  passivation.rs
  remember_entities.rs
  state_store.rs
  query.rs
  serialization.rs
```

### Public API

```rust
let key = EntityTypeKey::<CounterCmd>::new("Counter");
let sharding = ClusterSharding::get(&system);

sharding.init(
    Entity::new(key.clone(), |entity| {
        Props::new(move || Counter::new(entity.id().to_owned()))
    })
    .with_role("backend")
    .with_stop_message(CounterCmd::Stop),
)?;

let counter = sharding.entity_ref_for(key, "counter-1");
counter.tell(CounterCmd::Increment)?;
```

Types:

- `EntityTypeKey<M>`,
- `Entity<M, E>`,
- `EntityContext<M> { type_key, entity_id, shard_ref }`,
- `EntityRef<M>`,
- `ShardingEnvelope<M> { entity_id, message }`,
- `ShardingMessageExtractor<E, M>`.

`EntityRef<M>` is actor-ref-like but not watchable. It represents a logical
entity that may passivate, move, or restart behind the scenes.

### Region

`ShardRegion<E, M>` exists on every node that hosts or proxies an entity type.

State:

- `type_name`,
- `entity_factory`,
- `extractor`,
- `coordinator_path`,
- `coordinator_ref`,
- `regions: HashMap<RegionRef, HashSet<ShardId>>`,
- `region_by_shard: HashMap<ShardId, RegionRef>`,
- `local_shards: HashMap<ShardId, ActorRef<ShardCommand>>`,
- `shard_buffers: MessageBufferMap<ShardId, Envelope<E>>`,
- `registered`,
- `graceful_shutdown`,
- `handoff_in_progress`.

Region flow:

1. Register with coordinator until `RegisterAck`.
2. For each user message, extract entity id and shard id.
3. If shard home is known and local, send to local `Shard`.
4. If shard home is known and remote, forward to remote region.
5. If shard home is unknown or rebalancing, buffer and ask coordinator
   `GetShardHome`.
6. On `ShardHome`, update cache and flush buffered messages.
7. On `HostShard`, spawn local `Shard`, then reply `ShardStarted`.
8. On `BeginHandOff`, remove shard home cache and ack so all regions buffer.
9. On `HandOff`, ask local shard to stop entities, then reply `ShardStopped`.
10. On graceful shutdown, hand off all local shards before stopping region.

Ordering:

- Messages sent through the same region to the same entity keep FIFO ordering
  as long as buffers do not overflow.
- During handoff, messages buffered between `BeginHandOff` and `HandOff` may be
  dropped if delivering them would violate ordering against messages already
  forwarded from another region.

### Shard

`Shard` manages all entities in one shard id.

State:

- `shard_id`,
- `entity_factory`,
- `entities: HashMap<EntityId, EntityState>`,
- `by_ref: HashMap<AnyActorRef, EntityId>`,
- `message_buffers: MessageBufferMap<EntityId, Envelope<M>>`,
- `passivation_strategy`,
- `remember_entities_provider`,
- `handoff_stop_message`,
- `lease` optional later.

Entity states:

```text
NoState
RememberedButNotCreated
RememberingStart
Active(ref)
Passivating(ref)
WaitingForRestart
RememberingStop
```

MVP can implement:

```text
NoState
Active(ref)
Passivating(ref)
```

Shard flow:

1. On first message for entity, spawn entity child.
2. Forward payload to entity.
3. On `Passivate`, mark entity passivating and send stop message to entity.
4. Buffer new messages for passivating entity.
5. On entity termination after passivation, remove entity.
6. If buffered messages exist, restart entity and flush buffer.
7. On handoff, stop all active entities and reply when all terminate.

### Coordinator

Coordinator is a singleton per entity type and role/data-center scope.

Messages:

```text
Register(region)
RegisterProxy(region)
RegisterAck(coordinator)
GetShardHome(shard_id)
ShardHome(shard_id, region)
ShardHomes(homes)
HostShard(shard_id)
ShardStarted(shard_id)
BeginHandOff(shard_id)
BeginHandOffAck(shard_id)
HandOff(shard_id)
ShardStopped(shard_id)
GracefulShutdownReq(region)
RegionStopped(region)
```

State:

```rust
pub struct CoordinatorState {
    shards: HashMap<ShardId, RegionRef>,
    regions: HashMap<RegionRef, Vec<ShardId>>,
    proxies: HashSet<RegionRef>,
    unallocated_shards: HashSet<ShardId>,
}
```

Runtime state:

- `alive_regions`,
- `rebalance_in_progress: HashMap<ShardId, PendingRequestSet>`,
- `rebalance_workers`,
- `graceful_shutdown_in_progress`,
- `region_termination_in_progress`,
- `all_regions_registered`.

Coordinator flow:

1. Region registers and is watched.
2. Coordinator sends `RegisterAck` and known shard homes.
3. `GetShardHome` returns existing home unless shard is rebalancing.
4. Unknown shard is allocated by allocation strategy to an active region.
5. Coordinator persists/replicates `ShardHomeAllocated`.
6. Coordinator sends `HostShard` to chosen region.
7. Periodic rebalance asks strategy for shard ids.
8. Rebalance worker sends `BeginHandOff` to all regions.
9. After all acks, worker sends `HandOff` to current owner.
10. On `ShardStopped`, coordinator deallocates shard and reallocates it.

Coordinator store:

```rust
pub trait CoordinatorStateStore {
    async fn load(&self) -> Result<CoordinatorState>;
    async fn update(&self, event: CoordinatorEvent) -> Result<CoordinatorState>;
}
```

Implementations:

- `MemoryCoordinatorStore`: MVP, no coordinator crash recovery.
- `DistributedDataCoordinatorStore`: CRDT-backed later.
- `PersistenceCoordinatorStore`: optional future.

No etcd store.

### Allocation

`ShardAllocationStrategy`:

```rust
pub trait ShardAllocationStrategy: Send + Sync {
    async fn allocate_shard(
        &self,
        requester: RegionRef,
        shard: &ShardId,
        current: &HashMap<RegionRef, Vec<ShardId>>,
    ) -> Result<RegionRef>;

    async fn rebalance(
        &self,
        current: &HashMap<RegionRef, Vec<ShardId>>,
        in_progress: &HashSet<ShardId>,
    ) -> Result<HashSet<ShardId>>;
}
```

Default strategy:

- allocate to region with fewest shards,
- rebalance from most-loaded regions to least-loaded regions,
- limit by absolute and relative rebalance limits,
- never rebalance a shard already in progress,
- avoid allocating to leaving/down/unreachable regions.

### Passivation

Manual passivation:

- entity sends `Passivate` to shard command ref,
- shard sends configured stop message to entity,
- shard buffers messages until termination,
- shard restarts entity if buffered messages arrive.

Automatic passivation later:

- idle timeout,
- per-shard or per-region active entity limit,
- least-recently-used strategy.

## `kairo-cluster-tools`

Higher-level cluster utilities.

Suggested modules:

```text
src/
  singleton/
  pubsub/
  topic/
  reliable_delivery/
```

### Singleton

Used by sharding coordinator when sharding runs across a role or data center.

Design:

- manager actor runs on all eligible members,
- oldest reachable eligible member hosts singleton child,
- proxy forwards to known singleton,
- on membership change, manager coordinates handover,
- on leaving/exiting/down, singleton stops or moves.

Singleton must consume cluster events only. It must not have a separate
membership source.

### PubSub and Topic

Later modules:

- local topic actor first,
- cluster topic over distributed-data registrations,
- best-effort delivery only unless reliable delivery module is used.

## `kairo-testkit`

Test support:

- `TestProbe<M>`,
- `ManualTime`,
- `ActorSystemTestKit`,
- `Behavior/Actor` harness,
- death watch assertions,
- deterministic scheduler hooks,
- multi-node harness for cluster membership and sharding.

Cluster tests must include:

- join through seed node,
- gossip convergence,
- concurrent gossip merge,
- failure detector unreachable/reachable,
- leader `Joining -> Up`,
- leaving/removal,
- downing,
- remote death watch,
- shard allocation and handoff.

## Implementation Order

1. `kairo-actor`: local typed spawn, tell, mailbox, stop, dead letters.
2. `kairo-actor`: death watch, timers, ask, adapters, supervision.
3. `kairo-serialization`: registry, stable manifests, system serializers.
4. `kairo-remote`: provider, remote ref, TCP transport, remote watch.
5. `kairo-cluster`: member, vector clock, gossip, reachability unit tests.
6. `kairo-cluster`: join/welcome, gossip status/full gossip, heartbeat, leader
   actions.
7. `kairo-cluster-sharding`: local-only region/shard/coordinator flow.
8. `kairo-cluster-tools`: singleton MVP for sharding coordinator.
9. `kairo-cluster-sharding`: cluster-aware allocation, rebalance, handoff,
   passivation.
10. `kairo-distributed-data`: CRDT replicator and coordinator state store.
11. `kairo-cluster-sharding`: remember entities.
12. `kairo-testkit`: multi-node cluster and sharding test harness.

## Critical Invariants

- A cluster member is identified by `(address, uid)`, never by address alone.
- Cluster membership state is produced by gossip merge plus local reachability
  observations only.
- External discovery can only provide initial contact points.
- Gossip merges are monotonic except tombstone pruning after the configured
  retention window.
- Only an observer updates its own reachability row.
- Leader actions run only after convergence.
- Actor refs point to an incarnation; path reuse creates a different ref.
- Local actor processing is single-consumer per actor.
- System messages have priority over user messages.
- Sharded entity refs are logical refs and are not death-watch subjects.
- Shard handoff buffers or drops messages explicitly to preserve ordering; it
  must never silently reorder.
