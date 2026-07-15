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

The local Pekko checkout used for this design is `~/IdeaProjects/pekko`.

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
  `~/IdeaProjects/pekko`.
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
  kairo-examples
  kairo-benchmarks
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
       -> kairo-serialization
  -> kairo-distributed-data
       -> kairo-actor
       -> kairo-cluster
       -> kairo-remote
       -> kairo-serialization
  -> kairo-cluster-sharding
       -> kairo-actor
       -> kairo-cluster
       -> kairo-distributed-data
       -> kairo-serialization
  -> kairo-cluster-tools
       -> kairo-actor
       -> kairo-cluster
       -> kairo-distributed-data
       -> kairo-remote
       -> kairo-serialization
  -> kairo-testkit
       -> kairo-actor
kairo-examples
  -> kairo
kairo-benchmarks
  -> kairo
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
- `kairo-examples` and `kairo-benchmarks` are leaf support crates. They may
  depend on the user-facing `kairo` facade to validate public workflows, but no
  runtime crate may depend on them.

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
[actor.task_executor]

[remote]
[remote.transport]

[cluster]
[cluster.seed]
[cluster.downing]

[cluster.sharding]
[cluster.sharding.least_shard_allocation]
[cluster.tools.singleton]
[cluster.tools.pubsub]

[observability.diagnostics]
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
  association_cache.rs
  association_outbound.rs
  association_inbound.rs
  association_pipeline.rs
  outbound.rs
  inbound.rs
  inbound_router.rs
  local_delivery.rs
  resolved_ref.rs
  system_inbound.rs
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
- `ResolvedActorRef<M>` is the typed local-or-remote provider result. It wraps
  either a local `ActorRef<M>` or a `RemoteActorRef<M>` and implements the same
  typed send boundary without introducing an erased user-message API.

`RemoteActorRef<M>`:

- keeps target path and remote address,
- serializes messages through `kairo-serialization`,
- enqueues outbound payloads into an association,
- intercepts `Watch`/`Unwatch` system messages into remote watch protocol.

Association routing:

- `RemoteAssociationAddress` is derived from the explicit
  `ActorRefWireData` protocol, system, host, and optional port fields,
- local-only actor refs without host metadata are rejected before outbound
  routing,
- `RemoteAssociationCache` maps remote association addresses to outbound
  association routes and does not hold cache locks while transport sends run,
- route owners may clear all cached outbound routes during transport shutdown
  to close concrete socket byte sinks even when typed remote refs still hold a
  cloned cache handle,
- association state checks remain in the guarded association outbound wrapper
  so a cache route still rejects quarantined or closed associations before
  touching the transport,
- `RemoteAssociationRouteInstaller` populates the shared cache from concrete
  stream-lane association pipelines, keeping socket byte sinks and association
  state in `kairo-remote` while higher-level cluster/ddata/tools adapters only
  depend on the cache as a route table.
- `RemoteAssociationRegistry` is the transport-independent association
  incarnation index. It owns address-indexed `RemoteAssociation` handles plus a
  UID-to-address index, creates associations by address, completes handshakes
  by activating the association with the observed UID, treats repeated
  address/UID handshakes as idempotent, and rejects one UID being bound to
  multiple remote addresses.

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

TCP association dialing:

- `TcpRemoteByteSink` adapts a connected `TcpStream` to the shared
  `RemoteByteSink` boundary and writes already-framed lane bytes,
- `TcpAssociationDialer` opens one TCP stream for each control, ordinary, and
  large lane and installs the resulting association pipeline into
  `RemoteAssociationCache`,
- `TcpAssociationListener` accepts the expected lane streams for one
  association and `TcpAssociationStreamReader` drains each TCP stream through
  the existing remote stream decoder and `RemoteFrameHandler` boundary,
- a handshaken listener can install reverse association routes by cloning the
  accepted control, ordinary, and large lane streams into `TcpRemoteByteSink`
  values before moving the original streams into lane readers,
- `TcpAssociationDialer::dial_with_reader` installs outbound byte sinks while
  also keeping reader handles for the dialing side's lane streams, so the same
  handshaken TCP association can carry frames in both directions,
- `TcpAcceptedAssociation::spawn_lane_readers` can move accepted lane streams
  onto background reader threads and join them through an explicit
  `TcpAssociationReaderHandle`, allowing one lane to deliver frames while the
  other lane streams remain open,
- `TcpAssociationListener::spawn_accept_loop` owns a bound listener in a
  stoppable background accept loop, creates lane readers for each complete
  accepted association, and reports accepted-association plus stream/frame
  counts through `TcpAssociationListenerHandle`,
- concrete TCP actor-system runtimes send and require an explicit association
  handshake before stream frames. The handshake carries stable local/remote
  `RemoteAssociationAddress` values, the sender system UID, and the lane id.
  Listeners reject lanes addressed to another local address, lanes from mixed
  remote association incarnations, or duplicate lane ids,
- `TcpHandshakeReadSettings` bounds each handshake body and read duration on
  accepted streams and symmetric dialer responses. The 64 KiB default body
  limit is checked against the declared length before allocation, and the
  five-second default read timeout rejects silent peers before their lane can
  enter association assembly; both limits are replaceable on the composed
  runtime builder. Invalid magic, unsupported versions, truncated bodies,
  oversized declarations, and wrong-target handshakes are rejected per socket
  without terminating the shared accept loop or installing identity/route
  state,
- `TcpAssociationAssemblySettings` bounds identity-keyed partial lane
  assemblies. The listener groups interleaved lanes by the complete remote
  address and UID, rejects duplicate lanes or concurrent incarnations for one
  address, expires an incomplete group after the five-second default lane
  arrival timeout, and admits at most 64 pending peer groups by default. Both
  limits are replaceable on the listener and composed runtime builders; an
  invalid or over-limit peer is dropped without terminating the accept loop or
  consuming lanes from another peer,
- after validating all lanes, actor-system listeners return the same stable
  handshake record in the reverse direction. Dialers validate all responses
  before installing a route, so both endpoints know the peer UID and lane
  identity before framed delivery begins,
- `TcpAcceptedAssociation` and `TcpAssociationListenerReport` retain the
  accepted remote identity when handshakes are enabled, preserving the address
  and UID that a later association registry and quarantine layer will need,
- `TcpAssociationListener` may be configured with a
  `RemoteAssociationRegistry`; validated handshakes complete the registry entry
  before accepted streams are handed to lane readers,
- `TcpAssociationListener` may also be configured with a
  `RemoteAssociationRouteInstaller`; accepted handshaken lane streams are
  cloned into reverse `TcpRemoteByteSink` values and installed as an outbound
  route for the remote association address, giving the local runtime a
  bidirectional route without making the association cache a membership store,
- a route installer configured with the runtime association registry uses that
  registry's handle for both accepted and dialed pipelines, keeping send guards,
  UID indexing, diagnostics, and quarantine on one incarnation state,
- `TcpRemoteActorRuntime` composes the concrete TCP listener, association
  cache, route installer, dialer, remote actor-ref provider, actor-system
  manifest registry, and remote death-watch actor into one non-generic
  lifecycle owner. Its builder registers every typed business protocol before
  bind and rejects duplicate manifests. Control-handler factories receive a
  bind-time context containing the effective canonical address, ActorSystem,
  codec registry, system UID, and shared association cache; their manifests
  are required on the control lane and added to the association's outbound
  classifier. `register_reliable_control_handler` additionally selects
  lifecycle manifests for reliable sequencing, while handler factories can use
  the context's reliability-aware outbound boundary for replies. The raw
  association cache remains available as the transport route table.
  `TcpRemoteActorSystem<M>` remains a compatibility facade that
  registers one protocol with the same runtime core,
- `tests/process_remoting.rs` runs the public composed runtime on both sides of
  an OS-process boundary. The receiver child publishes only its bound canonical
  address and typed actor path; the independent sender builds its own codec
  registry and ActorSystem, dials over TCP, delivers through `RemoteActorRef<M>`,
  and both processes complete bounded shutdown without shared runtime state,
- `TcpRemoteActorRuntime::shutdown_with_timeout` stops the runtime-owned
  reliable-delivery scheduler and remote death-watch actors before clearing
  outbound association routes and
  stopping the TCP listener, joins dialing-side lane readers after route
  shutdown, and preserves the shutdown ordering shape Pekko uses for remoting
  internals before transport shutdown,
- `TcpAssociationReaderSupervisor` models the stateless inbound lane restart
  decision: by default any lane or association reader failure plans a full
  inbound-stream restart, a configured restart limit can stop the inbound
  streams, and late failures after stop are ignored,
- `TcpAssociationReaderHandle::join_with_supervisor` folds lane reader
  failures into `TcpAssociationSupervisedReadReport`, and
  `TcpAssociationListenerReport` carries those structured supervision
  decisions alongside accepted identity and frame counts,
- accepted and dialed lane readers carry weak route-lifecycle tokens rather
  than strong pipeline/cache ownership. Completion or failure of any reader
  removes and closes its still-current route, or closes its still-live stale
  pipeline, which shuts down all sibling lane sockets without allowing reader
  ownership to keep the association alive. Repeated close requests preserve
  the first terminal reason, so shutdown is not overwritten by a late reader,
- a fresh validated handshake replaces a previously identified `Closed`
  registry handle, including for the same peer UID, without reopening the old
  handle. This permits an explicitly redialed live process to restore its route
  while preserving terminal-handle semantics. A quarantined UID remains
  rejected until a new incarnation UID arrives, and an unidentified closed
  entry cannot be revived,
- automatic reconnect scheduling/backoff and richer provider lifecycle
  ownership remain separate integration work.

Outbound lanes:

- control/system lane, bounded to 256 frames by default,
- ordinary lane, bounded to 1,024 frames by default,
- large-message lane, bounded to 32 frames by default.

The composed runtime wraps each concrete lane sink in one
`QueuedRemoteByteSink`. `send` encodes and uses bounded `try_send`, so actor
turns never wait for `TcpStream::write_all`; one named writer thread per lane
owns FIFO socket writes. `RemoteOutboundQueueSettings` can replace all three
capacities before bind. Ordinary and large overflow return explicit delivery
errors. Control overflow quarantines the association's current remote UID and
causes later sends to fail at the association guard. Closing a route shuts down
the underlying socket to interrupt an active write and joins the lane owner.
The three queued writers also share a first-failure coordinator: a concrete
write failure closes the shared association state and all raw sibling sockets,
so later sends on every lane reject at the association guard and lane readers
remove the failed route instead of leaving a partially live association.

Inbound pipeline:

1. decode frame,
2. verify association and remote uid,
3. deserialize payload,
4. resolve target ref,
5. enqueue to target or dead letters.

`ActorSystemRemoteInboundRegistry` composes the transport-neutral association
lane readers with a frozen-by-bind manifest dispatch table. Control-lane
death-watch manifests are delivered to the actor-backed remote watcher, while
registered business manifests are deserialized by their typed `RemoteInbound<M>`
handlers and resolved through the local `ActorSystem` registry. The older
`ActorSystemRemoteInbound<M>` remains as a single-protocol compatibility
surface.
Recipients addressed to the local system's canonical remote host and port are
normalized to local actor paths before registry lookup, matching Pekko's
provider behavior for addresses owned by the local node. Recipients addressed
to other hosts or systems remain foreign/missing rather than being silently
localized.

### System Message Delivery

Death watch relies on system messages. Ordinary user messages are at-most-once,
but system watch/unwatch/termination notifications need stronger practical
delivery:

- sequence system messages per association,
- ack received system messages,
- retry until ack or quarantine,
- preserve ordering within the system-message lane.

The stable wire core uses `ReliableSystemEnvelope`, `ReliableSystemAck`, and
`ReliableSystemNack`, with explicit registered serializer IDs and nested stable
`RemoteEnvelope` bytes. `ReliableSystemSender` retains a bounded FIFO beginning
at sequence one, applies cumulative ack/nack progress, rejects replies from a
different `(local UID, remote UID)` pair, and drains/reset sequences when the
remote UID changes. `ReliableSystemReceiver` delivers only the next expected
sequence, acknowledges duplicates without redelivery, and nacks gaps with the
highest contiguous sequence. `TcpRemoteActorRuntime` composes manifest
selection around that state machine: remote watch/unwatch/termination traffic
is reliable by default, heartbeat and other refreshable control traffic remains
at-most-once, and additional lifecycle protocols opt in during pre-bind
registration. One runtime-owned ActorSystem scheduler actor retries each
association's retained FIFO. Cumulative replies clear sender state;
buffer/control overflow, invalid identity or sequence transitions, and
acknowledgement silence past the bounded give-up duration quarantine and close
the exact registry-owned incarnation and report retained messages through
`ReliableSystemDeliveryObserver`. A handshake for a new UID replaces terminal
state and begins again at sequence one.

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
  heartbeat_remote.rs
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

Remote heartbeat transport:

- remote heartbeat requests are addressed to `/system/cluster/heartbeatReceiver`
  on the target node,
- heartbeat responses are addressed to `/system/cluster/heartbeatSender` or a
  configured sender actor-ref path supplied in the outbound envelope metadata,
- `HeartbeatRemoteReceiverOutbound` can be registered as the typed receiver
  route used by `HeartbeatSender`,
- `HeartbeatRemoteReceiverInbound` replies to request sender metadata with a
  stable `HeartbeatRsp` payload,
- `HeartbeatRemoteResponseInbound` deserializes response envelopes and feeds
  `HeartbeatSenderMsg::HeartbeatResponse` back into the local heartbeat sender.

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

Membership transport:

- `ClusterMembershipWireOutbound` serializes `Join`, `Welcome`, and
  `GossipEnvelope` using stable cluster protocol codecs before transport.
- `ClusterMembershipRemoteEnvelopeOutbound` wraps those serialized payloads in
  `RemoteEnvelope` metadata addressed to `/system/cluster/core/daemon` on the
  target node.
- `ClusterSystemInbound` is the cluster system-frame router for decoded remote
  envelopes. It dispatches membership manifests to
  `ClusterMembershipWireInbound`, heartbeat requests to
  `HeartbeatRemoteReceiverInbound`, and heartbeat responses to
  `HeartbeatRemoteResponseInbound` after validating the stable system actor
  recipient path for the local node.
- `register_cluster_system_inbound` registers all cluster system manifests on
  `TcpRemoteActorRuntime` before bind. Its factory receives the effective
  `UniqueAddress` and the remoting runtime's association cache, so membership
  and heartbeat traffic share the same listener, incarnation registry, routes,
  and lane classifier as unrelated typed business protocols.
- `ClusterTcpAssociationRuntime` is the configured-peer socket runtime for
  legacy cluster-only control traffic. It binds a handshaken TCP listener,
  owns a shared
  `RemoteAssociationCache`, association registry, route installer, dialer, and
  dialing-side lane readers, and routes live socket frames into
  `ClusterSystemInbound`; composed ActorSystems should use the shared remoting
  registration while higher cluster runtime ownership is migrated.
- Cluster TCP runtime traffic uses a cluster lane classifier so `Join`,
  `Welcome`, `GossipEnvelope`, `Heartbeat`, and `HeartbeatRsp` all travel on
  the control/system lane.
- `ClusterAssociationPeerState` is the pure cluster-derived association
  planner. It consumes `CurrentClusterState` snapshots and cluster events,
  excludes self, removes peers marked unreachable by the local node, preserves
  peers only from membership state, and emits explicit dial/remove effects for
  TCP runtime owners. Observations from other nodes do not by themselves remove
  a peer, matching Pekko's `validNodeForGossip` rule.
- `ClusterTcpPeerRoutes` applies those dial/remove effects to a
  `ClusterTcpAssociationRuntime`, owns per-peer route registrations, closes and
  removes cached routes when peers are removed, and deliberately keeps
  membership state out of the socket route owner.
- `ClusterTcpPeerRuntime` composes the socket runtime, peer planner, and
  peer-route owner for the first cluster-derived TCP lifecycle boundary. It
  accepts membership snapshots and events, applies the resulting dial/remove
  effects to live routes, and clears those peer routes before listener
  shutdown.
- `ClusterTcpPeerReconnectState` records failed cluster peer dials as
  deterministic pending retries with an explicit retry interval. The lifecycle
  owner retries only peers still desired by membership, clears retry state when
  a peer is dialed or removed.
- `ClusterTcpPeerConnector` is the actor-backed bridge from cluster
  subscriptions to the peer runtime. It subscribes for an initial snapshot,
  applies later membership events to `ClusterTcpPeerRuntime`, accepts explicit
  deterministic retry ticks, can schedule fixed-delay retry ticks through
  actor timers, and shuts the runtime down when the actor stops.
- `ClusterTcpPeerBootstrap` binds the cluster TCP peer runtime, spawns the
  connector under an explicit actor name, and registers coordinated shutdown to
  stop the connector before cluster shutdown so socket routes are cleared
  through the actor stop path.
- The outbound may use `RemoteAssociationCache` for association routing, but
  the cache is not a membership source of truth. Cluster membership remains
  gossip plus local failure-detector observations.
- Local-only target addresses are rejected at the remote-envelope boundary.

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
- `SplitBrainResolver`: role-aware `keep-majority` and `keep-oldest`
  decisions run behind the actor-backed stable-after provider; indirectly
  connected graphs are detected from reachability observer/subject cycles and
  unreachable nodes that have still seen current gossip, then combined with
  the ordinary strategy decision after filtering reachability records between
  indirectly connected nodes.
- Lease-majority is modeled as `LeaseMajorityHook`: it can acquire or deny a
  caller-provided lease before applying a split-brain lease-majority decision,
  but it is not membership truth; gossip and reachability remain the only
  membership and partition evidence.
- Broader data-center-aware policy coverage remains later work.

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
  gossip.rs
  gossip_transport.rs
  remote_targets.rs
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

Remote association integration:

- cluster-route state selects remote replicas and builds `/system/ddata`
  `RemoteEnvelope` recipients,
- `ReplicatorRemoteAssociationCacheOutbound` may deliver those envelopes
  through `kairo-remote::RemoteAssociationCache`,
- the association cache is only an outbound transport route table; it is not a
  source of cluster membership truth,
- concrete socket associations are expected to populate the shared cache once
  socket-backed transport exists.
- `ReplicatorTcpAssociationRuntime` is the configured-peer socket runtime for
  `/system/ddata` traffic. It binds a handshaken TCP listener, owns a shared
  `RemoteAssociationCache`, association registry, route installer, dialer, and
  dialing-side lane readers, and routes bidirectional request/reply envelopes
  through remote association frames.
- `ReplicatorTcpPeerRoutes` consumes cluster membership-derived
  `ClusterAssociationPeerChange` values, applies them to
  `ReplicatorTcpAssociationRuntime`, owns per-peer route registrations, and
  closes/removes cached routes when peers become locally unreachable or leave.
  It deliberately keeps cluster membership state out of distributed-data socket
  route ownership.
- `ReplicatorTcpPeerReconnectState` tracks failed cluster-derived ddata peer
  dials with explicit retry settings, attempt counts, due-time calculation, and
  clear-on-success/remove behavior. It is pure state so actor/runtime ownership
  can drive it deterministically with manual time in tests.
- `ReplicatorTcpPeerRuntime` composes the cluster peer planner, peer-route
  owner, reconnect state, and `ReplicatorTcpAssociationRuntime` into one
  multi-peer ownership boundary for distributed-data sockets. It applies
  cluster snapshots/events, retries due failed dials, clears routes and pending
  retries on shutdown, and still treats sockets only as delivery state.
- `ReplicatorTcpPeerConnector` is the actor-backed bridge from cluster
  subscription events and actor timers into the peer runtime. It subscribes for
  an initial snapshot, applies later membership/reachability events, drives
  explicit or timer-based retry turns, exposes typed snapshots for tests, and
  shuts down the owned runtime when the actor stops.
- `ReplicatorTcpPeerBootstrap` binds the distributed-data TCP peer runtime,
  spawns the connector under an explicit actor name, and registers coordinated
  shutdown to stop the connector before cluster shutdown so socket routes are
  cleared through the same actor stop path.

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
  coordinator_discovery.rs
  coordinator_remote_home.rs
  coordinator_remote_regions.rs
  coordinator_remote_registration.rs
  coordinator_remote_reply.rs
  coordinator_remote_target.rs
  coordinator_system_inbound.rs
  region_coordinator_discovery.rs
  region_discovery_subscriber.rs
  region_remote_coordinator.rs
  region_remote_coordinator_transport.rs
  region_remote/control.rs
  region_system_inbound.rs
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
- `ShardingEnvelopeRouter<M>` as the typed adapter that accepts
  `ShardingEnvelope<M>`, computes the documented stable shard id, and forwards
  into a registered `ShardRegionActor<M>` without requiring business messages
  to contain entity ids,
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
- `local_shard_spawner`,
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

Implementation shape:

- `LocalShardSpawner<M>` owns local shard child construction for the region.
- The default local-shard mode spawns `ShardActor<M>` for deterministic
  plan-level orchestration and remember-entity store testing.
- Entity-backed local-shard mode spawns `EntityShardActor<M>`, preserving the
  same region routing, buffering, and coordinator allocation flow while letting
  shard delivery plans reach typed entity children.
- Remote region route mode uses `RoutedShardEnvelope` with explicit shard id,
  entity id, and nested serialized business message metadata. Outbound and
  inbound adapters translate between typed `ShardRegionMsg<M>` and stable
  `RemoteEnvelope` payloads at `/system/sharding/region`.
- Coordinator discovery state consumes `CurrentClusterState` snapshots and
  `ClusterEvent` member changes, filters members by coordinator role/status,
  and computes Pekko-style oldest-first likely coordinator candidates before
  actor-ref or remote-target registration is wired into the region actor.
- Region coordinator-discovery wiring maps those likely coordinator nodes to
  typed local coordinator refs for the current vertical slice, refreshes the
  region's registration target when the selected coordinator changes, and
  leaves remote singleton target resolution as the later transport-backed
  extension.
- Remote coordinator target resolution derives stable `ActorRefWireData`
  recipients under `/system/sharding/coordinator` from discovered
  `UniqueAddress` values. This keeps remote registration on the stable
  sharding wire protocol instead of pretending remote coordinators are local
  `ActorRef<ShardCoordinatorMsg<M>>` values.
- Remote coordinator registration uses a transport-neutral bridge that
  serializes stable `Register` envelopes to resolved coordinator recipients
  with region sender metadata, and decodes `RegisterAck` replies addressed to
  the region before typed region state consumes the acknowledgement.
- Remote coordinator shard-home requests use a separate bridge that serializes
  stable `GetShardHome` envelopes to the coordinator target and decodes
  `ShardHome` replies back into explicit region wire data for later local
  routing integration.
- Region remote-coordinator state consumes decoded remote `RegisterAck` and
  `ShardHome` values, rejects stale acknowledgements that do not match the
  selected remote coordinator target, and maps remote region wire refs to
  region ids through their stable actor-ref path strings before replaying
  buffered deliveries through the existing region runtime.
- Region remote-coordinator transport composes the stable registration and
  shard-home bridges so a region can send `Register` on remote coordinator
  discovery/retry and send pending `GetShardHome` requests after a matching
  remote `RegisterAck`, without exposing local coordinator messages on the
  wire.
- Region remote-coordinator transport also composes a focused shutdown bridge
  for stable `GracefulShutdownReq` and `RegionStopped` envelopes, keeping
  region shutdown notification on the sharding wire protocol instead of
  serializing local actor messages.
- Region system inbound routing dispatches stable remote envelopes addressed
  to `/system/sharding/region` by manifest: routed entity envelopes enter the
  local region delivery path, while decoded `RegisterAck` and `ShardHome`
  replies enter the remote-coordinator region messages.
- Coordinator system inbound routing dispatches stable remote envelopes
  addressed to `/system/sharding/coordinator` by manifest: decoded `Register`
  commands register the remote region by its stable actor-ref path, attach a
  remote region control target to the coordinator handoff transport, and reply
  with `RegisterAck`, while decoded `GetShardHome` commands enter the
  coordinator actor, dispatch `HostShard` for newly allocated homes, and reply
  with `ShardHome` when the runtime returns a known or newly allocated remote
  region home.
- Coordinator system inbound routing also accepts decoded
  `GracefulShutdownReq` and `RegionStopped` messages, maps their stable
  region wire refs into coordinator region ids, and re-enters the same
  shutdown and region-termination paths used by local regions.
- Remote region control targets serialize coordinator-driven `HostShard`,
  `BeginHandOff`, and `HandOff` commands as stable remote envelopes addressed
  to `/system/sharding/region`; coordinator system inbound routing accepts
  stable `ShardStarted`, `BeginHandOffAck`, and `ShardStopped` replies and
  forwards handoff acknowledgements back to active handoff workers.
- Region-side remote control inbound decodes stable `HostShard`,
  `BeginHandOff`, and `HandOff` commands, re-enters normal region actor state
  transitions, replies with stable `ShardStarted`/`BeginHandOffAck`, and
  replies with `ShardStopped` when a remote handoff targets a shard that is no
  longer local.
- Region-side remote handoff keeps the stable remote `HandOff` command limited
  to the shard id. The receiving region supplies the local entity stop message
  through an explicit stop-message factory, forwards handoff to the hosted
  shard, observes the shard handoff plan, asks for stopper completion when
  needed, marks the local shard stopped, and replies with stable
  `ShardStopped` to the remote coordinator.
- Local graceful region shutdown is modeled as explicit actor messages:
  `ShardRegionActor<M>` marks graceful shutdown in progress, sends
  `GracefulShutdownReq` to its registered local coordinator, rejects later
  host-shard requests through the existing runtime flag, and stops once local
  shard children and shard buffers are gone. The coordinator marks that region
  as gracefully shutting down, excludes it from future allocation, starts
  handoff workers for every shard currently owned by the region, and
  reallocates completed handoffs through the existing shard-home path.
- Remote graceful region shutdown preserves the same observable coordinator
  state transitions through stable `GracefulShutdownReq(region)` and
  `RegionStopped(region)` wire messages whose `region` value is
  `ActorRefWireData`; remote messages use registered codecs and do not depend
  on local `ShardCoordinatorMsg<M>` enum layout.
- `ShardRegionDiscoverySubscriber<M>` owns the cluster subscription for this
  discovery path, requests an initial cluster snapshot, forwards later cluster
  events to `ShardRegionActor<M>`, and unsubscribes when stopped.

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
- `entity_factory: EntityActorFactory<M>` for typed local entity child construction,
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

Implementation shape:

- `ShardRuntime<M>` remains the focused deterministic state machine that plans
  starts, deliveries, passivation buffering, termination, handoff, and
  remember-entity updates.
- `ShardActor<M>` exposes that planner as an actor protocol for deterministic
  orchestration tests.
- `EntityShardActor<M>` composes `ShardRuntime<M>` with
  `EntityActorFactory<M>` to spawn typed local entity actors, deliver business
  messages to those children, watch child termination, and feed termination
  back through the same shard state transitions.

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
- proxy route targets can be local watchable actor refs or remote actor refs
  for `RemoteMessage` protocols; buffering and oldest-member selection are the
  same for both,
- on membership change, manager coordinates handover,
- singleton handover system messages use stable manifests and explicit codecs:
  `HandOverToMe`, `HandOverInProgress`, `HandOverDone`, and
  `TakeOverFromMe` carry the sender `UniqueAddress`,
- remote manager handover envelopes are addressed to
  `/system/singleton/manager`; outbound adapters interpret manager effects and
  inbound adapters validate the recipient path before dispatching into the
  actor-backed manager protocol,
- on leaving/exiting/down, singleton stops or moves.

Singleton must consume cluster events only. It must not have a separate
membership source.

### PubSub and Topic

Later modules:

- local topic actor first,
- cluster topic over distributed-data registrations,
- best-effort delivery only unless reliable delivery module is used.

Remote gossip wiring:

- `PubSubGossipWireOutbound` serializes actor-local status and delta gossip
  messages into stable `PubSubSerializedGossip` payloads,
- `PubSubRemoteEnvelopeOutbound` wraps those payloads in `RemoteEnvelope`
  metadata addressed to the peer mediator at `/system/pubsub`,
- the outbound may use `kairo-remote::RemoteAssociationCache` so concrete
  associations can be shared with other cluster subsystems,
- pubsub still consumes cluster membership/events for peer selection; the
  association cache is only an outbound transport route table.

Remote publish and path delivery:

- remote pubsub user-message delivery uses a stable `PubSubPublishEnvelope`
  that carries topic, optional selected group, and the already-serialized
  business message,
- remote pubsub path delivery uses a stable `PubSubPathEnvelope` that carries
  the logical actor path, whether the command is `Send` or `SendToAll`, and
  the already-serialized business message,
- local pubsub messages remain serialization-free; `M: RemoteMessage` is only
  required when a remote delivery target is registered,
- `PubSubRemoteDeliveryOutbound` wraps publish/group and path delivery for the
  peer mediator at `/system/pubsub`, and `PubSubRemoteDeliveryInbound`
  validates the recipient path before dispatching into the actor-backed
  mediator's local delivery protocol,
- one-message-per-group routing is planned before serialization; remote
  envelopes carry the selected group rather than rerunning group selection on
  the receiving node.

Cluster-tools inbound routing:

- `ClusterToolsSystemInbound<M>` is the transport-neutral inbound dispatch
  boundary for cluster-tools remote envelopes,
- it routes pubsub status/delta manifests to the pubsub gossip wire inbound,
  pubsub publish/path manifests to the pubsub delivery inbound, and singleton
  handover manifests to the singleton manager inbound,
- it validates `/system/pubsub` gossip recipients before delivery and delegates
  publish/path/singleton recipient validation to the focused inbound adapters,
- it implements the remote frame-handler boundary so future socket association
  readers can dispatch decoded cluster-tools frames without each subsystem
  owning its own stream reader.

Cluster-tools TCP runtime:

- `ClusterToolsTcpAssociationRuntime<M>` is the configured-peer socket runtime
  for cluster-tools system traffic,
- it binds a handshaken TCP listener, owns a shared
  `RemoteAssociationCache`, association registry, route installer, dialer, and
  dialing-side lane readers,
- it uses a cluster-tools lane classifier so pubsub gossip, pubsub publish
  envelopes, pubsub path envelopes, and singleton handover messages are treated
  as control/system traffic,
- it routes live socket frames into the existing `ClusterToolsSystemInbound<M>`
  boundary and can send return traffic over the same bidirectional
  association,
- `ClusterToolsTcpPeerRoutes` consumes cluster membership-derived
  `ClusterAssociationPeerChange` values, applies them to
  `ClusterToolsTcpAssociationRuntime<M>`, owns per-peer route registrations,
  and closes/removes cached routes when peers become locally unreachable or
  leave,
- `ClusterToolsTcpPeerRuntime<M>` composes the cluster-tools TCP runtime,
  membership-derived peer planner, route owner, and focused reconnect state
  module so snapshots/events and retry ticks can drive live pubsub/singleton
  association routes through one lifecycle boundary,
- `ClusterToolsTcpPeerConnector<M>` is the actor-backed bridge from cluster
  subscriptions to the cluster-tools peer runtime. It subscribes for an
  initial snapshot, applies later member/reachability events, exposes runtime
  snapshots for diagnostics, and can schedule fixed-delay retry ticks through
  actor timers,
- `ClusterToolsTcpPeerBootstrap<M>` binds the cluster-tools TCP peer runtime,
  spawns the connector under the actor system, and registers a coordinated
  shutdown actor-termination task so configured socket routes are stopped
  before cluster shutdown,
- peer selection still comes from cluster membership/tool state; the TCP
  runtime and route table do not become cluster membership sources.

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

The initial component order has produced substantial state-machine and focused
test coverage. Remaining implementation follows the runtime dependency chain
below. Passing later component tests does not bypass an earlier integration
gate.

1. `kairo-actor`: completed the recorded production dispatcher, bounded task
   executor, and single-driver scheduler model while preserving single-consumer
   typed mailboxes, system-message priority, synchronous `Actor::receive`, and
   deterministic manual time.
2. `kairo-remote`: converge business and system traffic on one
   ActorSystem-owned listener and association lifecycle. Add heterogeneous
   manifest dispatch, bounded non-blocking lanes, reliable ordered system
   messages, and defensive handshake/lifecycle limits.
3. `kairo-cluster`: build the public extension and daemon lifecycle around the
   existing pure membership model and transport components. Complete seed
   contact, init/join/welcome, gossip status/full gossip scheduling, heartbeat,
   convergence-gated leader actions, leave/removal, and coordinated shutdown.
4. `kairo-distributed-data`, `kairo-cluster-sharding`, and
   `kairo-cluster-tools`: consume the composed remoting and real cluster event
   lifecycle instead of independently assembled listeners or injected
   membership snapshots.
5. `kairo`: expose cohesive ActorSystem extensions and make the final sharding
   workflow available through `ClusterSharding::get`, `init`, and
   `entity_ref_for` without low-level actor assembly.
6. `kairo-testkit` and examples: validate the complete join, leave, failure,
   rebalance, handoff, restart, and recovery workflow with independently running
   local nodes.
7. M13: perform API hardening, failure review, release-mode performance tuning,
   platform CI expansion, documentation convergence, and legacy removal
   planning only after steps 1-6 no longer require architecture replacement.

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
