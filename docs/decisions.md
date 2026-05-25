# Architecture Decisions

## ADR-0001: Cluster Membership Uses Gossip, Not Etcd

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

## ADR-0002: Configuration Starts With TOML

Status: Accepted

Context:
Kairo needs a practical configuration file format before the rewrite reaches
the full configuration subsystem. HOCON has a stronger configuration model and
may be a better long-term format, but it should wait until `hocon-rs` is
refactored into the shape needed by this project.

Decision:
The first configuration file format is TOML. Runtime settings structs must
remain format-neutral, and builder-based configuration remains first-class.
HOCON support is deferred until the project intentionally adopts a suitable
`hocon-rs`.

Consequences:
- Initial configuration work can stay small and dependency-light.
- Users get readable config files early.
- A future HOCON loader can map into the same typed settings model.
- The project should not add `hocon-rs` or HOCON syntax support before that
  decision is revisited.

## ADR-0003: Initial Local Actors Use Typed Mailboxes And Worker Threads

Status: Accepted

Context:
M1 needs a runnable local actor loop before remoting, clustering, supervision,
or dispatcher policy can be layered on top. Pekko separates actor refs, actor
cells, mailboxes, and dispatchers; Kairo should preserve the observable
semantics without copying that inheritance-heavy runtime shape.

Decision:
The first local runtime slice stores a typed mailbox sender inside
`ActorRef<M>` and runs each spawned actor on a local worker thread. Messages
enter the mailbox through `tell`, are processed one at a time by synchronous
`Actor::receive`, and a context stop marks the current actor ref as stopped.
Post-stop sends are rejected and recorded through the system dead-letter handle.

Consequences:
- M1 has a small runnable vertical slice for spawn, tell, receive, stop, and
  dead letters.
- Local message protocols remain typed and require no serialization.
- The worker-thread runtime is an implementation baseline, not the final
  dispatcher contract; later M1/M2 work can introduce system lanes, throughput
  limits, supervision, and deterministic test dispatchers behind the same typed
  ref surface.

## ADR-0004: Actor Context Stop Uses Self Or Direct Child Targets

Status: Accepted

Context:
Pekko typed `ActorContext.stop` accepts a child actor ref and rejects refs that
are not direct children. Kairo's initial public contract also uses
`ctx.stop(ctx.myself())` as the explicit self-stop mechanism instead of typed
behavior returns.

Decision:
`Context::stop` accepts any typed `ActorRef<N>` but only stops the current actor
or a direct child of the current actor. Invalid targets return an explicit
`ActorError::InvalidStopTarget`.

Consequences:
- Child actors can be stopped without sharing the parent's message protocol.
- Actors cannot silently stop siblings or unrelated local actors through their
  context.
- Kairo preserves its Rust-first self-stop API while keeping Pekko's direct
  child restriction for child stops.

## ADR-0005: Local Death Watch Uses Signals And Explicit Conflicts

Status: Accepted

Context:
Pekko typed death watch delivers `Terminated` as a signal for `watch`, and
delivers the caller's protocol message for `watchWith`. Re-registering with a
different notification is a runtime error in Pekko.

Decision:
Kairo local death watch stores watch registrations in a focused registry.
`Context::watch` delivers `Signal::Terminated(AnyActorRef)`, while
`Context::watch_with` sends the provided typed message to the watcher mailbox.
Conflicting repeated registrations return `ActorError::AlreadyWatching` instead
of throwing. Because Kairo messages do not require `Eq`, repeated
`watch_with` calls for the same subject must `unwatch` first instead of being
compared for idempotence.

Consequences:
- Default death-watch notifications use the existing actor signal path.
- Custom watch notifications remain typed and local-only without introducing a
  global message enum.
- Conflicts are explicit Rust errors, and changing a custom watch message is an
  explicit `unwatch` plus `watch_with` operation.

## ADR-0006: Initial Scheduler Uses Cancellable Local Tasks

Status: Accepted

Context:
Pekko exposes `scheduleOnce(delay, target, msg)` with a cancellable handle. M2
needs the same actor-facing behavior before the deterministic testkit scheduler
exists.

Decision:
Kairo's initial scheduler is a dependency-free local backend that starts a
single cancellable task for each `schedule_once`. The task sleeps for the delay
and then sends the typed message through the target `ActorRef<M>`. Cancellation
is best-effort until the task completes and reports whether it won the race
against delivery.

Consequences:
- Scheduled messages re-enter actors through normal typed mailboxes.
- Local messages still require no serialization.
- The public `Cancellable` and `schedule_once` API can remain stable while a
  deterministic scheduler backend is added for `kairo-testkit` later.

## ADR-0007: Keyed Timers Use Mailbox Envelopes And Generations

Status: Accepted

Context:
Pekko typed timers guarantee that cancelled or replaced timer messages are not
received, even if the old timer marker was already enqueued in the actor
mailbox. This is stronger than a best-effort cancellable scheduled send.

Decision:
Kairo keyed self timers enqueue a timer envelope containing the key,
generation, and message. Each actor context owns timer state. When a timer
envelope reaches the actor turn, the runtime checks that the key is still
active and the generation matches before delivering the user message. Starting a
new timer for the same key cancels the old task and advances the generation.

Consequences:
- Cancelled and replaced timer messages are discarded before user `receive`.
- Timer messages remain local typed messages and require no serialization.
- Active timers are cancelled when the owning actor stops.
- Fixed-delay repeating timers use the same key/generation envelope mechanism.
- Fixed-rate repeating timers use the same key/generation envelope mechanism
  and schedule against the planned cadence, so delayed ticks can catch up
  without delivering cancelled or replaced generations.

## ADR-0008: Event Stream Uses Exact Typed Local Channels

Status: Accepted

Context:
Pekko's event stream is class-based and delivers events to actor refs
subscribed to a class or superclass. Kairo's primary user API is typed
`ActorRef<M>`, and local messages do not require serialization.

Decision:
Kairo's initial local event stream is keyed by the concrete Rust `TypeId` of
the event message. `EventStream::subscribe` accepts an `ActorRef<M>`, and
`publish` clones an event of type `M` to current subscribers of that exact
type. Duplicate subscriptions are suppressed.

Consequences:
- Event-stream delivery remains local, typed, and free of a global message enum.
- Subscribers only receive events matching their exact Rust message type.
- Broader class/subtype-style matching is deferred until there is a concrete
  Rust design need for it.

## ADR-0009: Pipe-To-Self Uses Task Handles And Mailbox Reentry

Status: Accepted

Context:
Pekko typed `pipeToSelf` adapts a completed future result into the actor's
protocol and sends it back to `self`, so actor state only changes during a
later mailbox turn. Kairo must preserve that observable behavior without adding
`AsyncActor` or requiring an async runtime in the initial local actor design.

Decision:
Kairo's initial `Context::pipe_to_self` and `Context::spawn_task` APIs start a
local dependency-free task thread and return a `TaskHandle`. The task receives
or captures only an `ActorRef<M>`, and completion sends a typed message back
through the normal actor mailbox. `pipe_to_self` requires the task closure to
return `Result<T, E>` so success and failure can both be mapped into the
actor's protocol.

Consequences:
- Actor state is never borrowed across external work.
- Completed task results follow normal mailbox ordering after enqueue.
- Local messages still require no serialization.
- A later async executor or deterministic testkit backend can replace the task
  runner without changing the typed mailbox reentry contract.

## ADR-0010: Message Adapters Enqueue Owner-Mailbox Closures

Status: Accepted

Context:
Pekko typed `messageAdapter` returns an `ActorRef<U>` for an external protocol,
but adapted messages are processed by the owning actor and the adapter function
is run on the owner actor turn. Kairo needs the same mailbox reentry behavior
without exposing erased messages as the primary API.

Decision:
Kairo message adapters create typed adapter refs that enqueue an adapted user
envelope into the owner mailbox. The envelope owns the external message and a
mapping closure, and the runtime evaluates the closure immediately before
calling the owner's `Actor::receive`. Adapter refs have internal `$adapter-*`
paths and do not spawn a separate worker actor.

Consequences:
- Adapter mapping can mutate adapter closure state only on the owner actor turn.
- Adapted messages share the owner mailbox instead of crossing a second actor
  mailbox.
- Local adapters remain typed and require no serialization.
- Registered one-per-message-type adapter replacement can be layered on top of
  this lower-level adapter ref mechanism later.

## ADR-0011: Local Ask Uses One-Shot Reply Refs

Status: Accepted

Context:
Pekko typed `ActorContext.ask` creates a temporary reply ref, sends a request
containing that ref, and maps either the reply or timeout back into the owning
actor's protocol through `pipeToSelf`. The response mapper is evaluated as part
of the owning actor's message processing rather than on the replying actor or
timeout thread.

Decision:
Kairo local ask creates a one-shot typed reply ref under the owner path with an
internal `$ask-*` name. The first reply or timeout wins through shared atomic
completion state. The winning result is enqueued as an adapted owner-mailbox
message, where the mapper converts `AskResult<Res>` into the owner's protocol
immediately before `Actor::receive`.

Consequences:
- Ask response mapping follows the same synchronous actor-turn rule as message
  adapters.
- Late replies are rejected and recorded in dead letters after the ask is
  completed.
- Local ask requires no serialization and does not introduce implicit sender as
  the primary user API.
- The initial implementation uses a dependency-free timeout thread; a later
  deterministic scheduler can replace that timing backend without changing the
  public ask contract.

## ADR-0012: Local Receptionist Uses Exact Typed Service Keys

Status: Accepted

Context:
Pekko typed receptionist registers actor refs by `ServiceKey`, immediately
replies to `Find` and `Subscribe` with the current listing, publishes listing
updates when services change, and removes registrations when registered actors
terminate. Kairo needs those local semantics before cluster receptionist and
group routing can be layered on top.

Decision:
Kairo's initial receptionist is a local typed registry owned by
`ActorSystemInner`. `ServiceKey<M>` is keyed by a user id plus Rust `TypeId`,
and each bucket stores typed service refs and typed listing subscribers.
Register, deregister, find, and subscribe are synchronous local API calls.
Actor termination removes matching service and subscriber refs and publishes an
updated listing to remaining subscribers.

Consequences:
- Local receptionist remains typed and serialization-free.
- Subscribers receive an immediate listing and later updates for exact service
  keys.
- Cluster receptionist can later mirror or replicate these local service-key
  buckets without making `kairo-actor` depend on cluster membership.
- Stable remote service-key manifests will need a separate serialization design
  before receptionist data crosses nodes.

## ADR-0013: Coordinated Shutdown Starts With Local Synchronous Phases

Status: Accepted

Context:
Pekko coordinated shutdown runs a named phase graph once, runs tasks in a phase
without ordering assumptions, waits for each phase before starting the next,
and allows tasks in early phases to register tasks for later phases. Kairo
needs the local lifecycle hook before remote, cluster, sharding, and tool
extensions can attach their own shutdown tasks.

Decision:
Kairo's initial coordinated shutdown is a local `CoordinatedShutdown` handle
owned by `ActorSystemInner`. It provides the standard Pekko phase names in a
fixed order, synchronous task registration, one-shot `run`/`run_from`,
shutdown reason recording, and actor termination tasks. Tasks in a phase are
run on local worker threads and the next phase waits for completion or phase
timeout. `ActorSystem::run_coordinated_shutdown` runs the shutdown phases and
then terminates top-level local actors.

Consequences:
- Local extensions can register orderly shutdown tasks without depending on
  remoting or cluster crates.
- Later phases can still be extended during an earlier phase.
- The initial implementation avoids async/runtime dependencies; deterministic
  timeout control can be replaced by the testkit scheduler later.
- Configuration-driven phase graphs, JVM hooks, and process exit behavior are
  intentionally deferred.

## ADR-0014: Initial Supervision Is A Props Strategy

Status: Accepted

Context:
Pekko typed supervision wraps behavior with directives such as stop, resume,
and restart. Resume keeps the existing behavior state, while restart creates a
fresh behavior instance and sends restart lifecycle signals. Kairo actors are
stateful Rust values built from `Props`, and the existing `Props::new` API is a
one-shot factory so it cannot safely rebuild every actor.

Decision:
Kairo's first supervision slice stores a `SupervisorStrategy` on `Props`.
`Stop` remains the default and preserves the previous failure behavior. `Resume`
drops the failing message and continues with the same actor value. `Restart`
requires `Props::restartable`, which stores a reusable Rust factory; on failure
the runtime cancels timers, stops children, sends `Signal::PreRestart` to the
old actor value, builds a fresh actor value, and invokes `started` on the new
value while preserving the actor ref path and incarnation.

Consequences:
- Existing one-shot actor factories keep their previous stop-on-failure
  semantics.
- Restart is explicit at construction time, avoiding accidental reuse of
  non-replayable captured state such as receivers.
- The first slice covers local self failure; parent deciders, escalation, retry
  limits, and backoff are deferred.

## ADR-0015: Manual Time Uses The Actor Scheduler Boundary

Status: Accepted

Context:
Pekko manual time is an explicitly triggered scheduler used by tests so timer
and scheduled-message behavior can be verified without sleeping. Kairo already
had real-time scheduler methods on `ActorSystem` and `Context`, but testkit
manual time could only send directly to actor refs until the actor runtime had
a scheduler injection boundary.

Decision:
Kairo keeps the real thread-sleeping scheduler as the default actor-system
backend and adds a `ManualScheduler` backend selected through
`ActorSystemBuilder`. `kairo-testkit::ManualTime` wraps that backend, and
`ActorSystemTestKit::with_manual_time` builds systems whose `schedule_once`,
single timers, fixed-delay timers, and fixed-rate timers are advanced by the
test thread.

Consequences:
- Production actor systems keep the existing real-time behavior by default.
- Tests can deterministically advance actor scheduler work without adding an
  async runtime or global clock.
- Scheduler backend state remains inside `kairo-actor`, avoiding a dependency
  cycle between the actor runtime and testkit.
- More precise virtual-time semantics, including time dilation and coordinated
  shutdown phase timeouts, can be layered on this boundary later.

## ADR-0016: Remote Serialization Starts With Explicit Codec Registration

Status: Accepted

Context:
Pekko remote serialization writes a serializer identifier, manifest, and
payload into the remote wire message, with serializer lookup driven by the
runtime serialization registry. Kairo needs the same stable wire metadata, but
must not rely on Rust type names, enum discriminants, or memory layout.

Decision:
`kairo-serialization` defines `RemoteMessage` metadata separately from
`MessageCodec<M>`. `Registry` requires explicit codec registration and maps
outbound Rust `TypeId` plus inbound `(serializer_id, manifest)` to a dynamic
codec bridge. Registration rejects empty manifests, duplicate serializer ids,
and duplicate manifests. `SerializedMessage` carries serializer id, manifest,
version, and payload bytes.

Consequences:
- Local-only actor messages remain serialization-free.
- Remote-capable messages publish stable metadata without choosing a wire
  format.
- Codec registration is explicit and testable before optional serde, cbor,
  json, or prost helper crates exist.
- The derive macro emits only the `RemoteMessage` metadata implementation;
  actor-ref/provider-aware serialization remains a later slice.

## ADR-0017: Actor Ref Wire Data Is Path-Based And Provider-Resolved

Status: Accepted

Context:
Pekko serializes actor refs as serialized actor paths and resolves them through
the current actor-system provider. Kairo needs the same provider boundary while
keeping `kairo-serialization` independent from `kairo-actor` and remoting.

Decision:
`kairo-serialization` owns `ActorRefWireData`, which stores the full serialized
actor path plus parsed protocol, system, host, and port fields. It also defines
an `ActorRefResolver` trait that provider implementations can use to materialize
local or remote refs from wire data. `RemoteEnvelope` now carries
`ActorRefWireData` for recipient and optional sender.

Consequences:
- Actor-ref serialization remains path-based and provider-resolved, matching
  Pekko's observable boundary.
- The core serialization crate still does not depend on actor runtime types.
- Remote provider resolution, cache behavior, and missing-ref fallback remain
  later remoting work.

## ADR-0018: System Protocol Manifests Are Declared With Protocol Types

Status: Accepted

Context:
Remote, cluster, distributed-data, and sharding protocols become public wire
contracts once nodes exchange them. The roadmap requires stable manifests for
these system protocols before behavior depends on them, and Pekko represents
remote watch, gossip, replicator, and sharding coordinator messages as
dedicated serialized protocol messages.

Decision:
Each owning crate declares its first system protocol message structs in a
focused `protocol` module and implements `RemoteMessage` with explicit
`kairo.<area>.<message>` manifests and version `1`. The first slice covers
remote watch/heartbeat messages, cluster join/welcome/gossip envelopes,
distributed-data replicator request/update/subscribe/change messages, and
sharding coordinator registration/home/handoff messages.

Consequences:
- Wire manifests live next to the subsystem that owns the protocol.
- Later codecs can register these protocol types without inventing manifests
  at the behavior implementation point.
- The structs are metadata contracts only for now; state machines, codecs, and
  rolling-version migrations remain separate implementation slices.

## ADR-0019: Shard IDs Use FNV-1a Over Entity IDs

Status: Accepted

Context:
The roadmap requires sharded business messages to avoid embedded entity IDs by
default and requires shard IDs to use a documented stable hash, not Rust's
`DefaultHasher`. Pekko's typed sharding routes through a `ShardingEnvelope`
containing the entity id and the business message, then maps entity ids to
shard ids with a configured number of shards.

Decision:
`kairo-cluster-sharding` introduces `ShardingEnvelope<M>` and changes
`EntityRef<M>` to send `ShardingEnvelope<M>` to the region while accepting plain
business messages from users. Shard ids are computed with 64-bit FNV-1a over
the UTF-8 entity id bytes and `hash % shard_count`, with a documented default
shard count of 100.

Consequences:
- User business messages do not need to carry entity ids.
- Shard allocation does not depend on Rust `DefaultHasher`, type names, enum
  discriminants, or process-local hash seeding.
- Future region/coordinator code can reuse the same stable hashing helper and
  expose configurable shard counts without changing the hash algorithm.

## ADR-0020: Gossip Merge Is Pure Cluster State

Status: Accepted

Context:
Pekko gossip membership is an immutable data structure containing members,
seen state, reachability, vector clock versions, and tombstones. Merging two
gossips first combines tombstones, prunes vector-clock entries for removed
nodes, picks the highest-priority member status for duplicate members, merges
reachability by observer record version, and clears `seen` for the new view.

Decision:
`kairo-cluster::Gossip` starts as a pure Rust state model with no transport or
daemon behavior. It stores normalized members, a seen set, reachability,
vector-clock state, and tombstones. Merge follows Pekko's observable order:
tombstones, vector-clock prune, member status priority, reachability merge, and
seen reset.

Consequences:
- Cluster membership remains gossip-based and does not introduce a central
  membership authority.
- The state machine can be tested deterministically before heartbeat,
  convergence, leader actions, or downing hooks are implemented.
- Later cluster daemons can build on these pure transitions instead of
  embedding merge rules in actor behavior.

## ADR-0021: Cluster Heartbeat I/O Uses Typed Routes First

Status: Accepted

Context:
Pekko's cluster heartbeat sender resolves a remote heartbeat receiver through
an actor selection at `/system/cluster/heartbeatReceiver`, sends a heartbeat,
and updates the failure detector when the receiver replies. Kairo does not yet
have the remote provider and association cache needed to resolve remote system
actor paths, but the heartbeat sender/receiver behavior is needed by the
cluster runtime milestone.

Decision:
The first Kairo heartbeat actor slice keeps the Pekko state transitions but
uses an explicit typed route table from `UniqueAddress` to
`ActorRef<HeartbeatReceiverMsg>`. `HeartbeatReceiver` replies to the supplied
typed sender ref, and `HeartbeatSender` owns periodic tick handling,
current-state initialization, cluster event updates, expected-first-heartbeat
monitoring, and response-driven failure-detector updates. Stable wire metadata
for `Heartbeat` and `HeartbeatRsp` lives in the cluster `protocol` module so
the later remote transport can carry the same protocol messages.

Consequences:
- The heartbeat runtime remains actor-backed and testable before remote actor
  selection exists.
- Cluster membership remains gossip/failure-detector based; route registration
  is a transport addressing concern, not membership authority.
- The route table can be replaced or populated by remote association/provider
  code later without changing the heartbeat state machine or wire manifests.

## ADR-0022: Cluster Subscription Snapshots Use A Typed Sum Protocol

Status: Accepted

Context:
Pekko's cluster subscription API can send `CurrentClusterState` as the first
message and later send cluster domain events to the same untyped `ActorRef`.
Kairo's public boundary is `ActorRef<M>`, so a subscriber that wants both the
initial snapshot and later events needs one explicit protocol type.

Decision:
Kairo exposes a public `Cluster` facade around the event publisher. The default
`Cluster::subscribe` sends an initial snapshot and later events through
`ActorRef<ClusterSubscriptionEvent>`, where `ClusterSubscriptionEvent` is a
typed sum of `CurrentState(CurrentClusterState)` and `Event(ClusterEvent)`.
Callers that only want domain events can still use `subscribe_events` with an
`ActorRef<ClusterEvent>`.

Consequences:
- The default subscription behavior preserves Pekko's snapshot-first public
  semantics without introducing erased messages as the user API.
- Event-only subscribers keep a narrower `ActorRef<ClusterEvent>` protocol.
- The facade remains a lightweight handle over the cluster event publisher
  until full membership actors own the publisher lifecycle.

## ADR-0023: Initial Split-Brain Resolver Hooks Are Synchronous Policies

Status: Accepted

Context:
Pekko's split-brain resolver is an actor-backed downing provider. It waits for
stable reachability, handles indirectly connected graphs, and can use lease
acquisition for lease-majority decisions. Kairo has the gossip, reachability,
and downing-plan state needed for deterministic decisions, but not yet the full
provider lifecycle, lease abstraction, or multi-node transport.

Decision:
Kairo's first concrete downing slice exposes synchronous
`SplitBrainResolverHook` policies for `down-all`, `keep-majority`, and
`keep-oldest`. These hooks implement the primary Pekko decisions over the
current gossip snapshot and feed the existing `DowningPlan`; lease-majority,
indirectly-connected graph handling, and stable-after actor timing remain
future provider work.

Consequences:
- Tests can cover concrete downing behavior without introducing a central
  membership authority or a premature lease dependency.
- The public downing boundary remains `DowningHook` plus `DowningPlan`, so an
  actor-backed provider can reuse the same policy decisions later.
- Full split-brain resolver parity still requires stable-after scheduling,
  indirectly-connected handling, and lease-majority support.

## ADR-0024: Remote Refs Start As Typed Recipient Boundaries

Status: Accepted

Context:
Pekko's `RemoteActorRefProvider` resolves non-local paths into immutable
`RemoteActorRef` values that serialize through the transport, while local paths
stay with the local provider. Kairo's current local `ActorRef<M>` is still
owned by `kairo-actor`; forcing a remote transport closure into that type now
would couple provider internals before inbound dispatch, associations, and
remote death watch are stable.

Decision:
The first remoting slice introduces `kairo_remote::RemoteActorRef<M>` as a
typed `Recipient<M>` implementation. It keeps the target wire path, serializes
`M: RemoteMessage` through the registry into a `RemoteEnvelope`, and hands the
envelope to a `RemoteOutbound` boundary. `RemoteActorRefProvider` resolves only
host-qualified remote paths into these refs and rejects local-only paths.
Later actor-system provider integration can wrap or compose this same remote
ref behavior when `ActorRef<M>` gains full location transparency.

Consequences:
- Remote send behavior is testable before TCP transport and inbound dispatch
  exist.
- Local-only messages still need no serialization because only the remote ref
  boundary requires `M: RemoteMessage`.
- The later provider work must preserve this wire-envelope behavior while
  adding local/remote resolution into the public actor-system API.

## ADR-0025: Initial Backoff Supervision Is Deterministic

Status: Accepted

Context:
Pekko's backoff supervisor uses exponential delays capped by `maxBackoff` and
can add random jitter through `randomFactor`. Kairo needs the restart
state-machine semantics for local supervision and later sharding passivation
work, while keeping early M2 behavior dependency-light and deterministic under
manual time.

Decision:
The initial `BackoffSupervisor` implements the on-stop restart flow with
`min_backoff * 2^restart_count` capped by `max_backoff`, typed child/restart
queries, manual reset, and automatic reset scheduling. It intentionally omits
jitter/random-factor configuration until there is a concrete runtime need and a
chosen randomness/testing policy.

Consequences:
- Backoff tests are deterministic with `ManualScheduler`.
- The observable restart ordering and capped exponential state transitions are
  available without adding a random dependency.
- Jitter can be added later as an explicit settings field without changing the
  child-watch and delayed-restart state machine.

## ADR-0026: Distributed-Data Reply Correlation Uses RemoteEnvelope Sender

Status: Accepted

Context:
Pekko's distributed-data read/write aggregators send `Read`, `Write`, and
`DeltaPropagation` messages as actors and receive `ReadResult`, `WriteAck`,
`WriteNack`, or `DeltaNack` replies through the normal sender actor-ref
mechanism. Kairo's stable replicator protocol payloads intentionally carry
CRDT and replica metadata, but adding operation ids to those payloads now
would create another wire contract before the remoting envelope correlation
path is used.

Decision:
Distributed-data request and reply correlation follows the remoting boundary:
the ddata payload stays focused on replicator semantics, while
`RemoteEnvelope.sender` carries the local aggregator actor ref when a remote
request expects replies. Remote replies target that sender actor ref. A focused
ddata remote-envelope bridge preserves recipient and optional sender
`ActorRefWireData` while serializing registered replicator protocol messages.

Consequences:
- Aggregator actor paths provide the correlation identity, matching Pekko's
  observable sender-based reply flow without adding request ids to every
  replicator payload.
- Stable ddata manifests remain unchanged; transport wiring composes payload
  codecs with the existing remote envelope metadata.
- Later actor-system provider integration must ensure temporary aggregation
  actors are resolvable for the lifetime of the operation and removed when the
  operation completes or times out.

## ADR-0027: Distributed-Data Quorum Failures Stay Distinct From Timeouts

Status: Accepted

Context:
Pekko's distributed-data replicator distinguishes successful updates, read
failures, write timeouts, and store or replication failures. Kairo's initial
`UpdateResponse` only had `Success`, `Timeout`, and `ModifyFailure`, which
would force impossible write quorums caused by NACKs or not-enough remaining
replicas to be reported as timeouts.

Decision:
`UpdateResponse` includes a general `Failure { key, reason }` variant for
non-modification failures that are known synchronously by the aggregation
operation. Timeout remains reserved for elapsed deadline behavior, while
`ModifyFailure` remains reserved for user update-function failures.

Consequences:
- Public write replies can preserve the observable distinction between an
  elapsed deadline and a failed quorum.
- Future durable-store or transport-level aggregation failures can use the
  same explicit failure variant without changing stable wire manifests.
- Existing local update behavior remains unchanged.

## ADR-0028: Distributed-Data Uses Remote Outbound Routes For Transport

Status: Accepted

Context:
Pekko's distributed-data replicator sends delta propagation, gossip status,
gossip payloads, and direct read/write messages to the same replicator path on
the target node. Kairo already wraps registered ddata payloads in stable
`RemoteEnvelope` recipient/sender metadata, and the remote crate owns the
association, lane, stream, and future socket boundaries.

Decision:
Distributed data may depend on `kairo-remote` for outbound association
delivery, but only after cluster route state has selected target replicas.
`ReplicatorRemoteAssociationRoutes` maps `ReplicaId` values to
`RemoteOutbound` association routes, and
`ReplicatorRemoteAssociationOutbound` adapts `ReplicatorRemoteEnvelope` values
into those routes. The inbound side uses one configured source `ReplicaId` per
association and dispatches stable ddata manifests to request or reply inbound
handlers after decoding the remote envelope frame. Missing association routes,
remote send failures, and unsupported inbound manifests are explicit delivery
errors.

Consequences:
- Distributed data remains a CRDT replication subsystem, not a membership
  authority.
- The same `/system/ddata` remote-envelope target path can be carried by
  transport-neutral tests, remote association pipelines, and later socket
  wiring.
- Automatic association-cache population remains a separate integration step
  and does not change stable ddata manifests.

## ADR-0029: Distributed-Data Gossip Digests Use Stable Envelope Bytes

Status: Accepted

Context:
Pekko's distributed-data full-state gossip compares per-key digests before
sending full CRDT envelopes, and uses a not-found digest to request data the
receiver is missing. Kairo needs equivalent status/gossip behavior, but must
not rely on Rust type names, enum discriminants, debug formatting, or memory
layout when comparing or serializing replicated data.

Decision:
Kairo distributed-data gossip uses 64-bit FNV-1a over the same explicit
`ReplicatorDataEnvelope` fields used by the stable wire protocol: CRDT
manifest, CRDT version, payload bytes, removed-replica pruning ids, and tagged
pruning-state fields. Variable-length fields are length-delimited before
hashing. Digest value `0` is reserved as the not-found marker, so a computed
zero digest is remapped to `1`.

Consequences:
- Gossip status comparison is deterministic across nodes without making Rust
  implementation details part of the contract.
- Full-state gossip can request missing keys independently from local CRDT
  payload serialization.
- Changing the digest algorithm later would require an intentional protocol
  version decision because mixed nodes compare these digest values.

## ADR-0030: Distributed-Data Gossip Ticks Use Deterministic Target Rotation

Status: Accepted

Context:
Pekko's distributed-data full-state gossip tick selects a random node from the
known replicas before sending a status digest. Kairo needs the same eventual
anti-entropy behavior, but early actor tests and manual scheduler scenarios
benefit from deterministic, timeout-light execution.

Decision:
Kairo's initial actor-backed full-state gossip tick rotates through reachable
remote replicas in deterministic order. Chunked status messages also advance
one chunk per tick. Inbound `Status` and `Gossip` handling still preserves the
Pekko state transitions: send differing or missing local keys as gossip,
request remote-only keys with the reserved not-found digest, merge inbound
full-state data, and send a non-recursive reply gossip when `send_back` is set.

Consequences:
- Gossip tests can assert exact target selection under manual time without
  introducing randomness or broad dependencies.
- Anti-entropy still progresses across reachable replicas and chunks.
- A later randomized or pluggable selection policy can be added as an explicit
  setting if production load distribution requires it.

## ADR-0031: Remote Association Cache Keys Use Structured Wire Addresses

Status: Accepted

Context:
Remote actor refs already carry explicit `ActorRefWireData` metadata for
protocol, actor system, host, port, and full path. As remoting starts to share
association routes with distributed data, sharding, and cluster tools, routing
by the full actor path or reparsing display strings would mix actor identity
with transport association identity.

Decision:
`RemoteAssociationCache` keys routes by a structured
`RemoteAssociationAddress` containing protocol, system, host, and optional
port. The cache derives this key from `ActorRefWireData` recipient metadata,
rejects local-only refs that have no host, and routes the original
`RemoteEnvelope` unchanged to the selected outbound association. Quarantine and
closed-state checks remain in `AssociationRemoteOutbound`, not in the cache.

Consequences:
- Actor path changes under the same remote system do not create independent
  association routes.
- Local-only refs cannot accidentally cross the remote transport boundary.
- Higher-level subsystems can share one transport-neutral association cache
  without making it a membership authority or embedding subsystem-specific
  routing rules in the remote crate.

## ADR-0032: PubSub Gossip Uses Remote Envelopes For Peer Mediators

Status: Accepted

Context:
Pekko distributed pubsub gossips status and delta messages to the same mediator
path on selected peer nodes using the local mediator's actor path with the peer
address. Kairo already has stable pubsub status/delta manifests and a shared
remote association cache, but pubsub must not make remoting a source of cluster
membership truth.

Decision:
Kairo pubsub wraps serialized status/delta gossip payloads in `RemoteEnvelope`
metadata addressed to `/system/pubsub` on the selected peer node.
`PubSubRemoteEnvelopeOutbound` may use `RemoteAssociationCache` as its outbound
route table, while peer selection remains owned by cluster/pubsub state.
Local-only peer addresses are rejected before remote envelope delivery.

Consequences:
- Pubsub gossip status/delta messages can share remote associations with other
  cluster subsystems without duplicating per-subsystem socket routing.
- The mediator path is a documented Kairo system path and can be made
  configurable later without changing the status/delta payload manifests.
- User publish/send message delivery remains a separate remote-wire decision
  because business messages need their own stable codec metadata.

## ADR-0033: Cluster Membership Uses Remote Envelopes For Core Daemon Traffic

Status: Accepted

Context:
Pekko sends cluster membership commands such as `Join`, `Welcome`, and
`GossipEnvelope` to the remote cluster core daemon path
`/system/cluster/core/daemon`. Kairo already has stable cluster protocol
codecs and a transport-neutral membership wire bridge, but still needs a
shared remote association boundary that does not turn remoting into a
membership authority.

Decision:
Kairo wraps serialized cluster membership payloads in `RemoteEnvelope` metadata
addressed to `/system/cluster/core/daemon` on the target node.
`ClusterMembershipRemoteEnvelopeOutbound` may use the shared
`RemoteAssociationCache` for routing, rejects local-only targets before
transport delivery, and leaves membership peer selection and state transitions
inside the gossip membership actor.

Consequences:
- Join, welcome, and gossip payloads can share remote associations with other
  cluster subsystems without duplicating transport route tables.
- The cluster core daemon path is a documented Kairo system path and can be
  made configurable later without changing cluster message manifests.
- Socket-backed cluster transport and heartbeat receiver routing remain
  separate integration steps.

## ADR-0034: Remote Inbound Composition Splits Business And System Traffic

Status: Accepted

Context:
The remote inbound pipeline must deliver ordinary user messages to typed local
actor refs while routing death-watch control messages to the remote watcher.
Both flows share association lane decoding and remote envelope framing, but
they should not make erased dynamic messages part of the user actor API.

Decision:
`ActorSystemRemoteInbound<M>` composes association lane readers with a focused
inbound frame router. Control-lane death-watch manifests are deserialized by
the remote-watch system inbound boundary and delivered to the actor-backed
watcher. Ordinary manifests are deserialized as `M` and resolved through the
local `ActorSystem` actor-ref registry. Death-watch manifests arriving on
ordinary or large lanes are rejected.

Consequences:
- The inbound socket wiring can be built around one reusable association
  reader without mixing business delivery and system watch state machines.
- Local typed actors remain addressed through `ActorRef<M>` and do not receive
  erased dynamic envelopes.
- Additional system protocols can later be added to the router with stable
  manifests and explicit lane rules.

## ADR-0035: Cluster Heartbeat Remote Routing Uses Stable System Paths

Status: Accepted

Context:
Pekko sends cluster heartbeats to `/system/cluster/heartbeatReceiver` and the
receiver replies to the sender, which updates the failure detector. Kairo's
first heartbeat slice used explicit typed receiver routes so the state machine
could be tested before remote actor selection existed. The next step needs
remote-envelope routing while keeping heartbeat observations as local
failure-detector input, not membership truth.

Decision:
Kairo heartbeat remote routing uses focused adapters around the existing typed
heartbeat actor protocols. `HeartbeatRemoteReceiverOutbound` can be registered
as a `HeartbeatSender` receiver route and wraps stable `Heartbeat` payloads in
`RemoteEnvelope` metadata addressed to `/system/cluster/heartbeatReceiver`.
`HeartbeatRemoteReceiverInbound` validates the receiver path, deserializes the
heartbeat, and replies to the envelope sender metadata with a stable
`HeartbeatRsp`. `HeartbeatRemoteResponseInbound` validates response recipient
metadata and feeds `HeartbeatSenderMsg::HeartbeatResponse` into the local
heartbeat sender.

Consequences:
- The heartbeat sender state machine and failure-detector semantics stay
  unchanged while remote associations begin carrying heartbeat traffic.
- Remote actor deployment and general actor selection are not required for
  cluster heartbeat routing.
- Socket-backed association population remains a later integration step; the
  heartbeat adapters accept any `RemoteOutbound`, including the shared
  `RemoteAssociationCache`.

## ADR-0036: Singleton Proxy Routes Use Local Or Remote Targets

Status: Accepted

Context:
Pekko's singleton proxy identifies the singleton actor at the oldest member's
actor path, watches the identified actor, forwards messages when available,
and buffers messages while the singleton is unknown. Kairo's first proxy slice
implemented the same buffering and oldest-member routing for local
`ActorRef<M>` values. Remote singleton delivery now needs to use existing
`RemoteActorRef<M>` support without making local messages require
serialization.

Decision:
Kairo singleton proxy routes store `SingletonProxyTarget<M>`. A local target
wraps a watchable `ActorRef<M>`. A remote target wraps `RemoteActorRef<M>` and
is available only for `M: RemoteMessage`, so remote business messages still use
stable manifests and registered codecs while local-only messages remain
serialization-free. The proxy's buffer, oldest-member selection, and
reidentification behavior are shared for local and remote targets; local
targets keep death-watch cleanup, while remote death-watch integration remains
a later provider-level step.

Consequences:
- Singleton proxy remote delivery reuses the typed remote actor boundary
  instead of adding an erased dynamic message path.
- Existing local singleton proxy behavior and tests continue to use
  `RegisterRoute { singleton: ActorRef<M> }`.
- Socket-backed route discovery and remote singleton manager handover messages
  remain separate integration steps.

## ADR-0037: Singleton Handover Uses Stable Remote Envelopes

Status: Accepted

Context:
Pekko singleton managers coordinate ownership changes with internal
`HandOverToMe`, `HandOverInProgress`, `HandOverDone`, and `TakeOverFromMe`
messages. Kairo already models those effects in the singleton manager runtime,
but remote manager wiring needs stable wire contracts and a transport-neutral
envelope adapter before socket transport can carry them.

Decision:
Kairo declares singleton handover messages as cluster-tools `RemoteMessage`
protocol types with explicit manifests, version `1`, fixed serializer IDs, and
wire payloads containing the sending `UniqueAddress`. The codecs reuse the
same explicit address encoding used by pubsub gossip messages and do not rely
on Rust type names, enum discriminants, or memory layout.

`SingletonManagerRemoteOutbound` maps runtime handover effects into serialized
remote envelopes addressed to `/system/singleton/manager` on the target node.
`SingletonManagerRemoteInbound` validates that recipient path and dispatches
decoded handover messages back into the actor-backed singleton manager protocol.
The sending node is explicit in the payload rather than inferred from an
implicit actor sender.

Consequences:
- Singleton manager remote handover traffic now has a stable metadata and codec
  contract plus focused remote-envelope outbound/inbound adapters.
- The runtime effect planner remains transport-neutral; the remote adapter is
  only an edge interpreter for already planned handover effects.
- Socket-backed association population and route discovery remain separate
  integration steps.
