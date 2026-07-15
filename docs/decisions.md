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
- A default unhandled `Signal::Terminated` or `Signal::ChildFailed` is a
  death pact and stops the watcher with an explicit `ActorError::DeathPact`,
  matching Pekko's observable unhandled-termination behavior.
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
the runtime cancels actor-owned timers, tasks, asks, and adapters, sends
`Signal::PreRestart` to the old actor value while existing children are still
visible, removes the restarting actor's death-watch registrations for children
that the default restart strategy is about to stop, stops those children, builds
a fresh actor value, and invokes `started` on the new value while preserving the
actor ref path and incarnation.

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
and updates the failure detector when the receiver replies. Kairo first needed
the heartbeat sender/receiver state machine before layering remote system-path
resolution, association-cache routing, and TCP system inbound delivery on top
of it.

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
- The heartbeat runtime remains actor-backed and testable independently from
  the later remote heartbeat envelope and TCP association paths.
- Cluster membership remains gossip/failure-detector based; route registration
  is a transport addressing concern, not membership authority.
- Remote heartbeat outbound/inbound adapters and cluster TCP system routing
  now reuse the same heartbeat state machine and wire manifests without making
  association routes cluster membership evidence.

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

## ADR-0023: Split-Brain Resolver Policies Use Actor-Backed Stable Timing

Status: Accepted

Context:
Pekko's split-brain resolver is an actor-backed downing provider. It waits for
stable reachability, handles indirectly connected graphs, and can use lease
acquisition for lease-majority decisions. Kairo has the gossip, reachability,
and downing-plan state needed for deterministic decisions, plus a local actor
runtime with deterministic timers. Lease-majority is now captured by ADR-0094
as an explicit downing hook, not as membership truth.

Decision:
Kairo exposes synchronous
`SplitBrainResolverHook` policies for `down-all`, `keep-majority`, and
`keep-oldest`. These hooks implement the primary Pekko decisions over the
current gossip snapshot and feed the existing `DowningPlan`. They also detect
indirectly connected nodes through Pekko's two observable signals: nodes that
are both negative reachability observers and unreachable subjects, and nodes
that are unreachable while still having seen the current gossip. Indirect
decisions down those nodes and combine with the ordinary strategy decision
after filtering reachability records between the indirectly connected nodes.
The actor-backed `DowningProviderActor` owns the stable-after timer and applies
hook decisions only after reachability has remained stable and the local node
is the reachable leader. Membership gossip reaches the provider through an
explicit typed registration message rather than hidden plugin lifecycle wiring
until full cluster bootstrap owns provider startup.

Consequences:
- Tests can cover concrete downing behavior without introducing a central
  membership authority or a premature lease dependency.
- The public downing boundary remains `DowningHook` plus `DowningPlan`, while
  provider timing is a focused actor rather than being folded into gossip
  state.
- Full split-brain resolver parity still requires broader live-socket
  validation beyond focused provider decisions.

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
codecs, a transport-neutral membership wire bridge, and a shared
remote-envelope association boundary that does not turn remoting into a
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
- Socket-backed cluster transport composes this boundary with the shared
  association cache, while heartbeat receiver routing remains a separate
  system path with its own stable manifests.

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

## ADR-0038: PubSub Remote Publish Envelopes Carry Serialized Business Messages

Status: Accepted

Context:
Pekko distributed pubsub sends `Publish` and one-message-per-group delivery
through the peer mediator actor, and its serializer wraps the user payload
inside pubsub protocol messages. Kairo's local pubsub protocol is generic and
must stay serialization-free for local-only use, while remote pubsub publish
delivery needs a stable wire boundary.

Decision:
Kairo uses a stable `PubSubPublishEnvelope` remote protocol message for remote
pubsub user delivery. The envelope contains the topic, an optional selected
group, and a nested `SerializedMessage` for the business payload. The business
message is serialized with its own registered `RemoteMessage` codec before the
pubsub envelope is serialized.

`PubSubRemoteDeliveryOutbound<M>` implements the local pubsub delivery
recipient boundary for `M: RemoteMessage`, maps broadcast publish and selected
group delivery into `RemoteEnvelope` traffic addressed to `/system/pubsub`, and
can use `RemoteAssociationCache` as its outbound route table.
`PubSubRemoteDeliveryInbound<M>` validates the recipient path, decodes the
pubsub envelope, decodes the typed business message, and dispatches it through
the actor-backed mediator's local delivery path.

Consequences:
- Local pubsub subscribers and publishes still do not require serialization.
- Remote pubsub delivery has stable manifests, versions, serializer IDs, and
  nested business-message metadata without relying on Rust type names or enum
  layout.
- One-message-per-group selection remains a sender-side planning decision; the
  remote envelope carries the selected group to the target mediator.
- Socket-backed association population remains a later integration step.

## ADR-0039: Association Pipelines Populate The Shared Remote Cache

Status: Accepted

Context:
Pekko remoting creates transport associations for remote addresses and routes
subsequent outbound messages for that address through the associated endpoint.
Kairo already has explicit `RemoteEnvelope` metadata, lane/stream framing,
association state, and a shared `RemoteAssociationCache`, but higher-level
subsystems still needed a focused way to populate that cache from a concrete
association pipeline before TCP listener/dialer code exists.

Decision:
Kairo introduces `RemoteAssociationRouteInstaller` in `kairo-remote`. It builds
an `AssociationOutboundPipeline` from concrete control, ordinary, and large
byte sinks, inserts the guarded pipeline into `RemoteAssociationCache` under a
structured `RemoteAssociationAddress`, reports whether a route was replaced,
and supports explicit route removal.

Consequences:
- Cluster, distributed-data, sharding, and cluster-tools adapters can share one
  route table once a socket layer supplies byte sinks.
- Association state remains owned by the remote pipeline; cache-routed sends
  still observe close/quarantine checks before any byte sink is touched.
- The cache remains only an outbound transport route table. Cluster membership
  and peer selection stay in cluster-owned state.
- TCP bind/dial lifecycle remains a later integration step around this
  transport-neutral installer.

## ADR-0040: Cluster Tools Remote Inbound Dispatches By Stable Manifest

Status: Accepted

Context:
Singleton manager handover, pubsub gossip, and pubsub publish delivery now have
separate stable manifests, codecs, and focused inbound adapters. Socket-backed
associations still need one transport-neutral cluster-tools system boundary so
frame readers can dispatch decoded `RemoteEnvelope` values without duplicating
manifest checks in each integration point.

Decision:
Kairo introduces `ClusterToolsSystemInbound<M>` in `kairo-cluster-tools`. It
routes stable pubsub status/delta manifests to `PubSubGossipWireInbound`,
pubsub publish manifests to `PubSubRemoteDeliveryInbound<M>`, and singleton
handover manifests to `SingletonManagerRemoteInbound`. Pubsub gossip recipient
validation happens at the system boundary; publish and singleton validation
remain in their focused inbound adapters. The router also implements
`RemoteFrameHandler` so future association readers can dispatch cluster-tools
frames through one boundary.

Consequences:
- Cluster-tools remote inbound logic is structured by responsibility instead
  of concentrated in one pubsub or singleton module.
- Stable manifests remain the dispatch contract; Rust enum discriminants,
  type names, and memory layout are not used for routing.
- The router is still transport-neutral. Socket listener/dialer lifecycle and
  actor-system installation remain later integration work.

## ADR-0041: TCP Outbound Dialing Installs Lane Pipelines

Status: Accepted

Context:
Kairo has transport-neutral remote envelope frames, lane stream encoding,
guarded association outbound state, and a shared association cache. The next
socket-backed step is an outbound TCP primitive that can populate the cache
with concrete byte sinks while keeping listener lifecycle and actor-system
installation separate.

Decision:
Kairo introduces `TcpRemoteByteSink` and `TcpAssociationDialer` in
`kairo-remote`. `TcpRemoteByteSink` wraps a connected `TcpStream` behind the
existing `RemoteByteSink` trait. `TcpAssociationDialer` resolves a
`RemoteAssociationAddress`, opens one TCP stream each for the control,
ordinary, and large lanes, builds an `AssociationOutboundPipeline` through
`RemoteAssociationRouteInstaller`, and installs that route into
`RemoteAssociationCache`.

Consequences:
- TCP remains below the actor API; actors and higher-level cluster subsystems
  still depend on typed refs, remote envelopes, and association caches.
- The first TCP slice is outbound-only. Listener acceptance, inbound lane
  readers, handshakes, reconnect/backoff policy, and coordinated shutdown
  ownership remain later work.
- Lane framing and association close/quarantine checks continue to live in the
  existing transport-neutral pipeline instead of in TCP-specific code.

## ADR-0042: TCP Inbound Streams Feed The Frame Handler Boundary

Status: Accepted

Context:
Outbound TCP dialing can populate the shared association cache with lane
pipelines, but the receiving side also needs a focused socket primitive that
accepts lane streams and feeds decoded frame payloads into the existing remote
inbound boundaries. Pekko Artery keeps lane transport, stream decoding, and
message dispatch as separate stages; Kairo should preserve that separation
with Rust modules instead of embedding TCP reads into actor or cluster code.

Decision:
Kairo splits TCP support into `dialer`, `sink`, and inbound modules.
`TcpAssociationListener` accepts the expected lane streams for one association,
and `TcpAssociationStreamReader` drains each accepted `TcpStream` through
`StreamFrameInbound` into a supplied `RemoteFrameHandler`. The TCP layer does
not deserialize messages or resolve actors; it only turns socket bytes into
remote stream frames for the existing transport-neutral inbound router.

Consequences:
- TCP inbound socket handling stays below actor, cluster, distributed-data,
  and cluster-tools protocols.
- The same frame-handler boundary can be used for actor-system remote inbound,
  distributed-data association inbound, or cluster-tools system inbound.
- Long-running listener loops, handshakes, reconnect/backoff behavior, and
  coordinated shutdown ownership remain later integration work.

## ADR-0043: Accepted TCP Associations Own Independent Lane Readers

Status: Accepted

Context:
Pekko Artery keeps TCP lane streams alive independently and dispatches decoded
frames to the appropriate inbound lane while other streams on the same
association remain open. Kairo's first inbound TCP slice could drain accepted
streams sequentially, which is enough for closed test sockets but does not
match the concurrent lane shape needed by live associations.

Decision:
`TcpAcceptedAssociation` exposes `spawn_lane_readers`, which moves each
accepted lane stream into its own reader thread and returns an explicit
`TcpAssociationReaderHandle`. Joining the handle waits for all lane readers,
accumulates their stream/frame counts, and returns the first reader failure or
panic after every thread has been joined.

Consequences:
- Live TCP associations can dispatch ordinary, control, or large-lane frames
  without waiting for sibling lane streams to close first.
- The reader handle is explicit lifecycle state that a future actor-system
  remote provider can own, stop, and supervise.
- Reader restart policy, handshake validation, provider ownership, and
  coordinated shutdown ownership remain separate integration work.

## ADR-0044: TCP Listener Loops Are Explicit Lifecycle Handles

Status: Accepted

Context:
Pekko's TCP transport owns a bound listener and attaches each incoming
connection to inbound stream processing while keeping transport lifecycle below
actor protocols. Kairo now has per-association lane readers, but provider
integration needs an explicit owner for a bound listener and the reader handles
it creates.

Decision:
`TcpAssociationListener::spawn_accept_loop` moves a bound listener into a
background accept loop. The loop accepts complete control, ordinary, and large
lane associations, starts independent lane readers for each accepted
association, and exits when its `TcpAssociationListenerHandle` is stopped.
Joining the handle waits for the listener and all reader handles, then returns
a report with accepted-association and stream/frame counts.

Consequences:
- The TCP listener lifecycle is explicit state that actor-system remote
  provider wiring can own later.
- The TCP layer still does not deserialize messages, resolve actors, or make
  membership decisions; it only feeds frame handlers.
- Reader restart policy and coordinated shutdown integration remain separate
  work.

## ADR-0045: TCP Actor-System Runtime Localizes Canonical Recipients

Status: Accepted

Context:
Pekko's remote actor ref provider resolves paths whose address belongs to the
local provider through the local actor tree, while foreign addresses produce
remote refs or missing refs. Kairo's TCP loopback runtime now sends messages
to remote-addressed actor paths such as
`kairo://receiver@127.0.0.1:port/user/target`, but local actor registries are
keyed by local paths such as `kairo://receiver/user/target`.

Decision:
`LocalActorInboundDelivery` can be constructed with `RemoteSettings`. When the
recipient wire data matches the local actor system protocol/name plus the
configured canonical host and port, the delivery adapter normalizes that
recipient to the local actor path before resolving it in the actor registry.
The canonical-address matching logic lives in a focused local-address module.
Foreign hosts, ports, protocols, or systems are not localized.

`TcpRemoteActorSystem<M>` is the first concrete lifecycle owner for a
message-protocol-specific TCP remote runtime. It binds the listener, installs
the inbound router and remote death-watch actor, exposes the provider/dialer,
and clears all cached outbound association routes during shutdown so cloned
typed remote refs cannot keep socket lanes open after the runtime stops.

Consequences:
- Loopback TCP `RemoteActorRef<M>` delivery can reach typed local actors
  without requiring local actor registries to store remote-addressed aliases.
- Address ownership remains explicit and does not turn discovery or TCP
  connection state into cluster membership truth.
- TCP shutdown has deterministic ownership of cached outbound socket lanes,
  but reconnect/backoff policy and richer provider supervision remain future
  work.

## ADR-0046: TCP Lane Streams Start With Association Handshakes

Status: Accepted

Context:
Pekko Artery sends handshake requests before ordinary user traffic is accepted.
Inbound handshakes validate that the request is addressed to the local node,
complete the association with the remote unique address, and reject or drop
traffic from unknown origins. Kairo does not yet implement the full Artery
UID/quarantine state machine in the TCP transport, but its concrete TCP lanes
still need a stable association identity before framed messages are delivered.

Decision:
Kairo adds a focused TCP handshake module under `kairo-remote/src/tcp`. A
handshake is written before the regular stream header on each concrete TCP
lane when a `TcpAssociationDialer` has a configured local association address.
The handshake carries:

- explicit local and remote `RemoteAssociationAddress` values,
- the sender system UID, matching Pekko's use of `UniqueAddress` as the
  source identity in `HandshakeReq`,
- the lane id for the stream,
- a fixed magic/version prefix plus length-prefixed stable wire payloads.

`TcpAssociationListener` can be configured with its local association address.
In that mode it reads one handshake from each accepted lane before handing the
stream to frame readers, rejects handshakes addressed to another local node,
rejects mixed remote addresses or UIDs, and rejects duplicate lane ids. Raw
listener tests may still omit the local address to exercise stream framing
without the association handshake layer.

Accepted handshaken associations keep their remote identity on
`TcpAcceptedAssociation`, and listener-loop shutdown reports collect those
identities. This mirrors Pekko's association registry direction without yet
implementing the full UID-indexed registry or quarantine state machine.

Consequences:
- `TcpRemoteActorSystem` now validates the peer and target address before
  delivering normal remote envelope frames.
- TCP listener reports expose explicit peer incarnation evidence for future
  diagnostics, association registry indexing, and quarantine decisions.
- The handshake format does not rely on Rust type names, enum discriminants,
  or memory layout.
- Quarantine after UID changes, retry/backoff, and reliable system-message
  delivery remain separate remote milestones.

## ADR-0047: Remote Associations Keep A Separate UID Registry

Status: Accepted

Context:
Pekko Artery keeps associations indexed by both remote address and remote UID.
Completing a handshake records the remote `UniqueAddress`; repeated completion
for the same address and UID is idempotent, while attempting to associate the
same UID with a different remote address is rejected as a UID collision. Kairo
needs the same observable incarnation boundary before quarantine and reconnect
policy are layered on top.

Decision:
Kairo adds `RemoteAssociationRegistry` as a focused `kairo-remote` module. The
registry owns address-indexed `RemoteAssociation` handles plus a UID-to-address
index. `association(address)` creates or reuses the address association and
starts its handshake state. `complete_handshake(address, uid)` indexes the UID,
activates the association with that UID, allows repeated handshakes for the
same address/UID pair, allows a later UID for the same address as a new
incarnation, and rejects a UID collision across different addresses with an
explicit `RemoteError::AssociationUidCollision`.

TCP listeners can be configured with this registry. After lane handshakes are
validated for local target, remote identity consistency, and lane uniqueness,
the listener completes the association in the registry before handing streams
to lane readers. `TcpRemoteActorSystem` owns one registry alongside its
association cache and exposes it for diagnostics and later quarantine work.

The same listener may be configured with a `RemoteAssociationRouteInstaller`.
For a validated handshaken association, the listener clones the accepted
control, ordinary, and large lane streams into reverse `TcpRemoteByteSink`
values and installs an outbound route for the remote association address. This
keeps inbound TCP lane ownership and reverse outbound route installation in
one transport module while preserving the transport-neutral association cache
boundary.

Consequences:
- Association identity state is not hidden inside TCP socket code or the route
  cache.
- Accepted TCP sockets can become bidirectional association routes for replies
  and remote system traffic without requiring a second outbound dial first.
- Future quarantine/reconnect logic has a stable address and UID index to
  build on.
- The registry does not turn TCP connections into cluster membership truth;
  cluster membership remains gossip plus local failure detector observations.

## ADR-0048: Provider Resolution Uses A Typed Local-Or-Remote Ref

Status: Accepted

Context:
Pekko's `RemoteActorRefProvider` resolves actor paths owned by the local
provider through the local actor registry and creates a `RemoteActorRef` only
for foreign addresses. Missing owned paths become empty/dead-letter refs that
preserve the requested path. Kairo needs the same observable provider behavior,
but it should keep the Rust typed-ref boundary rather than introduce an erased
message API.

Decision:
Kairo adds `ResolvedActorRef<M>` as a focused `kairo-remote` module. It wraps
either a local `ActorRef<M>` or a `RemoteActorRef<M>` and implements the typed
`Recipient<M>` send boundary. `RemoteActorRefProvider::resolve_actor_ref`
returns this enum when the provider is configured with an `ActorSystem`.
Local-only actor paths and canonical remote paths owned by the local system are
resolved through the local actor registry. Unknown owned paths return missing
local refs that keep the normalized local path and publish dead letters on
send. Foreign paths still resolve to `RemoteActorRef<M>`.

The existing remote-only `resolve` API remains available for call sites that
specifically require a `RemoteActorRef<M>`, such as explicit outbound TCP
association tests.

Consequences:
- Provider-level location transparency no longer requires callers to know
  whether an owned canonical address should be local.
- The local-or-remote boundary stays typed by `M`; no global message enum or
  erased user protocol is introduced.
- Remote deployment remains out of scope. Resolving a foreign path creates a
  remote ref to an existing remote actor path.

## ADR-0049: TCP Associations Own Bidirectional Lane Readers

Status: Accepted

Context:
Pekko remoting treats an association as the bidirectional communication
boundary between two actor systems. Kairo's first TCP slices installed outbound
routes on the dialing side and read lanes on the accepting side, which was
enough for one-way message tests but could not carry remote death-watch
heartbeat acknowledgements or replies back over the same association.

Decision:
Kairo keeps the existing explicit control, ordinary, and large lane streams,
but both sides now own read and write handles for a completed handshaken TCP
association. The listener validates the handshakes, completes the association
registry entry, clones each accepted lane stream into a `TcpRemoteByteSink`,
and installs a reverse route to the remote identity before moving the original
streams into lane reader threads. The dialer adds `dial_with_reader`, which
connects the three lanes, clones them into outbound byte sinks, and spawns
dialing-side reader handles for frames written by the accepting peer.

`TcpRemoteActorSystem` uses this bidirectional dialing path and stores the
dialing-side reader handles. Runtime shutdown clears the route cache, causing
the byte sinks to shut down their sockets, then joins those reader handles
before joining the listener.

Consequences:
- A single handshaken TCP association can now carry typed remote messages in
  both directions.
- Reverse routes are still explicit cache entries and are not cluster
  membership evidence.
- Reader supervision is now covered by `TcpAssociationReaderSupervisor` and
  ADR-0092. Outbound reconnect/backoff policy remains a separate runtime
  concern from this bidirectional lane ownership decision.

## ADR-0050: Inbound Remote Watch Is Separate From Outbound Watch Intent

Status: Accepted

Context:
Pekko's `RemoteWatcher` receives `WatchRemote` from the local provider when a
local actor starts watching a remote watchee. That local command records the
outbound watch, starts heartbeat monitoring for the remote address, and sends
the actual remote system watch toward the watchee. On the watched node, the
remote system watch is installed against the local watchee; it is not treated
as a new local request to watch the sender's node.

Kairo's first remote death-watch wire slice reused the same `WatchRemote`
command for both outbound watch intent and decoded inbound wire messages. Once
TCP associations became bidirectional, an inbound watch could therefore echo a
new outbound watch back through the local remote watcher and try to monitor the
wrong address.

Decision:
Kairo keeps outbound `Watch` and `Unwatch` commands for local watch intent and
adds explicit `InboundWatch` and `InboundUnwatch` commands for decoded remote
death-watch protocol messages. `RemoteDeathWatchState` now stores inbound
remote-watch registrations in a separate map from the outbound watched-address
state. Inbound registrations are idempotent and produce no outbound watch,
heartbeat, or failure-detector effects.

The inbound state is intentionally separate so local actor termination can
drive remote per-watchee termination notifications over the system lane without
touching outbound watch intent. Heartbeat replies still use the existing
remote watcher sender metadata and outbound heartbeat-ack effect path.

Consequences:
- Receiving a wire `WatchRemote` records the remote watcher of a local watchee
  without recursively sending another `WatchRemote` to the peer.
- Outbound heartbeat monitoring remains tied only to local watch intent.
- The split matches Pekko's observable directionality while keeping Kairo's
  Rust implementation as explicit command/state variants instead of Scala
  provider interception.
- Per-watchee remote termination delivery now reuses the inbound watch map
  without changing the outbound heartbeat state machine.

## ADR-0051: Cluster Tools Use A Configured-Peer TCP Runtime

Status: Accepted

Context:
Distributed pubsub and cluster singleton already have transport-neutral remote
envelope adapters and a shared `ClusterToolsSystemInbound<M>` router. After
the TCP association layer became bidirectional, cluster-tools traffic needed a
concrete socket runtime that wires those existing boundaries to live lanes
without making remoting responsible for cluster membership or peer selection.

Decision:
Kairo adds `ClusterToolsTcpAssociationRuntime<M>` in a focused
`kairo-cluster-tools` module. The runtime binds a handshaken TCP listener,
owns a shared `RemoteAssociationCache`, association registry, route installer,
dialer, and dialing-side lane readers, and routes accepted frames into
`ClusterToolsSystemInbound<M>`.

The runtime installs a cluster-tools lane classifier so pubsub gossip,
pubsub publish envelopes, and singleton handover messages all travel on the
control/system lane. A configured peer can be dialed explicitly, and the same
bidirectional TCP association can carry pubsub status/delta, pubsub publish,
and singleton handover traffic in both directions.

Consequences:
- Cluster tools now have a runnable socket-backed vertical slice for one
  configured peer.
- Membership-derived peer planning, reconnectable multi-peer actor ownership,
  timer ticks, and bootstrap lifecycle wiring are layered by later accepted
  cluster-tools TCP ADRs; this configured-peer runtime itself still does not
  read or mutate cluster membership.
- The TCP integration reuses `kairo-remote` association primitives instead of
  adding tool-specific socket code.

## ADR-0052: Cluster Control Traffic Uses A Configured-Peer TCP Runtime

Status: Accepted

Context:
Pekko routes cluster daemon and heartbeat traffic through system actors while
cluster membership itself remains gossip plus local failure-detector
observations. Kairo already had transport-neutral membership and heartbeat
wire adapters plus bidirectional TCP association primitives, but the cluster
crate still needed a concrete socket-backed vertical slice that could carry
join, welcome, gossip, heartbeat, and heartbeat response frames without making
remoting the source of membership truth.

Decision:
Kairo adds a focused `ClusterSystemInbound` router and
`ClusterTcpAssociationRuntime` in `kairo-cluster`. `ClusterSystemInbound`
validates the stable system-recipient path for the local node, then dispatches
membership manifests to `ClusterMembershipWireInbound`, heartbeat requests to
`HeartbeatRemoteReceiverInbound`, and heartbeat responses to
`HeartbeatRemoteResponseInbound`.

`ClusterTcpAssociationRuntime` binds a handshaken TCP listener, owns a shared
`RemoteAssociationCache`, association registry, route installer, dialer, and
dialing-side lane readers, and routes decoded socket frames into
`ClusterSystemInbound`. It installs a cluster lane classifier so `Join`,
`Welcome`, `GossipEnvelope`, `Heartbeat`, and `HeartbeatRsp` travel on the
control/system lane. Peers are dialed explicitly for this slice.

Consequences:
- Cluster control traffic now has a runnable socket-backed vertical slice for
  one configured peer.
- Membership state is still owned by the gossip membership actor; remote
  association routes are delivery paths, not cluster membership evidence.
- Membership-derived peer planning, reconnect/backoff policy, actor-backed
  multi-peer ownership, timer ticks, and bootstrap lifecycle wiring are layered
  by the later accepted cluster TCP peer ADRs without changing this inbound
  routing boundary.

## ADR-0053: Cluster Association Peers Are Planned From Membership Events

Status: Accepted

Context:
The configured-peer cluster TCP runtime can carry membership and heartbeat
traffic once a peer is explicitly dialed. The next integration step needs
cluster-derived peer discovery without making remoting or socket associations
the source of cluster membership truth. Pekko's cluster daemon only gossips to
valid peers: never self, and not nodes marked unreachable by the local node;
unreachability observations from other nodes do not by themselves stop gossip
attempts from this node.

Decision:
Kairo adds `ClusterAssociationPeerState` as a pure planner in
`kairo-cluster`. It consumes `CurrentClusterState` snapshots and
`ClusterEvent` updates, keeps membership-derived peer state separate from
remote association state, rejects non-self local-only peer addresses, and emits
explicit `Dial` and `Remove` effects with stable `RemoteAssociationAddress`
targets.

The planner follows Pekko's local-observer reachability rule. It removes a peer
when the local node marks that peer unreachable or terminated, redials when the
peer becomes reachable again, and preserves active peers when only another
observer reports unreachability.

Consequences:
- Cluster-derived peer discovery is now deterministic and testable without
  owning sockets or mutating cluster membership.
- TCP peer-route ownership consumes the planner effects instead of inferring
  peers from ad hoc route-table state.
- Reconnect/backoff policy and actor-system lifecycle ownership remain
  separate work.

## ADR-0054: Cluster TCP Peer Routes Apply Membership Plans Explicitly

Status: Accepted

Context:
`ClusterAssociationPeerState` produces deterministic dial/remove plans from
cluster membership and local reachability, while `ClusterTcpAssociationRuntime`
owns concrete TCP listeners, dialers, and association cache routes. Kairo needs
an integration layer between those two pieces that owns route registrations
without treating the route table as membership state.

Decision:
Kairo adds `ClusterTcpPeerRoutes` in `kairo-cluster`. It consumes
`ClusterAssociationPeerChange` values, dials peers through
`ClusterTcpAssociationRuntime`, records one route registration per peer
identity, and removes plus closes cached association routes when a peer is
removed by the planner.

The route owner does not subscribe to cluster events itself and does not keep
membership snapshots. It applies already-derived plans so future actor-system
or cluster-daemon wiring can decide when snapshots/events are fed into the
planner and when reconnect/backoff policies retry failed dials.

Consequences:
- Cluster-derived peer plans can now affect live TCP association routes in a
  tested vertical slice.
- Membership state, peer planning, and socket route ownership remain separate
  modules.
- Reconnect/backoff policy and long-lived actor ownership are layered by the
  later accepted cluster TCP runtime, connector, timer, and bootstrap ADRs
  without changing this route-ownership boundary.

## ADR-0055: Cluster TCP Peer Runtime Owns The Route Lifecycle

Status: Accepted

Context:
The cluster crate had the three pieces needed for membership-derived TCP
routes: `ClusterTcpAssociationRuntime` owned live sockets,
`ClusterAssociationPeerState` planned dial/remove effects from gossip
membership and local reachability, and `ClusterTcpPeerRoutes` applied those
effects to route registrations. The next vertical slice needed a lifecycle
owner that could accept cluster snapshots/events without collapsing those
responsibilities into one file or letting the route table become membership
truth.

Decision:
Kairo adds `ClusterTcpPeerRuntime` in a focused `tcp_peer_runtime` module. It
owns a `ClusterTcpAssociationRuntime`, a `ClusterAssociationPeerState`, and a
`ClusterTcpPeerRoutes` value. Snapshot and event methods feed the planner,
then apply the resulting changes to live TCP routes. Shutdown clears active
peer routes before stopping the underlying TCP listener.

Consequences:
- Cluster membership snapshots and events can now drive live TCP association
  routes through one explicit lifecycle boundary.
- Membership state, peer planning, socket routing, and socket transport remain
  structured modules rather than being concentrated in the crate root.
- Reconnect/backoff policy, long-lived actor ownership, and actor-system
  lifecycle integration are layered by the later accepted cluster TCP
  reconnect, connector, timer, and bootstrap ADRs.

## ADR-0056: Cluster TCP Peer Reconnects Are Deterministic Retry State

Status: Accepted

Context:
Pekko keeps cluster gossip moving through periodic ticks, while remoting and
Artery use explicit handshake/restart retry intervals for failed outbound
links. Kairo's `ClusterTcpPeerRuntime` could apply membership-derived peer
routes, but a failed dial left the peer desired by membership without a
structured way to retry later.

Decision:
Kairo adds `ClusterTcpPeerReconnectState` in a focused module. Dial failures
record a pending retry for the peer identity, the next retry time is computed
from an explicit retry interval, and `ClusterTcpPeerRuntime` exposes a
tick-style `retry_due_peer_routes` method. Successful dials and skipped
already-active routes clear retry state, and membership removal or local
unreachability cancels obsolete retry attempts.

Consequences:
- Failed cluster peer dials can recover when the peer listener becomes
  available without treating the remote association cache as membership truth.
- Retry timing is deterministic and testable; no sleeping retry thread or
  broad dependency is introduced.
- Periodic timer cadence is layered separately on the actor-backed connector.

## ADR-0057: Cluster TCP Peer Connector Bridges Events To Runtime

Status: Accepted

Context:
`ClusterTcpPeerRuntime` can apply cluster snapshots and events to live TCP
association routes, and `ClusterTcpPeerReconnectState` can retain deterministic
retry intent after failed dials. The remaining local wiring gap was an
actor-backed boundary that subscribes to the cluster event stream and owns the
runtime lifecycle without moving membership truth into remoting.

Decision:
Kairo adds `ClusterTcpPeerConnector` in a focused module. The connector actor
subscribes to `Cluster` with an initial snapshot, adapts
`ClusterSubscriptionEvent` messages into its own protocol, forwards snapshots
and events into `ClusterTcpPeerRuntime`, and records the latest route report or
error for deterministic tests and diagnostics. It also accepts explicit
`RetryDuePeerRoutes` messages with a caller-provided monotonic timestamp so
tests and future schedulers can drive reconnect attempts without sleeping.

When the connector stops, it unsubscribes from cluster events and shuts down
the owned TCP peer runtime. The connector does not inspect the association
cache to infer membership and does not mutate gossip state.

Consequences:
- Cluster membership events can now drive multi-peer TCP route ownership from
  an actor mailbox turn.
- Retry attempts are actor-addressable and deterministic; the connector can
  also drive them from actor timers without adding a sleeping retry thread.
- Membership state, peer planning, socket route ownership, reconnect state,
  and actor lifecycle wiring remain separate modules.

## ADR-0058: Cluster TCP Peer Retries Use Actor Timers

Status: Accepted

Context:
`ClusterTcpPeerConnector` could drive reconnect attempts through explicit
`RetryDuePeerRoutes` messages, which kept tests deterministic but still left
periodic retry cadence to callers. Pekko keeps distributed cluster work moving
through scheduled actor ticks. Kairo should do the same without introducing a
background retry thread or making transport state authoritative for
membership.

Decision:
Kairo adds `ClusterTcpPeerConnectorSettings` with an explicit non-zero retry
interval, initial retry delay, and an automatic-ticks switch. When automatic
ticks are enabled, `ClusterTcpPeerConnector` starts a fixed-delay actor timer
and feeds due retry timestamps back into `ClusterTcpPeerRuntime`. Tests can
still disable automatic ticks and send explicit retry messages, or use
manual-time actor systems to advance the timer deterministically.

Consequences:
- Cluster TCP peer reconnects can run from actor timers in normal runtimes and
  from manual time in tests.
- Retry scheduling remains actor-owned and deterministic, with no broad async
  runtime or sleeping retry worker.
- The retry timer only drives desired peers retained by membership-derived
  state; it does not infer membership from association cache routes.

## ADR-0059: Cluster-Tools TCP Peer Routes Apply Membership Plans Explicitly

Status: Accepted

Context:
`ClusterToolsTcpAssociationRuntime<M>` can carry pubsub gossip, pubsub publish
delivery, and singleton handover messages over bidirectional TCP associations,
but it was still configured by directly dialing a concrete peer. Pekko's
cluster-tools actors subscribe to cluster membership and keep peer selection
separate from transport state. Kairo already has a cluster membership-derived
peer planner in `kairo-cluster`; cluster-tools needs a route owner that can
consume those plans without making its association cache a membership source.

Decision:
Kairo adds `ClusterToolsTcpPeerRoutes` in a focused module. It consumes
`ClusterAssociationPeerChange` values, dials peers through
`ClusterToolsTcpAssociationRuntime<M>`, records one route registration per
peer identity, and closes plus removes cached routes when a peer is removed by
membership or local reachability.

Consequences:
- Cluster-tools TCP routes can now be driven by cluster membership-derived
  dial/remove plans in a tested vertical slice.
- Pubsub/singleton membership state, peer planning, and socket route ownership
  remain separate from the TCP runtime and association cache.
- Reconnect policy and actor-backed multi-peer runtime ownership are layered in
  later accepted cluster-tools TCP ADRs without changing this route ownership
  boundary.

## ADR-0060: Cluster-Tools TCP Peer Runtime Owns Reconnectable Routes

Status: Accepted

Context:
`ClusterToolsTcpPeerRoutes` can apply membership-derived dial/remove plans to
the socket runtime, but callers still had to combine membership planning, route
application, retry state, and shutdown cleanup themselves. Pekko's
cluster-tools pubsub mediator and singleton components consume cluster events
and scheduled ticks from actor turns, while keeping transport associations as
delivery paths rather than membership evidence.

Decision:
Kairo adds `ClusterToolsTcpPeerRuntime<M>` in a focused module. It owns
`ClusterToolsTcpAssociationRuntime<M>`, `ClusterAssociationPeerState`,
`ClusterToolsTcpPeerRoutes`, and `ClusterToolsTcpPeerReconnectState` from a
separate reconnect module. Snapshot and event methods feed the
membership-derived planner and apply the resulting route effects; failed dials
record pending retries; explicit retry ticks attempt due peers; successful or
removed peers clear retry state. Shutdown clears pending retries and active
peer routes before stopping the listener.

Consequences:
- Cluster-tools pubsub/singleton socket routes now have a reconnectable
  lifecycle boundary that is driven by the actor-backed connector layered in
  the next ADR.
- Reconnect timing remains deterministic and testable; no sleeping retry
  thread or central membership store is introduced.
- Actor-backed automatic retry ticks and cluster subscription ownership are
  layered by `ClusterToolsTcpPeerConnector<M>`.

## ADR-0061: Cluster-Tools TCP Connector Owns Actor-Backed Subscription Ticks

Status: Accepted

Context:
After `ClusterToolsTcpPeerRuntime<M>` owned route and reconnect state, callers
still needed to manually subscribe to cluster events and schedule retry ticks.
Pekko cluster-tools components are actor-owned and react to cluster snapshots,
member/reachability events, and scheduled ticks within actor turns.

Decision:
Kairo adds `ClusterToolsTcpPeerConnector<M>` in its own module. The connector
subscribes to cluster snapshots/events through a message adapter, feeds those
events into the cluster-tools TCP peer runtime, exposes a typed snapshot for
diagnostics/tests, supports explicit retry ticks, and can schedule fixed-delay
retry ticks with actor timers. Stopping the connector unsubscribes from the
cluster event stream and shuts down the owned peer runtime.

Consequences:
- Cluster-tools pubsub/singleton TCP routes now have actor-backed membership
  subscription and timer ownership without turning TCP associations into
  membership truth.
- Retry behavior remains deterministic in tests through explicit messages or
  manual-time driven actor timers.
- Runtime binding, connector spawning, and coordinated shutdown are layered on
  top of the connector by `ClusterToolsTcpPeerBootstrap<M>`.

## ADR-0062: Cluster-Tools TCP Bootstrap Registers Coordinated Shutdown

Status: Accepted

Context:
`ClusterToolsTcpPeerRuntime<M>` and `ClusterToolsTcpPeerConnector<M>` provided
the socket lifecycle and actor-backed cluster subscription boundary, but users
still had to wire runtime binding, connector spawning, and coordinated shutdown
manually. Pekko cluster-tools extensions own their system actors and add
shutdown hooks where needed instead of requiring every caller to duplicate that
orchestration.

Decision:
Kairo adds `ClusterToolsTcpPeerBootstrap<M>` in a focused module. The bootstrap
binds the cluster-tools TCP peer runtime from explicit `RemoteSettings`, spawns
the connector with explicit connector settings, exposes the connector ref,
self node, and local association address, and registers an actor-termination
task with coordinated shutdown. The default task runs in
`before-cluster-shutdown`, and callers can override connector name, connector
settings, shutdown phase, task name, and timeout through a Rust builder.

Consequences:
- Cluster-tools socket integration has one reusable top-level lifecycle
  boundary without making transport associations membership authority.
- Coordinated shutdown closes the connector-owned runtime through the actor
  stop path, preserving the same cleanup behavior as explicit actor stop.
- The runnable cluster-tools TCP example and multi-node smoke tests exercise
  this bootstrap path; remaining M13 work is broader release hardening rather
  than missing lifecycle wiring.

## ADR-0063: Distributed-Data TCP Peer Routes Stay Membership-Derived

Status: Accepted

Context:
`ReplicatorTcpAssociationRuntime` can bind and dial concrete ddata socket
associations, but distributed-data still needed a reusable owner for
cluster-derived peer routes. Pekko's replicator derives remote peers from
cluster member and reachability events; socket associations are delivery paths,
not membership truth.

Decision:
Kairo adds `ReplicatorTcpPeerRoutes` in a focused module. It consumes
`ClusterAssociationPeerChange` values produced by the shared cluster peer
planner, dials routes through `ReplicatorTcpAssociationRuntime`, tracks
per-peer route registrations, closes the guarded association when removing a
known route, and removes cached routes when peers leave or become locally
unreachable. The ddata TCP runtime exposes a narrow `remove_route` method so
route ownership does not manipulate cache internals directly.

Consequences:
- Distributed-data gets the first multi-peer socket route ownership boundary
  while preserving gossip/reachability as the only membership source.
- Later reconnect state, runtime lifecycle, actor connector, and bootstrap
  modules reuse this route owner instead of manipulating association caches
  directly.
- Reconnect policy and actor-backed multi-peer runtime ownership are layered in
  later accepted ADRs without changing this route ownership boundary.

## ADR-0064: Distributed-Data TCP Reconnect Policy Is Pure State

Status: Accepted

Context:
Distributed-data TCP peer routes need retry behavior when a cluster-derived
peer is reachable according to membership but the socket dial is not yet
available. Pekko keeps membership and reachability as cluster facts while
transport availability is a local delivery concern.

Decision:
Kairo models distributed-data TCP peer retries with
`ReplicatorTcpPeerReconnectState`, a focused pure state machine. It validates a
non-zero retry interval, records failed peer targets with attempt counts and
deterministic next-retry times, exposes due targets for later runtime/actor
drivers, and clears pending retries when a route succeeds or the peer is
removed.

Consequences:
- Retry policy can be tested without sockets, actors, or sleeping.
- Runtime and connector layers compose route ownership and retry state without
  making retry state another source of cluster membership truth.
- Actor-backed connector wiring and coordinated-shutdown ownership are layered
  by later distributed-data TCP ADRs.

## ADR-0065: Distributed-Data TCP Peer Runtime Owns Route Lifecycle

Status: Accepted

Context:
Distributed-data had separate pieces for socket associations, cluster-derived
route changes, and retry state. The next runtime boundary needs to compose
those pieces without moving cluster membership truth into the socket layer.

Decision:
Kairo adds `ReplicatorTcpPeerRuntime` as a focused owner for distributed-data
TCP peer lifecycle. It binds the configured `/system/ddata` TCP association
runtime, derives its local `UniqueAddress`, applies cluster snapshots and
events through `ClusterAssociationPeerState`, applies route changes through
`ReplicatorTcpPeerRoutes`, records failed dials through
`ReplicatorTcpPeerReconnectState`, retries due targets when driven by explicit
time, and clears routes/retries before listener shutdown.

Consequences:
- Distributed-data has the same local route/reconnect ownership shape as
  cluster-tools while staying in a separate module and crate surface.
- Tests can cover success, failed dial retry, peer removal, and shutdown
  cleanup without actor connector wiring.
- Actor-backed cluster subscription and coordinated-shutdown bootstrap are
  layered by the connector and bootstrap ADRs that follow.

## ADR-0066: Distributed-Data TCP Peer Connector Is Actor-Driven

Status: Accepted

Context:
Distributed-data TCP peer routing now has pure route, reconnect, and runtime
owners, but it still needed the actor boundary that subscribes to cluster
events and drives retries through actor timers. Pekko's replicator keeps
membership/reachability updates actor-driven and treats transport as local
delivery state.

Decision:
Kairo adds `ReplicatorTcpPeerConnector` as a focused actor module. The
connector subscribes to `Cluster` with an initial snapshot, forwards cluster
snapshots/events into `ReplicatorTcpPeerRuntime`, drives retries through
explicit messages or fixed-delay actor timers, exposes a typed snapshot for
tests and diagnostics, unsubscribes when stopped, and shuts down the owned peer
runtime from the actor stop path. Connector tests use `kairo-testkit` as a
dev-dependency to verify cluster subscription, retry, route removal, and
manual-time timer behavior.

Consequences:
- Distributed-data socket peer ownership can now run from actor turns instead
  of only direct method calls.
- Membership and reachability remain cluster-derived; the connector does not
  invent a socket-backed membership source.
- Runtime bootstrap and coordinated-shutdown registration are layered by the
  bootstrap ADR that follows.

## ADR-0067: Distributed-Data TCP Bootstrap Owns Shutdown Registration

Status: Accepted

Context:
Distributed-data TCP peer lifecycle can now run through an actor-backed
connector, but users still need one facade that binds the runtime, spawns the
connector, and wires coordinated shutdown. Cluster-tools already uses this
layering so socket cleanup follows actor stop semantics.

Decision:
Kairo adds `ReplicatorTcpPeerBootstrap` with explicit identity and settings
structs. The bootstrap binds `ReplicatorTcpPeerRuntime`, spawns
`ReplicatorTcpPeerConnector` under a configured actor name, records the local
node/address for callers, and registers an actor termination task in
`PHASE_BEFORE_CLUSTER_SHUTDOWN` by default. Bootstrap settings own the remote
runtime settings, connector settings, shutdown phase/task name, and timeout so
the constructor stays explicit without long argument lists.

Consequences:
- Distributed-data gets a single entry point for socket peer runtime and
  connector startup while keeping each responsibility in a focused module.
- Coordinated shutdown uses the same connector stop path as explicit actor
  stop, so route cleanup and runtime shutdown remain centralized.
- The runnable distributed-data TCP example and multi-node smoke tests exercise
  this bootstrap path; remaining M13 work is broader release hardening rather
  than missing lifecycle wiring.

## ADR-0068: Receive Timeout Uses Cloneable Typed Timeout Messages

Status: Accepted

Context:
Pekko typed `setReceiveTimeout` schedules a protocol message after actor
inactivity, cancels the pending timeout before an influencing message is
processed, and reschedules it afterward. Kairo must preserve that mailbox
reentry behavior without untyped marker messages or borrowing actor state
outside a synchronous receive turn.

Decision:
Kairo adds focused receive-timeout state to `kairo-actor`. `Context` stores a
timeout duration, a cloneable typed timeout message factory, generation
metadata, and a cancellable scheduler task. Timeout tasks enqueue typed
receive-timeout envelopes, and the actor turn accepts only the current
generation so cancelled or reset timeout messages already in the mailbox are
discarded before user `receive`. `Context::set_receive_timeout` requires the
timeout message to be `Clone`, which is the Rust ownership replacement for
Pekko's immutable object reference reuse.

Consequences:
- Receive timeouts remain local typed messages and require no serialization.
- Actor state still changes only during a later mailbox turn.
- Reset and cancellation semantics do not rely on racing scheduler task
  cancellation alone.
- Message types that cannot be cloned can still model idle behavior with an
  explicit timer or a small cloneable timeout command that carries a reply
  handle or key.

## ADR-0069: Failed Child Watch Uses A Typed Signal Variant

Status: Accepted

Context:
Pekko typed `ChildFailed` is a specialized `Terminated` signal delivered when
a watched direct child terminates due to failure. Other watchers observe normal
termination, and `watchWith` delivers the caller-provided protocol message
instead of exposing the failure cause.

Decision:
Kairo extends `Signal` with `ChildFailed { actor, reason }`. Local actor
failure stop paths carry the failure reason into the death-watch registry.
Signal-based watch registrations compare the watcher path with the terminated
actor's direct parent path: the parent receives `ChildFailed` when the subject
stopped from failure, while non-parent signal watchers receive
`Terminated`. Custom `watch_with` registrations ignore the failure cause and
send the registered typed message.

Consequences:
- Parent-child failure observation matches Pekko's observable typed
  lifecycle behavior without copying Scala's signal class inheritance.
- Non-parent watchers and custom watch messages keep their previous
  termination behavior.
- Failure reasons are explicit strings because Kairo actor failures are
  represented by `ActorError`, not JVM `Throwable` instances.

## ADR-0070: Probe Death Watch Uses Typed System Watch Messages

Status: Accepted

Context:
Pekko's typed test probe can watch an actor and assert that a matching
termination signal is observed. Kairo's `TestProbe<M>` is intentionally a
typed message receiver rather than a special untyped actor, so lifecycle
assertions need to preserve the typed boundary without adding a broad dynamic
probe protocol.

Decision:
Kairo exposes `ActorSystem::watch_with` as a typed public hook for harnesses
and infrastructure code that own an `ActorRef<M>` but are not inside an actor
`Context`. `TestProbe<M>::watch_with` registers a caller-provided typed
message, and the specialized `TestProbe<AnyActorRef>` adds
`watch_terminated` and `expect_terminated` helpers that encode termination as
the watched actor's `AnyActorRef`.

Consequences:
- Probe lifecycle assertions reuse the same local death-watch registry as
  actor-context `watch_with`.
- Testkit remains structured around typed probe messages rather than a
  catch-all dynamic event queue.
- External code still cannot observe local actor state transitions without an
  explicit watched actor ref and explicit typed notification message.

## ADR-0071: Await Assertions Retry Result Values Instead Of Panics

Status: Accepted

Context:
Pekko's typed testkit `awaitAssert` retries a by-name assertion, catching
non-fatal exceptions until the assertion succeeds or the timeout expires.
Rust test assertions usually panic, but panic catching imposes unwind-safety
constraints and is a poor default API for deterministic actor tests.

Decision:
Kairo adds `kairo-testkit::await_assert` as a focused polling helper that
retries a `FnMut() -> Result<T, E>` until it returns `Ok(T)` or the timeout
expires. The timeout error preserves the attempt count, elapsed time, and last
error value. Zero retry intervals yield the thread instead of sleeping, and
zero maximum duration still evaluates the assertion once.

Consequences:
- Tests can express eventually true conditions without relying on panic
  recovery.
- The helper preserves Pekko's polling behavior while using Rust's explicit
  `Result` contract.
- Callers that want panic-style assertions can wrap them in their own
  `catch_unwind` boundary, but the testkit default remains explicit and typed.

## ADR-0072: Probe Message Fishing Uses Borrowed Classification

Status: Accepted

Context:
Pekko's typed `fishForMessage` consumes probe messages under one deadline and
classifies each message as complete, fail, continue-and-collect, or
continue-and-ignore. Kairo needs the same deterministic testing behavior while
preserving Rust ownership of typed messages and avoiding a dynamic probe event
queue.

Decision:
Kairo adds a focused `fishing` module with `FishingOutcome`.
`TestProbe::fish_for_message` receives typed messages from the probe queue,
passes each message by shared reference to the caller's classifier, and then
either returns collected messages, reports an explicit failure reason, keeps
collecting, or drops ignored messages. The loop uses one overall timeout
deadline rather than restarting the timeout for each received message.

Consequences:
- Tests can inspect typed message streams without cloning or requiring `Debug`
  for successful fishing.
- Ignored messages are intentionally consumed, matching the probe-draining
  behavior of Pekko's fishing API.
- Timeout diagnostics report the number of collected messages instead of
  stringifying arbitrary typed messages.

## ADR-0073: Probe Fixed-Count Receive Uses One Deadline

Status: Accepted

Context:
Pekko's typed `receiveMessages` waits for a requested number of probe messages
under one overall timeout and reports how many messages arrived before the
deadline. Kairo needs this deterministic batch assertion without weakening the
typed probe message boundary.

Decision:
Kairo adds `TestProbe::receive_messages(count, timeout)`. The method drains up
to `count` typed messages from the probe queue using one deadline shared by
the whole batch. A count of zero returns an empty vector without touching the
queue. If the deadline expires first, the method returns
`ProbeError::ReceiveMessagesTimeout` with the requested and received counts.

Consequences:
- Batch probe assertions preserve send order and match Pekko's shared-deadline
  behavior.
- The API returns owned typed messages without requiring cloning or dynamic
  downcasting.
- Timeout diagnostics avoid stringifying arbitrary messages and instead report
  objective batch progress.

## ADR-0074: Manual-Time No-Message Checks Allow Mailbox Settlement

Status: Accepted

Context:
Pekko's manual time helper advances the explicit scheduler and then calls
`expectNoMessage(Duration.Zero)` on each probe. In Kairo, manual scheduled
actions enqueue messages into normal actor mailboxes, and the probe actor moves
those messages into the probe queue on the dispatcher thread.

Decision:
Kairo adds `ManualTime::expect_no_msg_for(duration, probes)` in the focused
manual-time module. The helper advances manual time and then checks each
same-typed probe with a short real-time settle window so due scheduled messages
can complete the mailbox-to-probe hop before the no-message assertion passes.
The Rust API accepts a slice of same-typed probes instead of Pekko's dynamic
varargs.

Consequences:
- Due scheduled probe messages fail the no-message assertion deterministically
  instead of racing the dispatcher thread.
- Manual time remains explicit for scheduler advancement while actor mailbox
  delivery continues to use the normal runtime path.
- Heterogeneous probe groups can call the helper once per message type without
  introducing an untyped testkit queue.

## ADR-0075: Cluster TCP Peer Bootstrap Owns Connector Shutdown

Status: Accepted

Context:
Cluster TCP membership routing now has separate modules for live socket
associations, membership-derived peer planning, peer route ownership,
reconnect state, and the actor-backed connector. Distributed-data and
cluster-tools already expose bootstrap facades that bind their peer runtimes,
spawn connector actors, and register coordinated-shutdown tasks. Cluster core
needs the same lifecycle boundary so normal runtime setup does not require
callers to manually preserve the route-owner shutdown ordering.

Decision:
Kairo adds `ClusterTcpPeerBootstrap` in a focused module. It binds a
`ClusterTcpPeerRuntime` from explicit `RemoteSettings` and node/system UIDs,
spawns `ClusterTcpPeerConnector` under a configurable actor name, exposes the
connector ref, self node, and local association address, and registers an
actor-termination task with coordinated shutdown. The default task runs in
`PHASE_BEFORE_CLUSTER_SHUTDOWN`, stopping the connector so its `stopped` hook
clears peer routes and shuts down the owned TCP runtime before later cluster
shutdown phases.

Consequences:
- Cluster TCP lifecycle ownership now matches the distributed-data and
  cluster-tools bootstrap shape.
- Socket route cleanup continues to flow through actor stop semantics instead
  of requiring callers to clear association caches directly.
- Bootstrap still accepts an explicit `ClusterSystemInbound` builder, so
  membership and heartbeat handlers remain focused modules rather than hidden
  global singletons.

## ADR-0076: Sharding Coordinator Discovery Starts As Pure Candidate State

Status: Accepted

Context:
Pekko shard regions track likely coordinator singleton locations from cluster
membership snapshots and member events. The logic is behavior-sensitive:
candidate members are filtered by role/status, sorted by cluster age, and
coordinator movement clears the region's cached coordinator before
registration retries resume. Kairo first needed the membership candidate state
before layering actor-ref and remote-target wiring for discovered coordinator
locations, and folding that state into `ShardRegionActor` would make the region
boundary harder to test.

Decision:
Kairo adds `CoordinatorDiscoveryState` as a focused pure state module in
`kairo-cluster-sharding`. It consumes `CurrentClusterState` and `ClusterEvent`
member changes, keeps only `Up`, `Leaving`, and `Exiting` members matching the
configured required roles, reports oldest-member movement, and computes the
same likely coordinator candidate ordering as Pekko's members-by-age
selection. Kairo starts with explicit required roles rather than a hardcoded
data-center role; future data-center support can supply that role through the
same settings.

Consequences:
- Cluster-event-driven coordinator discovery stays testable independently from
  the local and remote region actor registration paths layered on top of it.
- Sharding keeps coordinator discovery data separate from routing, buffering,
  handoff, and remember-entity state.
- Downed and removed members are dropped from candidate state immediately,
  matching Kairo's explicit candidate set of statuses of interest.

## ADR-0077: Region Discovery Wiring Uses Explicit Coordinator Targets First

Status: Accepted

Context:
Pekko shard regions send registration to actor selections for the likely
coordinator singleton locations computed from cluster membership. Kairo has
typed actor refs and stable remote envelopes; the first wiring layer makes
coordinator targets explicit so local refs and remote wire recipients can share
the same discovery state. The region actor reacts to cluster snapshots/events
through focused discovery messages, keeping membership selection outside its
routing and buffering code while still refreshing the normal registration
boundary when the selected coordinator appears or moves.

Decision:
Kairo adds a focused `RegionCoordinatorDiscovery` bridge in
`kairo-cluster-sharding`. It owns the mapping from discovered coordinator
nodes to explicit local or remote coordinator targets, uses
`CoordinatorDiscoveryState` for membership semantics, and returns registration
plans only when the selected coordinator target changes. Local targets produce
`RegionRegistrationConfig` values, while remote targets produce
`ShardCoordinatorRemoteTarget` plans for the remote coordinator bridge. The
region actor accepts discovery snapshots/events and refreshes its existing
registration boundary from that bridge.

Consequences:
- Region actor code remains focused on applying plans, routing messages, and
  asking a registered coordinator for shard homes.
- Local coordinator refs and remote wire recipients share the same discovery
  path; remote registration is handled by the remote coordinator transport
  state layered after discovery.
- Adding higher-level singleton discovery/bootstrap can reuse this bridge
  without changing the pure discovery state machine or making cluster
  membership authoritative in sharding.

## ADR-0078: Sharding Discovery Subscription Is Owned By A Focused Actor

Status: Accepted

Context:
Pekko shard regions subscribe to cluster member events in `preStart`, process
the initial membership state and later events through the same region receive
loop, and unsubscribe during stop. Kairo's `ShardRegionActor` already has
focused routing, buffering, handoff, registration, and coordinator-discovery
plan application responsibilities. Adding cluster subscription ownership
directly to that actor would blur the region runtime boundary.

Decision:
Kairo adds `ShardRegionDiscoverySubscriber<M>` in a focused sharding module.
The subscriber actor owns the `ClusterSubscriptionEvent` adapter, subscribes
with an initial snapshot, forwards snapshots/events to
`ShardRegionMsg::CoordinatorDiscoverySnapshot` and
`ShardRegionMsg::CoordinatorDiscoveryEvent`, exposes a deterministic snapshot
for tests, and unsubscribes when stopped. The region actor remains the place
where discovery plans are applied and registration is retried.

Consequences:
- Sharding now has an explicit actor-backed owner for the cluster subscription
  that drives coordinator discovery.
- Region actor logic stays structured around region messages rather than
  cluster facade lifecycle details.
- `ShardRegionBootstrap` owns the higher-level construction path that spawns a
  discovery-enabled region alongside its subscriber while keeping local and
  remote coordinator target selection in the focused discovery bridge.

## ADR-0079: Remote Sharding Coordinator Targets Are Wire Recipients

Status: Accepted

Context:
Pekko shard regions register with coordinator actor selections derived from
cluster member addresses. Kairo's local coordinator API is a typed
`ActorRef<ShardCoordinatorMsg<M>>`, but the remote coordinator protocol is
already expressed as stable wire messages such as `Register`,
`RegisterAck`, `GetShardHome`, and `ShardHome`. Treating a remote coordinator
as a local typed coordinator ref would either require serializing the local
enum or hiding the actual wire contract.

Decision:
Kairo adds a focused remote coordinator target module that derives stable
`ActorRefWireData` recipients from discovered `UniqueAddress` values and the
documented `/system/sharding/coordinator` path. Region coordinator discovery
can now select either a local typed coordinator target, which produces a
`RegionRegistrationConfig`, or a remote coordinator target, which produces a
stable wire recipient for the remote registration, shard-home, and shutdown
bridges.

Consequences:
- Remote coordinator discovery advances without relying on Rust enum
  discriminants, type names, or local-only coordinator messages as wire
  contracts.
- The remote registration outbound/reply bridge remains a separate
  transport-facing module that serializes `Register` and correlates
  `RegisterAck` explicitly.
- Local registration behavior remains unchanged for current runnable sharding
  tests.

## ADR-0080: Remote Sharding Coordinator Registration Uses Stable Protocol Messages

Status: Accepted

Context:
Pekko shard regions repeatedly send `Register` to the discovered coordinator
selection and treat `RegisterAck` as the point where the region has an active
coordinator. Kairo has stable sharding system messages and wire recipients for
remote coordinator targets, but local region code still uses typed
`ShardCoordinatorMsg<M>` refs for same-node registration. Serializing that
local enum would make Rust implementation details part of the remote wire
contract.

Decision:
Kairo adds `ShardCoordinatorRemoteRegistrationOutbound` and
`ShardCoordinatorRemoteRegistrationInbound` as a focused transport-neutral
bridge. Outbound registration serializes the stable `Register` protocol
message to the resolved coordinator `ActorRefWireData` recipient and includes
the region's wire ref as sender metadata by default. Inbound registration
validates that replies are addressed to the expected region, deserializes only
stable `RegisterAck` payloads, and returns an explicit decoded acknowledgement
for later region-state integration.

Consequences:
- Remote sharding registration now uses stable manifests, versions, serializer
  ids, and registered codecs instead of local typed coordinator enums.
- Transport concerns stay outside `ShardRegionActor`, and decoded
  acknowledgements can be integrated into region registration state in a
  smaller follow-up slice.
- Remote shard-home request/reply handling remains a separate bridge with its
  own stable protocol tests.

## ADR-0081: Remote Sharding Shard-Home Lookup Uses A Focused Wire Bridge

Status: Accepted

Context:
After registration, Pekko shard regions ask the active coordinator for shard
homes with `GetShardHome` whenever buffered messages need a location, and they
apply `ShardHome` replies to the region's shard-home cache before replaying
buffered messages. Kairo's local region currently asks a typed
`ShardCoordinatorMsg<M>` ref, while the remote coordinator protocol already
has stable `GetShardHome` and `ShardHome` messages.

Decision:
Kairo adds `ShardCoordinatorRemoteHomeOutbound` and
`ShardCoordinatorRemoteHomeInbound` as a focused transport-neutral bridge.
Outbound shard-home lookup serializes stable `GetShardHome` messages to the
resolved coordinator `ActorRefWireData` recipient and uses the region wire ref
as sender metadata by default. Inbound shard-home handling validates replies
are addressed to the expected region, deserializes only stable `ShardHome`
payloads, and returns decoded shard id plus region wire data for later region
routing integration.

Consequences:
- Remote shard-home lookup no longer needs to treat the local
  `ShardCoordinatorMsg<M>` enum as a wire protocol.
- Remote registration and remote shard-home lookup remain separate modules,
  matching their different state transitions in the region flow.
- Mapping decoded remote region wire data into local `RegionId` routing state
  remains an explicit follow-up step instead of being hidden in the codec.

## ADR-0082: Region Remote Coordinator State Owns Decoded Reply Application

Status: Accepted

Context:
Pekko shard regions apply `RegisterAck` by recording the active coordinator
and apply `ShardHome` by updating the region's shard-home cache and replaying
buffered messages. Kairo now decodes these remote messages through stable wire
bridges, but applying them directly in `ShardRegionActor` would mix remote
target validation, wire-ref mapping, and region runtime state transitions in
one actor file.

Decision:
Kairo adds `RegionRemoteCoordinator` as a focused sharding module. It tracks
the selected remote coordinator target, marks matching decoded
`RegisterAck` values as registered, rejects stale acknowledgements for a
different coordinator recipient, and maps decoded `ShardHome` region refs to
`RegionId` values using the stable actor-ref path string. `ShardRegionActor`
consumes the resulting plans and reuses the existing region runtime to record
homes and replay buffered messages.

Consequences:
- Remote reply semantics are integrated into region behavior without
  serializing or exposing the local `ShardCoordinatorMsg<M>` enum as a wire
  contract.
- Remote region identities are stable across nodes because they use explicit
  `ActorRefWireData` paths instead of process-local actor-ref values.
- Outbound registration retry and shard-home requests are driven by
  `RegionRemoteCoordinatorTransport`, keeping remote send orchestration
  separate from decoded reply state.

## ADR-0083: Region Remote Coordinator Sends Compose Stable Bridges

Status: Accepted

Context:
Pekko shard regions send `Register` repeatedly to likely coordinator
locations until an acknowledgement arrives, then send `GetShardHome` for
buffered shards. Kairo already has transport-neutral stable bridges for
remote coordinator registration and shard-home lookup, and region state can
consume decoded replies. The remaining question is where outbound remote
registration and shard-home requests should be driven from.

Decision:
Kairo adds `RegionRemoteCoordinatorTransport` as a focused sharding module
owned by the region actor. It composes
`ShardCoordinatorRemoteRegistrationOutbound` and
`ShardCoordinatorRemoteHomeOutbound`, using the configured region
`ActorRefWireData` as sender metadata. `ShardRegionActor` invokes this
transport when discovery selects a remote coordinator, on registration retry
ticks, and after a matching remote `RegisterAck` when pending buffered shards
need `GetShardHome` requests.

Consequences:
- Region-driven remote coordinator sends use stable sharding protocol messages
  and registered codecs rather than serializing local actor protocol enums.
- The actor retains Pekko's observable retry and buffered-shard request flow,
  while wire construction remains outside the main region actor file.
- A later system inbound router can feed decoded remote envelopes into the
  existing region messages and coordinator actors without changing this
  outbound state boundary.

## ADR-0084: Shard Region System Inbound Routes By Stable Manifest

Status: Accepted

Context:
Remote sharding traffic addressed to the region system path carries different
stable protocol messages: user-routed entity envelopes, coordinator
registration acknowledgements, and shard-home replies. These messages should
enter the same typed region behavior as locally decoded messages, but the
routing decision must remain at the stable manifest boundary rather than
depending on Rust enum variants or type names.

Decision:
Kairo adds `ShardRegionSystemInbound<M>` as a focused region-side inbound
router. It dispatches `RoutedShardEnvelope` to `ShardRegionRemoteInbound<M>`,
`RegisterAck` to `ShardCoordinatorRemoteRegistrationInbound`, and `ShardHome`
to `ShardCoordinatorRemoteHomeInbound`, then sends the decoded
`ShardRegionMsg<M>` to the region actor. Missing handlers, unsupported
manifests, wrong recipients, and actor-send failures are reported explicitly.

Consequences:
- Region-side remote envelope routing is now testable independently of TCP
  listener wiring.
- Stable manifests remain the dispatch contract for sharding system messages.
- Coordinator-side inbound routing remains a separate slice so coordinator
  request/reply semantics can be handled without growing the region router.

## ADR-0085: Shard Coordinator System Inbound Routes Through Actor Turns

Status: Accepted

Context:
Pekko shard coordinators receive `Register` and `GetShardHome` as normal actor
messages. Kairo's remote boundary carries these messages as stable
`RemoteEnvelope` payloads addressed to `/system/sharding/coordinator`, while
the coordinator runtime still owns the registration and allocation state.

Decision:
Kairo adds `ShardCoordinatorSystemInbound<M>` as a focused coordinator-side
inbound router. It validates the coordinator recipient, dispatches by stable
manifest, decodes `Register` and `GetShardHome` through registered codecs, and
sends explicit coordinator actor messages. Remote region identity is tracked in
`CoordinatorRemoteRegions` by stable actor-ref path, and remote `RegisterAck`
or `ShardHome` replies are built by `CoordinatorRemoteReplyTarget`.

Consequences:
- Remote coordinator commands re-enter synchronous coordinator actor turns
  instead of mutating coordinator runtime state from the transport boundary.
- The wire protocol remains stable and independent of local
  `ShardCoordinatorMsg<M>` enum layout or Rust type names.
- Transport-backed remote `HostShard`, handoff, and shard-start
  acknowledgements can be layered on the same remote region identity table.

## ADR-0086: Remote Region Control Uses Stable Envelopes

Status: Accepted

Context:
Pekko coordinators send `HostShard`, `BeginHandOff`, and `HandOff` to shard
regions, and receive `ShardStarted`, `BeginHandOffAck`, and `ShardStopped`
back through actor messages. Kairo's local `HandoffTransport` already models
that sequence for typed local regions, but remote regions need the same control
flow without serializing local `ShardRegionMsg<M>` enum variants.

Decision:
Kairo adds `ShardRegionRemoteControlOutbound<M>` as a focused remote region
target. It implements the existing region recipient boundary for coordinator
control messages by serializing stable sharding protocol commands to
`/system/sharding/region` with coordinator sender metadata. Coordinator system
inbound routing now decodes stable control replies and re-enters coordinator
actor turns; handoff replies are forwarded to active handoff workers by shard
id and stable remote region path.

Consequences:
- Coordinator allocation and rebalance workers can target remote regions
  through the same transport abstraction used for local regions.
- Remote sharding control messages use stable manifests and codecs rather than
  local Rust enum layout, discriminants, or type names.
- Region-side inbound execution of stable remote `HostShard`, `BeginHandOff`,
  and `HandOff` commands is implemented by `ShardRegionSystemInbound` and the
  `ShardRegionRemoteControlReplyTarget`, so stable commands re-enter local
  region actor turns before replies are serialized.

## ADR-0087: Remote Region Control Inbound Re-enters Region Actors

Status: Accepted

Context:
Pekko shard regions handle coordinator control commands inside the region
actor: `HostShard` starts or confirms a local shard and replies
`ShardStarted`, `BeginHandOff` removes shard-home routing and replies
`BeginHandOffAck`, and `HandOff` replies `ShardStopped` immediately when no
local shard is active. Kairo now receives the same commands as stable remote
envelopes at `/system/sharding/region`.

Decision:
Kairo adds `ShardRegionRemoteControlInbound` and
`ShardRegionRemoteControlReplyTarget`. The inbound bridge validates the
recipient, requires coordinator sender metadata, decodes stable control
commands, and sends explicit remote-control messages into `ShardRegionActor`.
The actor reuses the existing region runtime transitions and the reply target
serializes stable `ShardStarted`, `BeginHandOffAck`, or immediate
`ShardStopped` replies.

Consequences:
- Remote control commands now follow normal synchronous region actor turns
  instead of mutating region runtime state at the remote boundary.
- Region system inbound remains a manifest router; codec and reply construction
  live in the focused remote-control bridge.
- Hosted-shard remote `HandOff` completion is handled by the later
  region-side stop-message factory decision.

## ADR-0088: Remote Region HandOff Uses A Local Stop-Message Factory

Status: Accepted

Context:
Pekko's remote coordinator sends `HandOff(shard)` without embedding an entity
stop message. The receiving region and shard already know the configured
`handOffStopMessage` and use it locally while replying `ShardStopped` to the
coordinator after entity stop handling completes. Kairo's sharding wire
contract likewise keeps `HandOff` stable and shard-id-only, while business
messages remain local unless users explicitly register remote codecs for them.

Decision:
Kairo adds `RegionRemoteHandOff<M>` as a focused region-side handoff module.
`ShardRegionActor<M>` can be configured with a stop-message factory and timeout
for remote handoff commands. When stable remote `HandOff` targets a hosted
local shard, the actor creates a fresh local stop message, forwards handoff to
the local shard, observes the resulting `ShardHandOffPlan<M>`, asks for
stopper completion when the shard starts an entity stopper, marks the shard
stopped, and sends a stable `ShardStopped` reply through the existing remote
control reply target.

Consequences:
- The remote handoff wire protocol remains independent of business message
  serialization and Rust enum layout.
- Stop messages stay local to the hosting region, matching Pekko's observable
  handoff flow while using an explicit Rust factory instead of Scala actor
  constructor state.
- Regions that do not configure a remote handoff stop-message source do not
  falsely acknowledge hosted remote handoff commands; callers must opt in when
  they host shards reachable from a remote coordinator.

## ADR-0089: Local Graceful Region Shutdown Reuses Handoff Workers

Status: Accepted

Context:
Pekko shard regions handle `GracefulShutdown` by marking the region as
shutting down, sending `GracefulShutdownReq` to the coordinator, and stopping
once hosted shards and shard buffers are gone. The coordinator marks that
region as gracefully shutting down, excludes it from new allocations, and
starts normal shard handoff for each shard currently owned by the region.

Decision:
Kairo adds explicit local `GracefulShutdown` and `GracefulShutdownReq` actor
messages. The region actor reuses its focused runtime flag and registered
coordinator reference, then stops after local shards and buffers are empty. The
coordinator runtime adds `RegionShutdownPlan`, which marks the region as
gracefully shutting down and creates normal `ShardRebalancePlan` values for
the region's current shards. The coordinator actor spawns the existing
handoff-worker actors for those plans and lets successful completion reallocate
through the existing shard-home path.

Consequences:
- Graceful shutdown uses the same handoff, allocation, remember-store, and
  region-hosting code paths as rebalancing instead of adding a second shard
  movement mechanism.
- The coordinator's existing graceful-shutdown exclusion now has an actor
  message path that can be driven by regions and deterministic tests.

## ADR-0090: Remote Graceful Sharding Shutdown Uses Stable Region Wire Refs

Status: Accepted

Context:
Pekko carries graceful region shutdown across the sharding coordinator protocol
with `GracefulShutdownReq(region)` and later region termination with
`RegionStopped(region)`. Kairo already had local typed shutdown messages, but
remote regions must not serialize `ShardCoordinatorMsg<M>` enum variants or
depend on Rust type names, enum discriminants, or memory layout.

Decision:
Kairo models remote graceful region shutdown as stable sharding protocol
messages `GracefulShutdownReq` and `RegionStopped`, each carrying
`ActorRefWireData` for the region. Explicit codecs register serializer ids and
manifests for those messages. A focused
`ShardCoordinatorRemoteShutdownOutbound` bridge sends the envelopes to the
selected remote coordinator with the region wire ref as sender metadata, and
`ShardCoordinatorSystemInbound<M>` decodes them before re-entering normal
coordinator actor turns.

Consequences:
- Remote and local graceful shutdown share the same coordinator runtime state
  transitions after inbound decoding.
- Region shutdown transport remains separate from registration and shard-home
  lookup transport, so the crate keeps one responsibility per module.
- Wire compatibility is tied to explicit manifests, versions, serializer ids,
  and `ActorRefWireData`, not to local Rust actor-message representation.

## ADR-0091: Remote Region Registration Installs Control Targets

Status: Accepted

Context:
Pekko coordinator registration stores the region actor ref as the region's
control target and, when allocating a new shard home, sends `HostShard` before
replying with `ShardHome`. Kairo's coordinator-side remote inbound path
decoded `Register` into coordinator state, but the handoff transport still
needed an explicit remote region control target before host-shard and handoff
commands could reach that registered remote region.

Decision:
`ShardCoordinatorSystemInbound<M>` now builds a
`ShardRegionRemoteControlOutbound<M>` from the registered region's stable
`ActorRefWireData` and includes it in `RegisterRemoteRegion`. The coordinator
actor installs that target into `CoordinatorHandoff` while applying remote
registration. For newly allocated shard homes, both local and remote
`RequestShardHome` paths dispatch `HostShard` through the handoff transport
before replying with `ShardHome`.

Consequences:
- Remote registration is enough for later coordinator-driven `HostShard`,
  `BeginHandOff`, and `HandOff` commands to use the stable region wire ref.
- Allocation ordering matches Pekko's observable flow: register region, choose
  home, send `HostShard`, then answer `ShardHome`.
- Region control transport remains modular and transport-neutral; the
  coordinator actor only sees a typed `HandoffRegionTarget<M>`.

## ADR-0092: TCP Reader Supervision Models Stateless Inbound Restart

Status: Accepted

Context:
Pekko Artery TCP treats inbound streams as stateless around the socket
connection: if any inbound lane fails, it tears down the inbound kill switch
and starts the inbound streams again after the lanes have stopped. Kairo's TCP
listener already owns lane reader handles, but restart behavior needed an
explicit state boundary before being folded into the listener/runtime loops.

Decision:
Kairo adds `TcpAssociationReaderSupervisor` with
`TcpAssociationReaderRestartSettings`, `TcpAssociationReaderFailure`, and
`TcpAssociationReaderSupervisionDecision`. The default policy plans a full
inbound-stream restart for every lane or association reader failure, matching
Pekko's stateless inbound restart shape. Callers may configure a finite restart
limit for tests or stricter runtime ownership, after which the supervisor
returns `StopInboundStreams`. Failures observed after an explicit stop are
ignored. Reader handles preserve the accepted lane id when one is known, expose
`TcpAssociationSupervisedReadReport`, and listener reports carry collected
supervision decisions alongside accepted identity and frame counts.

Consequences:
- TCP reader supervision is deterministic and testable without adding a broad
  async stream dependency or hiding restart counters in listener threads.
- Listener/runtime wiring can consume structured restart/stop decisions instead
  of inspecting error strings.
- This decision covers inbound lane restart policy only; outbound stream
  backoff and reconnect ownership remain separate transport/runtime concerns.

## ADR-0093: Actor-Owned Task Cancellation Uses Scoped Self Refs

Status: Accepted

Context:
Kairo's actor model keeps `Actor::receive` synchronous and lets external work
return through `Context::spawn_task` or `pipe_to_self`. The architecture says
actor-owned tasks are cancelled when the owner stops or restarts, but Rust
standard threads cannot be preemptively killed safely by the actor runtime.

Decision:
Actor contexts maintain a task generation token in the focused `tasks` module.
`spawn_task` and `pipe_to_self` hand task closures a scoped self ref that
forwards to the real actor ref only while the generation is still current.
Actor stop and restart increment the generation, so stale task completions are
rejected and recorded through dead letters instead of re-entering the stopped
or restarted actor.

Consequences:
- Task closures may still finish their external Rust thread work, but they
  cannot mutate actor state by sending through the stale scoped self ref after
  lifecycle cancellation.
- The runtime keeps cancellation behavior explicit and dependency-light without
  adding a broad async runtime or unsafe thread termination.
- A later async backend can replace the execution primitive while preserving
  the same generation-scoped delivery contract.

## ADR-0094: Lease Majority Is A Downing Hook, Not Membership Truth

Status: Accepted

Context:
Pekko's lease-majority split-brain strategy acquires a configured lease before
downing the unreachable side, delays acquisition on the minority side, and
reverses the decision when lease acquisition is denied. Kairo must preserve
those observable downing transitions without introducing etcd, Kubernetes
leases, or any other central authoritative cluster-membership store.

Decision:
Kairo models lease-majority as `LeaseMajorityHook` in the focused cluster
downing module. The hook takes explicit `LeaseMajoritySettings` and a
caller-provided `LeaseMajorityLease` trait implementation. The lease can grant
or deny the downing attempt, but it cannot add, remove, or authorize cluster
members. Majority/minority and indirectly connected calculations still come
from gossip and reachability. `DowningProviderActor` honors the hook's
minority-side acquisition delay through a separate deterministic timer before
applying the hook's decision.

Consequences:
- Lease-majority behavior is testable with deterministic in-memory leases and
  manual time, without adding a broad coordination dependency.
- The lease is a split-brain tie-break hook only; gossip remains the source of
  membership state and reachability remains the source of partition evidence.
- Future concrete lease integrations can implement `LeaseMajorityLease` behind
  crate or feature boundaries without changing the cluster membership state
  machine.

## ADR-0095: Downing Configuration Uses Structured Strategy Variants

Status: Accepted

Context:
The TOML-first facade originally stored `cluster.downing.strategy` as a raw
string. That was enough for early validation, but lease-majority needs
strategy-specific data such as lease name, role, acquisition delay, and release
timing. Keeping those as loosely related strings would make invalid
combinations easier to construct programmatically.

Decision:
`ClusterDowningConfig` now stores `ClusterDowningStrategyConfig`, an enum with
explicit variants for `none`, `down-all`, `keep-majority`, `keep-oldest`, and
`lease-majority`. TOML still uses simple stable string names, but parsing maps
them into the structured enum and validates strategy-specific fields.

Consequences:
- Programmatic settings are format-neutral and typed instead of carrying
  unvalidated stringly state.
- TOML stays stable and readable while the runtime facade can grow conversion
  helpers per strategy.
- Lease-majority settings can be validated without introducing a concrete
  lease dependency into the `kairo` facade crate.

## ADR-0096: Extensions Are Type-Keyed Actor-System Services

Status: Accepted

Context:
Pekko extensions are loaded once per actor system and retrieved through an
extension id. Kairo needs the same observable lifetime and lookup semantics,
but JVM class-name loading, reflective setup overrides, and Scala singleton
extension ids do not translate cleanly to a Rust-first API.

Decision:
Kairo models extensions with `ExtensionRegistry` in `kairo-actor`. Extensions
are registered explicitly through `ActorSystem::register_extension`, keyed by
the Rust `TypeId` of the extension type, created at most once per actor system,
and returned as `Arc<T>`. Lookups use `ActorSystem::extension::<T>()` and report
an explicit `ActorError::ExtensionNotRegistered` when no instance exists.

Consequences:
- Extension instances are type-safe, thread-safe, and scoped to one actor
  system without relying on string class names or global registries.
- Mutable actor-like behavior should still live in actors; extensions are best
  used as shared system services, registries, handles, or integration points.
- Future config-driven eager loading can layer explicit registrar functions on
  top of the same registry without introducing HOCON or JVM reflection.

## ADR-0097: Observability Configuration Starts Backend-Neutral

Status: Accepted

Context:
M11 requires runtime diagnostics for dead letters, quarantine, serialization,
remote delivery, and gossip, plus logging/tracing and metrics hooks where
useful. The rewrite already exposes several diagnostic events and failure
paths, but choosing a concrete logging, tracing, or metrics backend too early
would add broad dependencies before the runtime needs them.

Decision:
The facade configuration introduces `ObservabilityConfig` and
`DiagnosticsConfig` as format-neutral settings. TOML maps
`[observability.diagnostics]` into explicit boolean flags for dead letters,
remote delivery failures, serialization failures, quarantine events, and gossip
state changes. The settings describe which diagnostic categories should be
published or surfaced; concrete logging, tracing, and metrics integrations will
be layered later by runtime modules that own the relevant events.

Consequences:
- Applications can configure diagnostic intent without depending on a specific
  logging or metrics crate.
- Runtime crates can add adapters from these flags to their existing event
  streams and failure paths in focused follow-up slices.
- The TOML schema remains stable while preserving the constraint against broad
  third-party dependencies before implementing code needs them.

## ADR-0098: Dead-Letter Diagnostics Separate Recording From Event Publication

Status: Accepted

Context:
Pekko publishes dead letters to the actor-system event stream and separately
configures dead-letter logging through settings such as `log-dead-letters`.
Kairo keeps a deterministic in-memory `DeadLetters` buffer for tests and
operator diagnostics, while M11 observability settings need a way to control
which diagnostic categories are surfaced through runtime publication.

Decision:
`ActorSystemBuilder::publish_dead_letters_to_event_stream` controls whether
recorded `DeadLetter` values are also published to the local `EventStream`.
The default remains enabled. The `kairo` facade maps
`observability.diagnostics.dead_letters` through `KairoSettings::actor_system_builder`.
Disabling the flag suppresses event-stream publication only; the local
`DeadLetters` record buffer still records rejected or undeliverable messages.

Consequences:
- Existing Pekko-like event-stream behavior remains the default.
- Tests and diagnostics can still inspect dead-letter records even when an
  application disables event-stream publication.
- Logging and metrics backends can be added later as subscribers or adapters
  without changing the actor send path.

## ADR-0099: Remote Inbound Diagnostics Use Observer Hooks

Status: Accepted

Context:
M11 requires diagnostics that identify serialization and remote delivery
failures. `RemoteInbound` is the point where stable wire metadata is decoded
and where typed delivery into a local target can fail. Adding logging or
metrics dependencies directly to this path would violate the dependency
discipline and make transport tests depend on a concrete observability backend.

Decision:
`RemoteInbound` exposes `with_diagnostics`, accepting a
`RemoteInboundDiagnostics` observer. When deserialization fails, the observer
receives a `RemoteInboundDiagnostic::SerializationFailure` with recipient,
optional sender, serializer id, manifest, version, and reason. When typed
delivery fails after successful decode, the observer receives
`RemoteInboundDiagnostic::DeliveryFailure` with recipient, optional sender, and
reason. The default constructor remains no-op.

Consequences:
- Remoting exposes structured diagnostic facts without selecting logging,
  tracing, or metrics crates.
- Tests and runtime integrations can collect remote inbound failures
  deterministically.
- Facade diagnostics helpers now map observability settings into
  caller-provided observer wrappers without changing wire decoding or local
  delivery semantics.

Follow-up:
`ActorSystemRemoteInbound` now exposes diagnostic-aware constructors that pass
the same observer into its business-message `RemoteInbound`, so actor-system
runtime composition can report serialization and local-delivery failures
without changing the remote death-watch system-control path.

`RemoteInboundDiagnosticFilter` and the facade
`DiagnosticsConfig::remote_inbound_diagnostics` helper map the
`serialization_failures` and `remote_delivery_failures` flags onto a
caller-provided observer. If both categories are disabled, the helper returns
no observer; otherwise it installs either the original observer or a filtering
wrapper. This keeps configuration backend-neutral while letting runtime
composition honor diagnostic category settings.

`ClusterEventPublisher::with_diagnostics` follows the same observer pattern for
gossip state changes. It records a backend-neutral `GossipStateChanged`
diagnostic with the previous gossip, current gossip, and computed cluster-event
diff only when the gossip value actually changes. The facade
`DiagnosticsConfig::cluster_diagnostics` helper maps
`gossip_state_changes` to this observer without introducing logging or metrics
dependencies.

`RemoteAssociation::with_diagnostics` follows the same backend-neutral observer
pattern for quarantine transitions. A quarantine emits a structured
`RemoteAssociationDiagnostic::Quarantined` value with the remote address,
optional remote UID, and reason after the association enters the quarantined
state. `RemoteAssociationDiagnosticFilter` and
`DiagnosticsConfig::remote_association_diagnostics` map the
`quarantine_events` flag onto caller-provided observers, returning no observer
when quarantine diagnostics are disabled.

`DiagnosticCounters` in the `kairo` facade provides a dependency-free adapter
for the enabled remote inbound, remote association, and cluster diagnostic
observer traits. It records per-category atomic counts and exposes
`DiagnosticCounterSnapshot` for applications that want to export metrics to
their own backend without Kairo selecting a logging, tracing, or metrics crate.
`DiagnosticTextSink` uses the same observer traits to emit stable single-line
diagnostic records through a caller-provided closure, giving applications a
concrete bridge to `log`, `tracing`, stderr, files, or tests while preserving
the dependency boundary.

## ADR-0100: Serialized Remote Envelopes Use Explicit Wire Helpers

Status: Accepted

Context:
M3 requires stable remote message metadata and remote envelopes before TCP
remoting, cluster, distributed-data, sharding, and cluster-tools system
protocols depend on them. Pekko carries serializer id, manifest, and payload
bytes explicitly, with sender and recipient metadata at the envelope boundary.
Kairo already has `WireWriter` and `WireReader` helpers for hand-written
system protocol bytes. The next serialization layer needed
`SerializedMessage` and `RemoteEnvelope` to own the canonical byte round-trip
that remoting and system protocols could share.

Decision:
`SerializedMessage::encode_wire` writes serializer id, manifest, version, and
payload bytes using `WireWriter`. `RemoteEnvelope::encode_wire` writes the
recipient actor-ref path, optional sender actor-ref path, and the serialized
message tuple. Decode uses `WireReader`, validates manifests with
`Manifest::try_new`, validates actor refs through `ActorRefWireData`, and
rejects unread trailing bytes. Payload-specific encoding remains owned by
registered `MessageCodec<M>` implementations.

Consequences:
- Remote and system protocol tests can assert a stable byte contract without
  adding serde, prost, bincode, or another broad serialization dependency.
- Actor-ref metadata remains validated at the serialization boundary instead
  of being treated as arbitrary strings after decode.
- Business payload codecs stay explicit and version-aware, preserving the rule
  that Rust type names, enum discriminants, and memory layout are not wire
  contracts.

## ADR-0101: Sharding Rebalance Gates On Coordinator Unavailable Regions

Status: Accepted

Context:
Pekko's least-shard allocation strategy avoids rebalance while needed cluster
members are unreachable, because rebalancing requires acknowledgements from
regions and proxies. Pekko obtains that evidence from `CurrentClusterState`
inside the actor-system-dependent allocation strategy. Kairo's
`ShardAllocationStrategy` is intentionally cluster-agnostic and synchronous, so
putting cluster reachability directly into the strategy trait would widen a
small testable boundary.

Decision:
Kairo stores unavailable region markers in `CoordinatorRuntime` and exposes
actor control messages to mark or unmark regions as unavailable. Rebalance
planning skips while any region is marked unavailable and resumes after the
set is cleared. Known shard-home replies remain available unless the owning
region is terminating, matching Pekko coordinator behavior where reachability
gates rebalance suitability rather than making existing shard homes disappear.

Consequences:
- The public allocation strategy trait stays independent from cluster
  membership, reachability, remoting, and actor-system state.
- Cluster or sharding integration layers can project reachability observations
  into explicit coordinator runtime markers.
- Focused runtime and actor tests pin unavailable-region rebalance skips,
  healing, and known-home reply behavior without requiring a multi-node cluster
  fixture.

## ADR-0102: Actor Execution Uses System-Owned Bounded Executors

Status: Accepted

Context:
ADR-0003 intentionally used one worker thread per actor as an M1 bootstrap.
The same baseline later used one thread per actor helper task and one sleeping
thread per real-time timer. That preserves synchronous actor turns, but it
cannot satisfy the production execution gate: idle actors and timers consume
OS threads, throughput only yields a dedicated thread, and the actor system
does not own execution capacity or shutdown. Pekko schedules a mailbox at most
once, processes system messages before a throughput-limited user batch, marks
the mailbox idle, and re-registers it if work raced with the idle transition.
Kairo needs those observable scheduling semantics without copying Pekko's
executor inheritance or JVM-specific implementation.

Decision:
Each `ActorSystem` owns three explicit execution components:

- an actor dispatcher backed by a fixed worker pool and a shared runnable
  queue;
- a separate bounded task executor for `spawn_task`, `pipe_to_self`, and
  scheduler callbacks that may perform blocking user work;
- one real-time scheduler driver that owns the timer queue, while the manual
  scheduler remains an injected deterministic backend.

An actor is represented by a resumable mailbox runner instead of a dedicated
thread. Mailbox enqueue atomically changes the runner from idle to scheduled
and submits it only on that transition. One dispatcher worker owns the actor
state for an activation, drains all currently available system messages before
user work, processes at most the configured number of user messages, marks the
runner idle, and immediately attempts to schedule it again when work remains.
The scheduled transition and post-idle recheck are the synchronization
boundary that prevents concurrent actor turns and lost wakeups. Actor startup
and final cleanup also run as dispatcher activations. Blocking while waiting
for children during stop or restart is retained initially for semantic
compatibility and will be audited separately after the dispatcher transition.

The default actor and task worker counts derive from
`std::thread::available_parallelism`, with explicit builder overrides for
tests and deployment tuning. Queue admission is non-blocking. Actor mailbox
overflow keeps the existing dead-letter behavior; task-executor saturation is
reported through an explicit spawn error rather than blocking an actor turn.
System termination stops actors first, then shuts down the scheduler, task
executor, and actor dispatcher without accepting new work. No async runtime or
new third-party dependency is introduced by this execution foundation.

Consequences:
- Thousands of idle actors no longer imply thousands of OS threads, while
  `Actor::receive` remains synchronous and never runs concurrently for one
  actor incarnation.
- Dispatcher throughput becomes a fairness boundary between mailbox
  activations instead of a `thread::yield_now` hint inside a dedicated actor
  thread.
- Blocking helper work cannot consume actor-dispatcher workers, though users
  must still size the bounded task executor for their workload.
- Real-time timers share one ordered driver and do not allocate one sleeping
  thread per timer; manual-time tests keep their current deterministic API.
- The transition requires focused lost-wakeup, single-turn, fairness,
  saturation, shutdown, and high-cardinality actor/timer tests before Phase 1
  is complete.

## ADR-0103: One ActorSystem Owns One Manifest-Dispatched Remoting Runtime

Status: Accepted

Context:
The current `TcpRemoteActorSystem<M>` proves typed TCP business delivery,
bidirectional handshaken associations, remote watch, and coordinated shutdown,
but its inbound boundary is generic over one business protocol. Cluster,
distributed data, sharding, and cluster tools consequently assemble separate
TCP listeners and association caches around their own frame handlers. That
cannot satisfy the composed-runtime gate: one node needs one canonical address,
one association incarnation registry, and one listener capable of carrying
unrelated registered business protocols plus system traffic. Pekko's remote
provider owns one transport lifecycle and dispatches serialized messages only
after the association boundary; Kairo needs the same ownership without making
an erased message type the user API.

Decision:
Each remote-enabled `ActorSystem` has exactly one `kairo-remote` runtime for a
canonical transport address and local system UID. That runtime owns the TCP
listener, dialer, association cache, address/UID registry, remote-watch actor,
lane writers/readers, inbound manifest table, and coordinated-shutdown task.
Cluster and the higher distributed crates register protocol manifests and
handlers with this runtime; they do not bind their own listeners or create
parallel association registries.

The runtime itself is not generic over one business message. Its builder
accepts typed protocol registrations before bind, rejects duplicate manifests,
and freezes the codec and inbound-handler tables when the listener starts. A
typed registration for `M: RemoteMessage` installs an internal manifest handler
that deserializes `M` with the shared registry and delivers it through the local
typed actor registry. System registrations install focused handlers for their
stable manifests. Internal type erasure is allowed only in this codec/manifest
dispatch table; `ActorRef<M>`, `RemoteActorRef<M>`, and `ResolvedActorRef<M>`
remain the public send and resolution boundaries.

Each association owns bounded non-blocking control, ordinary, and optional
large-message queues plus one writer owner per concrete lane. `tell` and remote
system sends enqueue framed work and never call `TcpStream::write_all` from an
actor turn. Ordinary or large-lane overflow returns an explicit delivery error
and diagnostic. Control-lane overflow is an association failure because it can
break lifecycle ordering. Writer or reader failure closes the whole association
incarnation and is reported to the runtime lifecycle owner rather than leaving
partially live sibling lanes.

A closed association handle is never reopened. A later validated handshake may
replace a `Closed` registry entry that previously completed identity, including
when the peer process still has the same UID; this is a new transport route for
the same process incarnation, and existing holders continue to observe the old
handle as closed. `Quarantined` remains terminal for its exact UID and can only
be replaced after a different UID completes the handshake. An unidentified
closed entry cannot be revived because it has no validated incarnation to
reconnect.

Handshake processing has configurable byte, read-timeout, lane-assembly
timeout, and pending-partial-association limits. Concurrent lane arrivals are
grouped by the complete remote address and UID; wrong-target, mixed-identity,
duplicate-lane, oversized, timed-out, and over-limit handshakes are rejected
before a route is installed. Runtime shutdown stops protocol system actors,
rejects new sends, drains or closes lane owners within the shutdown budget,
removes routes, stops readers and the listener, and completes once even when
called through coordinated shutdown and an explicit handle.

Consequences:
- Two unrelated business protocols and all registered system manifests can use
  the same listener and association without a global business-message enum.
- Canonical address, association UID, quarantine state, and shutdown ordering
  have one owner per ActorSystem.
- Existing cluster/ddata/sharding/tools socket runtimes become adapters or are
  retired as their handlers move behind the composed runtime.
- Registration is an intentional startup contract; adding a new wire protocol
  requires a stable manifest, codec, and inbound handler before bind.
- Bounded queues make slow-peer behavior explicit and keep socket blocking off
  actor dispatcher threads.

## ADR-0104: Reliable System Delivery Is Sequenced Per Association Incarnation

Status: Accepted

Context:
TCP preserves bytes on one connection, but actor lifecycle messages must also
survive association replacement and reconnect without duplication or silent
reordering. Pekko wraps reliable system messages with a per-association
sequence number, retains unacknowledged messages in a bounded buffer, emits
cumulative acknowledgements, resends on a timer, deduplicates old sequence
numbers, and quarantines after buffer overflow or terminal acknowledgement
silence. Not every control message needs that cost: handshakes, heartbeats, and
periodic gossip are control traffic but are safe to refresh at-most-once.

Decision:
Lane choice and reliability are separate manifest properties. All system and
control protocols use the control lane, but only manifests registered as
`ReliableSystem` enter reliable delivery. Remote watch, unwatch, termination,
and other lifecycle protocols whose loss can violate actor or sharding
semantics use this class. Handshakes, heartbeat probes/replies, periodic gossip,
and refreshable status traffic remain at-most-once control messages.

The reliable sender state is scoped to `(remote address, remote UID)`. After a
handshake completes, sequence numbers begin at one and increase monotonically.
The sender wraps each stable `RemoteEnvelope` in a stable reliable-system wire
envelope, retains it in a bounded unacknowledged buffer before enqueueing, and
transmits retained entries in sequence order. The receiver keeps the next
expected sequence for the sending incarnation:

- the expected sequence is delivered once and advances the cumulative ack;
- an older sequence is a duplicate, is not delivered again, and receives the
  current cumulative ack;
- a future sequence is not delivered out of order and receives a nack carrying
  the highest contiguous sequence.

Acks and nacks include the association identities needed to reject stale
replies from an earlier UID. Acks cumulatively remove retained entries. A
single ActorSystem-scheduler retry tick per active association resends
unacknowledged entries in order after the configured interval. A new remote UID
clears old sequence state and routes the old unacknowledged messages to the
remote-delivery failure/dead-letter boundary; sequence state is never reused
across incarnations.

Reliable-buffer overflow, control-queue overflow, acknowledgement silence past
the configured give-up deadline, or an invalid ack/nack transition quarantines
the exact remote incarnation, closes its routes, fails retained delivery
records explicitly, and rejects further ordinary system sends until a new UID
completes a handshake. Ordinary business-message failure remains at-most-once
and does not by itself quarantine a healthy association.

Consequences:
- Reliable system messages are ordered and deduplicated across retry/reconnect
  while ordinary business traffic stays at-most-once.
- Retry and acknowledgement state cannot leak from an old process incarnation
  at the same host/port into the new one.
- Control protocols declare whether loss is terminal or refreshable instead of
  treating every control-lane frame as reliable.
- Buffer and give-up limits make terminal failure observable and bounded rather
  than allowing an association to retain lifecycle messages forever.

## ADR-0105: TCP Lane Handshakes Establish Both Peer Incarnations

Status: Accepted

Context:
Reliable system delivery is scoped to the complete remote address and system
UID. The original TCP lane handshake carried the dialer's identity to the
accepting runtime, which was enough to validate inbound lanes and install a
reverse route. The dialing runtime did not learn the accepting runtime's UID,
however, so its outbound route could not sequence, reject stale replies, or
quarantine the exact peer incarnation. The route pipeline also owned a second
`RemoteAssociation` value instead of the handle indexed by the runtime's
association registry.

Decision:
When both endpoints configure complete local identities, each accepted lane
returns the same stable `TcpAssociationHandshake` record after all incoming
lanes have been validated. The response reverses the direction: it carries the
accepting runtime's address and UID, the dialer's target address, and the lane
identifier. The dialer opens and writes all lanes before reading responses so
the receiver can validate the association as a group without deadlock. It
validates the response target, identity consistency, and lane uniqueness before
installing the route.

`RemoteAssociationRouteInstaller` can be configured with the runtime's
`RemoteAssociationRegistry`. In that mode both accepted and dialed pipelines
use the registry-owned association handle. Completing either side of the
handshake activates that same handle with the observed remote UID, so queue
failure, reliable-delivery failure, diagnostics, and normal send guards observe
one incarnation state.

Raw transport tests may still configure only a local address and use the
one-way request form when peer-incarnation state is intentionally outside the
test. Actor-system runtimes configure complete identities and require the
response.

Consequences:
- Both sides of an actor-system TCP association know the peer UID before framed
  messages are delivered.
- Reliable sender state and acknowledgements can be keyed by the actual remote
  incarnation without treating discovery or socket state as cluster truth.
- Quarantining the registry handle immediately guards the installed route; no
  parallel association state can remain active.
- The stable handshake encoding is reused in both directions; no Rust type or
  layout detail is added to the wire contract.

## ADR-0106: The Composed Runtime Owns Bounded Peer Redial

Status: Accepted

Context:
Closing a failed route and allowing a fresh same-UID handshake fixes the
association state transition, but callers still have to notice transport loss
and invoke `dial` again. Pekko keeps outbound association intent above an
individual stream and lazily restarts failed streams with bounded restart
policy. Kairo's higher cluster, distributed-data, and cluster-tools connectors
already model desired peers separately from concrete routes. The composed
runtime needs the same ownership without making a typed actor send block on a
socket reconnect or silently replaying ordinary at-most-once messages.

Decision:
`TcpRemoteActorRuntime::dial` records persistent intent for the complete remote
association address before attempting the synchronous initial dial. The call
still returns that attempt's success or error immediately. One runtime-owned
worker, shared by all managed peers, observes missing routes and retries them;
there is no worker per actor ref, message, lane, or peer. Retry delay grows
exponentially from a configurable non-zero minimum and is capped by a
configurable maximum. A successful route resets its attempt count.

Typed `tell` never performs or waits for reconnect. An ordinary message that
finds no route still fails at-most-once and is not retained for replay. Reliable
system messages continue to use ADR-0104's separate bounded retention and UID-
scoped sequencing. Every retry performs the full lane handshake, so the
association registry remains the authority for accepting a same-UID transport
replacement, rejecting a quarantined UID, or admitting a new incarnation.
Closing a concrete transport route preserves `Quarantined` as the stronger
terminal state instead of replacing it with `Closed`.

A route registration represents one concrete connection, not the persistent
peer intent. Closing that registration therefore permits automatic redial.
`TcpRemoteActorRuntime::disconnect` is the permanent operation: it first clears
the managed-peer intent and then removes and closes the current route. Runtime
shutdown clears all intents, wakes and joins the retry owner before listener and
route teardown, and rejects or closes any attempt that completes after its
intent was removed.

Consequences:
- Route recovery no longer depends on callers polling reader handles or
  rebuilding typed remote refs.
- Failed ordinary sends remain visible and are never converted into implicit
  durable delivery.
- Retry work is bounded to one runtime owner and capped backoff, while complete
  handshakes preserve quarantine and incarnation rules.
- Higher distributed connectors can migrate their peer intent into the shared
  runtime instead of owning parallel transport retry loops.

## ADR-0107: Cluster Daemon Messages Use Explicit Stable Wire Contracts

Status: Accepted

Context:
The component cluster runtime already has stable codecs for `Join`, `Welcome`,
full gossip, and heartbeat traffic, but the daemon lifecycle also requires seed
contact, gossip-status negotiation, user leave/down actions, and exit
confirmation. Those messages must be fixed before an ActorSystem-owned daemon
can safely compose them with remoting. Pekko exchanges configuration during
seed contact, but copying its full configuration object would couple Kairo's
wire protocol to a parser representation and could expose unrelated settings.

Decision:
Kairo defines `InitJoin`, `InitJoinAck`, `InitJoinNack`, `GossipStatus`,
`Leave`, `Down`, and `ExitingConfirmed` as ordinary Rust structs implementing
`RemoteMessage`. Their stable manifests are under `kairo.cluster.*`, their
serializer IDs are the reserved sequential range `2005..=2011`, and their
version-one payloads use hand-written `WireWriter`/`WireReader` codecs.

`InitJoin` carries opaque deterministic digest bytes, not a serialized settings
tree. `InitJoinAck` returns an explicit `ClusterConfigCheck` value:
`Unchecked`, `Compatible`, or `Incompatible`. Contact, leave, and down messages
use `Address` because they select an actor-system location before or independent
of a known incarnation. Gossip status and exit confirmation use
`UniqueAddress`, because accepting either for the wrong UID would violate
incarnation safety. Unknown versions, invalid enum codes, and trailing bytes
are rejected.

The responder is constructed with either a local deterministic digest or no
checker. Equal digests produce `Compatible`, unequal digests produce
`Incompatible`, and an absent checker produces `Unchecked`; empty bytes remain
ordinary digest data rather than an implicit compatibility mode.

Seed-contact envelopes use `/system/cluster/core/daemon` as both recipient and
sender path. The validated sender address is the contact origin used by the
formation state machine; `InitJoinAck.address` remains the responder's
canonical Join target and cannot substitute for transport origin validation.

Consequences:
- Cluster formation can add actor ownership without reopening the public wire
  format.
- Seed discovery remains contact-only; only accepted `Join`/`Welcome` gossip
  changes membership truth.
- Configuration compatibility is explicit without making TOML, another parser,
  or sensitive configuration part of the wire contract.
- Ack/Nack acceptance is bound to the contacted remote daemon rather than a
  payload-supplied address.
- Rolling-version behavior can be extended by adding intentional codec versions
  rather than relying on Rust enum layout or type names.

## ADR-0108: Cluster Daemon Materializes At Composed-Remoting Bind

Status: Accepted

Context:
The cluster daemon needs the remoting runtime's effective canonical address,
codec registry, shared outbound, and inbound control registration. Starting
seed contact before bind would use an unknown port or send before configured
seed routes exist. A second cluster listener would violate the composed runtime
contract.

Decision:
`register_cluster_daemon` registers cluster control manifests on a
`TcpRemoteActorRuntimeBuilder`. During bind its factory materializes the actor
hierarchy `/system/cluster/core/daemon` and the daemon's publisher, membership,
seed process, responder, and typed wire children using the effective runtime
context. The seed process is created inactive. `ClusterDaemonRegistration::activate`
dials configured seed addresses through the bound runtime and then sends the
typed `Start` command. An empty configured seed list means the local canonical
address is the sole seed and therefore self-forms.

Membership inbound creates and caches typed per-`UniqueAddress` reply actors on
first Join/Gossip contact so Welcome and talkback do not depend on predeclared
members. Discovery and configured seeds still provide contact intent only;
Join/Welcome gossip remains membership truth.

Consequences:
- Cluster control and business protocols share one listener, registry,
  association cache, and outbound lifecycle.
- No seed message is emitted before canonical bind and managed route setup.
- The stable daemon path is represented by the real actor hierarchy as well as
  validated envelope metadata.
- Bootstrap activation is an explicit post-bind step until the public
  ActorSystem extension owns both operations behind one builder call.

## ADR-0109: Periodic Gossip Uses Stable Seen Digests And Typed Ownership

Status: Accepted

Context:
Seed admission establishes the initial membership view, but joined nodes do
not reach convergence or advance `Joining` members without recurring gossip
and leader actions. Pekko avoids full-state transfer when peers share a view by
negotiating vector clocks and a digest of the seen table. Kairo needs the same
observable negotiation rules without random scheduling making state-machine
tests nondeterministic.

Decision:
`Gossip::seen_digest` is SHA-1 over the comma-separated canonical addresses of
the sorted seen table. This intentionally matches Pekko's non-security digest;
it is an equality optimization and is not used for authentication or
membership identity.

The daemon owns a typed `ClusterGossipProcess`. Each fixed-delay tick requests
the authoritative gossip from `ClusterMembership`, asks that owner to run
leader actions, and selects a locally reachable non-self member. Members not in
the seen table are preferred and receive full gossip; otherwise the process
sends `GossipStatus`. Candidate ordering and selection are deterministic
round-robin rather than random. Receiving status follows Pekko's negotiation:
unknown or locally unreachable senders are ignored, a different seen digest
gets full gossip, a newer remote clock gets local status, an older or concurrent
remote clock gets full gossip, and identical state ends the exchange.

Status is routed through the shared cluster control lane at the real daemon
path. Periodic outbound failure is best-effort and does not terminate the
gossip actor, so later rounds can converge after association recovery.

Consequences:
- Seed-formed nodes can converge and advance to `Up` without injected ticks or
  membership snapshots.
- Full gossip remains the membership truth; status messages only choose which
  side sends it.
- Reproducible target selection diverges from Pekko's randomized probability
  tuning but preserves reachability and different-view preference semantics.
- SHA-1 is a narrow direct dependency for wire-compatible seen equality, not a
  security primitive.

## ADR-0110: Cluster Peer Intent Uses A Non-Owning Remoting Handle

Status: Accepted

Context:
Seed contact gives a joining node an association to a configured contact, but
three-node gossip requires direct routes between members learned only after
formation. The existing cluster-only peer bootstrap owns a separate listener
and therefore cannot be placed inside the composed ActorSystem daemon. Moving
the whole remoting runtime into a cluster actor would make cluster responsible
for business protocols, remote death watch, reliable delivery, and transport
shutdown.

Decision:
The shared `TcpRemoteActorRuntime` exposes a cloneable `TcpRemotePeerManager`.
The handle shares the runtime's association cache and managed reconnect owner;
it can connect or disconnect an address and inspect connection intent, but it
cannot bind or shut down the listener. `TcpRemoteActorRuntime` remains the sole
transport lifecycle owner.

`ClusterRemotePeerConnector` subscribes to the cluster event publisher with an
initial snapshot, feeds snapshots/events through the existing
`ClusterAssociationPeerState`, and serially applies derived dial/remove effects
to the peer manager. Connect calls run through actor tasks because socket
connection may block; completion returns through the connector mailbox.
Initial failures remain managed by the remoting reconnect worker. Removing a
member removes reconnect intent and closes its current route. Stopping the
connector unsubscribes and removes all cluster-derived peer intent.

The daemon starts the connector from post-bind activation, because the peer
manager does not exist during protocol registration. Seed/contact addresses
still supply only initial transport intent. Gossip membership and local
reachability observations remain the source of every later peer decision.

Consequences:
- Three independently bound nodes can form a direct association mesh and
  converge without injected membership snapshots.
- Cluster does not create a second listener or become the remoting shutdown
  owner.
- Blocking connection work stays outside synchronous actor receive turns while
  command ordering remains actor-owned.
- The older cluster-only TCP bootstrap remains a compatibility boundary until
  callers migrate to the composed daemon.

## ADR-0111: Heartbeat Reachability Retains Recovery Routes

Status: Accepted

Context:
The composed daemon had heartbeat wire codecs and inbound handlers but no owner
for the stable sender, receiver, periodic scheduling, or membership-derived
remote routes. Feeding a local failure-detector verdict into gossip also creates
a lifecycle hazard: if the same unreachable observation removes managed peer
intent, the transport loses the route needed for a later heartbeat response to
prove recovery.

Decision:
The cluster root owns real actors at `/system/cluster/heartbeatSender` and
`/system/cluster/heartbeatReceiver`. A typed `ClusterHeartbeatConnector`
subscribes with the current cluster snapshot, creates one remote heartbeat route
for each non-self member, initializes the sender only after those routes exist,
and forwards later membership and reachability events. Removal stops a route;
transient outbound send failure does not, because heartbeat traffic is
refreshable and later ticks must retry.

`HeartbeatSender` evaluates its deadline failure detector on periodic ticks and
reports only verdict transitions to `ClusterMembership`. These records always
use the local node as observer; heartbeat never removes a member directly. A
successful response after a reported failure emits the corresponding reachable
observation.

The composed `ClusterRemotePeerConnector` keeps managed reconnect intent for
locally unreachable members and removes it only when membership reaches
`Removed`. The legacy cluster-only peer planner retains its existing behavior
of removing locally unreachable routes.

Consequences:
- Healthy composed nodes exchange heartbeat requests and responses through the
  same control lane and association lifecycle as other cluster protocols.
- Local detector failure becomes gossip reachability and can recover without an
  external route reinstall or injected membership snapshot.
- Unreachable members continue to consume reconnect attempts until membership
  removal; reconnect backoff and transport quarantine remain remoting concerns.
- Membership truth stays gossip plus local observations, not transport route
  state or discovery.

## ADR-0112: Graceful Leave Is A Phased Gossip Transition

Status: Accepted

Context:
Kairo already modeled `Leaving`, `Exiting`, convergence, exiting confirmation,
and the standard coordinated-shutdown phase names, but the composed daemon did
not connect them. Stopping the cluster root directly made every planned exit
look like a failure-detector event and left the stable `Leave`, `Down`, and
`ExitingConfirmed` codecs outside the shared control manifest router.

Decision:
`ClusterMembership` owns address-based leave and down commands. Leave changes
only eligible matching members to `Leaving`; the existing leader action remains
the sole owner of `Leaving -> Exiting` and removal after convergence. Membership
also retains incarnation-specific `ExitingConfirmed` facts and supplies them to
leader actions until the member disappears.

The composed daemon owns a typed leave coordinator subscribed to the current
membership view. Synchronous coordinated-shutdown tasks communicate with that
actor through completion channels: `cluster-leave` initiates and waits for
leaving, `cluster-exiting` waits for the convergence transition,
`cluster-exiting-done` sends confirmation, and `cluster-shutdown` stops and
waits for the cluster root. This keeps gossip mutation on actor turns while
allowing the existing synchronous shutdown runner to enforce its phase order.
When a public leave reaches self `Exiting` before shutdown is running, the
coordinator starts that same phase sequence from an ActorSystem-owned task; it
does not block its receive turn on shutdown tasks that message it back.

`Leave`, `Down`, and `ExitingConfirmed` join the nine existing manifests in the
shared cluster control router. Exit confirmation is best-effort and is sent to
all remaining members instead of Pekko's two potential-leader optimization;
only the current leader can act on it, and sending to all avoids duplicating
leader-candidate routing policy in the shutdown coordinator.

The controlled `Cluster` facade exposes self identity plus leave and down next
to its existing state and subscription operations. Event-only `Cluster`
instances used by legacy connectors reject control operations explicitly.

Consequences:
- A healthy leaving node is removed and tombstoned without waiting for failure
  detection or manual downing.
- Cluster actors stop before later transport and ActorSystem termination phases,
  while remoting remains the owner of listener shutdown.
- Repeated leave is idempotent, and a lost confirmation retains Pekko's fallback:
  the exiting node can still be removed after it becomes unreachable.
- Explicit join was still a bootstrap operation at this checkpoint; ADR-0113
  adds the ActorSystem-owned lifecycle access boundary.

## ADR-0113: Cluster Lifecycle Access Is An ActorSystem Extension

Status: Accepted

Context:
The composed cluster daemon exposed a bootstrap handle after bind, but normal
application code had no system-scoped way to retrieve cluster state or issue
lifecycle operations. Configured seed joining also started unconditionally,
which prevented an application from explicitly choosing a contact after the
shared remoting runtime was active.

Decision:
Post-bind cluster activation installs one typed `ClusterExtension` in the
existing ActorSystem extension registry. The extension owns a clone of the
composed daemon handle and exposes self identity, state subscriptions, join,
leave, and down through the controlled `Cluster` facade. Legacy event-only
`Cluster` values remain valid and continue to reject lifecycle operations.

Configured seed joining remains the default. Setting
`ClusterDaemonBootstrapSettings::with_auto_join(false)` starts the daemon and
peer-management actors without initializing membership. The first public join
request becomes the daemon's immutable join intent. Self-join uses the local
seed-wire effect; remote join establishes managed transport intent from an
ActorSystem task, sends the stable remote `Join` effect after connection, and
retries at the configured seed interval until the local `UniqueAddress` appears
in cluster events. Further join requests are ignored, matching Pekko's
observable rule that an initialized or already-joining cluster cannot switch
join targets.

Kairo validates protocol and remote host presence but deliberately permits a
contact whose ActorSystem name differs from the local name. Existing Kairo seed
formation and remote addressing use the complete canonical address as endpoint
identity, so adding Pekko's same-system-name restriction would break the
established Rust API contract without improving wire safety.

Consequences:
- Applications retrieve the composed cluster runtime type-safely from their
  ActorSystem after activation and no longer need to retain a daemon handle for
  normal operations.
- Explicit join performs blocking connection work outside synchronous actor
  turns and retains reconnect intent until membership confirms success.
- Automatic and manual formation share the same stable Join/Welcome membership
  protocol and the same daemon ownership; discovery remains contact-only.
- Binding and post-bind activation remain two explicit bootstrap steps until a
  later configured facade owns the full remote-plus-cluster builder lifecycle.

## ADR-0114: Distributed Data Shares Cluster And Remoting Lifecycle

Status: Accepted

Context:
Kairo's distributed-data state machines, CRDTs, consistency logic, and legacy
TCP peer bootstrap were mature, but that bootstrap owned a separate listener
and accepted manually supplied cluster snapshots. Composed applications need
`/system/ddata` to use the same canonical node, association incarnation, peer
routes, cluster events, extension registry, and shutdown ordering as ordinary
remote actors and cluster protocols. The existing replicator actor is generic
over one `DeltaReplicatedData` type, so a Rust-first public lifecycle also has
to state how that typed namespace is owned.

Decision:
`register_distributed_data<D>` registers all ten stable replicator
request/reply manifests on the ordinary lane of an existing
`TcpRemoteActorRuntimeBuilder`. Cluster daemon registration must occur first.
The bind-time factory obtains the materialized cluster handle and shared remote
context, spawns the typed replicator at `/system/ddata` plus its cluster
connector, and uses the shared serialization registry and association cache;
it never binds another listener.

The connector remains the sole bridge from cluster snapshots/events to
distributed-data routing. It derives outbound targets and an inbound canonical
address-to-`ReplicaId` map from eligible live cluster members. Sender-derived
inbound traffic is rejected when the envelope has no valid remote sender or
the canonical address is absent from that map. Reachability does not erase a
live identity; removal does. Association state and sender claims consequently
cannot create membership facts.

Post-bind activation requires the ActorSystem's `ClusterExtension`, installs
one `DistributedDataExtension<D>`, and schedules both connector and replicator
termination before cluster shutdown. The first composed slice enables periodic
full-state/status gossip and delta propagation. The default delta interval is
derived as one fifth of the gossip interval with Pekko's 200-millisecond floor;
an explicit positive interval supports deterministic testing and specialized
deployments. Cluster-derived aggregation targets feed the existing typed
read/write sessions; canonical temporary actor refs cross the wire as reply
recipients, and stable ack/nack/result manifests return through the same
ordinary-lane router. One registration owns the stable `/system/ddata`
manifest namespace for one configured `D` family per ActorSystem; broadening
this into a heterogeneous data registry is future API work rather than type
erasure in the current actor boundary.

Consequences:
- Distributed data and cluster membership now run on one ActorSystem-owned TCP
  listener and association lifecycle.
- Real cluster formation drives replica routes and validates inbound source
  identity without making distributed data or transport a membership authority.
- Applications retrieve the typed replicator and connector through the
  ActorSystem extension after activation.
- A broader multi-type registration facade plus process and fault coverage
  remain later Phase 4 work; the legacy standalone TCP
  bootstrap remains a compatibility and focused-test boundary meanwhile.

## ADR-0115: Cluster Sharding Uses Typed Type Registrations On One Runtime

Status: Accepted

Context:
The sharding crate already contained typed regions, entity-backed shards,
coordinator allocation, remote protocols, handoff, and remember-entity stores,
but applications and tests assembled those components manually. All entity
types share the same stable wire manifests, while a normal ActorSystem must be
able to host more than one typed `EntityTypeKey<M>` without registering the
same manifest repeatedly or exposing an erased message API.

Decision:
`register_cluster_sharding` registers `RoutedShardEnvelope` on the ordinary
lane and the twelve coordinator/region manifests on reliable ordered control
delivery before the shared TCP runtime binds. A single internal router selects
the type-specific inbound handler by canonical recipient path. Type names are
encoded into stable coordinator and region system paths; the internal registry
uses `Any` only to recover an already initialized typed handle, while public
traffic remains `EntityRef<M>`, `ActorRef<ShardingEnvelope<M>>`, and registered
`RemoteMessage` codecs.

Activation installs one non-generic `ClusterSharding` ActorSystem extension.
`init` spawns a coordinator candidate, entity-backed region, envelope router,
and membership connector for the type. The connector derives remote targets
only from cluster snapshots/events and updates targets before forwarding the
same membership fact to coordinator discovery. Initialization requires an
explicit `M` stop message so host-shard dispatch and later handoff share a
fully configured transport rather than recording allocations that cannot be
started. Coordinator replies may reconstruct a local home only when its
`RegionId` is itself valid canonical actor-ref wire data.

Consequences:
- Multiple typed entity kinds share one listener, association lifecycle, and
  manifest router without a global business-message enum.
- A remote requester can buffer, resolve a home on another node, forward the
  encoded business message, and start the remote entity on demand.
- Gossip membership remains the source of coordinator and region targets;
  remoting does not create cluster truth.
- Coordinator singleton failover/state recovery, proxy-only role placement,
  composed remember stores, and process/fault acceptance remain later M9 and
  Phase 4 checkpoints.

## ADR-0116: Cluster Tools Register On The Shared Control Lane

Status: Accepted

Context:
Cluster-tools pubsub and singleton already had stable codecs, focused inbound
adapters, and a combined `ClusterToolsSystemInbound<M>`, but their runnable TCP
bootstrap owned a separate listener and route lifecycle. Composed applications
need those protocols to share the ActorSystem's canonical address,
associations, lane classification, and shutdown ownership with ordinary
remoting and clustering before higher-level singleton and pubsub extensions
can be authoritative consumers of cluster events.

Decision:
`register_cluster_tools_system_inbound<M>` registers all eight stable pubsub
and singleton manifests on the control lane of an existing
`TcpRemoteActorRuntimeBuilder`. Its factory runs after bind with the effective
canonical `UniqueAddress` and the shared association cache, then returns the
existing transport-neutral `ClusterToolsSystemInbound<M>`. The function does
not bind, dial, infer peers, or own shutdown; those responsibilities remain in
the shared remoting and cluster lifecycle.

Consequences:
- Pubsub gossip, serialized pubsub delivery, and singleton handover can enter
  their existing actor boundaries through the same listener and association.
- Stable control classification is configured once by the shared runtime
  builder instead of by a cluster-tools-specific listener.
- The legacy standalone cluster-tools TCP bootstrap remains a focused-test and
  compatibility boundary while public singleton/pubsub extensions and their
  cluster-event connectors are composed in later checkpoints.

## ADR-0117: Distributed Pubsub Is A Typed Shared-Runtime Extension

Status: Accepted

Context:
The pubsub mediator, gossip state, stable wire adapters, and shared inbound seam
were independently runnable, but applications still had to assemble them and
manually supply peers. Pekko's mediator subscribes to cluster membership,
gossips only with known eligible nodes, removes departed buckets, and owns one
dynamic user-message namespace. Kairo must preserve those observable
membership and convergence semantics without adopting an untyped `Any` API or
binding another transport.

Decision:
`register_distributed_pubsub<M>` registers the four stable pubsub manifests on
the control lane of the existing `TcpRemoteActorRuntimeBuilder`. Its bind-time
factory spawns the typed mediator, gossip actor, and a connector that consumes
the real cluster snapshot and later member events. The connector adds
role-eligible Up/WeaklyUp peers, removes Left/Downed/Removed peers, drives a
fixed-delay gossip tick, and forwards accepted deltas into the mediator routing
registry. Activation installs `DistributedPubSubExtension<M>` and orders all
three actors before cluster shutdown.

One registration owns `/system/pubsub` and the common publish/path envelope
manifests for one configured `RemoteMessage` type `M` per ActorSystem. Local
messages remain typed and serialization-free; broad heterogeneous pubsub would
require an explicit stable inner-manifest router rather than a global Rust
message enum or type erasure at the user boundary.

Consequences:
- Subscription convergence and remote publish delivery now run through the
  same cluster-derived associations as actor remoting and membership.
- Transport reachability cannot add a pubsub peer or recreate a removed
  incarnation; cluster membership remains authoritative.
- The typed extension is coherent for one application protocol today, while a
  future multi-protocol router remains an explicit wire/API enhancement.

## ADR-0118: Cluster Singleton Uses Named Stable Delivery Endpoints

Status: Accepted

Context:
Pekko singleton proxies discover the current singleton incarnation rather than
guessing an actor path. Kairo actor paths likewise include per-incarnation UIDs,
so constructing `/system/manager/singleton` from membership alone can target no
actor or, after restart, the wrong incarnation. At the same time, all named
singletons share the same handover manifests and must coexist on one remote
runtime without an erased public message API.

Decision:
`register_cluster_singleton` installs the four handover manifests on reliable
ordered control delivery and `SingletonMessageEnvelope` on the ordinary lane.
Activation installs one non-generic `ClusterSingleton` extension. Each
`init(Singleton<A>)` hashes the logical name with fixed FNV-1a 64-bit and uses
that token for deterministic manager, proxy, delivery, and connector paths. A
single internal inbound registry dispatches shared manifests by canonical
recipient path. The stable business envelope carries the registered serialized
`A::Msg`; its typed handler decodes the inner message and tells a local delivery
actor whose target is refreshed from the live manager child. The public API
remains `ClusterSingletonRef<A::Msg>`.

The connector subscribes to the real cluster snapshot and member events,
applies one `SingletonOldestTracker` result to manager and proxy, and builds
remote typed recipients only for eligible members. `Left` and `Exited` advance
oldest selection while the cluster association is still retained, enabling the
handover controls to finish before `Removed` becomes terminal.

Subsystems whose actor protocol is intentionally local-only use
`ClusterSingleton::init_local`. It runs the same manager, oldest tracker, and
remote handover protocol, but does not register remote business-message targets
for that name. The owning subsystem must retain its own stable wire adapter and
may route decoded commands through the returned typed local proxy. This is the
integration mode for protocols such as the sharding coordinator enum, which
contains local reply refs and must not become a serialization contract.

Consequences:
- Multiple named and differently typed singletons share one listener and stable
  manifest set without a global business-message enum.
- Remote delivery never depends on Rust type names, enum discriminants, memory
  layout, or guessed actor UIDs.
- Business messages require registered `RemoteMessage` codecs only when the
  singleton can route remotely; local actor delivery remains typed.
- FNV-1a path hashing is a wire-visible compatibility rule and must not be
  replaced with `DefaultHasher` or another process-dependent hash.

## ADR-0119: Sharding Owns Stable Coordinator Endpoints Over Cluster Singleton

Status: Accepted

Context:
Pekko hosts each shard coordinator below a cluster-singleton manager, while
regions address a stable coordinator protocol. Kairo's `ShardCoordinatorMsg<M>`
contains local typed refs, replies, and business stop messages, so making that
enum a `RemoteMessage` would violate the stable wire contract and local-message
rules. Spawning one coordinator candidate on every node also leaves ownership
outside the public cluster-tools lifecycle.

Decision:
`kairo-cluster-sharding` depends on the public `kairo-cluster-tools` API.
`register_cluster_sharding_with_singleton` composes and activates
`ClusterSingleton` before `ClusterSharding`. For each entity type, sharding
registers a role-scoped `Singleton<ShardCoordinatorActor<M>>` through
`ClusterSingleton::init_local` and retains the existing stable sharding
coordinator endpoint. That endpoint forwards decoded local coordinator commands
to `ClusterSingletonRef<ShardCoordinatorMsg<M>>`; only the four singleton
handover controls cross the singleton wire boundary, while remote region and
coordinator traffic continues to use sharding's explicit manifests and codecs.

The lower-level `register_cluster_sharding` path keeps direct coordinator
construction when no singleton extension is installed. This remains useful for
single-node and focused compatibility tests, but composed distributed sharding
uses the singleton registration helper.

Consequences:
- Oldest-node handover changes the live coordinator actor without changing the
  documented sharding recipient path or exposing actor incarnation UIDs.
- Sharding coordinator roles map directly to singleton eligibility scope.
- A successor coordinator can rebuild live region registrations and serve new
  allocations; durable recovery of prior allocations remains a separate store
  and lifecycle concern.
- Enabling the facade's `cluster-sharding` feature also enables cluster tools,
  matching the runtime dependency used by distributed coordinator placement.

## ADR-0120: Coordinator Recovery Stores Shard Existence Before Reassignment

Status: Accepted

Context:
A new singleton coordinator must not infer prior allocations from region actor
paths or transport reachability. Pekko's remember-entities coordinator store
persists the set of known shard ids, loads it during coordinator startup, and
reassigns those shards only after current regions register. Kairo already had
that actor/runtime state machine, but the public sharding extension always
constructed a storeless coordinator.

Decision:
`Entity::with_coordinator_remember_store` accepts a typed local
`ActorRef<RememberCoordinatorStoreMsg>` and timeout. Both direct and
cluster-singleton coordinator factories use the same supplied store. A
store-backed coordinator enables remember mode, loads before normal message
processing with a bounded stash, merges loaded shard ids into the unallocated
set, persists a new shard before allocating it, and reallocates remembered
shards after live region registration.

The store owns shard existence only. It does not persist or restore region actor
refs, actor UIDs, transport routes, or cluster membership. Tests may use a
shared in-memory implementation; distributed deployments must provide a store
whose state is available to every eligible singleton owner.

Consequences:
- Selected coordinator failover recovers known shards without replaying a
  business request or reviving a stale region owner.
- Store load/update failure remains explicit through the coordinator ask
  timeout and actor failure path rather than silently dropping persistence.
- The existing distributed-data remember store can be selected through the
  public entity definition; remembered entities also require region/shard store
  wiring in addition to coordinator shard recovery.

## ADR-0121: Coordinator Stores Preserve DData And Ask Failures Explicitly

Status: Accepted

Context:
The memory/custom coordinator store replies with typed values, while the
existing GSet-backed store replies with `Result<_, ShardingError>`. Treating a
ddata failure as an empty shard set would lose recovery state, while converting
every failure to an ask timeout would hide the actual store error.

Decision:
The internal coordinator store target supports both
`ActorRef<RememberCoordinatorStoreMsg>` and
`ActorRef<RememberCoordinatorDDataStoreMsg>`. Ask callbacks flatten the nested
transport result into `CoordinatorRememberStoreError::Ask` or
`CoordinatorRememberStoreError::Store`. `ShardCoordinatorMsg` carries that
explicit result back to the actor. Any load or update failure stops the current
coordinator incarnation so singleton supervision/restart can retry recovery;
no failure becomes empty state.

`Entity::with_coordinator_ddata_remember_store` exposes the ddata target without
erasing either actor protocol or adding serialization to local coordinator
messages.

Consequences:
- Memory and ddata stores share one coordinator lifecycle without a global
  dynamic message API.
- Ddata read/update errors remain distinguishable from local ask timeouts.
- A real distributed deployment still must run compatible ddata store actors
  on eligible owners and validate their replication before failover; this ADR
  defines the typed adapter, not a new membership or persistence authority.
