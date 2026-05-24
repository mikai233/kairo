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
