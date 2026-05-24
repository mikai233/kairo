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
- Fixed-rate timers can be added later without changing the mailbox envelope.

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
