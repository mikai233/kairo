# Progress

## Current Milestone

M2: Lifecycle, Supervision, Patterns, And Testkit is in progress. The M1 local
actor runtime vertical slice is runnable and remains the foundation for M2
work. The M3 serialization foundation has started with stable remote message
metadata and codec registration.

Implemented:

- `kairo-actor` can spawn a typed local actor under `/user`.
- `ActorRef<M>::tell` enqueues typed messages into the actor mailbox.
- `ActorRef<M>` and `IgnoreRef<M>` implement the `Recipient<M>` send boundary.
- Actors process messages one at a time through synchronous `Actor::receive`.
- Actors receive a synchronous `Signal::PostStop` during local termination.
- `Context::stop(ctx.myself())` stops the current local actor ref.
- `ActorSystem::stop` can stop an idle local actor through the system lane.
- `ActorSystem::terminate` stops top-level `/user` and `/system` actors,
  waits for termination, and rejects later spawns.
- `Context::system`, `Context::spawn`, and `Context::spawn_anonymous` are
  available for local actors.
- `ActorSystem::spawn_system` can spawn framework-owned actors under `/system`
  so remoting and later cluster services can use stable system paths without
  occupying `/user`.
- `Context::parent`, `Context::children`, and `Context::child` expose local
  actor-tree introspection.
- `Context::stop` can stop the current actor or a typed direct child actor ref
  without stopping the parent, and returns an explicit error for invalid
  targets.
- `Context::spawn` and `Context::spawn_anonymous` now reject child creation
  once the owning actor is stopping, including during `PostStop`, so stopped
  actors cannot create orphan children under a dead parent path.
- `ActorSystemBuilder::dispatcher_throughput` configures local mailbox batch
  throughput before worker yield.
- `ActorSystemBuilder::mailbox_capacity` configures an optional bounded user
  mailbox for local actors; overflow rejects the send, records a dead letter,
  and leaves the actor running while system messages remain on their separate
  priority lane.
- `ActorSystem` exposes a type-keyed extension registry; extensions are created
  at most once per actor system, retrieved as `Arc<T>`, and report explicit
  lookup errors when missing.
- `ActorSystemBuilder` construction and scheduler/dispatcher wiring now live in
  a focused `system::builder` submodule instead of the actor-system runtime
  operations file.
- Mailbox tests now pin the actor runtime contract that system messages are
  dequeued before already queued user messages while preserving FIFO order
  within the system lane.
- Mailbox tests now also cover bounded user-lane overflow and prove the system
  lane remains available when the user lane is full.
- Stopping a local actor recursively requests child stops, rejects later user
  messages while child termination is still in progress, and runs the parent's
  `stopped` hook and external death-watch notifications after children have
  terminated.
- Actor-system termination now has focused coverage that top-level actor stop
  waits recursively for descendant child termination before the system reports
  `terminated`.
- Actor-system termination retries now keep timed-out child handles visible,
  so a later `terminate` attempt cannot report `terminated` while a
  previously requested child stop is still blocked.
- Actor-system termination now uses one timeout deadline across both `/user`
  and `/system` guardians, so a blocked framework-owned actor cannot receive a
  fresh full timeout after user-guardian shutdown has already consumed part of
  the caller's budget.
- Actor-system termination now requests stops for both `/user` and `/system`
  guardian children before waiting, so a timed-out user actor cannot prevent
  framework-owned system actors from entering shutdown during the same
  termination attempt.
- Sends after stop are rejected and recorded as dead letters.
- Missing local actor refs reject user messages and record dead letters.
- Dead letters are now also published to the local typed event stream as
  `DeadLetter` events, matching Pekko's observable dead-letter subscription
  model while preserving Kairo's deterministic `DeadLetters` record buffer for
  tests.
- System stop drains queued user messages to dead letters before delivery.
- Duplicate live names under `/user` are rejected; stopped names can be reused
  with a new path incarnation.
- Local actor refs, child links, and reserved names remain present through
  `PostStop`; stopping children keep their logical names reserved until
  termination completes, and the names can be reused only after the stopped
  hook and local registry cleanup have run.
- User actor names follow stable actor path element validation; `$`-prefixed
  names are reserved for internal actors such as anonymous children.
- Focused `kairo-actor` tests cover tell ordering, system stop, and post-stop
  dead letters, duplicate names, path incarnation reuse, context system access,
  child spawning, parent/child stop ordering, recipient behavior, and
  `PostStop` signal delivery, missing-ref dead letters, and dispatcher
  throughput settings, actor-system termination, actor name validation, context
  parent/child introspection, context child stop, and invalid context stop
  targets, local death-watch signals, custom watch messages, and unwatch.
- Focused scheduler tests cover delayed delivery, cancellation, and scheduled
  self messages re-entering the actor mailbox.
- Actor-system scheduler entry points now return already-cancelled handles
  after termination has started, so post-termination scheduling does not retain
  delayed work or enqueue dead letters.
- Focused timer tests cover single timers, active-key cleanup, replacement,
  cancellation after enqueue, and actor-stop timer cleanup.
- Focused fixed-delay timer tests cover repeated delivery, cancellation, and
  replacement generation filtering.
- Focused event-stream tests cover typed subscription, duplicate subscription
  suppression, exact event-type matching, publishing, and unsubscribe.
- `kairo-actor` runtime code is split by responsibility across modules instead
  of living in a single `lib.rs`.
- The local actor mailbox run loop, callback panic boundary, supervision
  failure handling, restart helpers, and child-stop wait logic now live in a
  focused `runtime` module instead of being embedded in `system.rs`.
- Actor runtime child-stop waiting and owner-scoped helper-ref cleanup now live
  in a focused `runtime::lifecycle` submodule instead of the mailbox run-loop
  file.
- `kairo-actor` crate docs now explain typed local protocols, why
  `Actor::receive` is synchronous, why local messages do not need
  serialization, and how external work returns through mailbox messages with a
  compile-checked example.
- Local actor name and child-tree bookkeeping now lives in a focused registry
  module instead of being embedded in the system runtime loop.
- Local actor spawning, reserved-name validation, worker-thread launch, and
  failed-spawn registry cleanup now live in a focused `system::spawn`
  submodule instead of the actor-system operations file.
- Actor reference lifecycle handles and termination latches now live in a
  focused `refs::lifecycle` submodule instead of being mixed into the typed
  send-path implementation.
- `ActorPath` now stores structured address, path segments, and incarnation UID
  metadata while preserving the stable display string.
- `Address` exposes explicit construction plus protocol, system, host, and
  port accessors so remote and cluster codecs can round-trip structured node
  addresses without relying on path string parsing as their primary API.
- `kairo-actor` now exposes an `ActorRefProvider` boundary plus a local
  provider that reports root, user, system, and dead-letter guardian refs and
  distinguishes local, missing-local, and non-local paths for later remote
  provider composition.
- The local actor provider now exposes `/temp` and allocates unique temporary
  paths under it; `Context::ask` reply refs use those temp paths instead of
  owner-local helper paths.
- Pending ask reply refs are registered under `/temp` for typed local
  resolution and are removed when the ask completes, times out, or fails to
  send the initial request.
- Actor-owned pending asks are lifecycle scoped: actor stop or restart
  completes the temp reply ref, unregisters it from `/temp`, and rejects stale
  replies instead of delivering them into the stopped or restarted owner.
- Actor-system termination now has focused coverage that outstanding
  actor-owned ask temp refs under `/temp` are cancelled and unregistered when
  their owner stops, and stale replies are rejected.
- Local death watch is available through `Context::watch`,
  `Context::watch_with`, and `Context::unwatch`.
- `Signal::Terminated` is delivered once to local watchers after the watched
  actor terminates; `watch_with` delivers a typed custom protocol message, and
  `unwatch` suppresses later local termination notification.
- Local death-watch cleanup now has focused coverage that a watcher stopped
  before its watched subject is removed from the subject's watcher set, so
  later subject termination does not enqueue stale termination traffic or dead
  letters for the stopped watcher.
- Local death-watch cleanup now also runs before a stopping watcher waits for
  child termination, matching Pekko's rule that a terminating actor unwatches
  its subjects before child-stop waiting can block and preventing stale
  `watch_with` termination messages from reaching dead letters during that
  window.
- Local death-watch restart coverage now pins that a `watch_with`
  registration survives an unrelated actor restart and still delivers the
  custom termination message to the rebuilt actor instance, matching Pekko's
  cell-owned death-watch state.
- Default `Signal::PreRestart` handling now invokes the actor's `stopped`
  cleanup hook before the actor instance is replaced, matching Pekko's default
  restart cleanup expectation while still allowing actors to override
  `signal` for custom pre-restart behavior.
- Restart supervision now builds the replacement actor only after the old actor
  has run `PreRestart` cleanup and restart-time child teardown has completed,
  matching Pekko's observable recreate ordering while keeping Kairo's explicit
  Rust `Props::restartable` factory boundary.
- Watching an already stopped local actor now has focused integration coverage:
  plain `watch` immediately delivers `Signal::Terminated`, while `watch_with`
  immediately delivers the caller's typed custom message.
- `Signal::ChildFailed` now reports a failed direct child to a parent that is
  watching that child for both receive-time and startup failures, while
  non-parent watchers still receive plain `Signal::Terminated`, parent watchers
  do not also receive a duplicate plain termination signal for the same failed
  child, and `watch_with` continues to deliver the caller's custom message.
- `Context::watch` and `Context::watch_with` reject attempts to watch the
  actor's own ref with an explicit `InvalidWatchTarget` error, matching the
  Rust architecture contract that self-watch is not a meaningful lifecycle
  subscription.
- Death-watch registrations reject switching between plain `watch` and
  `watch_with` for the same watched actor without an intervening `unwatch`,
  preserving the Pekko rule that termination-message changes must be explicit.
- Local death-watch coverage now also pins the positive notification-change
  path: after `unwatch`, actors can switch from `watch` to `watch_with` or
  from `watch_with` back to `watch` without receiving stale notifications from
  the old registration.
- Death-watch registration and notification state lives in a focused
  `death_watch` module.
- `ActorSystem::schedule_once`, `Context::schedule_once`, and
  `Context::schedule_once_self` can deliver typed messages after a delay through
  a cancellable local scheduler handle.
- Scheduler state lives in a focused `scheduler` module, and `Cancellable`
  reports cancellation and completion state.
- `Context::start_single_timer`, `cancel_timer`, `cancel_all_timers`, and
  `is_timer_active` provide keyed self timers with generation filtering so
  cancelled or replaced timer messages are discarded even if already enqueued.
- `Context::start_timer_with_fixed_delay` provides keyed repeated self timers
  with the same cancellation and replacement filtering.
- `Context::start_timer_at_fixed_rate` provides keyed repeated self timers that
  schedule against the planned cadence and preserve the same generation
  filtering as single and fixed-delay timers.
- Timer state and envelopes live in a focused `timers` module and active timers
  are cancelled when the owning actor stops.
- `Context::set_receive_timeout`, `cancel_receive_timeout`, and
  `receive_timeout` provide typed local idle notifications that cancel before
  influencing messages and reschedule afterward, using generation-filtered
  receive-timeout envelopes so stale timeout messages are discarded before
  actor `receive`.
- Receive-timeout state and envelopes live in a focused `receive_timeout`
  module, and active receive-timeout tasks are cancelled when the owning actor
  stops or restarts.
- `CoordinatedShutdown::add_cancellable_task` now returns a
  `ShutdownTaskHandle` that can cancel pending task registrations before their
  phase starts, while duplicate task names remain distinct registrations and
  earlier phases can cancel later-phase work.
- `CoordinatedShutdown::add_actor_termination_task` now preserves Pekko's
  wait-only semantics when no stop message is supplied; cluster,
  distributed-data, and cluster-tools TCP bootstrap facades register explicit
  connector stop tasks for coordinated shutdown.
- Cluster, distributed-data, and cluster-tools TCP bootstrap facades now spawn
  their framework-owned connector actors under `/system` with
  `ActorSystem::spawn_system`, keeping internal socket services out of `/user`
  while preserving their coordinated-shutdown stop tasks.
- Distributed-data TCP peer connector coverage now pins that a failed dial's
  pending reconnect is cleared when the peer leaves cluster membership before
  a retry succeeds, so removed peers do not keep stale retry state.
- Cluster TCP peer connector coverage now pins the same stale pending-reconnect
  cleanup when a peer leaves membership before retry, keeping socket route
  lifecycle behavior aligned with distributed-data.
- Cluster-tools TCP peer connector coverage now pins the same stale
  pending-reconnect cleanup when a peer leaves membership before retry, keeping
  pubsub and singleton socket route lifecycle behavior aligned with cluster and
  distributed-data.
- Cluster, distributed-data, and cluster-tools TCP peer connector coverage now
  pins explicit live-route clearing: each actor-backed connector removes an
  installed association route, records the removed target in the last route
  report, clears active targets, and leaves the error state clear.
- Cluster TCP peer connector route application now runs through queued
  actor-owned tasks so blocking TCP dials do not hold the actor message turn;
  connector snapshots are served from cached runtime state and task
  completions re-enter through the mailbox.
- TCP association shutdown now treats explicit stop as a stopped reader
  supervision state, opportunistically collects finished lane readers, and
  detaches late socket readers so connector actor termination is not paced by
  OS socket read wakeups.
- `RemoteSettings` now carries an optional TCP connect timeout while retaining
  the existing one-second default; socket-heavy tests use short explicit
  timeouts for deterministic failed-dial retry coverage.
- `kairo-cluster` TCP peer bootstrap coverage now drives the failed-dial
  pending reconnect path through the bootstrap facade and proves the retry
  state is cleared when gossip removes the peer before retry.
- `kairo-distributed-data` TCP peer bootstrap coverage now drives the same
  failed-dial pending reconnect path through the bootstrap facade and proves
  gossip removal clears retry state before the replicator route ever succeeds.
- `kairo-cluster-tools` TCP peer bootstrap coverage now drives the same
  failed-dial pending reconnect path through the bootstrap facade and proves
  gossip removal clears retry state before pubsub or singleton routes succeed.
- Coordinated-shutdown phase metadata, run state, and task execution now live
  in focused submodules instead of one mixed implementation file.
- Focused coordinated-shutdown tests now pin `run_from` phase selection and
  explicit unknown-phase errors.
- `ActorSystem::event_stream` and `Context::event_stream` expose a local typed
  event stream for exact Rust event types.
- Event-stream publication now delivers outside the subscription-table lock so
  failed subscribers can be removed and their resulting dead letters can be
  published without recursively deadlocking the stream.
- Event-stream subscription state lives in a focused `event_stream` module.
- `Context::spawn_task` starts external local work with only a typed self ref,
  and `Context::pipe_to_self` maps task success or failure back into the
  actor's protocol through the normal mailbox.
- Actor-owned task sends are lifecycle scoped: task-originated messages through
  `Context::spawn_task`/`pipe_to_self` are rejected once the owner stops or
  restarts, so stale task completions cannot re-enter the restarted actor.
- Task bridge state and handles live in a focused `tasks` module.
- `Context::message_adapter` creates typed local adapter refs that enqueue
  adapted protocol messages into the owning actor's mailbox.
- Message adapter refs now terminate with their owning actor, notify local
  death-watch subscribers for the adapter path when the owner stops or
  restarts, and discard already queued stale adapter messages after lifecycle
  cancellation.
- Adapted user-message envelopes live in the mailbox runtime, and adapter ref
  construction lives in a focused `adapters` module.
- `Props::with_stash_capacity` enables opt-in typed stash support, and
  `Context::stash`, `unstash`, `unstash_all`, `clear_stash`, and stash
  inspection helpers provide FIFO replay before later mailbox messages with
  explicit disabled/full errors.
- Stash state lives in a focused `stash` module rather than being embedded in
  the actor runtime loop.
- `Context::ask` creates a one-shot typed local reply ref, maps replies or
  `AskError::Timeout` back into the owning actor's mailbox, and rejects late
  replies after completion.
- Ask timeouts now use the actor system scheduler instead of a dedicated
  sleeping thread, so manual-scheduler tests can deterministically advance ask
  timeout delivery and completed asks cancel their pending timeout task.
- Ask state and timeout handling live in a focused `asks` module.
- `ActorSystem::receptionist` and `Context::receptionist` expose a local typed
  receptionist with `ServiceKey<M>`, `Listing<M>`, register, deregister, find,
  subscribe, immediate listings, update publication, and actor-termination
  cleanup.
- Local receptionist coverage now pins Pekko's multi-key cleanup semantics:
  when one actor is registered for several service keys, actor termination
  removes that actor from each listing and publishes empty updates to each
  subscriber.
- Local receptionist coverage now also pins multi-key subscriber cleanup:
  when one subscriber actor is subscribed to several service keys, subscriber
  termination removes it from each bucket so later service updates do not send
  stale listings to dead letters.
- Local receptionist state lives in a focused `receptionist` module.
- `ActorSystem::coordinated_shutdown` exposes local coordinated shutdown with
  standard phase names, one-shot run semantics, task registration, later-phase
  task registration during a run, actor termination tasks, shutdown reasons,
  and `ActorSystem::run_coordinated_shutdown` for task execution followed by
  top-level actor termination.
- Coordinated shutdown state lives in a focused `coordinated_shutdown` module.
- `Props::with_supervisor` supports explicit stop, resume, and restart
  directives for local actor receive failures; stop remains the default.
- `Props::restartable` provides the reusable actor factory required by restart
  supervision, which cancels actor-owned timers/tasks/asks/adapters, sends
  `Signal::PreRestart` to the old actor value before default child teardown,
  removes death-watch registrations for children stopped by that restart,
  rebuilds actor state, reruns `started`, and preserves the actor ref path.
- Restart supervision now has focused coverage that resolving the preserved
  path through the local actor registry still reaches the rebuilt actor state,
  matching Pekko's stable `self` reference across actor recreation.
- Actor startup failures now enter the same supervision boundary: default and
  resume strategies stop the actor, escalation reports the failed child to the
  parent, and bounded restart retries startup through `Props::restartable`
  without emitting `PreRestart` for an actor that never fully started.
- Actor callback panics in `started`, `receive`, and signal handling are caught
  at the runtime boundary where possible and converted into explicit
  `ActorError` failures, so receive panics follow the configured supervision
  strategy and startup panics enter the startup supervision path.
- Actor signal handler errors now enter the configured supervision strategy
  instead of being discarded; death-watch signal failures can stop or restart
  the watching actor through the same local supervision boundary as receive
  failures.
- `SupervisorStrategy::restart_with_limit` supports Pekko-style bounded
  restarts by allowing a configured number of restarts within a time window,
  stopping the actor when the limit is exceeded, and resetting the count after
  the window elapses.
- Bounded restart supervision now treats a restarted actor's `started`
  failure as another restart failure while budget remains, so restart-time
  startup failures can retry instead of stopping immediately after the first
  failed rebuilt instance.
- Restart supervision now defaults to stopping children and exposes explicit
  child-preserving restart policies for callers that want Pekko-style
  `withStopChildren(false)` semantics without changing the default behavior.
- Child-preserving restart supervision now has focused coverage that a parent
  watch registration for a preserved child survives the parent actor instance
  restart and still delivers the custom termination message when that child
  stops later, matching Pekko's cell-owned death-watch state.
- `SupervisorStrategy::Escalate` now routes a child receive failure back to
  the parent through a structured system message, so the parent applies its own
  supervision policy to the escalated failure. A parent with the default stop
  strategy stops, while a restartable parent can restart and stop its children
  through the existing restart path.
- `BackoffSupervisor` provides a structured on-stop supervisor actor with
  explicit `BackoffSupervisorSettings`, deterministic exponential restart
  delays capped by `max_backoff`, manual or automatic restart-count reset, typed
  child queries, and typed message forwarding to the current child.
- Supervision strategy definitions live in a focused `supervision` module.
- `kairo-testkit` exposes a typed `TestProbe<M>` backed by a local actor and
  queue, plus `ActorSystemTestKit` for creating probe-backed local actor
  systems in tests.
- `kairo-testkit` now exposes a spawn-backed `ActorHarness<M>` for tests
  centered on one actor under the real local runtime, with typed sends,
  owned probe creation, stop assertions, and optional manual time.
- `ActorSystemTestKit` and `ActorHarness` can now create typed dead-letter
  probes by subscribing `TestProbe<DeadLetter>` instances to the local event
  stream.
- `ActorSystemTestKit` and `ActorHarness` can now also create generic typed
  event-stream probes for deterministic assertions on local event
  publications beyond dead letters.
- `TestProbe<M>` can register typed death-watch messages through
  `watch_with`, remove those registrations through `unwatch`, and
  `TestProbe<AnyActorRef>` provides `watch_terminated` and
  `expect_terminated` helpers for deterministic local lifecycle assertions.
- `TestProbe<M>` can now stop its backing probe actor and assert its
  termination with timeout diagnostics, matching Pekko's probe-stop testing
  shape while preserving Kairo's typed actor refs.
- `TestProbe::expect_msg_matching` can now assert a typed predicate while
  returning the original message, improving deterministic assertions for
  structured actor, cluster, sharding, and distributed-data events.
- `kairo-testkit::await_assert` retries result-returning test assertions until
  success or timeout and reports the final error with attempt metadata.
- `TestProbe::receive_messages` collects a fixed number of typed messages
  under one shared deadline and reports how many were received when the
  deadline expires.
- `TestProbe::fish_for_message` classifies probe messages as complete, fail,
  continue-and-collect, or continue-and-ignore under one shared deadline, with
  the fishing outcome API kept in a focused testkit module.
- `kairo-testkit::within` and `TestProbe::within` now provide a Rust-shaped
  shared-deadline scope with explicit remaining-time access for composing
  multiple probe assertions under one timeout.
- `Within::await_assert` retries result-returning polling assertions against
  the surrounding shared deadline, so nested probe and state assertions do not
  accidentally receive fresh independent timeouts.
- `kairo-testkit::ManualTime` can deterministically advance scheduled
  one-shot deliveries to actor refs and supports cancellation through
  `ManualTimeHandle`.
- `ManualTime::expect_no_msg_for` advances manual time and verifies probes
  remain quiet after a short dispatcher settle window, including heterogeneous
  `TestProbe<M>` protocol types through the `NoMessageProbe` boundary.
- `kairo-testkit::MultiNodeTestKit` now owns multiple named local actor systems
  for deterministic integration tests, can create typed probes on specific
  nodes, wires optional manual time per node, can advance all manual-time node
  clocks together for synchronized multi-node scenarios, and reports empty,
  duplicate, unknown, or non-manual node errors explicitly without making cluster
  membership part of the testkit.
- `MultiNodeTestKit` can now spawn typed user actors and framework-owned
  `/system` actors on specific named nodes, letting multi-node cluster,
  distributed-data, sharding, and cluster-tools tests create node-local
  subjects without manually reaching through each node's actor system.
- `MultiNodeTestKit` can now create typed event-stream probes and dead-letter
  probes on specific named nodes, so cluster and sharding integration tests can
  assert node-local lifecycle and diagnostics events without sharing one global
  probe system.
- `MultiNodeTestKit::enter_barrier` now provides named local multi-node phase
  coordination with explicit waiting/passed status, wrong-barrier order errors,
  duplicate-arrival errors, and unknown-node validation for future cluster and
  sharding integration tests.
- `MultiNodeTestKit::await_barrier` now provides timeout-based blocking local
  multi-node phase synchronization, including explicit timeout diagnostics with
  arrived and remaining node sets.
- `MultiNodeTestKit::await_barriers` now runs ordered local multi-node barrier
  phases under one shared timeout budget, matching Pekko's sequential barrier
  synchronization shape while preserving Kairo's explicit result status API.
- `kairo-testkit` crate docs now describe typed probes, batch/fishing
  assertions, await assertions, manual time, multi-node local harnesses, and
  compile-checked examples.
- `kairo-testkit` crate docs now include a rustdoc-checked
  `MultiNodeTestKit::await_barriers` example that coordinates ordered local
  multi-node phases across two named actor systems and shuts the harness down
  explicitly.
- Testkit code is split into focused `probe`, `fishing`, `assertions`,
  `manual_time`, `multi_node`, and `system` modules instead of living in one
  crate root.
- Testkit probe, await-assert, fishing, manual-time, and multi-node tests now
  live in focused sibling test modules instead of one broad crate test file.
- `ActorSystemBuilder::manual_scheduler` can build actor systems backed by a
  manual scheduler, and `ActorSystemTestKit::with_manual_time` wires that
  scheduler into `ManualTime`.
- Manual time now drives `ActorSystem::schedule_once`, single actor timers, and
  repeated fixed-delay/fixed-rate timer backends, and actor receive timeouts
  without real sleeps.
- Receive-timeout scheduling now happens after actor startup or a completed
  influencing message turn rather than inside `set_receive_timeout`, preventing
  stale timeout generations when manual time advances immediately after an
  in-callback acknowledgement.
- `kairo-serialization` is split into focused `message`, `manifest`, `codec`,
  `registry`, `envelope`, and `errors` modules.
- `kairo-serialization` crate docs now explain that local actor messages do
  not need serialization, while remote messages require stable manifests,
  versions, serializer ids, registered codecs, and compile-checked examples.
- `RemoteMessage`, `MessageCodec<M>`, `DynCodec`, `SerializedMessage`, and
  `RemoteEnvelope` define the stable metadata and payload boundary for remote
  messages.
- `RemoteInbound::with_diagnostics` can attach a backend-neutral
  `RemoteInboundDiagnostics` sink that records structured inbound
  serialization failures and delivery failures with recipient, optional sender,
  wire manifest/version, serializer id, and error reason.
- Remote inbound diagnostics now also cover registered-but-wrong wire message
  types: if the `(serializer_id, manifest)` codec decodes a message that does
  not match the typed inbound boundary, delivery is skipped and a structured
  serialization failure is reported with the original wire metadata.
- `ActorSystemRemoteInbound` can now carry that same
  `RemoteInboundDiagnostics` observer through the actor-system inbound frame
  router, so runtime-composed business message decode and local-delivery
  failures emit structured diagnostics before TCP socket wiring selects a
  concrete logging or metrics backend.
- `RemoteInboundDiagnosticFilter` and
  `DiagnosticsConfig::remote_inbound_diagnostics` now map the
  `[observability.diagnostics]` serialization and remote-delivery flags onto
  caller-provided remote inbound observers, returning no observer when both
  categories are disabled and preserving backend-neutral diagnostics.
- `RemoteAssociation::with_diagnostics`,
  `RemoteAssociationDiagnosticFilter`, and
  `DiagnosticsConfig::remote_association_diagnostics` now surface structured
  association quarantine diagnostics with remote address, optional remote UID,
  and reason, while honoring the
  `[observability.diagnostics].quarantine_events` flag.
- `ClusterEventPublisher::with_diagnostics`,
  `ClusterDiagnosticFilter`, and `DiagnosticsConfig::cluster_diagnostics` now
  surface backend-neutral gossip state-change diagnostics with previous gossip,
  current gossip, and computed cluster-event diff data, while honoring the
  `[observability.diagnostics].gossip_state_changes` flag.
- `kairo-serialization::WireWriter` and `WireReader` provide a small shared
  stable binary helper for explicit system-protocol codecs, using
  length-prefixed UTF-8 strings and byte payloads, optional strings, boolean
  markers, and big-endian numeric fields instead of Rust memory layout.
- `Registry` implements explicit codec registration, outbound type lookup, and
  inbound `(serializer_id, manifest)` lookup.
- `Registry::deserialize_dyn` now decodes wire messages through the registered
  `(serializer_id, manifest)` codec into the dynamic runtime boundary while
  preserving the wire version for rolling-compatible codecs.
- Serialization registration rejects empty manifests, duplicate serializer ids,
  and duplicate manifests.
- Typed deserialization now rejects unexpected wire manifests before dynamic
  decoding, giving remote and system inbound paths explicit metadata
  diagnostics instead of generic type-mismatch failures.
- Focused serialization tests prove wire metadata includes serializer id,
  manifest, version, and bytes, and does not depend on Rust type names or enum
  discriminants.
- `KairoRemoteMessage` derive parses
  `#[kairo(manifest = "...", version = N)]` and emits only the
  `RemoteMessage` metadata implementation; it does not select or generate a
  codec.
- `KairoRemoteMessage` derive integration coverage now also pins split
  manifest/version attributes and enum message protocols, matching the
  documented public metadata shape without generating codecs or relying on enum
  discriminants.
- `kairo-actor-macros` crate docs now document that macro support is
  metadata-only, local messages do not need macros or serialization,
  serializer ids/codecs remain explicit, and `KairoRemoteMessage` has a
  compile-checked manifest/version example.
- `ActorRefWireData` stores serialized actor-ref paths with explicit protocol,
  system, host, and port metadata, and `ActorRefResolver` defines the provider
  boundary for formatting and resolving those refs later.
- `ActorRefWireData` now rejects addressed remote paths that omit a port, and
  rejects structured host/port metadata where only one side is present, keeping
  actor-ref wire addresses aligned with Pekko's `system@host:port` remote
  address shape.
- `RemoteEnvelope` carries actor-ref wire data for recipient and optional
  sender rather than unstructured path strings.
- `kairo-remote`, `kairo-cluster`, `kairo-distributed-data`, and
  `kairo-cluster-sharding` now have focused `protocol` modules declaring the
  first stable `RemoteMessage` manifests for remote watch/heartbeat, cluster
  gossip, distributed-data replicator, and sharding coordinator protocols.
- `kairo-remote` is split into focused modules for settings, errors, outbound
  delivery, association state, provider resolution, remote refs, and protocol
  metadata instead of concentrating remote logic in the crate root.
- `kairo-remote` crate docs now describe typed remote refs, provider
  resolution, stable remote message metadata and codec registration,
  association/stream module boundaries, remote death-watch semantics, and a
  compile-checked outbound send example.
- `RemoteActorRef<M>` serializes `RemoteMessage` values through the registry
  into `RemoteEnvelope` values, preserves optional sender actor-ref wire data,
  implements the typed `Recipient<M>` boundary, and returns rejected messages
  on serialization or outbound failures.
- `RemoteActorRefProvider` resolves stable remote actor-ref paths with explicit
  host metadata into typed `RemoteActorRef<M>` values and rejects local-only
  paths instead of silently treating them as remote refs.
- `RemoteActorRefProvider` remote-only resolution now also rejects this
  provider's owned canonical `system@host:port` address; callers that need
  Pekko-style local-or-remote resolution use `resolve_actor_ref`, which maps
  owned canonical addresses back to local refs and preserves foreign addresses
  as remote refs.
- `RemoteActorRefProvider` can now compose with the actor crate's
  `LocalActorRefProvider` boundary for owned local-path resolution, while
  retaining the existing actor-system convenience constructor.
- `RemoteActorRefProvider` and the TCP remote actor-system resolver now have
  focused coverage that owned canonical `/system` actor paths resolve back to
  local framework-owned actors, preserving the same local/remote composition
  boundary used by remote watch, cluster, distributed-data, and cluster-tools
  services.
- `RemoteActorRefProvider::resolver::<M>()` now exposes a typed
  `ActorRefResolver` adapter for deserialization-style actor-ref resolution,
  mapping owned canonical addresses back to local `ResolvedActorRef<M>` values
  and preserving foreign addressed paths as remote refs.
- `RemoteActorRefProvider` now formats local `ResolvedActorRef<M>` values with
  the provider's canonical `system@host:port` address, preserves existing
  remote-recipient wire metadata for remote refs, and rejects local refs from
  another actor system instead of producing a misleading remote address.
- `RemoteAssociation` records the initial association state transitions for
  idle, handshaking, active, quarantined, and closed remoting links.
- `kairo-remote::register_remote_protocol_codecs` registers stable explicit
  codecs and serializer ids for remote watch, unwatch, heartbeat,
  heartbeat-ack, and address-terminated system messages using length-prefixed
  actor-ref paths and big-endian numeric fields.
- `RemoteInbound<M>` provides the first inbound envelope pipeline by
  deserializing `RemoteEnvelope` payloads through the registry and passing the
  typed message, recipient wire data, and optional sender wire data to an
  explicit local delivery boundary.
- `kairo-remote` now has a transport-neutral remote envelope frame codec that
  encodes recipient/sender actor-ref wire data, serializer id, manifest,
  version, and payload bytes using explicit big-endian fields and rejects
  invalid frame magic, unsupported frame versions, and truncated payloads before
  a concrete TCP transport is introduced.
- `kairo-remote` now has a focused transport bridge module that adapts
  `RemoteOutbound` envelope sends to framed byte sinks and adapts framed bytes
  back into the typed `RemoteInbound` delivery pipeline while preserving
  explicit send, frame, and codec failures.
- `kairo-remote` now has focused remote stream framing for future TCP
  associations, including explicit connection magic, stable control/ordinary/
  large stream ids, big-endian frame lengths, incremental decode, max-frame
  rejection, and truncated stream detection.
- `kairo-remote` now has a focused outbound lane module that classifies
  stable remote system manifests onto the control lane, ordinary business
  messages onto the ordinary lane, optionally routes large encoded frames to a
  large lane, and propagates lane send failures explicitly.
- `kairo-remote` now has a focused stream-sink bridge that writes control,
  ordinary, and large lane frames to independent stream byte sinks, preserves
  one connection header per lane stream, and propagates stream write failures
  with lane context.
- `kairo-remote` now has a focused inbound stream bridge that incrementally
  decodes stream bytes, dispatches complete frame payloads with their stable
  stream id, propagates handler failures, and detects truncated inbound
  streams before TCP socket wiring is introduced.
- `kairo-remote` now has a focused association-guarded outbound wrapper that
  composes association state with any transport outbound, allows sends while
  idle/handshaking/active, rejects quarantined or closed associations before
  forwarding, and propagates inner transport failures explicitly.
- `kairo-remote` now has a focused association inbound bridge that keeps
  control, ordinary, and large lane stream readers separate, decodes lane
  bytes into remote envelope frames, forwards them through typed inbound
  delivery, and propagates invalid stream, truncated lane, and missing codec
  failures before a concrete socket transport is introduced.
- `kairo-remote` now has a transport-neutral association outbound pipeline
  that composes association send gating, lane classification, stream framing,
  and byte sinks into a reusable boundary for future TCP wiring, with tests
  driving serialized envelopes through stream bytes into typed inbound
  delivery.
- `kairo-remote` now has a focused association cache that derives structured
  remote association addresses from explicit actor-ref wire metadata, routes
  remote envelopes to shared outbound association routes, rejects local-only
  recipients before transport send, reports missing routes explicitly, and
  preserves guarded association close/quarantine checks.
- `kairo-remote` now has a focused association route installer that populates
  the shared `RemoteAssociationCache` from concrete stream-lane association
  pipelines, reports replaced routes, supports explicit removal, and keeps
  guarded association state shared with cache-routed sends so closed or
  quarantined associations reject before touching byte sinks.
- `kairo-remote` now has a minimal TCP outbound association dialer: it adapts
  connected `TcpStream` values to `RemoteByteSink`, opens separate control,
  ordinary, and large lane streams for a remote association address, and
  installs the resulting pipeline into the shared `RemoteAssociationCache`.
- `kairo-remote` TCP support is now split into focused `dialer`, `sink`, and
  inbound modules. `TcpAssociationListener` accepts the lane streams for one
  association, and `TcpAssociationStreamReader` drains accepted TCP streams
  through the existing remote stream decoder and `RemoteFrameHandler` boundary.
- Remote TCP inbound support now has focused listener, accepted-association,
  stream-reader, report, and error modules instead of concentrating accept-loop
  ownership, lane-reader joins, stream decoding, reverse-route setup, and
  report types in one file.
- Accepted TCP associations can now spawn one reader thread per accepted lane
  stream and join them through `TcpAssociationReaderHandle`, so ordinary,
  control, and large lanes can be drained independently while sibling lane
  streams remain open.
- TCP listeners can now be moved into a stoppable background accept loop via
  `TcpAssociationListener::spawn_accept_loop`, which accepts complete lane
  associations, starts their independent lane readers, and joins them through
  an explicit `TcpAssociationListenerHandle` report.
- TCP lane streams can now carry an explicit association handshake with stable
  local/remote association addresses, the sender system UID, and lane ids
  before normal stream frames; actor-system TCP runtimes require and validate
  those handshakes so accepted control, ordinary, and large lanes are
  addressed to the local canonical node and come from one remote association
  incarnation.
- TCP listener lifecycle reports now retain accepted handshaken remote
  identities, including address and UID, so later association-registry,
  quarantine, and diagnostics work can build on explicit peer incarnation
  evidence instead of re-parsing socket paths.
- Handshaken TCP associations are now bidirectional at the actor-system
  runtime boundary: listeners install reverse association routes from accepted
  lane stream clones, dialers can spawn dialing-side lane readers, and the TCP
  runtime keeps those reader handles so a single dial can carry typed messages
  in both directions before shutdown joins the readers.
- Cluster, distributed-data, and cluster-tools TCP association runtimes now
  track dial-created outbound pipelines and close their lane sinks during
  shutdown before joining dialing-side readers, so live route registrations
  cannot keep socket lanes open after routes have been cleared.
- `kairo-remote` now has a focused `RemoteAssociationRegistry` with
  address-indexed associations and a stable UID index. Validated TCP handshakes
  can complete associations into the registry, repeated handshakes for the
  same address/UID are idempotent, and UID collisions across addresses are
  rejected explicitly.
- TCP listeners can now install reverse outbound routes for accepted
  handshaken associations, so a concrete TCP actor-system runtime shares one
  route installer for dialed outbound lanes and inbound peer lanes instead of
  treating accepted streams as receive-only sockets.
- `kairo-remote` now has a focused TCP actor-system runtime boundary that
  binds a listener, owns the local association cache, provider, dialer, remote
  death-watch actor, and inbound router composition, and clears outbound
  association routes during shutdown so typed remote refs cannot keep socket
  lanes open after the runtime stops.
- `TcpRemoteActorSystem::resolver::<M>()` now exposes the same typed
  `ActorRefResolver` adapter as `RemoteActorRefProvider`, with runtime-level
  coverage for owned canonical local refs and foreign remote refs.
- The TCP actor-system runtime now spawns its remote death-watch actor under
  `/system/remote-watch`, aligning the local actor path with the stable system
  watcher path used by remote death-watch wire metadata.
- TCP actor-system runtime shutdown now stops the runtime-owned remote
  death-watch actor with an explicit timeout before clearing association
  routes and stopping the listener, so remoting lifecycle ownership includes
  its local system actor instead of leaving it running after socket shutdown.
- TCP actor-system runtimes can now register their shutdown path with local
  coordinated shutdown, and runtime shutdown explicitly closes active dialed
  lane pipelines before joining reader threads so live route registrations
  cannot keep socket lanes open after shutdown begins.
- `kairo-remote` now has a focused TCP reader supervision state machine:
  inbound lane or association reader failures plan full inbound-stream
  restarts by default, finite restart limits can stop inbound streams, and
  late failures after stop are ignored deterministically.
- TCP reader handles now preserve lane identity while joining reader threads,
  expose `TcpAssociationSupervisedReadReport`, and listener lifecycle reports
  include structured reader supervision decisions instead of reducing lane
  failures to untyped error strings.
- `kairo-remote` TCP socket tests now live in focused child modules for
  sink/dialer, association/handshake, and reader supervision, leaving the
  parent TCP test module for shared fixtures.
- `kairo-actor` now keeps a typed local actor-ref registry keyed by exact
  actor path, removes refs before termination is observable, and exposes local
  resolution helpers so remoting can resolve inbound recipients without making
  erased messages part of the user API.
- `kairo-remote` now has a focused local actor inbound delivery adapter that
  resolves remote recipient wire data through an `ActorSystem`, tells the
  typed local actor, and routes unknown or type-mismatched targets through
  missing-ref dead-letter diagnostics.
- Local remote delivery now normalizes recipient paths addressed to the
  system's own canonical remote host/port back to local actor paths before
  registry resolution, while leaving foreign remote addresses as missing refs.
- `kairo-remote` now has a typed `ResolvedActorRef<M>` provider result for
  local-or-remote location transparency. A provider configured with an
  `ActorSystem` resolves local-only paths and owned canonical remote paths
  through the local actor registry, returns missing local refs for unknown
  owned paths, and preserves remote refs for foreign addresses.
- `RemoteActorRefProvider` tests now pin that local `ResolvedActorRef<M>`
  delivery does not require a registered codec even when the ref was resolved
  through the remote provider, and that owned canonical remote paths for
  missing local actors normalize back to missing local refs without a codec,
  preserving the local-message/no-serialization boundary while keeping remote
  sends codec-backed.
- `kairo-remote` now has a focused remote death-watch state module that tracks
  watched remote actor pairs and watched addresses, plans heartbeat
  start/stop/send effects, re-watches after remote UID changes, emits
  address-terminated effects for unreachable addresses, and resets failure
  detection when watching resumes after an unreachable observation.
- Remote death-watch state tests now live in a focused sibling test module
  instead of the production state-machine file, preserving coverage for watch,
  unwatch, heartbeat, remote UID, inbound watch, and unreachable transitions.
- Remote death-watch state now keeps inbound remote watch registrations
  separate from outbound watch intent, so decoded wire watch/unwatch messages
  record the remote watcher of a local watchee without starting local outbound
  heartbeat monitoring or echoing another watch message back to the peer.
- `kairo-remote` now has an actor-backed remote death-watch command handler
  that wraps the focused state machine in synchronous actor turns, emits
  transport-neutral effects through an explicit sink boundary, handles
  heartbeat ticks, heartbeat acks, unreachable observations, watch/unwatch
  commands, inbound remote watch/unwatch registrations, and reports
  deterministic watch statistics for tests and future diagnostics, including
  ordered watched ref pairs and watched remote addresses in addition to
  counters.
- Remote death-watch actor coverage now lives in a focused sibling test module
  and validates inbound remote unwatch cleanup at the actor boundary, proving
  decoded inbound `UnwatchRemote` commands remove remote watchers without
  producing outbound watch or heartbeat effects.
- `kairo-remote` now has a focused remote death-watch outbound effect sink
  that serializes watch, unwatch, heartbeat, and re-watch effects through the
  registered remote protocol codecs to the stable `/system/remote-watch`
  recipient path on the target address, observes local timer/failure-detector
  effects explicitly, and propagates missing-codec or outbound failures.
- Remote death-watch outbound sink coverage now lives in a focused sibling
  test module and decodes the emitted `WatchRemote`, `RemoteHeartbeat`,
  `UnwatchRemote`, and `RemoteHeartbeatAck` payloads to prove the effect sink
  uses stable registered codecs rather than manifest-only assertions.
- `kairo-remote` now has a focused remote death-watch inbound protocol
  delivery adapter that maps decoded remote watch/unwatch/heartbeat/heartbeat
  ack messages into the actor-backed remote watcher, derives remote addresses
  from stable sender actor-ref wire data, replies to inbound heartbeats with
  local UID heartbeat acknowledgements, and drives re-watch effects from
  heartbeat acks with new remote UIDs.
- Remote death-watch inbound protocol delivery coverage now lives in a focused
  sibling test module and validates that inbound `UnwatchRemote` removes the
  recorded remote watcher without producing outbound watch or heartbeat
  effects.
- `kairo-remote` now has a focused remote death-watch system inbound boundary
  that dispatches remote envelopes by stable manifest, deserializes
  watch/unwatch/heartbeat/heartbeat-ack protocol messages through the
  registered codecs, routes them to the actor-backed remote watcher, and
  rejects unknown death-watch manifests or missing codecs explicitly.
- Remote death-watch system inbound and frame routing now also recognize the
  stable `AddressTerminated` control manifest, preserve its optional remote
  UID, and route it into the actor-backed remote watcher as an unreachable
  address observation instead of treating a registered control-lane protocol
  as ordinary business traffic.
- `kairo-remote` now has a focused inbound frame router that decodes remote
  envelope frames once, dispatches remote death-watch manifests from the
  control lane to the system inbound boundary, routes ordinary business
  manifests to the typed inbound delivery path, and rejects misrouted
  death-watch frames on non-control lanes.
- `kairo-remote` now has an `ActorSystemRemoteInbound` composition boundary
  that wires association lane readers to the inbound frame router, local actor
  recipient resolution, and actor-backed remote death watch so framed inbound
  bytes can reach typed local actors or system watch handlers without exposing
  erased messages as the user API.
- Remote local-delivery coverage now also pins the owned canonical missing-ref
  path: an inbound recipient addressed at this node's canonical host/port is
  mapped back to the local actor path before the missing-ref dead-letter
  diagnostic is recorded.
- TCP actor-system runtime tests now cover the remote death-watch control-lane
  path across a bidirectional association: outbound watch registration reaches
  the peer as an inbound local-watchee registration, heartbeat messages are
  acknowledged over the reverse lane, and the watcher re-sends watch metadata
  after observing the peer UID.
- TCP actor-system runtime tests now also send the stable `AddressTerminated`
  control protocol across a real association route and verify the receiver's
  actor-backed remote watcher observes the unreachable sender address with the
  remote UID preserved.
- `kairo-distributed-data::register_ddata_protocol_codecs` registers stable
  explicit codecs and serializer ids for the initial replicator get, update,
  subscribe, and changed protocol messages.
- `kairo-distributed-data::register_ddata_protocol_codecs` also registers the
  first remote delta-propagation wire messages with explicit sender replica,
  reply flag, per-key CRDT manifest/version/payload metadata, from/to delta
  sequence numbers, and ack/nack responses.
- `kairo-distributed-data::register_ddata_protocol_codecs` now registers
  stable direct read/write aggregation protocol messages, including full CRDT
  envelope payloads, optional sender replica metadata, write ack/nack, and
  optional read results.
- `kairo-distributed-data` protocol codec implementation is split into
  focused client, delta, direct read/write, gossip, and shared wire-helper
  modules while preserving the stable serializer ids and registration facade.
- `kairo-distributed-data` now has focused read/write aggregation wire helpers
  that convert typed `DataEnvelope<D>` values to stable
  `ReplicatorDataEnvelope`, `ReplicatorWrite`, `ReplicatorRead`, and
  `ReplicatorReadResult` messages through explicit CRDT codecs and reject
  manifest mismatches.
- `kairo-distributed-data` now has focused CRDT foundation modules for
  `ReplicatedData`, delta CRDT contracts, `ReplicaId`, `GSet`, `GCounter`,
  `PNCounter`, and `ORSet` instead of concentrating data logic in the crate
  root.
- `kairo-distributed-data` crate docs now describe the structured CRDT,
  replicator state, aggregation, delta, gossip, pruning, cluster-route, and
  remote-association boundaries, plus a compile-checked local CRDT update and
  change-flush example.
- `GSet` preserves immutable add-only union semantics with accumulated deltas;
  `GCounter` stores per-replica absolute counts and merges by maximum value;
  `PNCounter` composes increment and decrement `GCounter`s with explicit
  overflow errors for supported integer bounds.
- `ORSet` preserves Pekko-style observed-remove set semantics with per-element
  dots, version-vector merge pruning, grouped deltas, observed remove winning
  over seen adds, and concurrent add survival.
- `kairo-distributed-data` now exposes `ReadConsistency` and
  `WriteConsistency` values, typed `DataEnvelope<D>`, local `GetResponse`,
  `UpdateOutcome`, change notifications, and `ReplicatorState<D>` for the
  first Pekko-style local get/update/write state transitions.
- `ReplicatorState<D>` stores reset full state after local delta-CRDT updates,
  returns the collected delta for later propagation, merges inbound full state
  by CRDT merge, applies inbound deltas to existing or zero state, and flushes
  changed keys deterministically for subscriber delivery.
- `kairo-distributed-data::ReplicatorActor<D>` wires the local state machine
  into synchronous actor turns for typed local get, update, full-state write,
  delta write, subscribe, unsubscribe, and explicit change flushing.
- Local distributed-data subscriptions send the current value on subscribe when
  present and later deliver queued `ReplicatorChange<D>` notifications only on
  flush, matching Pekko's separated update and notification turns.
- `kairo-distributed-data` now has stable built-in CRDT data codecs for string
  `GSet`, `GCounter`, and `PNCounter`, with explicit manifests, codec version
  metadata, deterministic sorted encoding, and big-endian counter values.
- `kairo-distributed-data::DeltaPropagationLog` tracks per-key delta sequence
  numbers, merges unsent deltas per target, advances sequence numbers for
  no-payload updates, selects remote replicas by Pekko-style round-robin
  slices, and cleans delta entries after all current targets have seen them.
- `kairo-distributed-data` now has focused delta wire translation helpers that
  convert `DeltaPropagation<Delta>` batches into remote protocol messages using
  registered CRDT data codecs, and reject manifest/version mismatches on
  decode.
- `DeltaPropagationTransport` publishes collected delta batches to typed
  `Recipient<ReplicatorDeltaPropagation>` targets, skips empty batches,
  reports missing targets and send/encode failures explicitly, and keeps the
  transport-facing orchestration separate from CRDT state and codecs.
- `kairo-distributed-data` now has a focused transport-neutral replicator wire
  bridge that maps delta propagation, direct write, and direct read protocol
  messages to serialized stable replicator payloads with target-replica
  metadata, and routes inbound serialized payloads into actor-backed
  delta/write/read receive paths with explicit CRDT codecs and reply refs.
- `kairo-distributed-data` now has a separate reply wire bridge for
  delta ACK/NACK, direct write ACK/NACK, and direct read-result payloads. Reply
  envelopes carry explicit source and target replica metadata, outbound reply
  routing uses the original request sender replica, and inbound decoding
  preserves the source replica for future aggregator orchestration.
- `kairo-distributed-data` now has actor-backed read/write aggregation
  receivers that consume decoded replica replies in synchronous actor turns:
  write aggregators complete on quorum success, impossible quorum, or timeout,
  emit full-state retry effects for delta NACKs, and read aggregators merge
  source-tagged read results once per replica before reporting success,
  not-found, decode failure, or timeout.
- `kairo-distributed-data` now has focused read/write aggregation operation
  adapters that translate short-lived aggregator completion events into public
  `UpdateResponse` and `GetResponse` values, while keeping full-state retry
  and read-decode diagnostic effects structured separately from public replies.
- `kairo-distributed-data` now has actor-backed read/write aggregation session
  actors that own the temporary operation lifecycle: they spawn short-lived
  aggregator children, publish initial primary read/write requests through the
  existing aggregation transport, retry full-state writes to a replica after
  delta NACKs, map terminal outcomes to public replies, and stop themselves.
- `ReplicatorActor<D>` can now opt into a focused aggregation spawner boundary
  so public non-local `Get` and `Update` commands create temporary aggregation
  session children, send remote read/write requests through the configured
  aggregation transport, and return timeout/failure responses through the
  normal public reply refs when remote replies do not arrive.
- `AggregationTransport` now supports sender-aware read/write delivery for
  remote-envelope targets. Temporary aggregation sessions encode their spawned
  aggregator child refs as stable `ActorRefWireData` senders, and
  `ReplicatorRemoteEnvelopeOutbound` preserves that sender metadata so remote
  ACK/NACK/read-result replies can target the aggregation child.
- Aggregation sessions and `ReplicatorAggregation` can now be configured with
  the node's remote settings so temporary aggregation child sender refs are
  published in canonical `system@host:port` form before crossing remote
  sender-aware transports.
- `kairo-distributed-data` now has a focused inbound remote-reply bridge for
  sender-addressed aggregation replies. `ReplicatorRemoteReplyInbound` decodes
  stable ACK/NACK/read-result manifests, tags them with the source
  `ReplicaId`, resolves the `RemoteEnvelope` recipient `ActorRefWireData` to a
  local temporary write or read aggregation child, and routes missing or
  mistyped targets through normal actor dead-letter diagnostics.
- `ReplicatorRemoteReplyInbound` can now be constructed with the local remote
  settings so replies addressed to the node's canonical `system@host:port`
  actor-ref form normalize back to local temporary aggregation child paths,
  matching the remote provider's local-address resolution behavior.
- `kairo-remote` now exposes the canonical local-address helper used by
  provider resolution and local inbound delivery, and distributed-data
  aggregation sender publication plus remote-reply recipient resolution share
  that helper instead of duplicating actor-ref path rewriting rules.
- `kairo-distributed-data` now has a transport-neutral remote request/reply
  bridge for direct replicator traffic. `ReplicatorRemoteRequestInbound`
  validates the addressed local replicator `RemoteEnvelope`, decodes stable
  delta/write/read manifests, applies them through the actor-backed
  `ReplicatorActor`, and spawns short-lived reply adapters that send
  ACK/NACK/read-result `RemoteEnvelope` replies back to the original sender
  actor ref.
- `ReplicatorRemoteReplyOutbound` can now serialize direct delta/write/read
  replica replies into `RemoteEnvelope` messages addressed to the original
  request sender actor ref, so direct remote requests and temporary
  aggregation sessions share the same Pekko-style sender-based correlation
  mechanism.
- `kairo-distributed-data` now has a focused remote-envelope bridge that wraps
  stable replicator payloads in `RemoteEnvelope` recipient/sender metadata,
  preserving the sender actor-ref wire data needed to correlate remote
  ACK/NACK/read-result replies with actor-backed aggregators without adding
  request ids to the ddata payload manifests.
- `DeltaReceiveTracker` tracks receive-side per-replica/key delta sequence
  numbers and models Pekko-style duplicate, missing, invalid-range, and
  in-order apply decisions before the transport loop is wired in.
- `DeltaReceiveTracker::apply_propagation` decodes complete remote delta
  propagation messages with a CRDT codec, applies each in-order delta, records
  decode failures separately from causal-range statuses, and summarizes
  reply-requested propagations as ack or nack.
- `ReplicatorActor<D>` records local update deltas into the propagation log and
  exposes explicit target-node configuration, propagation collection, and
  cleanup messages for the future remote transport loop.
- `kairo-distributed-data` now has a focused actor-backed delta propagation
  tick loop. `DeltaPropagationLoop` publishes collected deltas through the
  existing transport-neutral `DeltaPropagationTransport`, cleans retained
  delta entries on a configurable Pekko-style tick divisor, and
  `ReplicatorActor` can run the loop either by explicit testable tick messages
  or scheduled one-shot self ticks driven by manual time.
- `kairo-distributed-data` now has focused removed-node pruning state
  bookkeeping. `PruningState`, `PruningTable`, and
  `RemovedNodePruningTracker` model Pekko-style initialized/performed pruning
  marker merge rules, all-reachable-clock dissemination delays, seen markers,
  obsolete performed-marker cleanup, and unknown-modified-replica collection
  without folding the logic into the crate root.
- `DataEnvelope<D>` now carries a structured `PruningTable`, preserves
  pruning metadata across full-state and delta merges, and exposes focused
  envelope operations for initializing removed-node pruning, recording seen
  markers, performing owner-collapse pruning, cleaning performed removed-node
  data during merges, and removing obsolete performed markers.
- `kairo-distributed-data::RemovedNodePruning` defines the CRDT pruning
  contract used by envelopes. `GCounter`, `PNCounter`, and `ORSet` implement
  removed-replica collapse and cleanup, while `GSet` explicitly provides the
  no-op implementation appropriate for data without per-replica dots.
- Distributed-data full-state wire envelopes now carry pruning metadata with
  explicit removed-replica entries and tagged initialized/performed states,
  preserving owner, seen replicas, and obsolete times across write and
  read-result codecs without relying on Rust enum discriminants.
- `ReplicatorState<D>` and `ReplicatorActor<D>` now run removed-node pruning
  through explicit synchronous actor messages: ticks collect unknown modified
  replicas, wait for the all-reachable dissemination threshold, initialize
  pruning markers when the local replica is leader, record seen markers,
  perform owner-collapse pruning after all live replicas have seen the marker,
  and remove obsolete performed markers.
- `kairo-distributed-data` now has a focused cluster-route state module that
  maps cluster member and reachability events into sorted remote replica sets,
  reachable-first aggregation inputs, removed-replica pruning candidates, and
  leader status without embedding cluster membership bookkeeping in the
  replicator actor.
- `ReplicatorActor<D>` can apply structured cluster-route updates in one
  synchronous turn, updating read/write aggregation replicas, delta
  propagation nodes, unreachable replica selection, removed-node pruning
  tracking, and removed-node delta cleanup together.
- `kairo-distributed-data` now has an actor-backed cluster connector that
  subscribes to `ClusterSubscriptionEvent` with Pekko-style initial event
  replay, owns the distributed-data route state, forwards structured route
  updates to the replicator through typed actor messages, and unsubscribes on
  stop.
- The distributed-data cluster connector now drives removed-node pruning ticks
  from cluster reachability: it maintains a Pekko-style all-reachable clock
  that pauses while matching replicas are unreachable, builds
  `RemovedNodePruningTick` values from current route/leader state and explicit
  pruning settings, and records typed pruning reports from the replicator.
- The distributed-data cluster connector now has focused timing settings and
  an injectable clock, schedules Pekko-style fixed-delay clock/pruning timer
  messages on actor startup, uses monotonic time for all-reachable elapsed
  accounting, and uses wall-clock millis for pruning marker TTL checks while
  remaining deterministic under manual scheduler tests.
- Distributed-data delta and direct read/write transports now use shared
  target registries so cloned loop/session transports observe later route
  registrations, and `ReplicatorRemoteRouteTargets` maps cluster
  `UniqueAddress` routes into stable remote-envelope targets at the documented
  `/system/ddata` replicator path for delta propagation and aggregation
  messages.
- The distributed-data cluster connector can now own remote route-target
  registries and refresh them from cluster route state in the same actor turn
  that forwards route updates to the replicator, so delta propagation and
  direct read/write aggregation sessions can discover remote-envelope targets
  from cluster membership changes.
- The distributed-data cluster connector implementation is now split across
  focused construction, runtime-helper, and test modules instead of keeping
  constructor, actor-turn, route-target, pruning, and fixture logic in one
  large source file.
- `kairo-distributed-data` now has a focused remote-association bridge:
  `ReplicatorRemoteAssociationRoutes` holds shared per-replica
  `kairo-remote` outbound association routes, and
  `ReplicatorRemoteAssociationOutbound` adapts `ReplicatorRemoteEnvelope`
  delivery into those routes with explicit missing-route and send failures.
  Tests cover routing through a real `AssociationOutboundPipeline` into remote
  stream bytes.
- `kairo-distributed-data` remote route targets can now use the shared
  `kairo-remote::RemoteAssociationCache` through
  `ReplicatorRemoteAssociationCacheOutbound`, so cluster-derived
  `/system/ddata` recipients for delta propagation, full-state gossip, and
  direct read/write traffic route through the common association cache without
  registering duplicate per-replica ddata association tables.
- `kairo-distributed-data` now has a focused inbound remote-association
  bridge. `ReplicatorRemoteAssociationInbound` implements the `kairo-remote`
  frame-handler boundary, decodes association frames into `RemoteEnvelope`
  values, dispatches delta/write/read manifests to the request inbound path,
  dispatches ACK/NACK/read-result manifests to the reply inbound path, and
  tags both with the configured source `ReplicaId` for the association.
- `kairo-distributed-data` now has a focused TCP association runtime for a
  configured remote replica. It binds a handshaken TCP listener, owns a shared
  `RemoteAssociationCache`, association registry, route installer, dialer, and
  dialing-side lane readers, and routes bidirectional `/system/ddata`
  request/reply envelopes through the same socket association primitives used
  by actor remoting.
- The distributed-data TCP association runtime now explicitly closes active
  dialed outbound lane pipelines during shutdown, with coverage retaining a
  live route registration across shutdown to prove reader joins complete.
- `kairo-distributed-data` now has a focused TCP peer-route owner that consumes
  cluster membership-derived dial/remove plans, applies them to
  `ReplicatorTcpAssociationRuntime`, keeps route registrations separate from
  membership state, and closes full outbound pipelines plus cached ddata routes
  when peers become locally unreachable or leave.
- `kairo-distributed-data` now has a pure TCP peer-reconnect state machine for
  distributed-data peer routes, with validated retry settings, per-peer attempt
  counts, deterministic due-time selection, and clear-on-success/remove
  behavior ready for actor/runtime integration.
- `kairo-distributed-data` now has a TCP peer runtime that owns the cluster
  peer planner, distributed-data route owner, reconnect state, and configured
  TCP association runtime together, applying membership snapshots/events,
  retrying due failed dials, and clearing active routes plus pending reconnects
  during shutdown.
- Distributed-data TCP peer runtime shutdown now has focused lifecycle coverage
  proving that a failed dial's pending reconnect is cleared and reported even
  when the peer never becomes reachable.
- Distributed-data TCP peer runtime tests now live in a focused sibling test
  module, keeping the runtime implementation file focused on routing,
  reconnect, and shutdown behavior.
- `kairo-distributed-data` now has an actor-backed TCP peer connector that
  subscribes to cluster snapshots/events, applies membership-derived ddata peer
  routes through `ReplicatorTcpPeerRuntime`, drives explicit and timer-based
  retry turns, exposes typed snapshots for deterministic tests, and shuts down
  the owned runtime when the connector actor stops.
- Distributed-data TCP peer connector socket tests now live in a focused
  sibling test module with serialized live listener fixtures, keeping the
  production connector module focused on actor/runtime behavior.
- `kairo-distributed-data` now has a TCP peer bootstrap facade that binds the
  distributed-data peer runtime, spawns the connector actor with explicit
  settings, and registers coordinated shutdown to stop the connector before
  cluster shutdown so socket cleanup goes through the actor stop path.
- Distributed-data TCP peer bootstrap now has a two-node socket validation:
  two real bound runtimes are spawned through the bootstrap facade, cluster
  membership is published to both connector actors, and each side installs a
  peer route for the other node through the actor-backed connector boundary;
  the same validation now runs coordinated shutdown afterward and asserts the
  live-route connector stops through the registered bootstrap shutdown task.
- The two-node distributed-data TCP peer bootstrap validation now uses
  `kairo-testkit::MultiNodeTestKit` to own named node systems and create
  per-node connector snapshot probes through the structured local multi-node
  harness.
- Distributed-data TCP peer bootstrap now validates payload delivery over an
  installed membership-derived peer route: a real bootstrap-owned sender
  association cache carries a stable-codec `ReplicatorRead` envelope to the
  remote inbound request receiver, preserving addressed refs and sender
  replica metadata before coordinated shutdown. The delivery scenario now uses
  `kairo-testkit::MultiNodeTestKit` so both live bootstrap-owned actor systems
  and their final shutdown are owned by the structured multi-node harness.
- Distributed-data TCP peer bootstrap now also has a three-node full-mesh
  socket validation: three real bound runtimes are spawned through the
  bootstrap facade, membership-derived routes from the first node carry stable
  `ReplicatorRead` request envelopes to both peer request receivers with
  preserved sender and recipient wire refs, and each connector installs routes
  for both remote peers before coordinated shutdown stops all live-route
  connectors; the same scenario now publishes a reduced two-node membership
  view and validates the surviving nodes remove the departed node's active
  route through the connector boundary. The same route-reduction scenario now
  also proves sends to the removed peer reject through the association cache
  and do not deliver another request to that removed peer, while the remaining
  route still carries stable `ReplicatorRead` traffic. The test now also pins
  the underlying association-cache route counts across full mesh, reduced
  membership, and coordinated shutdown cleanup for all three bootstrap-owned
  runtimes.
- Distributed-data TCP peer bootstrap lifecycle coverage now validates
  membership removal: after two bound runtimes install socket routes from a
  shared gossip snapshot, publishing a sender-side snapshot without the peer
  removes that peer route from the actor-backed connector before shutdown.
- Distributed-data TCP peer bootstrap lifecycle coverage now also validates
  replacement peer routing: after removing a departed peer's route, publishing
  a new `UniqueAddress` for a replacement receiver installs a fresh
  membership-derived replicator socket route through the same connector. The
  scenario now sends a stable `ReplicatorRead` through the original route,
  verifies the removed route rejects later sends without delivering to the old
  receiver, then sends and decodes a `ReplicatorRead` through the replacement
  route.
- Distributed-data TCP peer bootstrap tests now live in a focused sibling test
  module, keeping the production bootstrap facade separate from socket fixture
  setup and socket validation data.
- Distributed-data TCP peer bootstrap socket helpers now live in a nested
  `tests::support` module, keeping route assertions, recording receivers,
  registry setup, runtime binding, and coordinated-shutdown fixture code out of
  the scenario tests.
- Distributed-data TCP peer bootstrap socket fixtures now serialize live
  listener setup within the test module and drive explicit retry ticks when a
  route snapshot reports pending reconnects, making concurrent bootstrap
  validation deterministic under load.
- `kairo-distributed-data` CRDT foundation tests for replica ids, GSet, ORSet,
  GCounter, PNCounter, deltas, pruning, and overflow now live in a focused
  sibling test module.
- `kairo-distributed-data` built-in CRDT codec round-trip and rejection tests
  now live in a focused sibling test module.
- `kairo-distributed-data` delta propagation log versioning, node selection,
  cleanup, deletion, and removed-node tests now live in a focused sibling test
  module.
- `kairo-distributed-data` delta propagation wire encoding and registered
  codec rejection tests now live in a focused sibling test module.
- `kairo-distributed-data` delta propagation transport send and missing-target
  tests now live in a focused sibling test module.
- `kairo-distributed-data` delta receive tracker ordering, gap detection, ack,
  and nack tests now live in a focused sibling test module.
- `kairo-distributed-data` read/write consistency and aggregation state tests
  now live in a focused sibling test module.
- `kairo-distributed-data` aggregation wire envelope and pruning marker tests
  now live in a focused sibling test module.
- `kairo-distributed-data` aggregation transport send and missing-target tests
  now live in a focused sibling test module.
- `kairo-distributed-data` aggregation session actor tests now live in a
  focused sibling test module, with the delta-NACK retry scenario asserting
  that the session republishes the manifest-tagged full-state write payload.
- `kairo-distributed-data` read/write aggregation actor scenario tests now
  live in a focused sibling test module, keeping actor receive logic separate
  from probe fixtures and quorum scenario data.
- `kairo-distributed-data` direct read/write receive tests now live in a
  focused sibling test module.
- `kairo-distributed-data` replicator state get, update, merge, delta, and
  flush tests now live in a focused sibling test module.
- `kairo-distributed-data` replicator actor local get/update, non-local
  read/write session, subscribe, and unsubscribe tests now live in a focused
  sibling test module.
- `kairo-distributed-data` replicator actor delta propagation collection,
  manual tick, cleanup, and scheduled tick tests now live in a focused sibling
  test module.
- `kairo-distributed-data` replicator actor inbound causal-delta and remote
  delta-propagation receive tests now live in a focused sibling test module.
- `kairo-distributed-data` replicator actor gossip tick, scheduled tick,
  status response, merge, and reply tests now live in a focused sibling test
  module.
- `kairo-distributed-data` replicator actor remote read/write planning and
  cluster-route planning tests now live in a focused sibling test module.
- `kairo-distributed-data` replicator actor removed-node pruning tick and
  pruning-readback tests now live in a focused sibling test module.
- `kairo-distributed-data` replicator actor remote direct read/write receive
  tests now live in a focused sibling test module.
- `kairo-distributed-data` remote reply outbound/inbound tests now live in a
  focused sibling test module, keeping stable reply-manifest routing coverage
  separate from the remote reply boundary implementation.
- `kairo-distributed-data` remote request inbound tests now live in a focused
  sibling test module, keeping sender-ref reply wiring and unsupported manifest
  coverage separate from the remote request dispatch implementation.
- `kairo-distributed-data` remote target registration tests now live in a
  focused sibling test module, keeping cluster-route fixture setup and
  association-cache outbound assertions separate from target construction.
- `kairo-distributed-data` serialized wire outbound/inbound tests now live in
  a focused sibling test module, keeping stable serializer-id and manifest
  dispatch coverage separate from the wire boundary implementation.
- `kairo-distributed-data` serialized reply wire tests now live in a focused
  sibling test module, keeping reply target validation, serializer-id, and
  source-replica decode coverage separate from the reply wire implementation.
- `kairo-distributed-data` remote envelope routing now lives in focused
  `types`, `error`, `outbound`, and `inbound` child modules, keeping stable
  remote-recipient validation and serialization wrappers out of a single mixed
  implementation file.
- `kairo-distributed-data` aggregation transport now separates target
  registry/sender-aware recipient wiring, transport reports, and publish
  orchestration into focused child modules instead of one large transport file.
- `kairo-distributed-data` full-state gossip planning now separates error
  handling, digest/chunk hashing, status/apply reports, status response
  planning, and apply/create operations into focused child modules.
- `kairo-distributed-data` aggregation operation actors now separate write
  operation handling, read operation handling, shared response mapping, and
  operation tests into focused child modules.
- `ReplicatorActor<D>` construction, client get/update/subscription handling,
  cluster route application, delta propagation ticks, gossip ticks/receives,
  and removed-node pruning ticks now live in focused child modules instead of
  one large actor implementation file.
- `kairo-examples` now includes a runnable distributed-data TCP peer bootstrap
  example, with reusable setup and one-shot reply helpers kept in focused
  example modules instead of placing route orchestration in one binary file.
- `kairo-distributed-data` now has stable full-state gossip status and gossip
  payload manifests, explicit codecs, and a focused gossip planning module
  that builds chunked digest status messages, detects differing or missing
  keys, requests missing remote keys with a reserved not-found digest, applies
  inbound full-state gossip merges, and plans Pekko-style send-back replies
  without folding this logic into the crate root.
- `ReplicatorActor<D>` now has an actor-backed full-state gossip loop:
  configured gossip ticks select reachable remote replicas, publish digest
  status messages through a focused gossip transport registry, receive gossip
  status and full-state gossip in synchronous actor turns, merge inbound state,
  and send Pekko-style gossip or not-found status replies through the same
  remote-envelope route targets used by delta and aggregation traffic.
- `ReplicatorActor<D>` can apply inbound versioned causal deltas through
  `WriteCausalDelta`, update local CRDT state only for in-order deltas, and
  reply with a typed `DeltaReceiveStatus` for future ack/nack mapping.
- `ReplicatorActor<D>` can also apply a complete remote
  `ReplicatorDeltaPropagation` in one synchronous actor turn using an explicit
  CRDT delta codec and returns a `DeltaPropagationReceiveReport` for ack/nack
  mapping.
- `kairo-distributed-data` now has focused read/write aggregation state
  machines with Pekko-style majority/min-cap/additional quorum calculation,
  reachable-first primary/secondary replica selection, write ack/nack progress,
  timeout reporting, and read-result CRDT merge behavior.
- `ReplicatorActor<D>` can configure remote replica reachability and produce
  typed remote read/write aggregation plans with selected primary/secondary
  targets and explicit quorum errors before transport-backed sends are wired
  in.
- `AggregationTransport` publishes planned read/write aggregation messages to
  typed remote-replicator recipients, sends primary replicas separately from
  secondary fallback replicas, and reports missing targets, encode failures,
  and send failures without mixing transport orchestration into CRDT state.
- `ReplicatorActor<D>` can now apply direct remote `ReplicatorWrite` messages
  by decoding manifest-tagged CRDT envelopes, merging full state, and returning
  typed ack/nack results; it can also serve direct remote `ReplicatorRead`
  messages by encoding optional local envelopes as `ReplicatorReadResult`.
- Direct read/write receive behavior lives in a focused distributed-data module
  so remote protocol handling stays separate from CRDT state, aggregation
  planning, and transport send orchestration.
- `kairo-cluster-sharding::register_sharding_protocol_codecs` registers stable
  explicit codecs and serializer ids for the initial region/coordinator
  registration, shard-home, host-shard, start, handoff, stopped, and routed
  shard-envelope protocol messages.
- Cluster-sharding protocol codec implementation is split into focused
  registration, coordinator-message codec, shard-control codec,
  routed-envelope codec, graceful-shutdown codec, wire-helper, and codec-test
  modules instead of concentrating stable sharding wire logic in one file.
- Serialization tests cover rolling-version decode behavior by proving codecs
  receive the wire `version` and can decode older payload shapes under the same
  stable manifest.
- `kairo-cluster-sharding` is split into focused API modules for entity refs,
  type keys, envelopes, hashing, errors, and protocol metadata.
- `ShardingEnvelope<M>` carries entity ids outside business messages, and
  `EntityRef<M>` wraps plain business messages in that envelope before sending
  to the region.
- `kairo-cluster-sharding` now has `ShardingEnvelopeRouter<M>`, a focused
  actor adapter that exposes the `ActorRef<ShardingEnvelope<M>>` boundary
  expected by `EntityRef<M>`, computes shard ids with the documented stable
  hash, and forwards envelopes into the registered shard-region actor.
- Shard IDs use documented 64-bit FNV-1a over entity id bytes with
  `hash % shard_count`; `DEFAULT_SHARD_COUNT` is 100.
- `kairo-cluster-sharding` now has a focused allocation module with
  `ShardAllocations`, a synchronous `ShardAllocationStrategy` boundary, and a
  least-shard allocation strategy that allocates new shards to the least-loaded
  region, limits rebalance rounds by absolute and relative limits, and skips
  rebalance when another shard is already in progress.
- `kairo-cluster-sharding` now has a focused coordinator state module with
  explicit coordinator events for region/proxy registration, termination, shard
  home allocation/deallocation, and remember-entities unallocated shard
  tracking, matching Pekko's event-applied coordinator state shape before the
  actor-backed coordinator is wired in.
- `kairo-cluster-sharding` now has a focused coordinator runtime planner for
  `GetShardHome`: it replies with known shard homes, defers requests while a
  shard is rebalancing, ignores requests before region registration completes,
  excludes graceful-shutdown and terminating regions from new allocations, and
  emits `HostShard` plans after successful least-shard allocation.
- `kairo-cluster-sharding` coordinator runtime now plans rebalance workers from
  the shard allocation strategy, marks selected owned shards in progress,
  includes registered regions and proxies as `BeginHandOff` participants,
  deallocates shard homes on successful completion, retries pending
  `GetShardHome` requests, and clears in-progress state on timeout.
- `kairo-cluster-sharding` now has an actor-backed shard coordinator boundary
  that wraps the focused coordinator runtime in synchronous actor turns,
  accepts explicit registration, shutdown marker, shard-home request,
  rebalance planning, and rebalance-completion commands, and replies with
  deterministic allocation, rebalance, completion, and state snapshot results.
- `kairo-cluster-sharding` shard coordinator actors can now run Pekko-style
  fixed-delay rebalance ticks, expose manual tick messages for deterministic
  tests, track in-progress rebalances in snapshots, and cancel/restart the
  rebalance timer when shutdown preparation changes.
- `kairo-cluster-sharding` now has a transport-neutral handoff delivery
  boundary that sends `BeginHandOff` to all planned handoff participants,
  sends `HandOff` to the owning region after acknowledgement orchestration,
  preserves typed region command recipients for local or future remote refs,
  and reports missing or failed delivery targets explicitly.
- `kairo-cluster-sharding` now has a focused shard-region runtime planner that
  buffers unknown shard messages, requests shard homes once per buffered shard,
  forwards buffered messages when a remote home is learned, starts local shards
  for local homes or `HostShard`, drops handoff buffers to preserve ordering,
  and emits explicit handoff ack/stopped plans.
- `kairo-cluster-sharding` now has an actor-backed shard-region boundary that
  wraps the focused region runtime in synchronous actor turns, accepts explicit
  route, host-shard, shard-home, shard-started, begin-handoff, handoff, and
  shard-stopped commands, and replies with deterministic route, start,
  handoff, and state snapshot plans.
- `kairo-cluster-sharding` now has a focused shard entity runtime planner for
  local shard behavior: first messages start entities, active entities receive
  business messages directly, passivating entities buffer later messages,
  passivation sends the configured stop message once, termination removes or
  restarts entities depending on buffered messages, and shard handoff emits
  stopper or stopped plans.
- `kairo-cluster-sharding` now has an actor-backed shard boundary that wraps
  the focused shard runtime in synchronous actor turns, accepts explicit
  delivery, passivation, entity-termination, handoff, handoff-stopper
  completion, and shutdown-preparation commands, and replies with
  deterministic entity delivery, passivation, restart, handoff, and state
  snapshot plans.
- `kairo-cluster-sharding` now has `EntityActorFactory<M>` and
  `EntityShardActor<M>`, a focused entity-backed shard boundary that spawns
  typed local entity children from shard delivery plans, forwards business
  messages to those children, sends passivation and handoff stop messages,
  watches child termination, and feeds observed termination back into the shard
  runtime.
- `ShardRegionActor<M>` can now be configured with entity-backed local shard
  children, so `EntityRef<M>` routes through the stable shard hash,
  registered region, coordinator allocation, local shard child, and finally
  into the typed entity actor without embedding entity ids in business
  messages.
- `kairo-cluster-sharding` now has focused remember-entity store state for the
  coordinator's add-only remembered shard set and each shard's started/stopped
  entity set, including Pekko-compatible five-key partitioning based on stable
  Java string hashing for future distributed-data-backed storage.
- `kairo-cluster-sharding` now has actor-backed remember-entity store
  boundaries for the coordinator shard set and per-shard entity set, wrapping
  the focused store state in synchronous actor turns with explicit add/get,
  update/get-entities, and deterministic state snapshot protocols.
- `kairo-cluster-sharding` now has a focused distributed-data-backed
  coordinator remember-entity store actor using the Pekko-compatible
  `shard-{typeName}-all` `GSet` key, typed replicator adapters, idempotent
  `AddShard` updates, explicit read/update failures, and deterministic actor
  tests over the local `ReplicatorActor`.
- `kairo-cluster-sharding` now has a focused distributed-data-backed shard
  remember-entity store actor using Pekko-compatible five-key
  `shard-{typeName}-{shardId}-{index}` ORSet storage, eager initial load,
  typed replicator adapters, grouped started/stopped updates by stable Java
  string hash partition, idempotent unknown stops, reload verification through
  a fresh store actor, and explicit read/update failures.
- `kairo-cluster-sharding` shard runtime and actor protocols can now consume
  remembered entity IDs after store load, deterministically mark them active,
  return startup plans for recovered entities, ignore empty IDs, and deliver
  later messages to recovered entities without treating them as first starts.
- `kairo-cluster-sharding` shard runtime and actor protocols can now enable
  remember-entity start writes, buffer first deliveries until the remember
  store confirms the start, and batch additional first-delivery starts into the
  next deterministic remember-store update.
- `kairo-cluster-sharding` shard runtime and actor protocols can now record
  remember-entity stop writes after passivated entity termination, keep entities
  in an explicit remembering-stop state until confirmation, restart buffered
  messages through a follow-up remembered start, and batch stops behind an
  in-flight remember-store update.
- `kairo-cluster-sharding` shard actors can now start in an explicit
  remember-entity loading phase, stash normal shard messages until remembered
  entity IDs are loaded, recover those IDs, and replay the stashed messages in
  order so remembered entities receive direct deliveries and new entities still
  trigger remembered-start store updates.
- `kairo-cluster-sharding` shard actors can now be constructed with a local
  remember-entity shard store actor, ask that store for remembered entity IDs
  during startup, send remembered start/stop updates produced by the shard
  runtime to the store, feed successful store replies back into the runtime,
  chain pending remembered updates, and stop themselves on store read, update,
  or timeout failure so a supervisor can restart the shard.
- `kairo-cluster-sharding` shard actors can now spawn a local remember-entity
  shard store child during startup from explicit store state, load remembered
  entities from that child, and persist remembered start updates through the
  spawned store without requiring callers to provide an external store ref.
- `kairo-cluster-sharding` shard coordinator actors can now load remembered
  shard IDs from a local remember-entity coordinator store during startup,
  stash coordinator messages until the load completes, merge remembered shard
  IDs into unallocated shard state, allocate remembered shards through the
  normal shard-home path, and persist newly allocated shard IDs back to the
  remember store.
- `kairo-cluster-sharding` shard region actors can now opt into local shard
  child spawning, including store-backed shard children with local
  remember-entity store state. `HostShard` can now create a child
  `ShardActor`, mark the shard started, expose the typed child ref for local
  delivery orchestration, and recover remembered entities before child shard
  delivery.
- `kairo-cluster-sharding` shard region actors can now host child shards from
  explicit remember-store actor refs, allowing multiple regions to share the
  same remembered entity store during handoff and rebalance orchestration
  tests instead of creating isolated per-region local stores.
- `kairo-cluster-sharding` shard region actors can now explicitly route local
  sharding envelopes into spawned child shard actors while preserving the
  route outcome separately from the child `ShardDeliverPlan`, so remembered
  entity delivery can flow through the region-owned shard child instead of
  requiring tests or callers to send directly to the child ref.
- `kairo-cluster-sharding` shard region actors can now host a shard and replay
  buffered local routes into the spawned store-backed shard child in FIFO
  order, preserving Pekko's region behavior of draining shard buffers after the
  local shard is initialized.
- `kairo-cluster-sharding` shard region actors can now forward local handoff
  delivery into spawned store-backed shard children with an explicit typed
  stop message, preserving the Pekko region behavior of dropping post-begin
  buffers before handing the shard off to its local child.
- `kairo-cluster-sharding` shard region actors can now complete a local
  store-backed shard child handoff by asking the child for handoff-stopper
  completion, stopping and removing the local shard child, and marking the
  shard stopped in region state.
- `kairo-cluster-sharding` shard region actors now restart remembered local
  shard children after unexpected termination through a configurable failure
  backoff, while preserving Pekko's distinction that handoff and graceful
  shutdown stops are not failure restarts.
- `kairo-cluster-sharding` now has an actor-backed handoff worker that follows
  Pekko's rebalance worker sequence: send `BeginHandOff` to participants,
  wait for acknowledgements, hand off the owner region's store-backed local
  shard child, complete the local handoff, and report whether the shard handoff
  finished successfully.
- `kairo-cluster-sharding` shard coordinator actors can now opt into typed
  handoff orchestration: rebalance plans spawn per-shard handoff worker
  children through a structured coordinator handoff module, worker completion
  is observed through the coordinator mailbox, and successful completion clears
  in-progress rebalance state and deallocates the shard home.
- `kairo-cluster-sharding` coordinator-driven handoff now re-enters the normal
  shard-home allocation path after successful worker completion and dispatches
  `HostShard` to the newly selected region through the structured handoff
  transport.
- `kairo-cluster-sharding` now has a focused local coordinator bootstrap
  helper that builds coordinator region state and handoff transport targets
  from typed local shard-region refs, rejects duplicate region IDs, and lets
  coordinator-driven handoff tests use structured bootstrap data instead of
  duplicating manual state/transport wiring.
- `kairo-cluster-sharding` shard coordinator actors can now accept typed local
  region registrations through the coordinator mailbox. Registration updates
  coordinator region state and the handoff transport target table in one actor
  turn, treats duplicate registrations idempotently like Pekko's `Register`
  path, and allows registered local regions to participate in subsequent
  coordinator-driven handoff workers.
- `ShardCoordinatorActor` constructors, `Props` builders, handoff construction,
  remember-store startup/loading, remembered-shard allocation, and allocated
  shard persistence now live in focused child modules instead of the main
  coordinator actor message-dispatch file.
- `kairo-cluster-sharding` shard region actors can now self-register with a
  local typed coordinator during actor startup, retry registration until the
  coordinator acknowledges it, expose registration status in region snapshots,
  and use the registered typed region target for coordinator-driven local
  handoff without requiring callers to send explicit registration messages.
- `kairo-cluster-sharding` shard coordinator runtime and actors now allocate
  remembered but unallocated shard homes after remembered-shard loading or
  local region registration, mirroring Pekko's remembered-entity coordinator
  behavior. Allocated remembered shards are persisted idempotently and, when a
  typed local handoff transport is available, dispatched back to the selected
  region as `HostShard` so the region starts hosting the remembered shard.
- `kairo-cluster-sharding` now validates the remembered-shard allocation path
  through the multi-node region-discovery harness: a coordinator node starts
  with remembered unallocated shard state, a separate region node discovers and
  registers with that coordinator from cluster membership, and the coordinator
  allocation is observed as an actually hosted region shard. The same scenario
  now hosts the shard through a local remember-entity store and verifies that
  first delivery to the pre-remembered entity goes through the normal region
  local-route boundary and is routed as recovered entity delivery rather than a
  fresh start.
- `ShardRegionActor` remote `HostShard` handling now has focused coverage that
  a region receiving the stable remote host command can spawn a store-backed
  local shard, send the stable `ShardStarted` reply, and recover remembered
  entities before the first local delivery.
- `kairo-cluster-sharding` shard region actors can now request shard homes
  from their registered local coordinator when local delivery buffers the first
  message for an unknown shard. Coordinator replies are applied through the
  normal region runtime, local shard children are started when this region is
  selected, and the buffered delivery is replayed into the child shard actor.
- `kairo-cluster-sharding` now has a structured region route transport for
  forwarding sharded business envelopes to another known shard-region target.
  Region actors can forward later messages for known remote shard homes and can
  replay buffered messages to the selected remote region after shard-home
  resolution while preserving per-message delivery reply refs.
- `kairo-cluster-sharding` now has a focused remote region route bridge:
  `ShardRegionRemoteOutbound<M>` wraps routed sharding envelopes in stable
  `RemoteEnvelope` messages addressed to `/system/sharding/region`, and
  `ShardRegionRemoteInbound<M>` validates that recipient, decodes the nested
  business message through registered codecs, and re-enters typed local region
  delivery.
- Remote region outbound adapters can now be installed directly as
  `RegionRouteTransport` targets, so known remote shard homes use the same
  structured route table as local region refs while producing stable remote
  envelopes for the transport layer.
- `kairo-cluster-sharding` now has focused coordinator discovery state that
  consumes cluster snapshots/events, filters coordinator candidates by
  role/status, preserves Pekko's members-by-age movement detection, and
  computes likely coordinator targets for future region registration without
  folding that logic into the region actor.
- `kairo-cluster-sharding` shard region actors can now accept coordinator
  discovery snapshots/events, use a focused region coordinator-discovery
  bridge to select a typed local coordinator target, and start the normal
  self-registration/retry flow when the selected coordinator appears or moves.
- `kairo-cluster-sharding` now has focused remote shard-coordinator target
  resolution that maps discovered `UniqueAddress` candidates to stable
  `/system/sharding/coordinator` `ActorRefWireData` recipients for future
  remote registration without treating the remote coordinator as a local typed
  `ShardCoordinatorMsg<M>` actor.
- `kairo-cluster-sharding` now has an actor-backed shard-region discovery
  subscriber that owns the cluster subscription, requests an initial cluster
  snapshot, forwards snapshots/events into the region's coordinator-discovery
  messages, and unsubscribes during actor stop.
- Shard-region discovery subscriber coverage now validates coordinator
  movement: a region registers with the first discovered local coordinator and
  re-registers with a newly selected local coordinator after cluster membership
  removes the previous coordinator.
- `kairo-cluster::VectorClock` provides immutable increment, compare, merge,
  and prune operations with Pekko-style `Same`, `Before`, `After`, and
  `Concurrent` ordering semantics.
- `kairo-cluster::Reachability` stores immutable observer/subject records with
  per-observer versions, reachable-by-default semantics, terminated-over-
  unreachable aggregation, pruning of all-reachable observer rows, and
  newest-observer-row merge behavior.
- `kairo-cluster::Gossip` stores members, seen state, reachability, vector
  clocks, and tombstones, and merges them using Pekko-style tombstone,
  vector-clock prune, highest member status, reachability merge, and seen-reset
  ordering.
- Cluster member data now lives in a focused `member` module instead of the
  crate root.
- `kairo-cluster::Convergence` checks Pekko-style gossip convergence for the
  current node, including first-convergence seen requirements, exiting
  confirmations, ignoring DOWN observers, and allowing unreachable DOWN or
  EXITING members to be skipped.
- Cluster members now carry optional `up_number` age metadata, and
  `kairo-cluster::LeaderSelection` chooses the current or role-specific leader
  from reachable non-DOWN members using Pekko-style UP/LEAVING preference and
  fallback status ordering.
- `kairo-cluster::LeaderActions` applies convergence-gated member transitions:
  JOINING/WEAKLY_UP to UP with assigned age, LEAVING to EXITING, and
  unreachable or confirmed DOWN/EXITING removals with tombstones and
  version/seen updates.
- `kairo-cluster::ClusterEvents` computes Pekko-style domain event diffs from
  old and new gossip snapshots, including member status events, removed-member
  events, reachability events, leader and role-leader changes, seen changes,
  reachability summaries, and tombstone changes in publication order.
- Cluster domain event model types, gossip diff logic, and diff tests now live
  in focused `events` submodules instead of one mixed event file.
- `kairo-cluster::DeadlineFailureDetector` and `FailureDetectorRegistry`
  provide deterministic heartbeat monitoring with Pekko-style semantics:
  unmonitored resources are available, first heartbeat starts monitoring,
  resources become unavailable after heartbeat interval plus acceptable pause,
  and removal forgets detector state.
- `kairo-cluster::DowningHook`, `DowningDecision`, and `DowningPlan` define the
  initial downing hook boundary and SBR-style decision mapping for downing
  reachable, unreachable, all, or self-quarantined members and are reused by
  the actor-backed downing provider.
- `kairo-cluster::SplitBrainResolverHook` provides the first concrete
  synchronous downing policies for `down-all`, `keep-majority`, and
  `keep-oldest`, including role-filtered majority decisions, tie-breaking by
  lowest address, oldest-member survival, and `down-if-alone` behavior.
- `kairo-cluster::SplitBrainResolverHook` now detects Pekko-style indirectly
  connected nodes from reachability observer/subject cycles and unreachable
  subjects that have still seen current gossip. Indirect decisions down those
  nodes and combine with the ordinary majority/oldest decision after filtering
  reachability records between indirectly connected nodes.
- `kairo-cluster` downing plan and split-brain resolver tests now live in a
  focused sibling test module instead of the production downing decision file.
- `kairo-cluster::LeaseMajorityHook` provides deterministic Pekko-style
  lease-majority downing with explicit lease acquisition, minority-side acquire
  delay, role-filtered majority/minority calculation, lease-denied reverse
  decisions, and indirect-connection reversal without making the lease a source
  of cluster membership truth.
- `kairo-cluster::DowningProviderActor` now wraps the downing hook boundary in
  an actor-backed stable-after timer: it observes gossip snapshots, tracks
  relevant unreachable members, resets or cancels stable-after and hook-supplied
  decision-delay timers when reachability changes, gates decisions to the
  reachable leader, and sends structured `ApplyDowningDecision` commands to the
  membership actor after the stable period.
- `kairo-cluster` downing-provider coverage now includes a synchronized
  multi-node manual-time scenario: two local node systems observe the same
  reachability snapshot, `MultiNodeTestKit::advance_all` advances their clocks
  together, and only the responsible leader provider emits a downing decision.
- `kairo-cluster` downing-provider actor tests now live in a focused sibling
  test module instead of the production downing-provider file.
- `kairo-cluster::ClusterMembership` can register a typed
  `DowningProviderActor` observer, forwards each current gossip snapshot to it,
  and applies the provider's stable downing decision through the existing
  membership state machine.
- `kairo-cluster` membership actor join, welcome, gossip, downing, and
  reachability tests now live in a focused sibling test module instead of the
  production membership actor file.
- `kairo-cluster` now has a focused live-socket membership/downing validation:
  a remote `Join` crosses a real TCP association into an actor-backed
  membership node, the registered downing provider observes the resulting
  gossip, and a deterministic stable-after timer applies the downing decision
  through the membership actor.
- `kairo-cluster::HeartbeatNodeRing` and `HeartbeatSenderState` model
  Pekko-style heartbeat receiver selection and sender bookkeeping, including
  deterministic ring ordering, configured receiver limits, unreachable receiver
  inclusion, and continued monitoring of removed-but-unavailable receivers until
  recovery.
- Cluster heartbeat ring and sender-state tests now live in a focused sibling
  test module instead of the production heartbeat state file.
- `kairo-cluster::HeartbeatReceiver` and `HeartbeatSender` provide the first
  actor-backed heartbeat I/O slice: current-state initialization, typed
  receiver route registration, periodic tick scheduling, heartbeat request and
  response messages with stable remote manifests, expected-first-heartbeat
  monitoring, cluster membership/reachability event updates, and
  failure-detector cleanup on stop.
- `kairo-cluster` heartbeat sender/receiver actor tests now live in a focused
  sibling test module instead of the production heartbeat actor file.
- `kairo-cluster` now has focused remote-envelope heartbeat routing:
  `HeartbeatRemoteReceiverOutbound` can be registered as a typed heartbeat
  receiver route and sends stable `Heartbeat` payloads to
  `/system/cluster/heartbeatReceiver`, `HeartbeatRemoteReceiverInbound`
  replies to request sender metadata with stable `HeartbeatRsp` payloads, and
  `HeartbeatRemoteResponseInbound` feeds remote responses back into the local
  heartbeat sender's failure detector path.
- Cluster heartbeat remote routing is split into focused error, path,
  outbound-actor, receiver-inbound, response-inbound, and test modules instead
  of concentrating remote heartbeat routing and fixtures in one file.
- `kairo-cluster::ClusterEventPublisher` is an actor-backed cluster event
  publisher that stores the latest gossip, publishes `ClusterEvent` diffs to
  typed subscribers, supports initial state replay as events, handles explicit
  event publication, unsubscribe, and current-state snapshot requests.
- Cluster event-publisher subscription and current-state snapshot data now live
  in a focused submodule instead of being embedded in the actor implementation.
- Cluster event-publisher tests now live in a focused sibling test module
  instead of the production event-publisher actor file.
- `kairo-cluster::Cluster` provides the first public cluster subscription
  facade over the event publisher, including snapshot-first typed subscriptions
  through `ClusterSubscriptionEvent`, replay-as-events subscriptions,
  event-only subscriptions, unsubscribe, current-state requests, and explicit
  publisher-unavailable errors.
- `kairo-cluster::ClusterMembership` provides the first actor-backed membership
  slice: self-join bootstrapping, remote join handling with `Welcome`, gossip
  envelope merge and talkback, reachability observations, explicit downing,
  convergence-gated leader actions, current-state/current-gossip requests, and
  event-publisher updates. Cluster `Join`, `Welcome`, and `GossipEnvelope`
  protocol structs now carry the membership data needed by those transitions
  while retaining stable manifests.
- `kairo-cluster::register_cluster_protocol_codecs` registers stable explicit
  codecs and serializer ids for heartbeat, heartbeat response, join, welcome,
  and gossip envelope messages, including member lists, seen state,
  reachability versions/records, vector clocks, and tombstones.
- Cluster protocol codec implementation is split into focused registration,
  control-message codec, gossip-message codec, wire-helper, and codec-test
  modules instead of concentrating stable membership wire logic and fixtures in
  one file.
- `kairo-cluster` now has a focused transport-neutral membership wire bridge
  that maps typed join, welcome, and gossip membership messages to serialized
  stable cluster protocol payloads with target-node routing metadata, routes
  inbound serialized payloads into the actor-backed membership state machine,
  and uses an actor-backed outbound adapter for welcome/gossip talkback replies
  without adding socket transport or a central membership authority.
- Cluster membership wire bridge tests now live in a focused sibling test
  module, keeping the production bridge file scoped to routing and
  serialization boundaries while preserving the existing join, welcome, gossip,
  and rejection coverage.
- `kairo-cluster` now has focused remote-envelope wiring for membership
  traffic: `ClusterMembershipRemoteEnvelopeOutbound` addresses serialized
  join, welcome, and gossip payloads to `/system/cluster/core/daemon` on the
  target node, can use the shared `kairo-remote::RemoteAssociationCache`,
  rejects local-only targets, and keeps cluster membership state owned by the
  gossip actor rather than remoting.
- `kairo-cluster` now has a focused system inbound router and configured-peer
  TCP association runtime for cluster control traffic. The runtime binds a
  handshaken listener, owns a shared `RemoteAssociationCache`, association
  registry, route installer, dialer, and dialing-side lane readers, routes
  join/welcome/gossip and heartbeat request/response envelopes through live
  socket associations, and keeps cluster membership truth in gossip plus local
  failure-detector observations rather than remoting.
- The cluster TCP association runtime now explicitly closes active dialed
  outbound lane pipelines during shutdown, with integration coverage retaining
  a live route registration across shutdown to prove reader joins complete.
- `kairo-cluster` now has a focused cluster-derived association peer planner
  that consumes `CurrentClusterState` snapshots and cluster events, excludes
  self, follows Pekko's local-observer reachability rule for gossip peer
  validity, rejects non-self local-only peers, and emits explicit dial/remove
  targets for future multi-peer TCP runtime ownership without making remoting a
  membership authority.
- `kairo-cluster` now has a focused TCP peer-route owner that applies
  cluster-derived dial/remove plans to `ClusterTcpAssociationRuntime`, keeps
  per-peer route registrations separate from membership state, and closes full
  outbound pipelines plus cached routes when peers become locally unreachable
  or leave.
- `kairo-cluster` now has a focused TCP peer runtime lifecycle owner that
  composes the cluster TCP socket runtime, membership-derived peer planner, and
  peer-route table, applies cluster snapshots/events to live routes, and clears
  peer routes before listener shutdown.
- `kairo-cluster` now has focused TCP peer reconnect state. Failed
  membership-derived dials are retained as deterministic pending retries with a
  configured retry interval, successful retries clear pending state, and member
  removal or local-unreachable events cancel obsolete retry attempts.
- Cluster TCP peer runtime shutdown now has focused lifecycle coverage proving
  that pending reconnects are cleared and reported after failed dials, and the
  runtime tests live in a sibling test module instead of the production file.
- `kairo-cluster` crate docs now explain gossip-based membership, vector-clock
  merge, observer-owned reachability/failure-detector observations, why
  discovery is contact-only, and why Kairo does not use etcd or another
  central membership authority, with a compile-checked example.
- `kairo-cluster` now has an actor-backed TCP peer connector that subscribes
  to cluster snapshots/events, feeds the cluster TCP peer runtime, exposes
  explicit deterministic retry ticks, can schedule fixed-delay retry ticks with
  actor timers, reports snapshots for tests, and shuts the owned TCP runtime
  down when the connector actor stops.
- Cluster TCP peer connector socket tests now live in a focused sibling test
  module with serialized live listener fixtures, keeping the production
  connector module focused on actor/runtime behavior.
- `kairo-cluster` now has a TCP peer bootstrap facade that binds the cluster
  TCP peer runtime, spawns the connector actor with explicit settings, exposes
  the connector ref/self node/local association address, and registers
  coordinated shutdown to stop the connector before cluster shutdown.
- Cluster TCP peer bootstrap now has a two-node socket validation: two real
  bound runtimes are spawned through the bootstrap facade, cluster membership
  is published to both connector actors, and each side installs a peer route
  for the other node through the actor-backed connector boundary; the same
  validation now runs coordinated shutdown afterward and asserts the live-route
  connector stops through the registered bootstrap shutdown task.
- The two-node cluster TCP peer bootstrap validation now uses
  `kairo-testkit::MultiNodeTestKit` to own named node systems and create
  per-node snapshot probes, exercising the structured local multi-node harness
  in an existing live socket route test.
- Cluster TCP peer bootstrap now validates payload delivery over an installed
  membership-derived peer route: a real bootstrap-owned sender association
  cache carries a stable-codec `Join` membership message to the remote
  membership inbound handler before coordinated shutdown. The delivery
  scenario now uses `kairo-testkit::MultiNodeTestKit` so both live
  bootstrap-owned actor systems and their final shutdown are owned by the
  structured multi-node harness.
- Cluster TCP peer bootstrap lifecycle coverage now validates membership
  removal: after a two-node socket route is installed through published gossip,
  publishing sender-local membership without the remote peer removes the
  sender's connector route and active target through the bootstrap actor
  boundary.
- Cluster TCP peer bootstrap lifecycle coverage now also validates replacement
  peer routing: after removing a departed peer's route, publishing a new
  `UniqueAddress` for a replacement receiver installs a fresh membership-
  derived socket route through the same bootstrap connector. The scenario now
  sends a stable membership `Join` through the old route, verifies the removed
  route rejects later sends without delivering to the old membership actor,
  then sends a `Join` through the replacement route.
- Cluster TCP peer bootstrap now also has a three-node full-mesh socket
  validation: the first actor-backed connector installs membership-derived
  routes to both remote peers, carries stable-codec `Join` membership messages
  to both peer membership inbound handlers over those routes, then the remaining
  connectors install the rest of the full mesh from the same published gossip
  snapshot before coordinated shutdown stops all live-route connectors; the same
  scenario now publishes a reduced two-node membership view and validates the
  surviving nodes remove the departed node's active route through the connector
  boundary. The test now also pins the underlying association-cache route
  counts across full mesh, reduced membership, and coordinated shutdown cleanup
  for all three bootstrap-owned runtimes.
- Cluster TCP peer bootstrap tests now live in a focused sibling test module,
  keeping the production bootstrap facade separate from socket fixture setup
  and two-node validation data.
- Cluster TCP peer bootstrap socket helpers now live in a nested
  `tests::support` module, keeping registry setup, inbound membership probes,
  runtime binding, route assertions, and coordinated-shutdown fixture code out
  of the scenario tests.
- Cluster TCP peer bootstrap socket fixtures now serialize live listener setup
  within the test module and drive explicit retry ticks when a route snapshot
  reports pending reconnects, making concurrent bootstrap validation
  deterministic under load.
- `kairo-cluster-tools` is split into focused topic and singleton modules, and
  now has Pekko-style singleton oldest-member tracking that filters by role,
  sorts eligible UP members by cluster age, reports oldest changes from member
  events, and marks takeover unsafe while older leaving/exiting/down members
  are still present.
- `kairo-cluster-tools` crate docs now describe the structured singleton,
  proxy, topic, pubsub registry/gossip, remote-envelope, TCP peer, and system
  inbound boundaries, plus a compile-checked oldest-member tracking example.
- `kairo-cluster-tools` now has a focused singleton manager runtime planner
  that turns oldest-member observations and handover messages into explicit
  start-singleton, stop-singleton, handover, takeover, and manager-stop
  effects, covering safe immediate startup, delayed takeover, previous-oldest
  removal, and handover completion before actor wiring is added.
- `kairo-cluster-tools` now has an actor-backed singleton manager boundary
  that wraps the focused handover runtime in synchronous actor turns, accepts
  explicit initial-observation, oldest-change, member-removal, handover, and
  termination protocol messages, and replies with deterministic planned
  effects plus state snapshots for future transport and singleton-child wiring.
- `kairo-cluster-tools` singleton manager runtime and actor boundary tests now
  live in a focused sibling test module instead of the broad crate-level test
  file.
- `kairo-cluster-tools` now has a typed local singleton manager actor that
  interprets manager start/stop effects by spawning the singleton child under
  the manager, watching the child, sending the configured typed termination
  message during handoff, and completing handoff after child termination.
- `kairo-cluster-tools` local singleton manager actor tests and their probe
  fixture now live in a focused sibling test module.
- `kairo-cluster-tools` now has a typed local singleton proxy actor that
  forwards messages to the current singleton, buffers messages while no
  singleton is identified, drops the oldest buffered message when the bounded
  buffer is full, flushes buffered messages in FIFO order when a singleton is
  identified, and clears the route when the watched singleton terminates.
- `kairo-cluster-tools` singleton proxy routing is now split into a focused
  route-table module. The actor can register typed local or future remote
  singleton targets by `UniqueAddress`, apply initial oldest-member
  observations and oldest-change events, discard the previously identified
  singleton when the oldest member changes, and flush buffered messages once a
  route for the current oldest member is available.
- `kairo-cluster-tools` singleton proxy routes now use a focused
  `SingletonProxyTarget` abstraction: local targets remain watchable
  `ActorRef<M>` values, while remote targets can wrap `RemoteActorRef<M>` for
  `RemoteMessage` protocols and receive buffered proxy messages through stable
  remote envelopes when they become the current oldest route.
- `kairo-cluster-tools` now declares stable singleton manager handover
  protocol metadata and explicit codecs for `HandOverToMe`,
  `HandOverInProgress`, `HandOverDone`, and `TakeOverFromMe` messages using
  explicit `UniqueAddress` fields and serializer IDs instead of Rust type
  names, enum discriminants, or memory layout.
- `kairo-cluster-tools` now has focused singleton manager remote-envelope
  wiring: `SingletonManagerRemoteOutbound` maps handover runtime effects to
  stable serialized envelopes addressed to `/system/singleton/manager`,
  `SingletonManagerRemoteInbound` validates the target manager path and
  dispatches decoded handover messages into the actor-backed manager protocol,
  and outbound sends can use the shared `kairo-remote::RemoteAssociationCache`
  boundary.
- `kairo-cluster-tools` topic support is split into focused name and local
  topic modules, with typed local subscriptions, duplicate suppression,
  unsubscribe/removal handling, broadcast delivery, and deterministic
  one-message-per-group publish routing for the first pubsub foundation.
- `kairo-cluster-tools` local topic broadcast, one-per-group routing,
  unsubscribe, and subscriber-removal tests now live in a focused sibling test
  module.
- `kairo-cluster-tools` now has a focused local pubsub mediator state over
  named local topics, including current-topic listing, routed publish,
  subscribe/unsubscribe delegation, empty-topic cleanup, and subscriber removal
  across all topics.
- `kairo-cluster-tools` now has an actor-backed local pubsub mediator protocol
  that wraps the local pubsub state in synchronous actor turns, sends typed
  subscribe acks, publish reports, and current-topic replies, watches
  subscribers, and removes terminated subscribers from all local topics.
- `kairo-cluster-tools` local pubsub state and actor boundary tests now live
  in a focused sibling test module.
- `kairo-cluster-tools` now has a focused distributed pubsub registration
  state with Pekko-style versioned owner buckets, present/tombstone entries,
  peer-version delta collection, delta merge, tombstone pruning, broadcast
  target planning, and deterministic one-target-per-group planning.
- `kairo-cluster-tools` distributed pubsub registry delta, tombstone, and
  target-planning tests now live in a focused sibling test module.
- `kairo-cluster-tools` now has a transport-neutral pubsub delivery planner
  that converts distributed topic registrations into explicit local and remote
  delivery targets for broadcast and one-message-per-group publishes.
- `kairo-cluster-tools` now has a transport-neutral pubsub delivery transport
  that sends planned publish effects to local or remote mediator recipients,
  reports missing/send failures explicitly, and uses group-specific mediator
  commands so one-message-per-group delivery reaches only selected groups.
- `kairo-cluster-tools` pubsub delivery planner and transport tests now live
  in a focused sibling test module.
- `kairo-cluster-tools` now has an actor-backed distributed pubsub registry
  gossip slice with explicit peer recipients, deterministic status ticks,
  status/delta exchange, known-peer filtering for inbound deltas, peer removal
  pruning, and delta-count inspection.
- `kairo-cluster-tools` pubsub gossip actor status, delta exchange,
  known-peer merge, and peer-removal tests now live in a focused sibling test
  module.
- `kairo-cluster-tools` now has an actor-backed distributed pubsub mediator
  slice that keeps local topic subscriptions, the replicated topic registry,
  and publish delivery planning in one synchronous actor turn. It registers
  and unregisters local topics/groups from subscription changes, merges remote
  registry deltas, routes broadcast publishes to local or remote mediators, and
  exposes deterministic registry/state snapshots for tests.
- `kairo-cluster-tools` distributed pubsub mediator now consumes cluster member
  lifecycle events for node removal, dropping remote mediator routes and
  registry buckets when members leave, are downed, or are removed, and it routes
  one-message-per-group publishes across local and remote mediator targets.
- `kairo-cluster-tools` distributed pubsub mediator protocol data and local
  delivery adapter now live in focused submodules instead of being embedded in
  the mediator actor implementation.
- `kairo-cluster-tools` distributed pubsub mediator actor-boundary tests now
  live in a focused sibling test module.
- `kairo-cluster-tools` now declares stable remote metadata and explicit codecs
  for distributed pubsub gossip status and delta messages, including
  `UniqueAddress`, bucket versions, topic/group registry entries, tombstones,
  and known-version maps without relying on Rust type names, discriminants, or
  memory layout.
- Cluster-tools protocol codec implementation is split into focused
  registration, pubsub codec, singleton codec, wire-helper, and codec-test
  modules instead of concentrating stable wire logic and fixtures in one file.
- `kairo-cluster-tools` now has a focused transport-neutral pubsub gossip wire
  bridge that maps actor-local status/delta gossip messages to serialized
  stable pubsub wire messages, carries target-node routing metadata for future
  transport wiring, and deserializes inbound status/delta payloads back into
  the actor-backed gossip state machine with explicit target and manifest
  validation.
- `kairo-cluster-tools` now has focused pubsub remote-envelope wiring:
  `PubSubRemoteEnvelopeOutbound` addresses serialized status/delta gossip to
  the peer mediator at `/system/pubsub`, can use the shared
  `kairo-remote::RemoteAssociationCache`, rejects local-only targets, and
  keeps peer selection in cluster/pubsub state rather than treating remoting as
  membership truth.
- `kairo-cluster-tools` distributed pubsub user-message delivery can now use
  stable remote envelopes: `PubSubPublishEnvelope` carries topic, optional
  selected group, and the already-serialized business message;
  `PubSubRemoteDeliveryOutbound` can be registered as a remote mediator target
  and can use `kairo-remote::RemoteAssociationCache`; and
  `PubSubRemoteDeliveryInbound` validates `/system/pubsub` envelopes before
  dispatching decoded publishes into the actor-backed mediator's local
  delivery path.
- `kairo-cluster-tools` now has a focused system inbound router that dispatches
  decoded cluster-tools remote envelopes by stable manifest: pubsub
  status/delta traffic routes to the gossip wire inbound, pubsub publish
  envelopes route to mediator local delivery, and singleton handover envelopes
  route to the singleton manager inbound, with recipient validation kept at the
  system boundary or focused adapter boundary.
- `kairo-cluster-tools` now has a focused TCP association runtime that binds a
  handshaken listener, owns a shared `RemoteAssociationCache`, association
  registry, route installer, dialer, and dialing-side lane readers, and routes
  pubsub gossip, pubsub publish envelopes, and singleton handover messages
  through live socket associations to the existing system inbound handlers.
- The cluster-tools TCP association runtime now explicitly closes active
  dialed outbound lane pipelines during shutdown, with integration coverage
  retaining a live route registration across shutdown to prove reader joins
  complete.
- `kairo-cluster-tools` now has a focused TCP peer-route owner that consumes
  cluster membership-derived dial/remove plans, applies them to
  `ClusterToolsTcpAssociationRuntime`, keeps route registrations separate from
  membership state, and closes full outbound pipelines plus cached routes when
  peers are removed by local reachability or membership changes.
- `kairo-cluster-tools` now has a focused TCP peer runtime lifecycle owner
  that composes the cluster-tools TCP socket runtime, membership-derived peer
  planner, peer-route table, and dedicated reconnect state module. It applies
  snapshots/events to live pubsub/singleton routes, retries failed dials on
  explicit ticks, and clears routes plus pending retries before shutdown.
- Cluster-tools TCP peer runtime shutdown now has focused lifecycle coverage
  proving failed-dial pending reconnects are cleared and reported, and its
  runtime tests live in a sibling test module instead of the production file.
- `kairo-cluster-tools` now has an actor-backed TCP peer connector that
  subscribes to cluster snapshots/events, feeds the cluster-tools TCP peer
  runtime, exposes route/reconnect snapshots, supports explicit deterministic
  retry ticks, and can schedule fixed-delay retry ticks with actor timers.
- Cluster-tools TCP peer connector socket tests now live in a focused sibling
  test module with serialized live listener fixtures, keeping the production
  connector module focused on actor/runtime behavior.
- `kairo-cluster-tools` now has a TCP peer bootstrap facade that binds the
  tools TCP peer runtime from remote transport settings, spawns the connector,
  exposes its connector ref/self node/local association address, and registers
  a coordinated-shutdown actor-termination task before cluster shutdown.
- Cluster-tools TCP peer bootstrap now has a two-node socket validation: two
  real bound runtimes are spawned through the bootstrap facade, cluster
  membership is published to both connector actors, and each side installs a
  peer route for the other node through the actor-backed connector boundary;
  the same validation now runs coordinated shutdown afterward and asserts the
  live-route connector stops through the registered bootstrap shutdown task.
- The two-node cluster-tools TCP peer bootstrap validation now uses
  `kairo-testkit::MultiNodeTestKit` to own named node systems and create
  per-node connector snapshot probes through the structured local multi-node
  harness.
- Cluster-tools TCP peer bootstrap now validates payload delivery over an
  installed membership-derived peer route: a real bootstrap-owned sender
  association cache carries a stable-codec pubsub publish envelope to the
  remote mediator inbound handler before coordinated shutdown. The delivery
  scenario now uses `kairo-testkit::MultiNodeTestKit` so both live
  bootstrap-owned actor systems and their final shutdown are owned by the
  structured multi-node harness.
- Cluster-tools TCP peer bootstrap lifecycle coverage now validates membership
  removal: after a two-node socket route is installed through published cluster
  gossip, publishing sender-local membership without the remote peer removes
  the sender's connector route and active target through the bootstrap actor
  boundary.
- Cluster-tools TCP peer bootstrap lifecycle coverage now also validates
  replacement peer routing: after removing a departed peer's route, publishing
  a new `UniqueAddress` for a replacement receiver installs a fresh
  membership-derived cluster-tools socket route through the same connector.
  The test now sends a stable pubsub publish through the old route, verifies
  the removed route rejects later sends without delivering to the old mediator,
  then sends a pubsub publish through the replacement route.
- Cluster-tools TCP peer bootstrap now also has a three-node full-mesh socket
  validation: the first actor-backed connector installs membership-derived
  routes to both remote peers, carries stable-codec pubsub publish envelopes to
  both peer mediator inbound handlers over those routes, then the remaining
  connectors install the rest of the full mesh from the same published gossip
  snapshot before coordinated shutdown stops all live-route connectors; the same
  scenario now publishes a reduced two-node membership view and validates the
  surviving nodes remove the departed node's active route through the connector
  boundary. The test now also pins the underlying association-cache route
  counts across full mesh, reduced membership, and coordinated shutdown cleanup
  for all three bootstrap-owned runtimes.
- Cluster-tools TCP peer bootstrap tests now live in a focused sibling test
  module, keeping the production bootstrap facade separate from socket fixture
  setup and socket validation data.
- Cluster-tools TCP peer bootstrap socket helpers now live in a nested
  `tests::support` module, keeping test message codecs, inbound probes,
  runtime binding, route assertions, and coordinated-shutdown fixture code out
  of the scenario tests.
- Cluster-tools TCP peer bootstrap socket fixtures now serialize live listener
  setup within the test module and drive explicit retry ticks when a route
  snapshot reports pending reconnects, making concurrent bootstrap validation
  deterministic under load.
- Cluster-tools singleton oldest-tracker tests now live in a focused child
  test module instead of the broad cluster-tools crate test file.
- Cluster-tools singleton proxy buffering, remote target, re-identification,
  and termination-watch tests now live in a focused sibling test module.
- `kairo-examples` now includes a runnable cluster-tools TCP peer bootstrap
  example, with pubsub gossip, pubsub delivery, singleton inbound wiring, and
  reusable route/snapshot setup kept in a focused example module.
- The `kairo` facade now has a `config` feature with format-neutral
  `KairoSettings` structs and a TOML loader for the initial `[actor]`,
  `[remote]`, `[cluster]`, `[cluster.sharding]`, and `[cluster.tools]`
  sections, including explicit type/value validation and unknown-key rejection.
- The `kairo` facade TOML loader now also parses
  `[observability.diagnostics]` into backend-neutral diagnostic category flags
  for dead letters, remote delivery failures, serialization failures,
  quarantine events, and gossip state changes.
- `KairoSettings::actor_system_builder` now maps the dead-letter diagnostic
  flag into `ActorSystemBuilder::publish_dead_letters_to_event_stream`, so
  applications can disable dead-letter event-stream publication while retaining
  the deterministic `DeadLetters` record buffer.
- The TOML loader now parses `[actor.mailboxes.*]` capacity settings into
  format-neutral `MailboxConfig` values, rejects zero capacities, and maps the
  default mailbox capacity into `ActorSystemBuilder::mailbox_capacity`.
- The TOML loader now separates file/root parsing, section-to-settings
  projection, and primitive value validation into focused modules instead of
  concentrating all configuration parsing logic in one file.
- The TOML loader now supports layered file loading through `load_toml_files`;
  later files recursively merge tables and override scalar/array values before
  the final document is validated and projected into format-neutral settings.
- The `kairo` facade crate docs now describe feature-gated module boundaries,
  the prelude, local-vs-remote serialization requirements, TOML-first
  configuration, and a compile-checked settings parse example.
- The `kairo` facade prelude now re-exports common remote, distributed-data,
  sharding, cluster-tools, and testkit entry points behind their feature flags,
  with facade compile coverage that keeps the user entrypoint structured while
  preserving the focused implementation crates.
- `KairoSettings` now exposes feature-gated runtime conversion helpers for
  actor-system dispatcher configuration, remote transport settings, cluster
  failure-detector/heartbeat settings, sharding shard counts, and cluster-tools
  pubsub settings while keeping the base config model usable without enabling
  every runtime crate.
- Remote transport configuration now carries an optional TCP connect timeout
  through format-neutral settings, TOML parsing, facade docs, and
  `RemoteSettings` conversion while preserving the runtime default when unset.
- Remote transport configuration now rejects whitespace-only canonical
  hostnames through both direct format-neutral validation and TOML-loaded
  settings before they can reach runtime address construction.
- `ClusterShardingConfig` now exposes validated shard-count, rebalance
  interval, and stable `shard_id_for` helpers, while `ClusterToolsConfig` maps
  singleton role settings into `SingletonScope` and pubsub settings into
  gossip interval/max-delta values plus a configured `PubSubGossipActor`.
- The TOML loader now rejects an explicitly empty
  `[cluster.tools.singleton].role` instead of silently widening singleton scope
  to all members, matching the format-neutral `ClusterToolsConfig` validation.
- The TOML loader now runs final `KairoSettings::validate` after projecting
  all sections, so file-loaded settings cannot bypass format-neutral validation
  rules such as rejecting whitespace-only singleton roles.
- `KairoSettings::validate` now validates all format-neutral configuration
  sections, including programmatically constructed actor, remote, cluster
  heartbeat, downing, sharding, and cluster-tools settings.
- Sharding TOML and direct `ClusterShardingConfig` validation now reject a
  zero `rebalance_interval`, keeping periodic rebalance configuration from
  starting an immediate timer loop in the coordinator runtime.
- `ClusterShardingConfig` now also carries format-neutral
  `remember_entities`, retry interval, handoff timeout, shard failure backoff,
  and shard-region query timeout settings, with TOML parsing, direct
  validation, runtime helper accessors, and facade docs coverage for the
  Pekko-aligned sharding timing knobs Kairo already models.
- `ClusterShardingConfig` now carries format-neutral least-shard allocation
  rebalance limits under `[cluster.sharding.least_shard_allocation]`, validates
  positive absolute and relative limits, and converts them into
  `LeastShardAllocationStrategy` for the cluster-sharding runtime.
- `ClusterDowningConfig` now stores a structured
  `ClusterDowningStrategyConfig` enum instead of a raw strategy string, and the
  TOML loader parses `none`, `down-all`, `keep-majority`, `keep-oldest`, and
  `lease-majority` settings with role, `down_if_alone`, lease name, acquire
  delay, and release timing validation.
- `ClusterDowningConfig` now rejects whitespace-only optional role filters in
  both direct format-neutral settings and TOML-loaded downing strategies, so
  split-brain resolver hooks cannot silently run with an unusable role match.
- `ClusterDowningConfig` can now convert parsed TOML downing strategies into
  runtime downing hooks for `none`, `down-all`, `keep-majority`, and
  `keep-oldest`; `lease-majority` intentionally requires a caller-provided
  lease implementation through explicit lease-majority settings/hook helpers.
- `RemoteAssociationAddress` now parses strict contact-address strings, and
  `ClusterSeedConfig` can validate and convert configured seed nodes into
  remote association addresses while keeping seeds as contact points rather
  than cluster membership truth. Migration notes now show seed, sharding, and
  cluster-tools runtime helper usage.
- `kairo-examples` now provides the first runnable example crate under
  `kairo-next`, with a `local_counter` example that demonstrates spawning a
  typed actor, sending local messages without serialization, replying through
  an explicit channel, and stopping the actor.
- `kairo-examples` crate docs now list the runnable vertical-slice examples,
  document the split between small binaries and reusable helper modules, and
  include a compile-checked local counter example.
- `kairo-examples` now keeps reusable example actor protocol/state in a focused
  `counter` module and includes a `configured_counter` example that loads
  `kairo.local.toml`, maps format-neutral settings into an actor-system
  builder, and runs the typed counter with configured dispatcher throughput.
- The configured-counter example workflow now lives in a focused reusable
  module with a structured observation result, and the examples smoke tests
  validate the TOML facade-to-builder path instead of duplicating setup inside
  the binary.
- The configured-counter example TOML now also exercises the facade
  `[cluster.sharding]` settings, and its reusable observation verifies shard
  count, remember-entity enablement, retry, handoff, failure-backoff,
  rebalance, and query-timeout helper values through the public example
  boundary.
- The configured-counter example TOML now also exercises
  `[cluster.sharding.least_shard_allocation]`, and its reusable observation
  verifies that parsed allocation limits convert through the public
  `LeastShardAllocationStrategy` helper.
- The configured-counter example TOML now also exercises remote transport
  hostname, port, and TCP connect-timeout settings, and its reusable
  observation verifies the parsed format-neutral values alongside actor and
  sharding settings.
- `kairo-examples` now includes an `ask_pipe_to_self` example with reusable
  calculation-service and pattern-coordinator modules, demonstrating
  `Context::ask` and `Context::pipe_to_self` without placing the actor logic
  in one binary file.
- `kairo-examples` now includes a runnable `remote_ping_pong` example that
  binds two TCP remoting actor systems, registers an explicit stable codec for
  a typed ping/pong protocol, sends a remote ping, and routes a remote pong
  back to the sender's canonical actor ref over the accepted association.
- `kairo-examples` now includes a runnable `ddata_counter` example that starts
  a local `ReplicatorActor<GCounter>`, observes initial not-found state,
  subscribes to the key, applies a local increment, flushes the change
  notification, and reads the CRDT value back.
- `kairo-examples` now includes a runnable `cluster_membership` example that
  subscribes through the cluster facade, observes the initial snapshot,
  publishes a peer `Up` gossip change, publishes member removal, and requests
  the final current state through reusable helper code with smoke coverage.
- `kairo-examples` now includes a runnable `cluster_tools_local` example that
  exercises local pubsub subscribe/publish/current-topics behavior and local
  singleton manager startup with typed access to the running singleton child.
- `kairo-examples` now includes a runnable local cluster-sharding example that
  wires a shard coordinator, local shard region, `ShardingEnvelopeRouter`, and
  `EntityRef<String>` through reusable helper code and demonstrates stable
  shard-id routing into an entity-backed local shard whose typed counter
  entity receives business messages.
- The local cluster-sharding example now exposes passivation helpers that
  send `ShardMsg::Passivate` through the hosted local shard, wait for the
  entity to disappear from shard state, and prove routing through the same
  `EntityRef` starts a fresh entity instance afterward.
- The local cluster-sharding example now also has a two-region graceful
  shutdown validation: it starts with a remembered shard backed by a shared
  remember store and hosted on `region-a`, sends a coordinator
  graceful-shutdown request for that region,
  waits until coordinator state and `region-b` shard lookup show the shard has
  moved to the surviving local region, and reads the replacement shard state
  to prove the remembered `entity-1` is active after handoff.
- `kairo-examples` now has integration smoke tests for the reusable
  `local_counter`, `ask_pipe_to_self`, and `cluster_sharding_local` modules,
  validating the example crate from the same public module boundary used by
  downstream callers instead of relying only on binary entry points.
- `kairo-examples` now includes a runnable cluster TCP peer bootstrap example
  with reusable setup code, publishing matching cluster membership snapshots
  and verifying two local cluster peer runtimes establish bidirectional socket
  routes before coordinated shutdown.
- `kairo-examples` now has localhost two-node TCP bootstrap smoke tests for
  cluster, distributed-data, and cluster-tools example modules, publishing
  matching cluster membership snapshots and verifying each side establishes
  one peer route before coordinated shutdown; the same example smoke suite now
  publishes a sender-local membership shrink and verifies the sender removes
  the departed peer route through the public reusable example module boundary.
- The TCP bootstrap example modules now expose routeful coordinated-shutdown
  observations, and the localhost smoke suite validates that cluster,
  distributed-data, and cluster-tools bootstrap-owned connector actors stop
  through coordinated shutdown after installing a live peer route through the
  public reusable example node boundary.
- The TCP bootstrap example modules now expose reusable three-node binding
  helpers, and the localhost smoke suite validates cluster, distributed-data,
  and cluster-tools full-mesh route installation followed by a membership
  shrink to the two surviving nodes. The live socket smoke module is serialized
  with a local mutex so coordinated shutdown validation is deterministic under
  the default parallel test runner.
- The TCP bootstrap example smoke suite now also validates replacement peer
  routing for cluster, distributed-data, and cluster-tools: after a sender
  removes a departed peer route, publishing a new `UniqueAddress` installs a
  route to the replacement peer through the public reusable example module
  boundary.
- The cluster TCP bootstrap example now keeps its public reusable node
  boundary wired to a real membership inbound recorder and shared association
  cache, and the smoke suite sends a stable-manifest `Join` across the
  bootstrapped socket route before validating the receiver observes the joining
  node and roles.
- The cluster TCP bootstrap example smoke suite now also validates sender-side
  route-preservation delivery: after a three-node sender removes one peer from
  its membership view, the remaining route still carries a stable-manifest
  `Join` to the surviving peer, while a send to the removed peer rejects
  through the association cache and leaves that peer's membership recorder
  quiet.
- The cluster TCP bootstrap example smoke suite now also validates replacement
  peer delivery: after a sender removes an old peer route and installs a route
  to a replacement peer, a stable-manifest `Join` reaches the replacement
  membership inbound actor and does not arrive at the removed peer.
- The cluster TCP bootstrap example smoke suite now validates failed-dial
  lifecycle cleanup through the public example boundary: when an unreachable
  peer enters membership, the connector reports a pending reconnect, and when
  that peer leaves the sender's membership view, both the pending reconnect
  and route count clear.
- The distributed-data TCP bootstrap example now keeps its public reusable
  node boundary wired to a real recording request receiver and shared
  association cache, and the smoke suite sends a stable-manifest
  `ReplicatorRead` across the bootstrapped socket route, decodes it on the
  receiver side, and validates both configured peer identity and payload
  sender metadata.
- The distributed-data TCP bootstrap example smoke suite now also validates
  sender-side route-preservation delivery: after a three-node sender removes
  one peer from its membership view, the remaining route still carries a
  stable-manifest `ReplicatorRead` to the surviving peer, while a send to the
  removed peer rejects through the association cache and leaves that peer's
  request recorder quiet.
- The distributed-data TCP bootstrap example smoke suite now also validates
  replacement peer delivery: after a sender removes an old peer route and
  installs a route to a replacement peer, a stable-manifest `ReplicatorRead`
  reaches the replacement receiver and decodes with the sender's replica
  metadata.
- The distributed-data TCP bootstrap example smoke suite now validates
  failed-dial lifecycle cleanup through the public example boundary: an
  unreachable peer produces a pending reconnect snapshot, and removing that
  peer from the sender's membership view clears both pending reconnects and
  active routes before coordinated shutdown.
- The cluster-tools TCP bootstrap example now keeps its public reusable node
  boundary wired to the real distributed pubsub mediator plus an example
  subscriber, and the smoke suite sends a stable-codec `PubSubStatus` publish
  across the bootstrapped socket route before validating delivery through the
  receiver mediator.
- The cluster-tools TCP bootstrap example smoke suite now also validates
  sender-side route-preservation delivery: after a three-node sender removes
  one peer from its membership view, the remaining route still carries a
  stable-codec `PubSubStatus` publish to the surviving peer, while a send to
  the removed peer rejects through the association cache and leaves that peer's
  mediator subscriber quiet.
- The cluster-tools TCP bootstrap example smoke suite now also validates
  replacement peer delivery: after a sender removes an old peer route and
  installs a route to a replacement peer, a stable-codec `PubSubStatus`
  publish reaches the replacement mediator subscriber and does not arrive at
  the removed peer.
- The cluster-tools TCP bootstrap example smoke suite now validates
  failed-dial lifecycle cleanup through the public example boundary: an
  unreachable peer produces a pending reconnect snapshot, and removing that
  peer from the sender's membership view clears both pending reconnects and
  active routes before coordinated shutdown.
- TCP bootstrap example smoke-test support now lives in a focused sibling
  module, keeping shared live-socket locking, node adapters, membership
  publication, and route-count assertions separate from the scenario tests.
- `kairo-cluster-sharding` now has a transport-neutral remote coordinator
  registration bridge that serializes stable `Register` envelopes to resolved
  coordinator recipients with region sender metadata and decodes
  `RegisterAck` replies addressed to the region.
- `kairo-cluster-sharding` now has a separate transport-neutral remote
  coordinator shard-home bridge that serializes stable `GetShardHome`
  envelopes to resolved coordinator recipients and decodes `ShardHome` replies
  addressed to the region without serializing local coordinator enums.
- `ShardRegionActor` now consumes decoded remote coordinator registration
  acknowledgements and shard-home replies through a focused
  `RegionRemoteCoordinator` state module, marking matching remote coordinator
  acknowledgements as registered and mapping remote region wire refs to stable
  path-based region ids before replaying buffered deliveries.
- `ShardRegionActor` can now drive outbound remote coordinator registration
  and shard-home lookup through `RegionRemoteCoordinatorTransport`, sending
  stable `Register` envelopes after remote coordinator discovery/retry and
  sending pending `GetShardHome` envelopes after a matching remote
  `RegisterAck`.
- Shard-region remote coordinator actor tests now decode outbound stable
  `Register`, `GetShardHome`, `GracefulShutdownReq`, and `RegionStopped`
  payloads and keep repeated registry, target, transport, and discovery setup
  in a nested support module instead of embedding fixture data in the scenario
  tests.
- `kairo-cluster-sharding` now has region-side system inbound routing that
  dispatches stable remote envelopes addressed to `/system/sharding/region`:
  `RoutedShardEnvelope` re-enters local region delivery, `RegisterAck` becomes
  `RemoteCoordinatorRegistrationAck`, and `ShardHome` becomes
  `RemoteCoordinatorShardHome`.
- `kairo-cluster-sharding` now has coordinator-side system inbound routing
  for stable remote envelopes addressed to `/system/sharding/coordinator`:
  decoded `Register` commands register remote regions through the coordinator
  actor and reply with `RegisterAck`, while decoded `GetShardHome` commands
  re-enter coordinator allocation state and reply with `ShardHome` for known
  or newly allocated remote homes.
- Shard-coordinator system inbound coverage now lives in a focused sibling
  test module and decodes `RegisterAck`, `HostShard`, and `ShardHome` reply
  payloads through registered codecs instead of relying on manifest-only
  assertions.
- Remote coordinator registration now also attaches a remote region control
  target to the coordinator handoff transport, and newly allocated shard homes
  dispatch `HostShard` before replying `ShardHome`, matching Pekko's
  allocation ordering for local and remote regions.
- `kairo-cluster-sharding` now has a transport-backed remote region control
  target for coordinator-driven `HostShard`, `BeginHandOff`, and `HandOff`
  sends, plus coordinator-side inbound routing for stable `ShardStarted`,
  `BeginHandOffAck`, and `ShardStopped` replies back into coordinator and
  handoff-worker actor turns.
- Remote region control now lives in focused target, outbound, inbound, reply,
  and test modules instead of concentrating coordinator-to-region control
  routing, stable reply envelopes, and fixtures in one file.
- `kairo-cluster-sharding` now has region-side inbound handling for stable
  remote `HostShard`, `BeginHandOff`, and `HandOff` commands; these re-enter
  region actor state transitions and emit stable `ShardStarted`,
  `BeginHandOffAck`, or immediate `ShardStopped` replies where the current
  region runtime can complete the command synchronously.
- Shard-region system inbound coverage now lives in a focused sibling test
  module and decodes the emitted `ShardStarted`, `BeginHandOffAck`, and
  `ShardStopped` reply payloads through registered codecs instead of asserting
  only stable manifests.
- `ShardRegionActor<M>` can now opt into a region-side remote handoff
  stop-message factory, so stable remote `HandOff` commands for locally hosted
  shards forward into the local shard, observe the shard handoff plan, ask for
  stopper completion when required, mark the shard stopped, and send stable
  `ShardStopped` replies without putting business stop messages on the wire.
- `ShardRegionActor` remote host-shard, remote handoff, and graceful shutdown
  lifecycle helpers now live in a focused `region_actor::remote_lifecycle`
  child module instead of growing the main region actor file.
- `ShardRegionActor` construction, builder-style configuration, `Props`
  helpers, and runtime accessor now live in a focused
  `region_actor::construction` child module, leaving the main region actor file
  for message dispatch and routing orchestration.
- `ShardRegionActor` local shard start, route delivery, buffered replay,
  remote-region forwarding, and local handoff helper logic now live in a
  focused `region_actor::local_routing` child module.
- `ShardRegionActor` coordinator registration, coordinator discovery,
  local/remote shard-home requests, and shard-home reply application now live
  in a focused `region_actor::coordinator_flow` child module.
- `kairo-cluster-sharding` now has a local graceful region-shutdown path:
  regions notify their registered coordinator with `GracefulShutdownReq`,
  coordinators mark that region as gracefully shutting down, start handoff
  workers for each shard it owns, exclude it from new allocations, reallocate
  completed handoffs through the normal shard-home path, and regions stop once
  their local shards and buffers are gone.
- `ShardRegionActor` now preserves Pekko's graceful-shutdown retry semantics:
  if a local or remote coordinator sends `HostShard` or the local buffered
  host-and-replay command while the region is already shutting down, the
  region rejects the host request and re-sends `GracefulShutdownReq` so a
  moved or lagging coordinator can stop allocating shards to the terminating
  region.
- `kairo-cluster-sharding` now has stable remote graceful-shutdown protocol
  messages and codecs for `GracefulShutdownReq(region)` and
  `RegionStopped(region)`, plus a focused region-to-coordinator shutdown
  transport and coordinator-side inbound routing that re-enters normal
  shutdown and region-termination actor turns.
- `kairo-cluster-sharding` crate docs now explain `EntityRef<M>` and
  `ShardingEnvelope<M>` routing, why sharded business messages do not embed
  entity ids by default, and the documented stable FNV-1a shard hash with a
  compile-checked example.
- `kairo-cluster-sharding` remember-entity, remember-coordinator, and stable
  shard-hash tests now live in a focused sibling test module instead of
  growing the crate-level test file further.
- `kairo-cluster-sharding` entity-ref, sharding-envelope, and remote region
  route tests now live in a focused sibling test module instead of the broad
  crate-level test file.
- `kairo-cluster-sharding` shard allocation and least-shard allocation
  strategy tests now live in a focused sibling test module.
- `LeastShardAllocationStrategy` constructor validation now has direct focused
  coverage for zero absolute limits and zero, negative, or non-finite relative
  limits, matching the facade's runtime configuration validation path.
- `kairo-cluster-sharding` coordinator state transition tests now live in a
  focused sibling test module.
- `kairo-cluster-sharding` coordinator runtime shard-home, remember-entity,
  rebalance deferral, and completion tests now live in a focused sibling test
  module.
- `kairo-cluster-sharding` coordinator actor allocation, remember-store,
  rebalance, and timer tests now live in a focused sibling test module.
- `kairo-cluster-sharding` coordinator actor remembered-shard allocation after
  region registration now lives with the focused coordinator actor tests.
- `kairo-cluster-sharding` region runtime buffering, shard-home, host-shard,
  handoff, and drop-plan tests now live in a focused sibling test module.
- `kairo-cluster-sharding` region actor local buffering, shard startup,
  remembered-entity recovery, direct local routing, and buffered replay tests
  now live in a focused sibling test module.
- `kairo-cluster-sharding` region actor local handoff, store-backed shard
  handoff forwarding, and handoff completion tests now live in a focused
  sibling test module.
- `kairo-cluster-sharding` handoff worker, coordinator handoff completion,
  and graceful shutdown orchestration tests now live in a focused sibling test
  module.
- `kairo-cluster-sharding` now has multi-node graceful region-shutdown
  validation: a coordinator node hands off a store-backed shard from one
  region node to another through the typed handoff transport, clears the
  shutting-down region, starts the shard on the replacement region, and proves
  the replacement shard recovers remembered entities from the shared store
  before subsequent delivery. The scenario now uses
  `MultiNodeTestKit::enter_barrier` to mark initial shard hosting,
  coordinator readiness, and graceful-shutdown completion across the
  coordinator and region node systems.
- `kairo-cluster-sharding` now also has multi-node remember-store
  passivation validation: a store-backed shard hosted on one region passivates
  and terminates a remembered entity, removes it from the shared remember
  store, and a replacement region hosting the same shard treats the next
  delivery as a fresh remembered start instead of recovering the stopped
  entity. The rehosted delivery now enters through the region-local routing
  boundary before the shard emits the fresh remembered-start store update.
- `kairo-cluster-sharding` transport-neutral handoff delivery success and
  missing-target tests now live in a focused sibling test module.
- `kairo-cluster-sharding` local coordinator bootstrap, manual region
  registration, self-registration, and discovered-local coordinator
  registration tests now live in a focused sibling test module.
- `kairo-cluster-sharding` region actor remote-coordinator registration ACK,
  remote register retry, remote shard-home request, and remote graceful
  shutdown send tests now live in a focused sibling test module.
- `kairo-cluster-sharding` now also pins the remote graceful-shutdown
  hosted-shard sequence: a region registered with a remote coordinator sends
  `GracefulShutdownReq`, withholds `RegionStopped` while it still owns a
  hosted shard, then emits remote handoff acknowledgements and `RegionStopped`
  only after the remote handoff stops the local shard.
- `kairo-cluster-sharding` coordinator-side remote graceful-shutdown
  orchestration now has actor-boundary coverage: a stable decoded
  `GracefulShutdownReq` from a remote region starts a handoff worker, sends
  stable remote `BeginHandOff` and `HandOff` commands, consumes decoded
  remote acknowledgements, and reallocates the stopped shard to a surviving
  local region.
- `kairo-cluster-sharding` shard-region discovery subscriber cluster-snapshot
  forwarding tests now live in a focused sibling test module.
- `kairo-cluster-sharding` now has multi-node harness coverage for shard-region
  coordinator discovery: a region node subscribes to cluster state, discovers a
  backend coordinator on another node, registers with that coordinator, and
  routes a buffered entity message through the registered shard-home flow.
- `kairo-cluster-sharding` region actor shard-home request, buffered
  post-registration replay, known remote-home forwarding, and decoded
  remote-home reply tests now live in a focused sibling test module.
- `kairo-cluster-sharding` actor-backed shard delivery, remembered-entity
  loading, remember-store, passivation, and handoff tests now live in a
  focused sibling test module.
- `kairo-cluster-sharding` basic shard runtime delivery, passivation,
  termination, and handoff tests now live in a focused sibling test module.
- `kairo-cluster-sharding` remember-entity shard runtime recovery, start/stop
  update, and batching tests now live in a focused sibling test module.
- `kairo-cluster-sharding` entity-shard actor child delivery and handoff tests
  now live in a focused sibling test module.
- `kairo-actor` event-stream subscription, duplicate-subscription,
  unsubscribe, and exact-type tests now live in a focused sibling test module.
- `kairo-actor` receptionist subscription, stop cleanup, and context-handle
  tests now live in a focused sibling test module.
- `kairo-actor` coordinated-shutdown phase ordering, one-shot run,
  later-phase registration, actor termination task, and system termination
  tests now live in a focused sibling test module.
- `kairo-actor` stash capacity, full-stash rejection, clear/inspection,
  unstash-all ordering, and limited unstash tests now live in a focused sibling
  test module.
- `kairo-actor` ask success, timeout, and late-reply rejection tests now live
  in a focused sibling test module.
- `kairo-actor` scheduler one-shot delivery, cancellation, and self-scheduling
  tests now live in a focused sibling test module.
- `kairo-actor` timer single-shot, cancellation, replacement, fixed-delay,
  fixed-rate, and actor-stop cleanup tests now live in a focused sibling test
  module.
- `kairo-actor` receive-timeout repeat and cancellation tests now live in a
  focused sibling test module.
- `kairo-actor` pipe-to-self success/failure and spawn-task send-back tests
  now live in a focused sibling test module.
- `kairo-actor` task integration tests now also pin stop/restart scoped
  delivery: stale `spawn_task` and `pipe_to_self` completions after owner stop
  or restart are rejected and do not re-enter the actor mailbox.
- `kairo-actor` message-adapter mapping and stopped-owner rejection tests now
  live in a focused sibling test module.
- `kairo-actor` message-adapter integration tests now pin owner-scoped
  adapter lifecycles across both owner stop and restart: old adapter refs stop
  and notify death-watch subscribers when the owner tears down its adapter
  scope.
- `kairo-actor` watch, watch_with, self-watch rejection, duplicate-watch
  rejection, unwatch, signal-failure, and parent-child watch tests now live in
  a focused sibling test module.
- `kairo-actor` backoff-supervisor restart-delay tests now live in a focused
  sibling test module.
- `kairo-actor` startup-failure, restart/resume strategy, restart-limit,
  child-preservation, and escalation supervision tests now live in a focused
  sibling test module with only the watch-shared probe fixture exposed.
- `kairo-actor` restart supervision now pins Pekko-aligned lifecycle ordering:
  `Signal::PreRestart` is delivered while children are still visible to the
  restarting parent, then the default restart strategy stops those children.
- `kairo-actor` restart supervision also matches Pekko's child-stop watch
  cleanup: child watches owned by the restarting parent are removed before
  default restart teardown stops those children, so stale `watch_with`
  messages from restart-driven child stops do not re-enter the restarted actor.
- `kairo-actor` tree-lifecycle tests now pin restart-time child termination
  ordering: a restarting parent waits for children stopped by default restart
  teardown before processing queued user messages, so replacement children
  cannot observe the old child name as reusable until termination completes.
- `kairo-actor` tree-lifecycle tests now pin stop-time child termination
  ordering: a stopping parent rejects and does not process user messages while
  waiting for a blocking child stop to finish.
- `kairo-actor` context spawn, parent/child introspection, direct-child stop,
  parent-before-child shutdown, actor-path metadata, and post-stop signal tests
  now live in a focused sibling test module.
- `kairo-actor` tree-lifecycle tests now pin startup-failed child cleanup:
  failed-start children are removed from the parent's child registry and their
  child name reservation is released so the parent can spawn a replacement
  under the same logical child name.
- `kairo-cluster` TCP membership/downing socket coverage now pins a three-node
  route-preservation path: one membership receiver can mark and down a sender
  as unreachable while the sender keeps its remaining TCP route live and
  delivers a later join to a second receiver.
- `kairo-cluster` TCP peer bootstrap coverage now pins sender-side route
  reduction with live membership delivery: after one of two remote peers leaves
  the sender's cluster membership view, the remaining membership route
  continues to deliver stable-manifest `Join` envelopes, sends to the removed
  peer reject through the association cache without another membership
  delivery, and the sender cache drops from two routes to one.
- `kairo-distributed-data` TCP peer bootstrap coverage now pins sender-side
  route reduction with live delivery: after one of two remote peers leaves the
  sender's cluster membership view, the remaining replicator route continues
  to deliver stable-manifest remote read envelopes, and the sender cache drops
  from two routes to one before coordinated shutdown clears it.
- `kairo-distributed-data` remote request inbound coverage now pins
  reply-requested delta propagation: a stable remote delta request is applied
  through the replicator actor, the temporary reply actor sends a stable
  delta-ack envelope back to the incoming remote sender, and the reply carries
  the configured local sender metadata.
- `kairo-cluster-tools` TCP peer bootstrap coverage now pins the same
  sender-side route reduction for pubsub delivery: after one of two remote
  peers leaves the sender's cluster membership view, the remaining tools route
  continues to deliver stable-manifest remote publish envelopes, sends to the
  removed peer reject through the association cache without another mediator
  delivery, and the sender cache drops from two routes to one before
  coordinated shutdown clears it.
- `kairo-distributed-data` TCP peer bootstrap two-node route coverage now
  pins coordinated shutdown cleanup of installed association routes on both
  peers after cluster membership installs them.
- `kairo-cluster-tools` TCP peer bootstrap two-node route coverage now pins
  coordinated shutdown cleanup of installed association routes on both peers
  after cluster membership installs them.
- `kairo-distributed-data` and `kairo-cluster-tools` TCP peer runtime
  coverage now pins direct shutdown cleanup of active peer routes before
  listener teardown, matching the existing cluster runtime lifecycle coverage.
- `kairo-cluster` TCP peer bootstrap two-node route coverage now pins
  coordinated shutdown cleanup of installed association routes on both peers
  after cluster membership installs them.
- `kairo-actor` local core spawn/tell, builder, recipient, name validation,
  dead-letter, local resolution, stop, name-reuse, and termination tests now
  live in a focused sibling test module, leaving the parent test module for
  shared fixtures.
- The repository README and `kairo-next` README now describe the active
  Rust-first rewrite workspace, the old `crates/` implementation as
  reference-only, the gossip-not-etcd cluster constraint, typed actor and
  sharding APIs, and the current runnable examples.
- `docs/migration.md` now documents the current migration path from the old
  reference crates to the `kairo` facade, including typed actor protocols,
  TOML configuration, remote-message wire metadata, sharding routing, cluster
  membership constraints, and validation commands. `docs/blocked.md` now
  records that there are no current external blockers.
- The README files and migration notes now describe the configured-counter
  example as a facade configuration path for both actor dispatcher settings
  and current `[cluster.sharding]` timing/remember-entity helpers, keeping
  user-facing docs aligned with the TOML loader and example smoke coverage.
- The README files, migration notes, and architecture configuration section
  now also document `[cluster.sharding.least_shard_allocation]` as the
  TOML-first facade path for runtime least-shard allocation limits.
- The `kairo` facade TOML loader now parses configuration input as a document
  table instead of a single TOML value, restoring empty-config defaults, file
  loading, unknown-key validation, and structured runtime settings tests with
  the current `toml` crate.

Not yet implemented:

- Full actor tree lifecycle semantics beyond recursive local stop,
  restart-time child handling, and terminating-child name reservation.
- Broader actor-system local/remote provider integration, optional codec
  helper crates, richer actor-system lifecycle wiring around the existing TCP
  association primitives, and broader cross-crate compatibility fixtures.
- Distributed-data still needs broader multi-node validation around the
  focused TCP association runtime, peer-route owner, reconnect state, peer
  runtime, actor-backed connector, and bootstrap beyond the current localhost
  two-node example smoke test, three-node bootstrap route/request-delivery
  validation, and focused sender-side route-reduction delivery coverage.
- Sharding remember-entity stores still need broader automatic region/shard
  orchestration beyond the current focused actor-level coverage and the
  multi-node graceful-shutdown validation that now proves remembered entity
  recovery after handoff plus passivated-entity removal before rehost through
  a shared remember store.
- Cluster, distributed-data, and cluster-tools socket integration still need
  broader lifecycle tests around the bootstrap facades beyond the current
  localhost crate, focused sender-side route-reduction delivery coverage, and
  example routeful coordinated-shutdown smoke tests.
- Multi-node cluster membership socket lifecycle orchestration still needs
  broader automated multi-node scenarios beyond the current local two-node
  membership/downing socket validation and focused three-node
  route-preservation coverage.

## Last Validation

```bash
cargo test -p kairo-testkit manual_time
cargo test -p kairo-actor ask_
cargo test -p kairo-actor restart_supervision_rebuilds_actor_state_and_keeps_ref_path
cargo test -p kairo-actor stopped_watcher_is_removed_from_subject_watchers
cargo test -p kairo-cluster-tools bootstrap_clears_pending_reconnect_when_peer_leaves_before_retry
cargo test -p kairo-cluster-tools bootstrap_sender_keeps_remaining_pubsub_route_delivering_after_peer_removed
cargo test -p kairo-distributed-data bootstrap_sender_keeps_remaining_route_delivering_after_peer_removed
cargo test -p kairo-cluster tcp_membership_socket_preserves_remaining_peer_after_one_peer_downs_sender
cargo test -p kairo-distributed-data bootstrap_clears_pending_reconnect_when_peer_leaves_before_retry
cargo test -p kairo-cluster bootstrap_clears_pending_reconnect_when_peer_leaves_before_retry
cargo test -p kairo-cluster-tools connector_clears_pending_reconnect_when_peer_leaves_membership
cargo test -p kairo-cluster connector_clears_pending_reconnect_when_peer_leaves_membership
cargo test -p kairo-distributed-data connector_clears_pending_reconnect_when_peer_leaves_membership
cargo test -p kairo-actor actor_system_terminate_waits_for_descendant_children_before_terminated
cargo test -p kairo-remote tcp_remote_actor_system_resolver_trait_resolves_local_and_remote_refs
cargo test -p kairo-cluster-sharding multi_node_passivated_entity_is_not_recovered_after_rehost
cargo test -p kairo-cluster-sharding --all-targets --all-features
cargo fmt --all -- --check
cargo clippy -p kairo-cluster-sharding --all-targets --all-features -- -D warnings
cargo test -p kairo-distributed-data bootstrap_three_nodes_install_full_mesh_peer_routes_from_cluster_membership
cargo test -p kairo-distributed-data --all-targets --all-features
cargo fmt --all -- --check
cargo clippy -p kairo-distributed-data --all-targets --all-features -- -D warnings
cargo test -p kairo-cluster-sharding multi_node_graceful_shutdown_rebalances_region_shard_across_nodes
cargo test -p kairo-cluster-sharding --all-targets --all-features
cargo fmt --all -- --check
cargo clippy -p kairo-cluster-sharding --all-targets --all-features -- -D warnings
cargo test -p kairo-cluster tcp_runtime_routes_membership_and_heartbeat_over_bidirectional_association
cargo test -p kairo-distributed-data tcp_runtime_routes_replicator_requests_and_replies_over_bidirectional_association
cargo test -p kairo-cluster-tools tcp_runtime_routes_pubsub_and_singleton_system_messages_bidirectionally
cargo fmt --all -- --check
cargo test -p kairo-cluster --all-targets --all-features
cargo test -p kairo-distributed-data --all-targets --all-features
cargo test -p kairo-cluster-tools --all-targets --all-features
cargo clippy -p kairo-cluster --all-targets --all-features -- -D warnings
cargo clippy -p kairo-distributed-data --all-targets --all-features -- -D warnings
cargo clippy -p kairo-cluster-tools --all-targets --all-features -- -D warnings
cargo test -p kairo-remote tcp_remote_actor_system_coordinated_shutdown_stops_runtime_once
cargo fmt --all -- --check
cargo test -p kairo-remote tcp_runtime
cargo test -p kairo-remote --all-targets --all-features
cargo clippy -p kairo-remote --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo-actor restart_supervision_waits_for_stopping_children_before_processing_messages
cargo fmt --all -- --check
cargo test -p kairo-actor --all-targets --all-features
cargo clippy -p kairo-actor --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo-actor ask_temp_ref_is_unregistered_when_actor_system_terminates
cargo fmt --all -- --check
cargo test -p kairo-actor --all-targets --all-features
cargo clippy -p kairo-actor --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo-cluster bootstrap_binds_connector_and_registers_coordinated_shutdown_stop
cargo test -p kairo-distributed-data bootstrap_binds_connector_and_registers_coordinated_shutdown_stop
cargo test -p kairo-cluster-tools bootstrap_binds_connector_and_registers_coordinated_shutdown_stop
cargo test -p kairo-cluster --all-targets --all-features
cargo test -p kairo-distributed-data --all-targets --all-features
cargo test -p kairo-cluster-tools --all-targets --all-features
cargo test -p kairo --all-targets --all-features
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets --all-features
git diff --check
cargo test -p kairo-cluster-sharding region_actor_restarts_remembered_local_shard_after_unexpected_stop
cargo test -p kairo-cluster-sharding region_actor_does_not_restart_remembered_local_shard_after_handoff_stop
cargo test -p kairo-cluster-sharding region_actor_local
cargo fmt --all -- --check
cargo clippy -p kairo-cluster-sharding --all-targets --all-features -- -D warnings
cargo test -p kairo-cluster-sharding --all-targets --all-features
cargo test -p kairo-actor restart_supervision_unwatches_children_before_restart_stop
cargo test -p kairo-actor supervision
cargo fmt --all -- --check
cargo test -p kairo-actor --all-targets --all-features
cargo clippy -p kairo-actor --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo-actor supervision
cargo fmt --all -- --check
cargo test -p kairo-actor --all-targets --all-features
cargo clippy -p kairo-actor --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo-actor --test adapters
cargo fmt --all -- --check
cargo test -p kairo-actor --all-targets --all-features
cargo clippy -p kairo-actor --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo-actor --test tasks
cargo fmt --all -- --check
cargo test -p kairo-actor --all-targets --all-features
cargo clippy -p kairo-actor --all-targets --all-features -- -D warnings
cargo test -p kairo-cluster bootstrap_two_nodes_install_peer_routes_from_cluster_membership
cargo fmt --all -- --check
cargo test -p kairo-cluster --all-targets --all-features
cargo clippy -p kairo-cluster --all-targets --all-features -- -D warnings
cargo test -p kairo-cluster-tools bootstrap_two_nodes_install_peer_routes_from_cluster_membership
cargo fmt --all -- --check
cargo test -p kairo-cluster-tools --all-targets --all-features
cargo clippy -p kairo-cluster-tools --all-targets --all-features -- -D warnings
cargo test -p kairo-distributed-data bootstrap_two_nodes_install_peer_routes_from_cluster_membership
cargo fmt --all -- --check
cargo test -p kairo-distributed-data --all-targets --all-features
cargo clippy -p kairo-distributed-data --all-targets --all-features -- -D warnings
cargo test -p kairo-actor child_startup_failure_cleans_parent_registry_and_releases_name
cargo test -p kairo-actor post_stop_rejects_late_child_spawns
cargo test -p kairo-actor tree_lifecycle
cargo fmt --all -- --check
cargo test -p kairo-actor --all-targets --all-features
cargo clippy -p kairo-actor --all-targets --all-features -- -D warnings
cargo test -p kairo-remote provider_local_resolution_does_not_require_registered_codec
cargo test -p kairo-remote provider_maps_owned_canonical_missing_path_to_local_missing_ref_without_codec
cargo test -p kairo-remote remote_watch_actor
cargo fmt --all -- --check
cargo test -p kairo-remote --all-targets --all-features
cargo clippy -p kairo-remote --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
cargo test -p kairo-cluster-sharding region_actor_repeats_graceful_shutdown_when_host_shard_arrives_during_shutdown
cargo test -p kairo-cluster-sharding region_actor_repeats_remote_graceful_shutdown_when_host_shard_arrives_during_shutdown
cargo test -p kairo-cluster-sharding --all-targets --all-features
cargo clippy -p kairo-cluster-sharding --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
cargo test -p kairo-examples cluster_sharding_local_example
cargo clippy -p kairo-examples --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo-examples tcp_peer_bootstrap
cargo test -p kairo-examples cluster_tcp_peer_bootstrap_reinstalls_route_for_replacement_peer
cargo test -p kairo-examples ddata_tcp_peer_bootstrap_reinstalls_route_for_replacement_peer
cargo test -p kairo-examples cluster_tools_tcp_peer_bootstrap_reinstalls_route_for_replacement_peer
cargo clippy -p kairo-examples --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo-cluster-tools bootstrap_three_nodes_install_full_mesh_peer_routes_from_cluster_membership
cargo test -p kairo-cluster-tools bootstrap_reinstalls_peer_route_for_replacement_unique_address
cargo test -p kairo-cluster-tools bootstrap
cargo test -p kairo-cluster-tools --all-targets --all-features
cargo clippy -p kairo-cluster-tools --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo-distributed-data bootstrap_three_nodes_install_full_mesh_peer_routes_from_cluster_membership
cargo test -p kairo-distributed-data bootstrap
cargo test -p kairo-distributed-data --all-targets --all-features
cargo clippy -p kairo-distributed-data --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo-cluster bootstrap_three_nodes_install_full_mesh_peer_routes_from_cluster_membership
cargo test -p kairo-cluster bootstrap
cargo test -p kairo-cluster --all-targets --all-features
cargo clippy -p kairo-cluster --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo-cluster events
cargo test -p kairo-cluster --all-targets --all-features
cargo clippy -p kairo-cluster --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo-cluster-tools distributed_pubsub_mediator
cargo test -p kairo-cluster-tools --all-targets --all-features
cargo clippy -p kairo-cluster-tools --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo-cluster event_publisher
cargo test -p kairo-cluster --all-targets --all-features
cargo clippy -p kairo-cluster --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo-cluster-sharding region_system_inbound_completes_hosted_remote_handoff_with_local_stop_message
cargo test -p kairo-cluster-sharding remote_control
cargo test -p kairo-cluster-sharding region_system_inbound
cargo test -p kairo-cluster-sharding graceful_shutdown
cargo test -p kairo-cluster-sharding remote_shutdown
cargo test -p kairo-cluster-sharding allocation
cargo test -p kairo-cluster-sharding coordinator_actor
cargo test -p kairo-cluster-sharding coordinator_state
cargo test -p kairo-cluster-sharding coordinator_runtime
cargo test -p kairo-cluster-sharding handoff_orchestration
cargo test -p kairo-cluster-sharding handoff_transport
cargo test -p kairo-cluster-sharding region_actor_handoff
cargo test -p kairo-cluster-sharding region_actor_local
cargo test -p kairo-cluster-sharding region_discovery_subscriber
cargo test -p kairo-cluster-sharding region_remote_coordinator_actor
cargo test -p kairo-cluster-sharding region_route_resolution
cargo test -p kairo-cluster-sharding region_registration
cargo test -p kairo-cluster-sharding region_runtime
cargo test -p kairo-cluster-sharding shard_actor
cargo test -p kairo-cluster-sharding shard_runtime
cargo test -p kairo-cluster-sharding shard_remember_runtime
cargo test -p kairo-cluster-sharding entity_shard_actor
cargo test -p kairo-cluster-sharding coordinator_system_inbound_routes_region_shutdown_messages
cargo test -p kairo-cluster-sharding coordinator_system_inbound_remote_graceful_shutdown_rebalances_to_local_region
cargo test -p kairo-cluster-sharding region_actor_sends_remote_graceful_shutdown_and_region_stopped
cargo test -p kairo-cluster-sharding region_actor_delays_remote_region_stopped_until_hosted_shard_handoff
cargo test -p kairo-cluster-sharding sharding_protocol_codecs_round_trip_handoff_messages
cargo test -p kairo-cluster-sharding remote_coordinator_transport
cargo test -p kairo-cluster-sharding coordinator_actor_dispatches_host_shard_on_new_allocation
cargo test -p kairo-cluster-sharding coordinator_system_inbound_routes_register_and_get_shard_home
cargo test -p kairo-cluster-sharding entity_ref_routes_through_registered_region_to_entity_actor
cargo test -p kairo-cluster-sharding entity_ref_routes_through_sharding_envelope_router_to_local_shard
cargo test -p kairo-cluster-sharding entity_routing
cargo test -p kairo-cluster-sharding region_actor_requests_shard_home_from_registered_coordinator_for_local_route
cargo test -p kairo-cluster-sharding remember
cargo test -p kairo-cluster-sharding --all-targets --all-features
cargo clippy -p kairo-cluster-sharding --all-targets --all-features -- -D warnings
cargo test -p kairo-actor event_stream
cargo test -p kairo-actor receptionist
cargo test -p kairo-actor coordinated_shutdown
cargo test -p kairo-actor stash
cargo test -p kairo-actor ask
cargo test -p kairo-actor scheduler
cargo test -p kairo-actor timer
cargo test -p kairo-actor receive_timeout
cargo test -p kairo-actor task
cargo test -p kairo-actor adapter
cargo test -p kairo-actor watch
cargo test -p kairo-actor backoff_supervisor
cargo test -p kairo-actor supervision
cargo test -p kairo-actor context_
cargo test -p kairo-actor parent_stop
cargo test -p kairo-actor post_stop_signal
cargo test -p kairo-actor local_core
cargo test -p kairo-actor --all-targets --all-features
cargo clippy -p kairo-actor --all-targets --all-features -- -D warnings
cargo test -p kairo-remote reader_supervisor
cargo test -p kairo-remote tcp_listener_accept_loop_spawns_and_joins_lane_readers
cargo test -p kairo-remote tcp_lane_reader_supervision_records_lane_restart_decision
cargo test -p kairo-remote tcp_listener_report_includes_reader_supervision_decisions
cargo test -p kairo-remote tcp_
cargo test -p kairo-remote --all-targets --all-features
cargo clippy -p kairo-remote --all-targets --all-features -- -D warnings
cargo test -p kairo-cluster downing::tests
cargo test -p kairo-cluster downing
cargo test -p kairo-cluster indirectly_connected
cargo test -p kairo-cluster all_observers_reports_negative_reachability_observers
cargo test -p kairo-cluster membership_actor
cargo test -p kairo-cluster bootstrap_two_nodes_install_peer_routes_from_cluster_membership
cargo test -p kairo-cluster bootstrap_three_nodes_install_full_mesh_peer_routes_from_cluster_membership
cargo test -p kairo-cluster bootstrap_reinstalls_peer_route_for_replacement_unique_address
cargo test -p kairo-cluster bootstrap
cargo test -p kairo-cluster peer_runtime_shutdown_clears_pending_reconnects_after_failed_dial
cargo test -p kairo-cluster tcp_peer_runtime
cargo test -p kairo-cluster --all-targets --all-features
cargo clippy -p kairo-cluster --all-targets --all-features -- -D warnings
cargo test -p kairo --all-targets --all-features config
cargo test -p kairo-distributed-data bootstrap_two_nodes_install_peer_routes_from_cluster_membership
cargo test -p kairo-distributed-data bootstrap_three_nodes_install_full_mesh_peer_routes_from_cluster_membership
cargo test -p kairo-distributed-data bootstrap_reinstalls_peer_route_for_replacement_unique_address
cargo test -p kairo-distributed-data bootstrap
cargo test -p kairo-distributed-data peer_runtime_shutdown_clears_pending_reconnects_after_failed_dial
cargo test -p kairo-distributed-data tcp_peer_runtime
cargo test -p kairo-distributed-data codec
cargo test -p kairo-distributed-data crdt_foundation
cargo test -p kairo-distributed-data crdt_codecs
cargo test -p kairo-distributed-data delta_propagation_log
cargo test -p kairo-distributed-data delta_wire
cargo test -p kairo-distributed-data delta_transport
cargo test -p kairo-distributed-data delta_receive_tracker
cargo test -p kairo-distributed-data aggregation_core
cargo test -p kairo-distributed-data aggregation_wire
cargo test -p kairo-distributed-data aggregation_transport
cargo test -p kairo-distributed-data direct_receive
cargo test -p kairo-distributed-data replicator_state
cargo test -p kairo-distributed-data replicator_actor_client
cargo test -p kairo-distributed-data replicator_actor_delta_loop
cargo test -p kairo-distributed-data replicator_actor_delta_receive
cargo test -p kairo-distributed-data replicator_actor_gossip
cargo test -p kairo-distributed-data replicator_actor_planning
cargo test -p kairo-distributed-data replicator_actor_pruning
cargo test -p kairo-distributed-data replicator_actor_remote_receive
cargo test -p kairo-distributed-data --all-targets --all-features
cargo clippy -p kairo-distributed-data --all-targets --all-features -- -D warnings
cargo test -p kairo-testkit test_probe
cargo test -p kairo-testkit manual_time
cargo test -p kairo-testkit multi_node
cargo test -p kairo-testkit await_assert
cargo test -p kairo-testkit --all-targets --all-features
cargo clippy -p kairo-testkit --all-targets --all-features -- -D warnings
cargo test -p kairo-cluster-tools bootstrap_two_nodes_install_peer_routes_from_cluster_membership
cargo test -p kairo-cluster-tools bootstrap_three_nodes_install_full_mesh_peer_routes_from_cluster_membership
cargo test -p kairo-cluster-tools bootstrap
cargo test -p kairo-cluster-tools peer_runtime_shutdown_clears_pending_reconnects_after_failed_dial
cargo test -p kairo-cluster-tools tcp_peer_runtime
cargo test -p kairo-cluster-tools singleton_oldest
cargo test -p kairo-cluster-tools singleton_manager
cargo test -p kairo-cluster-tools singleton_proxy
cargo test -p kairo-cluster-tools local_singleton_manager
cargo test -p kairo-cluster-tools local_topic
cargo test -p kairo-cluster-tools local_pubsub
cargo test -p kairo-cluster-tools pubsub_registry
cargo test -p kairo-cluster-tools pubsub_gossip
cargo test -p kairo-cluster-tools pubsub_delivery
cargo test -p kairo-cluster-tools distributed_pubsub_mediator
cargo test -p kairo-cluster-tools --all-targets --all-features
cargo clippy -p kairo-cluster-tools --all-targets --all-features -- -D warnings
cargo test -p kairo-cluster multi_node_downing_providers_advance_together_and_only_leader_decides
cargo test -p kairo-actor startup_failure
cargo test -p kairo-actor bounded_restart_supervision
cargo test -p kairo-actor startup_failure_escalates_to_parent_supervision
cargo test -p kairo-actor receive_panic
cargo test -p kairo-actor startup_panic
cargo test -p kairo-actor restart_supervision_rebuilds_actor_after_receive_panic
cargo test -p kairo-actor signal_failure
cargo test -p kairo-actor restart_supervision_rebuilds_actor_after_signal_failure
cargo test -p kairo-actor watch_self
cargo test -p kairo-actor watch_with_self
cargo test -p kairo-actor requires_unwatch_first
cargo test -p kairo-actor --test death_watch
cargo test -p kairo-actor --test tasks
cargo test -p kairo-actor --test adapters
cargo test -p kairo-actor mailbox::tests
cargo test -p kairo-actor --all-targets --all-features
cargo clippy -p kairo-actor --all-targets --all-features -- -D warnings
cargo test -p kairo-cluster-sharding --all-targets --all-features
cargo clippy -p kairo-cluster-sharding --all-targets --all-features -- -D warnings
cargo test -p kairo-distributed-data bootstrap_installed_peer_route_delivers_remote_request_to_receiver
cargo test -p kairo-cluster-tools bootstrap_installed_peer_route_delivers_pubsub_publish_to_receiver
cargo test -p kairo-distributed-data bootstrap
cargo test -p kairo-cluster-tools bootstrap
cargo fmt --all -- --check
cargo clippy -p kairo-distributed-data --all-targets --all-features -- -D warnings
cargo clippy -p kairo-cluster-tools --all-targets --all-features -- -D warnings
cargo test -p kairo-distributed-data --all-targets --all-features
cargo test -p kairo-cluster-tools --all-targets --all-features
cargo test -p kairo-cluster-sharding multi_node_region_discovery_registers_and_routes_via_coordinator_node
cargo test -p kairo-cluster-sharding region_discovery
cargo test -p kairo-cluster-sharding region_actor_requests_shard_home_from_registered_coordinator_for_local_route
cargo test -p kairo-cluster-sharding region_actor_registers_with_discovered_local_coordinator
cargo fmt --all -- --check
cargo test -p kairo-cluster-sharding --all-targets --all-features
cargo clippy -p kairo-cluster-sharding --all-targets --all-features -- -D warnings
cargo test -p kairo-cluster bootstrap_installed_peer_route_delivers_membership_join_to_receiver
cargo test -p kairo-cluster bootstrap
cargo test -p kairo-cluster bootstrap_three_nodes_install_full_mesh_peer_routes_from_cluster_membership
cargo fmt --all -- --check
cargo test -p kairo-cluster --all-targets --all-features
cargo clippy -p kairo-cluster --all-targets --all-features -- -D warnings
cargo test -p kairo-cluster-tools bootstrap_three_nodes_install_full_mesh_peer_routes_from_cluster_membership
cargo test -p kairo-cluster-tools --all-targets --all-features
cargo fmt --all -- --check
cargo clippy -p kairo-cluster-tools --all-targets --all-features -- -D warnings
cargo test -p kairo-cluster-sharding multi_node_region_discovery_allocates_remembered_shard_on_registration
cargo test -p kairo-cluster-sharding --all-targets --all-features
cargo fmt --all -- --check
cargo clippy -p kairo-cluster-sharding --all-targets --all-features -- -D warnings
cargo test -p kairo-actor parent_stop_does_not_process_user_messages_while_waiting_for_children
cargo test -p kairo-actor --all-targets --all-features
cargo fmt --all -- --check
cargo clippy -p kairo-actor --all-targets --all-features -- -D warnings
cargo test -p kairo-serialization --all-targets --all-features
cargo test -p kairo-remote --all-targets --all-features
cargo fmt --all -- --check
cargo clippy -p kairo-serialization --all-targets --all-features -- -D warnings
cargo clippy -p kairo-remote --all-targets --all-features -- -D warnings
cargo test -p kairo-actor stopping_watcher_is_removed_before_waiting_for_children -- --nocapture
cargo test -p kairo-actor --all-targets --all-features
cargo fmt --all -- --check
cargo clippy -p kairo-actor --all-targets --all-features -- -D warnings
cargo test -p kairo-actor watch_with_survives_unrelated_actor_restart -- --nocapture
cargo test -p kairo-actor --all-targets --all-features
cargo fmt --all -- --check
cargo clippy -p kairo-actor --all-targets --all-features -- -D warnings
cargo test -p kairo-actor bounded_restart_supervision_retries_restarted_startup_failure -- --nocapture
cargo test -p kairo-actor --all-targets --all-features
cargo fmt --all -- --check
cargo clippy -p kairo-actor --all-targets --all-features -- -D warnings
cargo test -p kairo-serialization actor_ref_resolution_goes_through_provider_trait
cargo test -p kairo-remote provider_
cargo test -p kairo-serialization --all-targets --all-features
cargo test -p kairo-remote --all-targets --all-features
cargo clippy -p kairo-serialization -p kairo-remote --all-targets --all-features -- -D warnings
cargo test -p kairo config --all-targets --all-features
cargo test -p kairo --all-targets --all-features
cargo fmt --all -- --check
cargo clippy -p kairo --all-targets --all-features -- -D warnings
cargo test -p kairo-actor local_core --all-targets --all-features
cargo test -p kairo config --all-targets --all-features
cargo test -p kairo-actor --all-targets --all-features
cargo test -p kairo --all-targets --all-features
cargo clippy -p kairo-actor -p kairo --all-targets --all-features -- -D warnings
cargo test -p kairo-actor event_stream --all-targets --all-features
cargo test -p kairo-actor local_core --all-targets --all-features
cargo test -p kairo-actor --all-targets --all-features
cargo clippy -p kairo-actor --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo-distributed-data remote_reply --all-targets --all-features
cargo test -p kairo-distributed-data --all-targets --all-features
cargo clippy -p kairo-distributed-data --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
git diff --check
cargo test -p kairo-distributed-data aggregation_session --all-targets --all-features
cargo test -p kairo-distributed-data replicator_actor_aggregation_uses_canonical_sender_ref --all-targets --all-features
cargo test -p kairo-distributed-data replicator_actor_client --all-targets --all-features
cargo test -p kairo-distributed-data --all-targets --all-features
cargo clippy -p kairo-distributed-data --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
cargo test -p kairo-remote local_address --all-targets --all-features
cargo test -p kairo-distributed-data remote_tcp::tests::tcp_runtime_routes_replicator_requests_and_replies_over_bidirectional_association --all-targets --all-features
cargo test -p kairo-distributed-data remote_reply_inbound_maps_owned_canonical_recipient_to_local_aggregator --all-targets --all-features
cargo test -p kairo-distributed-data aggregation_session --all-targets --all-features
cargo test -p kairo-remote --all-targets --all-features
cargo test -p kairo-distributed-data --all-targets --all-features
cargo clippy -p kairo-remote --all-targets --all-features -- -D warnings
cargo clippy -p kairo-distributed-data --all-targets --all-features -- -D warnings
cargo test -p kairo config_converts_downing_strategy_to_runtime_hook --all-targets --all-features
cargo test -p kairo config_converts_lease_majority_with_explicit_lease --all-targets --all-features
cargo test -p kairo config --all-targets --all-features
cargo test -p kairo --all-targets --all-features
cargo clippy -p kairo --all-targets --all-features -- -D warnings
cargo test -p kairo-remote local_delivery_maps_owned_canonical_missing_recipient_to_local_dead_letters --all-targets --all-features
cargo test -p kairo-remote --all-targets --all-features
cargo fmt --all -- --check
cargo clippy -p kairo-remote --all-targets --all-features -- -D warnings
cargo test -p kairo-actor actor_system_terminate_retry_still_waits_for_timed_out_child --all-targets --all-features
cargo test -p kairo-actor --all-targets --all-features
cargo fmt --all -- --check
cargo clippy -p kairo-actor --all-targets --all-features -- -D warnings
cargo test -p kairo-cluster-sharding region_actor_remote_host_shard_spawns_store_backed_shard_and_recovers_entities --all-targets --all-features
cargo test -p kairo-cluster-sharding --all-targets --all-features
cargo fmt --all -- --check
cargo clippy -p kairo-cluster-sharding --all-targets --all-features -- -D warnings
cargo test -p kairo-distributed-data remote_request_inbound_applies_delta_and_replies_to_sender_ref_when_requested --all-targets --all-features
cargo test -p kairo-distributed-data --all-targets --all-features
cargo fmt --all -- --check
cargo clippy -p kairo-distributed-data --all-targets --all-features -- -D warnings
cargo test -p kairo-distributed-data peer_runtime_shutdown_clears_active_peer_routes_before_listener_stop --all-targets --all-features
cargo test -p kairo-cluster-tools peer_runtime_shutdown_clears_active_peer_routes_before_listener_stop --all-targets --all-features
cargo test -p kairo-distributed-data --all-targets --all-features
cargo test -p kairo-cluster-tools --all-targets --all-features
cargo fmt --all -- --check
cargo clippy -p kairo-distributed-data --all-targets --all-features -- -D warnings
cargo clippy -p kairo-cluster-tools --all-targets --all-features -- -D warnings
cargo test -p kairo-cluster connector_clear_routes_removes_active_peer_routes --all-targets --all-features
cargo test -p kairo-distributed-data connector_clear_routes_removes_active_peer_routes --all-targets --all-features
cargo test -p kairo-cluster-tools connector_clear_routes_removes_active_peer_routes --all-targets --all-features
cargo test -p kairo-cluster --all-targets --all-features
cargo test -p kairo-distributed-data --all-targets --all-features
cargo test -p kairo-cluster-tools --all-targets --all-features
cargo fmt --all -- --check
cargo clippy -p kairo-cluster --all-targets --all-features -- -D warnings
cargo clippy -p kairo-distributed-data --all-targets --all-features -- -D warnings
cargo clippy -p kairo-cluster-tools --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo-remote provider_remote_only_resolve_rejects_owned_canonical_address --all-targets --all-features
cargo test -p kairo-remote --all-targets --all-features
cargo fmt --all -- --check
cargo clippy -p kairo-remote --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo-examples ddata_tcp_peer_bootstrap_delivers_remote_read_request --all-targets --all-features
cargo fmt --all -- --check
cargo clippy -p kairo-examples --all-targets --all-features -- -D warnings
cargo test -p kairo-examples --test tcp_bootstrap_smoke --all-features
cargo test -p kairo-examples --all-targets --all-features
cargo test -p kairo-examples cluster_tools_tcp_peer_bootstrap_delivers_remote_pubsub_publish --all-targets --all-features
cargo fmt --all -- --check
cargo test -p kairo-examples --test tcp_bootstrap_smoke --all-features
cargo test -p kairo-examples --all-targets --all-features
cargo clippy -p kairo-examples --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo-examples cluster_tcp_peer_bootstrap_delivers_remote_join --all-targets --all-features
cargo fmt --all -- --check
cargo test -p kairo-examples --test tcp_bootstrap_smoke --all-features
cargo test -p kairo-examples --all-targets --all-features
cargo clippy -p kairo-examples --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo-examples cluster_tcp_peer_bootstrap_keeps_remaining_join_route_after_peer_removed --all-targets --all-features
cargo test -p kairo-examples cluster_tcp_peer_bootstrap_clears_pending_reconnect_when_peer_leaves --all-targets --all-features
cargo fmt --all -- --check
cargo test -p kairo-examples --test tcp_bootstrap_smoke --all-features
cargo test -p kairo-examples --all-targets --all-features
cargo clippy -p kairo-examples --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo-examples ddata_tcp_peer_bootstrap_keeps_remaining_read_route_after_peer_removed --all-targets --all-features
cargo test -p kairo-examples ddata_tcp_peer_bootstrap_clears_pending_reconnect_when_peer_leaves --all-targets --all-features
cargo test -p kairo-examples cluster_tools_tcp_peer_bootstrap_keeps_remaining_pubsub_route_after_peer_removed --all-targets --all-features
cargo test -p kairo-examples cluster_tools_tcp_peer_bootstrap_clears_pending_reconnect_when_peer_leaves --all-targets --all-features
cargo fmt --all -- --check
cargo test -p kairo-examples --test tcp_bootstrap_smoke --all-features
cargo test -p kairo-examples --all-targets --all-features
cargo clippy -p kairo-examples --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo-examples cluster_sharding_local_example_gracefully_moves_region_shard --all-targets --all-features
cargo test -p kairo-examples --all-targets --all-features
cargo fmt --all -- --check
cargo clippy -p kairo-examples --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo-examples ddata_tcp_peer_bootstrap_delivers_read_to_replacement_peer --all-targets --all-features
cargo test -p kairo-examples --test tcp_bootstrap_smoke --all-features
cargo test -p kairo-examples --all-targets --all-features
cargo fmt --all -- --check
cargo clippy -p kairo-examples --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo-examples cluster_tcp_peer_bootstrap_delivers_join_to_replacement_peer --all-targets --all-features
cargo test -p kairo-examples cluster_tools_tcp_peer_bootstrap_delivers_pubsub_to_replacement_peer --all-targets --all-features
cargo test -p kairo-examples --test tcp_bootstrap_smoke --all-features
cargo test -p kairo-examples --all-targets --all-features
cargo fmt --all -- --check
cargo clippy -p kairo-examples --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo-examples shutdown_stops_connector_after_live_route --test tcp_bootstrap_smoke --all-features
cargo test -p kairo-examples --test tcp_bootstrap_smoke --all-features
cargo test -p kairo-examples --all-targets --all-features
cargo fmt --all -- --check
cargo clippy -p kairo-examples --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo-actor restart_supervision_builds_replacement_after_pre_restart_and_child_stop --all-targets --all-features
cargo test -p kairo-actor supervision --all-targets --all-features
cargo test -p kairo-actor tree_lifecycle --all-targets --all-features
cargo test -p kairo-actor --all-targets --all-features
cargo fmt --all -- --check
cargo clippy -p kairo-actor --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo-remote address_terminated --all-targets --all-features
cargo test -p kairo-remote remote_watch --all-targets --all-features
cargo test -p kairo-remote inbound_router --all-targets --all-features
cargo test -p kairo-remote --all-targets --all-features
cargo fmt --all -- --check
cargo clippy -p kairo-remote --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo-remote tcp_remote_actor_system_routes_address_terminated_to_remote_death_watch --all-targets --all-features
cargo test -p kairo-remote tcp_remote_actor_system --all-targets --all-features
cargo test -p kairo-remote --all-targets --all-features
cargo fmt --all -- --check
cargo clippy -p kairo-remote --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo-actor receptionist --all-targets --all-features
cargo test -p kairo-actor --all-targets --all-features
cargo fmt --all -- --check
cargo clippy -p kairo-actor --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo-actor receptionist --all-targets --all-features
cargo test -p kairo-actor --all-targets --all-features
cargo fmt --all -- --check
cargo clippy -p kairo-actor --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo-serialization deserialize --all-targets --all-features
cargo test -p kairo-serialization --all-targets --all-features
cargo fmt --all -- --check
cargo clippy -p kairo-serialization --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo-remote inbound_reports_registered_wrong_message_type_as_serialization_failure --all-targets --all-features
cargo test -p kairo-remote tcp_remote_actor_system_routes_address_terminated_to_remote_death_watch --all-targets --all-features
cargo test -p kairo-remote --all-targets --all-features
cargo fmt --all -- --check
cargo clippy -p kairo-remote --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo config --all-targets --all-features
cargo test -p kairo --doc --all-features
cargo fmt --all -- --check
cargo clippy -p kairo --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo-examples configured_counter_example_smoke --all-targets --all-features
cargo test -p kairo-examples --test examples_smoke --all-features
cargo test -p kairo-examples --doc --all-features
cargo fmt --all -- --check
cargo clippy -p kairo-examples --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo --doc --all-features
cargo test -p kairo-examples --doc --all-features
cargo fmt --all -- --check
git diff --check
cargo test -p kairo config --all-targets --all-features
cargo test -p kairo --doc --all-features
cargo fmt --all -- --check
cargo clippy -p kairo --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo-examples configured_counter_example_smoke --all-targets --all-features
cargo test -p kairo-examples --test examples_smoke --all-features
cargo test -p kairo-examples --doc --all-features
cargo fmt --all -- --check
cargo clippy -p kairo-examples --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo --doc --all-features
cargo test -p kairo-examples --doc --all-features
cargo fmt --all -- --check
git diff --check
cargo test -p kairo-cluster-sharding allocation --all-targets --all-features
cargo fmt --all -- --check
cargo clippy -p kairo-cluster-sharding --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo-actor watch_then_unwatch_then_watch_with_changes_notification --all-targets --all-features
cargo test -p kairo-actor watch_with_then_unwatch_then_watch_changes_notification --all-targets --all-features
cargo test -p kairo-actor watch --all-targets --all-features
cargo fmt --all -- --check
cargo clippy -p kairo-actor --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo-testkit test_probe_unwatch_suppresses_custom_termination_message --all-targets --all-features
cargo test -p kairo-testkit probe --all-targets --all-features
cargo test -p kairo-testkit --doc --all-features
cargo fmt --all -- --check
cargo clippy -p kairo-actor --all-targets --all-features -- -D warnings
cargo clippy -p kairo-testkit --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo-testkit await_barrier --all-targets --all-features
cargo test -p kairo-testkit multi_node --all-targets --all-features
cargo test -p kairo-testkit --doc --all-features
cargo fmt --all -- --check
cargo clippy -p kairo-testkit --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo-testkit await_barriers --all-targets --all-features
cargo test -p kairo-testkit multi_node --all-targets --all-features
cargo test -p kairo-testkit --doc --all-features
cargo fmt --all -- --check
cargo clippy -p kairo-testkit --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo-distributed-data bootstrap_sender_keeps_remaining_route_delivering_after_peer_removed --all-targets --all-features
cargo test -p kairo-distributed-data tcp_peer_bootstrap --all-targets --all-features
cargo fmt --all -- --check
cargo clippy -p kairo-distributed-data --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo-cluster-tools bootstrap_sender_keeps_remaining_pubsub_route_delivering_after_peer_removed --all-targets --all-features
cargo test -p kairo-cluster-tools tcp_peer_bootstrap --all-targets --all-features
cargo fmt --all -- --check
cargo clippy -p kairo-cluster-tools --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo-cluster bootstrap_sender_keeps_remaining_membership_route_delivering_after_peer_removed --all-targets --all-features
cargo test -p kairo-cluster tcp_peer_bootstrap --all-targets --all-features
cargo fmt --all -- --check
cargo clippy -p kairo-cluster --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo-examples cluster_tcp_peer_bootstrap_keeps_remaining_join_route_after_peer_removed --all-targets --all-features
cargo test -p kairo-examples ddata_tcp_peer_bootstrap_keeps_remaining_read_route_after_peer_removed --all-targets --all-features
cargo test -p kairo-examples cluster_tools_tcp_peer_bootstrap_keeps_remaining_pubsub_route_after_peer_removed --all-targets --all-features
cargo test -p kairo-examples --test tcp_bootstrap_smoke --all-features
cargo test -p kairo-examples --all-targets --all-features
cargo fmt --all -- --check
cargo clippy -p kairo-examples --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo-cluster-sharding multi_node_region_discovery_allocates_remembered_shard_on_registration --all-targets --all-features
cargo test -p kairo-cluster-sharding region_discovery --all-targets --all-features
cargo test -p kairo-cluster-sharding --all-targets --all-features
cargo fmt --all -- --check
cargo clippy -p kairo-cluster-sharding --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo-actor actor_system_terminate_uses_one_timeout_across_user_and_system_guardians --all-targets --all-features
cargo test -p kairo-actor actor_system_terminate_requests_system_stop_even_when_user_stop_times_out --all-targets --all-features
cargo test -p kairo-actor actor_system_schedule_once_after_termination_is_cancelled --all-targets --all-features
cargo test -p kairo-actor scheduler --all-targets --all-features
cargo test -p kairo-actor clear_stash_drops_buffered_messages_and_updates_inspection_state --all-targets --all-features
cargo test -p kairo-actor local_core --all-targets --all-features
cargo test -p kairo-actor --all-targets --all-features
cargo fmt --all -- --check
cargo clippy -p kairo-actor --all-targets --all-features -- -D warnings
cargo test -p kairo-actor receive_timeout --all-targets --all-features
cargo test -p kairo-actor --all-targets --all-features
cargo test -p kairo-testkit --all-targets --all-features
cargo test -p kairo-cluster connector_automatic_retry_timer_drives_due_peer_routes --all-targets --all-features -- --nocapture
cargo test -p kairo-cluster peer_runtime_retries_failed_peer_dial_after_retry_interval --all-targets --all-features -- --nocapture
cargo test -p kairo-cluster tcp_runtime_routes_membership_and_heartbeat_over_bidirectional_association --all-targets --all-features -- --nocapture
cargo test -p kairo-remote tcp_reader_join_after_stop_ignores_late_stream_failures --all-targets --all-features -- --nocapture
cargo test -p kairo-cluster --all-targets --all-features
cargo test -p kairo-remote --all-targets --all-features
cargo test -p kairo-distributed-data --all-targets --all-features
cargo test -p kairo-cluster-tools --all-targets --all-features
cargo fmt --all -- --check
cargo clippy -p kairo-remote -p kairo-cluster -p kairo-distributed-data -p kairo-cluster-tools --all-targets --all-features -- -D warnings
cargo test -p kairo config --all-targets --all-features
cargo test -p kairo --doc --all-features
cargo clippy -p kairo --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo-examples configured_counter_example_smoke --all-targets --all-features
cargo test -p kairo-examples --test examples_smoke --all-features
cargo test -p kairo-examples --all-targets --all-features
cargo fmt --all -- --check
cargo clippy -p kairo-examples --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo-testkit --doc --all-features
cargo test -p kairo-testkit await_barriers --all-targets --all-features
cargo test -p kairo-testkit multi_node --all-targets --all-features
cargo fmt --all -- --check
cargo clippy -p kairo-testkit --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo-actor-macros --test remote_message --all-features
cargo test -p kairo-actor-macros --all-targets --all-features
cargo fmt --all -- --check
cargo clippy -p kairo-actor-macros --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo-serialization typed_deserialize_rejects_unexpected_manifest_before_decoding --all-targets --all-features
cargo test -p kairo-serialization --all-targets --all-features
cargo fmt --all -- --check
cargo clippy -p kairo-serialization --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo-testkit within --all-targets --all-features
cargo test -p kairo-testkit --all-targets --all-features
cargo test -p kairo-testkit --doc --all-features
cargo fmt --all -- --check
cargo clippy -p kairo-testkit --all-targets --all-features -- -D warnings
git diff --check
cargo test -p kairo toml_config_rejects_empty_singleton_role --all-targets --all-features
cargo test -p kairo toml_config_rejects_blank_singleton_role_after_projection --all-targets --all-features
cargo test -p kairo toml_config_rejects_blank_remote_hostname --all-targets --all-features
cargo test -p kairo toml_config_rejects_blank_downing_role --all-targets --all-features
cargo test -p kairo toml_config_loads_layered_files_with_later_overrides --all-targets --all-features
cargo test -p kairo config_validate_checks_all_format_neutral_sections --all-targets --all-features
cargo test -p kairo config --all-targets --all-features
cargo test -p kairo --doc --all-features
cargo fmt --all -- --check
cargo clippy -p kairo --all-targets --all-features -- -D warnings
git diff --check
```
