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
- `ActorSystem::terminate` stops top-level `/user` actors, waits for
  termination, and rejects later spawns.
- `Context::system`, `Context::spawn`, and `Context::spawn_anonymous` are
  available for local actors.
- `Context::parent`, `Context::children`, and `Context::child` expose local
  actor-tree introspection.
- `Context::stop` can stop the current actor or a typed direct child actor ref
  without stopping the parent, and returns an explicit error for invalid
  targets.
- `ActorSystemBuilder::dispatcher_throughput` configures local mailbox batch
  throughput before worker yield.
- Stopping a local actor recursively requests child stops and runs the parent's
  `stopped` hook after children have terminated.
- Sends after stop are rejected and recorded as dead letters.
- Missing local actor refs reject user messages and record dead letters.
- System stop drains queued user messages to dead letters before delivery.
- Duplicate live names under `/user` are rejected; stopped names can be reused
  with a new path incarnation.
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
- Focused timer tests cover single timers, active-key cleanup, replacement,
  cancellation after enqueue, and actor-stop timer cleanup.
- Focused fixed-delay timer tests cover repeated delivery, cancellation, and
  replacement generation filtering.
- Focused event-stream tests cover typed subscription, duplicate subscription
  suppression, exact event-type matching, publishing, and unsubscribe.
- `kairo-actor` runtime code is split by responsibility across modules instead
  of living in a single `lib.rs`.
- Local actor name and child-tree bookkeeping now lives in a focused registry
  module instead of being embedded in the system runtime loop.
- `ActorPath` now stores structured address, path segments, and incarnation UID
  metadata while preserving the stable display string.
- Local death watch is available through `Context::watch`,
  `Context::watch_with`, and `Context::unwatch`.
- `Signal::Terminated` is delivered once to local watchers after the watched
  actor terminates; `watch_with` delivers a typed custom protocol message, and
  `unwatch` suppresses later local termination notification.
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
- `ActorSystem::event_stream` and `Context::event_stream` expose a local typed
  event stream for exact Rust event types.
- Event-stream subscription state lives in a focused `event_stream` module.
- `Context::spawn_task` starts external local work with only a typed self ref,
  and `Context::pipe_to_self` maps task success or failure back into the
  actor's protocol through the normal mailbox.
- Task bridge state and handles live in a focused `tasks` module.
- `Context::message_adapter` creates typed local adapter refs that enqueue
  adapted protocol messages into the owning actor's mailbox.
- Adapted user-message envelopes live in the mailbox runtime, and adapter ref
  construction lives in a focused `adapters` module.
- `Context::ask` creates a one-shot typed local reply ref, maps replies or
  `AskError::Timeout` back into the owning actor's mailbox, and rejects late
  replies after completion.
- Ask state and timeout handling live in a focused `asks` module.
- `ActorSystem::receptionist` and `Context::receptionist` expose a local typed
  receptionist with `ServiceKey<M>`, `Listing<M>`, register, deregister, find,
  subscribe, immediate listings, update publication, and actor-termination
  cleanup.
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
  supervision, which sends `Signal::PreRestart`, cancels timers, stops children,
  rebuilds actor state, reruns `started`, and preserves the actor ref path.
- Supervision strategy definitions live in a focused `supervision` module.
- `kairo-testkit` exposes a typed `TestProbe<M>` backed by a local actor and
  queue, plus `ActorSystemTestKit` for creating probe-backed local actor
  systems in tests.
- `kairo-testkit::ManualTime` can deterministically advance scheduled
  one-shot deliveries to actor refs and supports cancellation through
  `ManualTimeHandle`.
- Testkit code is split into focused `probe`, `manual_time`, and `system`
  modules instead of living in one crate root.
- `ActorSystemBuilder::manual_scheduler` can build actor systems backed by a
  manual scheduler, and `ActorSystemTestKit::with_manual_time` wires that
  scheduler into `ManualTime`.
- Manual time now drives `ActorSystem::schedule_once`, single actor timers, and
  repeated fixed-delay/fixed-rate timer backends without real sleeps.
- `kairo-serialization` is split into focused `message`, `manifest`, `codec`,
  `registry`, `envelope`, and `errors` modules.
- `RemoteMessage`, `MessageCodec<M>`, `DynCodec`, `SerializedMessage`, and
  `RemoteEnvelope` define the stable metadata and payload boundary for remote
  messages.
- `Registry` implements explicit codec registration, outbound type lookup, and
  inbound `(serializer_id, manifest)` lookup.
- Serialization registration rejects empty manifests, duplicate serializer ids,
  and duplicate manifests.
- Focused serialization tests prove wire metadata includes serializer id,
  manifest, version, and bytes, and does not depend on Rust type names or enum
  discriminants.
- `KairoRemoteMessage` derive parses
  `#[kairo(manifest = "...", version = N)]` and emits only the
  `RemoteMessage` metadata implementation; it does not select or generate a
  codec.
- `ActorRefWireData` stores serialized actor-ref paths with explicit protocol,
  system, host, and port metadata, and `ActorRefResolver` defines the provider
  boundary for resolving those refs later.
- `RemoteEnvelope` carries actor-ref wire data for recipient and optional
  sender rather than unstructured path strings.
- `kairo-remote`, `kairo-cluster`, `kairo-distributed-data`, and
  `kairo-cluster-sharding` now have focused `protocol` modules declaring the
  first stable `RemoteMessage` manifests for remote watch/heartbeat, cluster
  gossip, distributed-data replicator, and sharding coordinator protocols.
- Serialization tests cover rolling-version decode behavior by proving codecs
  receive the wire `version` and can decode older payload shapes under the same
  stable manifest.
- `kairo-cluster-sharding` is split into focused API modules for entity refs,
  type keys, envelopes, hashing, errors, and protocol metadata.
- `ShardingEnvelope<M>` carries entity ids outside business messages, and
  `EntityRef<M>` wraps plain business messages in that envelope before sending
  to the region.
- Shard IDs use documented 64-bit FNV-1a over entity id bytes with
  `hash % shard_count`; `DEFAULT_SHARD_COUNT` is 100.
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
- `kairo-cluster::DeadlineFailureDetector` and `FailureDetectorRegistry`
  provide deterministic heartbeat monitoring with Pekko-style semantics:
  unmonitored resources are available, first heartbeat starts monitoring,
  resources become unavailable after heartbeat interval plus acceptable pause,
  and removal forgets detector state.

Not yet implemented:

- Full actor tree lifecycle semantics beyond recursive local stop.
- Parent-level supervision escalation and restart limits/backoff.
- Concrete actor-system/provider actor-ref resolution, protocol codecs,
  optional codec helper crates, and broader cross-crate compatibility fixtures.
- Sharding region, shard, coordinator allocation, handoff, passivation,
  rebalancing, and remember-entity storage.
- Cluster membership actors, heartbeat sender/receiver actors, actor-backed
  cluster event subscription/publication, and downing hooks.

## Last Validation

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets --all-features
```
