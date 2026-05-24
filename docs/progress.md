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
- `Address` exposes explicit construction plus protocol, system, host, and
  port accessors so remote and cluster codecs can round-trip structured node
  addresses without relying on path string parsing as their primary API.
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
- `kairo-serialization::WireWriter` and `WireReader` provide a small shared
  stable binary helper for explicit system-protocol codecs, using
  length-prefixed UTF-8 strings and byte payloads, optional strings, boolean
  markers, and big-endian numeric fields instead of Rust memory layout.
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
- `kairo-remote` is split into focused modules for settings, errors, outbound
  delivery, association state, provider resolution, remote refs, and protocol
  metadata instead of concentrating remote logic in the crate root.
- `RemoteActorRef<M>` serializes `RemoteMessage` values through the registry
  into `RemoteEnvelope` values, preserves optional sender actor-ref wire data,
  implements the typed `Recipient<M>` boundary, and returns rejected messages
  on serialization or outbound failures.
- `RemoteActorRefProvider` resolves stable remote actor-ref paths with explicit
  host metadata into typed `RemoteActorRef<M>` values and rejects local-only
  paths instead of silently treating them as remote refs.
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
- `kairo-distributed-data` now has focused read/write aggregation wire helpers
  that convert typed `DataEnvelope<D>` values to stable
  `ReplicatorDataEnvelope`, `ReplicatorWrite`, `ReplicatorRead`, and
  `ReplicatorReadResult` messages through explicit CRDT codecs and reject
  manifest mismatches.
- `kairo-distributed-data` now has focused CRDT foundation modules for
  `ReplicatedData`, delta CRDT contracts, `ReplicaId`, `GSet`, `GCounter`, and
  `PNCounter` instead of concentrating data logic in the crate root.
- `GSet` preserves immutable add-only union semantics with accumulated deltas;
  `GCounter` stores per-replica absolute counts and merges by maximum value;
  `PNCounter` composes increment and decrement `GCounter`s with explicit
  overflow errors for supported integer bounds.
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
  registration, shard-home, host-shard, start, handoff, and stopped protocol
  messages.
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
- `kairo-cluster-sharding` now has a focused shard-region runtime planner that
  buffers unknown shard messages, requests shard homes once per buffered shard,
  forwards buffered messages when a remote home is learned, starts local shards
  for local homes or `HostShard`, drops handoff buffers to preserve ordering,
  and emits explicit handoff ack/stopped plans.
- `kairo-cluster-sharding` now has a focused shard entity runtime planner for
  local shard behavior: first messages start entities, active entities receive
  business messages directly, passivating entities buffer later messages,
  passivation sends the configured stop message once, termination removes or
  restarts entities depending on buffered messages, and shard handoff emits
  stopper or stopped plans.
- `kairo-cluster-sharding` now has focused remember-entity store state for the
  coordinator's add-only remembered shard set and each shard's started/stopped
  entity set, including Pekko-compatible five-key partitioning based on stable
  Java string hashing for future distributed-data-backed storage.
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
- `kairo-cluster::DowningHook`, `DowningDecision`, and `DowningPlan` define the
  initial downing hook boundary and SBR-style decision mapping for downing
  reachable, unreachable, all, or self-quarantined members before actor-backed
  providers are wired in.
- `kairo-cluster::SplitBrainResolverHook` provides the first concrete
  synchronous downing policies for `down-all`, `keep-majority`, and
  `keep-oldest`, including role-filtered majority decisions, tie-breaking by
  lowest address, oldest-member survival, and `down-if-alone` behavior. Full
  stable-after provider timing, indirectly-connected graph handling, and
  lease-majority remain future work.
- `kairo-cluster::HeartbeatNodeRing` and `HeartbeatSenderState` model
  Pekko-style heartbeat receiver selection and sender bookkeeping, including
  deterministic ring ordering, configured receiver limits, unreachable receiver
  inclusion, and continued monitoring of removed-but-unavailable receivers until
  recovery.
- `kairo-cluster::HeartbeatReceiver` and `HeartbeatSender` provide the first
  actor-backed heartbeat I/O slice: current-state initialization, typed
  receiver route registration, periodic tick scheduling, heartbeat request and
  response messages with stable remote manifests, expected-first-heartbeat
  monitoring, cluster membership/reachability event updates, and
  failure-detector cleanup on stop.
- `kairo-cluster::ClusterEventPublisher` is an actor-backed cluster event
  publisher that stores the latest gossip, publishes `ClusterEvent` diffs to
  typed subscribers, supports initial state replay as events, handles explicit
  event publication, unsubscribe, and current-state snapshot requests.
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
- `kairo-cluster-tools` is split into focused topic and singleton modules, and
  now has Pekko-style singleton oldest-member tracking that filters by role,
  sorts eligible UP members by cluster age, reports oldest changes from member
  events, and marks takeover unsafe while older leaving/exiting/down members
  are still present.
- `kairo-cluster-tools` now has a focused singleton manager runtime planner
  that turns oldest-member observations and handover messages into explicit
  start-singleton, stop-singleton, handover, takeover, and manager-stop
  effects, covering safe immediate startup, delayed takeover, previous-oldest
  removal, and handover completion before actor wiring is added.
- `kairo-cluster-tools` topic support is split into focused name and local
  topic modules, with typed local subscriptions, duplicate suppression,
  unsubscribe/removal handling, broadcast delivery, and deterministic
  one-message-per-group publish routing for the first pubsub foundation.

Not yet implemented:

- Full actor tree lifecycle semantics beyond recursive local stop.
- Parent-level supervision escalation and restart limits/backoff.
- Full actor-system local/remote provider integration, optional codec helper
  crates, transport-backed associations, actor-system-backed inbound target
  resolution, and broader cross-crate compatibility fixtures.
- Distributed-data transport-backed remote delta propagation, direct write/read
  aggregators, pruning scheduling, and gossip-backed replication.
- Actor-backed sharding region/shard/coordinator wiring, coordinator rebalance
  timers, transport-backed handoff delivery, and distributed-data-backed
  remember-entity store actors.
- Actor-backed cluster singleton manager/proxy handover and distributed pubsub
  mediator/topic replication.
- Multi-node cluster membership transport/routing, remote-backed heartbeat
  receiver routing, actor-backed downing provider timing, indirectly-connected
  split-brain handling, and lease-majority support.

## Last Validation

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets --all-features
```
