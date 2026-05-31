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
- Local actor refs, child links, and reserved names are removed before
  `PostStop`-side completion is observable, so name reuse is deterministic
  once the stopped hook has run.
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
- `kairo-actor` crate docs now explain typed local protocols, why
  `Actor::receive` is synchronous, why local messages do not need
  serialization, and how external work returns through mailbox messages with a
  compile-checked example.
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
- `Signal::ChildFailed` now reports a failed direct child to a parent that is
  watching that child, while non-parent watchers still receive plain
  `Signal::Terminated` and `watch_with` continues to deliver the caller's
  custom message.
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
- `Props::with_stash_capacity` enables opt-in typed stash support, and
  `Context::stash`, `unstash`, `unstash_all`, `clear_stash`, and stash
  inspection helpers provide FIFO replay before later mailbox messages with
  explicit disabled/full errors.
- Stash state lives in a focused `stash` module rather than being embedded in
  the actor runtime loop.
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
- `SupervisorStrategy::restart_with_limit` supports Pekko-style bounded
  restarts by allowing a configured number of restarts within a time window,
  stopping the actor when the limit is exceeded, and resetting the count after
  the window elapses.
- Restart supervision now defaults to stopping children and exposes explicit
  child-preserving restart policies for callers that want Pekko-style
  `withStopChildren(false)` semantics without changing the default behavior.
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
- `TestProbe<M>` can register typed death-watch messages through
  `watch_with`, and `TestProbe<AnyActorRef>` provides `watch_terminated` and
  `expect_terminated` helpers for deterministic local lifecycle assertions.
- `kairo-testkit::await_assert` retries result-returning test assertions until
  success or timeout and reports the final error with attempt metadata.
- `TestProbe::receive_messages` collects a fixed number of typed messages
  under one shared deadline and reports how many were received when the
  deadline expires.
- `TestProbe::fish_for_message` classifies probe messages as complete, fail,
  continue-and-collect, or continue-and-ignore under one shared deadline, with
  the fishing outcome API kept in a focused testkit module.
- `kairo-testkit::ManualTime` can deterministically advance scheduled
  one-shot deliveries to actor refs and supports cancellation through
  `ManualTimeHandle`.
- `ManualTime::expect_no_msg_for` advances manual time and verifies same-typed
  probes remain quiet after a short dispatcher settle window.
- `kairo-testkit` crate docs now describe typed probes, batch/fishing
  assertions, await assertions, manual time, and compile-checked examples.
- Testkit code is split into focused `probe`, `fishing`, `assertions`,
  `manual_time`, and `system` modules instead of living in one crate root.
- `ActorSystemBuilder::manual_scheduler` can build actor systems backed by a
  manual scheduler, and `ActorSystemTestKit::with_manual_time` wires that
  scheduler into `ManualTime`.
- Manual time now drives `ActorSystem::schedule_once`, single actor timers, and
  repeated fixed-delay/fixed-rate timer backends, and actor receive timeouts
  without real sleeps.
- `kairo-serialization` is split into focused `message`, `manifest`, `codec`,
  `registry`, `envelope`, and `errors` modules.
- `kairo-serialization` crate docs now explain that local actor messages do
  not need serialization, while remote messages require stable manifests,
  versions, serializer ids, registered codecs, and compile-checked examples.
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
- `kairo-actor-macros` crate docs now document that macro support is
  metadata-only, local messages do not need macros or serialization,
  serializer ids/codecs remain explicit, and `KairoRemoteMessage` has a
  compile-checked manifest/version example.
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
- TCP actor-system runtime shutdown now stops the runtime-owned remote
  death-watch actor with an explicit timeout before clearing association
  routes and stopping the listener, so remoting lifecycle ownership includes
  its local system actor instead of leaving it running after socket shutdown.
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
- `kairo-remote` now has a focused remote death-watch state module that tracks
  watched remote actor pairs and watched addresses, plans heartbeat
  start/stop/send effects, re-watches after remote UID changes, emits
  address-terminated effects for unreachable addresses, and resets failure
  detection when watching resumes after an unreachable observation.
- Remote death-watch state now keeps inbound remote watch registrations
  separate from outbound watch intent, so decoded wire watch/unwatch messages
  record the remote watcher of a local watchee without starting local outbound
  heartbeat monitoring or echoing another watch message back to the peer.
- `kairo-remote` now has an actor-backed remote death-watch command handler
  that wraps the focused state machine in synchronous actor turns, emits
  transport-neutral effects through an explicit sink boundary, handles
  heartbeat ticks, heartbeat acks, unreachable observations, watch/unwatch
  commands, inbound remote watch/unwatch registrations, and reports
  deterministic watch statistics for tests and future diagnostics.
- `kairo-remote` now has a focused remote death-watch outbound effect sink
  that serializes watch, unwatch, heartbeat, and re-watch effects through the
  registered remote protocol codecs to the stable `/system/remote-watch`
  recipient path on the target address, observes local timer/failure-detector
  effects explicitly, and propagates missing-codec or outbound failures.
- `kairo-remote` now has a focused remote death-watch inbound protocol
  delivery adapter that maps decoded remote watch/unwatch/heartbeat/heartbeat
  ack messages into the actor-backed remote watcher, derives remote addresses
  from stable sender actor-ref wire data, replies to inbound heartbeats with
  local UID heartbeat acknowledgements, and drives re-watch effects from
  heartbeat acks with new remote UIDs.
- `kairo-remote` now has a focused remote death-watch system inbound boundary
  that dispatches remote envelopes by stable manifest, deserializes
  watch/unwatch/heartbeat/heartbeat-ack protocol messages through the
  registered codecs, routes them to the actor-backed remote watcher, and
  rejects unknown death-watch manifests or missing codecs explicitly.
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
- TCP actor-system runtime tests now cover the remote death-watch control-lane
  path across a bidirectional association: outbound watch registration reaches
  the peer as an inbound local-watchee registration, heartbeat messages are
  acknowledged over the reverse lane, and the watcher re-sends watch metadata
  after observing the peer UID.
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
- `kairo-distributed-data` now has a focused inbound remote-reply bridge for
  sender-addressed aggregation replies. `ReplicatorRemoteReplyInbound` decodes
  stable ACK/NACK/read-result manifests, tags them with the source
  `ReplicaId`, resolves the `RemoteEnvelope` recipient `ActorRefWireData` to a
  local temporary write or read aggregation child, and routes missing or
  mistyped targets through normal actor dead-letter diagnostics.
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
- `kairo-distributed-data` now has a focused TCP peer-route owner that consumes
  cluster membership-derived dial/remove plans, applies them to
  `ReplicatorTcpAssociationRuntime`, keeps route registrations separate from
  membership state, and closes/removes cached ddata routes when peers become
  locally unreachable or leave.
- `kairo-distributed-data` now has a pure TCP peer-reconnect state machine for
  distributed-data peer routes, with validated retry settings, per-peer attempt
  counts, deterministic due-time selection, and clear-on-success/remove
  behavior ready for actor/runtime integration.
- `kairo-distributed-data` now has a TCP peer runtime that owns the cluster
  peer planner, distributed-data route owner, reconnect state, and configured
  TCP association runtime together, applying membership snapshots/events,
  retrying due failed dials, and clearing active routes plus pending reconnects
  during shutdown.
- `kairo-distributed-data` now has an actor-backed TCP peer connector that
  subscribes to cluster snapshots/events, applies membership-derived ddata peer
  routes through `ReplicatorTcpPeerRuntime`, drives explicit and timer-based
  retry turns, exposes typed snapshots for deterministic tests, and shuts down
  the owned runtime when the connector actor stops.
- `kairo-distributed-data` now has a TCP peer bootstrap facade that binds the
  distributed-data peer runtime, spawns the connector actor with explicit
  settings, and registers coordinated shutdown to stop the connector before
  cluster shutdown so socket cleanup goes through the actor stop path.
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
  reachable, unreachable, all, or self-quarantined members and are reused by
  the actor-backed downing provider.
- `kairo-cluster::SplitBrainResolverHook` provides the first concrete
  synchronous downing policies for `down-all`, `keep-majority`, and
  `keep-oldest`, including role-filtered majority decisions, tie-breaking by
  lowest address, oldest-member survival, and `down-if-alone` behavior.
  Indirectly-connected graph handling and lease-majority remain future work.
- `kairo-cluster::DowningProviderActor` now wraps the downing hook boundary in
  an actor-backed stable-after timer: it observes gossip snapshots, tracks
  relevant unreachable members, resets or cancels the timer when reachability
  changes, gates decisions to the reachable leader, and sends structured
  `ApplyDowningDecision` commands to the membership actor after the stable
  period.
- `kairo-cluster::ClusterMembership` can register a typed
  `DowningProviderActor` observer, forwards each current gossip snapshot to it,
  and applies the provider's stable downing decision through the existing
  membership state machine.
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
- `kairo-cluster::HeartbeatReceiver` and `HeartbeatSender` provide the first
  actor-backed heartbeat I/O slice: current-state initialization, typed
  receiver route registration, periodic tick scheduling, heartbeat request and
  response messages with stable remote manifests, expected-first-heartbeat
  monitoring, cluster membership/reachability event updates, and
  failure-detector cleanup on stop.
- `kairo-cluster` now has focused remote-envelope heartbeat routing:
  `HeartbeatRemoteReceiverOutbound` can be registered as a typed heartbeat
  receiver route and sends stable `Heartbeat` payloads to
  `/system/cluster/heartbeatReceiver`, `HeartbeatRemoteReceiverInbound`
  replies to request sender metadata with stable `HeartbeatRsp` payloads, and
  `HeartbeatRemoteResponseInbound` feeds remote responses back into the local
  heartbeat sender's failure detector path.
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
- `kairo-cluster` now has a focused transport-neutral membership wire bridge
  that maps typed join, welcome, and gossip membership messages to serialized
  stable cluster protocol payloads with target-node routing metadata, routes
  inbound serialized payloads into the actor-backed membership state machine,
  and uses an actor-backed outbound adapter for welcome/gossip talkback replies
  without adding socket transport or a central membership authority.
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
- `kairo-cluster` now has a focused cluster-derived association peer planner
  that consumes `CurrentClusterState` snapshots and cluster events, excludes
  self, follows Pekko's local-observer reachability rule for gossip peer
  validity, rejects non-self local-only peers, and emits explicit dial/remove
  targets for future multi-peer TCP runtime ownership without making remoting a
  membership authority.
- `kairo-cluster` now has a focused TCP peer-route owner that applies
  cluster-derived dial/remove plans to `ClusterTcpAssociationRuntime`, keeps
  per-peer route registrations separate from membership state, and closes and
  removes cached routes when peers become locally unreachable or leave.
- `kairo-cluster` now has a focused TCP peer runtime lifecycle owner that
  composes the cluster TCP socket runtime, membership-derived peer planner, and
  peer-route table, applies cluster snapshots/events to live routes, and clears
  peer routes before listener shutdown.
- `kairo-cluster` now has focused TCP peer reconnect state. Failed
  membership-derived dials are retained as deterministic pending retries with a
  configured retry interval, successful retries clear pending state, and member
  removal or local-unreachable events cancel obsolete retry attempts.
- `kairo-cluster` crate docs now explain gossip-based membership, vector-clock
  merge, observer-owned reachability/failure-detector observations, why
  discovery is contact-only, and why Kairo does not use etcd or another
  central membership authority, with a compile-checked example.
- `kairo-cluster` now has an actor-backed TCP peer connector that subscribes
  to cluster snapshots/events, feeds the cluster TCP peer runtime, exposes
  explicit deterministic retry ticks, can schedule fixed-delay retry ticks with
  actor timers, reports snapshots for tests, and shuts the owned TCP runtime
  down when the connector actor stops.
- `kairo-cluster` now has a TCP peer bootstrap facade that binds the cluster
  TCP peer runtime, spawns the connector actor with explicit settings, exposes
  the connector ref/self node/local association address, and registers
  coordinated shutdown to stop the connector before cluster shutdown.
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
- `kairo-cluster-tools` now has a typed local singleton manager actor that
  interprets manager start/stop effects by spawning the singleton child under
  the manager, watching the child, sending the configured typed termination
  message during handoff, and completing handoff after child termination.
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
- `kairo-cluster-tools` now has a focused local pubsub mediator state over
  named local topics, including current-topic listing, routed publish,
  subscribe/unsubscribe delegation, empty-topic cleanup, and subscriber removal
  across all topics.
- `kairo-cluster-tools` now has an actor-backed local pubsub mediator protocol
  that wraps the local pubsub state in synchronous actor turns, sends typed
  subscribe acks, publish reports, and current-topic replies, watches
  subscribers, and removes terminated subscribers from all local topics.
- `kairo-cluster-tools` now has a focused distributed pubsub registration
  state with Pekko-style versioned owner buckets, present/tombstone entries,
  peer-version delta collection, delta merge, tombstone pruning, broadcast
  target planning, and deterministic one-target-per-group planning.
- `kairo-cluster-tools` now has a transport-neutral pubsub delivery planner
  that converts distributed topic registrations into explicit local and remote
  delivery targets for broadcast and one-message-per-group publishes.
- `kairo-cluster-tools` now has a transport-neutral pubsub delivery transport
  that sends planned publish effects to local or remote mediator recipients,
  reports missing/send failures explicitly, and uses group-specific mediator
  commands so one-message-per-group delivery reaches only selected groups.
- `kairo-cluster-tools` now has an actor-backed distributed pubsub registry
  gossip slice with explicit peer recipients, deterministic status ticks,
  status/delta exchange, known-peer filtering for inbound deltas, peer removal
  pruning, and delta-count inspection.
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
- `kairo-cluster-tools` now declares stable remote metadata and explicit codecs
  for distributed pubsub gossip status and delta messages, including
  `UniqueAddress`, bucket versions, topic/group registry entries, tombstones,
  and known-version maps without relying on Rust type names, discriminants, or
  memory layout.
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
- `kairo-cluster-tools` now has a focused TCP peer-route owner that consumes
  cluster membership-derived dial/remove plans, applies them to
  `ClusterToolsTcpAssociationRuntime`, keeps route registrations separate from
  membership state, and closes/removes cached routes when peers are removed by
  local reachability or membership changes.
- `kairo-cluster-tools` now has a focused TCP peer runtime lifecycle owner
  that composes the cluster-tools TCP socket runtime, membership-derived peer
  planner, peer-route table, and dedicated reconnect state module. It applies
  snapshots/events to live pubsub/singleton routes, retries failed dials on
  explicit ticks, and clears routes plus pending retries before shutdown.
- `kairo-cluster-tools` now has an actor-backed TCP peer connector that
  subscribes to cluster snapshots/events, feeds the cluster-tools TCP peer
  runtime, exposes route/reconnect snapshots, supports explicit deterministic
  retry ticks, and can schedule fixed-delay retry ticks with actor timers.
- `kairo-cluster-tools` now has a TCP peer bootstrap facade that binds the
  tools TCP peer runtime from remote transport settings, spawns the connector,
  exposes its connector ref/self node/local association address, and registers
  a coordinated-shutdown actor-termination task before cluster shutdown.
- `kairo-examples` now includes a runnable cluster-tools TCP peer bootstrap
  example, with pubsub gossip, pubsub delivery, singleton inbound wiring, and
  reusable route/snapshot setup kept in a focused example module.
- The `kairo` facade now has a `config` feature with format-neutral
  `KairoSettings` structs and a TOML loader for the initial `[actor]`,
  `[remote]`, `[cluster]`, `[cluster.sharding]`, and `[cluster.tools]`
  sections, including explicit type/value validation and unknown-key rejection.
- The `kairo` facade crate docs now describe feature-gated module boundaries,
  the prelude, local-vs-remote serialization requirements, TOML-first
  configuration, and a compile-checked settings parse example.
- `KairoSettings` now exposes feature-gated runtime conversion helpers for
  actor-system dispatcher configuration, remote transport settings, cluster
  failure-detector/heartbeat settings, sharding shard counts, and cluster-tools
  pubsub settings while keeping the base config model usable without enabling
  every runtime crate.
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
- `kairo-examples` now includes an `ask_pipe_to_self` example with reusable
  calculation-service and pattern-coordinator modules, demonstrating
  `Context::ask` and `Context::pipe_to_self` without placing the actor logic
  in one binary file.
- `kairo-examples` now includes a runnable local cluster-sharding example that
  wires a shard coordinator, local shard region, `ShardingEnvelopeRouter`, and
  `EntityRef<String>` through reusable helper code and demonstrates stable
  shard-id routing into an entity-backed local shard whose typed counter
  entity receives business messages.
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
  one peer route before coordinated shutdown.
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
- `kairo-cluster-sharding` now has a transport-backed remote region control
  target for coordinator-driven `HostShard`, `BeginHandOff`, and `HandOff`
  sends, plus coordinator-side inbound routing for stable `ShardStarted`,
  `BeginHandOffAck`, and `ShardStopped` replies back into coordinator and
  handoff-worker actor turns.
- `kairo-cluster-sharding` now has region-side inbound handling for stable
  remote `HostShard`, `BeginHandOff`, and `HandOff` commands; these re-enter
  region actor state transitions and emit stable `ShardStarted`,
  `BeginHandOffAck`, or immediate `ShardStopped` replies where the current
  region runtime can complete the command synchronously.
- `ShardRegionActor<M>` can now opt into a region-side remote handoff
  stop-message factory, so stable remote `HandOff` commands for locally hosted
  shards forward into the local shard, observe the shard handoff plan, ask for
  stopper completion when required, mark the shard stopped, and send stable
  `ShardStopped` replies without putting business stop messages on the wire.
- `kairo-cluster-sharding` now has a local graceful region-shutdown path:
  regions notify their registered coordinator with `GracefulShutdownReq`,
  coordinators mark that region as gracefully shutting down, start handoff
  workers for each shard it owns, exclude it from new allocations, reallocate
  completed handoffs through the normal shard-home path, and regions stop once
  their local shards and buffers are gone.
- `kairo-cluster-sharding` crate docs now explain `EntityRef<M>` and
  `ShardingEnvelope<M>` routing, why sharded business messages do not embed
  entity ids by default, and the documented stable FNV-1a shard hash with a
  compile-checked example.
- The repository README and `kairo-next` README now describe the active
  Rust-first rewrite workspace, the old `crates/` implementation as
  reference-only, the gossip-not-etcd cluster constraint, typed actor and
  sharding APIs, and the current runnable examples.

Not yet implemented:

- Full actor tree lifecycle semantics beyond recursive local stop and
  restart-time child handling.
- Full actor-system local/remote provider integration, optional codec helper
  crates, reader supervision/restart policy, richer actor-system lifecycle
  wiring around the existing TCP association primitives, and broader
  cross-crate compatibility fixtures.
- Distributed-data still needs broader multi-node validation around the
  focused TCP association runtime, peer-route owner, reconnect state, peer
  runtime, actor-backed connector, and bootstrap beyond the current localhost
  two-node example smoke test.
- Sharding remember-entity stores still need broader automatic region/shard
  orchestration, including restart backoff policy integration and broader
  multi-node validation of the discovery subscriber plus region/coordinator
  flow. Graceful region shutdown still needs stable remote wire messages and
  multi-node validation.
- Cluster, distributed-data, and cluster-tools socket integration still need
  broader multi-node tests around the bootstrap facades beyond the current
  localhost two-node example smoke tests.
- Multi-node cluster membership socket lifecycle orchestration still needs
  indirectly-connected split-brain handling, lease-majority support, and
  broader automated multi-node scenarios beyond the current local two-node
  membership/downing socket validation.

## Last Validation

```bash
cargo fmt --all -- --check
cargo test -p kairo-cluster-sharding region_system_inbound_completes_hosted_remote_handoff_with_local_stop_message
cargo test -p kairo-cluster-sharding remote_control
cargo test -p kairo-cluster-sharding region_system_inbound
cargo test -p kairo-cluster-sharding graceful_shutdown
cargo test -p kairo-cluster-sharding --all-targets --all-features
cargo clippy -p kairo-cluster-sharding --all-targets --all-features -- -D warnings
git diff --check
```
