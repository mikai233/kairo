# Progress

## Current Milestone

M2: Lifecycle, Supervision, Patterns, And Testkit is in progress. The M1 local
actor runtime vertical slice is runnable and remains the foundation for M2
work.

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
- Timer state and envelopes live in a focused `timers` module and active timers
  are cancelled when the owning actor stops.
- `ActorSystem::event_stream` and `Context::event_stream` expose a local typed
  event stream for exact Rust event types.
- Event-stream subscription state lives in a focused `event_stream` module.

Not yet implemented:

- Full actor tree lifecycle semantics beyond recursive local stop.
- Coordinated shutdown.
- Fixed-rate timers, supervision, ask, adapters, receptionist, and
  deterministic testkit support.

## Last Validation

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets --all-features
```
