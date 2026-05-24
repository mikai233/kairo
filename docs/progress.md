# Progress

## Current Milestone

M1: Local Actor Runtime Core is in progress.

Implemented:

- `kairo-actor` can spawn a typed local actor under `/user`.
- `ActorRef<M>::tell` enqueues typed messages into the actor mailbox.
- Actors process messages one at a time through synchronous `Actor::receive`.
- `Context::stop(ctx.myself())` stops the current local actor ref.
- `ActorSystem::stop` can stop an idle local actor through the system lane.
- `Context::system`, `Context::spawn`, and `Context::spawn_anonymous` are
  available for local actors.
- Stopping a local actor recursively requests child stops and runs the parent's
  `stopped` hook after children have terminated.
- Sends after stop are rejected and recorded as dead letters.
- System stop drains queued user messages to dead letters before delivery.
- Duplicate live names under `/user` are rejected; stopped names can be reused
  with a new path incarnation.
- Focused `kairo-actor` tests cover tell ordering, system stop, and post-stop
  dead letters, duplicate names, path incarnation reuse, context system access,
  child spawning, and parent/child stop ordering.

Not yet implemented:

- Full actor tree lifecycle semantics beyond recursive local stop.
- Dispatcher throughput controls.
- External `ActorSystem` stop APIs and coordinated shutdown.
- Death watch, supervision, timers, ask, adapters, event stream, receptionist,
  and deterministic testkit support.

## Last Validation

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets --all-features
```
