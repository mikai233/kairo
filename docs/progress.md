# Progress

## Current Milestone

M1: Local Actor Runtime Core is in progress.

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
- `ActorSystemBuilder::dispatcher_throughput` configures local mailbox batch
  throughput before worker yield.
- Stopping a local actor recursively requests child stops and runs the parent's
  `stopped` hook after children have terminated.
- Sends after stop are rejected and recorded as dead letters.
- Missing local actor refs reject user messages and record dead letters.
- System stop drains queued user messages to dead letters before delivery.
- Duplicate live names under `/user` are rejected; stopped names can be reused
  with a new path incarnation.
- Focused `kairo-actor` tests cover tell ordering, system stop, and post-stop
  dead letters, duplicate names, path incarnation reuse, context system access,
  child spawning, parent/child stop ordering, recipient behavior, and
  `PostStop` signal delivery, missing-ref dead letters, and dispatcher
  throughput settings, and actor-system termination.
- `kairo-actor` runtime code is split by responsibility across modules instead
  of living in a single `lib.rs`.

Not yet implemented:

- Full actor tree lifecycle semantics beyond recursive local stop.
- Coordinated shutdown.
- Death watch, supervision, timers, ask, adapters, event stream, receptionist,
  and deterministic testkit support.

## Last Validation

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets --all-features
```
