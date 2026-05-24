# Progress

## Current Milestone

M1: Local Actor Runtime Core is in progress.

Implemented:

- `kairo-actor` can spawn a typed local actor under `/user`.
- `ActorRef<M>::tell` enqueues typed messages into the actor mailbox.
- Actors process messages one at a time through synchronous `Actor::receive`.
- `Context::stop(ctx.myself())` stops the current local actor ref.
- `ActorSystem::stop` can stop an idle local actor through the system lane.
- Sends after stop are rejected and recorded as dead letters.
- System stop drains queued user messages to dead letters before delivery.
- Focused `kairo-actor` tests cover tell ordering, system stop, and post-stop
  dead letters.

Not yet implemented:

- Parent/child actor tree operations beyond root `/user` spawn paths.
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
