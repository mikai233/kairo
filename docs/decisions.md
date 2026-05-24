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
