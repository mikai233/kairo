# Agent Instructions

This repository is a ground-up Rust redesign of Kairo, an Akka/Pekko-inspired
actor, remoting, cluster, and sharding framework. Agents should treat
[docs/goal.md](docs/goal.md) as the product and milestone target, and
[kairo-next/ARCHITECTURE.md](kairo-next/ARCHITECTURE.md) as the technical
contract.

The legacy root `crates/` implementation was removed after its M13 removal
gates were verified. Historical sources are available through Git history; all
implementation work belongs under `kairo-next/`.

## Start Of Each Turn

1. Read `docs/goal.md`.
2. Read `kairo-next/ARCHITECTURE.md`.
3. Read `docs/progress.md` if it exists.
4. Inspect the current git diff before editing.
5. Run or inspect the most relevant failing test when the task is about code.
6. Choose the smallest verifiable task that advances the current milestone.

## End Of Each Turn

1. Run the relevant tests for the changed area.
2. Run formatting checks when practical.
3. Update `docs/progress.md` if milestone status changed.
4. Update `docs/decisions.md` when a new design decision is made.
5. Update `docs/blocked.md` when progress is blocked by an external decision.
6. In goal mode, do not commit merely because a turn ended. When the run is
   authorized to commit, commit only at an appropriate verified logical
   checkpoint as defined below; otherwise leave a clear status report.

## Hard Constraints

- Do not recreate or revive the legacy root `crates/` workspace. Use Git
  history for archaeology and keep all implementation under `kairo-next/`.
- Use `~/IdeaProjects/pekko` as the local semantic reference for
  Pekko/Akka behavior before implementing actor, remote, cluster, distributed
  data, sharding, or cluster-tools logic.
- Preserve Pekko semantics where they define observable behavior, but do not
  copy Scala inheritance, implicit APIs, builders, or DSL shape into Rust.
- Do not collapse the rewrite into one crate. Keep the `kairo-next/crates/*`
  workspace boundary unless a documented architecture decision changes it.
- Do not model cluster membership through etcd, Kubernetes leases, or any
  central authoritative store. Cluster membership is gossip plus local failure
  detector observations.
- Discovery may provide seed/contact addresses only. It must not be the source
  of cluster truth.
- Do not add `AsyncActor` in the initial design. `Actor::receive` is
  synchronous; async work returns to the actor through messages.
- Do not require serialization for local-only messages.
- Remote messages must use stable `RemoteMessage` metadata and registered
  codecs. Do not rely on Rust enum discriminants, Rust type names, memory
  layout, or compiler-generated details as wire contracts.
- Do not make a global message enum or erased `DynMessage` the primary user
  API. `ActorRef<M>` is the typed boundary.
- Do not force sharded entity IDs into business messages. Prefer
  `EntityRef<M>` and `ShardingEnvelope<M>`; extractors are optional adapters.
- Do not use Rust `DefaultHasher` for cross-node shard allocation. Use a fixed,
  documented stable hash.
- Do not implement remote actor deployment before local actors, remoting, and
  cluster membership are stable.
- Use TOML as the first configuration file format. Do not introduce HOCON or a
  `hocon-rs` dependency until that parser is intentionally selected later.
- Do not add broad third-party dependencies "just in case". Add dependencies
  only when the implementing code needs them.
- Do not delete or weaken tests to make failures pass.

## Engineering Priorities

Correctness comes first, followed by testability, semantic parity with
Pekko/Akka where it matters, Rust-first API ergonomics, failure behavior,
performance, and surface polish.

Prefer runnable vertical slices over large incomplete subsystems. The most
important early loops are:

```text
spawn -> tell -> mailbox -> actor receive -> stop/dead letters
local actor -> remote envelope -> transport -> remote actor mailbox
seed join -> welcome -> gossip convergence -> cluster event
EntityRef -> ShardingEnvelope -> region -> shard -> entity actor
```

Keep implementation structure modular. Do not pile unrelated logic into one
large `lib.rs`; split code by responsibility into focused modules that match
the crate boundary and architecture documents.

## Pekko Reference Discipline

When a task touches behavior already solved by Pekko, inspect the relevant
local files under `~/IdeaProjects/pekko` first.

Default reference areas:

```text
actor-typed/.../ActorRef.scala
actor-typed/.../Behavior.scala
actor/.../ActorCell.scala
actor/.../dispatch/Mailbox.scala
actor/.../dungeon/DeathWatch.scala
remote/.../RemoteActorRefProvider.scala
remote/.../RemoteWatcher.scala
remote/.../MessageSerializer.scala
cluster/.../Gossip.scala
cluster/.../MembershipState.scala
cluster/.../Reachability.scala
cluster/.../VectorClock.scala
cluster/.../ClusterDaemon.scala
cluster/.../ClusterHeartbeat.scala
distributed-data/.../Replicator.scala
cluster-sharding/.../ShardRegion.scala
cluster-sharding/.../Shard.scala
cluster-sharding/.../ShardCoordinator.scala
cluster-sharding-typed/.../ClusterSharding.scala
```

Use those files to extract state machines, message flows, convergence rules,
ordering guarantees, and failure semantics. Then design the Rust implementation
with Rust ownership, typed APIs, modules, traits, enums, explicit errors, and
feature-gated crates. If the Rust design intentionally diverges from Pekko,
record the reason in `docs/decisions.md`.

## Validation Commands

Use these as the default full validation target:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets --all-features
```

During early scaffold work, the minimum validation is:

```bash
cargo fmt --all
cargo check --workspace --all-targets --all-features
```

Later milestones should add focused examples and multi-node tests, for example:

```bash
cargo test -p kairo-actor
cargo test -p kairo-remote
cargo test -p kairo-cluster
cargo test -p kairo-cluster-sharding
cargo run -p kairo-examples --example local_counter
cargo run -p kairo-examples --example cluster_sharding_demo
```

## Commit Checkpoint Discipline

A commit is a verified logical checkpoint, not a transcript of an agent turn,
one test case, or one validation command. Atomicity is semantic: a coherent
commit may touch several files or crates when they implement one behavior.

Commit only when all of the following are true:

- The change is independently explainable, reviewable, and revertible.
- The relevant focused tests pass and the tree does not rely on an immediate
  follow-up commit to become correct.
- Implementation, regression tests, and necessary user/status documentation
  for the same behavior are included together.
- The commit has one primary reason to change, even when that reason crosses
  multiple crates.

Do not split work solely to make commits smaller. By default, squash:

- consecutive test cases for the same state machine, invariant, or test module;
- an implementation and the tests that prove that implementation;
- follow-up compile, lint, formatting, or wording fixes created while completing
  the same task;
- mirrored cluster, distributed-data, sharding, or cluster-tools changes that
  enforce one cross-crate lifecycle invariant;
- validation-only or progress-log edits associated with the same checkpoint.

Keep commits separate when they provide a real review or rollback boundary,
such as:

- an accepted ADR or wire/public contract decision;
- a behavior-neutral prerequisite refactor that stands on its own;
- an independent regression fix with its own test;
- unrelated behavior that should be reviewable and revertible separately.

Commit-count guidance is a review heuristic, not a quota: an ordinary focused
task should normally result in 1-5 final commits, and a full execution phase in
roughly 5-15. If substantially more checkpoints are necessary, explain their
independent review or rollback value in the status report.

Temporary `fixup!` commits are acceptable only on an unpushed task branch.
Squash them before push, merge, or handoff. Never rewrite published history
unless the user explicitly authorizes it.

`docs/progress.md` is a current status document, not an append-only command
transcript. Update it only when milestone/phase status, an exit gate, a known
gap, or the concise latest validation summary changes. Put detailed validation
commands and results in the status report, commit body, PR, or CI logs.
`docs/decisions.md` records durable design decisions, not implementation diary
entries.

## Commit Message Convention

Use Conventional Commits for all commits:

```text
<type>(optional-scope): <description>
```

Allowed types:

```text
feat      user-facing feature or milestone capability
fix       bug fix or behavioral correction
docs      documentation-only change
test      tests or test fixtures
refactor  code change without intended behavior change
perf      performance improvement
build     build system, dependency, or workspace change
ci        CI configuration change
chore     maintenance that does not fit another type
```

Rules:

- Keep the description imperative, lowercase unless it names an identifier,
  and under 72 characters when practical.
- Use a scope when it clarifies ownership, such as `actor`, `remote`,
  `cluster`, `serialization`, `ddata`, `sharding`, `testkit`, or `docs`.
- Mark breaking changes with `!` after the type or scope, and explain the
  impact in the body.
- Include a short body when the reason, migration path, or validation is not
  obvious from the subject.
- Do not mix unrelated work in one commit.

Examples:

```text
docs: add goal mode roadmap
feat(actor): run typed mailbox receive loop
fix(cluster): preserve vector clock dominance on merge
test(sharding): cover buffered handoff delivery
refactor(remote): split association state machine
feat(serialization)!: require manifest versions for remote messages
```

## Task Template

When writing or following a task, keep it concrete:

```text
Task: Implement X.
Context: X belongs to milestone M?, and the relevant files are ...
Expected behavior: ...
Tests: ...
Do not change: ...
Validation: cargo test -p ...
Commit boundary: describe which changes form one verified checkpoint and which
separate ADR/refactor checkpoints, if any, are justified.
```

Example:

```text
Task: Implement the minimal local actor mailbox receive loop.
Context: This belongs to M1. `kairo-actor` already defines `Actor`,
`ActorRef`, `ActorSystem`, and `Props` skeletons.
Expected behavior:
  - `ActorSystem::spawn` creates a typed actor ref under `/user`.
  - `ActorRef::tell` enqueues one message without blocking.
  - the actor processes one message at a time using synchronous `receive`.
  - sending after stop routes the message to dead letters.
Tests:
  - kairo_actor::tests::spawn_and_tell
  - kairo_actor::tests::stopped_actor_goes_to_dead_letters
Do not change:
  - Do not add `AsyncActor`.
  - Do not introduce remoting into `kairo-actor`.
Validation:
  cargo test -p kairo-actor
Commit boundary:
  - Commit the mailbox loop, its focused tests, and the milestone status update
    together after validation.
  - Keep any prerequisite public Actor API decision in a separate ADR commit.
```
