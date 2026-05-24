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
