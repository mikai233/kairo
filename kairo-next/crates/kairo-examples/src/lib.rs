//! Runnable example targets for the Kairo rewrite.
//!
//! The example binaries under `examples/` are small entry points over reusable
//! modules in this crate. That keeps actor protocols, setup helpers, one-shot
//! replies, sharding wiring, distributed-data TCP setup, and cluster-tools TCP
//! setup structured instead of concentrating orchestration in one binary.
//!
//! Current runnable examples:
//!
//! - `local_counter`: typed local actor spawn, tell, explicit reply channel,
//!   and stop.
//! - `configured_counter`: layered TOML-first settings loaded through the
//!   `kairo` facade, mapped into an actor-system builder, and read back
//!   through format-neutral sharding and diagnostics helpers.
//! - `ask_pipe_to_self`: synchronous actor turns with ask and external work
//!   returning through mailbox messages.
//! - `cluster_sharding_local`: local coordinator, shard region, stable shard
//!   hash, `EntityRef`, and entity-backed shard delivery.
//! - `remote_ping_pong`: two TCP remoting actor systems exchanging a typed
//!   ping and pong through registered stable message codecs.
//! - `ddata_counter`: local distributed-data `GCounter` updates through a
//!   `ReplicatorActor` with change subscription delivery.
//! - `cluster_membership`: cluster event subscription, snapshot delivery,
//!   member-up publication, member removal, and current-state request.
//! - `cluster_tools_local`: local pubsub subscribe/publish/topics plus local
//!   singleton manager startup and singleton access.
//! - `cluster_tcp_peer_bootstrap`: two local cluster TCP peer runtimes using
//!   cluster-derived route plans.
//! - `ddata_tcp_peer_bootstrap`: two local distributed-data TCP peer runtimes
//!   using cluster-derived route plans.
//! - `cluster_tools_tcp_peer_bootstrap`: two local cluster-tools TCP peer
//!   runtimes for pubsub/singleton system traffic.
//!
//! Run examples from the workspace root with
//! `cargo run -p kairo-examples --example <name>`.
//!
//! ```
//! use std::sync::mpsc;
//! use std::time::Duration;
//!
//! use kairo::prelude::*;
//! use kairo_examples::counter::{CounterCmd, spawn_counter};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let system = ActorSystem::builder("example-doc").build()?;
//! let counter = spawn_counter(&system, "counter", 40)?;
//! let (reply_to, replies) = mpsc::channel();
//!
//! counter.tell(CounterCmd::Increment)?;
//! counter.tell(CounterCmd::Get { reply_to })?;
//! assert_eq!(replies.recv_timeout(Duration::from_secs(1))?, 41);
//!
//! counter.tell(CounterCmd::Stop)?;
//! assert!(counter.wait_for_stop(Duration::from_secs(1)));
//! system.terminate(Duration::from_secs(1))?;
//! # Ok(())
//! # }
//! ```

pub mod cluster_membership;
pub mod cluster_tcp;
pub mod cluster_tools_local;
pub mod cluster_tools_tcp;
pub mod configured_counter;
pub mod counter;
pub mod ddata_counter;
pub mod ddata_tcp;
pub mod patterns;
pub mod remote_ping_pong;
pub mod reply;
pub mod sharding_local;
