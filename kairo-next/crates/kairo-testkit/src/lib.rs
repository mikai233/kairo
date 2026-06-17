//! Deterministic test utilities for local actor workflows.
//!
//! `kairo-testkit` keeps tests inside Kairo's typed actor model. A
//! [`TestProbe`] is itself backed by a local actor, so code under test sends to
//! an `ActorRef<M>` and tests assert against a typed message queue. The
//! [`ActorSystemTestKit`] owns a local actor system for one test and can create
//! probe actors under that system. [`ActorHarness`] is a small spawn-backed
//! wrapper for tests centered on one actor under the real runtime.
//!
//! The helpers mirror Pekko-style testkit capabilities while staying
//! Rust-first:
//!
//! - [`TestProbe::expect_msg`], [`TestProbe::expect_msg_matching`], and
//!   [`TestProbe::expect_no_msg`] assert direct probe traffic.
//! - [`ActorHarness`] spawns one actor with owned probes, subject death-watch
//!   helpers, stop assertions, shared-deadline assertions, and optional manual
//!   time while preserving normal actor-system semantics.
//! - [`ActorSystemTestKit::create_event_probe`] subscribes a typed probe to
//!   local event-stream publications, and
//!   [`ActorSystemTestKit::create_dead_letter_probe`] specializes it for dead
//!   letters.
//! - [`TestProbe::receive_messages`] collects a fixed batch under one shared
//!   deadline.
//! - [`TestProbe::watch`] on `TestProbe<Signal>` observes plain
//!   `Signal::Terminated`, while [`TestProbe::watch_with`] and
//!   [`TestProbe::unwatch`] register and remove typed lifecycle notifications
//!   through the same death-watch path as actors.
//!   [`TestProbe::expect_terminated_within`] composes termination assertions
//!   with a shared [`Within`] deadline.
//! - [`TestProbe::fish_for_message`] classifies incoming messages with
//!   [`FishingOutcome`].
//! - [`TestProbe::await_assert`] retries probe-centered assertions with a
//!   structured last-error report.
//! - [`within`] runs a block against one shared deadline and exposes
//!   [`Within::remaining`] for nested probe assertions; [`Within::await_assert`]
//!   and [`TestProbe::await_assert_within`] retry polling assertions against
//!   that same deadline.
//! - [`TestProbe::expect_msg_within`],
//!   [`TestProbe::expect_msg_matching_within`],
//!   [`TestProbe::expect_no_msg_for_within`],
//!   [`TestProbe::receive_messages_within`], and
//!   [`TestProbe::fish_for_message_within`] apply probe receive assertions to
//!   the same shared [`Within`] deadline.
//! - [`await_assert`] retries result-returning assertions without relying on
//!   panic recovery.
//! - [`ManualTime`] drives systems built with the manual scheduler backend,
//!   including bounded advancement through scheduled deadlines.
//! - [`MultiNodeTestKit`] owns named actor systems for local multi-node
//!   integration tests without making cluster membership part of the testkit.
//! - [`MultiNodeTestKit::advance_all`],
//!   [`MultiNodeTestKit::advance_all_to_next`], and
//!   [`MultiNodeTestKit::advance_all_until_idle`] drive manual-time node clocks
//!   together for deterministic multi-node scheduling.
//! - [`MultiNodeTestKit::spawn_on`] and
//!   [`MultiNodeTestKit::spawn_system_on`] spawn typed user or framework-owned
//!   actors on a specific named node.
//! - [`MultiNodeTestKit::create_event_probe_on`] and
//!   [`MultiNodeTestKit::create_dead_letter_probe_on`] subscribe probes on a
//!   specific named node for node-local lifecycle and diagnostics assertions.
//! - [`MultiNodeTestKit::watch_terminated_on`],
//!   [`MultiNodeTestKit::expect_terminated_on`],
//!   [`MultiNodeTestKit::expect_terminated_on_within`], and
//!   [`MultiNodeTestKit::within`] compose node-local lifecycle observations and
//!   cross-node probe assertions under deterministic multi-node test budgets.
//! - [`MultiNodeTestKit::enter_barrier`] coordinates named local multi-node
//!   phases with explicit waiting/passed status and ordering errors.
//! - [`MultiNodeTestKit::await_barrier`] blocks a node at a named local
//!   multi-node phase until all participants arrive or a timeout expires.
//! - [`MultiNodeTestKit::await_barriers`] runs ordered local multi-node phases
//!   under one shared timeout budget.
//! - [`MultiNodeTestKit::await_barrier_within`] and
//!   [`MultiNodeTestKit::await_barriers_within`] apply barrier coordination to
//!   an existing shared [`Within`] deadline.
//!
//! ## Probe-backed actor test
//!
//! ```no_run
//! use std::time::Duration;
//!
//! use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, Context, Props};
//! use kairo_testkit::ActorSystemTestKit;
//!
//! enum EchoMsg {
//!     Ping(ActorRef<&'static str>),
//! }
//!
//! struct Echo;
//!
//! impl Actor for Echo {
//!     type Msg = EchoMsg;
//!
//!     fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
//!         match msg {
//!             EchoMsg::Ping(reply_to) => {
//!                 reply_to
//!                     .tell("pong")
//!                     .map_err(|error| ActorError::Message(error.to_string()))?;
//!             }
//!         }
//!         Ok(())
//!     }
//! }
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let kit = ActorSystemTestKit::new("testkit-docs")?;
//! let probe = kit.create_probe::<&'static str>("probe")?;
//! let echo = kit.system().spawn("echo", Props::new(|| Echo))?;
//!
//! echo.tell(EchoMsg::Ping(probe.actor_ref()))?;
//! assert_eq!(probe.expect_msg(Duration::from_secs(1))?, "pong");
//!
//! kit.shutdown(Duration::from_secs(1))?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Shared deadline probe assertions
//!
//! ```
//! use std::time::Duration;
//!
//! use kairo_testkit::ActorSystemTestKit;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let kit = ActorSystemTestKit::new("within-docs")?;
//! let probe = kit.create_probe::<&'static str>("probe")?;
//!
//! probe.actor_ref().tell("first")?;
//! probe.actor_ref().tell("second")?;
//!
//! let messages = probe.within(Duration::from_millis(50), |probe, scope| {
//!     let first = probe.expect_msg_eq_within("first", scope)?;
//!     let second = probe.expect_msg_matching_within(scope, |msg| msg.starts_with("sec"))?;
//!     probe.expect_no_msg_for_within(Duration::from_millis(1), scope)?;
//!     Ok::<_, kairo_testkit::ProbeError>(vec![first, second])
//! })?;
//!
//! assert_eq!(messages, vec!["first", "second"]);
//! kit.shutdown(Duration::from_secs(1))?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Manual time
//!
//! ```no_run
//! use std::time::Duration;
//!
//! use kairo_testkit::ActorSystemTestKit;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let (kit, time) = ActorSystemTestKit::with_manual_time("manual-time-docs")?;
//! let probe = kit.create_probe::<&'static str>("probe")?;
//!
//! time.schedule_once(Duration::from_secs(1), probe.actor_ref(), "tick");
//! time.expect_no_msg_for(Duration::from_millis(999), &[&probe])?;
//!
//! time.advance(Duration::from_millis(1));
//! assert_eq!(probe.expect_msg(Duration::from_secs(1))?, "tick");
//!
//! kit.shutdown(Duration::from_secs(1))?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Local multi-node barriers
//!
//! ```
//! use std::sync::Arc;
//! use std::thread;
//! use std::time::Duration;
//!
//! use kairo_testkit::MultiNodeTestKit;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let kit = Arc::new(MultiNodeTestKit::new(["node-a", "node-b"])?);
//! let waiting_kit = Arc::clone(&kit);
//! let waiter = thread::spawn(move || {
//!     waiting_kit.await_barriers(
//!         ["started", "ready"],
//!         "node-a",
//!         Duration::from_secs(1),
//!     )
//! });
//!
//! let main_statuses =
//!     kit.await_barriers(["started", "ready"], "node-b", Duration::from_secs(1))?;
//! assert!(main_statuses.iter().all(|status| status.passed()));
//!
//! let waiter_statuses = waiter
//!     .join()
//!     .expect("waiting node should not panic")?;
//! assert!(waiter_statuses.iter().all(|status| status.passed()));
//!
//! let kit = Arc::try_unwrap(kit).expect("all shared kit refs should be released");
//! kit.shutdown(Duration::from_secs(1))?;
//! # Ok(())
//! # }
//! ```

mod actor_harness;
mod assertions;
mod fishing;
mod manual_time;
mod multi_node;
mod probe;
mod system;
mod within;

pub use actor_harness::{ActorHarness, ActorHarnessError};
pub use assertions::{AwaitAssertError, await_assert};
pub use fishing::FishingOutcome;
pub use manual_time::{ManualTime, ManualTimeHandle, NoMessageProbe};
pub use multi_node::{
    MultiNode, MultiNodeBarrierStatus, MultiNodeError, MultiNodeResult, MultiNodeTestKit,
};
pub use probe::{ProbeError, TestProbe};
pub use system::ActorSystemTestKit;
pub use within::{Within, WithinError, within};

#[cfg(test)]
mod tests;
