//! Deterministic test utilities for local actor workflows.
//!
//! `kairo-testkit` keeps tests inside Kairo's typed actor model. A
//! [`TestProbe`] is itself backed by a local actor, so code under test sends to
//! an `ActorRef<M>` and tests assert against a typed message queue. The
//! [`ActorSystemTestKit`] owns a local actor system for one test and can create
//! probe actors under that system.
//!
//! The helpers mirror Pekko-style testkit capabilities while staying
//! Rust-first:
//!
//! - [`TestProbe::expect_msg`], [`TestProbe::expect_msg_matching`], and
//!   [`TestProbe::expect_no_msg`] assert direct probe traffic.
//! - [`TestProbe::receive_messages`] collects a fixed batch under one shared
//!   deadline.
//! - [`TestProbe::watch_with`] and [`TestProbe::unwatch`] register and remove
//!   typed lifecycle notifications through the same death-watch path as actors.
//! - [`TestProbe::fish_for_message`] classifies incoming messages with
//!   [`FishingOutcome`].
//! - [`await_assert`] retries result-returning assertions without relying on
//!   panic recovery.
//! - [`ManualTime`] drives systems built with the manual scheduler backend.
//! - [`MultiNodeTestKit`] owns named actor systems for local multi-node
//!   integration tests without making cluster membership part of the testkit.
//! - [`MultiNodeTestKit::enter_barrier`] coordinates named local multi-node
//!   phases with explicit waiting/passed status and ordering errors.
//! - [`MultiNodeTestKit::await_barrier`] blocks a node at a named local
//!   multi-node phase until all participants arrive or a timeout expires.
//! - [`MultiNodeTestKit::await_barriers`] runs ordered local multi-node phases
//!   under one shared timeout budget.
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

mod assertions;
mod fishing;
mod manual_time;
mod multi_node;
mod probe;
mod system;

pub use assertions::{AwaitAssertError, await_assert};
pub use fishing::FishingOutcome;
pub use manual_time::{ManualTime, ManualTimeHandle, NoMessageProbe};
pub use multi_node::{
    MultiNode, MultiNodeBarrierStatus, MultiNodeError, MultiNodeResult, MultiNodeTestKit,
};
pub use probe::{ProbeError, TestProbe};
pub use system::ActorSystemTestKit;

#[cfg(test)]
mod tests;
