//! Typed local actor API and runtime primitives.
//!
//! `kairo-actor` is the local runtime foundation. Users define a protocol as
//! normal Rust types, implement [`Actor`] for stateful actor structs, and send
//! messages through [`ActorRef<M>`]. Local messages only need `Send + 'static`;
//! they do not need serialization metadata.
//!
//! Actor state changes happen during short synchronous [`Actor::receive`]
//! turns. This preserves the core actor invariant that a single actor processes
//! one mailbox message at a time. Work that would block, wait, or outlive the
//! current turn should run outside the actor and return through the mailbox
//! using [`Context::pipe_to_self`], [`Context::spawn_task`], [`Context::ask`],
//! timers, or message adapters. Kairo intentionally does not provide an
//! `AsyncActor` in the initial design because holding `&mut self` across an
//! await point would make actor state observable outside a mailbox turn and
//! complicate supervision and cancellation.
//!
//! This follows the observable Pekko/Akka model while using Rust ownership and
//! explicit error values instead of Scala behavior returns or implicit sender
//! APIs.
//!
//! ```no_run
//! use std::time::Duration;
//!
//! use kairo_actor::{Actor, ActorError, ActorRef, ActorResult, ActorSystem, Context, Props};
//!
//! enum CounterMsg {
//!     Add(i64),
//!     AddFromTask(i64),
//!     Get(ActorRef<i64>),
//!     Stop,
//! }
//!
//! struct Counter {
//!     value: i64,
//! }
//!
//! impl Actor for Counter {
//!     type Msg = CounterMsg;
//!
//!     fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
//!         match msg {
//!             CounterMsg::Add(delta) => {
//!                 self.value += delta;
//!             }
//!             CounterMsg::AddFromTask(delta) => {
//!                 ctx.pipe_to_self(
//!                     move || Ok::<i64, ()>(delta),
//!                     |result| CounterMsg::Add(result.unwrap_or(0)),
//!                 )?;
//!             }
//!             CounterMsg::Get(reply_to) => {
//!                 reply_to
//!                     .tell(self.value)
//!                     .map_err(|error| ActorError::Message(error.to_string()))?;
//!             }
//!             CounterMsg::Stop => ctx.stop(ctx.myself())?,
//!         }
//!         Ok(())
//!     }
//! }
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let system = ActorSystem::builder("actor-docs").build()?;
//! let counter = system.spawn("counter", Props::new(|| Counter { value: 0 }))?;
//! let replies = system.spawn("replies", Props::new(|| ReplySink))?;
//!
//! counter.tell(CounterMsg::Add(1))?;
//! counter.tell(CounterMsg::AddFromTask(41))?;
//! counter.tell(CounterMsg::Get(replies))?;
//! counter.tell(CounterMsg::Stop)?;
//! system.terminate(Duration::from_secs(1))?;
//! # Ok(())
//! # }
//!
//! struct ReplySink;
//!
//! impl Actor for ReplySink {
//!     type Msg = i64;
//!
//!     fn receive(&mut self, _ctx: &mut Context<Self::Msg>, _msg: Self::Msg) -> ActorResult {
//!         Ok(())
//!     }
//! }
//! ```

mod actor;
mod adapters;
mod asks;
mod backoff;
mod coordinated_shutdown;
mod dead_letters;
mod death_watch;
mod dispatcher;
mod error;
mod event_stream;
mod extensions;
mod mailbox;
mod path;
mod provider;
mod receive_timeout;
mod receptionist;
mod refs;
mod registry;
mod runtime;
mod scheduler;
mod signal;
mod stash;
mod supervision;
mod system;
mod tasks;
mod timers;

pub use actor::{Actor, Context, Props};
pub use asks::{AskError, AskResult};
pub use backoff::{
    BackoffReset, BackoffSettingsError, BackoffSupervisor, BackoffSupervisorMsg,
    BackoffSupervisorSettings, CurrentChild, RestartCount,
};
pub use coordinated_shutdown::{
    CoordinatedShutdown, PHASE_ACTOR_SYSTEM_TERMINATE, PHASE_BEFORE_ACTOR_SYSTEM_TERMINATE,
    PHASE_BEFORE_CLUSTER_SHUTDOWN, PHASE_BEFORE_SERVICE_UNBIND, PHASE_CLUSTER_EXITING,
    PHASE_CLUSTER_EXITING_DONE, PHASE_CLUSTER_LEAVE, PHASE_CLUSTER_SHARDING_SHUTDOWN_REGION,
    PHASE_CLUSTER_SHUTDOWN, PHASE_SERVICE_REQUESTS_DONE, PHASE_SERVICE_STOP, PHASE_SERVICE_UNBIND,
    ShutdownTaskHandle,
};
pub use dead_letters::{DeadLetter, DeadLetters};
pub use dispatcher::DispatcherSettings;
pub use error::{ActorError, ActorResult, SendError};
pub use event_stream::EventStream;
pub use extensions::{Extension, ExtensionRegistry};
pub use mailbox::MailboxSettings;
pub use path::{ActorPath, Address};
pub use provider::{ActorRefProvider, ActorRefResolveResult, LocalActorRefProvider};
pub use receptionist::{Deregistered, Listing, Receptionist, Registered, ServiceKey};
pub use refs::{ActorRef, AnyActorRef, IgnoreRef, Recipient};
pub use scheduler::{Cancellable, ManualScheduler};
pub use signal::Signal;
pub use supervision::SupervisorStrategy;
pub use system::{ActorSystem, ActorSystemBuilder};
pub use tasks::{TaskExecutorSettings, TaskHandle};
pub use timers::TimerKey;

pub mod prelude {
    pub use crate::{
        Actor, ActorError, ActorPath, ActorRef, ActorRefProvider, ActorRefResolveResult,
        ActorResult, ActorSystem, AskError, AskResult, BackoffReset, BackoffSettingsError,
        BackoffSupervisor, BackoffSupervisorMsg, BackoffSupervisorSettings, Cancellable, Context,
        CoordinatedShutdown, CurrentChild, DeadLetter, DeadLetters, Deregistered,
        DispatcherSettings, EventStream, Extension, ExtensionRegistry, IgnoreRef, Listing,
        LocalActorRefProvider, MailboxSettings, ManualScheduler, PHASE_ACTOR_SYSTEM_TERMINATE,
        PHASE_BEFORE_ACTOR_SYSTEM_TERMINATE, PHASE_BEFORE_CLUSTER_SHUTDOWN,
        PHASE_BEFORE_SERVICE_UNBIND, PHASE_CLUSTER_EXITING, PHASE_CLUSTER_EXITING_DONE,
        PHASE_CLUSTER_LEAVE, PHASE_CLUSTER_SHARDING_SHUTDOWN_REGION, PHASE_CLUSTER_SHUTDOWN,
        PHASE_SERVICE_REQUESTS_DONE, PHASE_SERVICE_STOP, PHASE_SERVICE_UNBIND, Props, Receptionist,
        Recipient, Registered, RestartCount, ServiceKey, ShutdownTaskHandle, Signal,
        SupervisorStrategy, TaskExecutorSettings, TaskHandle, TimerKey,
    };
}

#[cfg(test)]
mod tests;
