//! Typed local actor API and runtime primitives.

mod actor;
mod dead_letters;
mod death_watch;
mod dispatcher;
mod error;
mod event_stream;
mod mailbox;
mod path;
mod refs;
mod registry;
mod scheduler;
mod signal;
mod system;
mod timers;

pub use actor::{Actor, Context, Props};
pub use dead_letters::{DeadLetter, DeadLetters};
pub use dispatcher::DispatcherSettings;
pub use error::{ActorError, ActorResult, SendError};
pub use event_stream::EventStream;
pub use path::{ActorPath, Address};
pub use refs::{ActorRef, AnyActorRef, IgnoreRef, Recipient};
pub use scheduler::Cancellable;
pub use signal::Signal;
pub use system::{ActorSystem, ActorSystemBuilder};
pub use timers::TimerKey;

pub mod prelude {
    pub use crate::{
        Actor, ActorError, ActorPath, ActorRef, ActorResult, ActorSystem, Cancellable, Context,
        DeadLetter, DeadLetters, DispatcherSettings, EventStream, IgnoreRef, Props, Recipient,
        Signal, TimerKey,
    };
}

#[cfg(test)]
mod tests;
