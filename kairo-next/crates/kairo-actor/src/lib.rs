//! Typed local actor API and runtime primitives.

mod actor;
mod dead_letters;
mod error;
mod mailbox;
mod path;
mod refs;
mod system;

pub use actor::{Actor, Context, Props};
pub use dead_letters::{DeadLetter, DeadLetters};
pub use error::{ActorError, ActorResult, SendError};
pub use path::{ActorPath, Address};
pub use refs::{ActorRef, AnyActorRef, IgnoreRef, Recipient};
pub use system::{ActorSystem, ActorSystemBuilder};

pub mod prelude {
    pub use crate::{
        Actor, ActorError, ActorPath, ActorRef, ActorResult, ActorSystem, Context, DeadLetter,
        DeadLetters, IgnoreRef, Props, Recipient,
    };
}

#[cfg(test)]
mod tests;
