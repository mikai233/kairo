//! Typed local actor API and runtime primitives.

mod actor;
mod adapters;
mod asks;
mod coordinated_shutdown;
mod dead_letters;
mod death_watch;
mod dispatcher;
mod error;
mod event_stream;
mod mailbox;
mod path;
mod receptionist;
mod refs;
mod registry;
mod scheduler;
mod signal;
mod supervision;
mod system;
mod tasks;
mod timers;

pub use actor::{Actor, Context, Props};
pub use asks::{AskError, AskResult};
pub use coordinated_shutdown::{
    CoordinatedShutdown, PHASE_ACTOR_SYSTEM_TERMINATE, PHASE_BEFORE_ACTOR_SYSTEM_TERMINATE,
    PHASE_BEFORE_CLUSTER_SHUTDOWN, PHASE_BEFORE_SERVICE_UNBIND, PHASE_CLUSTER_EXITING,
    PHASE_CLUSTER_EXITING_DONE, PHASE_CLUSTER_LEAVE, PHASE_CLUSTER_SHARDING_SHUTDOWN_REGION,
    PHASE_CLUSTER_SHUTDOWN, PHASE_SERVICE_REQUESTS_DONE, PHASE_SERVICE_STOP, PHASE_SERVICE_UNBIND,
};
pub use dead_letters::{DeadLetter, DeadLetters};
pub use dispatcher::DispatcherSettings;
pub use error::{ActorError, ActorResult, SendError};
pub use event_stream::EventStream;
pub use path::{ActorPath, Address};
pub use receptionist::{Listing, Receptionist, ServiceKey};
pub use refs::{ActorRef, AnyActorRef, IgnoreRef, Recipient};
pub use scheduler::{Cancellable, ManualScheduler};
pub use signal::Signal;
pub use supervision::SupervisorStrategy;
pub use system::{ActorSystem, ActorSystemBuilder};
pub use tasks::TaskHandle;
pub use timers::TimerKey;

pub mod prelude {
    pub use crate::{
        Actor, ActorError, ActorPath, ActorRef, ActorResult, ActorSystem, AskError, AskResult,
        Cancellable, Context, CoordinatedShutdown, DeadLetter, DeadLetters, DispatcherSettings,
        EventStream, IgnoreRef, Listing, ManualScheduler, PHASE_ACTOR_SYSTEM_TERMINATE,
        PHASE_BEFORE_ACTOR_SYSTEM_TERMINATE, PHASE_BEFORE_CLUSTER_SHUTDOWN,
        PHASE_BEFORE_SERVICE_UNBIND, PHASE_CLUSTER_EXITING, PHASE_CLUSTER_EXITING_DONE,
        PHASE_CLUSTER_LEAVE, PHASE_CLUSTER_SHARDING_SHUTDOWN_REGION, PHASE_CLUSTER_SHUTDOWN,
        PHASE_SERVICE_REQUESTS_DONE, PHASE_SERVICE_STOP, PHASE_SERVICE_UNBIND, Props, Receptionist,
        Recipient, ServiceKey, Signal, SupervisorStrategy, TaskHandle, TimerKey,
    };
}

#[cfg(test)]
mod tests;
