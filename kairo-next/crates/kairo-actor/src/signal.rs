use crate::AnyActorRef;

#[derive(Debug, Clone, PartialEq, Eq)]
/// Lifecycle notification delivered through [`crate::Actor::signal`].
pub enum Signal {
    /// The actor is about to restart after a failed turn.
    PreRestart,
    /// The actor has stopped permanently.
    PostStop,
    /// A watched actor terminated normally or was stopped.
    Terminated(AnyActorRef),
    /// A watched child terminated because its actor turn failed.
    ChildFailed {
        /// Reference to the failed child incarnation.
        actor: AnyActorRef,
        /// Failure description reported by the child runtime.
        reason: String,
    },
}
