use crate::AnyActorRef;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Signal {
    PreRestart,
    PostStop,
    Terminated(AnyActorRef),
    ChildFailed { actor: AnyActorRef, reason: String },
}
