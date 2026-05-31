use std::fmt::{self, Display, Formatter};
use std::time::Duration;

pub type ActorResult = Result<(), ActorError>;

#[derive(Debug, Clone, thiserror::Error)]
pub enum ActorError {
    #[error("{0}")]
    Message(String),
    #[error("actor name `{0}` is invalid")]
    InvalidName(String),
    #[error("actor `{0}` already exists")]
    DuplicateName(String),
    #[error("actor system is terminating")]
    SystemTerminating,
    #[error("actor system termination timed out")]
    TerminationTimeout,
    #[error("failed to spawn actor task: {0}")]
    TaskSpawn(String),
    #[error("ask target rejected request: {0}")]
    AskSend(String),
    #[error("unknown coordinated shutdown phase `{0}`")]
    UnknownShutdownPhase(String),
    #[error("coordinated shutdown task name must not be empty")]
    InvalidShutdownTaskName,
    #[error("coordinated shutdown task failed: {0}")]
    ShutdownTaskFailed(String),
    #[error("coordinated shutdown phase `{phase}` timed out after {timeout:?}")]
    ShutdownPhaseTimeout { phase: String, timeout: Duration },
    #[error("dispatcher throughput must be greater than zero")]
    InvalidThroughput,
    #[error("actor `{actor}` is not self or a direct child of `{owner}`")]
    InvalidStopTarget { actor: String, owner: String },
    #[error("actor `{actor}` cannot watch itself")]
    InvalidWatchTarget { actor: String },
    #[error("actor `{watcher}` is already watching `{actor}` with another notification")]
    AlreadyWatching { actor: String, watcher: String },
    #[error("stash is not enabled for this actor")]
    StashDisabled,
    #[error("stash is full at capacity {capacity}")]
    StashFull { capacity: usize },
}

pub struct SendError<M> {
    pub(crate) message: M,
    pub(crate) reason: String,
}

impl<M> SendError<M> {
    pub fn new(message: M, reason: impl Into<String>) -> Self {
        Self {
            message,
            reason: reason.into(),
        }
    }

    pub fn into_message(self) -> M {
        self.message
    }

    pub fn reason(&self) -> &str {
        &self.reason
    }
}

impl<M> Display for SendError<M> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.reason)
    }
}

impl<M> fmt::Debug for SendError<M> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("SendError")
            .field("reason", &self.reason)
            .finish_non_exhaustive()
    }
}

impl<M> std::error::Error for SendError<M> {}
