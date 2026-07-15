use std::fmt::{self, Display, Formatter};
use std::time::Duration;

/// Conventional result type returned by actor lifecycle and receive methods.
pub type ActorResult = Result<(), ActorError>;

#[derive(Debug, Clone, thiserror::Error)]
/// Failures produced by the local actor runtime and its public patterns.
pub enum ActorError {
    /// Actor-defined or integration-specific failure text.
    #[error("{0}")]
    Message(String),
    /// A requested actor name is not a valid path element.
    #[error("actor name `{0}` is invalid")]
    InvalidName(String),
    /// A live or terminating child already reserves the requested name.
    #[error("actor `{0}` already exists")]
    DuplicateName(String),
    /// The actor system has started termination and rejects new work.
    #[error("actor system is terminating")]
    SystemTerminating,
    /// Actor-system termination did not complete within its deadline.
    #[error("actor system termination timed out")]
    TerminationTimeout,
    /// A helper task could not be submitted or started.
    #[error("failed to spawn actor task: {0}")]
    TaskSpawn(String),
    /// The target rejected an ask request during ask setup.
    #[error("ask target rejected request: {0}")]
    AskSend(String),
    /// A coordinated-shutdown task referenced an unknown phase.
    #[error("unknown coordinated shutdown phase `{0}`")]
    UnknownShutdownPhase(String),
    /// A coordinated-shutdown task name was empty.
    #[error("coordinated shutdown task name must not be empty")]
    InvalidShutdownTaskName,
    /// A coordinated-shutdown task returned an error.
    #[error("coordinated shutdown task failed: {0}")]
    ShutdownTaskFailed(String),
    /// One coordinated-shutdown phase exceeded its deadline.
    #[error("coordinated shutdown phase `{phase}` timed out after {timeout:?}")]
    ShutdownPhaseTimeout {
        /// Phase that exceeded its deadline.
        phase: String,
        /// Configured phase deadline.
        timeout: Duration,
    },
    /// Dispatcher throughput was configured as zero.
    #[error("dispatcher throughput must be greater than zero")]
    InvalidThroughput,
    /// Dispatcher worker count was configured as zero.
    #[error("dispatcher worker count must be greater than zero")]
    InvalidDispatcherWorkers,
    /// Task-executor worker count was configured as zero.
    #[error("task executor worker count must be greater than zero")]
    InvalidTaskExecutorWorkers,
    /// Task-executor queue capacity was configured as zero.
    #[error("task executor queue capacity must be greater than zero")]
    InvalidTaskExecutorCapacity,
    /// Mailbox capacity was configured as zero.
    #[error("mailbox capacity must be greater than zero")]
    InvalidMailboxCapacity,
    /// A requested actor-system extension has not been registered.
    #[error("extension `{0}` is not registered")]
    ExtensionNotRegistered(&'static str),
    /// An actor attempted to stop a reference outside its owned child boundary.
    #[error("actor `{actor}` is not self or a direct child of `{owner}`")]
    InvalidStopTarget {
        /// Rejected stop target path.
        actor: String,
        /// Actor that attempted the stop.
        owner: String,
    },
    /// The referenced actor is already in its stopping lifecycle.
    #[error("actor `{actor}` is stopping")]
    ActorStopping {
        /// Stopping actor path.
        actor: String,
    },
    /// An unhandled termination signal triggered death pact.
    #[error("death pact triggered by terminated actor `{actor}`")]
    DeathPact {
        /// Terminated actor path.
        actor: String,
    },
    /// An actor attempted to watch itself.
    #[error("actor `{actor}` cannot watch itself")]
    InvalidWatchTarget {
        /// Self-watch target path.
        actor: String,
    },
    /// The watcher already registered a different notification for the subject.
    #[error("actor `{watcher}` is already watching `{actor}` with another notification")]
    AlreadyWatching {
        /// Watched subject path.
        actor: String,
        /// Watcher path.
        watcher: String,
    },
    /// The actor was spawned without a stash.
    #[error("stash is not enabled for this actor")]
    StashDisabled,
    /// The actor's stash has reached its configured capacity.
    #[error("stash is full at capacity {capacity}")]
    StashFull {
        /// Maximum number of stashed messages.
        capacity: usize,
    },
}

/// Failed non-blocking message send that retains ownership of the message.
pub struct SendError<M> {
    pub(crate) message: M,
    pub(crate) reason: String,
}

impl<M> SendError<M> {
    /// Creates an error from the rejected message and failure reason.
    pub fn new(message: M, reason: impl Into<String>) -> Self {
        Self {
            message,
            reason: reason.into(),
        }
    }

    /// Returns ownership of the rejected message.
    pub fn into_message(self) -> M {
        self.message
    }

    /// Returns the delivery failure reason.
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
