use std::fmt::{self, Display, Formatter};

pub type ActorResult = Result<(), ActorError>;

#[derive(Debug, thiserror::Error)]
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
    #[error("dispatcher throughput must be greater than zero")]
    InvalidThroughput,
    #[error("actor `{actor}` is not self or a direct child of `{owner}`")]
    InvalidStopTarget { actor: String, owner: String },
    #[error("actor `{watcher}` is already watching `{actor}` with another notification")]
    AlreadyWatching { actor: String, watcher: String },
}

pub struct SendError<M> {
    pub(crate) message: M,
    pub(crate) reason: String,
}

impl<M> SendError<M> {
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
