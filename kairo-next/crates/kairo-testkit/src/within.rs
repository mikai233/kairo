use std::fmt::{self, Display, Formatter};
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct Within {
    timeout: Duration,
    deadline: Instant,
}

impl Within {
    pub fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            deadline: Instant::now() + timeout,
        }
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    pub fn remaining(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }

    pub fn is_elapsed(&self) -> bool {
        self.remaining().is_zero()
    }

    fn elapsed(&self) -> Duration {
        self.timeout.saturating_sub(self.remaining())
    }
}

pub fn within<T, E, F>(timeout: Duration, assertion: F) -> Result<T, WithinError<E>>
where
    F: FnOnce(&Within) -> Result<T, E>,
{
    let scope = Within::new(timeout);
    let result = assertion(&scope).map_err(WithinError::Assertion)?;
    if scope.is_elapsed() {
        Err(WithinError::Timeout {
            timeout,
            elapsed: scope.elapsed(),
        })
    } else {
        Ok(result)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WithinError<E> {
    Assertion(E),
    Timeout {
        timeout: Duration,
        elapsed: Duration,
    },
}

impl<E: Display> Display for WithinError<E> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Assertion(error) => write!(f, "within assertion failed: {error}"),
            Self::Timeout { timeout, elapsed } => {
                write!(
                    f,
                    "within block exceeded timeout {timeout:?} after {elapsed:?}"
                )
            }
        }
    }
}

impl<E> std::error::Error for WithinError<E> where E: std::error::Error + 'static {}
