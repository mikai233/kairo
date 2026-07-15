use std::fmt::{self, Display, Formatter};
use std::time::{Duration, Instant};

use crate::assertions::{AwaitAssertError, await_assert};

/// Shared-deadline scope for composing several test assertions.
///
/// `Within` tracks one timeout budget and exposes the remaining time so nested
/// probe receives, polling assertions, and custom checks do not accidentally
/// receive independent fresh deadlines.
#[derive(Debug, Clone)]
pub struct Within {
    timeout: Duration,
    deadline: Instant,
}

impl Within {
    /// Starts a new shared-deadline scope from the current instant.
    pub fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            deadline: Instant::now() + timeout,
        }
    }

    /// Returns the original timeout budget for the scope.
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Returns the remaining time before the shared deadline expires.
    pub fn remaining(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }

    /// Returns whether the shared deadline has elapsed.
    pub fn is_elapsed(&self) -> bool {
        self.remaining().is_zero()
    }

    /// Re-runs a fallible assertion under this scope's remaining deadline.
    ///
    /// This is the scoped counterpart to [`await_assert`]. The retry loop uses
    /// the current remaining time when the method is called, so earlier work in
    /// the same [`Within`] scope consumes part of the budget.
    pub fn await_assert<T, E, F>(
        &self,
        interval: Duration,
        assertion: F,
    ) -> Result<T, AwaitAssertError<E>>
    where
        F: FnMut() -> Result<T, E>,
    {
        await_assert(self.remaining(), interval, assertion)
    }

    fn elapsed(&self) -> Duration {
        self.timeout.saturating_sub(self.remaining())
    }
}

/// Runs a block under one shared timeout budget.
///
/// The block receives a [`Within`] scope so nested assertions can use
/// [`Within::remaining`]. If the block returns an assertion error, it is
/// reported as [`WithinError::Assertion`]. If the block returns successfully
/// after the deadline elapsed, the result is replaced by
/// [`WithinError::Timeout`].
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

/// Error returned from [`within`] and `TestProbe::within`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WithinError<E> {
    /// The block's assertion returned an error before the deadline check passed.
    Assertion(E),
    /// The block returned successfully after the shared deadline elapsed.
    Timeout {
        /// Configured maximum duration for the block.
        timeout: Duration,
        /// Actual duration observed when the block returned.
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
