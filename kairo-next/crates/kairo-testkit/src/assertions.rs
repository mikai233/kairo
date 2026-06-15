use std::fmt::{self, Display, Formatter};
use std::thread;
use std::time::{Duration, Instant};

/// Re-runs a fallible assertion until it succeeds or a shared timeout expires.
///
/// The assertion returns `Ok(T)` when the expected condition is met and `Err(E)`
/// while the condition is not ready yet. `await_assert` sleeps for at most
/// `interval` between attempts, never past the `max` deadline, and reports the
/// final error if the deadline expires.
pub fn await_assert<T, E, F>(
    max: Duration,
    interval: Duration,
    mut assertion: F,
) -> Result<T, AwaitAssertError<E>>
where
    F: FnMut() -> Result<T, E>,
{
    let started = Instant::now();
    let deadline = started + max;
    let mut attempts = 0;

    loop {
        attempts += 1;
        match assertion() {
            Ok(value) => return Ok(value),
            Err(last_error) => {
                let now = Instant::now();
                if now >= deadline {
                    return Err(AwaitAssertError {
                        attempts,
                        elapsed: now.duration_since(started),
                        last_error,
                    });
                }

                let remaining = deadline.duration_since(now);
                let sleep_for = remaining.min(interval);
                if sleep_for.is_zero() {
                    thread::yield_now();
                } else {
                    thread::sleep(sleep_for);
                }
            }
        }
    }
}

/// Timeout report returned by [`await_assert`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AwaitAssertError<E> {
    attempts: usize,
    elapsed: Duration,
    last_error: E,
}

impl<E> AwaitAssertError<E> {
    /// Returns how many times the assertion closure was called.
    pub fn attempts(&self) -> usize {
        self.attempts
    }

    /// Returns how long the retry loop ran before timing out.
    pub fn elapsed(&self) -> Duration {
        self.elapsed
    }

    /// Returns the last assertion error observed before timeout.
    pub fn last_error(&self) -> &E {
        &self.last_error
    }

    /// Consumes the timeout report and returns the last assertion error.
    pub fn into_last_error(self) -> E {
        self.last_error
    }
}

impl<E: Display> Display for AwaitAssertError<E> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "assertion did not pass after {} attempts over {:?}: {}",
            self.attempts, self.elapsed, self.last_error
        )
    }
}

impl<E: Display + fmt::Debug> std::error::Error for AwaitAssertError<E> {}
