use std::fmt::{self, Display, Formatter};
use std::thread;
use std::time::{Duration, Instant};

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AwaitAssertError<E> {
    attempts: usize,
    elapsed: Duration,
    last_error: E,
}

impl<E> AwaitAssertError<E> {
    pub fn attempts(&self) -> usize {
        self.attempts
    }

    pub fn elapsed(&self) -> Duration {
        self.elapsed
    }

    pub fn last_error(&self) -> &E {
        &self.last_error
    }

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
