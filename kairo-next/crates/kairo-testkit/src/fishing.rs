use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FishingOutcome {
    Complete,
    Fail(String),
    Continue,
    ContinueAndIgnore,
}

pub(crate) fn remaining_until(deadline: Instant) -> Option<Duration> {
    deadline.checked_duration_since(Instant::now())
}
