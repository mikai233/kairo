use std::time::{Duration, Instant};

/// Classification returned from `TestProbe::fish_for_message` predicates.
///
/// Fishing consumes probe messages until the predicate completes, fails, or the
/// shared timeout expires. Outcomes decide whether the current message is kept
/// in the collected result list and whether fishing should continue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FishingOutcome {
    /// Keep the current message and finish successfully.
    Complete,
    /// Stop immediately with the supplied failure reason.
    Fail(String),
    /// Keep the current message and continue fishing.
    Continue,
    /// Drop the current message from the collected result and continue fishing.
    ContinueAndIgnore,
}

pub(crate) fn remaining_until(deadline: Instant) -> Option<Duration> {
    deadline.checked_duration_since(Instant::now())
}
