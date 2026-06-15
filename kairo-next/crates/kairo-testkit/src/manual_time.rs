use std::time::Duration;

use kairo_actor::{ActorRef, Cancellable, ManualScheduler};

use crate::probe::{ProbeError, TestProbe};

/// Cancellation handle returned by manual-time scheduling helpers.
pub type ManualTimeHandle = Cancellable;

const NO_MESSAGE_SETTLE: Duration = Duration::from_millis(50);

/// Type-erased no-message assertion used by [`ManualTime::expect_no_msg_for`].
///
/// This trait lets one manual-time assertion check probes with different
/// message protocols after advancing the same scheduler.
pub trait NoMessageProbe {
    /// Asserts that the probe receives no message during `duration`.
    fn expect_no_msg(&self, duration: Duration) -> Result<(), ProbeError>;
}

impl<M> NoMessageProbe for TestProbe<M>
where
    M: Send + 'static,
{
    fn expect_no_msg(&self, duration: Duration) -> Result<(), ProbeError> {
        TestProbe::expect_no_msg(self, duration)
    }
}

/// Deterministic clock for actor-system scheduler tests.
///
/// `ManualTime` wraps the actor runtime's [`ManualScheduler`] and exposes a
/// test-facing API for advancing scheduled actor sends, timers, receive
/// timeouts, and manually scheduled probe messages without waiting for wall
/// clock time.
#[derive(Debug, Clone, Default)]
pub struct ManualTime {
    scheduler: ManualScheduler,
}

impl ManualTime {
    /// Creates a new manual-time controller with its clock at zero.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns a clone of the underlying actor-runtime manual scheduler.
    ///
    /// Actor-system testkits use this scheduler when built through
    /// [`ActorSystemTestKit::with_manual_time`](crate::ActorSystemTestKit::with_manual_time).
    pub fn scheduler(&self) -> ManualScheduler {
        self.scheduler.clone()
    }

    /// Returns the current manual clock value.
    pub fn now(&self) -> Duration {
        self.scheduler.now()
    }

    /// Returns the next active scheduled deadline on the manual clock.
    pub fn next_deadline(&self) -> Option<Duration> {
        self.scheduler.next_deadline()
    }

    /// Schedules one message to be sent to an actor ref after `delay`.
    ///
    /// The message is delivered when the manual clock reaches the due time and
    /// due work is run by [`Self::advance`] or [`Self::run_due`]. The returned
    /// handle can cancel the scheduled delivery before it runs.
    pub fn schedule_once<M>(
        &self,
        delay: Duration,
        target: ActorRef<M>,
        message: M,
    ) -> ManualTimeHandle
    where
        M: Send + 'static,
    {
        self.scheduler.schedule_once(delay, target, message)
    }

    /// Moves the manual clock forward and runs all work due at or before the new time.
    pub fn advance(&self, amount: Duration) {
        self.scheduler.advance(amount);
    }

    /// Advances to the next active scheduled deadline and runs due work.
    ///
    /// Returns `false` when no active scheduled work exists.
    pub fn advance_to_next(&self) -> bool {
        let Some(deadline) = self.next_deadline() else {
            return false;
        };
        self.advance(deadline.saturating_sub(self.now()));
        true
    }

    /// Advances time and verifies that each supplied probe stays quiet.
    ///
    /// The probe slice may contain heterogeneous [`TestProbe<M>`] protocol
    /// types through [`NoMessageProbe`]. After advancing the scheduler, each
    /// probe gets a short dispatcher settle window so due messages are observed
    /// deterministically.
    pub fn expect_no_msg_for(
        &self,
        duration: Duration,
        probes: &[&dyn NoMessageProbe],
    ) -> Result<(), ProbeError> {
        self.advance(duration);
        for probe in probes {
            probe.expect_no_msg(NO_MESSAGE_SETTLE)?;
        }
        Ok(())
    }

    /// Runs all currently due scheduled work without advancing the clock.
    pub fn run_due(&self) {
        self.scheduler.run_due();
    }

    /// Returns the number of pending scheduled entries.
    pub fn pending_count(&self) -> usize {
        self.scheduler.pending_count()
    }
}
