use std::time::Duration;

use kairo_actor::{ActorRef, Cancellable, ManualScheduler};

use crate::probe::{ProbeError, TestProbe};

pub type ManualTimeHandle = Cancellable;

const NO_MESSAGE_SETTLE: Duration = Duration::from_millis(50);

#[derive(Debug, Clone, Default)]
pub struct ManualTime {
    scheduler: ManualScheduler,
}

impl ManualTime {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn scheduler(&self) -> ManualScheduler {
        self.scheduler.clone()
    }

    pub fn now(&self) -> Duration {
        self.scheduler.now()
    }

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

    pub fn advance(&self, amount: Duration) {
        self.scheduler.advance(amount);
    }

    pub fn expect_no_msg_for<M>(
        &self,
        duration: Duration,
        probes: &[&TestProbe<M>],
    ) -> Result<(), ProbeError>
    where
        M: Send + 'static,
    {
        self.advance(duration);
        for probe in probes {
            probe.expect_no_msg(NO_MESSAGE_SETTLE)?;
        }
        Ok(())
    }

    pub fn run_due(&self) {
        self.scheduler.run_due();
    }

    pub fn pending_count(&self) -> usize {
        self.scheduler.pending_count()
    }
}
