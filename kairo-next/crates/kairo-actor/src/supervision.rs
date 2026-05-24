use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SupervisorStrategy {
    #[default]
    Stop,
    Resume,
    Restart,
    RestartWithLimit {
        max_restarts: usize,
        within: Duration,
    },
}

impl SupervisorStrategy {
    pub fn restart_with_limit(max_restarts: usize, within: Duration) -> Self {
        Self::RestartWithLimit {
            max_restarts,
            within,
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct SupervisionState {
    restart_window_started: Option<Instant>,
    restart_count: usize,
}

impl SupervisionState {
    pub(crate) fn restart_allowed(
        &mut self,
        max_restarts: usize,
        within: Duration,
        now: Instant,
    ) -> bool {
        if max_restarts == 0 {
            return false;
        }

        let reset_window = self
            .restart_window_started
            .is_none_or(|started| !within.is_zero() && now.duration_since(started) > within);
        if reset_window {
            self.restart_window_started = Some(now);
            self.restart_count = 0;
        }

        if self.restart_count >= max_restarts {
            false
        } else {
            self.restart_count += 1;
            true
        }
    }
}
