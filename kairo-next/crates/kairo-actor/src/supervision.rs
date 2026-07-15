use crate::ActorPath;

use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
/// Directive applied when an actor fails during startup or message processing.
pub enum SupervisorStrategy {
    /// Stop the failed actor.
    #[default]
    Stop,
    /// Drop the failed message and continue with the current actor instance.
    Resume,
    /// Recreate the actor and stop its existing children.
    Restart,
    /// Recreate the actor while retaining existing child incarnations.
    RestartPreservingChildren,
    /// Propagate the failure to the parent actor.
    Escalate,
    /// Restart only while the configured restart budget permits it.
    RestartWithLimit {
        /// Maximum restarts admitted during one window.
        max_restarts: usize,
        /// Duration of the restart accounting window.
        within: Duration,
        /// Whether existing children are stopped during restart.
        stop_children: bool,
    },
}

impl SupervisorStrategy {
    /// Creates a bounded restart strategy that stops existing children.
    pub fn restart_with_limit(max_restarts: usize, within: Duration) -> Self {
        Self::RestartWithLimit {
            max_restarts,
            within,
            stop_children: true,
        }
    }

    /// Creates an unbounded restart strategy that retains existing children.
    pub fn restart_preserving_children() -> Self {
        Self::RestartPreservingChildren
    }

    /// Creates a bounded restart strategy that retains existing children.
    pub fn restart_with_limit_preserving_children(max_restarts: usize, within: Duration) -> Self {
        Self::RestartWithLimit {
            max_restarts,
            within,
            stop_children: false,
        }
    }

    pub(crate) fn stop_children_on_restart(self) -> bool {
        match self {
            Self::RestartPreservingChildren => false,
            Self::RestartWithLimit { stop_children, .. } => stop_children,
            _ => true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SupervisionFailure {
    child: ActorPath,
    reason: String,
}

impl SupervisionFailure {
    pub(crate) fn new(child: ActorPath, reason: impl Into<String>) -> Self {
        Self {
            child,
            reason: reason.into(),
        }
    }

    pub(crate) fn child(&self) -> &ActorPath {
        &self.child
    }

    pub(crate) fn reason(&self) -> &str {
        &self.reason
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

    pub(crate) fn startup_restart_allowed(
        &mut self,
        max_restarts: usize,
        within: Duration,
        now: Instant,
    ) -> bool {
        if max_restarts <= 1 {
            return false;
        }

        let reset_window = self
            .restart_window_started
            .is_none_or(|started| !within.is_zero() && now.duration_since(started) > within);
        if reset_window {
            self.restart_window_started = Some(now);
            self.restart_count = 0;
        }

        if self.restart_count + 1 >= max_restarts {
            false
        } else {
            self.restart_count += 1;
            true
        }
    }
}
