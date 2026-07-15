use std::fmt::{self, Formatter};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::death_watch::TerminationCause;
use crate::path::ActorPath;
use crate::supervision::SupervisionFailure;

#[derive(Clone)]
pub(crate) struct LocalActorHandle {
    pub(super) path: ActorPath,
    pub(super) terminated: Arc<TerminationLatch>,
    pub(super) stop: Arc<dyn Fn() + Send + Sync>,
    pub(super) restart: Arc<dyn Fn() + Send + Sync>,
    pub(super) supervise: Arc<dyn Fn(SupervisionFailure) + Send + Sync>,
}

impl fmt::Debug for LocalActorHandle {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalActorHandle")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl LocalActorHandle {
    pub(crate) fn path(&self) -> &ActorPath {
        &self.path
    }

    pub(crate) fn request_stop(&self) {
        (self.stop)();
    }

    pub(crate) fn request_restart(&self) {
        (self.restart)();
    }

    pub(crate) fn request_supervision(&self, failure: SupervisionFailure) {
        (self.supervise)(failure);
    }

    pub(crate) fn wait_for_stop(&self, timeout: Duration) -> bool {
        self.terminated.wait(timeout)
    }
}

#[derive(Debug, Default)]
pub(crate) struct TerminationLatch {
    cause: Mutex<Option<TerminationCause>>,
    changed: Condvar,
}

impl TerminationLatch {
    pub(crate) fn mark_stopped(&self) {
        self.mark_stopped_with_cause(TerminationCause::Stopped);
    }

    pub(crate) fn mark_stopped_with_cause(&self, cause: TerminationCause) {
        let mut stopped_cause = self.cause.lock().expect("termination latch poisoned");
        *stopped_cause = Some(cause);
        self.changed.notify_all();
    }

    pub(super) fn cause(&self) -> Option<TerminationCause> {
        self.cause
            .lock()
            .expect("termination latch poisoned")
            .clone()
    }

    pub(super) fn wait(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut stopped_cause = self.cause.lock().expect("termination latch poisoned");
        while stopped_cause.is_none() {
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                return false;
            };
            let (next_cause, wait) = self
                .changed
                .wait_timeout(stopped_cause, remaining)
                .expect("termination latch poisoned");
            stopped_cause = next_cause;
            if wait.timed_out() && stopped_cause.is_none() {
                return false;
            }
        }
        true
    }
}
