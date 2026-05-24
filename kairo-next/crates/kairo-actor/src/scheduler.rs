use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::thread;
use std::time::Duration;

use crate::ActorRef;

const SCHEDULED: u8 = 0;
const CANCELLED: u8 = 1;
const COMPLETED: u8 = 2;

#[derive(Clone)]
pub struct Cancellable {
    state: Arc<AtomicU8>,
}

impl Cancellable {
    fn new() -> Self {
        Self {
            state: Arc::new(AtomicU8::new(SCHEDULED)),
        }
    }

    pub fn cancel(&self) -> bool {
        self.state
            .compare_exchange(SCHEDULED, CANCELLED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    pub fn is_cancelled(&self) -> bool {
        self.state.load(Ordering::Acquire) == CANCELLED
    }

    pub fn is_completed(&self) -> bool {
        self.state.load(Ordering::Acquire) == COMPLETED
    }

    fn complete(&self) -> bool {
        self.state
            .compare_exchange(SCHEDULED, COMPLETED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }
}

impl fmt::Debug for Cancellable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Cancellable")
            .field("is_cancelled", &self.is_cancelled())
            .field("is_completed", &self.is_completed())
            .finish()
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct Scheduler;

impl Scheduler {
    pub(crate) fn schedule_once<M>(
        &self,
        delay: Duration,
        target: ActorRef<M>,
        message: M,
    ) -> Cancellable
    where
        M: Send + 'static,
    {
        let cancellable = Cancellable::new();
        let task = cancellable.clone();
        thread::Builder::new()
            .name("kairo-scheduler-once".to_string())
            .spawn(move || {
                thread::sleep(delay);
                if task.complete() {
                    let _ = target.tell(message);
                }
            })
            .expect("failed to spawn scheduler task");
        cancellable
    }
}
