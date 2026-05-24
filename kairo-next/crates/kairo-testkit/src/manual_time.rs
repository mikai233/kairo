use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

use kairo_actor::ActorRef;

const SCHEDULED: u8 = 0;
const CANCELLED: u8 = 1;
const COMPLETED: u8 = 2;

#[derive(Default)]
pub struct ManualTime {
    now: Duration,
    next_order: u64,
    scheduled: Vec<Scheduled>,
}

impl ManualTime {
    pub fn now(&self) -> Duration {
        self.now
    }

    pub fn schedule_once<M>(
        &mut self,
        delay: Duration,
        target: ActorRef<M>,
        message: M,
    ) -> ManualTimeHandle
    where
        M: Send + 'static,
    {
        let handle = ManualTimeHandle::new();
        let task = Scheduled {
            deadline: self.now + delay,
            order: self.next_order,
            handle: handle.clone(),
            action: Some(Box::new(move || {
                let _ = target.tell(message);
            })),
        };
        self.next_order += 1;
        self.scheduled.push(task);
        handle
    }

    pub fn advance(&mut self, amount: Duration) {
        self.now += amount;
        self.run_due();
    }

    pub fn run_due(&mut self) {
        let mut pending = Vec::with_capacity(self.scheduled.len());
        let mut due = Vec::new();

        for task in self.scheduled.drain(..) {
            if task.deadline <= self.now {
                due.push(task);
            } else {
                pending.push(task);
            }
        }

        due.sort_by_key(|task| (task.deadline, task.order));
        self.scheduled = pending;

        for mut task in due {
            if task.handle.complete()
                && let Some(action) = task.action.take()
            {
                action();
            }
        }
    }

    pub fn pending_count(&self) -> usize {
        self.scheduled
            .iter()
            .filter(|task| !task.handle.is_cancelled())
            .count()
    }
}

struct Scheduled {
    deadline: Duration,
    order: u64,
    handle: ManualTimeHandle,
    action: Option<Box<dyn FnOnce() + Send>>,
}

#[derive(Clone)]
pub struct ManualTimeHandle {
    state: Arc<AtomicU8>,
}

impl ManualTimeHandle {
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

impl std::fmt::Debug for ManualTimeHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManualTimeHandle")
            .field("is_cancelled", &self.is_cancelled())
            .field("is_completed", &self.is_completed())
            .finish()
    }
}
