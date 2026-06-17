use std::fmt;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU8, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use crate::ActorRef;
use crate::receive_timeout::ReceiveTimeoutEnvelope;
use crate::timers::TimerEnvelope;

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

    pub(crate) fn cancelled() -> Self {
        Self {
            state: Arc::new(AtomicU8::new(CANCELLED)),
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

    fn is_active(&self) -> bool {
        self.state.load(Ordering::Acquire) == SCHEDULED
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

#[derive(Clone)]
pub(crate) struct Scheduler {
    backend: Arc<dyn SchedulerBackend>,
}

impl Scheduler {
    pub(crate) fn real() -> Self {
        Self {
            backend: Arc::new(RealScheduler),
        }
    }

    pub(crate) fn schedule_once<M>(
        &self,
        delay: Duration,
        target: ActorRef<M>,
        message: M,
    ) -> Cancellable
    where
        M: Send + 'static,
    {
        self.backend.schedule_once(
            delay,
            Box::new(move || {
                let _ = target.tell(message);
            }),
        )
    }

    pub(crate) fn schedule_action(
        &self,
        delay: Duration,
        action: impl FnOnce() + Send + 'static,
    ) -> Cancellable {
        self.backend.schedule_once(delay, Box::new(action))
    }

    pub(crate) fn schedule_timer<M>(
        &self,
        delay: Duration,
        target: ActorRef<M>,
        key: String,
        generation: u64,
        message: M,
    ) -> Cancellable
    where
        M: Send + 'static,
    {
        self.backend.schedule_once(
            delay,
            Box::new(move || {
                target.send_timer(TimerEnvelope::new(key, generation, message));
            }),
        )
    }

    pub(crate) fn schedule_receive_timeout<M>(
        &self,
        delay: Duration,
        target: ActorRef<M>,
        timeout: ReceiveTimeoutEnvelope<M>,
    ) -> Cancellable
    where
        M: Send + 'static,
    {
        self.backend.schedule_once(
            delay,
            Box::new(move || {
                target.send_receive_timeout(timeout);
            }),
        )
    }

    pub(crate) fn schedule_timer_with_fixed_delay<M>(
        &self,
        initial_delay: Duration,
        delay: Duration,
        target: ActorRef<M>,
        key: String,
        generation: u64,
        message: M,
    ) -> Cancellable
    where
        M: Clone + Send + 'static,
    {
        self.backend.schedule_repeated(
            initial_delay,
            delay,
            RepeatingMode::FixedDelay,
            Box::new(move || {
                target.send_timer(TimerEnvelope::new(key.clone(), generation, message.clone()));
            }),
        )
    }

    pub(crate) fn schedule_timer_at_fixed_rate<M>(
        &self,
        initial_delay: Duration,
        interval: Duration,
        target: ActorRef<M>,
        key: String,
        generation: u64,
        message: M,
    ) -> Cancellable
    where
        M: Clone + Send + 'static,
    {
        self.backend.schedule_repeated(
            initial_delay,
            interval,
            RepeatingMode::FixedRate,
            Box::new(move || {
                target.send_timer(TimerEnvelope::new(key.clone(), generation, message.clone()));
            }),
        )
    }
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::real()
    }
}

impl fmt::Debug for Scheduler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Scheduler").finish_non_exhaustive()
    }
}

trait SchedulerBackend: Send + Sync + 'static {
    fn schedule_once(&self, delay: Duration, action: Box<dyn FnOnce() + Send>) -> Cancellable;

    fn schedule_repeated(
        &self,
        initial_delay: Duration,
        interval: Duration,
        mode: RepeatingMode,
        action: Box<dyn FnMut() + Send>,
    ) -> Cancellable;
}

#[derive(Debug)]
struct RealScheduler;

impl SchedulerBackend for RealScheduler {
    fn schedule_once(&self, delay: Duration, action: Box<dyn FnOnce() + Send>) -> Cancellable {
        let cancellable = Cancellable::new();
        let task = cancellable.clone();
        thread::Builder::new()
            .name("kairo-scheduler-once".to_string())
            .spawn(move || {
                thread::sleep(delay);
                if task.complete() {
                    action();
                }
            })
            .expect("failed to spawn scheduler task");
        cancellable
    }

    fn schedule_repeated(
        &self,
        initial_delay: Duration,
        interval: Duration,
        mode: RepeatingMode,
        mut action: Box<dyn FnMut() + Send>,
    ) -> Cancellable {
        let cancellable = Cancellable::new();
        let task = cancellable.clone();
        let thread_name = match mode {
            RepeatingMode::FixedDelay => "kairo-timer-fixed-delay",
            RepeatingMode::FixedRate => "kairo-timer-fixed-rate",
        };
        thread::Builder::new()
            .name(thread_name.to_string())
            .spawn(move || match mode {
                RepeatingMode::FixedDelay => {
                    thread::sleep(initial_delay);
                    while task.is_active() {
                        action();
                        thread::sleep(interval);
                    }
                }
                RepeatingMode::FixedRate => {
                    let mut next_tick = Instant::now() + initial_delay;
                    sleep_until(next_tick, &task);

                    while task.is_active() {
                        action();
                        next_tick += interval;
                        sleep_until(next_tick, &task);
                    }
                }
            })
            .expect("failed to spawn repeated scheduler task");
        cancellable
    }
}

#[derive(Clone, Default)]
pub struct ManualScheduler {
    inner: Arc<Mutex<ManualState>>,
}

impl ManualScheduler {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn now(&self) -> Duration {
        self.inner.lock().expect("manual scheduler poisoned").now
    }

    pub fn schedule_once<M>(&self, delay: Duration, target: ActorRef<M>, message: M) -> Cancellable
    where
        M: Send + 'static,
    {
        self.schedule_once_action(
            delay,
            Box::new(move || {
                let _ = target.tell(message);
            }),
        )
    }

    pub fn advance(&self, amount: Duration) {
        let run_to = self.now() + amount;
        self.run_until(run_to);
    }

    pub fn run_due(&self) {
        self.run_until(self.now());
    }

    fn run_until(&self, run_to: Duration) {
        loop {
            let Some(mut scheduled) = self.pop_due(run_to) else {
                self.inner.lock().expect("manual scheduler poisoned").now = run_to;
                return;
            };

            self.inner.lock().expect("manual scheduler poisoned").now = scheduled.deadline;
            match &mut scheduled.action {
                ScheduledAction::Once(action) => {
                    if scheduled.cancellable.complete()
                        && let Some(action) = action.take()
                    {
                        action();
                    }
                }
                ScheduledAction::Repeated { action, .. } => {
                    if scheduled.cancellable.is_active() {
                        action();
                    }
                }
            }

            if scheduled.should_repeat() {
                scheduled.advance_deadline(self.now());
                self.inner
                    .lock()
                    .expect("manual scheduler poisoned")
                    .scheduled
                    .push(scheduled);
            }
        }
    }

    pub fn pending_count(&self) -> usize {
        self.inner
            .lock()
            .expect("manual scheduler poisoned")
            .scheduled
            .iter()
            .filter(|scheduled| scheduled.cancellable.is_active())
            .count()
    }

    /// Returns the earliest active scheduled deadline on the manual clock.
    pub fn next_deadline(&self) -> Option<Duration> {
        self.inner
            .lock()
            .expect("manual scheduler poisoned")
            .scheduled
            .iter()
            .filter(|scheduled| scheduled.cancellable.is_active())
            .map(|scheduled| scheduled.deadline)
            .min()
    }

    pub(crate) fn into_scheduler(self) -> Scheduler {
        Scheduler {
            backend: Arc::new(self),
        }
    }

    fn schedule_once_action(
        &self,
        delay: Duration,
        action: Box<dyn FnOnce() + Send>,
    ) -> Cancellable {
        let cancellable = Cancellable::new();
        self.push_scheduled(
            delay,
            cancellable.clone(),
            ScheduledAction::Once(Some(action)),
        );
        cancellable
    }

    fn push_scheduled(&self, delay: Duration, cancellable: Cancellable, action: ScheduledAction) {
        {
            let mut state = self.inner.lock().expect("manual scheduler poisoned");
            let scheduled = Scheduled {
                deadline: state.now + delay,
                order: state.next_order,
                cancellable,
                action,
            };
            state.next_order += 1;
            state.scheduled.push(scheduled);
        }
        if delay.is_zero() {
            self.run_due();
        }
    }

    fn pop_due(&self, run_to: Duration) -> Option<Scheduled> {
        let mut state = self.inner.lock().expect("manual scheduler poisoned");
        let index = state
            .scheduled
            .iter()
            .enumerate()
            .filter(|(_, scheduled)| scheduled.deadline <= run_to)
            .min_by_key(|(_, scheduled)| (scheduled.deadline, scheduled.order))
            .map(|(index, _)| index)?;
        Some(state.scheduled.swap_remove(index))
    }
}

impl fmt::Debug for ManualScheduler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ManualScheduler")
            .field("now", &self.now())
            .field("pending_count", &self.pending_count())
            .field("next_deadline", &self.next_deadline())
            .finish()
    }
}

impl SchedulerBackend for ManualScheduler {
    fn schedule_once(&self, delay: Duration, action: Box<dyn FnOnce() + Send>) -> Cancellable {
        self.schedule_once_action(delay, action)
    }

    fn schedule_repeated(
        &self,
        initial_delay: Duration,
        interval: Duration,
        mode: RepeatingMode,
        action: Box<dyn FnMut() + Send>,
    ) -> Cancellable {
        let cancellable = Cancellable::new();
        self.push_scheduled(
            initial_delay,
            cancellable.clone(),
            ScheduledAction::Repeated {
                interval,
                mode,
                action,
            },
        );
        cancellable
    }
}

#[derive(Default)]
struct ManualState {
    now: Duration,
    next_order: u64,
    scheduled: Vec<Scheduled>,
}

struct Scheduled {
    deadline: Duration,
    order: u64,
    cancellable: Cancellable,
    action: ScheduledAction,
}

impl Scheduled {
    fn should_repeat(&self) -> bool {
        matches!(self.action, ScheduledAction::Repeated { interval, .. } if interval > Duration::ZERO)
            && self.cancellable.is_active()
    }

    fn advance_deadline(&mut self, now: Duration) {
        if let ScheduledAction::Repeated { interval, mode, .. } = &self.action {
            self.deadline = match mode {
                RepeatingMode::FixedDelay => now + *interval,
                RepeatingMode::FixedRate => self.deadline + *interval,
            };
        }
    }
}

enum ScheduledAction {
    Once(Option<Box<dyn FnOnce() + Send>>),
    Repeated {
        interval: Duration,
        mode: RepeatingMode,
        action: Box<dyn FnMut() + Send>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RepeatingMode {
    FixedDelay,
    FixedRate,
}

fn sleep_until(deadline: Instant, task: &Cancellable) {
    while task.is_active() {
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            return;
        };
        thread::sleep(remaining);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::time::Duration;

    use super::*;

    #[test]
    fn manual_scheduler_runs_nested_work_from_due_task_time() {
        let scheduler = ManualScheduler::new();
        let nested = scheduler.clone();
        let (observed_tx, observed_rx) = mpsc::channel();

        scheduler.schedule_once_action(
            Duration::from_secs(1),
            Box::new(move || {
                observed_tx
                    .send(nested.now())
                    .expect("outer observation should send");
                let nested_again = nested.clone();
                let observed_tx = observed_tx.clone();
                nested.schedule_once_action(
                    Duration::from_millis(500),
                    Box::new(move || {
                        observed_tx
                            .send(nested_again.now())
                            .expect("nested observation should send");
                    }),
                );
            }),
        );

        scheduler.advance(Duration::from_secs(2));

        assert_eq!(observed_rx.recv().unwrap(), Duration::from_secs(1));
        assert_eq!(observed_rx.recv().unwrap(), Duration::from_millis(1500));
        assert_eq!(scheduler.now(), Duration::from_secs(2));
        assert_eq!(scheduler.pending_count(), 0);
    }
}
