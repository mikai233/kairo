use std::cmp::Ordering as CmpOrdering;
use std::collections::BinaryHeap;
use std::fmt;
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
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
            backend: Arc::new(RealScheduler::new()),
        }
    }

    pub(crate) fn shutdown(&self) {
        self.backend.shutdown();
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

    fn shutdown(&self);
}

struct RealScheduler {
    inner: Arc<RealSchedulerInner>,
    driver: Mutex<Option<JoinHandle<()>>>,
}

impl RealScheduler {
    fn new() -> Self {
        Self {
            inner: Arc::new(RealSchedulerInner {
                state: Mutex::new(RealSchedulerState {
                    accepting: true,
                    next_order: 0,
                    scheduled: BinaryHeap::new(),
                }),
                changed: Condvar::new(),
            }),
            driver: Mutex::new(None),
        }
    }

    fn schedule(&self, delay: Duration, action: ScheduledAction) -> Cancellable {
        let cancellable = Cancellable::new();
        self.ensure_driver_started();
        let mut state = self.inner.state.lock().expect("real scheduler poisoned");
        if !state.accepting {
            cancellable.cancel();
            return cancellable;
        }
        let scheduled = RealScheduled {
            deadline: Instant::now() + delay,
            order: state.next_order,
            cancellable: cancellable.clone(),
            action,
        };
        state.next_order = state.next_order.wrapping_add(1);
        state.scheduled.push(scheduled);
        self.inner.changed.notify_one();
        cancellable
    }

    fn ensure_driver_started(&self) {
        let mut driver = self.driver.lock().expect("real scheduler driver poisoned");
        if driver.is_some()
            || !self
                .inner
                .state
                .lock()
                .expect("real scheduler poisoned")
                .accepting
        {
            return;
        }
        let inner = Arc::clone(&self.inner);
        *driver = Some(
            thread::Builder::new()
                .name("kairo-scheduler".to_string())
                .spawn(move || run_real_scheduler(inner))
                .expect("failed to spawn scheduler driver"),
        );
    }

    fn stop(&self) {
        let pending = {
            let mut state = self.inner.state.lock().expect("real scheduler poisoned");
            if !state.accepting {
                Vec::new()
            } else {
                state.accepting = false;
                let pending = state.scheduled.drain().collect::<Vec<_>>();
                self.inner.changed.notify_all();
                pending
            }
        };
        for scheduled in pending {
            scheduled.cancellable.cancel();
        }

        let driver = self
            .driver
            .lock()
            .expect("real scheduler driver poisoned")
            .take();
        if let Some(driver) = driver
            && driver.thread().id() != thread::current().id()
        {
            let _ = driver.join();
        }
    }
}

impl fmt::Debug for RealScheduler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.inner.state.lock().expect("real scheduler poisoned");
        f.debug_struct("RealScheduler")
            .field("accepting", &state.accepting)
            .field("scheduled", &state.scheduled.len())
            .finish_non_exhaustive()
    }
}

impl Drop for RealScheduler {
    fn drop(&mut self) {
        self.stop();
    }
}

impl SchedulerBackend for RealScheduler {
    fn schedule_once(&self, delay: Duration, action: Box<dyn FnOnce() + Send>) -> Cancellable {
        self.schedule(delay, ScheduledAction::Once(Some(action)))
    }

    fn schedule_repeated(
        &self,
        initial_delay: Duration,
        interval: Duration,
        mode: RepeatingMode,
        action: Box<dyn FnMut() + Send>,
    ) -> Cancellable {
        if interval.is_zero() {
            return Cancellable::cancelled();
        }
        self.schedule(
            initial_delay,
            ScheduledAction::Repeated {
                interval,
                mode,
                action,
            },
        )
    }

    fn shutdown(&self) {
        self.stop();
    }
}

struct RealSchedulerInner {
    state: Mutex<RealSchedulerState>,
    changed: Condvar,
}

struct RealSchedulerState {
    accepting: bool,
    next_order: u64,
    scheduled: BinaryHeap<RealScheduled>,
}

struct RealScheduled {
    deadline: Instant,
    order: u64,
    cancellable: Cancellable,
    action: ScheduledAction,
}

impl PartialEq for RealScheduled {
    fn eq(&self, other: &Self) -> bool {
        self.deadline == other.deadline && self.order == other.order
    }
}

impl Eq for RealScheduled {}

impl PartialOrd for RealScheduled {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for RealScheduled {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        other
            .deadline
            .cmp(&self.deadline)
            .then_with(|| other.order.cmp(&self.order))
    }
}

fn run_real_scheduler(inner: Arc<RealSchedulerInner>) {
    while let Some(scheduled) = wait_for_due(&inner) {
        run_scheduled(&inner, scheduled);
    }
}

fn wait_for_due(inner: &RealSchedulerInner) -> Option<RealScheduled> {
    let mut state = inner.state.lock().expect("real scheduler poisoned");
    loop {
        if !state.accepting {
            return None;
        }
        while state
            .scheduled
            .peek()
            .is_some_and(|scheduled| !scheduled.cancellable.is_active())
        {
            state.scheduled.pop();
        }
        let Some(scheduled) = state.scheduled.peek() else {
            state = inner.changed.wait(state).expect("real scheduler poisoned");
            continue;
        };
        let Some(remaining) = scheduled.deadline.checked_duration_since(Instant::now()) else {
            return state.scheduled.pop();
        };
        let (next_state, _) = inner
            .changed
            .wait_timeout(state, remaining)
            .expect("real scheduler poisoned");
        state = next_state;
    }
}

fn run_scheduled(inner: &RealSchedulerInner, mut scheduled: RealScheduled) {
    match &mut scheduled.action {
        ScheduledAction::Once(action) => {
            if scheduled.cancellable.complete()
                && let Some(action) = action.take()
            {
                let _ = panic::catch_unwind(AssertUnwindSafe(action));
            }
        }
        ScheduledAction::Repeated {
            interval,
            mode,
            action,
        } => {
            if !scheduled.cancellable.is_active() {
                return;
            }
            if panic::catch_unwind(AssertUnwindSafe(action)).is_err() {
                scheduled.cancellable.cancel();
                return;
            }
            if !scheduled.cancellable.is_active() {
                return;
            }
            scheduled.deadline = match mode {
                RepeatingMode::FixedDelay => Instant::now() + *interval,
                RepeatingMode::FixedRate => scheduled.deadline + *interval,
            };
            let mut state = inner.state.lock().expect("real scheduler poisoned");
            if state.accepting {
                scheduled.order = state.next_order;
                state.next_order = state.next_order.wrapping_add(1);
                state.scheduled.push(scheduled);
                inner.changed.notify_one();
            } else {
                drop(state);
                scheduled.cancellable.cancel();
            }
        }
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
            if !state.accepting {
                cancellable.cancel();
                return;
            }
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
        if interval.is_zero() {
            return Cancellable::cancelled();
        }
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

    fn shutdown(&self) {
        let scheduled = {
            let mut state = self.inner.lock().expect("manual scheduler poisoned");
            state.accepting = false;
            std::mem::take(&mut state.scheduled)
        };
        for scheduled in scheduled {
            scheduled.cancellable.cancel();
        }
    }
}

struct ManualState {
    accepting: bool,
    now: Duration,
    next_order: u64,
    scheduled: Vec<Scheduled>,
}

impl Default for ManualState {
    fn default() -> Self {
        Self {
            accepting: true,
            now: Duration::ZERO,
            next_order: 0,
            scheduled: Vec::new(),
        }
    }
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

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::mpsc;
    use std::thread;
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

    #[test]
    fn real_scheduler_runs_thousands_of_actions_on_one_driver_thread() {
        const ACTIONS: usize = 2_000;
        let scheduler = Scheduler::real();
        let (observed_tx, observed_rx) = mpsc::channel();

        for _ in 0..ACTIONS {
            let observed_tx = observed_tx.clone();
            scheduler.schedule_action(Duration::from_millis(20), move || {
                observed_tx.send(thread::current().id()).unwrap();
            });
        }

        let mut thread_ids = HashSet::new();
        for _ in 0..ACTIONS {
            thread_ids.insert(observed_rx.recv_timeout(Duration::from_secs(2)).unwrap());
        }
        assert_eq!(thread_ids.len(), 1, "scheduler threads: {thread_ids:?}");
        scheduler.shutdown();
    }

    #[test]
    fn callback_panic_does_not_stop_real_scheduler_driver() {
        let scheduler = Scheduler::real();
        let panicked = scheduler.schedule_action(Duration::ZERO, || panic!("scheduled failure"));
        let (recovered_tx, recovered_rx) = mpsc::channel();
        let recovered = scheduler.schedule_action(Duration::ZERO, move || {
            recovered_tx.send(()).unwrap();
        });

        recovered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(panicked.is_completed());
        assert!(recovered.is_completed());
        scheduler.shutdown();
    }

    #[test]
    fn repeated_callback_panic_aborts_only_that_schedule() {
        let scheduler = Scheduler::real();
        let backend = Arc::clone(&scheduler.backend);
        let panicked = backend.schedule_repeated(
            Duration::ZERO,
            Duration::from_millis(1),
            RepeatingMode::FixedDelay,
            Box::new(|| panic!("repeated scheduled failure")),
        );
        let (recovered_tx, recovered_rx) = mpsc::channel();
        scheduler.schedule_action(Duration::from_millis(10), move || {
            recovered_tx.send(()).unwrap();
        });

        recovered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(panicked.is_cancelled());
        scheduler.shutdown();
    }

    #[test]
    fn real_scheduler_shutdown_cancels_pending_work_and_wakes_driver() {
        const ACTIONS: usize = 2_000;
        let scheduler = Scheduler::real();
        let handles = (0..ACTIONS)
            .map(|_| scheduler.schedule_action(Duration::from_secs(60), || {}))
            .collect::<Vec<_>>();

        let started = Instant::now();
        scheduler.shutdown();

        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(handles.iter().all(Cancellable::is_cancelled));
        assert!(
            scheduler
                .schedule_action(Duration::ZERO, || {})
                .is_cancelled()
        );
    }
}
