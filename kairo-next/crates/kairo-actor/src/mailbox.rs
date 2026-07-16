use std::collections::VecDeque;
use std::fmt;
use std::sync::{Arc, Mutex};

use crate::receive_timeout::ReceiveTimeoutEnvelope;
use crate::signal::Signal;
use crate::supervision::SupervisionFailure;
use crate::timers::TimerEnvelope;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SystemMessage {
    Stop,
    Restart,
    Signal(Signal),
    GatedSignal(Signal),
    SupervisionFailure(SupervisionFailure),
}

#[derive(Debug)]
pub(crate) enum Dequeued<M> {
    System(SystemMessage),
    User(UserEnvelope<M>),
    Closed,
}

pub(crate) enum UserEnvelope<M> {
    Message(M),
    Timer(TimerEnvelope<M>),
    ReceiveTimeout(ReceiveTimeoutEnvelope<M>),
    Adapted(Box<dyn FnOnce() -> Option<M> + Send>),
}

impl<M: fmt::Debug> fmt::Debug for UserEnvelope<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Message(message) => f.debug_tuple("Message").field(message).finish(),
            Self::Timer(timer) => f.debug_tuple("Timer").field(timer).finish(),
            Self::ReceiveTimeout(timeout) => {
                f.debug_tuple("ReceiveTimeout").field(timeout).finish()
            }
            Self::Adapted(_) => f.debug_tuple("Adapted").finish_non_exhaustive(),
        }
    }
}

pub(crate) struct Mailbox<M> {
    settings: MailboxSettings,
    state: Mutex<MailboxState<M>>,
}

impl<M> fmt::Debug for Mailbox<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.state.lock().expect("mailbox poisoned");
        f.debug_struct("Mailbox")
            .field("settings", &self.settings)
            .field("system_messages", &state.system.len())
            .field("user_messages", &state.user.len())
            .field("closed", &state.closed)
            .finish_non_exhaustive()
    }
}

pub(crate) trait MailboxScheduler: Send + Sync + 'static {
    fn schedule(self: Arc<Self>) -> bool;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
/// Capacity policy for an actor's user-message mailbox lane.
pub struct MailboxSettings {
    user_capacity: Option<usize>,
}

impl MailboxSettings {
    /// Creates an unbounded user-message mailbox policy.
    pub fn unbounded() -> Self {
        Self {
            user_capacity: None,
        }
    }

    /// Creates a user-message mailbox bounded to `user_capacity` messages.
    pub fn bounded(user_capacity: usize) -> Self {
        Self {
            user_capacity: Some(user_capacity),
        }
    }

    /// Returns the user-message capacity, or `None` for an unbounded mailbox.
    pub fn user_capacity(&self) -> Option<usize> {
        self.user_capacity
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MailboxEnqueueError<T> {
    Closed(T),
    Full(T),
}

impl<T> MailboxEnqueueError<T> {
    pub(crate) fn into_message(self) -> T {
        match self {
            Self::Closed(message) | Self::Full(message) => message,
        }
    }

    pub(crate) fn reason(&self) -> &'static str {
        match self {
            Self::Closed(_) => "actor mailbox is closed",
            Self::Full(_) => "actor mailbox is full",
        }
    }

    pub(crate) fn is_closed(&self) -> bool {
        matches!(self, Self::Closed(_))
    }
}

struct MailboxState<M> {
    system: VecDeque<SystemMessage>,
    user: VecDeque<UserEnvelope<M>>,
    closed: bool,
    scheduler: Option<Arc<dyn MailboxScheduler>>,
}

impl<M> MailboxState<M> {
    fn scheduler_for_new_message(&self) -> Option<Arc<dyn MailboxScheduler>> {
        if self.system.is_empty() && self.user.is_empty() {
            self.scheduler.clone()
        } else {
            None
        }
    }
}

impl<M> Default for Mailbox<M> {
    fn default() -> Self {
        Self::new(MailboxSettings::default())
    }
}

impl<M> Mailbox<M> {
    pub(crate) fn new(settings: MailboxSettings) -> Self {
        Self {
            settings,
            state: Mutex::new(MailboxState {
                system: VecDeque::new(),
                user: VecDeque::new(),
                closed: false,
                scheduler: None,
            }),
        }
    }

    pub(crate) fn set_scheduler(&self, scheduler: Arc<dyn MailboxScheduler>) {
        self.state.lock().expect("mailbox poisoned").scheduler = Some(scheduler);
    }

    pub(crate) fn clear_scheduler(&self) {
        self.state
            .lock()
            .expect("mailbox poisoned")
            .scheduler
            .take();
    }

    pub(crate) fn enqueue_user(&self, message: M) -> Result<(), MailboxEnqueueError<M>> {
        let mut state = self.state.lock().expect("mailbox poisoned");
        if state.closed {
            return Err(MailboxEnqueueError::Closed(message));
        }
        if self.is_full(&state) {
            return Err(MailboxEnqueueError::Full(message));
        }
        let scheduler = state.scheduler_for_new_message();
        state.user.push_back(UserEnvelope::Message(message));
        drop(state);
        Self::schedule(scheduler);
        Ok(())
    }

    pub(crate) fn prepend_user_messages(&self, mut messages: Vec<M>) -> Result<(), Vec<M>> {
        let mut state = self.state.lock().expect("mailbox poisoned");
        if state.closed {
            return Err(messages);
        }
        let scheduler = if messages.is_empty() {
            None
        } else {
            state.scheduler_for_new_message()
        };
        for message in messages.drain(..).rev() {
            state.user.push_front(UserEnvelope::Message(message));
        }
        drop(state);
        Self::schedule(scheduler);
        Ok(())
    }

    pub(crate) fn enqueue_timer(
        &self,
        timer: TimerEnvelope<M>,
    ) -> Result<(), MailboxEnqueueError<TimerEnvelope<M>>> {
        let mut state = self.state.lock().expect("mailbox poisoned");
        if state.closed {
            return Err(MailboxEnqueueError::Closed(timer));
        }
        if self.is_full(&state) {
            return Err(MailboxEnqueueError::Full(timer));
        }
        let scheduler = state.scheduler_for_new_message();
        state.user.push_back(UserEnvelope::Timer(timer));
        drop(state);
        Self::schedule(scheduler);
        Ok(())
    }

    pub(crate) fn enqueue_receive_timeout(
        &self,
        timeout: ReceiveTimeoutEnvelope<M>,
    ) -> Result<(), MailboxEnqueueError<ReceiveTimeoutEnvelope<M>>> {
        let mut state = self.state.lock().expect("mailbox poisoned");
        if state.closed {
            return Err(MailboxEnqueueError::Closed(timeout));
        }
        if self.is_full(&state) {
            return Err(MailboxEnqueueError::Full(timeout));
        }
        let scheduler = state.scheduler_for_new_message();
        state.user.push_back(UserEnvelope::ReceiveTimeout(timeout));
        drop(state);
        Self::schedule(scheduler);
        Ok(())
    }

    pub(crate) fn enqueue_adapted<U, F>(
        &self,
        message: U,
        adapt: F,
    ) -> Result<(), MailboxEnqueueError<U>>
    where
        U: Send + 'static,
        F: FnOnce(U) -> Option<M> + Send + 'static,
    {
        let mut state = self.state.lock().expect("mailbox poisoned");
        if state.closed {
            return Err(MailboxEnqueueError::Closed(message));
        }
        if self.is_full(&state) {
            return Err(MailboxEnqueueError::Full(message));
        }
        let scheduler = state.scheduler_for_new_message();
        state
            .user
            .push_back(UserEnvelope::Adapted(Box::new(move || adapt(message))));
        drop(state);
        Self::schedule(scheduler);
        Ok(())
    }

    pub(crate) fn enqueue_system(&self, message: SystemMessage) {
        let mut state = self.state.lock().expect("mailbox poisoned");
        if state.closed {
            return;
        }
        let scheduler = state.scheduler_for_new_message();
        state.system.push_back(message);
        drop(state);
        Self::schedule(scheduler);
    }

    pub(crate) fn try_dequeue(&self) -> Option<Dequeued<M>> {
        let mut state = self.state.lock().expect("mailbox poisoned");
        if let Some(message) = state.system.pop_front() {
            return Some(Dequeued::System(message));
        }
        if let Some(message) = state.user.pop_front() {
            return Some(Dequeued::User(message));
        }
        if state.closed {
            return Some(Dequeued::Closed);
        }
        None
    }

    pub(crate) fn try_dequeue_system(&self) -> Option<SystemMessage> {
        self.state
            .lock()
            .expect("mailbox poisoned")
            .system
            .pop_front()
    }

    pub(crate) fn close_and_drain_user(&self) -> usize {
        let mut state = self.state.lock().expect("mailbox poisoned");
        state.closed = true;
        state.system.clear();
        let drained = state
            .user
            .iter()
            .filter(|envelope| matches!(envelope, UserEnvelope::Message(_)))
            .count();
        state.user.clear();
        drained
    }

    pub(crate) fn has_messages(&self) -> bool {
        let state = self.state.lock().expect("mailbox poisoned");
        !state.system.is_empty() || !state.user.is_empty()
    }

    fn schedule(scheduler: Option<Arc<dyn MailboxScheduler>>) {
        if let Some(scheduler) = scheduler {
            scheduler.schedule();
        }
    }

    fn is_full(&self, state: &MailboxState<M>) -> bool {
        self.settings
            .user_capacity
            .is_some_and(|capacity| state.user.len() >= capacity)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[derive(Default)]
    struct CountingScheduler {
        schedules: AtomicUsize,
    }

    impl MailboxScheduler for CountingScheduler {
        fn schedule(self: Arc<Self>) -> bool {
            self.schedules.fetch_add(1, Ordering::SeqCst);
            true
        }
    }

    #[test]
    fn enqueue_coalesces_wakeup_until_drained_and_clear_suppresses_later_wakeup() {
        let mailbox = Mailbox::default();
        let scheduler = Arc::new(CountingScheduler::default());
        mailbox.set_scheduler(scheduler.clone());

        mailbox.enqueue_user("first").unwrap();
        mailbox.enqueue_system(SystemMessage::Stop);
        assert_eq!(scheduler.schedules.load(Ordering::SeqCst), 1);

        assert!(matches!(
            mailbox.try_dequeue(),
            Some(Dequeued::System(SystemMessage::Stop))
        ));
        assert!(matches!(
            mailbox.try_dequeue(),
            Some(Dequeued::User(UserEnvelope::Message("first")))
        ));
        mailbox.enqueue_user("second").unwrap();
        assert_eq!(scheduler.schedules.load(Ordering::SeqCst), 2);

        assert!(matches!(
            mailbox.try_dequeue(),
            Some(Dequeued::User(UserEnvelope::Message("second")))
        ));
        mailbox.clear_scheduler();
        mailbox.enqueue_user("third").unwrap();
        assert_eq!(scheduler.schedules.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn dequeue_prioritizes_system_messages_over_queued_user_messages() {
        let mailbox = Mailbox::default();

        mailbox.enqueue_user("first-user").unwrap();
        mailbox.enqueue_user("second-user").unwrap();
        mailbox.enqueue_system(SystemMessage::Stop);

        assert!(matches!(
            mailbox.try_dequeue(),
            Some(Dequeued::System(SystemMessage::Stop))
        ));
        assert!(matches!(
            mailbox.try_dequeue(),
            Some(Dequeued::User(UserEnvelope::Message("first-user")))
        ));
        assert!(matches!(
            mailbox.try_dequeue(),
            Some(Dequeued::User(UserEnvelope::Message("second-user")))
        ));
    }

    #[test]
    fn dequeue_preserves_fifo_order_within_system_lane() {
        let mailbox: Mailbox<()> = Mailbox::default();

        mailbox.enqueue_system(SystemMessage::Signal(Signal::PreRestart));
        mailbox.enqueue_system(SystemMessage::Stop);

        assert!(matches!(
            mailbox.try_dequeue(),
            Some(Dequeued::System(SystemMessage::Signal(Signal::PreRestart)))
        ));
        assert!(matches!(
            mailbox.try_dequeue(),
            Some(Dequeued::System(SystemMessage::Stop))
        ));
    }

    #[test]
    fn bounded_mailbox_rejects_user_overflow_without_closing() {
        let mailbox = Mailbox::new(MailboxSettings::bounded(1));

        mailbox.enqueue_user("first").unwrap();
        let error = mailbox
            .enqueue_user("second")
            .expect_err("bounded mailbox should reject overflow");

        assert_eq!(error.reason(), "actor mailbox is full");
        assert!(!error.is_closed());
        assert_eq!(error.into_message(), "second");
        assert!(matches!(
            mailbox.try_dequeue(),
            Some(Dequeued::User(UserEnvelope::Message("first")))
        ));
        mailbox.enqueue_user("third").unwrap();
        assert!(matches!(
            mailbox.try_dequeue(),
            Some(Dequeued::User(UserEnvelope::Message("third")))
        ));
    }

    #[test]
    fn bounded_mailbox_keeps_system_lane_available_when_user_lane_is_full() {
        let mailbox = Mailbox::new(MailboxSettings::bounded(1));

        mailbox.enqueue_user("user").unwrap();
        mailbox.enqueue_system(SystemMessage::Stop);

        assert!(matches!(
            mailbox.try_dequeue(),
            Some(Dequeued::System(SystemMessage::Stop))
        ));
        assert!(matches!(
            mailbox.try_dequeue(),
            Some(Dequeued::User(UserEnvelope::Message("user")))
        ));
    }
}
