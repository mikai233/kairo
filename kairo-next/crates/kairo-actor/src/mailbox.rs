use std::collections::VecDeque;
use std::fmt;
use std::sync::{Condvar, Mutex};

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

#[derive(Debug)]
pub(crate) struct Mailbox<M> {
    settings: MailboxSettings,
    state: Mutex<MailboxState<M>>,
    ready: Condvar,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MailboxSettings {
    user_capacity: Option<usize>,
}

impl MailboxSettings {
    pub fn unbounded() -> Self {
        Self {
            user_capacity: None,
        }
    }

    pub fn bounded(user_capacity: usize) -> Self {
        Self {
            user_capacity: Some(user_capacity),
        }
    }

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

#[derive(Debug)]
struct MailboxState<M> {
    system: VecDeque<SystemMessage>,
    user: VecDeque<UserEnvelope<M>>,
    closed: bool,
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
            }),
            ready: Condvar::new(),
        }
    }

    pub(crate) fn enqueue_user(&self, message: M) -> Result<(), MailboxEnqueueError<M>> {
        let mut state = self.state.lock().expect("mailbox poisoned");
        if state.closed {
            return Err(MailboxEnqueueError::Closed(message));
        }
        if self.is_full(&state) {
            return Err(MailboxEnqueueError::Full(message));
        }
        state.user.push_back(UserEnvelope::Message(message));
        self.ready.notify_one();
        Ok(())
    }

    pub(crate) fn prepend_user_messages(&self, mut messages: Vec<M>) -> Result<(), Vec<M>> {
        let mut state = self.state.lock().expect("mailbox poisoned");
        if state.closed {
            return Err(messages);
        }
        for message in messages.drain(..).rev() {
            state.user.push_front(UserEnvelope::Message(message));
        }
        self.ready.notify_one();
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
        state.user.push_back(UserEnvelope::Timer(timer));
        self.ready.notify_one();
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
        state.user.push_back(UserEnvelope::ReceiveTimeout(timeout));
        self.ready.notify_one();
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
        state
            .user
            .push_back(UserEnvelope::Adapted(Box::new(move || adapt(message))));
        self.ready.notify_one();
        Ok(())
    }

    pub(crate) fn enqueue_system(&self, message: SystemMessage) {
        let mut state = self.state.lock().expect("mailbox poisoned");
        if state.closed {
            return;
        }
        state.system.push_back(message);
        self.ready.notify_one();
    }

    pub(crate) fn dequeue(&self) -> Dequeued<M> {
        let mut state = self.state.lock().expect("mailbox poisoned");
        loop {
            if let Some(message) = state.system.pop_front() {
                return Dequeued::System(message);
            }
            if let Some(message) = state.user.pop_front() {
                return Dequeued::User(message);
            }
            if state.closed {
                return Dequeued::Closed;
            }
            state = self.ready.wait(state).expect("mailbox poisoned");
        }
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

    pub(crate) fn close_and_drain_user(&self) -> usize {
        let mut state = self.state.lock().expect("mailbox poisoned");
        state.closed = true;
        state.system.clear();
        let drained = state.user.len();
        state.user.clear();
        self.ready.notify_all();
        drained
    }

    fn is_full(&self, state: &MailboxState<M>) -> bool {
        self.settings
            .user_capacity
            .is_some_and(|capacity| state.user.len() >= capacity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
