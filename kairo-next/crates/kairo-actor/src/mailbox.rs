use std::collections::VecDeque;
use std::fmt;
use std::sync::{Condvar, Mutex};

use crate::signal::Signal;
use crate::timers::TimerEnvelope;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SystemMessage {
    Stop,
    Signal(Signal),
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
    Adapted(Box<dyn FnOnce() -> M + Send>),
}

impl<M: fmt::Debug> fmt::Debug for UserEnvelope<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Message(message) => f.debug_tuple("Message").field(message).finish(),
            Self::Timer(timer) => f.debug_tuple("Timer").field(timer).finish(),
            Self::Adapted(_) => f.debug_tuple("Adapted").finish_non_exhaustive(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct Mailbox<M> {
    state: Mutex<MailboxState<M>>,
    ready: Condvar,
}

#[derive(Debug)]
struct MailboxState<M> {
    system: VecDeque<SystemMessage>,
    user: VecDeque<UserEnvelope<M>>,
    closed: bool,
}

impl<M> Default for Mailbox<M> {
    fn default() -> Self {
        Self {
            state: Mutex::new(MailboxState {
                system: VecDeque::new(),
                user: VecDeque::new(),
                closed: false,
            }),
            ready: Condvar::new(),
        }
    }
}

impl<M> Mailbox<M> {
    pub(crate) fn enqueue_user(&self, message: M) -> Result<(), M> {
        let mut state = self.state.lock().expect("mailbox poisoned");
        if state.closed {
            return Err(message);
        }
        state.user.push_back(UserEnvelope::Message(message));
        self.ready.notify_one();
        Ok(())
    }

    pub(crate) fn enqueue_timer(&self, timer: TimerEnvelope<M>) -> Result<(), TimerEnvelope<M>> {
        let mut state = self.state.lock().expect("mailbox poisoned");
        if state.closed {
            return Err(timer);
        }
        state.user.push_back(UserEnvelope::Timer(timer));
        self.ready.notify_one();
        Ok(())
    }

    pub(crate) fn enqueue_adapted<U, F>(&self, message: U, adapt: F) -> Result<(), U>
    where
        U: Send + 'static,
        F: FnOnce(U) -> M + Send + 'static,
    {
        let mut state = self.state.lock().expect("mailbox poisoned");
        if state.closed {
            return Err(message);
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
}
