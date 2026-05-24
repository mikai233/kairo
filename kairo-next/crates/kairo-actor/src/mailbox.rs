use std::collections::VecDeque;
use std::sync::{Condvar, Mutex};

use crate::signal::Signal;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SystemMessage {
    Stop,
    Signal(Signal),
}

#[derive(Debug)]
pub(crate) enum Dequeued<M> {
    System(SystemMessage),
    User(M),
    Closed,
}

#[derive(Debug)]
pub(crate) struct Mailbox<M> {
    state: Mutex<MailboxState<M>>,
    ready: Condvar,
}

#[derive(Debug)]
struct MailboxState<M> {
    system: VecDeque<SystemMessage>,
    user: VecDeque<M>,
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
        state.user.push_back(message);
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

    pub(crate) fn close_and_drain_user(&self) -> Vec<M> {
        let mut state = self.state.lock().expect("mailbox poisoned");
        state.closed = true;
        state.system.clear();
        let messages = state.user.drain(..).collect();
        self.ready.notify_all();
        messages
    }
}
