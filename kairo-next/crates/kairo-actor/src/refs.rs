use std::fmt::{self, Formatter};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::dead_letters::DeadLetters;
use crate::error::SendError;
use crate::mailbox::{Mailbox, SystemMessage};
use crate::path::ActorPath;
use crate::signal::Signal;

pub trait Recipient<M: Send + 'static> {
    fn tell(&self, message: M) -> Result<(), SendError<M>>;
}

pub struct ActorRef<M> {
    pub(crate) path: ActorPath,
    pub(crate) target: ActorRefTarget<M>,
    pub(crate) dead_letters: DeadLetters,
    _message: PhantomData<fn(M)>,
}

pub(crate) struct ActorRefTarget<M> {
    pub(crate) mailbox: Option<Arc<Mailbox<M>>>,
    pub(crate) stopped: Arc<AtomicBool>,
    pub(crate) terminated: Arc<TerminationLatch>,
    stopped_reason: &'static str,
}

impl<M> ActorRef<M> {
    pub(crate) fn new(
        path: ActorPath,
        mailbox: Arc<Mailbox<M>>,
        stopped: Arc<AtomicBool>,
        terminated: Arc<TerminationLatch>,
        dead_letters: DeadLetters,
    ) -> Self {
        Self {
            path,
            target: ActorRefTarget {
                mailbox: Some(mailbox),
                stopped,
                terminated,
                stopped_reason: "actor is stopped",
            },
            dead_letters,
            _message: PhantomData,
        }
    }

    pub(crate) fn missing(path: ActorPath, dead_letters: DeadLetters) -> Self {
        let terminated = Arc::new(TerminationLatch::default());
        terminated.mark_stopped();
        Self {
            path,
            target: ActorRefTarget {
                mailbox: None,
                stopped: Arc::new(AtomicBool::new(true)),
                terminated,
                stopped_reason: "actor does not exist",
            },
            dead_letters,
            _message: PhantomData,
        }
    }
}

impl<M> Clone for ActorRef<M> {
    fn clone(&self) -> Self {
        Self {
            path: self.path.clone(),
            target: self.target.clone(),
            dead_letters: self.dead_letters.clone(),
            _message: PhantomData,
        }
    }
}

impl<M> fmt::Debug for ActorRef<M> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ActorRef")
            .field("path", &self.path)
            .field("stopped", &self.target.stopped.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

impl<M: Send + 'static> ActorRef<M> {
    pub fn path(&self) -> &ActorPath {
        &self.path
    }

    pub fn tell(&self, message: M) -> Result<(), SendError<M>> {
        if self.target.stopped.load(Ordering::Acquire) {
            self.dead_letters
                .publish::<M>(self.path.clone(), self.target.stopped_reason);
            return Err(SendError {
                message,
                reason: self.target.stopped_reason.to_string(),
            });
        }

        let Some(mailbox) = &self.target.mailbox else {
            self.dead_letters
                .publish::<M>(self.path.clone(), "actor does not exist");
            return Err(SendError {
                message,
                reason: "actor does not exist".to_string(),
            });
        };

        mailbox.enqueue_user(message).map_err(|message| {
            self.target.stopped.store(true, Ordering::Release);
            self.dead_letters
                .publish::<M>(self.path.clone(), "actor mailbox is closed");
            SendError {
                message,
                reason: "actor mailbox is closed".to_string(),
            }
        })
    }

    pub fn as_any(&self) -> AnyActorRef {
        AnyActorRef {
            path: self.path.clone(),
        }
    }

    pub fn is_stopped(&self) -> bool {
        self.target.stopped.load(Ordering::Acquire)
    }

    pub fn wait_for_stop(&self, timeout: Duration) -> bool {
        self.target.terminated.wait(timeout)
    }

    pub(crate) fn is_terminated(&self) -> bool {
        self.target.terminated.is_stopped()
    }

    pub(crate) fn send_system_signal(&self, signal: Signal) {
        if !self.target.stopped.load(Ordering::Acquire)
            && let Some(mailbox) = &self.target.mailbox
        {
            mailbox.enqueue_system(SystemMessage::Signal(signal));
        }
    }

    pub(crate) fn request_stop(&self) {
        if !self.target.stopped.swap(true, Ordering::AcqRel) {
            if let Some(mailbox) = &self.target.mailbox {
                mailbox.enqueue_system(SystemMessage::Stop);
            } else {
                self.target.terminated.mark_stopped();
            }
        }
    }

    pub(crate) fn to_local_handle(&self) -> LocalActorHandle {
        let actor = self.clone();
        LocalActorHandle {
            path: self.path.clone(),
            terminated: Arc::clone(&self.target.terminated),
            stop: Arc::new(move || actor.request_stop()),
        }
    }
}

impl<M> Clone for ActorRefTarget<M> {
    fn clone(&self) -> Self {
        Self {
            mailbox: self.mailbox.clone(),
            stopped: Arc::clone(&self.stopped),
            terminated: Arc::clone(&self.terminated),
            stopped_reason: self.stopped_reason,
        }
    }
}

impl<M: Send + 'static> Recipient<M> for ActorRef<M> {
    fn tell(&self, message: M) -> Result<(), SendError<M>> {
        ActorRef::tell(self, message)
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct AnyActorRef {
    path: ActorPath,
}

impl AnyActorRef {
    pub(crate) fn from_path(path: ActorPath) -> Self {
        Self { path }
    }

    pub fn path(&self) -> &ActorPath {
        &self.path
    }
}

impl fmt::Debug for AnyActorRef {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("AnyActorRef")
            .field("path", &self.path)
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct IgnoreRef<M> {
    path: ActorPath,
    _message: PhantomData<fn(M)>,
}

impl<M> IgnoreRef<M> {
    pub fn new() -> Self {
        Self {
            path: ActorPath::new("kairo://local/ignore"),
            _message: PhantomData,
        }
    }

    pub fn path(&self) -> &ActorPath {
        &self.path
    }
}

impl<M> Default for IgnoreRef<M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<M: Send + 'static> Recipient<M> for IgnoreRef<M> {
    fn tell(&self, _message: M) -> Result<(), SendError<M>> {
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct LocalActorHandle {
    path: ActorPath,
    terminated: Arc<TerminationLatch>,
    stop: Arc<dyn Fn() + Send + Sync>,
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

    pub(crate) fn wait_for_stop(&self, timeout: Duration) -> bool {
        self.terminated.wait(timeout)
    }
}

#[derive(Debug, Default)]
pub(crate) struct TerminationLatch {
    stopped: Mutex<bool>,
    changed: Condvar,
}

impl TerminationLatch {
    pub(crate) fn mark_stopped(&self) {
        let mut stopped = self.stopped.lock().expect("termination latch poisoned");
        *stopped = true;
        self.changed.notify_all();
    }

    fn is_stopped(&self) -> bool {
        *self.stopped.lock().expect("termination latch poisoned")
    }

    fn wait(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut stopped = self.stopped.lock().expect("termination latch poisoned");
        while !*stopped {
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                return false;
            };
            let (next_stopped, wait) = self
                .changed
                .wait_timeout(stopped, remaining)
                .expect("termination latch poisoned");
            stopped = next_stopped;
            if wait.timed_out() && !*stopped {
                return false;
            }
        }
        true
    }
}
