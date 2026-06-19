use std::fmt::{self, Formatter};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

mod lifecycle;

use crate::dead_letters::DeadLetters;
use crate::error::SendError;
use crate::mailbox::{Mailbox, SystemMessage};
use crate::path::ActorPath;
use crate::receive_timeout::ReceiveTimeoutEnvelope;
use crate::signal::Signal;
use crate::supervision::SupervisionFailure;
use crate::timers::TimerEnvelope;

pub(crate) use lifecycle::{LocalActorHandle, TerminationLatch};

pub trait Recipient<M: Send + 'static> {
    fn tell(&self, message: M) -> Result<(), SendError<M>>;
}

pub struct ActorRef<M> {
    pub(crate) path: ActorPath,
    pub(crate) target: ActorRefTarget<M>,
    pub(crate) dead_letters: DeadLetters,
    _message: PhantomData<fn(M)>,
}

type AdapterSend<M> = Arc<dyn Fn(M) -> Result<(), SendError<M>> + Send + Sync>;

pub(crate) struct ActorRefTarget<M> {
    pub(crate) mailbox: Option<Arc<Mailbox<M>>>,
    pub(crate) adapter: Option<AdapterSend<M>>,
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
                adapter: None,
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
                adapter: None,
                stopped: Arc::new(AtomicBool::new(true)),
                terminated,
                stopped_reason: "actor does not exist",
            },
            dead_letters,
            _message: PhantomData,
        }
    }

    pub(crate) fn adapter<N, F>(
        path: ActorPath,
        owner: ActorRef<N>,
        stopped: Arc<AtomicBool>,
        terminated: Arc<TerminationLatch>,
        map: F,
    ) -> Self
    where
        M: Send + 'static,
        N: Send + 'static,
        F: FnMut(M) -> N + Send + 'static,
    {
        let mailbox = owner
            .target
            .mailbox
            .clone()
            .expect("message adapters require a live local owner mailbox");
        let owner_stopped = Arc::clone(&owner.target.stopped);
        let map = Arc::new(Mutex::new(map));
        let adapter_path = path.clone();
        let dead_letters = owner.dead_letters.clone();
        let adapter_dead_letters = dead_letters.clone();
        let adapter_stopped = Arc::clone(&stopped);
        let adapter = Arc::new(move |message: M| {
            if owner_stopped.load(Ordering::Acquire) || adapter_stopped.load(Ordering::Acquire) {
                adapter_dead_letters.publish::<M>(adapter_path.clone(), "actor is stopped");
                return Err(SendError {
                    message,
                    reason: "actor is stopped".to_string(),
                });
            }

            let map = Arc::clone(&map);
            let stopped = Arc::clone(&adapter_stopped);
            mailbox
                .enqueue_adapted(message, move |message| {
                    if stopped.load(Ordering::Acquire) {
                        return None;
                    }
                    let mut map = map.lock().expect("message adapter poisoned");
                    Some((map)(message))
                })
                .map_err(|error| {
                    let reason = error.reason();
                    adapter_dead_letters.publish::<M>(adapter_path.clone(), reason);
                    SendError {
                        message: error.into_message(),
                        reason: reason.to_string(),
                    }
                })
        });
        Self {
            path,
            target: ActorRefTarget {
                mailbox: None,
                adapter: Some(adapter),
                stopped,
                terminated,
                stopped_reason: "actor is stopped",
            },
            dead_letters,
            _message: PhantomData,
        }
    }

    pub(crate) fn function<F>(
        path: ActorPath,
        dead_letters: DeadLetters,
        stopped: Arc<AtomicBool>,
        terminated: Arc<TerminationLatch>,
        stopped_reason: &'static str,
        send: F,
    ) -> Self
    where
        M: Send + 'static,
        F: Fn(M) -> Result<(), SendError<M>> + Send + Sync + 'static,
    {
        Self {
            path,
            target: ActorRefTarget {
                mailbox: None,
                adapter: Some(Arc::new(send)),
                stopped,
                terminated,
                stopped_reason,
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

        if let Some(adapter) = &self.target.adapter {
            return adapter(message);
        }

        let Some(mailbox) = &self.target.mailbox else {
            self.dead_letters
                .publish::<M>(self.path.clone(), "actor does not exist");
            return Err(SendError {
                message,
                reason: "actor does not exist".to_string(),
            });
        };

        mailbox.enqueue_user(message).map_err(|error| {
            if error.is_closed() {
                self.target.stopped.store(true, Ordering::Release);
            }
            let reason = error.reason();
            self.dead_letters.publish::<M>(self.path.clone(), reason);
            SendError {
                message: error.into_message(),
                reason: reason.to_string(),
            }
        })
    }

    pub(crate) fn prepend_user_messages(&self, messages: Vec<M>) -> Result<(), Vec<M>> {
        let Some(mailbox) = &self.target.mailbox else {
            return Err(messages);
        };
        mailbox.prepend_user_messages(messages)
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

    pub(crate) fn send_gated_system_signal(&self, signal: Signal) {
        if !self.target.stopped.load(Ordering::Acquire)
            && let Some(mailbox) = &self.target.mailbox
        {
            mailbox.enqueue_system(SystemMessage::GatedSignal(signal));
        }
    }

    pub(crate) fn send_timer(&self, timer: TimerEnvelope<M>) {
        if self.target.stopped.load(Ordering::Acquire) {
            self.dead_letters
                .publish::<M>(self.path.clone(), self.target.stopped_reason);
            return;
        }

        let Some(mailbox) = &self.target.mailbox else {
            self.dead_letters
                .publish::<M>(self.path.clone(), "actor does not exist");
            return;
        };

        if let Err(error) = mailbox.enqueue_timer(timer) {
            if error.is_closed() {
                self.target.stopped.store(true, Ordering::Release);
            }
            self.dead_letters
                .publish::<M>(self.path.clone(), error.reason());
        }
    }

    pub(crate) fn send_receive_timeout(&self, timeout: ReceiveTimeoutEnvelope<M>) {
        if self.target.stopped.load(Ordering::Acquire) {
            self.dead_letters
                .publish::<M>(self.path.clone(), self.target.stopped_reason);
            return;
        }

        let Some(mailbox) = &self.target.mailbox else {
            self.dead_letters
                .publish::<M>(self.path.clone(), "actor does not exist");
            return;
        };

        if let Err(error) = mailbox.enqueue_receive_timeout(timeout) {
            if error.is_closed() {
                self.target.stopped.store(true, Ordering::Release);
            }
            self.dead_letters
                .publish::<M>(self.path.clone(), error.reason());
        }
    }

    pub(crate) fn request_stop(&self) {
        if self.target.adapter.is_some() {
            if !self.target.stopped.swap(true, Ordering::AcqRel) {
                self.target.terminated.mark_stopped();
            }
            return;
        }

        if !self.target.stopped.swap(true, Ordering::AcqRel) {
            if let Some(mailbox) = &self.target.mailbox {
                mailbox.enqueue_system(SystemMessage::Stop);
            } else {
                self.target.terminated.mark_stopped();
            }
        }
    }

    pub(crate) fn request_restart(&self) {
        if !self.target.stopped.load(Ordering::Acquire)
            && let Some(mailbox) = &self.target.mailbox
        {
            mailbox.enqueue_system(SystemMessage::Restart);
        }
    }

    pub(crate) fn to_local_handle(&self, restartable: bool) -> LocalActorHandle {
        let actor = self.clone();
        let restart_actor = self.clone();
        let supervisor_actor = self.clone();
        LocalActorHandle {
            path: self.path.clone(),
            terminated: Arc::clone(&self.target.terminated),
            stop: Arc::new(move || actor.request_stop()),
            restart: Arc::new(move || {
                if restartable {
                    restart_actor.request_restart();
                }
            }),
            supervise: Arc::new(move |failure| supervisor_actor.request_supervision(failure)),
        }
    }

    fn request_supervision(&self, failure: SupervisionFailure) {
        if !self.target.stopped.load(Ordering::Acquire)
            && let Some(mailbox) = &self.target.mailbox
        {
            mailbox.enqueue_system(SystemMessage::SupervisionFailure(failure));
        }
    }
}

impl<M> Clone for ActorRefTarget<M> {
    fn clone(&self) -> Self {
        Self {
            mailbox: self.mailbox.clone(),
            adapter: self.adapter.clone(),
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
