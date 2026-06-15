use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::refs::ActorRef;
use crate::scheduler::Cancellable;
use crate::system::ActorSystem;

pub(crate) struct ReceiveTimeoutEnvelope<M> {
    generation: u64,
    message: M,
}

impl<M> ReceiveTimeoutEnvelope<M> {
    pub(crate) fn new(generation: u64, message: M) -> Self {
        Self {
            generation,
            message,
        }
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) fn into_message(self) -> M {
        self.message
    }
}

impl<M: fmt::Debug> fmt::Debug for ReceiveTimeoutEnvelope<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReceiveTimeoutEnvelope")
            .field("generation", &self.generation)
            .field("message", &self.message)
            .finish()
    }
}

pub(crate) struct ReceiveTimeoutState<M> {
    timeout: Option<Duration>,
    message_factory: Option<Arc<dyn Fn() -> M + Send + Sync>>,
    generation: u64,
    cancellable: Option<Cancellable>,
}

impl<M> Default for ReceiveTimeoutState<M> {
    fn default() -> Self {
        Self {
            timeout: None,
            message_factory: None,
            generation: 0,
            cancellable: None,
        }
    }
}

impl<M> fmt::Debug for ReceiveTimeoutState<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReceiveTimeoutState")
            .field("timeout", &self.timeout)
            .field("generation", &self.generation)
            .field("has_message", &self.message_factory.is_some())
            .field("has_cancellable", &self.cancellable.is_some())
            .finish()
    }
}

impl<M> ReceiveTimeoutState<M>
where
    M: Send + 'static,
{
    pub(crate) fn set(&mut self, timeout: Duration, message: M)
    where
        M: Clone,
    {
        let message = Arc::new(Mutex::new(message));
        self.timeout = Some(timeout);
        self.message_factory = Some(Arc::new(move || {
            message
                .lock()
                .expect("receive timeout message poisoned")
                .clone()
        }));
        self.generation = self.generation.wrapping_add(1);
        self.cancel_task();
    }

    pub(crate) fn reschedule(&mut self, system: &ActorSystem, target: ActorRef<M>) {
        self.cancel_task();
        let (Some(timeout), Some(message_factory)) = (self.timeout, self.message_factory.as_ref())
        else {
            return;
        };
        self.generation = self.generation.wrapping_add(1);
        let generation = self.generation;
        let message = message_factory();
        self.cancellable = Some(system.schedule_receive_timeout(
            timeout,
            target,
            ReceiveTimeoutEnvelope::new(generation, message),
        ));
    }
}

impl<M> ReceiveTimeoutState<M> {
    pub(crate) fn timeout(&self) -> Option<Duration> {
        self.timeout
    }

    pub(crate) fn cancel(&mut self) {
        self.timeout = None;
        self.message_factory = None;
        self.generation = self.generation.wrapping_add(1);
        self.cancel_task();
    }

    pub(crate) fn cancel_task(&mut self) {
        if let Some(cancellable) = self.cancellable.take() {
            cancellable.cancel();
        }
    }

    pub(crate) fn accept(&mut self, envelope: &ReceiveTimeoutEnvelope<M>) -> bool {
        let accepted = self.timeout.is_some() && envelope.generation() == self.generation;
        if accepted {
            self.cancellable = None;
        }
        accepted
    }
}
