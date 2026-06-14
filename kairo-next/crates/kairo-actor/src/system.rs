use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

mod builder;
mod spawn;

use crate::coordinated_shutdown::CoordinatedShutdown;
use crate::dead_letters::DeadLetters;
use crate::death_watch::{
    DeathWatchKind, DeathWatchRegistration, DeathWatchRegistry, TerminationCause,
};
use crate::dispatcher::DispatcherSettings;
use crate::error::ActorError;
use crate::event_stream::EventStream;
use crate::mailbox::MailboxSettings;
use crate::path::{ActorPath, Address};
use crate::provider::LocalActorRefProvider;
use crate::receive_timeout::ReceiveTimeoutEnvelope;
use crate::receptionist::Receptionist;
use crate::refs::{ActorRef, AnyActorRef};
use crate::registry::ActorRegistry;
use crate::runtime::stop_children_with_timeout;
use crate::scheduler::{Cancellable, Scheduler};
use crate::signal::Signal;

pub use builder::ActorSystemBuilder;

#[derive(Debug, Clone)]
pub struct ActorSystem {
    name: String,
    address: Address,
    inner: Arc<ActorSystemInner>,
}

#[derive(Debug, Default)]
pub(crate) struct ActorSystemInner {
    pub(crate) next_uid: AtomicU64,
    pub(crate) next_anonymous: AtomicU64,
    pub(crate) next_temp: AtomicU64,
    pub(crate) terminating: AtomicBool,
    pub(crate) terminated: AtomicBool,
    pub(crate) registry: ActorRegistry,
    pub(crate) death_watch: DeathWatchRegistry,
    pub(crate) dispatcher: DispatcherSettings,
    pub(crate) mailbox: MailboxSettings,
    pub(crate) scheduler: Scheduler,
    pub(crate) event_stream: EventStream,
    pub(crate) receptionist: Receptionist,
    pub(crate) coordinated_shutdown: CoordinatedShutdown,
    pub(crate) dead_letters: DeadLetters,
}

impl ActorSystem {
    pub fn builder(name: impl Into<String>) -> ActorSystemBuilder {
        ActorSystemBuilder::new(name)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn address(&self) -> &Address {
        &self.address
    }

    pub fn dead_letters(&self) -> DeadLetters {
        self.inner.dead_letters.clone()
    }

    pub fn dispatcher_settings(&self) -> DispatcherSettings {
        self.inner.dispatcher
    }

    pub fn mailbox_settings(&self) -> MailboxSettings {
        self.inner.mailbox
    }

    pub fn event_stream(&self) -> EventStream {
        self.inner.event_stream.clone()
    }

    pub fn receptionist(&self) -> Receptionist {
        self.inner.receptionist.clone()
    }

    pub fn coordinated_shutdown(&self) -> CoordinatedShutdown {
        self.inner.coordinated_shutdown.clone()
    }

    pub fn provider(&self) -> LocalActorRefProvider {
        LocalActorRefProvider::new(self.clone())
    }

    pub fn run_coordinated_shutdown(
        &self,
        reason: impl Into<String>,
        termination_timeout: Duration,
    ) -> Result<(), ActorError> {
        self.coordinated_shutdown().run(reason)?;
        self.terminate(termination_timeout)
    }

    pub fn schedule_once<M>(&self, delay: Duration, target: ActorRef<M>, message: M) -> Cancellable
    where
        M: Send + 'static,
    {
        self.inner.scheduler.schedule_once(delay, target, message)
    }

    pub(crate) fn schedule_action(
        &self,
        delay: Duration,
        action: impl FnOnce() + Send + 'static,
    ) -> Cancellable {
        self.inner.scheduler.schedule_action(delay, action)
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
        self.inner
            .scheduler
            .schedule_timer(delay, target, key, generation, message)
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
        self.inner
            .scheduler
            .schedule_receive_timeout(delay, target, timeout)
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
        self.inner.scheduler.schedule_timer_with_fixed_delay(
            initial_delay,
            delay,
            target,
            key,
            generation,
            message,
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
        self.inner.scheduler.schedule_timer_at_fixed_rate(
            initial_delay,
            interval,
            target,
            key,
            generation,
            message,
        )
    }

    pub fn stop<M: Send + 'static>(&self, actor: &ActorRef<M>) {
        actor.request_stop();
    }

    pub fn is_terminating(&self) -> bool {
        self.inner.terminating.load(Ordering::Acquire)
    }

    pub fn is_terminated(&self) -> bool {
        self.inner.terminated.load(Ordering::Acquire)
    }

    pub fn terminate(&self, timeout: Duration) -> Result<(), ActorError> {
        self.inner.terminating.store(true, Ordering::Release);
        let user_root = self.user_root_path();
        stop_children_with_timeout(&self.inner, user_root.as_str(), timeout)?;
        let system_root = self.system_root_path();
        stop_children_with_timeout(&self.inner, system_root.as_str(), timeout)?;
        self.inner.terminated.store(true, Ordering::Release);
        Ok(())
    }

    pub fn missing_ref<M>(&self, path: impl Into<String>) -> ActorRef<M> {
        ActorRef::missing(ActorPath::new(path), self.inner.dead_letters.clone())
    }

    pub fn resolve_local<M>(&self, path: impl AsRef<str>) -> Option<ActorRef<M>>
    where
        M: Send + 'static,
    {
        self.inner.registry.resolve_ref(path.as_ref())
    }

    pub fn resolve_local_or_missing<M>(&self, path: impl Into<String>) -> ActorRef<M>
    where
        M: Send + 'static,
    {
        let path = path.into();
        self.resolve_local(&path)
            .unwrap_or_else(|| self.missing_ref(path))
    }

    pub(crate) fn has_local_actor(&self, path: &ActorPath) -> bool {
        self.inner.registry.handle_of(path).is_some() || self.inner.registry.contains_ref(path)
    }

    pub(crate) fn register_temp_ref<M>(&self, actor: ActorRef<M>)
    where
        M: Send + 'static,
    {
        self.inner.registry.add_ref(actor);
    }

    pub(crate) fn unregister_temp_ref(&self, path: &ActorPath) {
        self.inner.registry.remove_ref(path);
    }

    pub(crate) fn children_of(&self, parent_path: &ActorPath) -> Vec<AnyActorRef> {
        self.inner.registry.children_of(parent_path)
    }

    pub(crate) fn child_of(&self, parent_path: &ActorPath, name: &str) -> Option<AnyActorRef> {
        self.inner.registry.child_of(parent_path, name)
    }

    pub(crate) fn is_child_of(&self, parent_path: &ActorPath, child_path: &ActorPath) -> bool {
        self.inner.registry.is_child_of(parent_path, child_path)
    }

    pub(crate) fn next_adapter_path(
        &self,
        owner_path: &ActorPath,
    ) -> Result<ActorPath, ActorError> {
        if self.is_terminating() {
            return Err(ActorError::SystemTerminating);
        }
        let id = self.inner.next_anonymous.fetch_add(1, Ordering::Relaxed);
        Ok(owner_path.child(format!("$adapter-{id}"), Some(id)))
    }

    pub(crate) fn next_ask_path(&self) -> Result<ActorPath, ActorError> {
        if self.is_terminating() {
            return Err(ActorError::SystemTerminating);
        }
        Ok(self.next_temp_path("ask"))
    }

    pub(crate) fn next_temp_path(&self, prefix: &str) -> ActorPath {
        let id = self.inner.next_temp.fetch_add(1, Ordering::Relaxed);
        let name = if prefix.is_empty() {
            format!("${id}")
        } else {
            format!("{prefix}${id}")
        };
        self.temp_root_path().child(name, Some(id))
    }

    pub(crate) fn watch<M, N>(
        &self,
        watcher: ActorRef<M>,
        subject: ActorRef<N>,
    ) -> Result<(), ActorError>
    where
        M: Send + 'static,
        N: Send + 'static,
    {
        if watcher.path() == subject.path() {
            return Err(ActorError::InvalidWatchTarget {
                actor: watcher.path().to_string(),
            });
        }
        let subject_ref = subject.as_any();
        let subject_parent = subject.path().parent();
        let watcher_path = watcher.path().clone();
        let registration = DeathWatchRegistration::new(
            watcher_path.clone(),
            DeathWatchKind::Signal,
            move |cause| {
                if let TerminationCause::Failed(reason) = cause
                    && subject_parent.as_ref() == Some(&watcher_path)
                {
                    watcher.send_system_signal(Signal::ChildFailed {
                        actor: subject_ref.clone(),
                        reason,
                    });
                } else {
                    watcher.send_system_signal(Signal::Terminated(subject_ref));
                }
            },
        );
        self.watch_registered(subject, registration)
    }

    pub fn watch_with<M, N>(
        &self,
        watcher: ActorRef<M>,
        subject: ActorRef<N>,
        message: M,
    ) -> Result<(), ActorError>
    where
        M: Send + 'static,
        N: Send + 'static,
    {
        if watcher.path() == subject.path() {
            return Err(ActorError::InvalidWatchTarget {
                actor: watcher.path().to_string(),
            });
        }
        let registration = DeathWatchRegistration::new(
            watcher.path().clone(),
            DeathWatchKind::Custom,
            move |_| {
                let _ = watcher.tell(message);
            },
        );
        self.watch_registered(subject, registration)
    }

    pub(crate) fn unwatch(&self, watcher: &ActorPath, subject: &ActorPath) {
        self.inner.death_watch.unwatch(subject, watcher);
    }

    pub(crate) fn root_path(&self) -> ActorPath {
        ActorPath::new(self.address.to_string())
    }

    pub(crate) fn user_root_path(&self) -> ActorPath {
        ActorPath::root(self.address.clone(), "user")
    }

    pub(crate) fn system_root_path(&self) -> ActorPath {
        ActorPath::root(self.address.clone(), "system")
    }

    pub(crate) fn temp_root_path(&self) -> ActorPath {
        ActorPath::root(self.address.clone(), "temp")
    }

    pub(crate) fn dead_letters_path(&self) -> ActorPath {
        ActorPath::root(self.address.clone(), "deadLetters")
    }

    fn watch_registered<N>(
        &self,
        subject: ActorRef<N>,
        registration: DeathWatchRegistration,
    ) -> Result<(), ActorError>
    where
        N: Send + 'static,
    {
        if subject.is_terminated() {
            registration.notify(TerminationCause::Stopped);
            return Ok(());
        }

        self.inner
            .death_watch
            .watch(subject.path().clone(), registration)?;
        if subject.is_terminated() {
            self.inner
                .death_watch
                .notify(subject.path(), TerminationCause::Stopped);
        }
        Ok(())
    }
}
