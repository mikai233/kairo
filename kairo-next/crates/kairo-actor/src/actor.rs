use crate::adapters::{self, AdapterScope};
use crate::asks::{self, AskError, AskScope};
use crate::error::{ActorError, ActorResult};
use crate::event_stream::EventStream;
use crate::path::ActorPath;
use crate::receive_timeout::{ReceiveTimeoutEnvelope, ReceiveTimeoutState};
use crate::receptionist::Receptionist;
use crate::refs::{ActorRef, AnyActorRef};
use crate::scheduler::Cancellable;
use crate::signal::Signal;
use crate::stash::StashState;
use crate::supervision::SupervisorStrategy;
use crate::system::ActorSystem;
use crate::tasks::{self, TaskHandle, TaskScope};
use crate::timers::{TimerEnvelope, TimerKey, TimerState};
use std::sync::Arc;
use std::time::Duration;

/// Stateful actor whose lifecycle and message turns run synchronously.
///
/// The runtime invokes at most one callback at a time for an actor instance.
/// Long-running work should return through messages instead of retaining
/// actor state outside the current callback.
pub trait Actor: Send + 'static {
    /// Protocol accepted by the actor's typed reference.
    type Msg: Send + 'static;

    /// Runs once after the actor instance is created and before user messages.
    fn started(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        Ok(())
    }

    /// Runs during normal stop and as the default pre-restart cleanup hook.
    fn stopped(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        Ok(())
    }

    /// Handles lifecycle and death-watch signals.
    ///
    /// The default implements death pact for unhandled termination signals.
    fn signal(&mut self, ctx: &mut Context<Self::Msg>, signal: Signal) -> ActorResult {
        match signal {
            Signal::PostStop | Signal::PreRestart => self.stopped(ctx),
            Signal::Terminated(actor) | Signal::ChildFailed { actor, .. } => {
                Err(ActorError::DeathPact {
                    actor: actor.path().to_string(),
                })
            }
        }
    }

    /// Processes one user message during a synchronous actor turn.
    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult;
}

/// Factory and runtime options used when spawning an actor.
pub struct Props<A> {
    builder: Option<Box<dyn FnOnce() -> A + Send>>,
    restart_builder: Option<Arc<dyn Fn() -> A + Send + Sync>>,
    supervisor: SupervisorStrategy,
    stash_capacity: Option<usize>,
}

impl<A: 'static> Props<A> {
    /// Creates one-shot props for an actor that cannot be recreated on restart.
    pub fn new<F>(builder: F) -> Self
    where
        F: FnOnce() -> A + Send + 'static,
    {
        Self {
            builder: Some(Box::new(builder)),
            restart_builder: None,
            supervisor: SupervisorStrategy::default(),
            stash_capacity: None,
        }
    }

    /// Creates props with a reusable factory and restart supervision by default.
    pub fn restartable<F>(builder: F) -> Self
    where
        F: Fn() -> A + Send + Sync + 'static,
    {
        let restart_builder: Arc<dyn Fn() -> A + Send + Sync> = Arc::new(builder);
        let initial_builder = Arc::clone(&restart_builder);
        Self {
            builder: Some(Box::new(move || initial_builder())),
            restart_builder: Some(restart_builder),
            supervisor: SupervisorStrategy::Restart,
            stash_capacity: None,
        }
    }

    /// Replaces the supervision strategy applied to actor failures.
    pub fn with_supervisor(mut self, supervisor: SupervisorStrategy) -> Self {
        self.supervisor = supervisor;
        self
    }

    /// Enables a bounded message stash for this actor.
    pub fn with_stash_capacity(mut self, capacity: usize) -> Self {
        self.stash_capacity = Some(capacity);
        self
    }

    /// Returns the configured supervision strategy.
    pub fn supervisor(&self) -> SupervisorStrategy {
        self.supervisor
    }

    pub(crate) fn build(&mut self) -> A {
        (self
            .builder
            .take()
            .expect("actor props may only build initial actor once"))()
    }

    pub(crate) fn restart(&self) -> Option<A> {
        self.restart_builder.as_ref().map(|builder| builder())
    }

    pub(crate) fn can_restart(&self) -> bool {
        self.restart_builder.is_some()
    }

    pub(crate) fn stash_capacity(&self) -> Option<usize> {
        self.stash_capacity
    }
}

#[derive(Debug)]
/// Capabilities and actor-system services available during an actor callback.
pub struct Context<M> {
    pub(crate) myself: ActorRef<M>,
    pub(crate) parent: ActorPath,
    pub(crate) system: ActorSystem,
    pub(crate) stop_requested: bool,
    pub(crate) timers: TimerState,
    pub(crate) receive_timeout: ReceiveTimeoutState<M>,
    pub(crate) stash: StashState<M>,
    pub(crate) tasks: TaskScope,
    pub(crate) asks: AskScope,
    pub(crate) adapters: AdapterScope,
}

impl<M: Send + 'static> Context<M> {
    /// Returns a typed reference to the current actor incarnation.
    pub fn myself(&self) -> ActorRef<M> {
        self.myself.clone()
    }

    /// Returns the owning actor system.
    pub fn system(&self) -> &ActorSystem {
        &self.system
    }

    /// Returns the message-type-erased parent reference.
    pub fn parent(&self) -> AnyActorRef {
        AnyActorRef::from_path(self.parent.clone())
    }

    /// Returns a snapshot of the actor's direct children.
    pub fn children(&self) -> Vec<AnyActorRef> {
        self.system.children_of(self.myself.path())
    }

    /// Looks up one direct child by logical name.
    pub fn child(&self, name: &str) -> Option<AnyActorRef> {
        self.system.child_of(self.myself.path(), name)
    }

    /// Returns the actor-system event stream.
    pub fn event_stream(&self) -> EventStream {
        self.system.event_stream()
    }

    /// Returns the actor-system local receptionist.
    pub fn receptionist(&self) -> Receptionist {
        self.system.receptionist()
    }

    /// Appends a message to this actor's configured stash.
    pub fn stash(&mut self, message: M) -> ActorResult {
        self.ensure_actor_active()?;
        self.stash.stash(message)
    }

    /// Prepends up to `limit` oldest stashed messages to the mailbox.
    pub fn unstash(&mut self, limit: usize) -> ActorResult {
        self.ensure_actor_active()?;
        let messages = self.stash.take(limit);
        self.prepend_stashed_messages(messages)
    }

    /// Prepends every stashed message to the mailbox in original order.
    pub fn unstash_all(&mut self) -> ActorResult {
        self.ensure_actor_active()?;
        self.drain_stash_to_mailbox()
    }

    pub(crate) fn drain_stash_to_mailbox(&mut self) -> ActorResult {
        let messages = self.stash.take_all();
        self.prepend_stashed_messages(messages)
    }

    fn prepend_stashed_messages(&self, messages: Vec<M>) -> ActorResult {
        if messages.is_empty() {
            return Ok(());
        }
        self.myself
            .prepend_user_messages(messages)
            .map_err(|messages| {
                ActorError::Message(format!("failed to unstash {} messages", messages.len()))
            })
    }

    /// Drops all currently stashed messages.
    pub fn clear_stash(&mut self) {
        self.stash.clear();
    }

    /// Returns the number of stashed messages.
    pub fn stash_len(&self) -> usize {
        self.stash.len()
    }

    /// Returns the stash capacity, or `None` when stashing is disabled.
    pub fn stash_capacity(&self) -> Option<usize> {
        self.stash.capacity()
    }

    /// Returns whether the configured stash is full.
    pub fn is_stash_full(&self) -> bool {
        self.stash.is_full()
    }

    /// Runs external work on the actor-system task executor.
    ///
    /// The task receives a scoped self reference whose sends are rejected after
    /// this actor stops or restarts.
    pub fn spawn_task<F>(&self, task: F) -> Result<TaskHandle, ActorError>
    where
        F: FnOnce(ActorRef<M>) + Send + 'static,
    {
        self.ensure_actor_active()?;
        tasks::spawn_task(
            self.system.task_executor(),
            self.tasks.scoped_ref(self.myself.clone()),
            task,
        )
    }

    /// Runs a fallible function externally and maps its result back to `Self::Msg`.
    pub fn pipe_to_self<T, E, F, Map>(&self, task: F, map: Map) -> Result<TaskHandle, ActorError>
    where
        T: Send + 'static,
        E: Send + 'static,
        F: FnOnce() -> Result<T, E> + Send + 'static,
        Map: FnOnce(Result<T, E>) -> M + Send + 'static,
    {
        self.ensure_actor_active()?;
        tasks::pipe_to_self(
            self.system.task_executor(),
            self.tasks.scoped_ref(self.myself.clone()),
            task,
            map,
        )
    }

    /// Creates a typed adapter that maps protocol `U` into this actor's protocol.
    pub fn message_adapter<U, F>(&self, map: F) -> Result<ActorRef<U>, ActorError>
    where
        U: Send + 'static,
        F: FnMut(U) -> M + Send + 'static,
    {
        self.ensure_actor_active()?;
        adapters::message_adapter(&self.system, &self.adapters, self.myself.clone(), map)
    }

    /// Sends a request containing a temporary reply ref and maps its result to self.
    pub fn ask<Req, Res, Create, Map>(
        &self,
        target: ActorRef<Req>,
        timeout: Duration,
        create_request: Create,
        map_response: Map,
    ) -> ActorResult
    where
        Req: Send + 'static,
        Res: Send + 'static,
        Create: FnOnce(ActorRef<Res>) -> Req,
        Map: FnOnce(Result<Res, AskError>) -> M + Send + 'static,
    {
        self.ensure_actor_active()?;
        asks::ask(
            &self.system,
            &self.asks,
            self.myself.clone(),
            target,
            timeout,
            create_request,
            map_response,
        )
    }

    /// Watches an actor and receives a lifecycle [`Signal`] when it terminates.
    pub fn watch<N: Send + 'static>(&mut self, actor: &ActorRef<N>) -> ActorResult {
        self.ensure_actor_active()?;
        self.system.watch(self.myself.clone(), actor.clone())
    }

    /// Watches an actor and enqueues `message` when it terminates.
    pub fn watch_with<N: Send + 'static>(
        &mut self,
        actor: &ActorRef<N>,
        message: M,
    ) -> ActorResult {
        self.ensure_actor_active()?;
        self.system
            .watch_with(self.myself.clone(), actor.clone(), message)
    }

    /// Removes this actor's death-watch registration for `actor`.
    pub fn unwatch<N: Send + 'static>(&mut self, actor: &ActorRef<N>) {
        self.system.unwatch(&self.myself, actor);
    }

    /// Schedules one message to any typed actor ref.
    pub fn schedule_once<N: Send + 'static>(
        &self,
        delay: Duration,
        target: ActorRef<N>,
        message: N,
    ) -> Cancellable {
        self.system.schedule_once(delay, target, message)
    }

    /// Schedules one message back to this actor.
    pub fn schedule_once_self(&self, delay: Duration, message: M) -> Cancellable {
        if self.ensure_actor_active().is_err() {
            return Cancellable::cancelled();
        }
        self.system
            .schedule_once(delay, self.myself.clone(), message)
    }

    /// Starts or replaces one keyed one-shot timer.
    pub fn start_single_timer(&mut self, key: impl Into<TimerKey>, delay: Duration, message: M) {
        if self.ensure_actor_active().is_err() {
            return;
        }
        let key = key.into();
        let generation = self.timers.next_generation();
        let cancellable = self.system.schedule_timer(
            delay,
            self.myself.clone(),
            key.as_str().to_string(),
            generation,
            message,
        );
        self.timers
            .start(key.as_str().to_string(), generation, false, cancellable);
    }

    /// Starts or replaces a keyed timer whose next delay begins after delivery.
    pub fn start_timer_with_fixed_delay(
        &mut self,
        key: impl Into<TimerKey>,
        initial_delay: Duration,
        delay: Duration,
        message: M,
    ) where
        M: Clone,
    {
        if self.ensure_actor_active().is_err() {
            return;
        }
        let key = key.into();
        if delay.is_zero() {
            self.timers.cancel(key.as_str());
            return;
        }
        let generation = self.timers.next_generation();
        let cancellable = self.system.schedule_timer_with_fixed_delay(
            initial_delay,
            delay,
            self.myself.clone(),
            key.as_str().to_string(),
            generation,
            message,
        );
        self.timers
            .start(key.as_str().to_string(), generation, true, cancellable);
    }

    /// Starts or replaces a keyed timer anchored to a fixed-rate cadence.
    pub fn start_timer_at_fixed_rate(
        &mut self,
        key: impl Into<TimerKey>,
        initial_delay: Duration,
        interval: Duration,
        message: M,
    ) where
        M: Clone,
    {
        if self.ensure_actor_active().is_err() {
            return;
        }
        let key = key.into();
        if interval.is_zero() {
            self.timers.cancel(key.as_str());
            return;
        }
        let generation = self.timers.next_generation();
        let cancellable = self.system.schedule_timer_at_fixed_rate(
            initial_delay,
            interval,
            self.myself.clone(),
            key.as_str().to_string(),
            generation,
            message,
        );
        self.timers
            .start(key.as_str().to_string(), generation, true, cancellable);
    }

    /// Cancels the active timer identified by `key`.
    pub fn cancel_timer(&mut self, key: impl AsRef<str>) {
        self.timers.cancel(key.as_ref());
    }

    /// Cancels all timers owned by this actor.
    pub fn cancel_all_timers(&mut self) {
        self.timers.cancel_all();
    }

    /// Returns whether a keyed timer is active.
    pub fn is_timer_active(&self, key: impl AsRef<str>) -> bool {
        self.timers.is_active(key.as_ref())
    }

    pub(crate) fn accept_timer(&mut self, timer: &TimerEnvelope<M>) -> bool {
        self.timers.accept(timer.key(), timer.generation())
    }

    /// Sets an inactivity timeout message, replacing any previous timeout.
    pub fn set_receive_timeout(&mut self, timeout: Duration, message: M)
    where
        M: Clone,
    {
        if self.ensure_actor_active().is_err() {
            self.receive_timeout.cancel();
            return;
        }
        self.receive_timeout.set(timeout, message);
    }

    /// Cancels the current receive timeout.
    pub fn cancel_receive_timeout(&mut self) {
        self.receive_timeout.cancel();
    }

    /// Returns the configured receive-timeout duration.
    pub fn receive_timeout(&self) -> Option<Duration> {
        self.receive_timeout.timeout()
    }

    pub(crate) fn before_influencing_message(&mut self) {
        self.receive_timeout.cancel_task();
    }

    pub(crate) fn after_influencing_message(&mut self) {
        if self.ensure_actor_active().is_err() {
            return;
        }
        self.receive_timeout
            .reschedule(&self.system, self.myself.clone());
    }

    pub(crate) fn accept_receive_timeout(&mut self, timeout: &ReceiveTimeoutEnvelope<M>) -> bool {
        self.receive_timeout.accept(timeout)
    }

    pub(crate) fn cancel_tasks(&mut self) {
        self.tasks.cancel_current();
    }

    pub(crate) fn cancel_asks(&mut self) {
        self.asks.cancel_current();
    }

    pub(crate) fn stop_adapters(&mut self) -> Vec<ActorPath> {
        self.adapters.stop_all()
    }

    /// Spawns a named direct child actor.
    pub fn spawn<A>(
        &self,
        name: impl AsRef<str>,
        props: Props<A>,
    ) -> Result<ActorRef<A::Msg>, ActorError>
    where
        A: Actor,
    {
        self.ensure_can_spawn_child()?;
        self.system
            .spawn_under(self.myself.path(), name.as_ref(), props)
    }

    /// Spawns a direct child with a runtime-generated reserved name.
    pub fn spawn_anonymous<A>(&self, props: Props<A>) -> Result<ActorRef<A::Msg>, ActorError>
    where
        A: Actor,
    {
        self.ensure_can_spawn_child()?;
        self.system.spawn_anonymous_under(self.myself.path(), props)
    }

    fn ensure_can_spawn_child(&self) -> Result<(), ActorError> {
        self.ensure_actor_active()
    }

    fn ensure_actor_active(&self) -> Result<(), ActorError> {
        if self.stop_requested || self.myself.is_stopped() {
            return Err(ActorError::ActorStopping {
                actor: self.myself.path().to_string(),
            });
        }
        Ok(())
    }

    /// Requests stop for this actor or one of its direct children.
    pub fn stop<N: Send + 'static>(&mut self, actor: ActorRef<N>) -> ActorResult {
        if actor.path() == self.myself.path() {
            self.stop_requested = true;
            actor.request_stop();
            Ok(())
        } else if self.system.is_child_of(self.myself.path(), actor.path()) {
            actor.request_stop();
            Ok(())
        } else {
            Err(ActorError::InvalidStopTarget {
                actor: actor.path().to_string(),
                owner: self.myself.path().to_string(),
            })
        }
    }
}
