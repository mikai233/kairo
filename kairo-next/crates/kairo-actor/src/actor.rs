use crate::adapters;
use crate::asks::{self, AskError};
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

pub trait Actor: Send + 'static {
    type Msg: Send + 'static;

    fn started(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        Ok(())
    }

    fn stopped(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        Ok(())
    }

    fn signal(&mut self, ctx: &mut Context<Self::Msg>, signal: Signal) -> ActorResult {
        match signal {
            Signal::PostStop => self.stopped(ctx),
            Signal::PreRestart | Signal::Terminated(_) | Signal::ChildFailed { .. } => Ok(()),
        }
    }

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult;
}

pub struct Props<A> {
    builder: Option<Box<dyn FnOnce() -> A + Send>>,
    restart_builder: Option<Arc<dyn Fn() -> A + Send + Sync>>,
    supervisor: SupervisorStrategy,
    stash_capacity: Option<usize>,
}

impl<A: 'static> Props<A> {
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

    pub fn with_supervisor(mut self, supervisor: SupervisorStrategy) -> Self {
        self.supervisor = supervisor;
        self
    }

    pub fn with_stash_capacity(mut self, capacity: usize) -> Self {
        self.stash_capacity = Some(capacity);
        self
    }

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

    pub(crate) fn stash_capacity(&self) -> Option<usize> {
        self.stash_capacity
    }
}

#[derive(Debug)]
pub struct Context<M> {
    pub(crate) myself: ActorRef<M>,
    pub(crate) parent: ActorPath,
    pub(crate) system: ActorSystem,
    pub(crate) stop_requested: bool,
    pub(crate) timers: TimerState,
    pub(crate) receive_timeout: ReceiveTimeoutState<M>,
    pub(crate) stash: StashState<M>,
    pub(crate) tasks: TaskScope,
}

impl<M: Send + 'static> Context<M> {
    pub fn myself(&self) -> ActorRef<M> {
        self.myself.clone()
    }

    pub fn system(&self) -> &ActorSystem {
        &self.system
    }

    pub fn parent(&self) -> AnyActorRef {
        AnyActorRef::from_path(self.parent.clone())
    }

    pub fn children(&self) -> Vec<AnyActorRef> {
        self.system.children_of(self.myself.path())
    }

    pub fn child(&self, name: &str) -> Option<AnyActorRef> {
        self.system.child_of(self.myself.path(), name)
    }

    pub fn event_stream(&self) -> EventStream {
        self.system.event_stream()
    }

    pub fn receptionist(&self) -> Receptionist {
        self.system.receptionist()
    }

    pub fn stash(&mut self, message: M) -> ActorResult {
        self.stash.stash(message)
    }

    pub fn unstash(&mut self, limit: usize) -> ActorResult {
        let messages = self.stash.take(limit);
        if messages.is_empty() {
            return Ok(());
        }
        self.myself
            .prepend_user_messages(messages)
            .map_err(|messages| {
                ActorError::Message(format!("failed to unstash {} messages", messages.len()))
            })
    }

    pub fn unstash_all(&mut self) -> ActorResult {
        let messages = self.stash.take_all();
        if messages.is_empty() {
            return Ok(());
        }
        self.myself
            .prepend_user_messages(messages)
            .map_err(|messages| {
                ActorError::Message(format!("failed to unstash {} messages", messages.len()))
            })
    }

    pub fn clear_stash(&mut self) {
        self.stash.clear();
    }

    pub fn stash_len(&self) -> usize {
        self.stash.len()
    }

    pub fn stash_capacity(&self) -> Option<usize> {
        self.stash.capacity()
    }

    pub fn is_stash_full(&self) -> bool {
        self.stash.is_full()
    }

    pub fn spawn_task<F>(&self, task: F) -> Result<TaskHandle, ActorError>
    where
        F: FnOnce(ActorRef<M>) + Send + 'static,
    {
        tasks::spawn_task(self.tasks.scoped_ref(self.myself.clone()), task)
    }

    pub fn pipe_to_self<T, E, F, Map>(&self, task: F, map: Map) -> Result<TaskHandle, ActorError>
    where
        T: Send + 'static,
        E: Send + 'static,
        F: FnOnce() -> Result<T, E> + Send + 'static,
        Map: FnOnce(Result<T, E>) -> M + Send + 'static,
    {
        tasks::pipe_to_self(self.tasks.scoped_ref(self.myself.clone()), task, map)
    }

    pub fn message_adapter<U, F>(&self, map: F) -> Result<ActorRef<U>, ActorError>
    where
        U: Send + 'static,
        F: FnMut(U) -> M + Send + 'static,
    {
        adapters::message_adapter(&self.system, self.myself.clone(), map)
    }

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
        asks::ask(
            &self.system,
            self.myself.clone(),
            target,
            timeout,
            create_request,
            map_response,
        )
    }

    pub fn watch<N: Send + 'static>(&mut self, actor: &ActorRef<N>) -> ActorResult {
        self.system.watch(self.myself.clone(), actor.clone())
    }

    pub fn watch_with<N: Send + 'static>(
        &mut self,
        actor: &ActorRef<N>,
        message: M,
    ) -> ActorResult {
        self.system
            .watch_with(self.myself.clone(), actor.clone(), message)
    }

    pub fn unwatch<N: Send + 'static>(&mut self, actor: &ActorRef<N>) {
        self.system.unwatch(self.myself.path(), actor.path());
    }

    pub fn schedule_once<N: Send + 'static>(
        &self,
        delay: Duration,
        target: ActorRef<N>,
        message: N,
    ) -> Cancellable {
        self.system.schedule_once(delay, target, message)
    }

    pub fn schedule_once_self(&self, delay: Duration, message: M) -> Cancellable {
        self.system
            .schedule_once(delay, self.myself.clone(), message)
    }

    pub fn start_single_timer(&mut self, key: impl Into<TimerKey>, delay: Duration, message: M) {
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

    pub fn start_timer_with_fixed_delay(
        &mut self,
        key: impl Into<TimerKey>,
        initial_delay: Duration,
        delay: Duration,
        message: M,
    ) where
        M: Clone,
    {
        let key = key.into();
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

    pub fn start_timer_at_fixed_rate(
        &mut self,
        key: impl Into<TimerKey>,
        initial_delay: Duration,
        interval: Duration,
        message: M,
    ) where
        M: Clone,
    {
        let key = key.into();
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

    pub fn cancel_timer(&mut self, key: impl AsRef<str>) {
        self.timers.cancel(key.as_ref());
    }

    pub fn cancel_all_timers(&mut self) {
        self.timers.cancel_all();
    }

    pub fn is_timer_active(&self, key: impl AsRef<str>) -> bool {
        self.timers.is_active(key.as_ref())
    }

    pub(crate) fn accept_timer(&mut self, timer: &TimerEnvelope<M>) -> bool {
        self.timers.accept(timer.key(), timer.generation())
    }

    pub fn set_receive_timeout(&mut self, timeout: Duration, message: M)
    where
        M: Clone,
    {
        self.receive_timeout
            .set(timeout, message, &self.system, self.myself.clone());
    }

    pub fn cancel_receive_timeout(&mut self) {
        self.receive_timeout.cancel();
    }

    pub fn receive_timeout(&self) -> Option<Duration> {
        self.receive_timeout.timeout()
    }

    pub(crate) fn before_influencing_message(&mut self) {
        self.receive_timeout.cancel_task();
    }

    pub(crate) fn after_influencing_message(&mut self) {
        self.receive_timeout
            .reschedule(&self.system, self.myself.clone());
    }

    pub(crate) fn accept_receive_timeout(&mut self, timeout: &ReceiveTimeoutEnvelope<M>) -> bool {
        self.receive_timeout.accept(timeout)
    }

    pub(crate) fn cancel_tasks(&mut self) {
        self.tasks.cancel_current();
    }

    pub fn spawn<A>(
        &self,
        name: impl AsRef<str>,
        props: Props<A>,
    ) -> Result<ActorRef<A::Msg>, ActorError>
    where
        A: Actor,
    {
        self.system
            .spawn_under(self.myself.path(), name.as_ref(), props)
    }

    pub fn spawn_anonymous<A>(&self, props: Props<A>) -> Result<ActorRef<A::Msg>, ActorError>
    where
        A: Actor,
    {
        self.system.spawn_anonymous_under(self.myself.path(), props)
    }

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
