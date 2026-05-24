use crate::adapters;
use crate::error::{ActorError, ActorResult};
use crate::event_stream::EventStream;
use crate::path::ActorPath;
use crate::refs::{ActorRef, AnyActorRef};
use crate::scheduler::Cancellable;
use crate::signal::Signal;
use crate::system::ActorSystem;
use crate::tasks::{self, TaskHandle};
use crate::timers::{TimerEnvelope, TimerKey, TimerState};
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
            Signal::PreRestart | Signal::Terminated(_) => Ok(()),
        }
    }

    fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult;
}

pub struct Props<A> {
    builder: Box<dyn FnOnce() -> A + Send>,
}

impl<A> Props<A> {
    pub fn new<F>(builder: F) -> Self
    where
        F: FnOnce() -> A + Send + 'static,
    {
        Self {
            builder: Box::new(builder),
        }
    }

    pub(crate) fn build(self) -> A {
        (self.builder)()
    }
}

#[derive(Debug)]
pub struct Context<M> {
    pub(crate) myself: ActorRef<M>,
    pub(crate) parent: ActorPath,
    pub(crate) system: ActorSystem,
    pub(crate) stop_requested: bool,
    pub(crate) timers: TimerState,
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

    pub fn spawn_task<F>(&self, task: F) -> Result<TaskHandle, ActorError>
    where
        F: FnOnce(ActorRef<M>) + Send + 'static,
    {
        tasks::spawn_task(self.myself.clone(), task)
    }

    pub fn pipe_to_self<T, E, F, Map>(&self, task: F, map: Map) -> Result<TaskHandle, ActorError>
    where
        T: Send + 'static,
        E: Send + 'static,
        F: FnOnce() -> Result<T, E> + Send + 'static,
        Map: FnOnce(Result<T, E>) -> M + Send + 'static,
    {
        tasks::pipe_to_self(self.myself.clone(), task, map)
    }

    pub fn message_adapter<U, F>(&self, map: F) -> Result<ActorRef<U>, ActorError>
    where
        U: Send + 'static,
        F: FnMut(U) -> M + Send + 'static,
    {
        adapters::message_adapter(&self.system, self.myself.clone(), map)
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
