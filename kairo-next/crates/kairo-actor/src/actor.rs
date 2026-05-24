use crate::error::{ActorError, ActorResult};
use crate::path::ActorPath;
use crate::refs::{ActorRef, AnyActorRef};
use crate::scheduler::Cancellable;
use crate::signal::Signal;
use crate::system::ActorSystem;
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
