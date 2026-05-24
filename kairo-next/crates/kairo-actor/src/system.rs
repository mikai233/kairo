use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use crate::actor::{Actor, Context, Props};
use crate::coordinated_shutdown::CoordinatedShutdown;
use crate::dead_letters::DeadLetters;
use crate::death_watch::{DeathWatchKind, DeathWatchRegistration, DeathWatchRegistry};
use crate::dispatcher::DispatcherSettings;
use crate::error::{ActorError, ActorResult};
use crate::event_stream::EventStream;
use crate::mailbox::{Dequeued, Mailbox, SystemMessage, UserEnvelope};
use crate::path::{ActorPath, Address};
use crate::receptionist::Receptionist;
use crate::refs::{ActorRef, AnyActorRef, TerminationLatch};
use crate::registry::ActorRegistry;
use crate::scheduler::{Cancellable, Scheduler};
use crate::signal::Signal;
use crate::supervision::SupervisorStrategy;
use crate::timers::TimerState;

#[derive(Debug, Clone)]
pub struct ActorSystem {
    name: String,
    address: Address,
    inner: Arc<ActorSystemInner>,
}

#[derive(Debug, Default)]
pub(crate) struct ActorSystemInner {
    next_uid: AtomicU64,
    next_anonymous: AtomicU64,
    terminating: AtomicBool,
    terminated: AtomicBool,
    registry: ActorRegistry,
    death_watch: DeathWatchRegistry,
    dispatcher: DispatcherSettings,
    scheduler: Scheduler,
    event_stream: EventStream,
    receptionist: Receptionist,
    coordinated_shutdown: CoordinatedShutdown,
    dead_letters: DeadLetters,
}

impl ActorSystem {
    pub fn builder(name: impl Into<String>) -> ActorSystemBuilder {
        ActorSystemBuilder {
            name: name.into(),
            dispatcher: DispatcherSettings::default(),
        }
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

    pub fn event_stream(&self) -> EventStream {
        self.inner.event_stream.clone()
    }

    pub fn receptionist(&self) -> Receptionist {
        self.inner.receptionist.clone()
    }

    pub fn coordinated_shutdown(&self) -> CoordinatedShutdown {
        self.inner.coordinated_shutdown.clone()
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
        self.inner.terminated.store(true, Ordering::Release);
        Ok(())
    }

    pub fn missing_ref<M>(&self, path: impl Into<String>) -> ActorRef<M> {
        ActorRef::missing(ActorPath::new(path), self.inner.dead_letters.clone())
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

    pub(crate) fn next_ask_path(&self, owner_path: &ActorPath) -> Result<ActorPath, ActorError> {
        if self.is_terminating() {
            return Err(ActorError::SystemTerminating);
        }
        let id = self.inner.next_anonymous.fetch_add(1, Ordering::Relaxed);
        Ok(owner_path.child(format!("$ask-{id}"), Some(id)))
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
            return Ok(());
        }
        let subject_ref = subject.as_any();
        let registration = DeathWatchRegistration::new(
            watcher.path().clone(),
            DeathWatchKind::Signal,
            move || watcher.send_system_signal(Signal::Terminated(subject_ref)),
        );
        self.watch_registered(subject, registration)
    }

    pub(crate) fn watch_with<M, N>(
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
            return Ok(());
        }
        let registration = DeathWatchRegistration::new(
            watcher.path().clone(),
            DeathWatchKind::Custom,
            move || {
                let _ = watcher.tell(message);
            },
        );
        self.watch_registered(subject, registration)
    }

    pub(crate) fn unwatch(&self, watcher: &ActorPath, subject: &ActorPath) {
        self.inner.death_watch.unwatch(subject, watcher);
    }

    pub fn spawn<A>(
        &self,
        name: impl AsRef<str>,
        props: Props<A>,
    ) -> Result<ActorRef<A::Msg>, ActorError>
    where
        A: Actor,
    {
        let parent_path = self.user_root_path();
        self.spawn_under(&parent_path, name.as_ref(), props)
    }

    pub(crate) fn spawn_under<A>(
        &self,
        parent_path: &ActorPath,
        name: &str,
        props: Props<A>,
    ) -> Result<ActorRef<A::Msg>, ActorError>
    where
        A: Actor,
    {
        self.spawn_under_with_name(parent_path, name, props, false)
    }

    pub(crate) fn spawn_anonymous_under<A>(
        &self,
        parent_path: &ActorPath,
        props: Props<A>,
    ) -> Result<ActorRef<A::Msg>, ActorError>
    where
        A: Actor,
    {
        let id = self.inner.next_anonymous.fetch_add(1, Ordering::Relaxed);
        let name = format!("$anon-{id}");
        self.spawn_under_with_name(parent_path, &name, props, true)
    }

    fn user_root_path(&self) -> ActorPath {
        ActorPath::root(self.address.clone(), "user")
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
            registration.notify();
            return Ok(());
        }

        self.inner
            .death_watch
            .watch(subject.path().clone(), registration)?;
        if subject.is_terminated() {
            self.inner.death_watch.notify(subject.path());
        }
        Ok(())
    }

    fn spawn_under_with_name<A>(
        &self,
        parent_path: &ActorPath,
        name: &str,
        props: Props<A>,
        allow_reserved_name: bool,
    ) -> Result<ActorRef<A::Msg>, ActorError>
    where
        A: Actor,
    {
        if self.is_terminating() {
            return Err(ActorError::SystemTerminating);
        }
        validate_actor_name(name, allow_reserved_name)?;

        let uid = self.inner.next_uid.fetch_add(1, Ordering::Relaxed);
        let registry_key = format!("{parent_path}/{name}");
        self.inner
            .registry
            .reserve_name(registry_key.clone(), uid, name)?;

        let mailbox = Arc::new(Mailbox::default());
        let path = parent_path.child(name, Some(uid));
        let stopped = Arc::new(AtomicBool::new(false));
        let terminated = Arc::new(TerminationLatch::default());
        let actor_ref = ActorRef::new(
            path.clone(),
            mailbox,
            Arc::clone(&stopped),
            Arc::clone(&terminated),
            self.inner.dead_letters.clone(),
        );
        let thread_ref = actor_ref.clone();
        let dead_letters = self.inner.dead_letters.clone();
        let system_inner = Arc::clone(&self.inner);
        let actor_name = name.to_string();
        let registry_key_for_thread = registry_key.clone();
        let thread_system = self.clone();
        let parent_path_for_registry = parent_path.to_string();
        let parent_path_for_thread = parent_path.clone();
        self.inner.registry.add_child(
            parent_path_for_registry.clone(),
            actor_ref.to_local_handle(),
        );

        if let Err(error) = thread::Builder::new()
            .name(format!("kairo-actor-{actor_name}"))
            .spawn(move || {
                run_actor(
                    props,
                    thread_ref,
                    dead_letters,
                    system_inner,
                    registry_key_for_thread,
                    thread_system,
                    parent_path_for_thread,
                );
            })
        {
            self.inner.registry.release_name(&registry_key);
            self.inner
                .registry
                .remove_child(&parent_path_for_registry, actor_ref.path());
            return Err(ActorError::Message(format!(
                "failed to spawn actor thread: {error}"
            )));
        }

        Ok(actor_ref)
    }
}

#[derive(Debug)]
pub struct ActorSystemBuilder {
    name: String,
    dispatcher: DispatcherSettings,
}

impl ActorSystemBuilder {
    pub fn dispatcher_throughput(mut self, throughput: usize) -> Self {
        self.dispatcher = DispatcherSettings::new(throughput);
        self
    }

    pub fn build(self) -> Result<ActorSystem, ActorError> {
        if self.dispatcher.throughput() == 0 {
            return Err(ActorError::InvalidThroughput);
        }
        Ok(ActorSystem {
            address: Address::local(self.name.clone()),
            name: self.name,
            inner: Arc::new(ActorSystemInner {
                dispatcher: self.dispatcher,
                ..ActorSystemInner::default()
            }),
        })
    }
}

fn validate_actor_name(name: &str, allow_reserved: bool) -> Result<(), ActorError> {
    let valid = if allow_reserved {
        ActorPath::is_valid_internal_name(name)
    } else {
        ActorPath::is_valid_actor_name(name)
    };
    if !valid {
        return Err(ActorError::InvalidName(name.to_string()));
    }
    Ok(())
}

fn run_actor<A>(
    mut props: Props<A>,
    actor_ref: ActorRef<A::Msg>,
    dead_letters: DeadLetters,
    system_inner: Arc<ActorSystemInner>,
    registry_key: String,
    thread_system: ActorSystem,
    parent_path: ActorPath,
) where
    A: Actor,
{
    let mut actor = props.build();
    let throughput = thread_system.dispatcher_settings().throughput();
    let mut context = Context {
        myself: actor_ref.clone(),
        parent: parent_path.clone(),
        system: thread_system,
        stop_requested: false,
        timers: TimerState::default(),
    };

    if actor.started(&mut context).is_err() || context.stop_requested {
        actor_ref.target.stopped.store(true, Ordering::Release);
    }

    let mailbox = actor_ref
        .target
        .mailbox
        .as_ref()
        .expect("live actor ref must have a mailbox");
    while !actor_ref.target.stopped.load(Ordering::Acquire) {
        let processed = process_dequeued(
            mailbox.dequeue(),
            &actor_ref,
            &mut actor,
            &mut context,
            &props,
            &system_inner,
        );
        let mut processed_user = usize::from(processed);

        while processed_user < throughput && !actor_ref.target.stopped.load(Ordering::Acquire) {
            let Some(next) = mailbox.try_dequeue() else {
                break;
            };
            if process_dequeued(
                next,
                &actor_ref,
                &mut actor,
                &mut context,
                &props,
                &system_inner,
            ) {
                processed_user += 1;
            }
        }

        if processed_user >= throughput && !actor_ref.target.stopped.load(Ordering::Acquire) {
            thread::yield_now();
        }
    }

    context.cancel_all_timers();
    for _ in 0..mailbox.close_and_drain_user() {
        dead_letters.publish::<A::Msg>(actor_ref.path.clone(), "actor is stopped");
    }

    stop_children(&system_inner, actor_ref.path.as_str());
    let _ = actor.signal(&mut context, Signal::PostStop);
    actor_ref.target.terminated.mark_stopped();
    system_inner.death_watch.remove_watcher(actor_ref.path());
    system_inner.death_watch.notify(actor_ref.path());
    system_inner.receptionist.remove_actor(actor_ref.path());
    system_inner.registry.release_name(&registry_key);
    system_inner
        .registry
        .remove_child(parent_path.as_str(), actor_ref.path());
}

fn process_dequeued<A>(
    dequeued: Dequeued<A::Msg>,
    actor_ref: &ActorRef<A::Msg>,
    actor: &mut A,
    context: &mut Context<A::Msg>,
    props: &Props<A>,
    system_inner: &ActorSystemInner,
) -> bool
where
    A: Actor,
{
    match dequeued {
        Dequeued::System(SystemMessage::Stop) | Dequeued::Closed => {
            actor_ref.target.stopped.store(true, Ordering::Release);
            false
        }
        Dequeued::System(SystemMessage::Signal(signal)) => {
            let _ = actor.signal(context, signal);
            false
        }
        Dequeued::User(UserEnvelope::Message(message)) => {
            if apply_receive_result(
                actor.receive(context, message),
                actor_ref,
                actor,
                context,
                props,
                system_inner,
            ) || context.stop_requested
            {
                actor_ref.target.stopped.store(true, Ordering::Release);
            }
            true
        }
        Dequeued::User(UserEnvelope::Adapted(adapt)) => {
            if apply_receive_result(
                actor.receive(context, adapt()),
                actor_ref,
                actor,
                context,
                props,
                system_inner,
            ) || context.stop_requested
            {
                actor_ref.target.stopped.store(true, Ordering::Release);
            }
            true
        }
        Dequeued::User(UserEnvelope::Timer(timer)) => {
            if context.accept_timer(&timer) {
                if apply_receive_result(
                    actor.receive(context, timer.into_message()),
                    actor_ref,
                    actor,
                    context,
                    props,
                    system_inner,
                ) || context.stop_requested
                {
                    actor_ref.target.stopped.store(true, Ordering::Release);
                }
                true
            } else {
                false
            }
        }
    }
}

fn apply_receive_result<A>(
    result: ActorResult,
    actor_ref: &ActorRef<A::Msg>,
    actor: &mut A,
    context: &mut Context<A::Msg>,
    props: &Props<A>,
    system_inner: &ActorSystemInner,
) -> bool
where
    A: Actor,
{
    if result.is_ok() {
        return false;
    }

    match props.supervisor() {
        SupervisorStrategy::Stop => true,
        SupervisorStrategy::Resume => false,
        SupervisorStrategy::Restart => {
            restart_actor(actor_ref, actor, context, props, system_inner).is_err()
        }
    }
}

fn restart_actor<A>(
    actor_ref: &ActorRef<A::Msg>,
    actor: &mut A,
    context: &mut Context<A::Msg>,
    props: &Props<A>,
    system_inner: &ActorSystemInner,
) -> ActorResult
where
    A: Actor,
{
    let Some(mut restarted) = props.restart() else {
        return Err(ActorError::Message(
            "restart supervision requires restartable props".to_string(),
        ));
    };

    context.cancel_all_timers();
    stop_children(system_inner, actor_ref.path.as_str());
    let _ = actor.signal(context, Signal::PreRestart);
    context.stop_requested = false;
    restarted.started(context)?;
    *actor = restarted;
    Ok(())
}

fn stop_children(system_inner: &ActorSystemInner, parent_path: &str) {
    let _ = stop_children_with_timeout(system_inner, parent_path, Duration::MAX);
}

fn stop_children_with_timeout(
    system_inner: &ActorSystemInner,
    parent_path: &str,
    timeout: Duration,
) -> Result<(), ActorError> {
    let children = system_inner.registry.take_children(parent_path);

    for child in &children {
        child.request_stop();
    }

    let deadline = Instant::now()
        .checked_add(timeout)
        .unwrap_or_else(|| Instant::now() + Duration::from_secs(60 * 60 * 24 * 365));
    for child in children {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or(ActorError::TerminationTimeout)?;
        if !child.wait_for_stop(remaining) {
            return Err(ActorError::TerminationTimeout);
        }
    }
    Ok(())
}
