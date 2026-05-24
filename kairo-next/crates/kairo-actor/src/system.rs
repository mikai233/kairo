use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use crate::actor::{Actor, Context, Props};
use crate::dead_letters::DeadLetters;
use crate::dispatcher::DispatcherSettings;
use crate::error::ActorError;
use crate::mailbox::{Dequeued, Mailbox, SystemMessage};
use crate::path::{ActorPath, Address};
use crate::refs::{ActorRef, LocalActorHandle, TerminationLatch};
use crate::signal::Signal;

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
    names: Mutex<HashMap<String, u64>>,
    children: Mutex<HashMap<String, Vec<LocalActorHandle>>>,
    dispatcher: DispatcherSettings,
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

    pub fn stop<M: Send + 'static>(&self, actor: &ActorRef<M>) {
        actor.request_stop();
    }

    pub fn missing_ref<M>(&self, path: impl Into<String>) -> ActorRef<M> {
        ActorRef::missing(ActorPath::new(path), self.inner.dead_letters.clone())
    }

    pub fn spawn<A>(
        &self,
        name: impl AsRef<str>,
        props: Props<A>,
    ) -> Result<ActorRef<A::Msg>, ActorError>
    where
        A: Actor,
    {
        let parent_path = format!("kairo://{}/user", self.name);
        self.spawn_under(&parent_path, name.as_ref(), props)
    }

    pub(crate) fn spawn_under<A>(
        &self,
        parent_path: &str,
        name: &str,
        props: Props<A>,
    ) -> Result<ActorRef<A::Msg>, ActorError>
    where
        A: Actor,
    {
        validate_actor_name(name)?;

        let uid = self.inner.next_uid.fetch_add(1, Ordering::Relaxed);
        let registry_key = format!("{parent_path}/{name}");
        {
            let mut names = self.inner.names.lock().expect("actor registry poisoned");
            if names.contains_key(&registry_key) {
                return Err(ActorError::DuplicateName(name.to_string()));
            }
            names.insert(registry_key.clone(), uid);
        }

        let mailbox = Arc::new(Mailbox::default());
        let path = ActorPath::new(format!("{parent_path}/{name}#{uid}"));
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
        let parent_path = parent_path.to_string();
        let parent_path_for_thread = parent_path.clone();
        self.inner
            .children
            .lock()
            .expect("actor children registry poisoned")
            .entry(parent_path.clone())
            .or_default()
            .push(actor_ref.to_local_handle());

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
            self.inner
                .names
                .lock()
                .expect("actor registry poisoned")
                .remove(&registry_key);
            remove_child_from_parent(&self.inner, &parent_path, actor_ref.path());
            return Err(ActorError::Message(format!(
                "failed to spawn actor thread: {error}"
            )));
        }

        Ok(actor_ref)
    }

    pub(crate) fn spawn_anonymous_under<A>(
        &self,
        parent_path: &str,
        props: Props<A>,
    ) -> Result<ActorRef<A::Msg>, ActorError>
    where
        A: Actor,
    {
        let id = self.inner.next_anonymous.fetch_add(1, Ordering::Relaxed);
        let name = format!("$anon-{id}");
        self.spawn_under(parent_path, &name, props)
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

fn validate_actor_name(name: &str) -> Result<(), ActorError> {
    if name.is_empty() || name.contains('/') || name.contains('#') {
        return Err(ActorError::InvalidName(name.to_string()));
    }
    Ok(())
}

fn run_actor<A>(
    props: Props<A>,
    actor_ref: ActorRef<A::Msg>,
    dead_letters: DeadLetters,
    system_inner: Arc<ActorSystemInner>,
    registry_key: String,
    thread_system: ActorSystem,
    parent_path: String,
) where
    A: Actor,
{
    let mut actor = props.build();
    let throughput = thread_system.dispatcher_settings().throughput();
    let mut context = Context {
        myself: actor_ref.clone(),
        system: thread_system,
        stop_requested: false,
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
        let processed = process_dequeued(mailbox.dequeue(), &actor_ref, &mut actor, &mut context);
        let mut processed_user = usize::from(processed);

        while processed_user < throughput && !actor_ref.target.stopped.load(Ordering::Acquire) {
            let Some(next) = mailbox.try_dequeue() else {
                break;
            };
            if process_dequeued(next, &actor_ref, &mut actor, &mut context) {
                processed_user += 1;
            }
        }

        if processed_user >= throughput && !actor_ref.target.stopped.load(Ordering::Acquire) {
            thread::yield_now();
        }
    }

    for message in mailbox.close_and_drain_user() {
        drop(message);
        dead_letters.publish::<A::Msg>(actor_ref.path.clone(), "actor is stopped");
    }

    stop_children(&system_inner, actor_ref.path.as_str());
    let _ = actor.signal(&mut context, Signal::PostStop);
    actor_ref.target.terminated.mark_stopped();
    system_inner
        .names
        .lock()
        .expect("actor registry poisoned")
        .remove(&registry_key);
    remove_child_from_parent(&system_inner, &parent_path, actor_ref.path());
}

fn process_dequeued<A>(
    dequeued: Dequeued<A::Msg>,
    actor_ref: &ActorRef<A::Msg>,
    actor: &mut A,
    context: &mut Context<A::Msg>,
) -> bool
where
    A: Actor,
{
    match dequeued {
        Dequeued::System(SystemMessage::Stop) | Dequeued::Closed => {
            actor_ref.target.stopped.store(true, Ordering::Release);
            false
        }
        Dequeued::User(message) => {
            if actor.receive(context, message).is_err() || context.stop_requested {
                actor_ref.target.stopped.store(true, Ordering::Release);
            }
            true
        }
    }
}

fn stop_children(system_inner: &ActorSystemInner, parent_path: &str) {
    let children = system_inner
        .children
        .lock()
        .expect("actor children registry poisoned")
        .remove(parent_path)
        .unwrap_or_default();

    for child in &children {
        child.request_stop();
    }

    for child in children {
        child.wait_for_stop();
    }
}

fn remove_child_from_parent(
    system_inner: &ActorSystemInner,
    parent_path: &str,
    child_path: &ActorPath,
) {
    let mut children = system_inner
        .children
        .lock()
        .expect("actor children registry poisoned");
    if let Some(siblings) = children.get_mut(parent_path) {
        siblings.retain(|child| child.path() != child_path);
        if siblings.is_empty() {
            children.remove(parent_path);
        }
    }
}
