//! Typed local actor API and runtime primitives.

use std::any::type_name;
use std::collections::{HashMap, VecDeque};
use std::fmt::{self, Display, Formatter};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

pub type ActorResult = Result<(), ActorError>;

#[derive(Debug, thiserror::Error)]
pub enum ActorError {
    #[error("{0}")]
    Message(String),
    #[error("actor name `{0}` is invalid")]
    InvalidName(String),
    #[error("actor `{0}` already exists")]
    DuplicateName(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Address {
    protocol: String,
    system: String,
    host: Option<String>,
    port: Option<u16>,
}

impl Address {
    pub fn local(system: impl Into<String>) -> Self {
        Self {
            protocol: "kairo".to_string(),
            system: system.into(),
            host: None,
            port: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ActorPath {
    value: String,
}

impl ActorPath {
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
        }
    }

    pub fn as_str(&self) -> &str {
        &self.value
    }
}

impl Display for ActorPath {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.value)
    }
}

pub struct SendError<M> {
    message: M,
    reason: String,
}

impl<M> SendError<M> {
    pub fn into_message(self) -> M {
        self.message
    }

    pub fn reason(&self) -> &str {
        &self.reason
    }
}

impl<M> Display for SendError<M> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.reason)
    }
}

impl<M> fmt::Debug for SendError<M> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("SendError")
            .field("reason", &self.reason)
            .finish_non_exhaustive()
    }
}

impl<M> std::error::Error for SendError<M> {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadLetter {
    recipient: ActorPath,
    message_type: &'static str,
    reason: String,
}

impl DeadLetter {
    pub fn recipient(&self) -> &ActorPath {
        &self.recipient
    }

    pub fn message_type(&self) -> &'static str {
        self.message_type
    }

    pub fn reason(&self) -> &str {
        &self.reason
    }
}

#[derive(Debug, Default)]
struct DeadLettersInner {
    records: Mutex<Vec<DeadLetter>>,
    changed: Condvar,
}

#[derive(Debug, Clone, Default)]
pub struct DeadLetters {
    inner: Arc<DeadLettersInner>,
}

impl DeadLetters {
    pub fn len(&self) -> usize {
        self.inner
            .records
            .lock()
            .expect("dead letters poisoned")
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn records(&self) -> Vec<DeadLetter> {
        self.inner
            .records
            .lock()
            .expect("dead letters poisoned")
            .clone()
    }

    pub fn wait_for_len(&self, expected: usize, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut records = self.inner.records.lock().expect("dead letters poisoned");
        while records.len() < expected {
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                return false;
            };
            let (next_records, wait) = self
                .inner
                .changed
                .wait_timeout(records, remaining)
                .expect("dead letters poisoned");
            records = next_records;
            if wait.timed_out() && records.len() < expected {
                return false;
            }
        }
        true
    }

    fn publish<M: Send + 'static>(&self, recipient: ActorPath, reason: impl Into<String>) {
        let mut records = self.inner.records.lock().expect("dead letters poisoned");
        records.push(DeadLetter {
            recipient,
            message_type: type_name::<M>(),
            reason: reason.into(),
        });
        self.inner.changed.notify_all();
    }
}

#[derive(Debug)]
pub struct ActorRef<M> {
    path: ActorPath,
    mailbox: Arc<Mailbox<M>>,
    stopped: Arc<AtomicBool>,
    dead_letters: DeadLetters,
    _message: PhantomData<fn(M)>,
}

impl<M> Clone for ActorRef<M> {
    fn clone(&self) -> Self {
        Self {
            path: self.path.clone(),
            mailbox: self.mailbox.clone(),
            stopped: Arc::clone(&self.stopped),
            dead_letters: self.dead_letters.clone(),
            _message: PhantomData,
        }
    }
}

impl<M: Send + 'static> ActorRef<M> {
    pub fn path(&self) -> &ActorPath {
        &self.path
    }

    pub fn tell(&self, message: M) -> Result<(), SendError<M>> {
        if self.stopped.load(Ordering::Acquire) {
            self.dead_letters
                .publish::<M>(self.path.clone(), "actor is stopped");
            return Err(SendError {
                message,
                reason: "actor is stopped".to_string(),
            });
        }

        self.mailbox.enqueue_user(message).map_err(|message| {
            self.stopped.store(true, Ordering::Release);
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
        self.stopped.load(Ordering::Acquire)
    }

    fn request_stop(&self) {
        if !self.stopped.swap(true, Ordering::AcqRel) {
            self.mailbox.enqueue_system(SystemMessage::Stop);
        }
    }
}

#[derive(Debug, Clone)]
pub struct AnyActorRef {
    path: ActorPath,
}

impl AnyActorRef {
    pub fn path(&self) -> &ActorPath {
        &self.path
    }
}

pub trait Actor: Send + 'static {
    type Msg: Send + 'static;

    fn started(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        Ok(())
    }

    fn stopped(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
        Ok(())
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

    pub fn build(self) -> A {
        (self.builder)()
    }
}

#[derive(Debug)]
pub struct Context<M> {
    myself: ActorRef<M>,
    stop_requested: bool,
}

impl<M: Send + 'static> Context<M> {
    pub fn myself(&self) -> ActorRef<M> {
        self.myself.clone()
    }

    pub fn stop(&mut self, actor: ActorRef<M>) {
        if actor.path == self.myself.path {
            self.stop_requested = true;
            actor.request_stop();
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SystemMessage {
    Stop,
}

#[derive(Debug)]
enum Dequeued<M> {
    System(SystemMessage),
    User(M),
    Closed,
}

#[derive(Debug)]
struct Mailbox<M> {
    state: Mutex<MailboxState<M>>,
    ready: Condvar,
}

#[derive(Debug)]
struct MailboxState<M> {
    system: VecDeque<SystemMessage>,
    user: VecDeque<M>,
    closed: bool,
}

impl<M> Default for Mailbox<M> {
    fn default() -> Self {
        Self {
            state: Mutex::new(MailboxState {
                system: VecDeque::new(),
                user: VecDeque::new(),
                closed: false,
            }),
            ready: Condvar::new(),
        }
    }
}

impl<M> Mailbox<M> {
    fn enqueue_user(&self, message: M) -> Result<(), M> {
        let mut state = self.state.lock().expect("mailbox poisoned");
        if state.closed {
            return Err(message);
        }
        state.user.push_back(message);
        self.ready.notify_one();
        Ok(())
    }

    fn enqueue_system(&self, message: SystemMessage) {
        let mut state = self.state.lock().expect("mailbox poisoned");
        if state.closed {
            return;
        }
        state.system.push_back(message);
        self.ready.notify_one();
    }

    fn dequeue(&self) -> Dequeued<M> {
        let mut state = self.state.lock().expect("mailbox poisoned");
        loop {
            if let Some(message) = state.system.pop_front() {
                return Dequeued::System(message);
            }
            if let Some(message) = state.user.pop_front() {
                return Dequeued::User(message);
            }
            if state.closed {
                return Dequeued::Closed;
            }
            state = self.ready.wait(state).expect("mailbox poisoned");
        }
    }

    fn close_and_drain_user(&self) -> Vec<M> {
        let mut state = self.state.lock().expect("mailbox poisoned");
        state.closed = true;
        state.system.clear();
        let messages = state.user.drain(..).collect();
        self.ready.notify_all();
        messages
    }
}

#[derive(Debug)]
pub struct ActorSystem {
    name: String,
    address: Address,
    inner: Arc<ActorSystemInner>,
}

#[derive(Debug, Default)]
struct ActorSystemInner {
    next_uid: AtomicU64,
    names: Mutex<HashMap<String, u64>>,
    dead_letters: DeadLetters,
}

impl ActorSystem {
    pub fn builder(name: impl Into<String>) -> ActorSystemBuilder {
        ActorSystemBuilder { name: name.into() }
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

    pub fn stop<M: Send + 'static>(&self, actor: &ActorRef<M>) {
        actor.request_stop();
    }

    pub fn spawn<A>(
        &self,
        name: impl AsRef<str>,
        props: Props<A>,
    ) -> Result<ActorRef<A::Msg>, ActorError>
    where
        A: Actor,
    {
        let name = name.as_ref();
        validate_actor_name(name)?;

        let uid = self.inner.next_uid.fetch_add(1, Ordering::Relaxed);
        {
            let mut names = self.inner.names.lock().expect("actor registry poisoned");
            if names.contains_key(name) {
                return Err(ActorError::DuplicateName(name.to_string()));
            }
            names.insert(name.to_string(), uid);
        }

        let mailbox = Arc::new(Mailbox::default());
        let path = ActorPath::new(format!("kairo://{}/user/{}#{}", self.name, name, uid));
        let stopped = Arc::new(AtomicBool::new(false));
        let actor_ref = ActorRef {
            path: path.clone(),
            mailbox,
            stopped: Arc::clone(&stopped),
            dead_letters: self.inner.dead_letters.clone(),
            _message: PhantomData,
        };
        let thread_ref = actor_ref.clone();
        let dead_letters = self.inner.dead_letters.clone();
        let system_inner = Arc::clone(&self.inner);
        let actor_name = name.to_string();

        if let Err(error) = thread::Builder::new()
            .name(format!("kairo-actor-{actor_name}"))
            .spawn(move || {
                run_actor(
                    props,
                    thread_ref,
                    stopped,
                    dead_letters,
                    system_inner,
                    actor_name,
                );
            })
        {
            self.inner
                .names
                .lock()
                .expect("actor registry poisoned")
                .remove(name);
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
}

impl ActorSystemBuilder {
    pub fn build(self) -> Result<ActorSystem, ActorError> {
        Ok(ActorSystem {
            address: Address::local(self.name.clone()),
            name: self.name,
            inner: Arc::new(ActorSystemInner::default()),
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
    stopped: Arc<AtomicBool>,
    dead_letters: DeadLetters,
    system_inner: Arc<ActorSystemInner>,
    actor_name: String,
) where
    A: Actor,
{
    let mut actor = props.build();
    let mut context = Context {
        myself: actor_ref.clone(),
        stop_requested: false,
    };

    if actor.started(&mut context).is_err() || context.stop_requested {
        stopped.store(true, Ordering::Release);
    }

    while !stopped.load(Ordering::Acquire) {
        match actor_ref.mailbox.dequeue() {
            Dequeued::System(SystemMessage::Stop) | Dequeued::Closed => {
                stopped.store(true, Ordering::Release);
            }
            Dequeued::User(message) => {
                if actor.receive(&mut context, message).is_err() || context.stop_requested {
                    stopped.store(true, Ordering::Release);
                }
            }
        }
    }

    for message in actor_ref.mailbox.close_and_drain_user() {
        drop(message);
        dead_letters.publish::<A::Msg>(actor_ref.path.clone(), "actor is stopped");
    }

    let _ = actor.stopped(&mut context);
    system_inner
        .names
        .lock()
        .expect("actor registry poisoned")
        .remove(&actor_name);
}

pub mod prelude {
    pub use crate::{
        Actor, ActorError, ActorPath, ActorRef, ActorResult, ActorSystem, Context, DeadLetter,
        DeadLetters, Props,
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    enum CounterMsg {
        Increment,
        Get(mpsc::Sender<usize>),
        Stop,
    }

    struct Counter {
        value: usize,
    }

    impl Actor for Counter {
        type Msg = CounterMsg;

        fn receive(&mut self, ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
            match msg {
                CounterMsg::Increment => self.value += 1,
                CounterMsg::Get(reply_to) => {
                    reply_to
                        .send(self.value)
                        .map_err(|error| ActorError::Message(error.to_string()))?;
                }
                CounterMsg::Stop => ctx.stop(ctx.myself()),
            }
            Ok(())
        }
    }

    #[test]
    fn spawned_actor_receives_messages_in_tell_order() {
        let system = ActorSystem::builder("test").build().unwrap();
        let counter = system
            .spawn("counter", Props::new(|| Counter { value: 0 }))
            .unwrap();
        let (reply_tx, reply_rx) = mpsc::channel();

        counter.tell(CounterMsg::Increment).unwrap();
        counter.tell(CounterMsg::Increment).unwrap();
        counter.tell(CounterMsg::Get(reply_tx)).unwrap();

        assert_eq!(reply_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 2);
    }

    #[test]
    fn stop_prevents_later_user_message_delivery() {
        let system = ActorSystem::builder("test").build().unwrap();
        let counter = system
            .spawn("counter", Props::new(|| Counter { value: 0 }))
            .unwrap();

        counter.tell(CounterMsg::Stop).unwrap();

        let mut rejected = None;
        for _ in 0..100 {
            match counter.tell(CounterMsg::Increment) {
                Ok(()) => thread::sleep(Duration::from_millis(5)),
                Err(error) => {
                    rejected = Some(error);
                    break;
                }
            }
        }

        let error = rejected.expect("message sent after stop should be rejected");
        assert_eq!(error.reason(), "actor is stopped");
        assert!(
            system
                .dead_letters()
                .wait_for_len(1, Duration::from_secs(1))
        );

        let records = system.dead_letters().records();
        assert_eq!(records[0].recipient(), counter.path());
        assert_eq!(records[0].reason(), "actor is stopped");
    }

    struct StopProbe {
        stopped: mpsc::Sender<()>,
    }

    impl Actor for StopProbe {
        type Msg = ();

        fn receive(&mut self, _ctx: &mut Context<Self::Msg>, _msg: Self::Msg) -> ActorResult {
            Ok(())
        }

        fn stopped(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
            self.stopped
                .send(())
                .map_err(|error| ActorError::Message(error.to_string()))
        }
    }

    #[test]
    fn actor_system_stop_wakes_idle_actor() {
        let system = ActorSystem::builder("test").build().unwrap();
        let (stopped_tx, stopped_rx) = mpsc::channel();
        let actor = system
            .spawn(
                "probe",
                Props::new(move || StopProbe {
                    stopped: stopped_tx,
                }),
            )
            .unwrap();

        system.stop(&actor);

        stopped_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(actor.is_stopped());
    }

    struct BlockingStart {
        release: mpsc::Receiver<()>,
        received: Arc<AtomicU64>,
    }

    impl Actor for BlockingStart {
        type Msg = ();

        fn started(&mut self, _ctx: &mut Context<Self::Msg>) -> ActorResult {
            self.release
                .recv()
                .map_err(|error| ActorError::Message(error.to_string()))?;
            Ok(())
        }

        fn receive(&mut self, _ctx: &mut Context<Self::Msg>, _msg: Self::Msg) -> ActorResult {
            self.received.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    #[test]
    fn system_stop_drains_queued_user_messages_to_dead_letters() {
        let system = ActorSystem::builder("test").build().unwrap();
        let (release_tx, release_rx) = mpsc::channel();
        let received = Arc::new(AtomicU64::new(0));
        let actor = system
            .spawn(
                "blocked",
                Props::new({
                    let received = Arc::clone(&received);
                    move || BlockingStart {
                        release: release_rx,
                        received,
                    }
                }),
            )
            .unwrap();

        actor.tell(()).unwrap();
        actor.tell(()).unwrap();
        system.stop(&actor);
        release_tx.send(()).unwrap();

        assert!(
            system
                .dead_letters()
                .wait_for_len(2, Duration::from_secs(1))
        );
        assert_eq!(received.load(Ordering::Relaxed), 0);
        assert_eq!(system.dead_letters().records()[0].recipient(), actor.path());
    }
}
