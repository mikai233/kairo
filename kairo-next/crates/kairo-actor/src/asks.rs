use std::fmt::{self, Display, Formatter};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crate::error::{ActorError, SendError};
use crate::refs::{ActorRef, TerminationLatch};
use crate::system::ActorSystem;

pub type AskResult<M> = Result<M, AskError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AskError {
    Timeout { timeout: Duration },
}

impl Display for AskError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Timeout { timeout } => write!(f, "ask timed out after {timeout:?}"),
        }
    }
}

impl std::error::Error for AskError {}

#[derive(Clone, Default)]
pub(crate) struct AskScope {
    registrations: Arc<Mutex<Vec<AskRegistration>>>,
}

impl fmt::Debug for AskScope {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let pending = self.registrations.lock().expect("ask scope poisoned").len();
        f.debug_struct("AskScope")
            .field("registrations", &pending)
            .finish_non_exhaustive()
    }
}

impl AskScope {
    pub(crate) fn register(&self, registration: AskRegistration) {
        self.registrations
            .lock()
            .expect("ask scope poisoned")
            .push(registration);
    }

    pub(crate) fn cancel_current(&self) {
        let registrations =
            std::mem::take(&mut *self.registrations.lock().expect("ask scope poisoned"));
        for registration in registrations {
            registration.cancel();
        }
    }
}

pub(crate) struct AskRegistration {
    system: ActorSystem,
    path: crate::path::ActorPath,
    state: Arc<AskState>,
    stopped: Arc<AtomicBool>,
    terminated: Arc<TerminationLatch>,
}

impl AskRegistration {
    fn cancel(self) {
        if self.state.complete() {
            self.stopped.store(true, Ordering::Release);
            self.terminated.mark_stopped();
            self.system.unregister_temp_ref(&self.path);
        }
    }
}

pub(crate) fn ask<M, Req, Res, Create, Map>(
    system: &ActorSystem,
    scope: &AskScope,
    owner: ActorRef<M>,
    target: ActorRef<Req>,
    timeout: Duration,
    create_request: Create,
    map_response: Map,
) -> Result<(), ActorError>
where
    M: Send + 'static,
    Req: Send + 'static,
    Res: Send + 'static,
    Create: FnOnce(ActorRef<Res>) -> Req,
    Map: FnOnce(AskResult<Res>) -> M + Send + 'static,
{
    let path = system.next_ask_path()?;
    let owner_mailbox = owner
        .target
        .mailbox
        .clone()
        .expect("ask requires a live local owner mailbox");
    let owner_path = owner.path().clone();
    let owner_stopped = Arc::clone(&owner.target.stopped);
    let dead_letters = owner.dead_letters.clone();
    let temp_registry = system.clone();
    let state = Arc::new(AskState::default());
    let stopped = Arc::new(AtomicBool::new(false));
    let terminated = Arc::new(TerminationLatch::default());
    let map_response = Arc::new(Mutex::new(Some(map_response)));

    let reply_ref = {
        let ask_path = path.clone();
        let ask_state = Arc::clone(&state);
        let reply_stopped = Arc::clone(&stopped);
        let reply_terminated = Arc::clone(&terminated);
        let reply_dead_letters = dead_letters.clone();
        let mapper = Arc::clone(&map_response);
        let mailbox = owner_mailbox.clone();
        let owner_stopped = Arc::clone(&owner_stopped);
        let temp_registry = temp_registry.clone();
        ActorRef::function(
            path,
            dead_letters.clone(),
            stopped.clone(),
            terminated.clone(),
            "ask is completed",
            move |reply: Res| {
                if owner_stopped.load(Ordering::Acquire) {
                    reply_dead_letters.publish::<Res>(ask_path.clone(), "actor is stopped");
                    return Err(SendError {
                        message: reply,
                        reason: "actor is stopped".to_string(),
                    });
                }

                if !ask_state.complete() {
                    reply_dead_letters.publish::<Res>(ask_path.clone(), "ask is completed");
                    return Err(SendError {
                        message: reply,
                        reason: "ask is completed".to_string(),
                    });
                }

                reply_stopped.store(true, Ordering::Release);
                reply_terminated.mark_stopped();
                temp_registry.unregister_temp_ref(&ask_path);
                enqueue_ask_result(
                    &mailbox,
                    AskResult::Ok(reply),
                    Arc::clone(&mapper),
                    &reply_dead_letters,
                    &owner_path,
                )
                .map_err(|result| {
                    let Ok(message) = result else {
                        unreachable!("reply path only enqueues successful ask results");
                    };
                    SendError {
                        message,
                        reason: "actor mailbox is closed".to_string(),
                    }
                })
            },
        )
    };
    system.register_temp_ref(reply_ref.clone());
    scope.register(AskRegistration {
        system: system.clone(),
        path: reply_ref.path().clone(),
        state: Arc::clone(&state),
        stopped: Arc::clone(&stopped),
        terminated: Arc::clone(&terminated),
    });

    spawn_timeout(TimeoutTask {
        timeout,
        owner_mailbox,
        owner_path: owner.path().clone(),
        owner_stopped,
        dead_letters,
        state: Arc::clone(&state),
        stopped: Arc::clone(&stopped),
        terminated: Arc::clone(&terminated),
        temp_registry: system.clone(),
        temp_path: reply_ref.path().clone(),
        map_response,
        _response: std::marker::PhantomData,
    })?;

    match target.tell(create_request(reply_ref.clone())) {
        Ok(()) => Ok(()),
        Err(error) => {
            state.complete();
            stopped.store(true, Ordering::Release);
            terminated.mark_stopped();
            system.unregister_temp_ref(reply_ref.path());
            Err(ActorError::AskSend(error.reason().to_string()))
        }
    }
}

struct TimeoutTask<M, Res, Map> {
    timeout: Duration,
    owner_mailbox: Arc<crate::mailbox::Mailbox<M>>,
    owner_path: crate::path::ActorPath,
    owner_stopped: Arc<AtomicBool>,
    dead_letters: crate::dead_letters::DeadLetters,
    state: Arc<AskState>,
    stopped: Arc<AtomicBool>,
    terminated: Arc<TerminationLatch>,
    temp_registry: ActorSystem,
    temp_path: crate::path::ActorPath,
    map_response: Arc<Mutex<Option<Map>>>,
    _response: std::marker::PhantomData<fn(Res)>,
}

fn spawn_timeout<M, Res, Map>(task: TimeoutTask<M, Res, Map>) -> Result<(), ActorError>
where
    M: Send + 'static,
    Res: Send + 'static,
    Map: FnOnce(AskResult<Res>) -> M + Send + 'static,
{
    thread::Builder::new()
        .name("kairo-ask-timeout".to_string())
        .spawn(move || {
            thread::sleep(task.timeout);
            if !task.state.complete() {
                return;
            }

            task.stopped.store(true, Ordering::Release);
            task.terminated.mark_stopped();
            task.temp_registry.unregister_temp_ref(&task.temp_path);
            if task.owner_stopped.load(Ordering::Acquire) {
                return;
            }

            let _ = enqueue_ask_result(
                &task.owner_mailbox,
                AskResult::Err(AskError::Timeout {
                    timeout: task.timeout,
                }),
                task.map_response,
                &task.dead_letters,
                &task.owner_path,
            );
        })
        .map_err(|error| ActorError::TaskSpawn(error.to_string()))?;
    Ok(())
}

fn enqueue_ask_result<M, Res, Map>(
    mailbox: &crate::mailbox::Mailbox<M>,
    result: AskResult<Res>,
    map_response: Arc<Mutex<Option<Map>>>,
    dead_letters: &crate::dead_letters::DeadLetters,
    owner_path: &crate::path::ActorPath,
) -> Result<(), AskResult<Res>>
where
    M: Send + 'static,
    Res: Send + 'static,
    Map: FnOnce(AskResult<Res>) -> M + Send + 'static,
{
    mailbox
        .enqueue_adapted(result, move |result| {
            let map_response = map_response
                .lock()
                .expect("ask response mapper poisoned")
                .take()
                .expect("ask response mapper must run at most once");
            Some(map_response(result))
        })
        .inspect_err(|_result| {
            dead_letters.publish::<M>(owner_path.clone(), "actor mailbox is closed");
        })
}

#[derive(Debug, Default)]
struct AskState {
    completed: AtomicBool,
}

impl AskState {
    fn complete(&self) -> bool {
        !self.completed.swap(true, Ordering::AcqRel)
    }
}
