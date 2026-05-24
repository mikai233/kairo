use std::collections::HashMap;
use std::fmt::{self, Formatter};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use crate::error::{ActorError, SendError};
use crate::refs::ActorRef;

pub const PHASE_BEFORE_SERVICE_UNBIND: &str = "before-service-unbind";
pub const PHASE_SERVICE_UNBIND: &str = "service-unbind";
pub const PHASE_SERVICE_REQUESTS_DONE: &str = "service-requests-done";
pub const PHASE_SERVICE_STOP: &str = "service-stop";
pub const PHASE_BEFORE_CLUSTER_SHUTDOWN: &str = "before-cluster-shutdown";
pub const PHASE_CLUSTER_SHARDING_SHUTDOWN_REGION: &str = "cluster-sharding-shutdown-region";
pub const PHASE_CLUSTER_LEAVE: &str = "cluster-leave";
pub const PHASE_CLUSTER_EXITING: &str = "cluster-exiting";
pub const PHASE_CLUSTER_EXITING_DONE: &str = "cluster-exiting-done";
pub const PHASE_CLUSTER_SHUTDOWN: &str = "cluster-shutdown";
pub const PHASE_BEFORE_ACTOR_SYSTEM_TERMINATE: &str = "before-actor-system-terminate";
pub const PHASE_ACTOR_SYSTEM_TERMINATE: &str = "actor-system-terminate";

const DEFAULT_PHASE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub struct CoordinatedShutdown {
    inner: Arc<CoordinatedShutdownInner>,
}

impl Default for CoordinatedShutdown {
    fn default() -> Self {
        Self {
            inner: Arc::new(CoordinatedShutdownInner {
                state: Mutex::new(ShutdownState::new(default_phases())),
                completed: Condvar::new(),
            }),
        }
    }
}

impl fmt::Debug for CoordinatedShutdown {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("CoordinatedShutdown")
            .field("reason", &self.reason())
            .finish_non_exhaustive()
    }
}

impl CoordinatedShutdown {
    pub fn reason(&self) -> Option<String> {
        self.inner
            .state
            .lock()
            .expect("coordinated shutdown poisoned")
            .reason
            .clone()
    }

    pub fn phases(&self) -> Vec<String> {
        self.inner
            .state
            .lock()
            .expect("coordinated shutdown poisoned")
            .phases
            .iter()
            .map(|phase| phase.name.clone())
            .collect()
    }

    pub fn add_task<F>(
        &self,
        phase: impl AsRef<str>,
        task_name: impl Into<String>,
        task: F,
    ) -> Result<(), ActorError>
    where
        F: FnOnce() -> Result<(), ActorError> + Send + 'static,
    {
        let task_name = task_name.into();
        if task_name.is_empty() {
            return Err(ActorError::InvalidShutdownTaskName);
        }
        let mut state = self
            .inner
            .state
            .lock()
            .expect("coordinated shutdown poisoned");
        ensure_phase(&state, phase.as_ref())?;
        state
            .tasks
            .entry(phase.as_ref().to_string())
            .or_default()
            .push(TaskEntry::new(task_name, task));
        Ok(())
    }

    pub fn add_actor_termination_task<M>(
        &self,
        phase: impl AsRef<str>,
        task_name: impl Into<String>,
        actor: ActorRef<M>,
        stop_message: Option<M>,
        timeout: Duration,
    ) -> Result<(), ActorError>
    where
        M: Send + 'static,
    {
        self.add_task(phase, task_name, move || {
            if let Some(message) = stop_message {
                actor.tell(message).map_err(send_error)?;
            } else {
                actor.request_stop();
            }
            if actor.wait_for_stop(timeout) {
                Ok(())
            } else {
                Err(ActorError::ShutdownTaskFailed(
                    "actor termination task timed out".to_string(),
                ))
            }
        })
    }

    pub fn run(&self, reason: impl Into<String>) -> Result<(), ActorError> {
        self.run_from(reason, None)
    }

    pub fn run_from(
        &self,
        reason: impl Into<String>,
        from_phase: Option<&str>,
    ) -> Result<(), ActorError> {
        let reason = reason.into();
        let phases = self.start_run(reason, from_phase)?;
        let result = self.run_phases(phases);
        self.complete_run(&result);
        result
    }

    fn start_run(
        &self,
        reason: String,
        from_phase: Option<&str>,
    ) -> Result<Vec<PhaseDefinition>, ActorError> {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("coordinated shutdown poisoned");

        if state.started {
            while !state.completed {
                state = self
                    .inner
                    .completed
                    .wait(state)
                    .expect("coordinated shutdown poisoned");
            }
            return state.result.clone().map_or(Ok(()), Err).map(|_| Vec::new());
        }

        if let Some(phase) = from_phase {
            ensure_phase(&state, phase)?;
        }
        state.started = true;
        state.reason = Some(reason);
        let phases = if let Some(from_phase) = from_phase {
            state
                .phases
                .iter()
                .skip_while(|phase| phase.name != from_phase)
                .cloned()
                .collect()
        } else {
            state.phases.clone()
        };
        Ok(phases)
    }

    fn run_phases(&self, phases: Vec<PhaseDefinition>) -> Result<(), ActorError> {
        for phase in phases {
            let tasks = {
                let mut state = self
                    .inner
                    .state
                    .lock()
                    .expect("coordinated shutdown poisoned");
                state.tasks.remove(&phase.name).unwrap_or_default()
            };

            run_phase(&phase, tasks)?;
        }
        Ok(())
    }

    fn complete_run(&self, result: &Result<(), ActorError>) {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("coordinated shutdown poisoned");
        state.completed = true;
        state.result = result.as_ref().err().cloned();
        self.inner.completed.notify_all();
    }
}

#[derive(Debug)]
struct CoordinatedShutdownInner {
    state: Mutex<ShutdownState>,
    completed: Condvar,
}

#[derive(Debug)]
struct ShutdownState {
    phases: Vec<PhaseDefinition>,
    tasks: HashMap<String, Vec<TaskEntry>>,
    started: bool,
    completed: bool,
    reason: Option<String>,
    result: Option<ActorError>,
}

impl ShutdownState {
    fn new(phases: Vec<PhaseDefinition>) -> Self {
        Self {
            phases,
            tasks: HashMap::new(),
            started: false,
            completed: false,
            reason: None,
            result: None,
        }
    }
}

#[derive(Debug, Clone)]
struct PhaseDefinition {
    name: String,
    timeout: Duration,
    recover: bool,
}

impl PhaseDefinition {
    fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            timeout: DEFAULT_PHASE_TIMEOUT,
            recover: false,
        }
    }
}

struct TaskEntry {
    name: String,
    task: Option<Box<dyn FnOnce() -> Result<(), ActorError> + Send>>,
}

impl TaskEntry {
    fn new(name: String, task: impl FnOnce() -> Result<(), ActorError> + Send + 'static) -> Self {
        Self {
            name,
            task: Some(Box::new(task)),
        }
    }

    fn run(mut self) -> Result<(), ActorError> {
        let task = self
            .task
            .take()
            .expect("coordinated shutdown task ran once");
        task().map_err(|error| {
            ActorError::ShutdownTaskFailed(format!("task `{}` failed: {error}", self.name))
        })
    }
}

impl fmt::Debug for TaskEntry {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskEntry")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

fn run_phase(phase: &PhaseDefinition, tasks: Vec<TaskEntry>) -> Result<(), ActorError> {
    if tasks.is_empty() {
        return Ok(());
    }

    let task_count = tasks.len();
    let (result_tx, result_rx) = mpsc::channel();
    for task in tasks {
        let result_tx = result_tx.clone();
        thread::Builder::new()
            .name(format!("kairo-shutdown-{}-{}", phase.name, task.name))
            .spawn(move || {
                let _ = result_tx.send(task.run());
            })
            .map_err(|error| ActorError::ShutdownTaskFailed(error.to_string()))?;
    }
    drop(result_tx);

    let deadline = Instant::now() + phase.timeout;
    for _ in 0..task_count {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or(Duration::ZERO);
        match result_rx.recv_timeout(remaining) {
            Ok(Ok(())) => {}
            Ok(Err(error)) if phase.recover => {
                drop(error);
            }
            Ok(Err(error)) => return Err(error),
            Err(mpsc::RecvTimeoutError::Timeout) if phase.recover => return Ok(()),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                return Err(ActorError::ShutdownPhaseTimeout {
                    phase: phase.name.clone(),
                    timeout: phase.timeout,
                });
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => return Ok(()),
        }
    }

    Ok(())
}

fn ensure_phase(state: &ShutdownState, phase: &str) -> Result<(), ActorError> {
    if state.phases.iter().any(|known| known.name == phase) {
        Ok(())
    } else {
        Err(ActorError::UnknownShutdownPhase(phase.to_string()))
    }
}

fn send_error<M>(error: SendError<M>) -> ActorError {
    ActorError::ShutdownTaskFailed(error.reason().to_string())
}

fn default_phases() -> Vec<PhaseDefinition> {
    [
        PHASE_BEFORE_SERVICE_UNBIND,
        PHASE_SERVICE_UNBIND,
        PHASE_SERVICE_REQUESTS_DONE,
        PHASE_SERVICE_STOP,
        PHASE_BEFORE_CLUSTER_SHUTDOWN,
        PHASE_CLUSTER_SHARDING_SHUTDOWN_REGION,
        PHASE_CLUSTER_LEAVE,
        PHASE_CLUSTER_EXITING,
        PHASE_CLUSTER_EXITING_DONE,
        PHASE_CLUSTER_SHUTDOWN,
        PHASE_BEFORE_ACTOR_SYSTEM_TERMINATE,
        PHASE_ACTOR_SYSTEM_TERMINATE,
    ]
    .into_iter()
    .map(PhaseDefinition::new)
    .collect()
}
