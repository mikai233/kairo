use std::fmt::{self, Formatter};
use std::sync::Arc;
use std::time::Duration;

use crate::error::{ActorError, SendError};
use crate::refs::ActorRef;

mod phase;
mod state;
mod task;

use phase::{PhaseDefinition, default_phases, ensure_phase};
use state::{CoordinatedShutdownInner, ShutdownState};
pub use task::ShutdownTaskHandle;
use task::{TaskEntry, run_phase};

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

#[derive(Clone)]
pub struct CoordinatedShutdown {
    inner: Arc<CoordinatedShutdownInner>,
}

impl Default for CoordinatedShutdown {
    fn default() -> Self {
        Self {
            inner: Arc::new(CoordinatedShutdownInner::new(default_phases())),
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
        self.add_cancellable_task(phase, task_name, task)
            .map(|_| ())
    }

    /// Register a shutdown task and return a handle that can cancel it before
    /// the phase starts running.
    ///
    /// Duplicate task names are distinct registrations, matching Pekko's
    /// observable coordinated-shutdown behavior.
    pub fn add_cancellable_task<F>(
        &self,
        phase: impl AsRef<str>,
        task_name: impl Into<String>,
        task: F,
    ) -> Result<ShutdownTaskHandle, ActorError>
    where
        F: FnOnce() -> Result<(), ActorError> + Send + 'static,
    {
        let task_name = task_name.into();
        if task_name.is_empty() {
            return Err(ActorError::InvalidShutdownTaskName);
        }
        let task = TaskEntry::new(task_name, task);
        let handle = task.handle();
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
            .push(task);
        Ok(handle)
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
            if let Some(message) = stop_message
                && !actor.is_stopped()
                && let Err(error) = actor.tell(message)
                && !actor.is_stopped()
            {
                return Err(send_error(error));
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

fn send_error<M>(error: SendError<M>) -> ActorError {
    ActorError::ShutdownTaskFailed(error.reason().to_string())
}
