use std::fmt::{self, Formatter};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use crate::error::ActorError;

use super::PhaseDefinition;

const TASK_PENDING: u8 = 0;
const TASK_CANCELLED: u8 = 1;
const TASK_RUNNING: u8 = 2;
const TASK_DONE: u8 = 3;

/// Handle for a coordinated-shutdown task registration.
#[derive(Clone, Debug)]
pub struct ShutdownTaskHandle {
    state: Arc<AtomicU8>,
}

impl ShutdownTaskHandle {
    /// Cancel the task if it has not started yet.
    pub fn cancel(&self) -> bool {
        self.state
            .compare_exchange(
                TASK_PENDING,
                TASK_CANCELLED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    /// Returns true when this registration was cancelled before running.
    pub fn is_cancelled(&self) -> bool {
        self.state.load(Ordering::Acquire) == TASK_CANCELLED
    }

    /// Returns true after the task has started or completed.
    pub fn is_running_or_done(&self) -> bool {
        matches!(self.state.load(Ordering::Acquire), TASK_RUNNING | TASK_DONE)
    }
}

pub(super) struct TaskEntry {
    pub(super) name: String,
    state: Arc<AtomicU8>,
    task: Option<Box<dyn FnOnce() -> Result<(), ActorError> + Send>>,
}

impl TaskEntry {
    pub(super) fn new(
        name: String,
        task: impl FnOnce() -> Result<(), ActorError> + Send + 'static,
    ) -> Self {
        Self {
            name,
            state: Arc::new(AtomicU8::new(TASK_PENDING)),
            task: Some(Box::new(task)),
        }
    }

    pub(super) fn handle(&self) -> ShutdownTaskHandle {
        ShutdownTaskHandle {
            state: Arc::clone(&self.state),
        }
    }

    fn run(mut self) -> Result<(), ActorError> {
        match self.state.compare_exchange(
            TASK_PENDING,
            TASK_RUNNING,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {}
            Err(TASK_CANCELLED) => return Ok(()),
            Err(_) => {
                return Err(ActorError::ShutdownTaskFailed(format!(
                    "task `{}` was already started",
                    self.name
                )));
            }
        }
        let task = self
            .task
            .take()
            .expect("coordinated shutdown task ran once");
        let result = task().map_err(|error| {
            ActorError::ShutdownTaskFailed(format!("task `{}` failed: {error}", self.name))
        });
        self.state.store(TASK_DONE, Ordering::Release);
        result
    }
}

impl fmt::Debug for TaskEntry {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskEntry")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

pub(super) fn run_phase(phase: &PhaseDefinition, tasks: Vec<TaskEntry>) -> Result<(), ActorError> {
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn non_recovering_phase_reports_timeout() {
        let phase = PhaseDefinition {
            name: "test-phase".to_string(),
            timeout: Duration::from_millis(20),
            recover: false,
        };
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let task = TaskEntry::new("blocked".to_string(), move || {
            started_tx
                .send(())
                .map_err(|error| ActorError::Message(error.to_string()))?;
            release_rx
                .recv()
                .map_err(|error| ActorError::Message(error.to_string()))?;
            Ok(())
        });

        let result = run_phase(&phase, vec![task]);

        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(matches!(
            result,
            Err(ActorError::ShutdownPhaseTimeout { phase, timeout })
                if phase == "test-phase" && timeout == Duration::from_millis(20)
        ));
        release_tx.send(()).unwrap();
    }

    #[test]
    fn recovering_phase_ignores_task_failure() {
        let phase = PhaseDefinition {
            name: "recovering-phase".to_string(),
            timeout: Duration::from_secs(1),
            recover: true,
        };
        let task = TaskEntry::new("failing".to_string(), || {
            Err(ActorError::Message("boom".to_string()))
        });

        let result = run_phase(&phase, vec![task]);

        assert!(result.is_ok());
    }

    #[test]
    fn recovering_phase_timeout_completes_ok() {
        let phase = PhaseDefinition {
            name: "recovering-phase".to_string(),
            timeout: Duration::from_millis(20),
            recover: true,
        };
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let task = TaskEntry::new("blocked".to_string(), move || {
            started_tx
                .send(())
                .map_err(|error| ActorError::Message(error.to_string()))?;
            release_rx
                .recv()
                .map_err(|error| ActorError::Message(error.to_string()))?;
            Ok(())
        });

        let result = run_phase(&phase, vec![task]);

        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(result.is_ok());
        release_tx.send(()).unwrap();
    }
}
