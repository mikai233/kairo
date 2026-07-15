use std::any::Any;
use std::collections::VecDeque;
use std::fmt::{self, Formatter};
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};

use crate::error::ActorError;
use crate::refs::ActorRef;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Configuration for the actor-system task executor.
pub struct TaskExecutorSettings {
    workers: usize,
    queue_capacity: usize,
}

impl TaskExecutorSettings {
    /// Default maximum number of tasks waiting for an executor worker.
    pub const DEFAULT_QUEUE_CAPACITY: usize = 1_024;

    /// Creates settings with an explicit worker count and queue capacity.
    pub fn new(workers: usize, queue_capacity: usize) -> Self {
        Self {
            workers,
            queue_capacity,
        }
    }

    /// Replaces the task-executor worker count.
    pub fn with_workers(mut self, workers: usize) -> Self {
        self.workers = workers;
        self
    }

    /// Replaces the pending-task queue capacity.
    pub fn with_queue_capacity(mut self, queue_capacity: usize) -> Self {
        self.queue_capacity = queue_capacity;
        self
    }

    /// Returns the task-executor worker count.
    pub fn workers(&self) -> usize {
        self.workers
    }

    /// Returns the maximum number of pending tasks.
    pub fn queue_capacity(&self) -> usize {
        self.queue_capacity
    }
}

impl Default for TaskExecutorSettings {
    fn default() -> Self {
        Self {
            workers: thread::available_parallelism()
                .map(usize::from)
                .unwrap_or(1),
            queue_capacity: Self::DEFAULT_QUEUE_CAPACITY,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct TaskScope {
    generation: Arc<AtomicU64>,
}

impl TaskScope {
    pub(crate) fn cancel_current(&self) {
        self.generation.fetch_add(1, Ordering::AcqRel);
    }

    pub(crate) fn scoped_ref<M>(&self, target: ActorRef<M>) -> ActorRef<M>
    where
        M: Send + 'static,
    {
        let expected_generation = self.generation.load(Ordering::Acquire);
        let generation = Arc::clone(&self.generation);
        let owner = target.clone();
        let dead_letters = target.dead_letters.clone();
        let task_path = target.path().clone();
        ActorRef::function(
            target.path().clone(),
            target.dead_letters.clone(),
            Arc::clone(&target.target.stopped),
            Arc::clone(&target.target.terminated),
            "actor is stopped",
            move |message| {
                if generation.load(Ordering::Acquire) != expected_generation {
                    dead_letters.publish::<M>(task_path.clone(), "actor task is cancelled");
                    return Err(crate::error::SendError {
                        message,
                        reason: "actor task is cancelled".to_string(),
                    });
                }
                owner.tell(message)
            },
        )
    }
}

/// Handle used to observe and join an actor-system helper task.
pub struct TaskHandle {
    completion: Arc<TaskCompletion>,
}

impl TaskHandle {
    /// Returns whether the task has finished or panicked.
    pub fn is_finished(&self) -> bool {
        self.completion.is_finished()
    }

    /// Waits for completion and propagates a task panic as the error value.
    pub fn join(self) -> thread::Result<()> {
        self.completion.join()
    }
}

impl fmt::Debug for TaskHandle {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskHandle")
            .field("finished", &self.is_finished())
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub(crate) struct TaskExecutorHandle {
    queue: Arc<TaskQueue>,
    settings: TaskExecutorSettings,
    workers: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl TaskExecutorHandle {
    fn spawn(&self, task: impl FnOnce() + Send + 'static) -> Result<TaskHandle, ActorError> {
        {
            let state = self
                .queue
                .state
                .lock()
                .expect("task executor queue poisoned");
            if !state.accepting {
                return Err(ActorError::TaskSpawn(
                    "task executor is shutting down".to_string(),
                ));
            }
        }
        ensure_workers_started(self.settings, &self.queue, &self.workers)?;
        let completion = Arc::new(TaskCompletion::default());
        let mut state = self
            .queue
            .state
            .lock()
            .expect("task executor queue poisoned");
        if !state.accepting {
            return Err(ActorError::TaskSpawn(
                "task executor is shutting down".to_string(),
            ));
        }
        if state.jobs.len() >= self.queue.capacity {
            return Err(ActorError::TaskSpawn(format!(
                "task executor queue is full at capacity {}",
                self.queue.capacity
            )));
        }
        state.jobs.push_back(TaskJob {
            task: Some(Box::new(task)),
            completion: Arc::clone(&completion),
        });
        self.queue.ready.notify_one();
        Ok(TaskHandle { completion })
    }
}

pub(crate) struct TaskExecutor {
    settings: TaskExecutorSettings,
    queue: Arc<TaskQueue>,
    workers: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl TaskExecutor {
    pub(crate) fn new(settings: TaskExecutorSettings) -> Result<Self, ActorError> {
        let queue = Arc::new(TaskQueue {
            capacity: settings.queue_capacity(),
            state: Mutex::new(TaskQueueState {
                jobs: VecDeque::new(),
                accepting: true,
                shutdown: false,
            }),
            ready: Condvar::new(),
        });
        Ok(Self {
            settings,
            queue,
            workers: Arc::new(Mutex::new(Vec::new())),
        })
    }

    pub(crate) fn settings(&self) -> TaskExecutorSettings {
        self.settings
    }

    pub(crate) fn handle(&self) -> TaskExecutorHandle {
        TaskExecutorHandle {
            queue: Arc::clone(&self.queue),
            settings: self.settings,
            workers: Arc::clone(&self.workers),
        }
    }

    pub(crate) fn shutdown(&self) {
        stop_queue(&self.queue);
    }

    fn join_workers(&self) {
        let current = thread::current().id();
        let mut workers = self.workers.lock().expect("task executor workers poisoned");
        let mut remaining = Vec::new();
        for worker in workers.drain(..) {
            if worker.thread().id() == current {
                remaining.push(worker);
            } else {
                let _ = worker.join();
            }
        }
        *workers = remaining;
    }
}

impl fmt::Debug for TaskExecutor {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskExecutor")
            .field("settings", &self.settings)
            .finish_non_exhaustive()
    }
}

impl Drop for TaskExecutor {
    fn drop(&mut self) {
        self.shutdown();
        self.join_workers();
    }
}

struct TaskQueue {
    capacity: usize,
    state: Mutex<TaskQueueState>,
    ready: Condvar,
}

struct TaskQueueState {
    jobs: VecDeque<TaskJob>,
    accepting: bool,
    shutdown: bool,
}

struct TaskJob {
    task: Option<Box<dyn FnOnce() + Send>>,
    completion: Arc<TaskCompletion>,
}

impl TaskJob {
    fn run(mut self) {
        let task = self.task.take().expect("task job may only run once");
        let result = panic::catch_unwind(AssertUnwindSafe(task));
        self.completion.finish(result);
    }
}

#[derive(Default)]
struct TaskCompletion {
    result: Mutex<Option<thread::Result<()>>>,
    completed: Condvar,
}

impl TaskCompletion {
    fn finish(&self, result: Result<(), Box<dyn Any + Send>>) {
        *self.result.lock().expect("task completion poisoned") = Some(result);
        self.completed.notify_all();
    }

    fn is_finished(&self) -> bool {
        self.result
            .lock()
            .expect("task completion poisoned")
            .is_some()
    }

    fn join(&self) -> thread::Result<()> {
        let mut result = self.result.lock().expect("task completion poisoned");
        while result.is_none() {
            result = self
                .completed
                .wait(result)
                .expect("task completion poisoned");
        }
        result.take().expect("task completion missing result")
    }
}

fn run_worker(queue: Arc<TaskQueue>) {
    loop {
        let job = {
            let mut state = queue.state.lock().expect("task executor queue poisoned");
            loop {
                if let Some(job) = state.jobs.pop_front() {
                    break Some(job);
                }
                if state.shutdown {
                    break None;
                }
                state = queue
                    .ready
                    .wait(state)
                    .expect("task executor queue poisoned");
            }
        };
        let Some(job) = job else {
            return;
        };
        job.run();
    }
}

fn ensure_workers_started(
    settings: TaskExecutorSettings,
    queue: &Arc<TaskQueue>,
    workers: &Mutex<Vec<JoinHandle<()>>>,
) -> Result<(), ActorError> {
    let mut workers = workers.lock().expect("task executor workers poisoned");
    if !workers.is_empty() {
        return Ok(());
    }
    let mut started = Vec::with_capacity(settings.workers());
    for index in 0..settings.workers() {
        let worker_queue = Arc::clone(queue);
        match thread::Builder::new()
            .name(format!("kairo-task-executor-{index}"))
            .spawn(move || run_worker(worker_queue))
        {
            Ok(worker) => started.push(worker),
            Err(error) => {
                stop_queue(queue);
                drop(workers);
                for worker in started {
                    let _ = worker.join();
                }
                return Err(ActorError::TaskSpawn(format!(
                    "failed to spawn task executor worker: {error}"
                )));
            }
        }
    }
    workers.extend(started);
    Ok(())
}

fn stop_queue(queue: &TaskQueue) {
    let mut state = queue.state.lock().expect("task executor queue poisoned");
    state.accepting = false;
    state.shutdown = true;
    queue.ready.notify_all();
}

pub(crate) fn spawn_task<M, F>(
    executor: TaskExecutorHandle,
    target: ActorRef<M>,
    task: F,
) -> Result<TaskHandle, ActorError>
where
    M: Send + 'static,
    F: FnOnce(ActorRef<M>) + Send + 'static,
{
    executor.spawn(move || task(target))
}

pub(crate) fn pipe_to_self<M, T, E, F, Map>(
    executor: TaskExecutorHandle,
    target: ActorRef<M>,
    task: F,
    map: Map,
) -> Result<TaskHandle, ActorError>
where
    M: Send + 'static,
    T: Send + 'static,
    E: Send + 'static,
    F: FnOnce() -> Result<T, E> + Send + 'static,
    Map: FnOnce(Result<T, E>) -> M + Send + 'static,
{
    executor.spawn(move || {
        let message = map(task());
        let _ = target.tell(message);
    })
}

#[cfg(test)]
mod executor_tests {
    use std::sync::mpsc;
    use std::time::Duration;

    use super::*;

    #[test]
    fn bounded_executor_rejects_work_without_blocking_when_queue_is_full() {
        let executor = TaskExecutor::new(TaskExecutorSettings::new(1, 1)).unwrap();
        let handle = executor.handle();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let running = handle
            .spawn(move || {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            })
            .unwrap();
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let queued = handle.spawn(|| {}).unwrap();

        let error = handle.spawn(|| {}).unwrap_err();
        assert!(matches!(
            error,
            ActorError::TaskSpawn(reason)
                if reason == "task executor queue is full at capacity 1"
        ));

        release_tx.send(()).unwrap();
        running.join().unwrap();
        queued.join().unwrap();
    }

    #[test]
    fn task_panic_is_reported_by_handle_without_losing_worker() {
        let executor = TaskExecutor::new(TaskExecutorSettings::new(1, 2)).unwrap();
        let handle = executor.handle();
        let panicked = handle.spawn(|| panic!("task failed")).unwrap();
        assert!(panicked.join().is_err());

        let (done_tx, done_rx) = mpsc::channel();
        let recovered = handle.spawn(move || done_tx.send(()).unwrap()).unwrap();
        recovered.join().unwrap();
        done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn executor_shutdown_rejects_new_work() {
        let executor = TaskExecutor::new(TaskExecutorSettings::new(1, 1)).unwrap();
        let handle = executor.handle();
        assert!(executor.workers.lock().unwrap().is_empty());
        executor.shutdown();

        let error = handle.spawn(|| {}).unwrap_err();
        assert!(matches!(
            error,
            ActorError::TaskSpawn(reason) if reason == "task executor is shutting down"
        ));
        assert!(executor.workers.lock().unwrap().is_empty());
    }

    #[test]
    fn executor_starts_configured_workers_on_first_task() {
        let executor = TaskExecutor::new(TaskExecutorSettings::new(2, 1)).unwrap();
        assert!(executor.workers.lock().unwrap().is_empty());

        executor.handle().spawn(|| {}).unwrap().join().unwrap();

        assert_eq!(executor.workers.lock().unwrap().len(), 2);
    }
}
