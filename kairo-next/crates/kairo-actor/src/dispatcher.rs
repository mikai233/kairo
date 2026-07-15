use std::cell::Cell;
use std::collections::VecDeque;
use std::fmt;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};

use crate::error::ActorError;

thread_local! {
    static IS_DISPATCHER_WORKER: Cell<bool> = const { Cell::new(false) };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DispatcherSettings {
    throughput: usize,
    workers: usize,
}

impl DispatcherSettings {
    pub const DEFAULT_THROUGHPUT: usize = 5;

    pub fn new(throughput: usize) -> Self {
        Self {
            throughput,
            workers: default_worker_count(),
        }
    }

    pub fn with_workers(mut self, workers: usize) -> Self {
        self.workers = workers;
        self
    }

    pub fn throughput(&self) -> usize {
        self.throughput
    }

    pub fn workers(&self) -> usize {
        self.workers
    }
}

impl Default for DispatcherSettings {
    fn default() -> Self {
        Self {
            throughput: Self::DEFAULT_THROUGHPUT,
            workers: default_worker_count(),
        }
    }
}

fn default_worker_count() -> usize {
    thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
}

pub(crate) trait DispatchTask: Send + Sync + 'static {
    fn run(self: Arc<Self>);
}

#[derive(Clone)]
pub(crate) struct DispatcherHandle {
    queue: Arc<DispatchQueue>,
}

impl DispatcherHandle {
    pub(crate) fn execute(&self, task: Arc<dyn DispatchTask>) -> bool {
        let mut state = self.queue.state.lock().expect("dispatcher queue poisoned");
        if !state.accepting {
            return false;
        }
        state.tasks.push_back(task);
        self.queue.ready.notify_one();
        true
    }
}

pub(crate) struct Dispatcher {
    queue: Arc<DispatchQueue>,
    workers: Mutex<Vec<JoinHandle<()>>>,
}

impl Dispatcher {
    pub(crate) fn new(settings: DispatcherSettings) -> Result<Self, ActorError> {
        let queue = Arc::new(DispatchQueue::default());
        let mut workers = Vec::with_capacity(settings.workers());
        for index in 0..settings.workers() {
            let worker_queue = Arc::clone(&queue);
            match thread::Builder::new()
                .name(format!("kairo-dispatcher-{index}"))
                .spawn(move || run_worker(worker_queue))
            {
                Ok(worker) => workers.push(worker),
                Err(error) => {
                    stop_queue(&queue);
                    for worker in workers {
                        let _ = worker.join();
                    }
                    return Err(ActorError::Message(format!(
                        "failed to spawn dispatcher worker: {error}"
                    )));
                }
            }
        }
        Ok(Self {
            queue,
            workers: Mutex::new(workers),
        })
    }

    pub(crate) fn handle(&self) -> DispatcherHandle {
        DispatcherHandle {
            queue: Arc::clone(&self.queue),
        }
    }

    pub(crate) fn shutdown(&self) {
        stop_queue(&self.queue);
        let current = thread::current().id();
        let mut workers = self.workers.lock().expect("dispatcher workers poisoned");
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

    pub(crate) fn run_one(&self) -> bool {
        if !IS_DISPATCHER_WORKER.get() {
            return false;
        }
        let task = self
            .queue
            .state
            .lock()
            .expect("dispatcher queue poisoned")
            .tasks
            .pop_front();
        if let Some(task) = task {
            task.run();
            true
        } else {
            false
        }
    }
}

impl fmt::Debug for Dispatcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let worker_count = self
            .workers
            .lock()
            .expect("dispatcher workers poisoned")
            .len();
        f.debug_struct("Dispatcher")
            .field("workers", &worker_count)
            .finish_non_exhaustive()
    }
}

impl Drop for Dispatcher {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[derive(Default)]
struct DispatchQueue {
    state: Mutex<DispatchQueueState>,
    ready: Condvar,
}

#[derive(Default)]
struct DispatchQueueState {
    tasks: VecDeque<Arc<dyn DispatchTask>>,
    accepting: bool,
    shutdown: bool,
}

fn run_worker(queue: Arc<DispatchQueue>) {
    IS_DISPATCHER_WORKER.set(true);
    loop {
        let task = {
            let mut state = queue.state.lock().expect("dispatcher queue poisoned");
            loop {
                if let Some(task) = state.tasks.pop_front() {
                    break Some(task);
                }
                if state.shutdown {
                    break None;
                }
                state = queue.ready.wait(state).expect("dispatcher queue poisoned");
            }
        };
        let Some(task) = task else {
            return;
        };
        task.run();
    }
}

fn stop_queue(queue: &DispatchQueue) {
    let mut state = queue.state.lock().expect("dispatcher queue poisoned");
    state.accepting = false;
    state.shutdown = true;
    queue.ready.notify_all();
}

impl DispatchQueue {
    fn start(&self) {
        self.state
            .lock()
            .expect("dispatcher queue poisoned")
            .accepting = true;
    }
}

impl Dispatcher {
    pub(crate) fn start(&self) {
        self.queue.start();
    }
}
