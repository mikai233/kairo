use std::fmt::{self, Formatter};
use std::thread::{self, JoinHandle};

use crate::error::ActorError;
use crate::refs::ActorRef;

pub struct TaskHandle {
    join: Option<JoinHandle<()>>,
}

impl TaskHandle {
    fn spawn(name: String, task: impl FnOnce() + Send + 'static) -> Result<Self, ActorError> {
        let join = thread::Builder::new()
            .name(name)
            .spawn(task)
            .map_err(|error| ActorError::TaskSpawn(error.to_string()))?;
        Ok(Self { join: Some(join) })
    }

    pub fn is_finished(&self) -> bool {
        match &self.join {
            Some(join) => join.is_finished(),
            None => true,
        }
    }

    pub fn join(mut self) -> thread::Result<()> {
        self.join
            .take()
            .expect("task handle must contain a join handle")
            .join()
    }
}

impl fmt::Debug for TaskHandle {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskHandle")
            .field("finished", &self.is_finished())
            .finish_non_exhaustive()
    }
}

pub(crate) fn spawn_task<M, F>(target: ActorRef<M>, task: F) -> Result<TaskHandle, ActorError>
where
    M: Send + 'static,
    F: FnOnce(ActorRef<M>) + Send + 'static,
{
    let name = task_thread_name("task", &target);
    TaskHandle::spawn(name, move || task(target))
}

pub(crate) fn pipe_to_self<M, T, E, F, Map>(
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
    let name = task_thread_name("pipe", &target);
    TaskHandle::spawn(name, move || {
        let message = map(task());
        let _ = target.tell(message);
    })
}

fn task_thread_name<M: Send + 'static>(kind: &str, target: &ActorRef<M>) -> String {
    let actor_name = target.path().name().unwrap_or("actor");
    format!("kairo-{kind}-{actor_name}")
}
