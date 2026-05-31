use std::fmt::{self, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::{self, JoinHandle};

use crate::error::ActorError;
use crate::refs::ActorRef;

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
