use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::error::ActorError;
use crate::path::ActorPath;
use crate::refs::{ActorRef, TerminationLatch};
use crate::system::ActorSystem;

#[derive(Clone, Debug, Default)]
pub(crate) struct AdapterScope {
    lifecycles: Arc<Mutex<Vec<AdapterLifecycle>>>,
}

impl AdapterScope {
    pub(crate) fn register(&self, path: ActorPath) -> (Arc<AtomicBool>, Arc<TerminationLatch>) {
        let stopped = Arc::new(AtomicBool::new(false));
        let terminated = Arc::new(TerminationLatch::default());
        self.lifecycles
            .lock()
            .expect("adapter lifecycle registry poisoned")
            .push(AdapterLifecycle {
                path,
                stopped: Arc::clone(&stopped),
                terminated: Arc::clone(&terminated),
            });
        (stopped, terminated)
    }

    pub(crate) fn stop_all(&self) -> Vec<ActorPath> {
        let lifecycles = self
            .lifecycles
            .lock()
            .expect("adapter lifecycle registry poisoned")
            .drain(..)
            .collect::<Vec<_>>();
        for lifecycle in &lifecycles {
            lifecycle.stopped.store(true, Ordering::Release);
            lifecycle.terminated.mark_stopped();
        }
        lifecycles
            .into_iter()
            .map(|lifecycle| lifecycle.path)
            .collect()
    }
}

#[derive(Clone, Debug)]
struct AdapterLifecycle {
    path: ActorPath,
    stopped: Arc<AtomicBool>,
    terminated: Arc<TerminationLatch>,
}

pub(crate) fn message_adapter<M, U, F>(
    system: &ActorSystem,
    scope: &AdapterScope,
    owner: ActorRef<M>,
    map: F,
) -> Result<ActorRef<U>, ActorError>
where
    M: Send + 'static,
    U: Send + 'static,
    F: FnMut(U) -> M + Send + 'static,
{
    let path = system.next_adapter_path(owner.path())?;
    let (stopped, terminated) = scope.register(path.clone());
    Ok(ActorRef::adapter(path, owner, stopped, terminated, map))
}
