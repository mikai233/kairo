use crate::error::ActorError;
use crate::refs::ActorRef;
use crate::system::ActorSystem;

pub(crate) fn message_adapter<M, U, F>(
    system: &ActorSystem,
    owner: ActorRef<M>,
    map: F,
) -> Result<ActorRef<U>, ActorError>
where
    M: Send + 'static,
    U: Send + 'static,
    F: FnMut(U) -> M + Send + 'static,
{
    let path = system.next_adapter_path(owner.path())?;
    Ok(ActorRef::adapter(path, owner, map))
}
