use std::fmt::{self, Formatter};
use std::sync::Arc;

use kairo_actor::{ActorPath, ActorRef, Recipient, SendError};
use kairo_remote::RemoteActorRef;
use kairo_serialization::RemoteMessage;

pub struct SingletonProxyTarget<M>
where
    M: Send + 'static,
{
    path: ActorPath,
    recipient: Arc<dyn Recipient<M> + Send + Sync>,
    watchable: Option<ActorRef<M>>,
}

impl<M> SingletonProxyTarget<M>
where
    M: Send + 'static,
{
    pub fn local(actor_ref: ActorRef<M>) -> Self {
        Self {
            path: actor_ref.path().clone(),
            recipient: Arc::new(actor_ref.clone()),
            watchable: Some(actor_ref),
        }
    }

    pub fn remote(remote_ref: RemoteActorRef<M>) -> Self
    where
        M: RemoteMessage,
    {
        Self {
            path: remote_ref.path().clone(),
            recipient: Arc::new(remote_ref),
            watchable: None,
        }
    }

    pub fn from_recipient(
        path: ActorPath,
        recipient: impl Recipient<M> + Send + Sync + 'static,
    ) -> Self {
        Self {
            path,
            recipient: Arc::new(recipient),
            watchable: None,
        }
    }

    pub fn path(&self) -> &ActorPath {
        &self.path
    }

    pub fn watchable(&self) -> Option<&ActorRef<M>> {
        self.watchable.as_ref()
    }

    pub fn tell(&self, message: M) -> Result<(), SendError<M>> {
        self.recipient.tell(message)
    }
}

impl<M> Clone for SingletonProxyTarget<M>
where
    M: Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            path: self.path.clone(),
            recipient: Arc::clone(&self.recipient),
            watchable: self.watchable.clone(),
        }
    }
}

impl<M> fmt::Debug for SingletonProxyTarget<M>
where
    M: Send + 'static,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("SingletonProxyTarget")
            .field("path", &self.path)
            .field("watchable", &self.watchable.is_some())
            .finish()
    }
}
