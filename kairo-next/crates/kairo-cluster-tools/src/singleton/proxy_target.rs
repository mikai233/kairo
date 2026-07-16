#![deny(missing_docs)]

use std::fmt::{self, Formatter};
use std::sync::Arc;

use kairo_actor::{ActorPath, ActorRef, Recipient, SendError};
use kairo_remote::RemoteActorRef;
use kairo_serialization::RemoteMessage;

/// Typed local, remote, or custom delivery endpoint for a singleton proxy.
///
/// Every target carries a stable actor path for identity and diagnostics.
/// Only targets created with [`Self::local`] expose a watchable local actor
/// reference; remote and custom recipients rely on connector-driven refresh.
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
    /// Wraps a watchable local typed actor reference.
    pub fn local(actor_ref: ActorRef<M>) -> Self {
        Self {
            path: actor_ref.path().clone(),
            recipient: Arc::new(actor_ref.clone()),
            watchable: Some(actor_ref),
        }
    }

    /// Wraps a typed remote actor reference.
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

    /// Wraps a custom typed recipient associated with `path`.
    ///
    /// The caller must ensure the supplied path identifies the recipient for
    /// route replacement and diagnostics. Custom recipients are not watchable.
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

    /// Returns the target's exact actor path.
    pub fn path(&self) -> &ActorPath {
        &self.path
    }

    /// Returns the local actor reference when this target is watchable.
    pub fn watchable(&self) -> Option<&ActorRef<M>> {
        self.watchable.as_ref()
    }

    /// Sends one message through the wrapped typed recipient.
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
