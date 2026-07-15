#![deny(missing_docs)]

use kairo_actor::{ActorPath, ActorRef, Recipient, SendError};
use kairo_serialization::RemoteMessage;

use crate::RemoteActorRef;

/// A typed actor reference resolved to either the local actor registry or a
/// remote transport.
pub enum ResolvedActorRef<M> {
    /// A reference owned by the local actor system.
    Local(ActorRef<M>),
    /// A reference reached through remoting.
    Remote(RemoteActorRef<M>),
}

impl<M> ResolvedActorRef<M>
where
    M: Send + 'static,
{
    /// Returns the resolved actor path.
    pub fn path(&self) -> &ActorPath {
        match self {
            Self::Local(actor_ref) => actor_ref.path(),
            Self::Remote(actor_ref) => actor_ref.path(),
        }
    }

    /// Returns `true` when this reference resolves locally.
    pub fn is_local(&self) -> bool {
        matches!(self, Self::Local(_))
    }

    /// Returns `true` when this reference resolves remotely.
    pub fn is_remote(&self) -> bool {
        matches!(self, Self::Remote(_))
    }

    /// Returns the local reference, if this reference resolves locally.
    pub fn as_local(&self) -> Option<&ActorRef<M>> {
        match self {
            Self::Local(actor_ref) => Some(actor_ref),
            Self::Remote(_) => None,
        }
    }

    /// Returns the remote reference, if this reference resolves remotely.
    pub fn as_remote(&self) -> Option<&RemoteActorRef<M>> {
        match self {
            Self::Local(_) => None,
            Self::Remote(actor_ref) => Some(actor_ref),
        }
    }
}

impl<M> ResolvedActorRef<M>
where
    M: RemoteMessage,
{
    /// Sends a message through the selected local or remote delivery path.
    ///
    /// On failure, the returned [`SendError`] retains ownership of `message`.
    pub fn tell(&self, message: M) -> Result<(), SendError<M>> {
        match self {
            Self::Local(actor_ref) => actor_ref.tell(message),
            Self::Remote(actor_ref) => actor_ref.tell(message),
        }
    }
}

impl<M> Clone for ResolvedActorRef<M> {
    fn clone(&self) -> Self {
        match self {
            Self::Local(actor_ref) => Self::Local(actor_ref.clone()),
            Self::Remote(actor_ref) => Self::Remote(actor_ref.clone()),
        }
    }
}

impl<M> Recipient<M> for ResolvedActorRef<M>
where
    M: RemoteMessage,
{
    fn tell(&self, message: M) -> Result<(), SendError<M>> {
        ResolvedActorRef::tell(self, message)
    }
}

#[cfg(test)]
mod tests {
    use kairo_actor::{Actor, ActorResult, ActorSystem, Context, Props};

    use super::*;

    struct LocalOnlyMsg;

    struct LocalOnlyActor;

    impl Actor for LocalOnlyActor {
        type Msg = LocalOnlyMsg;

        fn receive(&mut self, _ctx: &mut Context<Self::Msg>, _msg: Self::Msg) -> ActorResult {
            Ok(())
        }
    }

    #[test]
    fn local_resolved_ref_accessors_do_not_require_remote_message() {
        let system = ActorSystem::builder("local-only").build().unwrap();
        let target = system
            .spawn("target", Props::new(|| LocalOnlyActor))
            .unwrap();
        let resolved = ResolvedActorRef::Local(target.clone());

        assert!(resolved.is_local());
        assert!(!resolved.is_remote());
        assert!(resolved.as_local().is_some());
        assert!(resolved.as_remote().is_none());
        assert_eq!(resolved.path(), target.path());
    }
}
