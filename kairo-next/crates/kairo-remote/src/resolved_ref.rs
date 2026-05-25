use kairo_actor::{ActorPath, ActorRef, Recipient, SendError};
use kairo_serialization::RemoteMessage;

use crate::RemoteActorRef;

pub enum ResolvedActorRef<M> {
    Local(ActorRef<M>),
    Remote(RemoteActorRef<M>),
}

impl<M> ResolvedActorRef<M>
where
    M: RemoteMessage,
{
    pub fn path(&self) -> &ActorPath {
        match self {
            Self::Local(actor_ref) => actor_ref.path(),
            Self::Remote(actor_ref) => actor_ref.path(),
        }
    }

    pub fn is_local(&self) -> bool {
        matches!(self, Self::Local(_))
    }

    pub fn is_remote(&self) -> bool {
        matches!(self, Self::Remote(_))
    }

    pub fn as_local(&self) -> Option<&ActorRef<M>> {
        match self {
            Self::Local(actor_ref) => Some(actor_ref),
            Self::Remote(_) => None,
        }
    }

    pub fn as_remote(&self) -> Option<&RemoteActorRef<M>> {
        match self {
            Self::Local(_) => None,
            Self::Remote(actor_ref) => Some(actor_ref),
        }
    }
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
