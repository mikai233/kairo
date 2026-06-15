use kairo_actor::{ActorPath, ActorRef, Recipient, SendError};
use kairo_serialization::RemoteMessage;

use crate::RemoteActorRef;

pub enum ResolvedActorRef<M> {
    Local(ActorRef<M>),
    Remote(RemoteActorRef<M>),
}

impl<M> ResolvedActorRef<M>
where
    M: Send + 'static,
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
}

impl<M> ResolvedActorRef<M>
where
    M: RemoteMessage,
{
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
