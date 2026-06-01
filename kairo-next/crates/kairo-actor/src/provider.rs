use crate::path::ActorPath;
use crate::refs::AnyActorRef;
use crate::system::ActorSystem;

/// Actor-reference provider boundary for local resolution and guardian refs.
///
/// Remoting can wrap this boundary later without making `kairo-actor` depend on
/// a remote transport or cluster membership.
pub trait ActorRefProvider: Send + Sync {
    fn resolve(&self, path: &ActorPath) -> ActorRefResolveResult;
    fn root_guardian(&self) -> AnyActorRef;
    fn user_guardian(&self) -> AnyActorRef;
    fn system_guardian(&self) -> AnyActorRef;
    fn dead_letters(&self) -> AnyActorRef;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActorRefResolveResult {
    Local(AnyActorRef),
    Missing(AnyActorRef),
    NonLocal(ActorPath),
}

impl ActorRefResolveResult {
    pub fn path(&self) -> &ActorPath {
        match self {
            Self::Local(actor) | Self::Missing(actor) => actor.path(),
            Self::NonLocal(path) => path,
        }
    }

    pub fn is_local(&self) -> bool {
        matches!(self, Self::Local(_))
    }

    pub fn is_missing(&self) -> bool {
        matches!(self, Self::Missing(_))
    }

    pub fn is_non_local(&self) -> bool {
        matches!(self, Self::NonLocal(_))
    }
}

#[derive(Debug, Clone)]
pub struct LocalActorRefProvider {
    system: ActorSystem,
}

impl LocalActorRefProvider {
    pub(crate) fn new(system: ActorSystem) -> Self {
        Self { system }
    }

    pub fn system(&self) -> &ActorSystem {
        &self.system
    }
}

impl ActorRefProvider for LocalActorRefProvider {
    fn resolve(&self, path: &ActorPath) -> ActorRefResolveResult {
        if path.address() != self.system.address() {
            return ActorRefResolveResult::NonLocal(path.clone());
        }

        if self.is_known_virtual_path(path) || self.system.has_local_actor(path) {
            ActorRefResolveResult::Local(AnyActorRef::from_path(path.clone()))
        } else {
            ActorRefResolveResult::Missing(AnyActorRef::from_path(path.clone()))
        }
    }

    fn root_guardian(&self) -> AnyActorRef {
        AnyActorRef::from_path(self.system.root_path())
    }

    fn user_guardian(&self) -> AnyActorRef {
        AnyActorRef::from_path(self.system.user_root_path())
    }

    fn system_guardian(&self) -> AnyActorRef {
        AnyActorRef::from_path(self.system.system_root_path())
    }

    fn dead_letters(&self) -> AnyActorRef {
        AnyActorRef::from_path(self.system.dead_letters_path())
    }
}

impl LocalActorRefProvider {
    fn is_known_virtual_path(&self, path: &ActorPath) -> bool {
        path == &self.system.root_path()
            || path == &self.system.user_root_path()
            || path == &self.system.system_root_path()
            || path == &self.system.dead_letters_path()
    }
}
