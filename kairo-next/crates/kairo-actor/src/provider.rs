use crate::path::ActorPath;
use crate::refs::AnyActorRef;
use crate::system::ActorSystem;

/// Actor-reference provider boundary for local resolution and guardian refs.
///
/// Remoting can wrap this boundary later without making `kairo-actor` depend on
/// a remote transport or cluster membership.
pub trait ActorRefProvider: Send + Sync {
    /// Resolves a canonical path as local, missing, or non-local.
    fn resolve(&self, path: &ActorPath) -> ActorRefResolveResult;
    /// Returns the root guardian reference.
    fn root_guardian(&self) -> AnyActorRef;
    /// Returns the user guardian reference.
    fn user_guardian(&self) -> AnyActorRef;
    /// Returns the system guardian reference.
    fn system_guardian(&self) -> AnyActorRef;
    /// Returns the temporary-actor guardian reference.
    fn temp_guardian(&self) -> AnyActorRef;
    /// Returns the virtual dead-letters reference.
    fn dead_letters(&self) -> AnyActorRef;
    /// Allocates a unique temporary path using `prefix`.
    fn temp_path(&self, prefix: &str) -> ActorPath;
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Result of resolving a canonical actor path.
pub enum ActorRefResolveResult {
    /// A live or virtual actor owned by the local actor system.
    Local(AnyActorRef),
    /// A local path for which no actor incarnation exists.
    Missing(AnyActorRef),
    /// A path belonging to another actor-system address.
    NonLocal(ActorPath),
}

impl ActorRefResolveResult {
    /// Returns the resolved or unresolved canonical path.
    pub fn path(&self) -> &ActorPath {
        match self {
            Self::Local(actor) | Self::Missing(actor) => actor.path(),
            Self::NonLocal(path) => path,
        }
    }

    /// Returns whether the result identifies a local actor.
    pub fn is_local(&self) -> bool {
        matches!(self, Self::Local(_))
    }

    /// Returns whether the local path is missing.
    pub fn is_missing(&self) -> bool {
        matches!(self, Self::Missing(_))
    }

    /// Returns whether the path belongs to a non-local address.
    pub fn is_non_local(&self) -> bool {
        matches!(self, Self::NonLocal(_))
    }
}

#[derive(Debug, Clone)]
/// Provider for local actor refs, guardians, and temporary paths.
pub struct LocalActorRefProvider {
    system: ActorSystem,
}

impl LocalActorRefProvider {
    pub(crate) fn new(system: ActorSystem) -> Self {
        Self { system }
    }

    /// Returns the actor system served by this provider.
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

    fn temp_guardian(&self) -> AnyActorRef {
        AnyActorRef::from_path(self.system.temp_root_path())
    }

    fn dead_letters(&self) -> AnyActorRef {
        AnyActorRef::from_path(self.system.dead_letters_path())
    }

    fn temp_path(&self, prefix: &str) -> ActorPath {
        self.system.next_temp_path(prefix)
    }
}

impl LocalActorRefProvider {
    fn is_known_virtual_path(&self, path: &ActorPath) -> bool {
        path == &self.system.root_path()
            || path == &self.system.user_root_path()
            || path == &self.system.system_root_path()
            || path == &self.system.temp_root_path()
            || path == &self.system.dead_letters_path()
    }
}
