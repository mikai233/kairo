use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::actor::{Actor, Props};
use crate::error::ActorError;
use crate::mailbox::Mailbox;
use crate::path::ActorPath;
use crate::refs::{ActorRef, TerminationLatch};
use crate::runtime::schedule_actor;

use super::ActorSystem;

impl ActorSystem {
    pub fn spawn<A>(
        &self,
        name: impl AsRef<str>,
        props: Props<A>,
    ) -> Result<ActorRef<A::Msg>, ActorError>
    where
        A: Actor,
    {
        let parent_path = self.user_root_path();
        self.spawn_under(&parent_path, name.as_ref(), props)
    }

    /// Spawn a framework-owned actor under `/system`.
    ///
    /// This is intended for Kairo subsystems such as remoting or cluster
    /// services that need stable system paths. User actors should use
    /// [`ActorSystem::spawn`] or [`Context::spawn`](crate::Context::spawn).
    pub fn spawn_system<A>(
        &self,
        name: impl AsRef<str>,
        props: Props<A>,
    ) -> Result<ActorRef<A::Msg>, ActorError>
    where
        A: Actor,
    {
        let parent_path = self.system_root_path();
        self.spawn_under(&parent_path, name.as_ref(), props)
    }

    pub(crate) fn spawn_under<A>(
        &self,
        parent_path: &ActorPath,
        name: &str,
        props: Props<A>,
    ) -> Result<ActorRef<A::Msg>, ActorError>
    where
        A: Actor,
    {
        self.spawn_under_with_name(parent_path, name, props, false)
    }

    pub(crate) fn spawn_anonymous_under<A>(
        &self,
        parent_path: &ActorPath,
        props: Props<A>,
    ) -> Result<ActorRef<A::Msg>, ActorError>
    where
        A: Actor,
    {
        let id = self.inner.next_anonymous.fetch_add(1, Ordering::Relaxed);
        let name = format!("$anon-{id}");
        self.spawn_under_with_name(parent_path, &name, props, true)
    }

    fn spawn_under_with_name<A>(
        &self,
        parent_path: &ActorPath,
        name: &str,
        props: Props<A>,
        allow_reserved_name: bool,
    ) -> Result<ActorRef<A::Msg>, ActorError>
    where
        A: Actor,
    {
        if self.is_terminating() {
            return Err(ActorError::SystemTerminating);
        }
        validate_actor_name(name, allow_reserved_name)?;

        let uid = self.inner.next_uid.fetch_add(1, Ordering::Relaxed);
        let registry_key = format!("{parent_path}/{name}");
        self.inner
            .registry
            .reserve_name(registry_key.clone(), uid, name)?;

        let mailbox = Arc::new(Mailbox::new(self.inner.mailbox));
        let path = parent_path.child(name, Some(uid));
        let stopped = Arc::new(AtomicBool::new(false));
        let terminated = Arc::new(TerminationLatch::default());
        let actor_ref = ActorRef::new(
            path.clone(),
            mailbox,
            Arc::clone(&stopped),
            Arc::clone(&terminated),
            self.inner.dead_letters.clone(),
        );
        self.inner.registry.add_ref(actor_ref.clone());
        let runner_ref = actor_ref.clone();
        let registry_key_for_runner = registry_key.clone();
        let runner_system = self.clone();
        let parent_path_for_registry = parent_path.to_string();
        let parent_path_for_runner = parent_path.clone();
        let actor_handle = actor_ref.to_local_handle(props.can_restart());
        self.inner.registry.add_handle(actor_handle.clone());
        self.inner
            .registry
            .add_child(parent_path_for_registry.clone(), actor_handle);

        if !schedule_actor(
            props,
            runner_ref,
            registry_key_for_runner,
            runner_system,
            parent_path_for_runner,
        ) {
            self.inner.registry.remove_ref(actor_ref.path());
            self.inner.registry.remove_handle(actor_ref.path());
            self.inner.registry.release_name(&registry_key);
            self.inner
                .registry
                .remove_child(&parent_path_for_registry, actor_ref.path());
            return Err(ActorError::SystemTerminating);
        }

        Ok(actor_ref)
    }
}

fn validate_actor_name(name: &str, allow_reserved: bool) -> Result<(), ActorError> {
    let valid = if allow_reserved {
        ActorPath::is_valid_internal_name(name)
    } else {
        ActorPath::is_valid_actor_name(name)
    };
    if !valid {
        return Err(ActorError::InvalidName(name.to_string()));
    }
    Ok(())
}
