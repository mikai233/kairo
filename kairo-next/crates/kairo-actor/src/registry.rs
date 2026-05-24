use std::any::Any;
use std::collections::HashMap;
use std::sync::Mutex;

use crate::error::ActorError;
use crate::path::ActorPath;
use crate::refs::{ActorRef, AnyActorRef, LocalActorHandle};

#[derive(Debug, Default)]
pub(crate) struct ActorRegistry {
    names: Mutex<HashMap<String, u64>>,
    children: Mutex<HashMap<String, Vec<LocalActorHandle>>>,
    refs: Mutex<HashMap<String, Box<dyn Any + Send + Sync>>>,
}

impl ActorRegistry {
    pub(crate) fn reserve_name(
        &self,
        registry_key: String,
        uid: u64,
        actor_name: &str,
    ) -> Result<(), ActorError> {
        let mut names = self.names.lock().expect("actor registry poisoned");
        if names.contains_key(&registry_key) {
            return Err(ActorError::DuplicateName(actor_name.to_string()));
        }
        names.insert(registry_key, uid);
        Ok(())
    }

    pub(crate) fn release_name(&self, registry_key: &str) {
        self.names
            .lock()
            .expect("actor registry poisoned")
            .remove(registry_key);
    }

    pub(crate) fn add_child(&self, parent_path: String, child: LocalActorHandle) {
        self.children
            .lock()
            .expect("actor children registry poisoned")
            .entry(parent_path)
            .or_default()
            .push(child);
    }

    pub(crate) fn add_ref<M>(&self, actor: ActorRef<M>)
    where
        M: Send + 'static,
    {
        self.refs
            .lock()
            .expect("actor ref registry poisoned")
            .insert(actor.path().to_string(), Box::new(actor));
    }

    pub(crate) fn remove_ref(&self, path: &ActorPath) {
        self.refs
            .lock()
            .expect("actor ref registry poisoned")
            .remove(path.as_str());
    }

    pub(crate) fn resolve_ref<M>(&self, path: &str) -> Option<ActorRef<M>>
    where
        M: Send + 'static,
    {
        self.refs
            .lock()
            .expect("actor ref registry poisoned")
            .get(path)
            .and_then(|actor| actor.downcast_ref::<ActorRef<M>>().cloned())
    }

    pub(crate) fn remove_child(&self, parent_path: &str, child_path: &ActorPath) {
        let mut children = self
            .children
            .lock()
            .expect("actor children registry poisoned");
        if let Some(siblings) = children.get_mut(parent_path) {
            siblings.retain(|child| child.path() != child_path);
            if siblings.is_empty() {
                children.remove(parent_path);
            }
        }
    }

    pub(crate) fn children_of(&self, parent_path: &ActorPath) -> Vec<AnyActorRef> {
        self.children
            .lock()
            .expect("actor children registry poisoned")
            .get(parent_path.as_str())
            .map(|children| {
                children
                    .iter()
                    .map(|child| AnyActorRef::from_path(child.path().clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub(crate) fn child_of(&self, parent_path: &ActorPath, name: &str) -> Option<AnyActorRef> {
        self.children
            .lock()
            .expect("actor children registry poisoned")
            .get(parent_path.as_str())
            .and_then(|children| {
                children
                    .iter()
                    .find(|child| child_name(parent_path, child.path()) == Some(name))
                    .map(|child| AnyActorRef::from_path(child.path().clone()))
            })
    }

    pub(crate) fn is_child_of(&self, parent_path: &ActorPath, child_path: &ActorPath) -> bool {
        self.children
            .lock()
            .expect("actor children registry poisoned")
            .get(parent_path.as_str())
            .map(|children| children.iter().any(|child| child.path() == child_path))
            .unwrap_or(false)
    }

    pub(crate) fn take_children(&self, parent_path: &str) -> Vec<LocalActorHandle> {
        self.children
            .lock()
            .expect("actor children registry poisoned")
            .remove(parent_path)
            .unwrap_or_default()
    }
}

fn child_name<'a>(parent_path: &ActorPath, child_path: &'a ActorPath) -> Option<&'a str> {
    if child_path.parent().as_ref() == Some(parent_path) {
        child_path.name()
    } else {
        None
    }
}
