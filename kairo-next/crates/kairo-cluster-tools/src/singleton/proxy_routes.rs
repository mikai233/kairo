use std::collections::HashMap;

use kairo_actor::ActorRef;
use kairo_cluster::UniqueAddress;

use crate::singleton::{SingletonOldestChange, SingletonOldestObservation};

pub struct SingletonProxyRoutes<M>
where
    M: Send + 'static,
{
    current_oldest: Option<UniqueAddress>,
    routes: HashMap<UniqueAddress, ActorRef<M>>,
}

impl<M> SingletonProxyRoutes<M>
where
    M: Send + 'static,
{
    pub fn new() -> Self {
        Self {
            current_oldest: None,
            routes: HashMap::new(),
        }
    }

    pub fn current_oldest(&self) -> Option<&UniqueAddress> {
        self.current_oldest.as_ref()
    }

    pub fn registered_routes(&self) -> usize {
        self.routes.len()
    }

    pub fn register_route(&mut self, node: UniqueAddress, singleton: ActorRef<M>) -> bool {
        let is_current_oldest = self.current_oldest.as_ref() == Some(&node);
        self.routes.insert(node, singleton);
        is_current_oldest
    }

    pub fn remove_route(&mut self, node: &UniqueAddress) -> bool {
        let removed = self.routes.remove(node).is_some();
        removed && self.current_oldest.as_ref() == Some(node)
    }

    pub fn apply_initial_observation(&mut self, observation: SingletonOldestObservation) -> bool {
        self.set_current_oldest(observation.oldest().cloned())
    }

    pub fn apply_oldest_change(&mut self, change: SingletonOldestChange) -> bool {
        match change {
            SingletonOldestChange::OldestChanged(oldest) => self.set_current_oldest(oldest),
        }
    }

    pub fn current_target(&self) -> Option<ActorRef<M>> {
        self.current_oldest
            .as_ref()
            .and_then(|node| self.routes.get(node))
            .cloned()
    }

    fn set_current_oldest(&mut self, oldest: Option<UniqueAddress>) -> bool {
        if self.current_oldest == oldest {
            return false;
        }

        self.current_oldest = oldest;
        true
    }
}

impl<M> Default for SingletonProxyRoutes<M>
where
    M: Send + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}
