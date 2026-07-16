use std::collections::HashMap;

use kairo_cluster::UniqueAddress;

use crate::singleton::{SingletonOldestChange, SingletonOldestObservation, SingletonProxyTarget};

pub(super) struct SingletonProxyRoutes<M>
where
    M: Send + 'static,
{
    current_oldest: Option<UniqueAddress>,
    routes: HashMap<UniqueAddress, SingletonProxyTarget<M>>,
}

impl<M> SingletonProxyRoutes<M>
where
    M: Send + 'static,
{
    pub(super) fn new() -> Self {
        Self {
            current_oldest: None,
            routes: HashMap::new(),
        }
    }

    pub(super) fn current_oldest(&self) -> Option<&UniqueAddress> {
        self.current_oldest.as_ref()
    }

    pub(super) fn registered_routes(&self) -> usize {
        self.routes.len()
    }

    pub(super) fn register_route(
        &mut self,
        node: UniqueAddress,
        singleton: SingletonProxyTarget<M>,
    ) -> bool {
        let is_current_oldest = self.current_oldest.as_ref() == Some(&node);
        self.routes.insert(node, singleton);
        is_current_oldest
    }

    pub(super) fn remove_route(&mut self, node: &UniqueAddress) -> bool {
        let removed = self.routes.remove(node).is_some();
        removed && self.current_oldest.as_ref() == Some(node)
    }

    pub(super) fn apply_initial_observation(
        &mut self,
        observation: SingletonOldestObservation,
    ) -> bool {
        self.set_current_oldest(observation.oldest().cloned())
    }

    pub(super) fn apply_oldest_change(&mut self, change: SingletonOldestChange) -> bool {
        match change {
            SingletonOldestChange::OldestChanged(oldest) => self.set_current_oldest(oldest),
            SingletonOldestChange::SelfRemoved | SingletonOldestChange::SelfDowned => false,
        }
    }

    pub(super) fn current_target(&self) -> Option<SingletonProxyTarget<M>> {
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
