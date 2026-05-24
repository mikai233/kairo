use std::time::Duration;

use kairo_actor::ActorRef;

use crate::ShardCoordinatorMsg;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegionRegistrationStatus {
    Disabled,
    Registering,
    Registered,
}

pub struct RegionRegistrationConfig<M>
where
    M: Send + 'static,
{
    coordinator: ActorRef<ShardCoordinatorMsg<M>>,
    retry_interval: Duration,
}

impl<M> RegionRegistrationConfig<M>
where
    M: Send + 'static,
{
    pub fn new(coordinator: ActorRef<ShardCoordinatorMsg<M>>, retry_interval: Duration) -> Self {
        Self {
            coordinator,
            retry_interval,
        }
    }

    pub fn coordinator(&self) -> &ActorRef<ShardCoordinatorMsg<M>> {
        &self.coordinator
    }

    pub fn retry_interval(&self) -> Duration {
        self.retry_interval
    }
}

impl<M> Clone for RegionRegistrationConfig<M>
where
    M: Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            coordinator: self.coordinator.clone(),
            retry_interval: self.retry_interval,
        }
    }
}

pub struct RegionRegistration<M>
where
    M: Send + 'static,
{
    coordinator: ActorRef<ShardCoordinatorMsg<M>>,
    retry_interval: Duration,
    registered: bool,
}

impl<M> RegionRegistration<M>
where
    M: Send + 'static,
{
    pub fn new(config: RegionRegistrationConfig<M>) -> Self {
        Self {
            coordinator: config.coordinator,
            retry_interval: config.retry_interval,
            registered: false,
        }
    }

    pub fn coordinator(&self) -> &ActorRef<ShardCoordinatorMsg<M>> {
        &self.coordinator
    }

    pub fn retry_interval(&self) -> Duration {
        self.retry_interval
    }

    pub fn status(&self) -> RegionRegistrationStatus {
        if self.registered {
            RegionRegistrationStatus::Registered
        } else {
            RegionRegistrationStatus::Registering
        }
    }

    pub fn is_registered(&self) -> bool {
        self.registered
    }

    pub fn mark_registering(&mut self) {
        self.registered = false;
    }

    pub fn mark_registered(&mut self) {
        self.registered = true;
    }
}
