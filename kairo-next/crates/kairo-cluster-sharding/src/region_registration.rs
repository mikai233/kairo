#![deny(missing_docs)]

use std::time::Duration;

use kairo_actor::ActorRef;

use crate::ShardCoordinatorMsg;

/// Region registration state exposed in runtime snapshots.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegionRegistrationStatus {
    /// No local or remote coordinator target is configured.
    Disabled,
    /// A target is configured but has not acknowledged registration.
    Registering,
    /// The selected coordinator has acknowledged registration.
    Registered,
}

/// Configuration for a typed local-coordinator registration session.
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
    /// Creates a local registration config for `coordinator`.
    pub fn new(coordinator: ActorRef<ShardCoordinatorMsg<M>>, retry_interval: Duration) -> Self {
        Self {
            coordinator,
            retry_interval,
        }
    }

    /// Returns the selected local coordinator.
    pub fn coordinator(&self) -> &ActorRef<ShardCoordinatorMsg<M>> {
        &self.coordinator
    }

    /// Returns the cadence for retrying registration until acknowledged.
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

/// Mutable registration session for a typed local coordinator.
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
    /// Starts an unacknowledged session from `config`.
    pub fn new(config: RegionRegistrationConfig<M>) -> Self {
        Self {
            coordinator: config.coordinator,
            retry_interval: config.retry_interval,
            registered: false,
        }
    }

    /// Returns the selected local coordinator.
    pub fn coordinator(&self) -> &ActorRef<ShardCoordinatorMsg<M>> {
        &self.coordinator
    }

    /// Returns the registration retry cadence.
    pub fn retry_interval(&self) -> Duration {
        self.retry_interval
    }

    /// Returns the current registration status.
    pub fn status(&self) -> RegionRegistrationStatus {
        if self.registered {
            RegionRegistrationStatus::Registered
        } else {
            RegionRegistrationStatus::Registering
        }
    }

    /// Returns whether the selected coordinator acknowledged registration.
    pub fn is_registered(&self) -> bool {
        self.registered
    }

    /// Marks the session unacknowledged so registration is retried.
    pub fn mark_registering(&mut self) {
        self.registered = false;
    }

    /// Records a successful acknowledgement from the selected coordinator.
    pub fn mark_registered(&mut self) {
        self.registered = true;
    }
}
