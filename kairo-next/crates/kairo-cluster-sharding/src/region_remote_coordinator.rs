#![deny(missing_docs)]

use std::time::Duration;

use kairo_serialization::ActorRefWireData;

use crate::{
    RegionId, RegionRegistrationStatus, ShardCoordinatorRemoteHome,
    ShardCoordinatorRemoteRegistrationAck, ShardCoordinatorRemoteTarget, ShardId,
};

/// Effect of applying a decoded remote registration acknowledgement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegionRemoteRegistrationPlan {
    /// The acknowledgement matches the currently selected coordinator.
    Registered {
        /// Stable wire ref of the coordinator that acknowledged registration.
        coordinator: ActorRefWireData,
    },
    /// The acknowledgement arrived after remote targeting was cleared.
    IgnoredNoTarget {
        /// Coordinator advertised by the ignored acknowledgement.
        coordinator: ActorRefWireData,
    },
    /// The acknowledgement belongs to a coordinator that is no longer selected.
    IgnoredStaleAck {
        /// Stable wire ref of the currently selected coordinator.
        expected: ActorRefWireData,
        /// Coordinator advertised by the stale acknowledgement.
        actual: ActorRefWireData,
    },
}

/// Local routing effect decoded from a remote shard-home reply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegionRemoteShardHomePlan {
    /// Shard whose home was resolved.
    pub shard: ShardId,
    /// Region id derived from the returned stable actor-ref path.
    pub region: RegionId,
}

/// Region-side registration session for one selected remote coordinator.
///
/// Changing or clearing the target invalidates any previous acknowledgement.
/// A session becomes registered only when the decoded acknowledgement
/// advertises the exact selected coordinator recipient.
#[derive(Clone, Default)]
pub struct RegionRemoteCoordinator {
    target: Option<ShardCoordinatorRemoteTarget>,
    retry_interval: Option<Duration>,
    registered: bool,
}

impl RegionRemoteCoordinator {
    /// Creates a session with no selected remote target.
    pub fn new() -> Self {
        Self::default()
    }

    /// Replaces the selected target and invalidates prior registration state.
    pub fn set_target(
        &mut self,
        target: Option<ShardCoordinatorRemoteTarget>,
        retry_interval: Option<Duration>,
    ) {
        self.target = target;
        self.retry_interval = retry_interval;
        self.registered = false;
    }

    /// Returns the selected remote coordinator target, if any.
    pub fn target(&self) -> Option<&ShardCoordinatorRemoteTarget> {
        self.target.as_ref()
    }

    /// Returns whether the selected target acknowledged registration.
    pub fn is_registered(&self) -> bool {
        self.target.is_some() && self.registered
    }

    /// Returns the retry cadence paired with the selected target.
    pub fn retry_interval(&self) -> Option<Duration> {
        self.retry_interval
    }

    /// Returns registration status, or `None` when no target is configured.
    pub fn status(&self) -> Option<RegionRegistrationStatus> {
        self.target.as_ref().map(|_| {
            if self.registered {
                RegionRegistrationStatus::Registered
            } else {
                RegionRegistrationStatus::Registering
            }
        })
    }

    /// Applies an acknowledgement without allowing stale coordinators to win.
    pub fn apply_registration_ack(
        &mut self,
        ack: ShardCoordinatorRemoteRegistrationAck,
    ) -> RegionRemoteRegistrationPlan {
        let Some(target) = &self.target else {
            return RegionRemoteRegistrationPlan::IgnoredNoTarget {
                coordinator: ack.coordinator,
            };
        };
        if &ack.coordinator != target.recipient() {
            return RegionRemoteRegistrationPlan::IgnoredStaleAck {
                expected: target.recipient().clone(),
                actual: ack.coordinator,
            };
        }
        self.registered = true;
        RegionRemoteRegistrationPlan::Registered {
            coordinator: ack.coordinator,
        }
    }
}

/// Converts a decoded remote shard-home reply into the region runtime's ids.
pub fn shard_home_plan_from_remote(home: ShardCoordinatorRemoteHome) -> RegionRemoteShardHomePlan {
    RegionRemoteShardHomePlan {
        shard: home.shard_id,
        region: region_id_from_wire_ref(&home.region),
    }
}

/// Derives the region runtime id from a stable actor-ref wire path.
pub fn region_id_from_wire_ref(region: &ActorRefWireData) -> RegionId {
    region.path().to_string()
}

#[cfg(test)]
mod tests {
    use kairo_actor::Address;
    use kairo_cluster::UniqueAddress;

    use crate::{DEFAULT_SHARD_COORDINATOR_REMOTE_PATH, ShardCoordinatorRemoteTarget};

    use super::*;

    #[test]
    fn remote_coordinator_marks_matching_ack_registered() {
        let target = target();
        let mut coordinator = RegionRemoteCoordinator::new();
        coordinator.set_target(Some(target.clone()), Some(Duration::from_millis(20)));

        let plan = coordinator.apply_registration_ack(ShardCoordinatorRemoteRegistrationAck {
            sender: Some(target.recipient().clone()),
            coordinator: target.recipient().clone(),
        });

        assert_eq!(
            plan,
            RegionRemoteRegistrationPlan::Registered {
                coordinator: target.recipient().clone()
            }
        );
        assert!(coordinator.is_registered());
        assert_eq!(
            coordinator.status(),
            Some(RegionRegistrationStatus::Registered)
        );
    }

    #[test]
    fn remote_coordinator_ignores_stale_ack() {
        let target = target();
        let stale = actor_ref("kairo://other@127.0.0.1:2559/system/sharding/coordinator");
        let mut coordinator = RegionRemoteCoordinator::new();
        coordinator.set_target(Some(target.clone()), Some(Duration::from_millis(20)));

        let plan = coordinator.apply_registration_ack(ShardCoordinatorRemoteRegistrationAck {
            sender: Some(stale.clone()),
            coordinator: stale.clone(),
        });

        assert_eq!(
            plan,
            RegionRemoteRegistrationPlan::IgnoredStaleAck {
                expected: target.recipient().clone(),
                actual: stale,
            }
        );
        assert!(!coordinator.is_registered());
    }

    fn target() -> ShardCoordinatorRemoteTarget {
        ShardCoordinatorRemoteTarget::for_node(
            UniqueAddress::new(
                Address::new("kairo", "remote", Some("127.0.0.1".to_string()), Some(2552)),
                2,
            ),
            DEFAULT_SHARD_COORDINATOR_REMOTE_PATH,
        )
        .unwrap()
    }

    fn actor_ref(path: &str) -> ActorRefWireData {
        ActorRefWireData::new(path).unwrap()
    }
}
