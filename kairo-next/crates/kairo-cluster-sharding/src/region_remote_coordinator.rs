use kairo_serialization::ActorRefWireData;

use crate::{
    RegionId, RegionRegistrationStatus, ShardCoordinatorRemoteHome,
    ShardCoordinatorRemoteRegistrationAck, ShardCoordinatorRemoteTarget, ShardId,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegionRemoteRegistrationPlan {
    Registered {
        coordinator: ActorRefWireData,
    },
    IgnoredNoTarget {
        coordinator: ActorRefWireData,
    },
    IgnoredStaleAck {
        expected: ActorRefWireData,
        actual: ActorRefWireData,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegionRemoteShardHomePlan {
    pub shard: ShardId,
    pub region: RegionId,
}

#[derive(Clone, Default)]
pub struct RegionRemoteCoordinator {
    target: Option<ShardCoordinatorRemoteTarget>,
    registered: bool,
}

impl RegionRemoteCoordinator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_target(&mut self, target: Option<ShardCoordinatorRemoteTarget>) {
        self.target = target;
        self.registered = false;
    }

    pub fn target(&self) -> Option<&ShardCoordinatorRemoteTarget> {
        self.target.as_ref()
    }

    pub fn is_registered(&self) -> bool {
        self.target.is_some() && self.registered
    }

    pub fn status(&self) -> Option<RegionRegistrationStatus> {
        self.target.as_ref().map(|_| {
            if self.registered {
                RegionRegistrationStatus::Registered
            } else {
                RegionRegistrationStatus::Registering
            }
        })
    }

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

pub fn shard_home_plan_from_remote(home: ShardCoordinatorRemoteHome) -> RegionRemoteShardHomePlan {
    RegionRemoteShardHomePlan {
        shard: home.shard_id,
        region: region_id_from_wire_ref(&home.region),
    }
}

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
        coordinator.set_target(Some(target.clone()));

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
        coordinator.set_target(Some(target.clone()));

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
