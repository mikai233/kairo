use std::collections::BTreeMap;
use std::time::Duration;

use kairo_actor::ActorRef;
use kairo_cluster::{ClusterEvent, CurrentClusterState, UniqueAddress};

use crate::{
    CoordinatorDiscoveryChange, CoordinatorDiscoverySettings, CoordinatorDiscoveryState,
    RegionRegistrationConfig, ShardCoordinatorMsg,
};

pub struct RegionCoordinatorDiscoveryConfig<M>
where
    M: Send + 'static,
{
    settings: CoordinatorDiscoverySettings,
    retry_interval: Duration,
    targets: BTreeMap<String, RegionCoordinatorDiscoveryTarget<M>>,
}

impl<M> RegionCoordinatorDiscoveryConfig<M>
where
    M: Send + 'static,
{
    pub fn new(settings: CoordinatorDiscoverySettings, retry_interval: Duration) -> Self {
        Self {
            settings,
            retry_interval,
            targets: BTreeMap::new(),
        }
    }

    pub fn with_local_coordinator(
        mut self,
        node: UniqueAddress,
        coordinator: ActorRef<ShardCoordinatorMsg<M>>,
    ) -> Self {
        self.add_local_coordinator(node, coordinator);
        self
    }

    pub fn add_local_coordinator(
        &mut self,
        node: UniqueAddress,
        coordinator: ActorRef<ShardCoordinatorMsg<M>>,
    ) {
        self.targets.insert(
            node.ordering_key(),
            RegionCoordinatorDiscoveryTarget { node, coordinator },
        );
    }

    pub fn settings(&self) -> &CoordinatorDiscoverySettings {
        &self.settings
    }

    pub fn retry_interval(&self) -> Duration {
        self.retry_interval
    }
}

impl<M> Clone for RegionCoordinatorDiscoveryConfig<M>
where
    M: Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            settings: self.settings.clone(),
            retry_interval: self.retry_interval,
            targets: self.targets.clone(),
        }
    }
}

struct RegionCoordinatorDiscoveryTarget<M>
where
    M: Send + 'static,
{
    node: UniqueAddress,
    coordinator: ActorRef<ShardCoordinatorMsg<M>>,
}

impl<M> Clone for RegionCoordinatorDiscoveryTarget<M>
where
    M: Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            node: self.node.clone(),
            coordinator: self.coordinator.clone(),
        }
    }
}

pub struct RegionCoordinatorDiscovery<M>
where
    M: Send + 'static,
{
    discovery: CoordinatorDiscoveryState,
    retry_interval: Duration,
    targets: BTreeMap<String, RegionCoordinatorDiscoveryTarget<M>>,
    selected: Option<UniqueAddress>,
}

impl<M> RegionCoordinatorDiscovery<M>
where
    M: Send + 'static,
{
    pub fn new(config: RegionCoordinatorDiscoveryConfig<M>) -> Self {
        Self {
            discovery: CoordinatorDiscoveryState::new(config.settings),
            retry_interval: config.retry_interval,
            targets: config.targets,
            selected: None,
        }
    }

    pub fn apply_snapshot(
        &mut self,
        state: &CurrentClusterState,
    ) -> RegionCoordinatorDiscoveryPlan<M> {
        let membership_change = self.discovery.apply_snapshot(state);
        self.plan(membership_change)
    }

    pub fn apply_event(&mut self, event: &ClusterEvent) -> RegionCoordinatorDiscoveryPlan<M> {
        let membership_change = self.discovery.apply_event(event);
        self.plan(membership_change)
    }

    pub fn candidates(&self) -> Vec<UniqueAddress> {
        self.discovery.coordinator_candidates()
    }

    pub fn selected(&self) -> Option<&UniqueAddress> {
        self.selected.as_ref()
    }

    fn plan(
        &mut self,
        membership_change: CoordinatorDiscoveryChange,
    ) -> RegionCoordinatorDiscoveryPlan<M> {
        let previous_selected = self.selected.clone();
        let selected = self.select_target().map(|target| target.node.clone());
        let registration_changed = previous_selected != selected;
        self.selected = selected.clone();
        let registration = if registration_changed {
            selected.as_ref().and_then(|node| {
                self.targets.get(&node.ordering_key()).map(|target| {
                    RegionRegistrationConfig::new(target.coordinator.clone(), self.retry_interval)
                })
            })
        } else {
            None
        };

        RegionCoordinatorDiscoveryPlan {
            membership_change,
            previous_selected,
            selected,
            registration_changed,
            registration,
        }
    }

    fn select_target(&self) -> Option<&RegionCoordinatorDiscoveryTarget<M>> {
        self.discovery
            .coordinator_candidates()
            .into_iter()
            .find_map(|node| self.targets.get(&node.ordering_key()))
    }
}

pub struct RegionCoordinatorDiscoveryPlan<M>
where
    M: Send + 'static,
{
    pub membership_change: CoordinatorDiscoveryChange,
    pub previous_selected: Option<UniqueAddress>,
    pub selected: Option<UniqueAddress>,
    pub registration_changed: bool,
    pub registration: Option<RegionRegistrationConfig<M>>,
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use kairo_actor::{Actor, ActorResult, Address, Context, Props};
    use kairo_cluster::{CurrentClusterState, Member, MemberStatus};
    use kairo_testkit::ActorSystemTestKit;

    use crate::{
        CoordinatorStateSnapshot, ShardCoordinatorMsg,
        region_coordinator_discovery::{
            RegionCoordinatorDiscovery, RegionCoordinatorDiscoveryConfig,
        },
    };

    use super::*;

    #[test]
    fn region_coordinator_discovery_selects_first_known_likely_target() {
        let kit = ActorSystemTestKit::new("region-discovery-selects-target").unwrap();
        let node_a = node("a", 1, 2551);
        let node_b = node("b", 2, 2552);
        let coordinator_b = kit
            .system()
            .spawn("coordinator-b", Props::new(|| DiscoveryCoordinatorProbe))
            .unwrap();
        let config = RegionCoordinatorDiscoveryConfig::new(
            CoordinatorDiscoverySettings::default().with_required_role("backend"),
            Duration::from_millis(20),
        )
        .with_local_coordinator(node_b.clone(), coordinator_b);
        let mut discovery = RegionCoordinatorDiscovery::new(config);

        let plan = discovery.apply_snapshot(&state(vec![
            member(node_a, MemberStatus::Leaving, ["backend"], 1),
            member(node_b.clone(), MemberStatus::Up, ["backend"], 2),
        ]));

        assert_eq!(plan.selected, Some(node_b));
        assert!(plan.registration_changed);
        assert!(plan.registration.is_some());
        kit.shutdown(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn region_coordinator_discovery_clears_registration_when_target_disappears() {
        let kit = ActorSystemTestKit::new("region-discovery-clears-target").unwrap();
        let node_a = node("a", 1, 2551);
        let coordinator_a = kit
            .system()
            .spawn("coordinator-a", Props::new(|| DiscoveryCoordinatorProbe))
            .unwrap();
        let config = RegionCoordinatorDiscoveryConfig::new(
            CoordinatorDiscoverySettings::default().with_required_role("backend"),
            Duration::from_millis(20),
        )
        .with_local_coordinator(node_a.clone(), coordinator_a);
        let mut discovery = RegionCoordinatorDiscovery::new(config);
        discovery.apply_snapshot(&state(vec![member(
            node_a.clone(),
            MemberStatus::Up,
            ["backend"],
            1,
        )]));

        let plan = discovery.apply_snapshot(&state(Vec::new()));

        assert_eq!(plan.previous_selected, Some(node_a));
        assert_eq!(plan.selected, None);
        assert!(plan.registration_changed);
        assert!(plan.registration.is_none());
        kit.shutdown(Duration::from_secs(1)).unwrap();
    }

    struct DiscoveryCoordinatorProbe;

    impl Actor for DiscoveryCoordinatorProbe {
        type Msg = ShardCoordinatorMsg<String>;

        fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
            if let ShardCoordinatorMsg::RegisterLocalRegion { reply_to, .. } = msg {
                let _ = reply_to.tell(Ok(CoordinatorStateSnapshot {
                    allocations: Default::default(),
                    proxies: Default::default(),
                    unallocated_shards: Default::default(),
                    rebalance_in_progress: Default::default(),
                    remember_entities: false,
                }));
            }
            Ok(())
        }
    }

    fn member(
        unique_address: UniqueAddress,
        status: MemberStatus,
        roles: impl IntoIterator<Item = &'static str>,
        up_number: u64,
    ) -> Member {
        Member::new(
            unique_address,
            roles.into_iter().map(ToString::to_string).collect(),
        )
        .with_status(status)
        .with_up_number(up_number)
    }

    fn node(system: &str, uid: u64, port: u16) -> UniqueAddress {
        UniqueAddress::new(
            Address::new("kairo", system, Some("127.0.0.1".to_string()), Some(port)),
            uid,
        )
    }

    fn state(members: Vec<Member>) -> CurrentClusterState {
        CurrentClusterState {
            members,
            unreachable: Vec::new(),
            seen_by: HashSet::new(),
            leader: None,
            role_leaders: HashMap::new(),
            member_tombstones: HashSet::new(),
        }
    }
}
