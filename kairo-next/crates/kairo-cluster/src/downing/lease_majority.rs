use std::error::Error;
use std::fmt;
use std::time::Duration;

use super::{DowningDecision, DowningHook, decision_members, split_brain};
use crate::{Gossip, UniqueAddress};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseMajoritySettings {
    lease_name: String,
    role: Option<String>,
    acquire_lease_delay_for_minority: Duration,
    release_after: Duration,
}

impl LeaseMajoritySettings {
    pub fn new(
        lease_name: impl Into<String>,
        role: Option<String>,
        acquire_lease_delay_for_minority: Duration,
        release_after: Duration,
    ) -> Result<Self, LeaseMajoritySettingsError> {
        let lease_name = lease_name.into();
        if lease_name.trim().is_empty() {
            return Err(LeaseMajoritySettingsError::EmptyLeaseName);
        }

        Ok(Self {
            lease_name,
            role,
            acquire_lease_delay_for_minority,
            release_after,
        })
    }

    pub fn lease_name(&self) -> &str {
        &self.lease_name
    }

    pub fn role(&self) -> Option<&str> {
        self.role.as_deref()
    }

    pub fn acquire_lease_delay_for_minority(&self) -> Duration {
        self.acquire_lease_delay_for_minority
    }

    pub fn release_after(&self) -> Duration {
        self.release_after
    }

    pub(super) fn role_filter(&self) -> &Option<String> {
        &self.role
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseMajoritySettingsError {
    EmptyLeaseName,
}

impl fmt::Display for LeaseMajoritySettingsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyLeaseName => f.write_str("lease-majority lease name must not be empty"),
        }
    }
}

impl Error for LeaseMajoritySettingsError {}

pub trait LeaseMajorityLease {
    fn acquire(&self, lease_name: &str) -> bool;
}

#[derive(Debug, Clone)]
pub struct LeaseMajorityHook<L> {
    settings: LeaseMajoritySettings,
    lease: L,
}

impl<L> LeaseMajorityHook<L> {
    pub fn new(settings: LeaseMajoritySettings, lease: L) -> Self {
        Self { settings, lease }
    }

    pub fn settings(&self) -> &LeaseMajoritySettings {
        &self.settings
    }

    pub fn lease(&self) -> &L {
        &self.lease
    }
}

impl<L> DowningHook for LeaseMajorityHook<L>
where
    L: LeaseMajorityLease,
{
    fn decide(&self, gossip: &Gossip, _self_node: &UniqueAddress) -> DowningDecision {
        let requested = lease_requested_decision(gossip);
        if self.lease.acquire(self.settings.lease_name()) {
            requested.acquired_decision()
        } else {
            requested.denied_decision()
        }
    }

    fn decision_delay(&self, gossip: &Gossip, _self_node: &UniqueAddress) -> Duration {
        if is_in_minority(gossip, self.settings.role_filter()) {
            self.settings.acquire_lease_delay_for_minority()
        } else {
            Duration::ZERO
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LeaseRequestedDecision {
    DownUnreachable,
    DownIndirectlyConnected,
}

impl LeaseRequestedDecision {
    fn acquired_decision(self) -> DowningDecision {
        match self {
            Self::DownUnreachable => DowningDecision::DownUnreachable,
            Self::DownIndirectlyConnected => DowningDecision::DownIndirectlyConnected,
        }
    }

    fn denied_decision(self) -> DowningDecision {
        match self {
            Self::DownUnreachable => DowningDecision::DownReachable,
            Self::DownIndirectlyConnected => DowningDecision::ReverseDownIndirectlyConnected,
        }
    }
}

fn lease_requested_decision(gossip: &Gossip) -> LeaseRequestedDecision {
    if split_brain::has_indirectly_connected(gossip) {
        LeaseRequestedDecision::DownIndirectlyConnected
    } else {
        LeaseRequestedDecision::DownUnreachable
    }
}

fn is_in_minority(gossip: &Gossip, role: &Option<String>) -> bool {
    let members = decision_members(gossip, role);
    if members.is_empty() {
        return false;
    }

    let unreachable = gossip.reachability().all_unreachable_or_terminated();
    let unreachable_size = members
        .iter()
        .filter(|member| unreachable.contains(&member.unique_address))
        .count();
    let members_size = members.len();

    if unreachable_size * 2 == members_size {
        unreachable.contains(&members[0].unique_address)
    } else {
        unreachable_size * 2 > members_size
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use kairo_actor::Address;

    use super::*;
    use crate::{DowningPlan, Member, MemberStatus, Reachability};

    #[test]
    fn lease_majority_acquired_downs_unreachable_side() {
        let node_a = node("a", 1);
        let node_b = node("b", 2);
        let node_c = node("c", 3);
        let hook = hook(RecordingLease::new(true), None, Duration::from_secs(3));
        let gossip = Gossip::from_members([
            member(node_a.clone(), MemberStatus::Up),
            member(node_b, MemberStatus::Up),
            member(node_c.clone(), MemberStatus::Up),
        ])
        .with_reachability(Reachability::new().unreachable(node_a.clone(), node_c.clone()));

        let plan = DowningPlan::from_hook(&hook, &gossip, &node_a);

        assert_eq!(hook.lease().calls(), vec!["cluster-lease".to_string()]);
        assert_eq!(plan.decision(), DowningDecision::DownUnreachable);
        assert_eq!(plan.nodes_to_down(), &[node_c]);
        assert_eq!(hook.decision_delay(&gossip, &node_a), Duration::ZERO);
    }

    #[test]
    fn lease_majority_denied_reverses_to_down_reachable_side() {
        let node_a = node("a", 1);
        let node_b = node("b", 2);
        let node_c = node("c", 3);
        let hook = hook(RecordingLease::new(false), None, Duration::from_secs(3));
        let gossip = Gossip::from_members([
            member(node_a.clone(), MemberStatus::Up),
            member(node_b.clone(), MemberStatus::Up),
            member(node_c.clone(), MemberStatus::Up),
        ])
        .with_reachability(
            Reachability::new()
                .unreachable(node_a.clone(), node_b)
                .unreachable(node_a.clone(), node_c),
        );

        let plan = DowningPlan::from_hook(&hook, &gossip, &node_a);

        assert_eq!(plan.decision(), DowningDecision::DownReachable);
        assert_eq!(plan.nodes_to_down(), std::slice::from_ref(&node_a));
        assert_eq!(
            hook.decision_delay(&gossip, &node_a),
            Duration::from_secs(3)
        );
    }

    #[test]
    fn lease_majority_denied_reverses_indirect_decision() {
        let node_a = node("a", 1);
        let node_b = node("b", 2);
        let node_c = node("c", 3);
        let hook = hook(RecordingLease::new(false), None, Duration::ZERO);
        let gossip = Gossip::from_members([
            member(node_a.clone(), MemberStatus::Up),
            member(node_b.clone(), MemberStatus::Up),
            member(node_c.clone(), MemberStatus::Up),
        ])
        .with_reachability(
            Reachability::new()
                .unreachable(node_a.clone(), node_b.clone())
                .unreachable(node_b.clone(), node_a.clone()),
        );

        let plan = DowningPlan::from_hook(&hook, &gossip, &node_a);

        assert_eq!(
            plan.decision(),
            DowningDecision::ReverseDownIndirectlyConnected
        );
        assert_eq!(plan.nodes_to_down(), &[node_a, node_b, node_c]);
    }

    #[test]
    fn lease_majority_role_filter_controls_minor_side_delay() {
        let node_a = node("a", 1);
        let node_b = node("b", 2);
        let node_c = node("c", 3);
        let hook = hook(
            RecordingLease::new(true),
            Some("backend".to_string()),
            Duration::from_secs(5),
        );
        let gossip = Gossip::from_members([
            member_with_roles(node_a.clone(), MemberStatus::Up, ["frontend"]),
            member_with_roles(node_b.clone(), MemberStatus::Up, ["backend"]),
            member_with_roles(node_c.clone(), MemberStatus::Up, ["backend"]),
        ])
        .with_reachability(Reachability::new().unreachable(node_a.clone(), node_b.clone()));

        assert_eq!(
            hook.decision_delay(&gossip, &node_a),
            Duration::from_secs(5)
        );
        assert_eq!(
            DowningPlan::from_hook(&hook, &gossip, &node_a).nodes_to_down(),
            &[node_b]
        );
    }

    #[test]
    fn lease_majority_settings_reject_empty_lease_name() {
        let error =
            LeaseMajoritySettings::new("  ", None, Duration::ZERO, Duration::ZERO).unwrap_err();

        assert_eq!(error, LeaseMajoritySettingsError::EmptyLeaseName);
    }

    fn hook(
        lease: RecordingLease,
        role: Option<String>,
        acquire_delay: Duration,
    ) -> LeaseMajorityHook<RecordingLease> {
        LeaseMajorityHook::new(
            LeaseMajoritySettings::new(
                "cluster-lease",
                role,
                acquire_delay,
                Duration::from_secs(30),
            )
            .unwrap(),
            lease,
        )
    }

    fn member(unique_address: UniqueAddress, status: MemberStatus) -> Member {
        Member::new(unique_address, Vec::new()).with_status(status)
    }

    fn member_with_roles(
        unique_address: UniqueAddress,
        status: MemberStatus,
        roles: impl IntoIterator<Item = &'static str>,
    ) -> Member {
        Member::new(
            unique_address,
            roles.into_iter().map(String::from).collect(),
        )
        .with_status(status)
    }

    fn node(system: &str, uid: u64) -> UniqueAddress {
        UniqueAddress::new(Address::local(system), uid)
    }

    #[derive(Debug, Clone)]
    struct RecordingLease {
        acquired: bool,
        calls: Arc<Mutex<Vec<String>>>,
    }

    impl RecordingLease {
        fn new(acquired: bool) -> Self {
            Self {
                acquired,
                calls: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl LeaseMajorityLease for RecordingLease {
        fn acquire(&self, lease_name: &str) -> bool {
            self.calls.lock().unwrap().push(lease_name.to_string());
            self.acquired
        }
    }
}
