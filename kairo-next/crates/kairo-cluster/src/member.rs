#![deny(missing_docs)]

use kairo_actor::Address;

use crate::ApplicationVersion;

/// Role prefix reserved for Pekko-compatible data-center membership metadata.
pub const DATA_CENTER_ROLE_PREFIX: &str = "dc-";
/// Data center used by historical members that do not advertise a reserved role.
pub const DEFAULT_DATA_CENTER: &str = "default";

/// Lifecycle state of one cluster member.
///
/// Variant order is not a wire contract; cluster merge priority is defined by
/// explicit policy in this module.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MemberStatus {
    /// The node has joined but has not reached convergence for promotion.
    Joining,
    /// The node is admitted without full convergence under weak-up policy.
    WeaklyUp,
    /// The node is a fully participating cluster member.
    Up,
    /// Graceful leave has begun but the node still participates in convergence.
    Leaving,
    /// Handoff completed and the node awaits converged removal.
    Exiting,
    /// The node was explicitly or automatically downed.
    Down,
    /// The node has been removed and must not remain in the live member set.
    Removed,
}

impl MemberStatus {
    /// Returns whether this status is eligible for normal leader selection.
    pub fn participates_in_leader_selection(self) -> bool {
        matches!(self, Self::Up | Self::Leaving)
    }

    /// Returns whether this status must see gossip during normal convergence.
    pub fn participates_in_convergence(self) -> bool {
        matches!(self, Self::Up | Self::Leaving)
    }

    /// Returns whether this status must see gossip during first-member convergence.
    pub fn participates_in_first_convergence(self) -> bool {
        matches!(
            self,
            Self::Joining | Self::WeaklyUp | Self::Up | Self::Leaving
        )
    }

    /// Returns whether unreachability in this status may be ignored for convergence.
    pub fn can_skip_unreachable_for_convergence(self) -> bool {
        matches!(self, Self::Down | Self::Exiting)
    }

    /// Returns whether reachability observations from this status remain authoritative.
    pub fn observes_convergence_reachability(self) -> bool {
        !matches!(self, Self::Down)
    }

    fn leader_fallback_rank(self) -> u8 {
        match self {
            Self::Up | Self::Leaving => 0,
            Self::WeaklyUp => 1,
            Self::Joining => 2,
            Self::Exiting => 3,
            Self::Down => 4,
            Self::Removed => 5,
        }
    }

    /// Returns whether a member seen only on one side of a merge is terminal enough to remove.
    pub fn is_removed_by_single_sided_merge(self) -> bool {
        matches!(self, Self::Down | Self::Exiting | Self::Removed)
    }

    fn priority(self) -> u8 {
        match self {
            Self::Joining => 0,
            Self::WeaklyUp => 1,
            Self::Up => 2,
            Self::Leaving => 3,
            Self::Exiting => 4,
            Self::Down => 5,
            Self::Removed => 6,
        }
    }
}

/// Canonical actor-system address paired with a process incarnation UID.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UniqueAddress {
    /// Canonical logical and transport address.
    pub address: Address,
    /// Actor-system incarnation identifier at that address.
    pub uid: u64,
}

impl UniqueAddress {
    /// Creates a unique cluster address.
    pub fn new(address: Address, uid: u64) -> Self {
        Self { address, uid }
    }

    /// Returns the deterministic address-plus-UID ordering representation.
    pub fn ordering_key(&self) -> String {
        format!("{}#{}", self.address, self.uid)
    }
}

/// Immutable membership fact for one unique cluster node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Member {
    /// Canonical address and incarnation of the member.
    pub unique_address: UniqueAddress,
    /// Current lifecycle status.
    pub status: MemberStatus,
    /// Roles advertised by the member.
    pub roles: Vec<String>,
    /// Monotonic age assigned when the member becomes `Up`.
    pub up_number: Option<u64>,
    /// Application version advertised when this node joined.
    pub app_version: ApplicationVersion,
}

impl Member {
    /// Creates a joining member with no assigned up number.
    pub fn new(unique_address: UniqueAddress, roles: Vec<String>) -> Self {
        Self {
            unique_address,
            status: MemberStatus::Joining,
            roles,
            up_number: None,
            app_version: ApplicationVersion::default(),
        }
    }

    /// Returns this member with a different lifecycle status.
    pub fn with_status(mut self, status: MemberStatus) -> Self {
        self.status = status;
        self
    }

    /// Returns this member with its cluster age assigned.
    pub fn with_up_number(mut self, up_number: u64) -> Self {
        self.up_number = Some(up_number);
        self
    }

    /// Returns this member with the application version advertised at join time.
    pub fn with_app_version(mut self, app_version: ApplicationVersion) -> Self {
        self.app_version = app_version;
        self
    }

    /// Returns whether the member advertises `role`.
    pub fn has_role(&self, role: &str) -> bool {
        self.roles.iter().any(|member_role| member_role == role)
    }

    /// Returns the Pekko-style data center derived from the reserved `dc-` role.
    ///
    /// Historical v1 members without that role belong to `default`.
    pub fn data_center(&self) -> &str {
        self.roles
            .iter()
            .find_map(|role| role.strip_prefix(DATA_CENTER_ROLE_PREFIX))
            .filter(|data_center| !data_center.is_empty())
            .unwrap_or(DEFAULT_DATA_CENTER)
    }

    /// Returns whether this member sorts older than `other`.
    ///
    /// Assigned up numbers order first; the unique address deterministically
    /// breaks equal or unassigned ages.
    pub fn is_older_than(&self, other: &Self) -> bool {
        self.age_key() < other.age_key()
    }

    /// Returns the deterministic key used when no normal leader candidate exists.
    pub fn leader_fallback_key(&self) -> (u8, (u64, String)) {
        (self.status.leader_fallback_rank(), self.age_key())
    }

    fn age_key(&self) -> (u64, String) {
        (
            self.up_number.unwrap_or(u64::MAX),
            self.unique_address.ordering_key(),
        )
    }

    /// Selects the membership fact that wins a concurrent gossip merge.
    ///
    /// Later lifecycle status wins; equal statuses choose the older member fact
    /// deterministically.
    pub fn highest_priority<'a>(left: &'a Self, right: &'a Self) -> &'a Self {
        if left.status == right.status {
            if left.is_older_than(right) {
                left
            } else {
                right
            }
        } else if left.status.priority() >= right.status.priority() {
            left
        } else {
            right
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node() -> UniqueAddress {
        UniqueAddress::new(Address::local("member-metadata"), 1)
    }

    #[test]
    fn member_defaults_historical_metadata_and_derives_reserved_data_center_role() {
        let historical = Member::new(node(), vec!["backend".to_string()]);
        assert!(historical.app_version.is_zero());
        assert_eq!(historical.data_center(), DEFAULT_DATA_CENTER);

        let current = Member::new(node(), vec!["backend".to_string(), "dc-west".to_string()])
            .with_app_version(ApplicationVersion::new("2.4.1").unwrap());
        assert_eq!(current.app_version.as_str(), "2.4.1");
        assert_eq!(current.data_center(), "west");
    }
}
