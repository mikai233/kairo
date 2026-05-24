use kairo_actor::Address;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MemberStatus {
    Joining,
    WeaklyUp,
    Up,
    Leaving,
    Exiting,
    Down,
    Removed,
}

impl MemberStatus {
    pub fn participates_in_leader_selection(self) -> bool {
        matches!(self, Self::Up | Self::Leaving)
    }

    pub fn participates_in_convergence(self) -> bool {
        matches!(self, Self::Up | Self::Leaving)
    }

    pub fn participates_in_first_convergence(self) -> bool {
        matches!(
            self,
            Self::Joining | Self::WeaklyUp | Self::Up | Self::Leaving
        )
    }

    pub fn can_skip_unreachable_for_convergence(self) -> bool {
        matches!(self, Self::Down | Self::Exiting)
    }

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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UniqueAddress {
    pub address: Address,
    pub uid: u64,
}

impl UniqueAddress {
    pub fn new(address: Address, uid: u64) -> Self {
        Self { address, uid }
    }

    pub fn ordering_key(&self) -> String {
        format!("{}#{}", self.address, self.uid)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Member {
    pub unique_address: UniqueAddress,
    pub status: MemberStatus,
    pub roles: Vec<String>,
    pub up_number: Option<u64>,
}

impl Member {
    pub fn new(unique_address: UniqueAddress, roles: Vec<String>) -> Self {
        Self {
            unique_address,
            status: MemberStatus::Joining,
            roles,
            up_number: None,
        }
    }

    pub fn with_status(mut self, status: MemberStatus) -> Self {
        self.status = status;
        self
    }

    pub fn with_up_number(mut self, up_number: u64) -> Self {
        self.up_number = Some(up_number);
        self
    }

    pub fn has_role(&self, role: &str) -> bool {
        self.roles.iter().any(|member_role| member_role == role)
    }

    pub fn is_older_than(&self, other: &Self) -> bool {
        self.age_key() < other.age_key()
    }

    pub fn leader_fallback_key(&self) -> (u8, (u64, String)) {
        (self.status.leader_fallback_rank(), self.age_key())
    }

    fn age_key(&self) -> (u64, String) {
        (
            self.up_number.unwrap_or(u64::MAX),
            self.unique_address.ordering_key(),
        )
    }

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
