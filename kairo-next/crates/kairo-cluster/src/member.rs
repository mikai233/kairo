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
}

impl Member {
    pub fn new(unique_address: UniqueAddress, roles: Vec<String>) -> Self {
        Self {
            unique_address,
            status: MemberStatus::Joining,
            roles,
        }
    }

    pub fn with_status(mut self, status: MemberStatus) -> Self {
        self.status = status;
        self
    }

    pub fn highest_priority<'a>(left: &'a Self, right: &'a Self) -> &'a Self {
        if left.status.priority() >= right.status.priority() {
            left
        } else {
            right
        }
    }
}
