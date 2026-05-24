//! Gossip-based cluster membership and cluster events.

pub use kairo_actor::Address;

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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UniqueAddress {
    pub address: Address,
    pub uid: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Member {
    pub unique_address: UniqueAddress,
    pub status: MemberStatus,
    pub roles: Vec<String>,
}
