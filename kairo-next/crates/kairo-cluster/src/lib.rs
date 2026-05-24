//! Gossip-based cluster membership and cluster events.

mod protocol;
mod reachability;
mod vector_clock;

pub use kairo_actor::Address;
pub use protocol::{GossipEnvelope, Join, Welcome};
pub use reachability::{Reachability, ReachabilityRecord, ReachabilityStatus};
pub use vector_clock::{VectorClock, VectorClockNode, VectorClockOrdering};

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
