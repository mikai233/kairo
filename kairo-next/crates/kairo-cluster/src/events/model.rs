use std::collections::HashSet;

use crate::{Member, MemberStatus, Reachability, UniqueAddress};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterEvent {
    Member(MemberEvent),
    Reachability(ReachabilityEvent),
    LeaderChanged {
        leader: Option<UniqueAddress>,
    },
    RoleLeaderChanged {
        role: String,
        leader: Option<UniqueAddress>,
    },
    SeenChanged {
        converged: bool,
        seen_by: HashSet<UniqueAddress>,
    },
    ReachabilityChanged {
        reachability: Reachability,
    },
    MemberTombstonesChanged {
        tombstones: HashSet<UniqueAddress>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemberEvent {
    Joined(Member),
    WeaklyUp(Member),
    Up(Member),
    Left(Member),
    Exited(Member),
    Downed(Member),
    Removed {
        member: Member,
        previous_status: MemberStatus,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReachabilityEvent {
    Unreachable(Member),
    Reachable(Member),
}
