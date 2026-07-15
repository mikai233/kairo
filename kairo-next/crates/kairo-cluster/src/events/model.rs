#![deny(missing_docs)]

use std::collections::HashSet;

use crate::{Member, MemberStatus, Reachability, UniqueAddress};

/// Observable change derived from two cluster gossip snapshots.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterEvent {
    /// A member joined or changed lifecycle status.
    Member(MemberEvent),
    /// A member became unreachable or recovered.
    Reachability(ReachabilityEvent),
    /// The locally selected cluster leader changed.
    LeaderChanged {
        /// New leader, or `None` when no member is eligible.
        leader: Option<UniqueAddress>,
    },
    /// The locally selected leader for one role changed.
    RoleLeaderChanged {
        /// Role whose leader changed.
        role: String,
        /// New role leader, or `None` when no member is eligible.
        leader: Option<UniqueAddress>,
    },
    /// Seen acknowledgements or the resulting convergence state changed.
    SeenChanged {
        /// Whether the new gossip snapshot is converged.
        converged: bool,
        /// Members that acknowledged the new gossip version.
        seen_by: HashSet<UniqueAddress>,
    },
    /// The observer-versioned reachability table changed.
    ReachabilityChanged {
        /// Complete new reachability table.
        reachability: Reachability,
    },
    /// The set of member-removal tombstones changed.
    MemberTombstonesChanged {
        /// Unique addresses currently protected by tombstones.
        tombstones: HashSet<UniqueAddress>,
    },
}

/// Observable membership lifecycle transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemberEvent {
    /// A member entered `Joining`.
    Joined(Member),
    /// A member entered `WeaklyUp`.
    WeaklyUp(Member),
    /// A member entered `Up` or received its final age assignment.
    Up(Member),
    /// A member began graceful leave.
    Left(Member),
    /// A leaving member completed handoff and entered `Exiting`.
    Exited(Member),
    /// A member was downed.
    Downed(Member),
    /// A member was removed from live gossip.
    Removed {
        /// Removed membership fact, normalized to `Removed` status.
        member: Member,
        /// Status immediately before removal.
        previous_status: MemberStatus,
    },
}

/// Observable aggregate reachability transition for one member.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReachabilityEvent {
    /// A non-self member gained an unreachable or terminated observation.
    Unreachable(Member),
    /// A previously unreachable member has no remaining negative observation.
    Reachable(Member),
}
