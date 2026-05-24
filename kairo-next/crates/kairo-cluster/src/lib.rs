//! Gossip-based cluster membership and cluster events.

mod convergence;
mod gossip;
mod leader;
mod leader_actions;
mod member;
mod protocol;
mod reachability;
mod vector_clock;

pub use convergence::{Convergence, ConvergenceBlocker};
pub use gossip::Gossip;
pub use leader::LeaderSelection;
pub use leader_actions::{LeaderActionError, LeaderActionOutcome, LeaderActions};
pub use member::{Member, MemberStatus, UniqueAddress};
pub use protocol::{GossipEnvelope, Join, Welcome};
pub use reachability::{Reachability, ReachabilityRecord, ReachabilityStatus};
pub use vector_clock::{VectorClock, VectorClockNode, VectorClockOrdering};
