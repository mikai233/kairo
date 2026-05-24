//! Gossip-based cluster membership and cluster events.

mod cluster;
mod convergence;
mod downing;
mod event_publisher;
mod events;
mod failure_detector;
mod gossip;
mod heartbeat;
mod heartbeat_actor;
mod leader;
mod leader_actions;
mod member;
mod membership_actor;
mod protocol;
mod reachability;
mod vector_clock;

pub use cluster::{Cluster, ClusterError};
pub use convergence::{Convergence, ConvergenceBlocker};
pub use downing::{DowningDecision, DowningHook, DowningPlan, NoDowning, StaticDowningHook};
pub use event_publisher::{
    ClusterEventPublisher, ClusterEventPublisherMsg, ClusterSubscriptionEvent,
    ClusterSubscriptionInitialState, CurrentClusterState, SubscriptionInitialState,
};
pub use events::{ClusterEvent, ClusterEvents, MemberEvent, ReachabilityEvent};
pub use failure_detector::{
    DeadlineFailureDetector, DeadlineFailureDetectorSettings, FailureDetectorError,
    FailureDetectorRegistry,
};
pub use gossip::Gossip;
pub use heartbeat::{HeartbeatError, HeartbeatNodeRing, HeartbeatSenderState};
pub use heartbeat_actor::{
    HeartbeatClock, HeartbeatReceiver, HeartbeatReceiverMsg, HeartbeatSender, HeartbeatSenderMsg,
    HeartbeatSenderSettings, HeartbeatSenderSnapshot, SystemHeartbeatClock,
};
pub use leader::LeaderSelection;
pub use leader_actions::{LeaderActionError, LeaderActionOutcome, LeaderActions};
pub use member::{Member, MemberStatus, UniqueAddress};
pub use membership_actor::{ClusterMembership, ClusterMembershipMsg};
pub use protocol::{GossipEnvelope, Heartbeat, HeartbeatRsp, Join, Welcome};
pub use reachability::{Reachability, ReachabilityRecord, ReachabilityStatus};
pub use vector_clock::{VectorClock, VectorClockNode, VectorClockOrdering};
