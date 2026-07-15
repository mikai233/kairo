#![deny(missing_docs)]
//! Typed local shard-region commands, orchestration results, and diagnostics.

use std::collections::BTreeSet;
use std::time::Duration;

use kairo_actor::{ActorRef, AskResult};
use kairo_cluster::{ClusterEvent, CurrentClusterState, UniqueAddress};

use crate::{
    BeginHandOffPlan, GetShardHome, GetShardHomePlan, HandOff, HandOffPlan, HostShardPlan,
    RegionDropReason, RegionId, RegionRegistrationStatus, RegionRouteDelivery, RegionRoutePlan,
    RegionRouteTarget, ShardCoordinatorMsg, ShardCoordinatorRemoteHome,
    ShardCoordinatorRemoteRegistrationAck, ShardCoordinatorRemoteTarget, ShardDeliverPlan,
    ShardHandOffPlan, ShardHomePlan, ShardId, ShardMsg, ShardRegionRemoteControlReplyTarget,
    ShardRegionRuntime, ShardStarted, ShardStartedPlan, ShardStopped, ShardingEnvelope,
    ShardingError,
};

/// Local actor protocol for one shard region.
///
/// This enum is not a wire contract. Stable remote coordinator, handoff, and
/// routed-entity messages are decoded by dedicated adapters and re-enter the
/// region through the corresponding `Remote*` variants. Reply references and
/// result messages preserve synchronous actor turns for asks and async work.
pub enum ShardRegionMsg<M>
where
    M: Send + 'static,
{
    /// Replaces membership-derived coordinator and remote-region targets.
    SetClusterTargets {
        /// Canonical node identity and typed ref for a local coordinator candidate.
        local_coordinator: (UniqueAddress, ActorRef<ShardCoordinatorMsg<M>>),
        /// Ordered remote coordinator candidates selected from cluster state.
        remote_coordinators: Vec<ShardCoordinatorRemoteTarget>,
        /// Remote region routes currently justified by cluster membership.
        remote_regions: Vec<RegionRouteTarget<M>>,
    },
    /// Evaluates one entity envelope without applying actor delivery effects.
    Route {
        /// Stable shard identifier derived before entering the region.
        shard: ShardId,
        /// Entity-addressed business message.
        message: ShardingEnvelope<M>,
        /// Recipient for the pure routing decision.
        reply_to: ActorRef<RegionRoutePlan<M>>,
    },
    /// Routes an envelope and applies local delivery, forwarding, or buffering effects.
    RouteToLocalShard {
        /// Stable target shard.
        shard: ShardId,
        /// Entity-addressed business message.
        message: ShardingEnvelope<M>,
        /// Recipient for the region-level routing outcome.
        route_reply_to: ActorRef<RegionLocalRoutePlan<M>>,
        /// Recipient for any local shard delivery plan.
        delivery_reply_to: ActorRef<ShardDeliverPlan<M>>,
    },
    /// Applies a coordinator request to host a shard on this region.
    HostShard {
        /// Shard to host.
        shard: ShardId,
        /// Recipient for startup, existing-host, or shutdown rejection details.
        reply_to: ActorRef<HostShardPlan<M>>,
    },
    /// Hosts a shard and explicitly reports buffered replay progress.
    HostShardAndReplayBuffered {
        /// Shard to host.
        shard: ShardId,
        /// Recipient for the replay result.
        reply_to: ActorRef<RegionBufferedReplayPlan>,
        /// Recipient for each replayed local shard delivery plan.
        delivery_reply_to: ActorRef<ShardDeliverPlan<M>>,
    },
    /// Records a coordinator-resolved local or remote shard home.
    RecordShardHome {
        /// Resolved shard.
        shard: ShardId,
        /// Region selected as the shard home.
        region: RegionId,
        /// Recipient for replay/forward planning or an inconsistent-home error.
        reply_to: ActorRef<Result<ShardHomePlan<M>, ShardingError>>,
    },
    /// Marks a newly created local shard ready and releases its buffer.
    MarkShardStarted {
        /// Started shard.
        shard: ShardId,
        /// Recipient for the startup acknowledgement and buffered envelopes.
        reply_to: ActorRef<ShardStartedPlan<M>>,
    },
    /// Applies phase one of coordinator-directed handoff.
    BeginHandOff {
        /// Shard whose cached home must be removed before acknowledgement.
        shard: ShardId,
        /// Recipient for the acknowledgement or shutdown suppression result.
        reply_to: ActorRef<BeginHandOffPlan>,
    },
    /// Applies phase two of coordinator-directed handoff.
    HandOff {
        /// Shard to stop or acknowledge absent.
        shard: ShardId,
        /// Recipient for local forwarding, immediate completion, and drop counts.
        reply_to: ActorRef<HandOffPlan>,
    },
    /// Re-enters a decoded remote host-shard command.
    RemoteHostShard {
        /// Shard requested by the remote coordinator.
        shard: ShardId,
        /// Stable remote reply target for `ShardStarted`.
        reply: ShardRegionRemoteControlReplyTarget,
    },
    /// Re-enters a decoded remote begin-handoff command.
    RemoteBeginHandOff {
        /// Shard entering the reorder-prevention phase.
        shard: ShardId,
        /// Stable remote reply target for `BeginHandOffAck`.
        reply: ShardRegionRemoteControlReplyTarget,
    },
    /// Re-enters a decoded remote handoff command.
    RemoteHandOff {
        /// Shard to hand off.
        shard: ShardId,
        /// Stable remote reply target for `ShardStopped`.
        reply: ShardRegionRemoteControlReplyTarget,
    },
    /// Returns the hosted shard's handoff plan to the remote-control flow.
    RemoteLocalShardHandOffObserved {
        /// Plan emitted by the local shard actor.
        plan: ShardHandOffPlan<M>,
        /// Maximum wait for an entity-stopper completion ask.
        timeout: Duration,
        /// Stable remote reply target retained across the local ask.
        reply: ShardRegionRemoteControlReplyTarget,
    },
    /// Returns the local entity-stopper ask used by a remote handoff.
    RemoteLocalShardHandOffStopperResult {
        /// Shard whose stopper was queried.
        shard: ShardId,
        /// Ask result indicating completion, stale state, timeout, or delivery failure.
        result: AskResult<bool>,
        /// Stable remote reply target for successful completion.
        reply: ShardRegionRemoteControlReplyTarget,
    },
    /// Begins graceful region shutdown and optionally returns the resulting snapshot.
    GracefulShutdown {
        /// Optional observer for the post-transition snapshot.
        reply_to: Option<ActorRef<ShardRegionSnapshot>>,
    },
    /// Forwards handoff to one hosted local shard.
    HandOffToLocalShard {
        /// Shard to stop.
        shard: ShardId,
        /// Application-defined entity stop message.
        stop_message: M,
        /// Recipient for the region-level handoff result.
        region_reply_to: ActorRef<RegionLocalHandOffPlan>,
        /// Recipient for the local shard actor's handoff plan.
        shard_reply_to: ActorRef<ShardHandOffPlan<M>>,
    },
    /// Waits for a hosted shard's entity stopper to complete.
    CompleteLocalShardHandOff {
        /// Shard whose handoff is being completed.
        shard: ShardId,
        /// Maximum stopper ask duration.
        timeout: Duration,
        /// Recipient for completion or a typed failure reason.
        reply_to: ActorRef<RegionLocalHandOffCompletionPlan>,
    },
    /// Returns a local entity-stopper completion ask to the mailbox.
    LocalShardHandOffStopperResult {
        /// Shard whose stopper was queried.
        shard: ShardId,
        /// Ask result from the local shard actor.
        result: AskResult<bool>,
        /// Recipient for the normalized completion plan.
        reply_to: ActorRef<RegionLocalHandOffCompletionPlan>,
    },
    /// Returns a local coordinator registration ask to the mailbox.
    CoordinatorRegistrationResult {
        /// Coordinator snapshot or registration delivery failure.
        result: Result<crate::CoordinatorStateSnapshot, ShardingError>,
    },
    /// Re-enters a validated remote coordinator registration acknowledgement.
    RemoteCoordinatorRegistrationAck {
        /// Stable acknowledgement including coordinator and region identity.
        ack: ShardCoordinatorRemoteRegistrationAck,
    },
    /// Retries local or remote coordinator registration.
    RetryCoordinatorRegistration,
    /// Retries unresolved shard-home requests for buffered shards.
    RetryPendingShardHomes,
    /// Returns a local coordinator shard-home ask to the mailbox.
    CoordinatorShardHomeResult {
        /// Shard originally requested, used to reject mismatched replies.
        requested_shard: ShardId,
        /// Coordinator plan or ask/delivery failure.
        result: Result<GetShardHomePlan, ShardingError>,
    },
    /// Re-enters a validated remote coordinator shard-home reply.
    RemoteCoordinatorShardHome {
        /// Stable shard-home reply carrying the requested shard and home region.
        home: ShardCoordinatorRemoteHome,
    },
    /// Applies the initial authoritative cluster snapshot to coordinator discovery.
    CoordinatorDiscoverySnapshot {
        /// Current cluster membership and reachability view.
        state: CurrentClusterState,
    },
    /// Applies one later cluster event to coordinator discovery.
    CoordinatorDiscoveryEvent {
        /// Membership or reachability change.
        event: ClusterEvent,
    },
    /// Consumes the result of best-effort buffered forwarding through an adapter.
    ForwardedBufferedRouteResult {
        /// Region routing result retained for observation and dead-letter handling.
        result: RegionLocalRoutePlan<M>,
    },
    /// Removes a stopped local shard and optionally returns the resulting snapshot.
    MarkShardStopped {
        /// Stopped shard.
        shard: ShardId,
        /// Optional observer for the post-removal snapshot.
        reply_to: Option<ActorRef<ShardRegionSnapshot>>,
    },
    /// Removes all cached homes for a stopped region.
    MarkRegionStopped {
        /// Stopped local or remote region.
        region: RegionId,
        /// Optional observer for the post-removal snapshot.
        reply_to: Option<ActorRef<ShardRegionSnapshot>>,
    },
    /// Fires a generation-checked local shard restart after backoff.
    RestartLocalShard {
        /// Shard eligible for restart.
        shard: ShardId,
        /// Restart generation used to reject a stale timer.
        generation: u64,
    },
    /// Directly changes the graceful-shutdown routing guard.
    SetGracefulShutdown {
        /// Whether graceful shutdown is active.
        in_progress: bool,
    },
    /// Directly changes the coordinated-shutdown preparation guard.
    SetPreparingForShutdown {
        /// Whether the node is preparing for shutdown.
        preparing: bool,
    },
    /// Requests a diagnostic region snapshot.
    GetState {
        /// Recipient for the snapshot.
        reply_to: ActorRef<ShardRegionSnapshot>,
    },
    /// Resolves a currently hosted local shard actor.
    GetLocalShard {
        /// Shard to resolve.
        shard: ShardId,
        /// Recipient for the local ref, or `None` when not hosted.
        reply_to: ActorRef<Option<ActorRef<ShardMsg<M>>>>,
    },
}

/// Actor-applied result of routing one entity envelope through a region.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegionLocalRoutePlan<M> {
    /// The envelope was sent to an already running local shard.
    DeliveredToLocalShard {
        /// Destination shard.
        shard: ShardId,
    },
    /// The runtime selected local delivery but no shard actor exists yet.
    MissingLocalShard {
        /// Missing local shard.
        shard: ShardId,
        /// Undelivered envelope retained by the result.
        message: ShardingEnvelope<M>,
    },
    /// The runtime selected a remote home that still needs transport delivery.
    Forward {
        /// Destination shard.
        shard: ShardId,
        /// Remote region home.
        region: RegionId,
        /// Envelope to forward.
        message: ShardingEnvelope<M>,
    },
    /// The envelope was submitted to a local or remote region target.
    ForwardedToRegion {
        /// Target and envelope delivery report.
        delivery: RegionRouteDelivery<M>,
    },
    /// The envelope entered the bounded shard buffer.
    Buffered {
        /// Buffered shard.
        shard: ShardId,
        /// First home request to send, or `None` when one is already outstanding.
        request: Option<GetShardHome>,
    },
    /// The region rejected the envelope.
    Dropped {
        /// Destination shard, or `None` when the shard id was empty.
        shard: Option<ShardId>,
        /// Why the envelope was dropped.
        reason: RegionDropReason,
        /// Undelivered envelope.
        message: ShardingEnvelope<M>,
    },
}

/// Result of hosting a local shard and replaying its buffered envelopes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegionBufferedReplayPlan {
    /// A local shard started and received all buffered envelopes.
    Replayed {
        /// Started shard.
        shard: ShardId,
        /// Coordinator acknowledgement for the started shard.
        started: ShardStarted,
        /// Number of envelopes replayed in FIFO order.
        replayed: usize,
    },
    /// No local shard factory was configured to create the shard.
    MissingLocalShardSpawner {
        /// Shard that could not be created.
        shard: ShardId,
    },
    /// Hosting was rejected because graceful shutdown is active.
    IgnoredGracefulShutdown {
        /// Rejected shard.
        shard: ShardId,
    },
}

/// Region-level result of forwarding handoff to a local shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegionLocalHandOffPlan {
    /// The handoff command was sent to a hosted shard.
    ForwardedToLocalShard {
        /// Hosted shard.
        shard: ShardId,
        /// Forwarded coordinator command.
        command: HandOff,
        /// Messages dropped to prevent cross-region reordering.
        dropped_buffered: usize,
    },
    /// Runtime state expected a local shard, but no actor ref was available.
    MissingLocalShard {
        /// Missing shard.
        shard: ShardId,
        /// Coordinator command that could not be forwarded.
        command: HandOff,
        /// Messages dropped before discovering the missing actor.
        dropped_buffered: usize,
    },
    /// The shard was already absent, so the region can acknowledge immediately.
    ReplyShardStopped {
        /// Absent shard.
        shard: ShardId,
        /// Immediate coordinator acknowledgement.
        stopped: ShardStopped,
        /// Messages dropped to preserve handoff ordering.
        dropped_buffered: usize,
    },
}

/// Normalized completion of a local shard's entity-stopper handoff.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegionLocalHandOffCompletionPlan {
    /// All entity children stopped and the shard can be acknowledged stopped.
    Completed {
        /// Completed shard.
        shard: ShardId,
        /// Coordinator acknowledgement.
        stopped: ShardStopped,
    },
    /// The stopper ask could not prove handoff completion.
    Failed {
        /// Incomplete shard.
        shard: ShardId,
        /// Typed failure cause.
        reason: RegionLocalHandOffCompletionFailure,
    },
}

/// Reason local shard handoff completion could not be proven.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegionLocalHandOffCompletionFailure {
    /// The region no longer has a ref for the expected local shard.
    MissingLocalShard,
    /// The shard reported that no entity-stopper handoff was active.
    StopperNotInProgress,
    /// The entity-stopper ask exceeded its deadline.
    StopperTimeout {
        /// Configured ask deadline.
        timeout: Duration,
    },
}

/// Diagnostic snapshot of region routing, shard lifecycle, buffering, and registration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardRegionSnapshot {
    /// This region's stable identifier.
    pub self_region: RegionId,
    /// Fully started local shards.
    pub local_shards: BTreeSet<ShardId>,
    /// Local shards whose actor startup has not completed.
    pub starting_shards: BTreeSet<ShardId>,
    /// Shards removed from routing while handoff completes.
    pub handing_off_shards: BTreeSet<ShardId>,
    /// Number of buffered entity envelopes across all shards.
    pub total_buffered: usize,
    /// Current local or remote coordinator registration state.
    pub registration_status: RegionRegistrationStatus,
}

impl<M> From<&ShardRegionRuntime<M>> for ShardRegionSnapshot {
    fn from(value: &ShardRegionRuntime<M>) -> Self {
        Self {
            self_region: value.self_region().clone(),
            local_shards: value.local_shards().clone(),
            starting_shards: value.starting_shards().clone(),
            handing_off_shards: value.handing_off_shards().clone(),
            total_buffered: value.total_buffered_count(),
            registration_status: RegionRegistrationStatus::Disabled,
        }
    }
}
