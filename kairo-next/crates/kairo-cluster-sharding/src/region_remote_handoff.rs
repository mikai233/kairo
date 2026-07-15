#![deny(missing_docs)]
//! Adapts stable remote handoff commands to typed local shard stopping.

use std::sync::Arc;
use std::time::Duration;

use crate::{HandOff, HandOffPlan, ShardHandOffPlan, ShardId, ShardStopped};

/// Supplies typed entity stop messages for handoffs requested over remoting.
///
/// The stable wire-level [`HandOff`] command cannot carry the local business
/// message type `M`. A region installs this factory to create a fresh stop
/// message whenever a remote coordinator asks its shard to stop.
#[derive(Clone)]
pub struct RegionRemoteHandOff<M> {
    stop_message: Arc<dyn Fn() -> M + Send + Sync>,
    timeout: Duration,
}

impl<M> RegionRemoteHandOff<M> {
    /// Creates remote-handoff settings from a stop-message factory and stopper timeout.
    pub fn new(stop_message: impl Fn() -> M + Send + Sync + 'static, timeout: Duration) -> Self {
        Self {
            stop_message: Arc::new(stop_message),
            timeout,
        }
    }

    /// Creates remote-handoff settings by cloning one stop-message value per request.
    pub fn from_message(stop_message: M, timeout: Duration) -> Self
    where
        M: Clone + Send + Sync + 'static,
    {
        Self::new(move || stop_message.clone(), timeout)
    }

    /// Produces the next typed entity stop message.
    pub fn next_stop_message(&self) -> M {
        (self.stop_message)()
    }

    /// Returns the maximum wait passed to entity-stopper completion.
    pub fn timeout(&self) -> Duration {
        self.timeout
    }
}

/// Typed action produced when a region handles a remote handoff command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegionRemoteHandOffAction<M> {
    /// Forward the command and a newly produced stop message to the local shard.
    ForwardToLocalShard {
        /// Local shard to stop.
        shard: ShardId,
        /// Stable handoff command received from the coordinator.
        command: HandOff,
        /// Typed message to send to each active entity.
        stop_message: M,
        /// Buffered messages discarded to prevent reordering across the handoff.
        dropped_buffered: usize,
        /// Maximum entity-stopper completion wait.
        timeout: Duration,
    },
    /// Reply immediately because this region no longer owns a local shard.
    ReplyShardStopped {
        /// Shard already absent from the region.
        shard: ShardId,
        /// Stable acknowledgement to return to the coordinator.
        stopped: ShardStopped,
        /// Buffered messages discarded to prevent reordering across the handoff.
        dropped_buffered: usize,
    },
    /// Fail local adaptation because no typed stop-message factory is configured.
    MissingStopMessage {
        /// Local shard that would have been stopped.
        shard: ShardId,
        /// Stable handoff command that could not be adapted.
        command: HandOff,
        /// Buffered messages already discarded to prevent reordering.
        dropped_buffered: usize,
    },
}

/// Adds typed remote-handoff settings to a region runtime decision.
pub fn plan_remote_handoff<M>(
    plan: HandOffPlan,
    remote_handoff: Option<&RegionRemoteHandOff<M>>,
) -> RegionRemoteHandOffAction<M> {
    match plan {
        HandOffPlan::ReplyShardStopped {
            shard,
            stopped,
            dropped_buffered,
        } => RegionRemoteHandOffAction::ReplyShardStopped {
            shard,
            stopped,
            dropped_buffered,
        },
        HandOffPlan::ForwardToLocalShard {
            shard,
            command,
            dropped_buffered,
        } => {
            if let Some(remote_handoff) = remote_handoff {
                RegionRemoteHandOffAction::ForwardToLocalShard {
                    shard,
                    command,
                    stop_message: remote_handoff.next_stop_message(),
                    dropped_buffered,
                    timeout: remote_handoff.timeout(),
                }
            } else {
                RegionRemoteHandOffAction::MissingStopMessage {
                    shard,
                    command,
                    dropped_buffered,
                }
            }
        }
    }
}

/// Action produced from a local shard's response to remote handoff.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegionRemoteShardHandOffAction {
    /// Return shard-stopped to the remote coordinator immediately.
    Complete {
        /// Shard whose local stop completed.
        shard: ShardId,
        /// Stable acknowledgement to return.
        stopped: ShardStopped,
    },
    /// Ask the region's entity stopper to finish stopping the shard.
    AskStopper {
        /// Shard whose entities are stopping.
        shard: ShardId,
        /// Maximum stopper completion wait.
        timeout: Duration,
    },
    /// Report that a handoff is already stopping this shard.
    AlreadyInProgress {
        /// Shard already being handed off.
        shard: ShardId,
    },
}

/// Converts a local shard handoff decision into its remote completion action.
pub fn plan_remote_shard_handoff<M>(
    plan: ShardHandOffPlan<M>,
    timeout: Duration,
) -> RegionRemoteShardHandOffAction {
    match plan {
        ShardHandOffPlan::ReplyShardStopped { shard, stopped } => {
            RegionRemoteShardHandOffAction::Complete { shard, stopped }
        }
        ShardHandOffPlan::StopImmediately { shard, stopped, .. } => {
            RegionRemoteShardHandOffAction::Complete { shard, stopped }
        }
        ShardHandOffPlan::StartEntityStopper { shard, .. } => {
            RegionRemoteShardHandOffAction::AskStopper { shard, timeout }
        }
        ShardHandOffPlan::AlreadyInProgress { shard } => {
            RegionRemoteShardHandOffAction::AlreadyInProgress { shard }
        }
    }
}
