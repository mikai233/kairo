use std::sync::Arc;
use std::time::Duration;

use crate::{HandOff, HandOffPlan, ShardHandOffPlan, ShardId, ShardStopped};

#[derive(Clone)]
pub struct RegionRemoteHandOff<M> {
    stop_message: Arc<dyn Fn() -> M + Send + Sync>,
    timeout: Duration,
}

impl<M> RegionRemoteHandOff<M> {
    pub fn new(stop_message: impl Fn() -> M + Send + Sync + 'static, timeout: Duration) -> Self {
        Self {
            stop_message: Arc::new(stop_message),
            timeout,
        }
    }

    pub fn from_message(stop_message: M, timeout: Duration) -> Self
    where
        M: Clone + Send + Sync + 'static,
    {
        Self::new(move || stop_message.clone(), timeout)
    }

    pub fn next_stop_message(&self) -> M {
        (self.stop_message)()
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegionRemoteHandOffAction<M> {
    ForwardToLocalShard {
        shard: ShardId,
        command: HandOff,
        stop_message: M,
        dropped_buffered: usize,
        timeout: Duration,
    },
    ReplyShardStopped {
        shard: ShardId,
        stopped: ShardStopped,
        dropped_buffered: usize,
    },
    MissingStopMessage {
        shard: ShardId,
        command: HandOff,
        dropped_buffered: usize,
    },
}

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegionRemoteShardHandOffAction {
    Complete {
        shard: ShardId,
        stopped: ShardStopped,
    },
    AskStopper {
        shard: ShardId,
        timeout: Duration,
    },
    AlreadyInProgress {
        shard: ShardId,
    },
}

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
