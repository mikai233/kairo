use std::collections::BTreeSet;

use kairo_actor::ActorRef;

use crate::{
    BeginHandOffPlan, GetShardHome, HandOffPlan, HostShardPlan, RegionDropReason, RegionId,
    RegionRoutePlan, ShardDeliverPlan, ShardHomePlan, ShardId, ShardMsg, ShardRegionRuntime,
    ShardStarted, ShardStartedPlan, ShardingEnvelope, ShardingError,
};

pub enum ShardRegionMsg<M> {
    Route {
        shard: ShardId,
        message: ShardingEnvelope<M>,
        reply_to: ActorRef<RegionRoutePlan<M>>,
    },
    RouteToLocalShard {
        shard: ShardId,
        message: ShardingEnvelope<M>,
        route_reply_to: ActorRef<RegionLocalRoutePlan<M>>,
        delivery_reply_to: ActorRef<ShardDeliverPlan<M>>,
    },
    HostShard {
        shard: ShardId,
        reply_to: ActorRef<HostShardPlan<M>>,
    },
    HostShardAndReplayBuffered {
        shard: ShardId,
        reply_to: ActorRef<RegionBufferedReplayPlan>,
        delivery_reply_to: ActorRef<ShardDeliverPlan<M>>,
    },
    RecordShardHome {
        shard: ShardId,
        region: RegionId,
        reply_to: ActorRef<Result<ShardHomePlan<M>, ShardingError>>,
    },
    MarkShardStarted {
        shard: ShardId,
        reply_to: ActorRef<ShardStartedPlan<M>>,
    },
    BeginHandOff {
        shard: ShardId,
        reply_to: ActorRef<BeginHandOffPlan>,
    },
    HandOff {
        shard: ShardId,
        reply_to: ActorRef<HandOffPlan>,
    },
    MarkShardStopped {
        shard: ShardId,
        reply_to: Option<ActorRef<ShardRegionSnapshot>>,
    },
    SetGracefulShutdown {
        in_progress: bool,
    },
    SetPreparingForShutdown {
        preparing: bool,
    },
    GetState {
        reply_to: ActorRef<ShardRegionSnapshot>,
    },
    GetLocalShard {
        shard: ShardId,
        reply_to: ActorRef<Option<ActorRef<ShardMsg<M>>>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegionLocalRoutePlan<M> {
    DeliveredToLocalShard {
        shard: ShardId,
    },
    MissingLocalShard {
        shard: ShardId,
        message: ShardingEnvelope<M>,
    },
    Forward {
        shard: ShardId,
        region: RegionId,
        message: ShardingEnvelope<M>,
    },
    Buffered {
        shard: ShardId,
        request: Option<GetShardHome>,
    },
    Dropped {
        shard: Option<ShardId>,
        reason: RegionDropReason,
        message: ShardingEnvelope<M>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegionBufferedReplayPlan {
    Replayed {
        shard: ShardId,
        started: ShardStarted,
        replayed: usize,
    },
    MissingLocalShardSpawner {
        shard: ShardId,
    },
    IgnoredGracefulShutdown {
        shard: ShardId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardRegionSnapshot {
    pub self_region: RegionId,
    pub local_shards: BTreeSet<ShardId>,
    pub starting_shards: BTreeSet<ShardId>,
    pub handing_off_shards: BTreeSet<ShardId>,
    pub total_buffered: usize,
}

impl<M> From<&ShardRegionRuntime<M>> for ShardRegionSnapshot {
    fn from(value: &ShardRegionRuntime<M>) -> Self {
        Self {
            self_region: value.self_region().clone(),
            local_shards: value.local_shards().clone(),
            starting_shards: value.starting_shards().clone(),
            handing_off_shards: value.handing_off_shards().clone(),
            total_buffered: value.total_buffered_count(),
        }
    }
}
