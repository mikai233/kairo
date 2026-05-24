use std::collections::BTreeSet;

use kairo_actor::{Actor, ActorRef, ActorResult, Context, Props};

use crate::{
    BeginHandOffPlan, HandOffPlan, HostShardPlan, RegionId, RegionRoutePlan, ShardHomePlan,
    ShardId, ShardRegionRuntime, ShardStartedPlan, ShardingEnvelope, ShardingError,
};

pub struct ShardRegionActor<M> {
    runtime: ShardRegionRuntime<M>,
}

impl<M> ShardRegionActor<M> {
    pub fn new(self_region: impl Into<RegionId>, buffer_capacity: usize) -> Self {
        Self {
            runtime: ShardRegionRuntime::new(self_region, buffer_capacity),
        }
    }

    pub fn props(self_region: impl Into<RegionId>, buffer_capacity: usize) -> Props<Self>
    where
        M: Send + 'static,
    {
        let self_region = self_region.into();
        Props::new(move || Self::new(self_region, buffer_capacity))
    }

    pub fn runtime(&self) -> &ShardRegionRuntime<M> {
        &self.runtime
    }
}

pub enum ShardRegionMsg<M> {
    Route {
        shard: ShardId,
        message: ShardingEnvelope<M>,
        reply_to: ActorRef<RegionRoutePlan<M>>,
    },
    HostShard {
        shard: ShardId,
        reply_to: ActorRef<HostShardPlan<M>>,
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardRegionSnapshot {
    pub self_region: RegionId,
    pub local_shards: BTreeSet<ShardId>,
    pub starting_shards: BTreeSet<ShardId>,
    pub handing_off_shards: BTreeSet<ShardId>,
    pub total_buffered: usize,
}

impl<M> Actor for ShardRegionActor<M>
where
    M: Send + 'static,
{
    type Msg = ShardRegionMsg<M>;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        match msg {
            ShardRegionMsg::Route {
                shard,
                message,
                reply_to,
            } => {
                let plan = self.runtime.route(shard, message);
                let _ = reply_to.tell(plan);
            }
            ShardRegionMsg::HostShard { shard, reply_to } => {
                let plan = self.runtime.host_shard(shard);
                let _ = reply_to.tell(plan);
            }
            ShardRegionMsg::RecordShardHome {
                shard,
                region,
                reply_to,
            } => {
                let plan = self.runtime.record_shard_home(shard, region);
                let _ = reply_to.tell(plan);
            }
            ShardRegionMsg::MarkShardStarted { shard, reply_to } => {
                let plan = self.runtime.mark_shard_started(shard);
                let _ = reply_to.tell(plan);
            }
            ShardRegionMsg::BeginHandOff { shard, reply_to } => {
                let plan = self.runtime.begin_handoff(shard);
                let _ = reply_to.tell(plan);
            }
            ShardRegionMsg::HandOff { shard, reply_to } => {
                let plan = self.runtime.handoff(shard);
                let _ = reply_to.tell(plan);
            }
            ShardRegionMsg::MarkShardStopped { shard, reply_to } => {
                self.runtime.mark_shard_stopped(&shard);
                reply_optional(reply_to, ShardRegionSnapshot::from(&self.runtime));
            }
            ShardRegionMsg::SetGracefulShutdown { in_progress } => {
                self.runtime.set_graceful_shutdown_in_progress(in_progress);
            }
            ShardRegionMsg::SetPreparingForShutdown { preparing } => {
                self.runtime.set_preparing_for_shutdown(preparing);
            }
            ShardRegionMsg::GetState { reply_to } => {
                let _ = reply_to.tell(ShardRegionSnapshot::from(&self.runtime));
            }
        }
        Ok(())
    }
}

impl<M> From<&ShardRegionRuntime<M>> for ShardRegionSnapshot {
    fn from(runtime: &ShardRegionRuntime<M>) -> Self {
        Self {
            self_region: runtime.self_region().clone(),
            local_shards: runtime.local_shards().clone(),
            starting_shards: runtime.starting_shards().clone(),
            handing_off_shards: runtime.handing_off_shards().clone(),
            total_buffered: runtime.total_buffered_count(),
        }
    }
}

fn reply_optional<M>(reply_to: Option<ActorRef<M>>, message: M)
where
    M: Send + 'static,
{
    if let Some(reply_to) = reply_to {
        let _ = reply_to.tell(message);
    }
}
