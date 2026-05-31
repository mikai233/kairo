use std::collections::BTreeMap;
use std::time::Duration;

use kairo_actor::{ActorError, ActorRef, Context};

use crate::{
    BeginHandOffAck, BeginHandOffPlan, GetShardHomePlan, HandoffRegionTarget, HandoffTransport,
    HandoffWorkerActor, HandoffWorkerMsg, RegionId, ShardCoordinatorMsg, ShardId,
    ShardRebalancePlan, ShardStopped,
};

pub struct CoordinatorHandoff<M>
where
    M: Send + 'static,
{
    stop_message: M,
    handoff_timeout: Duration,
    transport: HandoffTransport<M>,
    active_workers: BTreeMap<ShardId, ActorRef<HandoffWorkerMsg<M>>>,
}

impl<M> CoordinatorHandoff<M>
where
    M: Clone + Send + 'static,
{
    pub fn new(stop_message: M, handoff_timeout: Duration, transport: HandoffTransport<M>) -> Self {
        Self {
            stop_message,
            handoff_timeout,
            transport,
            active_workers: BTreeMap::new(),
        }
    }

    pub fn spawn_workers(
        &mut self,
        ctx: &Context<ShardCoordinatorMsg<M>>,
        plans: &[ShardRebalancePlan],
    ) -> Result<Vec<ShardId>, ActorError> {
        let mut spawned = Vec::new();
        for plan in plans {
            if self.active_workers.contains_key(&plan.shard) {
                continue;
            }

            let worker = ctx.spawn_anonymous(HandoffWorkerActor::props(
                plan.clone(),
                self.stop_message.clone(),
                self.handoff_timeout,
                self.transport.clone(),
            ))?;
            let reply_to = ctx.message_adapter(ShardCoordinatorMsg::HandoffWorkerDone)?;
            worker
                .tell(HandoffWorkerMsg::Start { reply_to })
                .map_err(|error| ActorError::Message(error.reason().to_string()))?;
            self.active_workers.insert(plan.shard.clone(), worker);
            spawned.push(plan.shard.clone());
        }
        Ok(spawned)
    }

    pub fn remove_worker(&mut self, shard: &ShardId) {
        self.active_workers.remove(shard);
    }

    pub fn register_region_target(&mut self, target: HandoffRegionTarget<M>) {
        self.transport.insert_target(target);
    }

    pub fn dispatch_host_shard(
        &self,
        ctx: &Context<ShardCoordinatorMsg<M>>,
        plan: &GetShardHomePlan,
    ) -> Result<(), ActorError> {
        let GetShardHomePlan::Allocated {
            host_region,
            host_shard,
            ..
        } = plan
        else {
            return Ok(());
        };

        let shard = host_shard.shard_id.clone();
        let reply_to = ctx.message_adapter(move |_| ShardCoordinatorMsg::HostShardObserved {
            shard: shard.clone(),
        })?;
        let report = self
            .transport
            .send_host_shard_to(host_region, &host_shard.shard_id, reply_to);
        if report.is_success() {
            Ok(())
        } else {
            Err(ActorError::Message(format!(
                "failed to dispatch host shard: {:?}",
                report.failures()
            )))
        }
    }

    pub fn active_worker_shards(&self) -> Vec<ShardId> {
        self.active_workers.keys().cloned().collect()
    }

    pub fn forward_remote_begin_handoff_ack(
        &self,
        region: RegionId,
        ack: BeginHandOffAck,
    ) -> Result<(), ActorError> {
        let Some(worker) = self.active_workers.get(&ack.shard_id) else {
            return Ok(());
        };
        let shard = ack.shard_id.clone();
        worker
            .tell(HandoffWorkerMsg::BeginHandOffAck {
                region,
                plan: BeginHandOffPlan::Ack { shard, ack },
            })
            .map_err(|error| ActorError::Message(error.reason().to_string()))
    }

    pub fn forward_remote_shard_stopped(
        &self,
        region: RegionId,
        stopped: ShardStopped,
    ) -> Result<(), ActorError> {
        let Some(worker) = self.active_workers.get(&stopped.shard_id) else {
            return Ok(());
        };
        worker
            .tell(HandoffWorkerMsg::RemoteShardStopped { region, stopped })
            .map_err(|error| ActorError::Message(error.reason().to_string()))
    }

    pub fn transport_mut(&mut self) -> &mut HandoffTransport<M> {
        &mut self.transport
    }
}

impl<M> std::fmt::Debug for CoordinatorHandoff<M>
where
    M: Send + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CoordinatorHandoff")
            .field("handoff_timeout", &self.handoff_timeout)
            .field(
                "active_workers",
                &self.active_workers.keys().collect::<Vec<_>>(),
            )
            .finish_non_exhaustive()
    }
}
