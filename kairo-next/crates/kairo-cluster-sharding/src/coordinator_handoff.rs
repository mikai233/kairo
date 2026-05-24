use std::collections::BTreeMap;
use std::time::Duration;

use kairo_actor::{ActorError, ActorRef, Context};

use crate::{
    HandoffTransport, HandoffWorkerActor, HandoffWorkerMsg, ShardCoordinatorMsg, ShardId,
    ShardRebalancePlan,
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
        ctx: &Context<ShardCoordinatorMsg>,
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

    pub fn active_worker_shards(&self) -> Vec<ShardId> {
        self.active_workers.keys().cloned().collect()
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
