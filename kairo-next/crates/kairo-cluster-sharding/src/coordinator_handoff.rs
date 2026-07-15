#![deny(missing_docs)]
//! Coordinator-owned shard handoff worker lifecycle and reply routing.

use std::collections::BTreeMap;
use std::time::Duration;

use kairo_actor::{ActorError, ActorRef, Context};

use crate::{
    BeginHandOffAck, BeginHandOffPlan, GetShardHomePlan, HandoffRegionTarget, HandoffTransport,
    HandoffWorkerActor, HandoffWorkerMsg, RegionId, ShardCoordinatorMsg, ShardId,
    ShardRebalancePlan, ShardStopped,
};

/// Owns the coordinator's active per-shard handoff workers and their transport routes.
///
/// At most one worker is tracked for each shard. The coordinator removes the
/// entry after receiving [`crate::HandoffWorkerDone`].
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
    /// Creates an empty handoff manager using the supplied entity stop message and timeout.
    pub fn new(stop_message: M, handoff_timeout: Duration, transport: HandoffTransport<M>) -> Self {
        Self {
            stop_message,
            handoff_timeout,
            transport,
            active_workers: BTreeMap::new(),
        }
    }

    /// Starts one anonymous handoff worker for every plan not already in progress.
    ///
    /// The returned shard IDs identify workers newly started by this call. If
    /// spawning or starting a later worker fails, workers already started by
    /// the call remain tracked and continue independently.
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

    /// Forgets the active worker for `shard`, if one is tracked.
    pub fn remove_worker(&mut self, shard: &ShardId) {
        self.active_workers.remove(shard);
    }

    /// Adds or replaces the transport route for a region.
    pub fn register_region_target(&mut self, target: HandoffRegionTarget<M>) {
        self.transport.insert_target(target);
    }

    /// Removes the transport route for a region.
    ///
    /// Active workers retain their cloned transport snapshot; coordinator
    /// termination forwarding is what releases them from a departed region.
    pub fn remove_region_target(&mut self, region: &RegionId) {
        self.transport.remove_target(region);
    }

    /// Best-effort dispatches the host command produced by an allocation plan.
    ///
    /// Non-allocation plans are ignored. Recipient absence or send rejection
    /// is also tolerated because allocation is already coordinator state and a
    /// region with buffered traffic can request the shard home again.
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
        // Host-shard delivery is best-effort, matching actor-send semantics.
        // The allocation is already durable in coordinator state and a region
        // with a buffered message will retry GetShardHome if this send races a
        // transient route gap.
        let _ = self
            .transport
            .send_host_shard_to(host_region, &host_shard.shard_id, reply_to);
        Ok(())
    }

    /// Returns the shard IDs with currently tracked workers in sorted order.
    pub fn active_worker_shards(&self) -> Vec<ShardId> {
        self.active_workers.keys().cloned().collect()
    }

    /// Routes a remote region's begin-handoff acknowledgement to its shard worker.
    ///
    /// A late acknowledgement for a shard without an active worker is ignored.
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

    /// Routes a remote owner's shard-stopped reply to its shard worker.
    ///
    /// A late reply for a shard without an active worker is ignored. The
    /// worker validates both the owner identity and shard identity.
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

    /// Notifies every active worker that a region terminated.
    ///
    /// During begin handoff this counts as that participant's acknowledgement;
    /// during owner shutdown, owner termination completes handoff successfully.
    pub fn forward_region_terminated(&self, region: &RegionId) -> Result<(), ActorError> {
        for worker in self.active_workers.values() {
            worker
                .tell(HandoffWorkerMsg::RegionTerminated {
                    region: region.clone(),
                })
                .map_err(|error| ActorError::Message(error.reason().to_string()))?;
        }
        Ok(())
    }

    /// Returns mutable access to the routes cloned into subsequently spawned workers.
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
