use std::collections::{BTreeMap, VecDeque};

use kairo_actor::ActorRef;

use crate::{ShardDeliverPlan, ShardId};

pub struct RegionHomeRequests<M>
where
    M: Send + 'static,
{
    delivery_replies_by_shard: BTreeMap<ShardId, VecDeque<ActorRef<ShardDeliverPlan<M>>>>,
}

impl<M> RegionHomeRequests<M>
where
    M: Send + 'static,
{
    pub fn new() -> Self {
        Self {
            delivery_replies_by_shard: BTreeMap::new(),
        }
    }

    pub fn remember_delivery(
        &mut self,
        shard: impl Into<ShardId>,
        delivery_reply_to: ActorRef<ShardDeliverPlan<M>>,
    ) {
        self.delivery_replies_by_shard
            .entry(shard.into())
            .or_default()
            .push_back(delivery_reply_to);
    }

    pub fn drain(&mut self, shard: &ShardId) -> Vec<ActorRef<ShardDeliverPlan<M>>> {
        self.delivery_replies_by_shard
            .remove(shard)
            .unwrap_or_default()
            .into_iter()
            .collect()
    }

    pub fn pending_shards(&self) -> impl Iterator<Item = &ShardId> + '_ {
        self.delivery_replies_by_shard.keys()
    }
}

impl<M> Default for RegionHomeRequests<M>
where
    M: Send + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}
