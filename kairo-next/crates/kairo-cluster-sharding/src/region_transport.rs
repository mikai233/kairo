use std::collections::BTreeMap;
use std::sync::Arc;

use kairo_actor::{ActorRef, Recipient};

use crate::{
    RegionId, RegionLocalRoutePlan, ShardDeliverPlan, ShardId, ShardRegionMsg, ShardingEnvelope,
};

type RegionRecipient<M> = Arc<dyn Recipient<ShardRegionMsg<M>> + Send + Sync>;

#[derive(Clone)]
pub struct RegionRouteTarget<M>
where
    M: Send + 'static,
{
    region: RegionId,
    recipient: RegionRecipient<M>,
}

impl<M> RegionRouteTarget<M>
where
    M: Send + 'static,
{
    pub fn new(
        region: impl Into<RegionId>,
        recipient: impl Recipient<ShardRegionMsg<M>> + Send + Sync + 'static,
    ) -> Self {
        Self {
            region: region.into(),
            recipient: Arc::new(recipient),
        }
    }

    pub fn from_arc(region: impl Into<RegionId>, recipient: RegionRecipient<M>) -> Self {
        Self {
            region: region.into(),
            recipient,
        }
    }

    pub fn region(&self) -> &RegionId {
        &self.region
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegionRouteDelivery<M> {
    Sent {
        shard: ShardId,
        region: RegionId,
    },
    MissingTarget {
        shard: ShardId,
        region: RegionId,
        message: ShardingEnvelope<M>,
    },
    SendFailed {
        shard: ShardId,
        region: RegionId,
        message: ShardingEnvelope<M>,
        reason: String,
    },
}

#[derive(Clone, Default)]
pub struct RegionRouteTransport<M>
where
    M: Send + 'static,
{
    targets: BTreeMap<RegionId, RegionRouteTarget<M>>,
}

impl<M> RegionRouteTransport<M>
where
    M: Send + 'static,
{
    pub fn new() -> Self {
        Self {
            targets: BTreeMap::new(),
        }
    }

    pub fn set_targets(&mut self, targets: impl IntoIterator<Item = RegionRouteTarget<M>>) {
        self.targets = targets
            .into_iter()
            .map(|target| (target.region.clone(), target))
            .collect();
    }

    pub fn insert_target(&mut self, target: RegionRouteTarget<M>) {
        self.targets.insert(target.region.clone(), target);
    }

    pub fn remove_target(&mut self, region: &RegionId) {
        self.targets.remove(region);
    }

    pub fn target_count(&self) -> usize {
        self.targets.len()
    }

    pub fn send_route_to(
        &self,
        region: &RegionId,
        shard: ShardId,
        message: ShardingEnvelope<M>,
        route_reply_to: ActorRef<RegionLocalRoutePlan<M>>,
        delivery_reply_to: ActorRef<ShardDeliverPlan<M>>,
    ) -> RegionRouteDelivery<M> {
        let Some(target) = self.targets.get(region) else {
            return RegionRouteDelivery::MissingTarget {
                shard,
                region: region.clone(),
                message,
            };
        };

        let forwarded_shard = shard.clone();
        let forwarded = ShardRegionMsg::RouteToLocalShard {
            shard: forwarded_shard,
            message,
            route_reply_to,
            delivery_reply_to,
        };
        match target.recipient.tell(forwarded) {
            Ok(()) => RegionRouteDelivery::Sent {
                shard,
                region: target.region.clone(),
            },
            Err(error) => {
                let reason = error.reason().to_string();
                match error.into_message() {
                    ShardRegionMsg::RouteToLocalShard { shard, message, .. } => {
                        RegionRouteDelivery::SendFailed {
                            shard,
                            region: target.region.clone(),
                            message,
                            reason,
                        }
                    }
                    _ => unreachable!("region route transport only sends route messages"),
                }
            }
        }
    }
}
