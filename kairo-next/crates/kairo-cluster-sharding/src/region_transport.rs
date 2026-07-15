#![deny(missing_docs)]

//! Typed delivery targets for routing an envelope to its owning shard region.
//!
//! A target may be a local region actor or a remote wire adapter implementing
//! [`Recipient`]. Local actor targets can additionally expose an [`ActorRef`]
//! for death watch. Failed lookup or delivery returns the original envelope so
//! the caller can preserve its buffering and retry semantics.

use std::collections::BTreeMap;
use std::sync::Arc;

use kairo_actor::{ActorRef, Recipient};

use crate::{
    RegionId, RegionLocalRoutePlan, ShardDeliverPlan, ShardId, ShardRegionMsg, ShardingEnvelope,
};

type RegionRecipient<M> = Arc<dyn Recipient<ShardRegionMsg<M>> + Send + Sync>;

#[derive(Clone)]
/// One addressable shard-region recipient.
pub struct RegionRouteTarget<M>
where
    M: Send + 'static,
{
    region: RegionId,
    recipient: RegionRecipient<M>,
    watch_ref: Option<ActorRef<ShardRegionMsg<M>>>,
}

impl<M> RegionRouteTarget<M>
where
    M: Send + 'static,
{
    /// Creates a target from any owned typed recipient.
    ///
    /// This form has no watchable actor reference. Use [`Self::from_actor_ref`]
    /// when region termination should participate in local death watch.
    pub fn new(
        region: impl Into<RegionId>,
        recipient: impl Recipient<ShardRegionMsg<M>> + Send + Sync + 'static,
    ) -> Self {
        Self {
            region: region.into(),
            recipient: Arc::new(recipient),
            watch_ref: None,
        }
    }

    /// Creates a target from an already shared typed recipient.
    ///
    /// This form has no watchable actor reference.
    pub fn from_arc(region: impl Into<RegionId>, recipient: RegionRecipient<M>) -> Self {
        Self {
            region: region.into(),
            recipient,
            watch_ref: None,
        }
    }

    /// Creates a target backed by a local, watchable region actor reference.
    pub fn from_actor_ref(region: impl Into<RegionId>, actor: ActorRef<ShardRegionMsg<M>>) -> Self {
        Self {
            region: region.into(),
            recipient: Arc::new(actor.clone()),
            watch_ref: Some(actor),
        }
    }

    /// Returns the stable region identifier used for routing-table lookup.
    pub fn region(&self) -> &RegionId {
        &self.region
    }

    /// Returns the local actor reference available for death watch, if any.
    pub fn watch_ref(&self) -> Option<&ActorRef<ShardRegionMsg<M>>> {
        self.watch_ref.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Outcome of attempting to forward one envelope to a shard region.
pub enum RegionRouteDelivery<M> {
    /// The target recipient accepted the local route command.
    Sent {
        /// Shard that will receive the envelope.
        shard: ShardId,
        /// Region that accepted the command.
        region: RegionId,
    },
    /// No target is currently registered for the requested region.
    MissingTarget {
        /// Shard that was not routed.
        shard: ShardId,
        /// Region absent from the routing table.
        region: RegionId,
        /// Original envelope returned for buffering or retry.
        message: ShardingEnvelope<M>,
    },
    /// The registered recipient rejected the route command.
    SendFailed {
        /// Shard that was not routed.
        shard: ShardId,
        /// Region whose recipient rejected the command.
        region: RegionId,
        /// Original envelope recovered from the failed send.
        message: ShardingEnvelope<M>,
        /// Human-readable actor-send failure reason.
        reason: String,
    },
}

#[derive(Clone, Default)]
/// Mutable routing table from region identifiers to typed delivery targets.
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
    /// Creates an empty routing table.
    pub fn new() -> Self {
        Self {
            targets: BTreeMap::new(),
        }
    }

    /// Replaces every target, with later duplicate region ids winning.
    pub fn set_targets(&mut self, targets: impl IntoIterator<Item = RegionRouteTarget<M>>) {
        self.targets = targets
            .into_iter()
            .map(|target| (target.region.clone(), target))
            .collect();
    }

    /// Inserts or replaces the target for its region id.
    pub fn insert_target(&mut self, target: RegionRouteTarget<M>) {
        self.targets.insert(target.region.clone(), target);
    }

    /// Removes the target for `region`, if present.
    pub fn remove_target(&mut self, region: &RegionId) {
        self.targets.remove(region);
    }

    /// Returns the number of registered region targets.
    pub fn target_count(&self) -> usize {
        self.targets.len()
    }

    /// Clones the watchable local actor reference for `region`, if available.
    pub fn watch_ref_for(&self, region: &RegionId) -> Option<ActorRef<ShardRegionMsg<M>>> {
        self.targets
            .get(region)
            .and_then(RegionRouteTarget::watch_ref)
            .cloned()
    }

    /// Forwards an envelope to the region that owns `shard`.
    ///
    /// The target receives a [`ShardRegionMsg::RouteToLocalShard`] carrying the
    /// route and delivery reply actors. Missing targets and rejected sends
    /// return the original envelope in [`RegionRouteDelivery`] so the caller
    /// does not lose application messages.
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
