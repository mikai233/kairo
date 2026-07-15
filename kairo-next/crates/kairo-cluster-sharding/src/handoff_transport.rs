#![deny(missing_docs)]
//! In-process delivery routes used by shard allocation and handoff orchestration.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use kairo_actor::{ActorRef, Recipient};

use crate::{
    BeginHandOffPlan, HandOffPlan, HostShardPlan, RegionId, RegionLocalHandOffCompletionPlan,
    RegionLocalHandOffPlan, ShardHandOffPlan, ShardRebalancePlan, ShardRegionMsg,
};

type RegionRecipient<M> = Arc<dyn Recipient<ShardRegionMsg<M>> + Send + Sync>;

/// A region identity paired with a recipient for region control messages.
///
/// Targets created from a concrete [`ActorRef`] also expose that ref for death
/// watch. Generic recipients, including remote-envelope adapters, are routable
/// but cannot be watched by the local actor runtime.
#[derive(Clone)]
pub struct HandoffRegionTarget<M>
where
    M: Send + 'static,
{
    region: RegionId,
    recipient: RegionRecipient<M>,
    watch_ref: Option<ActorRef<ShardRegionMsg<M>>>,
}

impl<M> HandoffRegionTarget<M>
where
    M: Send + 'static,
{
    /// Creates an unwatchable target from any thread-safe region recipient.
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

    /// Creates an unwatchable target from a shared region recipient.
    pub fn from_arc(region: impl Into<RegionId>, recipient: RegionRecipient<M>) -> Self {
        Self {
            region: region.into(),
            recipient,
            watch_ref: None,
        }
    }

    /// Creates a target backed by a watchable local actor reference.
    pub fn from_actor_ref(region: impl Into<RegionId>, actor: ActorRef<ShardRegionMsg<M>>) -> Self {
        Self {
            region: region.into(),
            recipient: Arc::new(actor.clone()),
            watch_ref: Some(actor),
        }
    }

    /// Returns the coordinator identity used to select this target.
    pub fn region(&self) -> &RegionId {
        &self.region
    }

    /// Returns the local actor ref when this target supports death watch.
    pub fn watch_ref(&self) -> Option<&ActorRef<ShardRegionMsg<M>>> {
        self.watch_ref.as_ref()
    }
}

/// Describes which control sends were accepted and which failed immediately.
///
/// A successful send means the recipient accepted the message; it does not
/// mean the region processed the command or completed the handoff.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandoffDeliveryReport {
    sent_to: Vec<HandoffDeliveryTarget>,
    failures: Vec<HandoffDeliveryFailure>,
}

impl HandoffDeliveryReport {
    /// Returns the targets whose recipients accepted their control messages.
    pub fn sent_to(&self) -> &[HandoffDeliveryTarget] {
        &self.sent_to
    }

    /// Returns missing routes and immediate recipient rejections.
    pub fn failures(&self) -> &[HandoffDeliveryFailure] {
        &self.failures
    }

    /// Returns `true` when every attempted delivery was accepted.
    pub fn is_success(&self) -> bool {
        self.failures.is_empty()
    }
}

/// Identifies one region control delivery attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandoffDeliveryTarget {
    /// Invalidates the shard-home view before owner shutdown.
    BeginHandOff {
        /// Region receiving the command.
        region: RegionId,
    },
    /// Starts or confirms a newly allocated local shard.
    HostShard {
        /// Region receiving the command.
        region: RegionId,
    },
    /// Asks the current owner to stop its local shard.
    HandOff {
        /// Region receiving the command.
        region: RegionId,
    },
    /// Asks a local owner to finish an entity-stopper-based handoff.
    CompleteHandOff {
        /// Region receiving the command.
        region: RegionId,
    },
}

/// An immediate failure to deliver a region control message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandoffDeliveryFailure {
    /// No route was registered for the selected region.
    MissingTarget {
        /// Delivery that could not be routed.
        target: HandoffDeliveryTarget,
    },
    /// The selected recipient rejected the message synchronously.
    SendFailed {
        /// Delivery rejected by the recipient.
        target: HandoffDeliveryTarget,
        /// Recipient-provided rejection reason.
        reason: String,
    },
}

/// Mutable region-route table used by coordinator and handoff workers.
///
/// Cloning the transport snapshots the route table while sharing each
/// recipient. A worker therefore keeps the routes that existed when it was
/// spawned; region termination is delivered separately to its state machine.
#[derive(Clone, Default)]
pub struct HandoffTransport<M>
where
    M: Send + 'static,
{
    targets: BTreeMap<RegionId, HandoffRegionTarget<M>>,
}

impl<M> HandoffTransport<M>
where
    M: Send + 'static,
{
    /// Creates a transport with no registered targets.
    pub fn new() -> Self {
        Self {
            targets: BTreeMap::new(),
        }
    }

    /// Replaces all registered targets, with the last duplicate region winning.
    pub fn set_targets(&mut self, targets: impl IntoIterator<Item = HandoffRegionTarget<M>>) {
        self.targets = targets
            .into_iter()
            .map(|target| (target.region.clone(), target))
            .collect();
    }

    /// Adds a target or replaces the existing target with the same region ID.
    pub fn insert_target(&mut self, target: HandoffRegionTarget<M>) {
        self.targets.insert(target.region.clone(), target);
    }

    /// Removes a target, returning silently when the region was absent.
    pub fn remove_target(&mut self, region: &RegionId) {
        self.targets.remove(region);
    }

    /// Returns the number of registered region routes.
    pub fn target_count(&self) -> usize {
        self.targets.len()
    }

    /// Sends begin-handoff to every participant in a rebalance plan.
    ///
    /// All participants are attempted even if an earlier delivery fails.
    pub fn send_begin_handoff(
        &self,
        plan: &ShardRebalancePlan,
        reply_to: ActorRef<BeginHandOffPlan>,
    ) -> HandoffDeliveryReport {
        let mut sent_to = Vec::new();
        let mut failures = Vec::new();

        for region in &plan.participants {
            let target = HandoffDeliveryTarget::BeginHandOff {
                region: region.clone(),
            };
            let Some(recipient) = self.targets.get(region) else {
                failures.push(HandoffDeliveryFailure::MissingTarget { target });
                continue;
            };

            let message = ShardRegionMsg::BeginHandOff {
                shard: plan.shard.clone(),
                reply_to: reply_to.clone(),
            };
            if let Err(error) = recipient.recipient.tell(message) {
                failures.push(HandoffDeliveryFailure::SendFailed {
                    target,
                    reason: error.reason().to_string(),
                });
            } else {
                sent_to.push(target);
            }
        }

        HandoffDeliveryReport { sent_to, failures }
    }

    /// Sends begin-handoff to one region and reports immediate delivery status.
    pub fn send_begin_handoff_to(
        &self,
        region: &RegionId,
        shard: &str,
        reply_to: ActorRef<BeginHandOffPlan>,
    ) -> HandoffDeliveryReport {
        let mut sent_to = Vec::new();
        let mut failures = Vec::new();
        let target = HandoffDeliveryTarget::BeginHandOff {
            region: region.clone(),
        };
        let Some(recipient) = self.targets.get(region) else {
            failures.push(HandoffDeliveryFailure::MissingTarget { target });
            return HandoffDeliveryReport { sent_to, failures };
        };

        let message = ShardRegionMsg::BeginHandOff {
            shard: shard.to_string(),
            reply_to,
        };
        if let Err(error) = recipient.recipient.tell(message) {
            failures.push(HandoffDeliveryFailure::SendFailed {
                target,
                reason: error.reason().to_string(),
            });
        } else {
            sent_to.push(target);
        }

        HandoffDeliveryReport { sent_to, failures }
    }

    /// Sends a basic handoff command to the plan's current owner.
    pub fn send_handoff(
        &self,
        plan: &ShardRebalancePlan,
        reply_to: ActorRef<HandOffPlan>,
    ) -> HandoffDeliveryReport {
        self.send_handoff_to(&plan.from_region, &plan.shard, reply_to)
    }

    /// Sends a host-shard command to one newly selected owner.
    pub fn send_host_shard_to(
        &self,
        region: &RegionId,
        shard: &str,
        reply_to: ActorRef<HostShardPlan<M>>,
    ) -> HandoffDeliveryReport {
        let mut sent_to = Vec::new();
        let mut failures = Vec::new();
        let target = HandoffDeliveryTarget::HostShard {
            region: region.clone(),
        };
        let Some(recipient) = self.targets.get(region) else {
            failures.push(HandoffDeliveryFailure::MissingTarget { target });
            return HandoffDeliveryReport { sent_to, failures };
        };

        let message = ShardRegionMsg::HostShard {
            shard: shard.to_string(),
            reply_to,
        };
        if let Err(error) = recipient.recipient.tell(message) {
            failures.push(HandoffDeliveryFailure::SendFailed {
                target,
                reason: error.reason().to_string(),
            });
        } else {
            sent_to.push(target);
        }

        HandoffDeliveryReport { sent_to, failures }
    }

    /// Sends a basic handoff command to one current owner.
    pub fn send_handoff_to(
        &self,
        region: &RegionId,
        shard: &str,
        reply_to: ActorRef<HandOffPlan>,
    ) -> HandoffDeliveryReport {
        let mut sent_to = Vec::new();
        let mut failures = Vec::new();
        let target = HandoffDeliveryTarget::HandOff {
            region: region.clone(),
        };
        let Some(recipient) = self.targets.get(region) else {
            failures.push(HandoffDeliveryFailure::MissingTarget { target });
            return HandoffDeliveryReport { sent_to, failures };
        };

        let message = ShardRegionMsg::HandOff {
            shard: shard.to_string(),
            reply_to,
        };
        if let Err(error) = recipient.recipient.tell(message) {
            failures.push(HandoffDeliveryFailure::SendFailed {
                target,
                reason: error.reason().to_string(),
            });
        } else {
            sent_to.push(target);
        }

        HandoffDeliveryReport { sent_to, failures }
    }

    /// Sends the local-shard handoff command, including its entity stop message.
    ///
    /// The two reply refs separately observe region forwarding and the local
    /// shard's decision to stop immediately or start an entity stopper.
    pub fn send_local_handoff_to(
        &self,
        region: &RegionId,
        shard: &str,
        stop_message: M,
        region_reply_to: ActorRef<RegionLocalHandOffPlan>,
        shard_reply_to: ActorRef<ShardHandOffPlan<M>>,
    ) -> HandoffDeliveryReport {
        let mut sent_to = Vec::new();
        let mut failures = Vec::new();
        let target = HandoffDeliveryTarget::HandOff {
            region: region.clone(),
        };
        let Some(recipient) = self.targets.get(region) else {
            failures.push(HandoffDeliveryFailure::MissingTarget { target });
            return HandoffDeliveryReport { sent_to, failures };
        };

        let message = ShardRegionMsg::HandOffToLocalShard {
            shard: shard.to_string(),
            stop_message,
            region_reply_to,
            shard_reply_to,
        };
        if let Err(error) = recipient.recipient.tell(message) {
            failures.push(HandoffDeliveryFailure::SendFailed {
                target,
                reason: error.reason().to_string(),
            });
        } else {
            sent_to.push(target);
        }

        HandoffDeliveryReport { sent_to, failures }
    }

    /// Asks a local region to complete a previously started entity stopper.
    pub fn send_complete_local_handoff_to(
        &self,
        region: &RegionId,
        shard: &str,
        timeout: Duration,
        reply_to: ActorRef<RegionLocalHandOffCompletionPlan>,
    ) -> HandoffDeliveryReport {
        let mut sent_to = Vec::new();
        let mut failures = Vec::new();
        let target = HandoffDeliveryTarget::CompleteHandOff {
            region: region.clone(),
        };
        let Some(recipient) = self.targets.get(region) else {
            failures.push(HandoffDeliveryFailure::MissingTarget { target });
            return HandoffDeliveryReport { sent_to, failures };
        };

        let message = ShardRegionMsg::CompleteLocalShardHandOff {
            shard: shard.to_string(),
            timeout,
            reply_to,
        };
        if let Err(error) = recipient.recipient.tell(message) {
            failures.push(HandoffDeliveryFailure::SendFailed {
                target,
                reason: error.reason().to_string(),
            });
        } else {
            sent_to.push(target);
        }

        HandoffDeliveryReport { sent_to, failures }
    }
}
