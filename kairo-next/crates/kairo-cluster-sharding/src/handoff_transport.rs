use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use kairo_actor::{ActorRef, Recipient};

use crate::{
    BeginHandOffPlan, HandOffPlan, HostShardPlan, RegionId, RegionLocalHandOffCompletionPlan,
    RegionLocalHandOffPlan, ShardHandOffPlan, ShardRebalancePlan, ShardRegionMsg,
};

type RegionRecipient<M> = Arc<dyn Recipient<ShardRegionMsg<M>> + Send + Sync>;

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

    pub fn from_arc(region: impl Into<RegionId>, recipient: RegionRecipient<M>) -> Self {
        Self {
            region: region.into(),
            recipient,
            watch_ref: None,
        }
    }

    pub fn from_actor_ref(region: impl Into<RegionId>, actor: ActorRef<ShardRegionMsg<M>>) -> Self {
        Self {
            region: region.into(),
            recipient: Arc::new(actor.clone()),
            watch_ref: Some(actor),
        }
    }

    pub fn region(&self) -> &RegionId {
        &self.region
    }

    pub fn watch_ref(&self) -> Option<&ActorRef<ShardRegionMsg<M>>> {
        self.watch_ref.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandoffDeliveryReport {
    sent_to: Vec<HandoffDeliveryTarget>,
    failures: Vec<HandoffDeliveryFailure>,
}

impl HandoffDeliveryReport {
    pub fn sent_to(&self) -> &[HandoffDeliveryTarget] {
        &self.sent_to
    }

    pub fn failures(&self) -> &[HandoffDeliveryFailure] {
        &self.failures
    }

    pub fn is_success(&self) -> bool {
        self.failures.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandoffDeliveryTarget {
    BeginHandOff { region: RegionId },
    HostShard { region: RegionId },
    HandOff { region: RegionId },
    CompleteHandOff { region: RegionId },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandoffDeliveryFailure {
    MissingTarget {
        target: HandoffDeliveryTarget,
    },
    SendFailed {
        target: HandoffDeliveryTarget,
        reason: String,
    },
}

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
    pub fn new() -> Self {
        Self {
            targets: BTreeMap::new(),
        }
    }

    pub fn set_targets(&mut self, targets: impl IntoIterator<Item = HandoffRegionTarget<M>>) {
        self.targets = targets
            .into_iter()
            .map(|target| (target.region.clone(), target))
            .collect();
    }

    pub fn insert_target(&mut self, target: HandoffRegionTarget<M>) {
        self.targets.insert(target.region.clone(), target);
    }

    pub fn remove_target(&mut self, region: &RegionId) {
        self.targets.remove(region);
    }

    pub fn target_count(&self) -> usize {
        self.targets.len()
    }

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

    pub fn send_handoff(
        &self,
        plan: &ShardRebalancePlan,
        reply_to: ActorRef<HandOffPlan>,
    ) -> HandoffDeliveryReport {
        self.send_handoff_to(&plan.from_region, &plan.shard, reply_to)
    }

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
