use std::collections::BTreeSet;

use crate::{ReplicatorGossip, ReplicatorGossipStatus, ReplicatorKey};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorGossipStatusPlan {
    gossip: Option<ReplicatorGossip>,
    missing_status: Option<ReplicatorGossipStatus>,
}

impl ReplicatorGossipStatusPlan {
    pub fn new(
        gossip: Option<ReplicatorGossip>,
        missing_status: Option<ReplicatorGossipStatus>,
    ) -> Self {
        Self {
            gossip,
            missing_status,
        }
    }

    pub fn gossip(&self) -> Option<&ReplicatorGossip> {
        self.gossip.as_ref()
    }

    pub fn missing_status(&self) -> Option<&ReplicatorGossipStatus> {
        self.missing_status.as_ref()
    }

    pub fn into_parts(self) -> (Option<ReplicatorGossip>, Option<ReplicatorGossipStatus>) {
        (self.gossip, self.missing_status)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorGossipApplyReport {
    changed_keys: BTreeSet<ReplicatorKey>,
    reply: Option<ReplicatorGossip>,
}

impl ReplicatorGossipApplyReport {
    pub fn new(changed_keys: BTreeSet<ReplicatorKey>, reply: Option<ReplicatorGossip>) -> Self {
        Self {
            changed_keys,
            reply,
        }
    }

    pub fn changed_keys(&self) -> &BTreeSet<ReplicatorKey> {
        &self.changed_keys
    }

    pub fn reply(&self) -> Option<&ReplicatorGossip> {
        self.reply.as_ref()
    }
}
