use std::collections::BTreeSet;

use crate::{ReplicatorGossip, ReplicatorGossipStatus, ReplicatorKey};

#[derive(Debug, Clone, PartialEq, Eq)]
/// The two independent responses that may result from a gossip status.
///
/// `gossip` supplies values the peer lacks or holds differently;
/// `missing_status` asks the peer for values absent locally.
pub struct ReplicatorGossipStatusPlan {
    gossip: Option<ReplicatorGossip>,
    missing_status: Option<ReplicatorGossipStatus>,
}

impl ReplicatorGossipStatusPlan {
    /// Creates a status-response plan from optional full-state and missing-key messages.
    pub fn new(
        gossip: Option<ReplicatorGossip>,
        missing_status: Option<ReplicatorGossipStatus>,
    ) -> Self {
        Self {
            gossip,
            missing_status,
        }
    }

    /// Returns the planned full-state gossip response, if any.
    pub fn gossip(&self) -> Option<&ReplicatorGossip> {
        self.gossip.as_ref()
    }

    /// Returns the planned missing-key status response, if any.
    pub fn missing_status(&self) -> Option<&ReplicatorGossipStatus> {
        self.missing_status.as_ref()
    }

    /// Consumes the plan into its independent response messages.
    pub fn into_parts(self) -> (Option<ReplicatorGossip>, Option<ReplicatorGossipStatus>) {
        (self.gossip, self.missing_status)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// The observable result of applying one full-state gossip message.
pub struct ReplicatorGossipApplyReport {
    changed_keys: BTreeSet<ReplicatorKey>,
    reply: Option<ReplicatorGossip>,
}

impl ReplicatorGossipApplyReport {
    /// Creates an apply report from changed keys and an optional reply.
    pub fn new(changed_keys: BTreeSet<ReplicatorKey>, reply: Option<ReplicatorGossip>) -> Self {
        Self {
            changed_keys,
            reply,
        }
    }

    /// Returns the deterministically ordered keys changed by the merge.
    pub fn changed_keys(&self) -> &BTreeSet<ReplicatorKey> {
        &self.changed_keys
    }

    /// Returns the full-state send-back response, if one was requested and needed.
    pub fn reply(&self) -> Option<&ReplicatorGossip> {
        self.reply.as_ref()
    }
}
