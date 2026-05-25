use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use kairo_actor::Recipient;

use crate::{
    ReplicaId, ReplicatorGossip, ReplicatorGossipApplyReport, ReplicatorGossipStatus,
    ReplicatorGossipStatusPlan,
};

type GossipStatusRecipient = Arc<dyn Recipient<ReplicatorGossipStatus> + Send + Sync>;
type GossipRecipient = Arc<dyn Recipient<ReplicatorGossip> + Send + Sync>;

#[derive(Clone, Default)]
pub struct ReplicatorGossipTargetRegistry {
    targets: Arc<RwLock<BTreeMap<ReplicaId, ReplicatorGossipTargetRecipients>>>,
}

impl ReplicatorGossipTargetRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_targets(&self, targets: impl IntoIterator<Item = ReplicatorGossipTarget>) {
        let mut guard = self.targets.write().expect("gossip targets poisoned");
        *guard = targets
            .into_iter()
            .map(|target| (target.replica, target.recipients))
            .collect();
    }

    pub fn insert_target(&self, target: ReplicatorGossipTarget) {
        self.targets
            .write()
            .expect("gossip targets poisoned")
            .insert(target.replica, target.recipients);
    }

    pub fn remove_target(&self, replica: &ReplicaId) {
        self.targets
            .write()
            .expect("gossip targets poisoned")
            .remove(replica);
    }

    pub fn target_count(&self) -> usize {
        self.targets.read().expect("gossip targets poisoned").len()
    }

    fn get(&self, replica: &ReplicaId) -> Option<ReplicatorGossipTargetRecipients> {
        self.targets
            .read()
            .expect("gossip targets poisoned")
            .get(replica)
            .cloned()
    }
}

#[derive(Clone)]
pub struct ReplicatorGossipTarget {
    replica: ReplicaId,
    recipients: ReplicatorGossipTargetRecipients,
}

impl ReplicatorGossipTarget {
    pub fn new(
        replica: ReplicaId,
        status_recipient: impl Recipient<ReplicatorGossipStatus> + Send + Sync + 'static,
        gossip_recipient: impl Recipient<ReplicatorGossip> + Send + Sync + 'static,
    ) -> Self {
        Self {
            replica,
            recipients: ReplicatorGossipTargetRecipients {
                status: Arc::new(status_recipient),
                gossip: Arc::new(gossip_recipient),
            },
        }
    }

    pub fn from_arcs(
        replica: ReplicaId,
        status_recipient: GossipStatusRecipient,
        gossip_recipient: GossipRecipient,
    ) -> Self {
        Self {
            replica,
            recipients: ReplicatorGossipTargetRecipients {
                status: status_recipient,
                gossip: gossip_recipient,
            },
        }
    }

    pub fn replica(&self) -> &ReplicaId {
        &self.replica
    }
}

#[derive(Clone)]
struct ReplicatorGossipTargetRecipients {
    status: GossipStatusRecipient,
    gossip: GossipRecipient,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorGossipTransportReport {
    sent_status_to: Vec<ReplicaId>,
    sent_gossip_to: Vec<ReplicaId>,
    failures: Vec<ReplicatorGossipTransportFailure>,
}

impl ReplicatorGossipTransportReport {
    pub fn empty() -> Self {
        Self {
            sent_status_to: Vec::new(),
            sent_gossip_to: Vec::new(),
            failures: Vec::new(),
        }
    }

    pub fn sent_status_to(&self) -> &[ReplicaId] {
        &self.sent_status_to
    }

    pub fn sent_gossip_to(&self) -> &[ReplicaId] {
        &self.sent_gossip_to
    }

    pub fn failures(&self) -> &[ReplicatorGossipTransportFailure] {
        &self.failures
    }

    pub fn is_success(&self) -> bool {
        self.failures.is_empty()
    }

    fn sent_status(replica: ReplicaId) -> Self {
        Self {
            sent_status_to: vec![replica],
            sent_gossip_to: Vec::new(),
            failures: Vec::new(),
        }
    }

    fn sent_gossip(replica: ReplicaId) -> Self {
        Self {
            sent_status_to: Vec::new(),
            sent_gossip_to: vec![replica],
            failures: Vec::new(),
        }
    }

    fn failed(failure: ReplicatorGossipTransportFailure) -> Self {
        Self {
            sent_status_to: Vec::new(),
            sent_gossip_to: Vec::new(),
            failures: vec![failure],
        }
    }

    pub fn extend(&mut self, other: Self) {
        self.sent_status_to.extend(other.sent_status_to);
        self.sent_gossip_to.extend(other.sent_gossip_to);
        self.failures.extend(other.failures);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicatorGossipTransportFailure {
    MissingTarget { replica: ReplicaId },
    SendStatusFailed { replica: ReplicaId, reason: String },
    SendGossipFailed { replica: ReplicaId, reason: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicatorGossipTickSkipReason {
    NotConfigured,
    NoReachableTargets,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorGossipTickReport {
    target: Option<ReplicaId>,
    status: Option<ReplicatorGossipStatus>,
    transport: ReplicatorGossipTransportReport,
    skipped: Option<ReplicatorGossipTickSkipReason>,
}

impl ReplicatorGossipTickReport {
    pub fn sent(
        target: ReplicaId,
        status: ReplicatorGossipStatus,
        transport: ReplicatorGossipTransportReport,
    ) -> Self {
        Self {
            target: Some(target),
            status: Some(status),
            transport,
            skipped: None,
        }
    }

    pub fn skipped(reason: ReplicatorGossipTickSkipReason) -> Self {
        Self {
            target: None,
            status: None,
            transport: ReplicatorGossipTransportReport::empty(),
            skipped: Some(reason),
        }
    }

    pub fn target(&self) -> Option<&ReplicaId> {
        self.target.as_ref()
    }

    pub fn status(&self) -> Option<&ReplicatorGossipStatus> {
        self.status.as_ref()
    }

    pub fn transport(&self) -> &ReplicatorGossipTransportReport {
        &self.transport
    }

    pub fn skipped_reason(&self) -> Option<ReplicatorGossipTickSkipReason> {
        self.skipped
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorGossipStatusReceiveReport {
    plan: ReplicatorGossipStatusPlan,
    transport: ReplicatorGossipTransportReport,
}

impl ReplicatorGossipStatusReceiveReport {
    pub fn new(
        plan: ReplicatorGossipStatusPlan,
        transport: ReplicatorGossipTransportReport,
    ) -> Self {
        Self { plan, transport }
    }

    pub fn plan(&self) -> &ReplicatorGossipStatusPlan {
        &self.plan
    }

    pub fn transport(&self) -> &ReplicatorGossipTransportReport {
        &self.transport
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorGossipReceiveReport {
    apply: ReplicatorGossipApplyReport,
    transport: ReplicatorGossipTransportReport,
}

impl ReplicatorGossipReceiveReport {
    pub fn new(
        apply: ReplicatorGossipApplyReport,
        transport: ReplicatorGossipTransportReport,
    ) -> Self {
        Self { apply, transport }
    }

    pub fn apply(&self) -> &ReplicatorGossipApplyReport {
        &self.apply
    }

    pub fn transport(&self) -> &ReplicatorGossipTransportReport {
        &self.transport
    }
}

#[derive(Clone, Default)]
pub struct ReplicatorGossipTransport {
    targets: ReplicatorGossipTargetRegistry,
}

impl ReplicatorGossipTransport {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_target_registry(targets: ReplicatorGossipTargetRegistry) -> Self {
        Self { targets }
    }

    pub fn set_targets(&self, targets: impl IntoIterator<Item = ReplicatorGossipTarget>) {
        self.targets.set_targets(targets);
    }

    pub fn insert_target(&self, target: ReplicatorGossipTarget) {
        self.targets.insert_target(target);
    }

    pub fn remove_target(&self, replica: &ReplicaId) {
        self.targets.remove_target(replica);
    }

    pub fn target_count(&self) -> usize {
        self.targets.target_count()
    }

    pub fn target_registry(&self) -> ReplicatorGossipTargetRegistry {
        self.targets.clone()
    }

    pub fn send_status(
        &self,
        replica: ReplicaId,
        status: ReplicatorGossipStatus,
    ) -> ReplicatorGossipTransportReport {
        let Some(target) = self.targets.get(&replica) else {
            return ReplicatorGossipTransportReport::failed(
                ReplicatorGossipTransportFailure::MissingTarget { replica },
            );
        };
        if let Err(error) = target.status.tell(status) {
            return ReplicatorGossipTransportReport::failed(
                ReplicatorGossipTransportFailure::SendStatusFailed {
                    replica,
                    reason: error.reason().to_string(),
                },
            );
        }
        ReplicatorGossipTransportReport::sent_status(replica)
    }

    pub fn send_gossip(
        &self,
        replica: ReplicaId,
        gossip: ReplicatorGossip,
    ) -> ReplicatorGossipTransportReport {
        let Some(target) = self.targets.get(&replica) else {
            return ReplicatorGossipTransportReport::failed(
                ReplicatorGossipTransportFailure::MissingTarget { replica },
            );
        };
        if let Err(error) = target.gossip.tell(gossip) {
            return ReplicatorGossipTransportReport::failed(
                ReplicatorGossipTransportFailure::SendGossipFailed {
                    replica,
                    reason: error.reason().to_string(),
                },
            );
        }
        ReplicatorGossipTransportReport::sent_gossip(replica)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::time::Duration;

    use super::*;

    #[derive(Clone)]
    struct ChannelRecipient<M> {
        tx: mpsc::Sender<M>,
    }

    impl<M> Recipient<M> for ChannelRecipient<M>
    where
        M: Send + 'static,
    {
        fn tell(&self, message: M) -> Result<(), kairo_actor::SendError<M>> {
            self.tx
                .send(message)
                .map_err(|error| kairo_actor::SendError::new(error.0, "channel closed"))
        }
    }

    fn status() -> ReplicatorGossipStatus {
        ReplicatorGossipStatus {
            entries: Vec::new(),
            chunk: 0,
            total_chunks: 1,
            to_system_uid: None,
            from_system_uid: None,
        }
    }

    fn gossip() -> ReplicatorGossip {
        ReplicatorGossip {
            entries: Vec::new(),
            send_back: false,
            to_system_uid: None,
            from_system_uid: None,
        }
    }

    #[test]
    fn gossip_transport_sends_status_and_gossip_to_registered_target() {
        let transport = ReplicatorGossipTransport::new();
        let (status_tx, status_rx) = mpsc::channel();
        let (gossip_tx, gossip_rx) = mpsc::channel();
        transport.insert_target(ReplicatorGossipTarget::new(
            ReplicaId::new("peer"),
            ChannelRecipient { tx: status_tx },
            ChannelRecipient { tx: gossip_tx },
        ));

        let status_report = transport.send_status(ReplicaId::new("peer"), status());
        let gossip_report = transport.send_gossip(ReplicaId::new("peer"), gossip());

        assert_eq!(status_report.sent_status_to(), &[ReplicaId::new("peer")]);
        assert_eq!(gossip_report.sent_gossip_to(), &[ReplicaId::new("peer")]);
        status_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        gossip_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn gossip_transport_reports_missing_target() {
        let transport = ReplicatorGossipTransport::new();

        let report = transport.send_status(ReplicaId::new("missing"), status());

        assert!(matches!(
            report.failures(),
            [ReplicatorGossipTransportFailure::MissingTarget { .. }]
        ));
    }
}
