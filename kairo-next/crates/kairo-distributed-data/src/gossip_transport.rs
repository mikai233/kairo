#![deny(missing_docs)]
//! Typed delivery targets and diagnostics for full-state gossip traffic.
//!
//! The transport deliberately owns only delivery state. Cluster membership
//! decides which replicas are eligible, while this module maps a logical
//! [`ReplicaId`] to separate recipients for digest status and full-state
//! gossip messages. Cloned registries and transports share subsequent route
//! replacement and removal.

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
/// A shared mapping from logical replicas to their typed gossip recipients.
///
/// Replacing the complete target set is atomic with respect to readers. A
/// cloned registry observes all later inserts, replacements, and removals.
pub struct ReplicatorGossipTargetRegistry {
    targets: Arc<RwLock<BTreeMap<ReplicaId, ReplicatorGossipTargetRecipients>>>,
}

impl ReplicatorGossipTargetRegistry {
    /// Creates an empty target registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Atomically replaces every registered target.
    ///
    /// When the iterator contains the same replica more than once, the last
    /// target for that replica wins.
    pub fn set_targets(&self, targets: impl IntoIterator<Item = ReplicatorGossipTarget>) {
        let mut guard = self.targets.write().expect("gossip targets poisoned");
        *guard = targets
            .into_iter()
            .map(|target| (target.replica, target.recipients))
            .collect();
    }

    /// Inserts or replaces the target for one logical replica.
    pub fn insert_target(&self, target: ReplicatorGossipTarget) {
        self.targets
            .write()
            .expect("gossip targets poisoned")
            .insert(target.replica, target.recipients);
    }

    /// Removes the target for `replica`, if one is registered.
    pub fn remove_target(&self, replica: &ReplicaId) {
        self.targets
            .write()
            .expect("gossip targets poisoned")
            .remove(replica);
    }

    /// Returns the number of logical replicas with registered targets.
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
/// The status and full-state gossip recipients for one logical replica.
pub struct ReplicatorGossipTarget {
    replica: ReplicaId,
    recipients: ReplicatorGossipTargetRecipients,
}

impl ReplicatorGossipTarget {
    /// Creates a target from concrete typed recipients.
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

    /// Creates a target from shared type-erased typed recipients.
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

    /// Returns the logical replica served by this target.
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
/// The accumulated outcome of one or more gossip transport sends.
///
/// [`Self::extend`] preserves send-attempt order and does not deduplicate
/// replica identifiers, allowing a status response and a gossip response to
/// the same replica to remain independently observable.
pub struct ReplicatorGossipTransportReport {
    sent_status_to: Vec<ReplicaId>,
    sent_gossip_to: Vec<ReplicaId>,
    failures: Vec<ReplicatorGossipTransportFailure>,
}

impl ReplicatorGossipTransportReport {
    /// Creates a report with no sends and no failures.
    pub fn empty() -> Self {
        Self {
            sent_status_to: Vec::new(),
            sent_gossip_to: Vec::new(),
            failures: Vec::new(),
        }
    }

    /// Returns the replicas that accepted a digest status message.
    pub fn sent_status_to(&self) -> &[ReplicaId] {
        &self.sent_status_to
    }

    /// Returns the replicas that accepted a full-state gossip message.
    pub fn sent_gossip_to(&self) -> &[ReplicaId] {
        &self.sent_gossip_to
    }

    /// Returns delivery failures in attempt order.
    pub fn failures(&self) -> &[ReplicatorGossipTransportFailure] {
        &self.failures
    }

    /// Returns `true` when every attempted send was accepted.
    ///
    /// An empty report is successful because it contains no failed attempt.
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

    /// Appends all successes and failures from `other` in their existing order.
    pub fn extend(&mut self, other: Self) {
        self.sent_status_to.extend(other.sent_status_to);
        self.sent_gossip_to.extend(other.sent_gossip_to);
        self.failures.extend(other.failures);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// A failed gossip transport send.
pub enum ReplicatorGossipTransportFailure {
    /// No delivery target was registered for the logical replica.
    MissingTarget {
        /// The unresolved logical replica.
        replica: ReplicaId,
    },
    /// The status recipient rejected the message.
    SendStatusFailed {
        /// The logical replica whose recipient rejected the message.
        replica: ReplicaId,
        /// The recipient-provided failure reason.
        reason: String,
    },
    /// The full-state gossip recipient rejected the message.
    SendGossipFailed {
        /// The logical replica whose recipient rejected the message.
        replica: ReplicaId,
        /// The recipient-provided failure reason.
        reason: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Why a scheduled full-state gossip tick did not select a target.
pub enum ReplicatorGossipTickSkipReason {
    /// The replicator has no gossip transport or CRDT codec.
    NotConfigured,
    /// The replicator knows no currently reachable remote target.
    NoReachableTargets,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// The planning and delivery result of one scheduled gossip tick.
///
/// A selected target and status describe the attempted tick; callers must
/// inspect [`Self::transport`] to determine whether delivery succeeded.
pub struct ReplicatorGossipTickReport {
    target: Option<ReplicaId>,
    status: Option<ReplicatorGossipStatus>,
    transport: ReplicatorGossipTransportReport,
    skipped: Option<ReplicatorGossipTickSkipReason>,
}

impl ReplicatorGossipTickReport {
    /// Creates a report for a tick that selected a target and attempted delivery.
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

    /// Creates a report for a tick skipped before status construction.
    pub fn skipped(reason: ReplicatorGossipTickSkipReason) -> Self {
        Self {
            target: None,
            status: None,
            transport: ReplicatorGossipTransportReport::empty(),
            skipped: Some(reason),
        }
    }

    /// Returns the selected target, or `None` for a skipped tick.
    pub fn target(&self) -> Option<&ReplicaId> {
        self.target.as_ref()
    }

    /// Returns the constructed status, or `None` for a skipped tick.
    pub fn status(&self) -> Option<&ReplicatorGossipStatus> {
        self.status.as_ref()
    }

    /// Returns the transport outcome.
    pub fn transport(&self) -> &ReplicatorGossipTransportReport {
        &self.transport
    }

    /// Returns why the tick was skipped, if it did not select a target.
    pub fn skipped_reason(&self) -> Option<ReplicatorGossipTickSkipReason> {
        self.skipped
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// The pure response plan and delivery outcome for an inbound gossip status.
pub struct ReplicatorGossipStatusReceiveReport {
    plan: ReplicatorGossipStatusPlan,
    transport: ReplicatorGossipTransportReport,
}

impl ReplicatorGossipStatusReceiveReport {
    /// Combines a status response plan with the resulting transport sends.
    pub fn new(
        plan: ReplicatorGossipStatusPlan,
        transport: ReplicatorGossipTransportReport,
    ) -> Self {
        Self { plan, transport }
    }

    /// Returns the response plan produced from the peer's digests.
    pub fn plan(&self) -> &ReplicatorGossipStatusPlan {
        &self.plan
    }

    /// Returns the outcome of delivering planned status or gossip responses.
    pub fn transport(&self) -> &ReplicatorGossipTransportReport {
        &self.transport
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// The merge result and reply-delivery outcome for inbound full-state gossip.
pub struct ReplicatorGossipReceiveReport {
    apply: ReplicatorGossipApplyReport,
    transport: ReplicatorGossipTransportReport,
}

impl ReplicatorGossipReceiveReport {
    /// Combines a gossip apply report with the resulting reply send.
    pub fn new(
        apply: ReplicatorGossipApplyReport,
        transport: ReplicatorGossipTransportReport,
    ) -> Self {
        Self { apply, transport }
    }

    /// Returns the local merge and optional reply result.
    pub fn apply(&self) -> &ReplicatorGossipApplyReport {
        &self.apply
    }

    /// Returns the outcome of delivering the optional gossip reply.
    pub fn transport(&self) -> &ReplicatorGossipTransportReport {
        &self.transport
    }
}

#[derive(Clone, Default)]
/// Routes typed gossip protocol messages to registered logical replicas.
///
/// The transport performs one synchronous [`Recipient::tell`] attempt per
/// call. It reports missing routes and recipient rejection without retrying;
/// scheduling and retry policy belong to the replicator actor.
pub struct ReplicatorGossipTransport {
    targets: ReplicatorGossipTargetRegistry,
}

impl ReplicatorGossipTransport {
    /// Creates a transport with an empty private target registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a transport backed by the supplied shared target registry.
    pub fn with_target_registry(targets: ReplicatorGossipTargetRegistry) -> Self {
        Self { targets }
    }

    /// Atomically replaces every registered target.
    pub fn set_targets(&self, targets: impl IntoIterator<Item = ReplicatorGossipTarget>) {
        self.targets.set_targets(targets);
    }

    /// Inserts or replaces one target.
    pub fn insert_target(&self, target: ReplicatorGossipTarget) {
        self.targets.insert_target(target);
    }

    /// Removes one target, if present.
    pub fn remove_target(&self, replica: &ReplicaId) {
        self.targets.remove_target(replica);
    }

    /// Returns the number of logical replicas with registered targets.
    pub fn target_count(&self) -> usize {
        self.targets.target_count()
    }

    /// Returns a clone of the shared target registry.
    pub fn target_registry(&self) -> ReplicatorGossipTargetRegistry {
        self.targets.clone()
    }

    /// Attempts to deliver one digest status message to `replica`.
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

    /// Attempts to deliver one full-state gossip message to `replica`.
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

        let status_report = transport.send_status(ReplicaId::new("missing"), status());
        let gossip_report = transport.send_gossip(ReplicaId::new("missing"), gossip());

        assert!(matches!(
            status_report.failures(),
            [ReplicatorGossipTransportFailure::MissingTarget { .. }]
        ));
        assert!(matches!(
            gossip_report.failures(),
            [ReplicatorGossipTransportFailure::MissingTarget { .. }]
        ));
    }

    #[test]
    fn cloned_registry_observes_target_replacement_and_removal() {
        let registry = ReplicatorGossipTargetRegistry::new();
        let transport = ReplicatorGossipTransport::with_target_registry(registry.clone());
        let (first_status_tx, first_status_rx) = mpsc::channel();
        let (first_gossip_tx, _first_gossip_rx) = mpsc::channel();
        registry.insert_target(ReplicatorGossipTarget::new(
            ReplicaId::new("peer"),
            ChannelRecipient {
                tx: first_status_tx,
            },
            ChannelRecipient {
                tx: first_gossip_tx,
            },
        ));

        assert!(
            transport
                .send_status(ReplicaId::new("peer"), status())
                .is_success()
        );
        first_status_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap();

        let (second_status_tx, second_status_rx) = mpsc::channel();
        let (second_gossip_tx, second_gossip_rx) = mpsc::channel();
        transport.insert_target(ReplicatorGossipTarget::new(
            ReplicaId::new("peer"),
            ChannelRecipient {
                tx: second_status_tx,
            },
            ChannelRecipient {
                tx: second_gossip_tx,
            },
        ));

        assert!(
            transport
                .send_status(ReplicaId::new("peer"), status())
                .is_success()
        );
        assert!(
            transport
                .send_gossip(ReplicaId::new("peer"), gossip())
                .is_success()
        );
        second_status_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
        second_gossip_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
        assert!(first_status_rx.try_recv().is_err());

        registry.remove_target(&ReplicaId::new("peer"));

        assert_eq!(transport.target_count(), 0);
        assert!(matches!(
            transport
                .send_status(ReplicaId::new("peer"), status())
                .failures(),
            [ReplicatorGossipTransportFailure::MissingTarget { .. }]
        ));
    }

    #[test]
    fn gossip_transport_reports_recipient_failures_per_message_kind() {
        let transport = ReplicatorGossipTransport::new();
        let (status_tx, status_rx) = mpsc::channel();
        let (gossip_tx, gossip_rx) = mpsc::channel();
        drop(status_rx);
        drop(gossip_rx);
        transport.insert_target(ReplicatorGossipTarget::new(
            ReplicaId::new("peer"),
            ChannelRecipient { tx: status_tx },
            ChannelRecipient { tx: gossip_tx },
        ));

        let status_report = transport.send_status(ReplicaId::new("peer"), status());
        let gossip_report = transport.send_gossip(ReplicaId::new("peer"), gossip());

        assert_eq!(
            status_report.failures(),
            &[ReplicatorGossipTransportFailure::SendStatusFailed {
                replica: ReplicaId::new("peer"),
                reason: "channel closed".to_string(),
            }]
        );
        assert_eq!(
            gossip_report.failures(),
            &[ReplicatorGossipTransportFailure::SendGossipFailed {
                replica: ReplicaId::new("peer"),
                reason: "channel closed".to_string(),
            }]
        );
        assert!(!status_report.is_success());
        assert!(!gossip_report.is_success());
    }
}
