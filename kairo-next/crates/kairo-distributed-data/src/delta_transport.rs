#![deny(missing_docs)]
//! Typed target routing and diagnostics for periodic delta propagation.
//!
//! The transport mirrors Pekko's best-effort propagation boundary while using
//! explicit Rust types: a selected [`DeltaPropagation`] is encoded for one
//! logical [`ReplicaId`] and delivered to that replica's typed recipient.
//! Delivery failures are reported independently and do not stop later targets;
//! full-state gossip provides eventual repair outside this module.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use kairo_actor::Recipient;

use crate::{
    CrdtDataCodec, DeltaPropagation, ReplicaId, ReplicatedData, ReplicatorDeltaPropagation,
};

type DeltaRecipient = Arc<dyn Recipient<ReplicatorDeltaPropagation> + Send + Sync>;

#[derive(Clone, Default)]
/// A shared mapping from logical replicas to delta recipients.
///
/// Clones observe all later complete replacements, individual replacements,
/// and removals. Cluster-owned route projection can therefore update active
/// propagation transports without reconstructing them.
pub struct DeltaPropagationTargetRegistry {
    targets: Arc<RwLock<BTreeMap<ReplicaId, DeltaRecipient>>>,
}

impl DeltaPropagationTargetRegistry {
    /// Creates an empty target registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Atomically replaces every registered target.
    ///
    /// If the iterator contains a replica more than once, the last target for
    /// that replica wins.
    pub fn set_targets(&self, targets: impl IntoIterator<Item = DeltaPropagationTarget>) {
        let mut guard = self.targets.write().expect("delta targets poisoned");
        *guard = targets
            .into_iter()
            .map(|target| (target.replica, target.recipient))
            .collect();
    }

    /// Inserts or replaces the target for one logical replica.
    pub fn insert_target(&self, target: DeltaPropagationTarget) {
        self.targets
            .write()
            .expect("delta targets poisoned")
            .insert(target.replica, target.recipient);
    }

    /// Removes the target for `replica`, if one is registered.
    pub fn remove_target(&self, replica: &ReplicaId) {
        self.targets
            .write()
            .expect("delta targets poisoned")
            .remove(replica);
    }

    /// Returns the number of logical replicas with registered targets.
    pub fn target_count(&self) -> usize {
        self.targets.read().expect("delta targets poisoned").len()
    }

    fn get(&self, replica: &ReplicaId) -> Option<DeltaRecipient> {
        self.targets
            .read()
            .expect("delta targets poisoned")
            .get(replica)
            .cloned()
    }
}

#[derive(Clone)]
/// The typed delta recipient for one logical replica.
pub struct DeltaPropagationTarget {
    replica: ReplicaId,
    recipient: DeltaRecipient,
}

impl DeltaPropagationTarget {
    /// Creates a target from a concrete typed recipient.
    pub fn new(
        replica: ReplicaId,
        recipient: impl Recipient<ReplicatorDeltaPropagation> + Send + Sync + 'static,
    ) -> Self {
        Self {
            replica,
            recipient: Arc::new(recipient),
        }
    }

    /// Creates a target from an existing shared type-erased typed recipient.
    pub fn from_arc(replica: ReplicaId, recipient: DeltaRecipient) -> Self {
        Self { replica, recipient }
    }

    /// Returns the logical replica served by this target.
    pub fn replica(&self) -> &ReplicaId {
        &self.replica
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// The outcome of publishing one deterministic per-replica delta selection.
///
/// Each collection follows the lexical replica order of the input
/// [`BTreeMap`]. Skipped empty batches are not failures, while every missing
/// route, encoding error, or recipient rejection is retained independently.
pub struct DeltaTransportReport {
    sent_to: Vec<ReplicaId>,
    skipped_empty: Vec<ReplicaId>,
    failures: Vec<DeltaTransportFailure>,
}

impl DeltaTransportReport {
    /// Creates a report with no sends, skips, or failures.
    pub fn empty() -> Self {
        Self {
            sent_to: Vec::new(),
            skipped_empty: Vec::new(),
            failures: Vec::new(),
        }
    }

    /// Returns replicas whose recipients accepted a propagation message.
    pub fn sent_to(&self) -> &[ReplicaId] {
        &self.sent_to
    }

    /// Returns replicas whose selected propagation contained no delta entries.
    pub fn skipped_empty(&self) -> &[ReplicaId] {
        &self.skipped_empty
    }

    /// Returns publication failures in deterministic target order.
    pub fn failures(&self) -> &[DeltaTransportFailure] {
        &self.failures
    }

    /// Returns `true` when every non-empty selected batch was delivered.
    ///
    /// Empty and all-skipped reports are successful because no send failed.
    pub fn is_success(&self) -> bool {
        self.failures.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// A failed delta publication for one logical replica.
pub enum DeltaTransportFailure {
    /// No delivery target was registered for the selected replica.
    MissingTarget {
        /// The unresolved logical replica.
        replica: ReplicaId,
    },
    /// The selected CRDT delta could not be encoded.
    EncodeFailed {
        /// The logical replica whose selected batch failed encoding.
        replica: ReplicaId,
        /// The codec-provided failure reason.
        reason: String,
    },
    /// The registered recipient rejected the encoded propagation message.
    SendFailed {
        /// The logical replica whose recipient rejected the message.
        replica: ReplicaId,
        /// The recipient-provided failure reason.
        reason: String,
    },
}

#[derive(Clone)]
/// Encodes and routes periodic delta selections to typed replica targets.
///
/// The source replica, reply-request flag, and codec are transport
/// configuration. Cloned transports share their target registry but retain
/// the same immutable configuration. Publication attempts each target once,
/// without retry; scheduling, retained-delta cleanup, and full-state fallback
/// belong to the propagation loop and replicator.
pub struct DeltaPropagationTransport<Codec> {
    from: ReplicaId,
    reply: bool,
    codec: Codec,
    targets: DeltaPropagationTargetRegistry,
}

impl<Codec> DeltaPropagationTransport<Codec> {
    /// Creates a one-way transport with an empty target registry.
    ///
    /// Periodic propagation defaults to not requesting ACK/NACK replies.
    pub fn new(from: ReplicaId, codec: Codec) -> Self {
        Self {
            from,
            reply: false,
            codec,
            targets: DeltaPropagationTargetRegistry::new(),
        }
    }

    /// Creates a one-way transport backed by a supplied shared target registry.
    pub fn with_target_registry(
        from: ReplicaId,
        codec: Codec,
        targets: DeltaPropagationTargetRegistry,
    ) -> Self {
        Self {
            from,
            reply: false,
            codec,
            targets,
        }
    }

    /// Configures whether encoded propagation messages request ACK/NACK replies.
    ///
    /// This consumes and returns the transport so reply behavior is fixed for
    /// every later publication through that handle.
    pub fn with_reply(mut self, reply: bool) -> Self {
        self.reply = reply;
        self
    }

    /// Atomically replaces every registered target.
    pub fn set_targets(&self, targets: impl IntoIterator<Item = DeltaPropagationTarget>) {
        self.targets.set_targets(targets);
    }

    /// Inserts or replaces one target.
    pub fn insert_target(&self, target: DeltaPropagationTarget) {
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
    pub fn target_registry(&self) -> DeltaPropagationTargetRegistry {
        self.targets.clone()
    }
}

impl<Codec> DeltaPropagationTransport<Codec> {
    /// Encodes and attempts every selected per-replica propagation.
    ///
    /// Empty batches are skipped before route resolution. A missing target is
    /// reported before encoding, and encode or send failure for one target does
    /// not prevent attempts for later targets. Outcomes retain the deterministic
    /// lexical replica order of `propagations` within each report collection.
    pub fn publish<Delta>(
        &self,
        propagations: BTreeMap<ReplicaId, DeltaPropagation<Delta>>,
    ) -> DeltaTransportReport
    where
        Delta: ReplicatedData,
        Codec: CrdtDataCodec<Delta>,
    {
        let mut sent_to = Vec::new();
        let mut skipped_empty = Vec::new();
        let mut failures = Vec::new();

        for (replica, propagation) in propagations {
            if propagation.is_empty() {
                skipped_empty.push(replica);
                continue;
            }

            let Some(target) = self.targets.get(&replica) else {
                failures.push(DeltaTransportFailure::MissingTarget { replica });
                continue;
            };

            let message = match crate::encode_delta_propagation(
                self.from.clone(),
                self.reply,
                &propagation,
                &self.codec,
            ) {
                Ok(message) => message,
                Err(error) => {
                    failures.push(DeltaTransportFailure::EncodeFailed {
                        replica,
                        reason: error.to_string(),
                    });
                    continue;
                }
            };

            if let Err(error) = target.tell(message) {
                failures.push(DeltaTransportFailure::SendFailed {
                    replica,
                    reason: error.reason().to_string(),
                });
            } else {
                sent_to.push(replica);
            }
        }

        DeltaTransportReport {
            sent_to,
            skipped_empty,
            failures,
        }
    }
}
