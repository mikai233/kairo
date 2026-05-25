use std::collections::BTreeMap;
use std::sync::Arc;

use kairo_actor::Recipient;

use crate::{
    CrdtDataCodec, DeltaPropagation, ReplicaId, ReplicatedData, ReplicatorDeltaPropagation,
};

type DeltaRecipient = Arc<dyn Recipient<ReplicatorDeltaPropagation> + Send + Sync>;

#[derive(Clone)]
pub struct DeltaPropagationTarget {
    replica: ReplicaId,
    recipient: DeltaRecipient,
}

impl DeltaPropagationTarget {
    pub fn new(
        replica: ReplicaId,
        recipient: impl Recipient<ReplicatorDeltaPropagation> + Send + Sync + 'static,
    ) -> Self {
        Self {
            replica,
            recipient: Arc::new(recipient),
        }
    }

    pub fn from_arc(replica: ReplicaId, recipient: DeltaRecipient) -> Self {
        Self { replica, recipient }
    }

    pub fn replica(&self) -> &ReplicaId {
        &self.replica
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaTransportReport {
    sent_to: Vec<ReplicaId>,
    skipped_empty: Vec<ReplicaId>,
    failures: Vec<DeltaTransportFailure>,
}

impl DeltaTransportReport {
    pub fn empty() -> Self {
        Self {
            sent_to: Vec::new(),
            skipped_empty: Vec::new(),
            failures: Vec::new(),
        }
    }

    pub fn sent_to(&self) -> &[ReplicaId] {
        &self.sent_to
    }

    pub fn skipped_empty(&self) -> &[ReplicaId] {
        &self.skipped_empty
    }

    pub fn failures(&self) -> &[DeltaTransportFailure] {
        &self.failures
    }

    pub fn is_success(&self) -> bool {
        self.failures.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeltaTransportFailure {
    MissingTarget { replica: ReplicaId },
    EncodeFailed { replica: ReplicaId, reason: String },
    SendFailed { replica: ReplicaId, reason: String },
}

#[derive(Clone)]
pub struct DeltaPropagationTransport<Codec> {
    from: ReplicaId,
    reply: bool,
    codec: Codec,
    targets: BTreeMap<ReplicaId, DeltaRecipient>,
}

impl<Codec> DeltaPropagationTransport<Codec> {
    pub fn new(from: ReplicaId, codec: Codec) -> Self {
        Self {
            from,
            reply: false,
            codec,
            targets: BTreeMap::new(),
        }
    }

    pub fn with_reply(mut self, reply: bool) -> Self {
        self.reply = reply;
        self
    }

    pub fn set_targets(&mut self, targets: impl IntoIterator<Item = DeltaPropagationTarget>) {
        self.targets = targets
            .into_iter()
            .map(|target| (target.replica, target.recipient))
            .collect();
    }

    pub fn insert_target(&mut self, target: DeltaPropagationTarget) {
        self.targets.insert(target.replica, target.recipient);
    }

    pub fn remove_target(&mut self, replica: &ReplicaId) {
        self.targets.remove(replica);
    }

    pub fn target_count(&self) -> usize {
        self.targets.len()
    }
}

impl<Codec> DeltaPropagationTransport<Codec> {
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
