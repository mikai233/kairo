use std::collections::BTreeMap;
use std::sync::Arc;

use kairo_actor::Recipient;

use crate::{
    CrdtDataCodec, DataEnvelope, ReadAggregationPlan, ReplicaId, ReplicatedData, ReplicatorRead,
    ReplicatorWrite, WriteAggregationPlan,
};

type WriteRecipient = Arc<dyn Recipient<ReplicatorWrite> + Send + Sync>;
type ReadRecipient = Arc<dyn Recipient<ReplicatorRead> + Send + Sync>;

#[derive(Clone)]
pub struct AggregationTarget {
    replica: ReplicaId,
    write_recipient: WriteRecipient,
    read_recipient: ReadRecipient,
}

impl AggregationTarget {
    pub fn new(
        replica: ReplicaId,
        write_recipient: impl Recipient<ReplicatorWrite> + Send + Sync + 'static,
        read_recipient: impl Recipient<ReplicatorRead> + Send + Sync + 'static,
    ) -> Self {
        Self {
            replica,
            write_recipient: Arc::new(write_recipient),
            read_recipient: Arc::new(read_recipient),
        }
    }

    pub fn from_arcs(
        replica: ReplicaId,
        write_recipient: WriteRecipient,
        read_recipient: ReadRecipient,
    ) -> Self {
        Self {
            replica,
            write_recipient,
            read_recipient,
        }
    }

    pub fn replica(&self) -> &ReplicaId {
        &self.replica
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregationTransportReport {
    sent_to: Vec<ReplicaId>,
    failures: Vec<AggregationTransportFailure>,
}

impl AggregationTransportReport {
    pub fn sent_to(&self) -> &[ReplicaId] {
        &self.sent_to
    }

    pub fn failures(&self) -> &[AggregationTransportFailure] {
        &self.failures
    }

    pub fn is_success(&self) -> bool {
        self.failures.is_empty()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregationTransportOperation {
    Read,
    Write,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggregationTransportFailure {
    MissingTarget {
        replica: ReplicaId,
        operation: AggregationTransportOperation,
    },
    EncodeFailed {
        replica: ReplicaId,
        operation: AggregationTransportOperation,
        reason: String,
    },
    SendFailed {
        replica: ReplicaId,
        operation: AggregationTransportOperation,
        reason: String,
    },
}

#[derive(Clone)]
pub struct AggregationTransport<Codec> {
    from: ReplicaId,
    codec: Codec,
    targets: BTreeMap<ReplicaId, AggregationTarget>,
}

impl<Codec> AggregationTransport<Codec> {
    pub fn new(from: ReplicaId, codec: Codec) -> Self {
        Self {
            from,
            codec,
            targets: BTreeMap::new(),
        }
    }

    pub fn set_targets(&mut self, targets: impl IntoIterator<Item = AggregationTarget>) {
        self.targets = targets
            .into_iter()
            .map(|target| (target.replica.clone(), target))
            .collect();
    }

    pub fn insert_target(&mut self, target: AggregationTarget) {
        self.targets.insert(target.replica.clone(), target);
    }

    pub fn remove_target(&mut self, replica: &ReplicaId) {
        self.targets.remove(replica);
    }

    pub fn target_count(&self) -> usize {
        self.targets.len()
    }
}

impl<Codec> AggregationTransport<Codec> {
    pub fn publish_write<D>(
        &self,
        plan: &WriteAggregationPlan,
        envelope: &DataEnvelope<D>,
    ) -> AggregationTransportReport
    where
        D: ReplicatedData,
        Codec: CrdtDataCodec<D>,
    {
        self.publish_write_to(plan.selection().primary(), plan, envelope)
    }

    pub fn publish_write_to_secondary<D>(
        &self,
        plan: &WriteAggregationPlan,
        envelope: &DataEnvelope<D>,
    ) -> AggregationTransportReport
    where
        D: ReplicatedData,
        Codec: CrdtDataCodec<D>,
    {
        self.publish_write_to(plan.selection().secondary(), plan, envelope)
    }

    pub fn publish_write_to_replicas<D>(
        &self,
        replicas: &[ReplicaId],
        plan: &WriteAggregationPlan,
        envelope: &DataEnvelope<D>,
    ) -> AggregationTransportReport
    where
        D: ReplicatedData,
        Codec: CrdtDataCodec<D>,
    {
        self.publish_write_to(replicas, plan, envelope)
    }

    pub fn publish_read<D>(&self, plan: &ReadAggregationPlan<D>) -> AggregationTransportReport
    where
        D: crate::DeltaReplicatedData,
    {
        self.publish_read_to(plan.selection().primary(), plan)
    }

    pub fn publish_read_to_secondary<D>(
        &self,
        plan: &ReadAggregationPlan<D>,
    ) -> AggregationTransportReport
    where
        D: crate::DeltaReplicatedData,
    {
        self.publish_read_to(plan.selection().secondary(), plan)
    }

    pub fn publish_read_to_replicas<D>(
        &self,
        replicas: &[ReplicaId],
        plan: &ReadAggregationPlan<D>,
    ) -> AggregationTransportReport
    where
        D: crate::DeltaReplicatedData,
    {
        self.publish_read_to(replicas, plan)
    }

    fn publish_write_to<D>(
        &self,
        replicas: &[ReplicaId],
        plan: &WriteAggregationPlan,
        envelope: &DataEnvelope<D>,
    ) -> AggregationTransportReport
    where
        D: ReplicatedData,
        Codec: CrdtDataCodec<D>,
    {
        let mut sent_to = Vec::new();
        let mut failures = Vec::new();

        for replica in replicas {
            let Some(target) = self.targets.get(replica) else {
                failures.push(AggregationTransportFailure::MissingTarget {
                    replica: replica.clone(),
                    operation: AggregationTransportOperation::Write,
                });
                continue;
            };

            let message = match crate::encode_write(
                plan.state().key(),
                Some(self.from.clone()),
                envelope,
                &self.codec,
            ) {
                Ok(message) => message,
                Err(error) => {
                    failures.push(AggregationTransportFailure::EncodeFailed {
                        replica: replica.clone(),
                        operation: AggregationTransportOperation::Write,
                        reason: error.to_string(),
                    });
                    continue;
                }
            };

            if let Err(error) = target.write_recipient.tell(message) {
                failures.push(AggregationTransportFailure::SendFailed {
                    replica: replica.clone(),
                    operation: AggregationTransportOperation::Write,
                    reason: error.reason().to_string(),
                });
            } else {
                sent_to.push(replica.clone());
            }
        }

        AggregationTransportReport { sent_to, failures }
    }

    fn publish_read_to<D>(
        &self,
        replicas: &[ReplicaId],
        plan: &ReadAggregationPlan<D>,
    ) -> AggregationTransportReport
    where
        D: crate::DeltaReplicatedData,
    {
        let mut sent_to = Vec::new();
        let mut failures = Vec::new();

        for replica in replicas {
            let Some(target) = self.targets.get(replica) else {
                failures.push(AggregationTransportFailure::MissingTarget {
                    replica: replica.clone(),
                    operation: AggregationTransportOperation::Read,
                });
                continue;
            };

            let message = crate::encode_read(plan.state().key(), Some(self.from.clone()));
            if let Err(error) = target.read_recipient.tell(message) {
                failures.push(AggregationTransportFailure::SendFailed {
                    replica: replica.clone(),
                    operation: AggregationTransportOperation::Read,
                    reason: error.reason().to_string(),
                });
            } else {
                sent_to.push(replica.clone());
            }
        }

        AggregationTransportReport { sent_to, failures }
    }
}
