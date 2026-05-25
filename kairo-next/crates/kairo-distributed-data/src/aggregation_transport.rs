use std::collections::BTreeMap;
use std::sync::Arc;

use kairo_actor::{Recipient, SendError};
use kairo_serialization::ActorRefWireData;

use crate::{
    CrdtDataCodec, DataEnvelope, ReadAggregationPlan, ReplicaId, ReplicatedData, ReplicatorRead,
    ReplicatorRemoteEnvelopeOutbound, ReplicatorWrite, WriteAggregationPlan,
};

type WriteRecipient = Arc<dyn Recipient<ReplicatorWrite> + Send + Sync>;
type ReadRecipient = Arc<dyn Recipient<ReplicatorRead> + Send + Sync>;
type SenderAwareWriteRecipient = Arc<dyn SenderAwareRecipient<ReplicatorWrite>>;
type SenderAwareReadRecipient = Arc<dyn SenderAwareRecipient<ReplicatorRead>>;

pub trait SenderAwareRecipient<M: Send + 'static>: Send + Sync {
    fn tell_with_sender(&self, message: M, sender: &ActorRefWireData) -> Result<(), SendError<M>>;
}

#[derive(Clone)]
pub struct AggregationTarget {
    replica: ReplicaId,
    write_recipient: WriteRecipient,
    read_recipient: ReadRecipient,
    sender_aware_write_recipient: Option<SenderAwareWriteRecipient>,
    sender_aware_read_recipient: Option<SenderAwareReadRecipient>,
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
            sender_aware_write_recipient: None,
            sender_aware_read_recipient: None,
        }
    }

    pub fn new_sender_aware(
        replica: ReplicaId,
        write_recipient: impl Recipient<ReplicatorWrite> + Send + Sync + 'static,
        read_recipient: impl Recipient<ReplicatorRead> + Send + Sync + 'static,
        sender_aware_write_recipient: impl SenderAwareRecipient<ReplicatorWrite> + 'static,
        sender_aware_read_recipient: impl SenderAwareRecipient<ReplicatorRead> + 'static,
    ) -> Self {
        Self {
            replica,
            write_recipient: Arc::new(write_recipient),
            read_recipient: Arc::new(read_recipient),
            sender_aware_write_recipient: Some(Arc::new(sender_aware_write_recipient)),
            sender_aware_read_recipient: Some(Arc::new(sender_aware_read_recipient)),
        }
    }

    pub fn remote_envelope(
        replica: ReplicaId,
        write_recipient: ReplicatorRemoteEnvelopeOutbound,
        read_recipient: ReplicatorRemoteEnvelopeOutbound,
    ) -> Self {
        Self::new_sender_aware(
            replica,
            write_recipient.clone(),
            read_recipient.clone(),
            write_recipient,
            read_recipient,
        )
    }

    pub fn with_sender_aware_write(
        mut self,
        recipient: impl SenderAwareRecipient<ReplicatorWrite> + 'static,
    ) -> Self {
        self.sender_aware_write_recipient = Some(Arc::new(recipient));
        self
    }

    pub fn with_sender_aware_read(
        mut self,
        recipient: impl SenderAwareRecipient<ReplicatorRead> + 'static,
    ) -> Self {
        self.sender_aware_read_recipient = Some(Arc::new(recipient));
        self
    }

    pub fn supports_sender_aware_write(&self) -> bool {
        self.sender_aware_write_recipient.is_some()
    }

    pub fn supports_sender_aware_read(&self) -> bool {
        self.sender_aware_read_recipient.is_some()
    }

    pub fn sender_aware(&self) -> bool {
        self.supports_sender_aware_write() && self.supports_sender_aware_read()
    }

    pub fn from_arcs_with_sender_aware(
        replica: ReplicaId,
        write_recipient: WriteRecipient,
        read_recipient: ReadRecipient,
        sender_aware_write_recipient: Option<SenderAwareWriteRecipient>,
        sender_aware_read_recipient: Option<SenderAwareReadRecipient>,
    ) -> Self {
        Self {
            replica,
            write_recipient,
            read_recipient,
            sender_aware_write_recipient,
            sender_aware_read_recipient,
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
            sender_aware_write_recipient: None,
            sender_aware_read_recipient: None,
        }
    }

    pub fn replica(&self) -> &ReplicaId {
        &self.replica
    }
}

impl SenderAwareRecipient<ReplicatorWrite> for ReplicatorRemoteEnvelopeOutbound {
    fn tell_with_sender(
        &self,
        message: ReplicatorWrite,
        sender: &ActorRefWireData,
    ) -> Result<(), SendError<ReplicatorWrite>> {
        self.clone().with_sender(Some(sender.clone())).tell(message)
    }
}

impl SenderAwareRecipient<ReplicatorRead> for ReplicatorRemoteEnvelopeOutbound {
    fn tell_with_sender(
        &self,
        message: ReplicatorRead,
        sender: &ActorRefWireData,
    ) -> Result<(), SendError<ReplicatorRead>> {
        self.clone().with_sender(Some(sender.clone())).tell(message)
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

    pub fn publish_write_with_sender<D>(
        &self,
        plan: &WriteAggregationPlan,
        envelope: &DataEnvelope<D>,
        sender: &ActorRefWireData,
    ) -> AggregationTransportReport
    where
        D: ReplicatedData,
        Codec: CrdtDataCodec<D>,
    {
        self.publish_write_to_with_sender(plan.selection().primary(), plan, envelope, Some(sender))
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

    pub fn publish_write_to_replicas_with_sender<D>(
        &self,
        replicas: &[ReplicaId],
        plan: &WriteAggregationPlan,
        envelope: &DataEnvelope<D>,
        sender: &ActorRefWireData,
    ) -> AggregationTransportReport
    where
        D: ReplicatedData,
        Codec: CrdtDataCodec<D>,
    {
        self.publish_write_to_with_sender(replicas, plan, envelope, Some(sender))
    }

    pub fn publish_read<D>(&self, plan: &ReadAggregationPlan<D>) -> AggregationTransportReport
    where
        D: crate::DeltaReplicatedData,
    {
        self.publish_read_to(plan.selection().primary(), plan)
    }

    pub fn publish_read_with_sender<D>(
        &self,
        plan: &ReadAggregationPlan<D>,
        sender: &ActorRefWireData,
    ) -> AggregationTransportReport
    where
        D: crate::DeltaReplicatedData,
    {
        self.publish_read_to_with_sender(plan.selection().primary(), plan, Some(sender))
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

    pub fn publish_read_to_replicas_with_sender<D>(
        &self,
        replicas: &[ReplicaId],
        plan: &ReadAggregationPlan<D>,
        sender: &ActorRefWireData,
    ) -> AggregationTransportReport
    where
        D: crate::DeltaReplicatedData,
    {
        self.publish_read_to_with_sender(replicas, plan, Some(sender))
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
        self.publish_write_to_with_sender(replicas, plan, envelope, None)
    }

    fn publish_write_to_with_sender<D>(
        &self,
        replicas: &[ReplicaId],
        plan: &WriteAggregationPlan,
        envelope: &DataEnvelope<D>,
        sender: Option<&ActorRefWireData>,
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

            let send_result = match (sender, &target.sender_aware_write_recipient) {
                (Some(sender), Some(recipient)) => recipient.tell_with_sender(message, sender),
                _ => target.write_recipient.tell(message),
            };

            if let Err(error) = send_result {
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
        self.publish_read_to_with_sender(replicas, plan, None)
    }

    fn publish_read_to_with_sender<D>(
        &self,
        replicas: &[ReplicaId],
        plan: &ReadAggregationPlan<D>,
        sender: Option<&ActorRefWireData>,
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
            let send_result = match (sender, &target.sender_aware_read_recipient) {
                (Some(sender), Some(recipient)) => recipient.tell_with_sender(message, sender),
                _ => target.read_recipient.tell(message),
            };

            if let Err(error) = send_result {
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
