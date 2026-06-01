use kairo_serialization::ActorRefWireData;

use super::{
    AggregationTarget, AggregationTargetRegistry, AggregationTransportFailure,
    AggregationTransportOperation, AggregationTransportReport,
};
use crate::{
    CrdtDataCodec, DataEnvelope, ReadAggregationPlan, ReplicaId, ReplicatedData,
    WriteAggregationPlan,
};

#[derive(Clone)]
pub struct AggregationTransport<Codec> {
    from: ReplicaId,
    codec: Codec,
    targets: AggregationTargetRegistry,
}

impl<Codec> AggregationTransport<Codec> {
    pub fn new(from: ReplicaId, codec: Codec) -> Self {
        Self {
            from,
            codec,
            targets: AggregationTargetRegistry::new(),
        }
    }

    pub fn with_target_registry(
        from: ReplicaId,
        codec: Codec,
        targets: AggregationTargetRegistry,
    ) -> Self {
        Self {
            from,
            codec,
            targets,
        }
    }

    pub fn set_targets(&mut self, targets: impl IntoIterator<Item = AggregationTarget>) {
        self.targets.set_targets(targets);
    }

    pub fn insert_target(&mut self, target: AggregationTarget) {
        self.targets.insert_target(target);
    }

    pub fn remove_target(&mut self, replica: &ReplicaId) {
        self.targets.remove_target(replica);
    }

    pub fn target_count(&self) -> usize {
        self.targets.target_count()
    }

    pub fn target_registry(&self) -> AggregationTargetRegistry {
        self.targets.clone()
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

        AggregationTransportReport::new(sent_to, failures)
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

        AggregationTransportReport::new(sent_to, failures)
    }
}
