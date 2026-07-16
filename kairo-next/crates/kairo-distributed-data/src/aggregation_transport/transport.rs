use kairo_serialization::ActorRefWireData;

use super::{
    AggregationTarget, AggregationTargetRegistry, AggregationTransportFailure,
    AggregationTransportOperation, AggregationTransportReport,
};
use crate::{
    CrdtDataCodec, DataEnvelope, ReadAggregationPlan, ReplicaId, ReplicatedData,
    WriteAggregationPlan,
};

/// Publishes planned quorum reads and writes to registered replica targets.
///
/// Each write fan-out serializes its immutable wire envelope at most once and
/// clones that stable message for individual recipients. Target lookup and
/// delivery diagnostics remain per replica.
#[derive(Clone)]
pub struct AggregationTransport<Codec> {
    from: ReplicaId,
    codec: Codec,
    targets: AggregationTargetRegistry,
}

impl<Codec> AggregationTransport<Codec> {
    /// Creates a transport with an empty target registry.
    pub fn new(from: ReplicaId, codec: Codec) -> Self {
        Self {
            from,
            codec,
            targets: AggregationTargetRegistry::new(),
        }
    }

    /// Creates a transport backed by an existing shared target registry.
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

    /// Atomically replaces every delivery target.
    pub fn set_targets(&mut self, targets: impl IntoIterator<Item = AggregationTarget>) {
        self.targets.set_targets(targets);
    }

    /// Inserts or replaces one delivery target.
    pub fn insert_target(&mut self, target: AggregationTarget) {
        self.targets.insert_target(target);
    }

    /// Removes the delivery target for `replica`, if present.
    pub fn remove_target(&mut self, replica: &ReplicaId) {
        self.targets.remove_target(replica);
    }

    /// Returns the number of registered delivery targets.
    pub fn target_count(&self) -> usize {
        self.targets.target_count()
    }

    /// Returns a shared handle to the target registry.
    pub fn target_registry(&self) -> AggregationTargetRegistry {
        self.targets.clone()
    }
}

impl<Codec> AggregationTransport<Codec> {
    /// Publishes a full-state write to the plan's primary replicas.
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

    /// Publishes a full-state write to primary replicas with a reply sender.
    ///
    /// A target uses its sender-aware write recipient when available and falls
    /// back to the ordinary recipient otherwise.
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

    /// Publishes a full-state write to the plan's delayed secondary replicas.
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

    /// Publishes a full-state write to an explicit replica set.
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

    /// Publishes a full-state write with a reply sender to an explicit set.
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

    /// Publishes a full-state read to the plan's primary replicas.
    pub fn publish_read<D>(&self, plan: &ReadAggregationPlan<D>) -> AggregationTransportReport
    where
        D: crate::DeltaReplicatedData,
    {
        self.publish_read_to(plan.selection().primary(), plan)
    }

    /// Publishes a full-state read to primary replicas with a reply sender.
    ///
    /// A target uses its sender-aware read recipient when available and falls
    /// back to the ordinary recipient otherwise.
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

    /// Publishes a full-state read to the plan's delayed secondary replicas.
    pub fn publish_read_to_secondary<D>(
        &self,
        plan: &ReadAggregationPlan<D>,
    ) -> AggregationTransportReport
    where
        D: crate::DeltaReplicatedData,
    {
        self.publish_read_to(plan.selection().secondary(), plan)
    }

    /// Publishes a full-state read to an explicit replica set.
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

    /// Publishes a full-state read with a reply sender to an explicit set.
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
        let mut encoded = None;

        for replica in replicas {
            let Some(target) = self.targets.get(replica) else {
                failures.push(AggregationTransportFailure::MissingTarget {
                    replica: replica.clone(),
                    operation: AggregationTransportOperation::Write,
                });
                continue;
            };

            let message = match encoded
                .get_or_insert_with(|| {
                    crate::encode_write(
                        plan.state().key(),
                        Some(self.from.clone()),
                        envelope,
                        &self.codec,
                    )
                    .map_err(|error| error.to_string())
                })
                .as_ref()
            {
                Ok(message) => message.clone(),
                Err(reason) => {
                    failures.push(AggregationTransportFailure::EncodeFailed {
                        replica: replica.clone(),
                        operation: AggregationTransportOperation::Write,
                        reason: reason.clone(),
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
        let mut encoded = None;

        for replica in replicas {
            let Some(target) = self.targets.get(replica) else {
                failures.push(AggregationTransportFailure::MissingTarget {
                    replica: replica.clone(),
                    operation: AggregationTransportOperation::Read,
                });
                continue;
            };

            let message = encoded
                .get_or_insert_with(|| {
                    crate::encode_read(plan.state().key(), Some(self.from.clone()))
                })
                .clone();
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
