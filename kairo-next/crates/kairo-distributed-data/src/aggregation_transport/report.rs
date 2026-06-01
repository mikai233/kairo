use crate::ReplicaId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregationTransportReport {
    pub(super) sent_to: Vec<ReplicaId>,
    pub(super) failures: Vec<AggregationTransportFailure>,
}

impl AggregationTransportReport {
    pub(super) fn new(sent_to: Vec<ReplicaId>, failures: Vec<AggregationTransportFailure>) -> Self {
        Self { sent_to, failures }
    }

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
