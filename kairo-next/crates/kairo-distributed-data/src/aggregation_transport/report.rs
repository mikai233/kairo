use crate::ReplicaId;

/// Per-replica outcome of publishing one aggregation request fan-out.
///
/// Successful and failed targets are both retained because a quorum operation
/// can continue after an individual route or mailbox failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregationTransportReport {
    pub(super) sent_to: Vec<ReplicaId>,
    pub(super) failures: Vec<AggregationTransportFailure>,
}

impl AggregationTransportReport {
    pub(super) fn new(sent_to: Vec<ReplicaId>, failures: Vec<AggregationTransportFailure>) -> Self {
        Self { sent_to, failures }
    }

    /// Returns replicas whose recipient accepted the request.
    pub fn sent_to(&self) -> &[ReplicaId] {
        &self.sent_to
    }

    /// Returns target resolution, encoding, and delivery failures.
    pub fn failures(&self) -> &[AggregationTransportFailure] {
        &self.failures
    }

    /// Returns `true` when every requested replica accepted the request.
    pub fn is_success(&self) -> bool {
        self.failures.is_empty()
    }
}

/// Identifies the request kind associated with a transport failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregationTransportOperation {
    /// A full-state read request.
    Read,
    /// A full-state write request.
    Write,
}

/// Failure to publish an aggregation request to one replica.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggregationTransportFailure {
    /// No delivery target is registered for the replica.
    MissingTarget {
        /// Replica that had no registered target.
        replica: ReplicaId,
        /// Request kind that could not be delivered.
        operation: AggregationTransportOperation,
    },
    /// The shared write envelope could not be encoded.
    EncodeFailed {
        /// Replica that would have received the encoded write.
        replica: ReplicaId,
        /// Request kind, currently always [`AggregationTransportOperation::Write`].
        operation: AggregationTransportOperation,
        /// Stable diagnostic derived from the serialization error.
        reason: String,
    },
    /// The resolved recipient rejected the request.
    SendFailed {
        /// Replica whose recipient rejected delivery.
        replica: ReplicaId,
        /// Request kind that was rejected.
        operation: AggregationTransportOperation,
        /// Diagnostic supplied by the recipient send error.
        reason: String,
    },
}
