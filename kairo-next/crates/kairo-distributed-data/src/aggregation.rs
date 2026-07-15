#![deny(missing_docs)]
//! Quorum calculation, replica selection, and read/write aggregation state.
//!
//! The state machines in this module count the local replica implicitly and
//! operate on a caller-supplied list of distinct remote replicas. Reachable
//! remotes are selected before unreachable remotes, while completion depends
//! on replies rather than reachability observations.

use std::collections::BTreeSet;

use crate::{
    DataEnvelope, DeltaReplicatedData, ReadConsistency, ReplicaId, ReplicatorKey, WriteConsistency,
};

const MAX_SECONDARY_NODES: usize = 10;

#[derive(Debug, Clone, PartialEq, Eq)]
/// Failure to construct a remote aggregation state machine.
pub enum AggregationError {
    /// Local consistency does not require a remote aggregation state machine.
    LocalConsistencyUnsupported,
    /// The requested write consistency cannot be met by the known remotes.
    NotEnoughReplicas {
        /// Number of remote acknowledgements required after counting local.
        required: usize,
        /// Number of known remote replicas.
        available: usize,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Primary and delayed-secondary remote replicas for one aggregation attempt.
pub struct ReplicaSelection {
    primary: Vec<ReplicaId>,
    secondary: Vec<ReplicaId>,
}

impl ReplicaSelection {
    /// Returns replicas contacted immediately.
    pub fn primary(&self) -> &[ReplicaId] {
        &self.primary
    }

    /// Returns up to ten remaining replicas eligible for delayed contact.
    pub fn secondary(&self) -> &[ReplicaId] {
        &self.secondary
    }
}

#[derive(Debug, Clone)]
/// Prepared write aggregation state together with its initial target selection.
pub struct WriteAggregationPlan {
    state: WriteAggregatorState,
    selection: ReplicaSelection,
}

impl WriteAggregationPlan {
    /// Combines an aggregation state machine with a target selection.
    pub fn new(state: WriteAggregatorState, selection: ReplicaSelection) -> Self {
        Self { state, selection }
    }

    /// Returns the write aggregation state machine.
    pub fn state(&self) -> &WriteAggregatorState {
        &self.state
    }

    /// Consumes the plan and returns its write aggregation state machine.
    pub fn into_state(self) -> WriteAggregatorState {
        self.state
    }

    /// Returns the initial primary and secondary targets.
    pub fn selection(&self) -> &ReplicaSelection {
        &self.selection
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Current completion result of a write aggregation.
pub enum WriteAggregationOutcome {
    /// More remote replies may still satisfy the requested consistency.
    InProgress,
    /// Enough distinct known replicas acknowledged the write.
    Success,
    /// Negative acknowledgements make the requested consistency impossible.
    Failed {
        /// Number of remote acknowledgements required.
        required: usize,
        /// Maximum number of remote acknowledgements still available.
        available: usize,
    },
    /// The deadline elapsed before enough acknowledgements arrived.
    Timeout {
        /// Number of remote acknowledgements required.
        required: usize,
        /// Number of distinct remote acknowledgements received.
        acknowledged: usize,
    },
}

#[derive(Debug, Clone)]
/// Tracks distinct remote ACK and NACK replies for one write.
pub struct WriteAggregatorState {
    key: ReplicatorKey,
    remote_nodes: Vec<ReplicaId>,
    required_remote_acks: usize,
    acked: BTreeSet<ReplicaId>,
    nacked: BTreeSet<ReplicaId>,
}

impl WriteAggregatorState {
    /// Creates a write aggregator for distinct known `remote_nodes`.
    ///
    /// The local replica is counted implicitly. Local consistency is rejected,
    /// as is a remote quorum that cannot be met by the supplied replica list.
    pub fn new(
        key: ReplicatorKey,
        consistency: &WriteConsistency,
        remote_nodes: Vec<ReplicaId>,
    ) -> Result<Self, AggregationError> {
        let required_remote_acks = required_remote_write_acks(consistency, remote_nodes.len())?;
        Ok(Self {
            key,
            remote_nodes,
            required_remote_acks,
            acked: BTreeSet::new(),
            nacked: BTreeSet::new(),
        })
    }

    /// Returns the key being written.
    pub fn key(&self) -> &ReplicatorKey {
        &self.key
    }

    /// Returns the number of distinct remote ACKs needed for success.
    pub fn required_remote_acks(&self) -> usize {
        self.required_remote_acks
    }

    /// Returns the known remote replica universe.
    pub fn remote_nodes(&self) -> &[ReplicaId] {
        &self.remote_nodes
    }

    /// Records an ACK from a known replica and returns the current outcome.
    ///
    /// Unknown and duplicate replies are ignored. An ACK supersedes an earlier
    /// NACK from the same replica.
    pub fn record_ack(&mut self, replica: &ReplicaId) -> WriteAggregationOutcome {
        if self.remote_nodes.contains(replica) {
            self.nacked.remove(replica);
            self.acked.insert(replica.clone());
        }
        self.outcome()
    }

    /// Records a NACK from a known replica and returns the current outcome.
    ///
    /// Unknown and duplicate replies are ignored, and a completed ACK cannot
    /// be superseded by a later NACK.
    pub fn record_nack(&mut self, replica: &ReplicaId) -> WriteAggregationOutcome {
        if self.remote_nodes.contains(replica) && !self.acked.contains(replica) {
            self.nacked.insert(replica.clone());
        }
        self.outcome()
    }

    /// Produces success or a timeout using the replies received so far.
    pub fn timeout(&self) -> WriteAggregationOutcome {
        if self.acked.len() >= self.required_remote_acks {
            WriteAggregationOutcome::Success
        } else {
            WriteAggregationOutcome::Timeout {
                required: self.required_remote_acks,
                acknowledged: self.acked.len(),
            }
        }
    }

    /// Returns the current write completion outcome without changing state.
    pub fn outcome(&self) -> WriteAggregationOutcome {
        if self.acked.len() >= self.required_remote_acks {
            return WriteAggregationOutcome::Success;
        }

        let available = self.remote_nodes.len().saturating_sub(self.nacked.len());
        if available < self.required_remote_acks {
            WriteAggregationOutcome::Failed {
                required: self.required_remote_acks,
                available,
            }
        } else {
            WriteAggregationOutcome::InProgress
        }
    }

    /// Selects reachable primary replicas first and caps delayed secondaries.
    pub fn select_replicas(&self, unreachable: &BTreeSet<ReplicaId>) -> ReplicaSelection {
        select_replicas(&self.remote_nodes, unreachable, self.required_remote_acks)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Current completion result of a read aggregation.
pub enum ReadAggregationOutcome<D> {
    /// More distinct remote replies are required.
    InProgress,
    /// The requested quorum returned and at least one value was found.
    Success {
        /// Merge of the local value and every received remote value.
        envelope: DataEnvelope<D>,
    },
    /// The requested quorum returned without finding the key.
    NotFound,
    /// The deadline elapsed or the known remote set cannot meet the quorum.
    Failure {
        /// Number of remote results required after counting local.
        required: usize,
        /// Number of accepted remote results received.
        received: usize,
    },
}

#[derive(Debug, Clone)]
/// Prepared read aggregation state together with its initial target selection.
pub struct ReadAggregationPlan<D>
where
    D: DeltaReplicatedData,
{
    state: ReadAggregatorState<D>,
    selection: ReplicaSelection,
}

impl<D> ReadAggregationPlan<D>
where
    D: DeltaReplicatedData,
{
    /// Combines a read aggregation state machine with a target selection.
    pub fn new(state: ReadAggregatorState<D>, selection: ReplicaSelection) -> Self {
        Self { state, selection }
    }

    /// Returns the read aggregation state machine.
    pub fn state(&self) -> &ReadAggregatorState<D> {
        &self.state
    }

    /// Consumes the plan and returns its read aggregation state machine.
    pub fn into_state(self) -> ReadAggregatorState<D> {
        self.state
    }

    /// Returns the initial primary and secondary targets.
    pub fn selection(&self) -> &ReplicaSelection {
        &self.selection
    }
}

#[derive(Debug, Clone)]
/// Tracks distinct remote read replies and their merged value.
pub struct ReadAggregatorState<D>
where
    D: DeltaReplicatedData,
{
    key: ReplicatorKey,
    remote_nodes: Vec<ReplicaId>,
    required_remote_reads: usize,
    received: usize,
    received_from: BTreeSet<ReplicaId>,
    result: Option<DataEnvelope<D>>,
}

impl<D> ReadAggregatorState<D>
where
    D: DeltaReplicatedData,
{
    /// Creates a read aggregator for distinct known `remote_nodes`.
    ///
    /// The local replica is counted implicitly, and `local_value` participates
    /// in the merged result without counting as a remote reply. Local
    /// consistency is rejected. An unavailable quorum is represented by the
    /// state's immediate [`ReadAggregationOutcome::Failure`] outcome.
    pub fn new(
        key: ReplicatorKey,
        consistency: &ReadConsistency,
        remote_nodes: Vec<ReplicaId>,
        local_value: Option<DataEnvelope<D>>,
    ) -> Result<Self, AggregationError> {
        let required_remote_reads = required_remote_read_results(consistency, remote_nodes.len())?;
        Ok(Self {
            key,
            remote_nodes,
            required_remote_reads,
            received: 0,
            received_from: BTreeSet::new(),
            result: local_value,
        })
    }

    /// Returns the key being read.
    pub fn key(&self) -> &ReplicatorKey {
        &self.key
    }

    /// Returns the number of distinct remote results needed for completion.
    pub fn required_remote_reads(&self) -> usize {
        self.required_remote_reads
    }

    /// Returns the known remote replica universe.
    pub fn remote_nodes(&self) -> &[ReplicaId] {
        &self.remote_nodes
    }

    /// Records a trusted, unidentified remote result.
    ///
    /// This lower-level operation counts every call. Transport-facing code
    /// should use [`Self::record_read_from`] to reject unknown and duplicate
    /// sources.
    pub fn record_read(&mut self, envelope: Option<DataEnvelope<D>>) -> ReadAggregationOutcome<D> {
        self.received += 1;
        self.result = match (self.result.take(), envelope) {
            (Some(left), Some(right)) => Some(left.merge(&right)),
            (Some(left), None) => Some(left),
            (None, Some(right)) => Some(right),
            (None, None) => None,
        };
        self.outcome()
    }

    /// Records at most one result from a known remote replica.
    ///
    /// Unknown and duplicate sources are ignored.
    pub fn record_read_from(
        &mut self,
        replica: &ReplicaId,
        envelope: Option<DataEnvelope<D>>,
    ) -> ReadAggregationOutcome<D> {
        if !self.remote_nodes.contains(replica) || !self.received_from.insert(replica.clone()) {
            return self.outcome();
        }
        self.record_read(envelope)
    }

    /// Produces the current terminal result or a read failure at the deadline.
    pub fn timeout(&self) -> ReadAggregationOutcome<D> {
        match self.outcome() {
            ReadAggregationOutcome::InProgress => ReadAggregationOutcome::Failure {
                required: self.required_remote_reads,
                received: self.received,
            },
            outcome => outcome,
        }
    }

    /// Returns the current read completion outcome without changing state.
    pub fn outcome(&self) -> ReadAggregationOutcome<D> {
        if self.required_remote_reads > self.remote_nodes.len() {
            return ReadAggregationOutcome::Failure {
                required: self.required_remote_reads,
                received: self.received,
            };
        }

        if self.received < self.required_remote_reads {
            return ReadAggregationOutcome::InProgress;
        }

        match &self.result {
            Some(envelope) => ReadAggregationOutcome::Success {
                envelope: envelope.clone(),
            },
            None => ReadAggregationOutcome::NotFound,
        }
    }

    /// Selects reachable primary replicas first and caps delayed secondaries.
    pub fn select_replicas(&self, unreachable: &BTreeSet<ReplicaId>) -> ReplicaSelection {
        select_replicas(&self.remote_nodes, unreachable, self.required_remote_reads)
    }
}

/// Calculates a capped majority including an optional minimum and extra votes.
///
/// The result never exceeds `total_replicas`. An `additional` count extends
/// the simple majority before the minimum cap and total cap are applied.
pub fn calculate_majority(min_cap: usize, total_replicas: usize, additional: usize) -> usize {
    let majority = (total_replicas / 2) + 1;
    total_replicas.min((majority + additional).max(min_cap))
}

fn required_remote_write_acks(
    consistency: &WriteConsistency,
    remote_count: usize,
) -> Result<usize, AggregationError> {
    required_remote_count_for_write(consistency, remote_count).and_then(|required| {
        ensure_available(required, remote_count)?;
        Ok(required)
    })
}

fn required_remote_read_results(
    consistency: &ReadConsistency,
    remote_count: usize,
) -> Result<usize, AggregationError> {
    required_remote_count_for_read(consistency, remote_count)
}

fn required_remote_count_for_write(
    consistency: &WriteConsistency,
    remote_count: usize,
) -> Result<usize, AggregationError> {
    let total = remote_count + 1;
    let required_total = match consistency {
        WriteConsistency::Local => return Err(AggregationError::LocalConsistencyUnsupported),
        WriteConsistency::To { replicas, .. } => *replicas,
        WriteConsistency::Majority { min_cap, .. } => calculate_majority(*min_cap, total, 0),
        WriteConsistency::MajorityPlus {
            additional,
            min_cap,
            ..
        } => calculate_majority(*min_cap, total, *additional),
        WriteConsistency::All { .. } => total,
    };
    Ok(required_total.saturating_sub(1))
}

fn required_remote_count_for_read(
    consistency: &ReadConsistency,
    remote_count: usize,
) -> Result<usize, AggregationError> {
    let total = remote_count + 1;
    let required_total = match consistency {
        ReadConsistency::Local => return Err(AggregationError::LocalConsistencyUnsupported),
        ReadConsistency::From { replicas, .. } => *replicas,
        ReadConsistency::Majority { min_cap, .. } => calculate_majority(*min_cap, total, 0),
        ReadConsistency::MajorityPlus {
            additional,
            min_cap,
            ..
        } => calculate_majority(*min_cap, total, *additional),
        ReadConsistency::All { .. } => total,
    };
    Ok(required_total.saturating_sub(1))
}

fn ensure_available(required: usize, available: usize) -> Result<(), AggregationError> {
    if required <= available {
        Ok(())
    } else {
        Err(AggregationError::NotEnoughReplicas {
            required,
            available,
        })
    }
}

fn select_replicas(
    remote_nodes: &[ReplicaId],
    unreachable: &BTreeSet<ReplicaId>,
    primary_size: usize,
) -> ReplicaSelection {
    let mut ordered = Vec::with_capacity(remote_nodes.len());
    ordered.extend(
        remote_nodes
            .iter()
            .filter(|node| !unreachable.contains(*node))
            .cloned(),
    );
    ordered.extend(
        remote_nodes
            .iter()
            .filter(|node| unreachable.contains(*node))
            .cloned(),
    );

    let primary_size = primary_size.min(ordered.len());
    let primary = ordered[..primary_size].to_vec();
    let secondary = ordered[primary_size..]
        .iter()
        .take(MAX_SECONDARY_NODES)
        .cloned()
        .collect();
    ReplicaSelection { primary, secondary }
}
