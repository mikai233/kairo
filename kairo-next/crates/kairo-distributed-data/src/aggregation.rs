use std::collections::BTreeSet;

use crate::{
    DataEnvelope, DeltaReplicatedData, ReadConsistency, ReplicaId, ReplicatorKey, WriteConsistency,
};

const MAX_SECONDARY_NODES: usize = 10;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggregationError {
    LocalConsistencyUnsupported,
    NotEnoughReplicas { required: usize, available: usize },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaSelection {
    primary: Vec<ReplicaId>,
    secondary: Vec<ReplicaId>,
}

impl ReplicaSelection {
    pub fn primary(&self) -> &[ReplicaId] {
        &self.primary
    }

    pub fn secondary(&self) -> &[ReplicaId] {
        &self.secondary
    }
}

#[derive(Debug, Clone)]
pub struct WriteAggregationPlan {
    state: WriteAggregatorState,
    selection: ReplicaSelection,
}

impl WriteAggregationPlan {
    pub fn new(state: WriteAggregatorState, selection: ReplicaSelection) -> Self {
        Self { state, selection }
    }

    pub fn state(&self) -> &WriteAggregatorState {
        &self.state
    }

    pub fn into_state(self) -> WriteAggregatorState {
        self.state
    }

    pub fn selection(&self) -> &ReplicaSelection {
        &self.selection
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteAggregationOutcome {
    InProgress,
    Success,
    Failed {
        required: usize,
        available: usize,
    },
    Timeout {
        required: usize,
        acknowledged: usize,
    },
}

#[derive(Debug, Clone)]
pub struct WriteAggregatorState {
    key: ReplicatorKey,
    remote_nodes: Vec<ReplicaId>,
    required_remote_acks: usize,
    acked: BTreeSet<ReplicaId>,
    nacked: BTreeSet<ReplicaId>,
}

impl WriteAggregatorState {
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

    pub fn key(&self) -> &ReplicatorKey {
        &self.key
    }

    pub fn required_remote_acks(&self) -> usize {
        self.required_remote_acks
    }

    pub fn remote_nodes(&self) -> &[ReplicaId] {
        &self.remote_nodes
    }

    pub fn record_ack(&mut self, replica: &ReplicaId) -> WriteAggregationOutcome {
        if self.remote_nodes.contains(replica) {
            self.nacked.remove(replica);
            self.acked.insert(replica.clone());
        }
        self.outcome()
    }

    pub fn record_nack(&mut self, replica: &ReplicaId) -> WriteAggregationOutcome {
        if self.remote_nodes.contains(replica) && !self.acked.contains(replica) {
            self.nacked.insert(replica.clone());
        }
        self.outcome()
    }

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

    pub fn select_replicas(&self, unreachable: &BTreeSet<ReplicaId>) -> ReplicaSelection {
        select_replicas(&self.remote_nodes, unreachable, self.required_remote_acks)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadAggregationOutcome<D> {
    InProgress,
    Success { envelope: DataEnvelope<D> },
    NotFound,
    Failure { required: usize, received: usize },
}

#[derive(Debug, Clone)]
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
    pub fn new(state: ReadAggregatorState<D>, selection: ReplicaSelection) -> Self {
        Self { state, selection }
    }

    pub fn state(&self) -> &ReadAggregatorState<D> {
        &self.state
    }

    pub fn into_state(self) -> ReadAggregatorState<D> {
        self.state
    }

    pub fn selection(&self) -> &ReplicaSelection {
        &self.selection
    }
}

#[derive(Debug, Clone)]
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

    pub fn key(&self) -> &ReplicatorKey {
        &self.key
    }

    pub fn required_remote_reads(&self) -> usize {
        self.required_remote_reads
    }

    pub fn remote_nodes(&self) -> &[ReplicaId] {
        &self.remote_nodes
    }

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

    pub fn timeout(&self) -> ReadAggregationOutcome<D> {
        match self.outcome() {
            ReadAggregationOutcome::InProgress => ReadAggregationOutcome::Failure {
                required: self.required_remote_reads,
                received: self.received,
            },
            outcome => outcome,
        }
    }

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

    pub fn select_replicas(&self, unreachable: &BTreeSet<ReplicaId>) -> ReplicaSelection {
        select_replicas(&self.remote_nodes, unreachable, self.required_remote_reads)
    }
}

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
