use std::collections::BTreeMap;

use crate::{
    CrdtDataCodec, DeltaReplicatedData, PruningTable, RemovedNodePruning, ReplicaId,
    ReplicatorDeltaAck, ReplicatorDeltaNack, ReplicatorDeltaPropagation, ReplicatorKey,
    ReplicatorState, decode_delta,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeltaReceiveStatus {
    Applied {
        from: ReplicaId,
        key: ReplicatorKey,
        previous_version: u64,
        to_version: u64,
        changed: bool,
    },
    AlreadyHandled {
        from: ReplicaId,
        key: ReplicatorKey,
        current_version: u64,
        to_version: u64,
    },
    Missing {
        from: ReplicaId,
        key: ReplicatorKey,
        current_version: u64,
        expected_from_version: u64,
        from_version: u64,
        to_version: u64,
    },
    InvalidRange {
        from: ReplicaId,
        key: ReplicatorKey,
        from_version: u64,
        to_version: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaPropagationReceiveReport {
    from: ReplicaId,
    reply_requested: bool,
    statuses: Vec<DeltaReceiveStatus>,
    failures: Vec<DeltaReceiveFailure>,
}

impl DeltaPropagationReceiveReport {
    pub fn from(&self) -> &ReplicaId {
        &self.from
    }

    pub fn reply_requested(&self) -> bool {
        self.reply_requested
    }

    pub fn statuses(&self) -> &[DeltaReceiveStatus] {
        &self.statuses
    }

    pub fn failures(&self) -> &[DeltaReceiveFailure] {
        &self.failures
    }

    pub fn is_success(&self) -> bool {
        self.failures.is_empty()
            && self.statuses.iter().all(|status| {
                matches!(
                    status,
                    DeltaReceiveStatus::Applied { .. } | DeltaReceiveStatus::AlreadyHandled { .. }
                )
            })
    }

    pub fn reply(&self) -> Option<DeltaReceiveReply> {
        if !self.reply_requested {
            return None;
        }

        if self.is_success() {
            Some(DeltaReceiveReply::Ack(ReplicatorDeltaAck))
        } else {
            Some(DeltaReceiveReply::Nack(ReplicatorDeltaNack))
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeltaReceiveFailure {
    DecodeFailed { key: String, reason: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaReceiveReply {
    Ack(ReplicatorDeltaAck),
    Nack(ReplicatorDeltaNack),
}

#[derive(Debug, Clone, Default)]
pub struct DeltaReceiveTracker {
    versions: BTreeMap<(ReplicaId, ReplicatorKey), u64>,
}

impl DeltaReceiveTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn current_version(&self, from: &ReplicaId, key: &ReplicatorKey) -> u64 {
        self.versions
            .get(&(from.clone(), key.clone()))
            .copied()
            .unwrap_or_default()
    }

    pub fn clear_from(&mut self, from: &ReplicaId) {
        self.versions.retain(|(node, _), _| node != from);
    }

    pub fn forget_key(&mut self, key: &ReplicatorKey) {
        self.versions
            .retain(|(_, existing_key), _| existing_key != key);
    }

    pub fn apply_propagation<D, Codec>(
        &mut self,
        state: &mut ReplicatorState<D>,
        propagation: &ReplicatorDeltaPropagation,
        codec: &Codec,
    ) -> DeltaPropagationReceiveReport
    where
        D: DeltaReplicatedData + RemovedNodePruning,
        Codec: CrdtDataCodec<D::Delta> + ?Sized,
    {
        self.apply_propagation_inner(state, propagation, codec, None)
    }

    pub(crate) fn apply_propagation_with_seen<D, Codec>(
        &mut self,
        state: &mut ReplicatorState<D>,
        propagation: &ReplicatorDeltaPropagation,
        codec: &Codec,
        seen_by: &ReplicaId,
    ) -> DeltaPropagationReceiveReport
    where
        D: DeltaReplicatedData + RemovedNodePruning,
        Codec: CrdtDataCodec<D::Delta> + ?Sized,
    {
        self.apply_propagation_inner(state, propagation, codec, Some(seen_by))
    }

    fn apply_propagation_inner<D, Codec>(
        &mut self,
        state: &mut ReplicatorState<D>,
        propagation: &ReplicatorDeltaPropagation,
        codec: &Codec,
        seen_by: Option<&ReplicaId>,
    ) -> DeltaPropagationReceiveReport
    where
        D: DeltaReplicatedData + RemovedNodePruning,
        Codec: CrdtDataCodec<D::Delta> + ?Sized,
    {
        let mut statuses = Vec::with_capacity(propagation.deltas.len());
        let mut failures = Vec::new();

        for delta in &propagation.deltas {
            match decode_delta(delta, codec) {
                Ok(decoded) => {
                    let pruning = decoded.pruning().clone();
                    statuses.push(self.apply_delta_pruned(
                        state,
                        propagation.from.clone(),
                        decoded.key().clone(),
                        decoded.from_version(),
                        decoded.to_version(),
                        decoded.into_delta(),
                        pruning,
                        seen_by,
                    ));
                }
                Err(error) => failures.push(DeltaReceiveFailure::DecodeFailed {
                    key: delta.key.clone(),
                    reason: error.to_string(),
                }),
            }
        }

        DeltaPropagationReceiveReport {
            from: propagation.from.clone(),
            reply_requested: propagation.reply,
            statuses,
            failures,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_delta_pruned<D>(
        &mut self,
        state: &mut ReplicatorState<D>,
        from: ReplicaId,
        key: ReplicatorKey,
        from_version: u64,
        to_version: u64,
        delta: D::Delta,
        pruning: PruningTable,
        seen_by: Option<&ReplicaId>,
    ) -> DeltaReceiveStatus
    where
        D: DeltaReplicatedData + RemovedNodePruning,
    {
        if from_version == 0 || to_version < from_version {
            return DeltaReceiveStatus::InvalidRange {
                from,
                key,
                from_version,
                to_version,
            };
        }

        let previous_version = self.current_version(&from, &key);
        if previous_version >= to_version {
            return DeltaReceiveStatus::AlreadyHandled {
                from,
                key,
                current_version: previous_version,
                to_version,
            };
        }

        let expected_from_version = previous_version + 1;
        if from_version > expected_from_version {
            return DeltaReceiveStatus::Missing {
                from,
                key,
                current_version: previous_version,
                expected_from_version,
                from_version,
                to_version,
            };
        }

        let merged = state.write_delta_pruned(key.clone(), delta, &pruning, wall_millis());
        let seen_changed =
            seen_by.is_some_and(|seen_by| state.mark_key_pruning_seen(&key, seen_by.clone()));
        self.versions
            .insert((from.clone(), key.clone()), to_version);
        DeltaReceiveStatus::Applied {
            from,
            key,
            previous_version,
            to_version,
            changed: merged || seen_changed,
        }
    }

    pub fn apply_delta<D>(
        &mut self,
        state: &mut ReplicatorState<D>,
        from: ReplicaId,
        key: ReplicatorKey,
        from_version: u64,
        to_version: u64,
        delta: D::Delta,
    ) -> DeltaReceiveStatus
    where
        D: DeltaReplicatedData,
    {
        if from_version == 0 || to_version < from_version {
            return DeltaReceiveStatus::InvalidRange {
                from,
                key,
                from_version,
                to_version,
            };
        }

        let previous_version = self.current_version(&from, &key);
        if previous_version >= to_version {
            return DeltaReceiveStatus::AlreadyHandled {
                from,
                key,
                current_version: previous_version,
                to_version,
            };
        }

        let expected_from_version = previous_version + 1;
        if from_version > expected_from_version {
            return DeltaReceiveStatus::Missing {
                from,
                key,
                current_version: previous_version,
                expected_from_version,
                from_version,
                to_version,
            };
        }

        let changed = state.write_delta(key.clone(), delta);
        self.versions
            .insert((from.clone(), key.clone()), to_version);
        DeltaReceiveStatus::Applied {
            from,
            key,
            previous_version,
            to_version,
            changed,
        }
    }
}

fn wall_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or(std::time::Duration::ZERO)
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}
