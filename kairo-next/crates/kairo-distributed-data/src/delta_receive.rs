#![deny(missing_docs)]
//! Causal delta propagation receive tracking and reply decisions.

use std::collections::BTreeMap;

use crate::{
    CrdtDataCodec, DeltaReplicatedData, PruningTable, RemovedNodePruning, ReplicaId,
    ReplicatorDeltaAck, ReplicatorDeltaNack, ReplicatorDeltaPropagation, ReplicatorKey,
    ReplicatorState, decode_delta,
};

#[derive(Debug, Clone, PartialEq, Eq)]
/// Result of applying one delta range from a remote replica.
pub enum DeltaReceiveStatus {
    /// The range followed the tracked causal version and was applied.
    Applied {
        /// Replica that sent the range.
        from: ReplicaId,
        /// Key affected by the range.
        key: ReplicatorKey,
        /// Previously tracked version for this replica and key.
        previous_version: u64,
        /// New tracked version after applying the range.
        to_version: u64,
        /// Whether applying the range changed data or pruning metadata.
        changed: bool,
    },
    /// The complete range was already reflected in the tracked version.
    AlreadyHandled {
        /// Replica that sent the range.
        from: ReplicaId,
        /// Key addressed by the range.
        key: ReplicatorKey,
        /// Version already tracked for this replica and key.
        current_version: u64,
        /// Last version carried by the duplicate range.
        to_version: u64,
    },
    /// One or more earlier causal ranges are missing.
    Missing {
        /// Replica that sent the range.
        from: ReplicaId,
        /// Key addressed by the range.
        key: ReplicatorKey,
        /// Version currently tracked for this replica and key.
        current_version: u64,
        /// Next version required for causal application.
        expected_from_version: u64,
        /// First version carried by the received range.
        from_version: u64,
        /// Last version carried by the received range.
        to_version: u64,
    },
    /// The range begins at zero or ends before it begins.
    InvalidRange {
        /// Replica that sent the range.
        from: ReplicaId,
        /// Key addressed by the range.
        key: ReplicatorKey,
        /// First version carried by the invalid range.
        from_version: u64,
        /// Last version carried by the invalid range.
        to_version: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Aggregate result for all delta ranges in one propagation message.
pub struct DeltaPropagationReceiveReport {
    from: ReplicaId,
    reply_requested: bool,
    ignored_removed_source: bool,
    statuses: Vec<DeltaReceiveStatus>,
    failures: Vec<DeltaReceiveFailure>,
}

impl DeltaPropagationReceiveReport {
    /// Returns the replica that sent the propagation.
    pub fn from(&self) -> &ReplicaId {
        &self.from
    }

    /// Returns whether the sender requested an acknowledgement.
    pub fn reply_requested(&self) -> bool {
        self.reply_requested
    }

    /// Returns whether the whole propagation was ignored because its source
    /// is removed globally or in an affected key's pruning metadata.
    pub fn ignored_removed_source(&self) -> bool {
        self.ignored_removed_source
    }

    /// Returns the per-range receive statuses.
    pub fn statuses(&self) -> &[DeltaReceiveStatus] {
        &self.statuses
    }

    /// Returns failures that prevented individual ranges from being decoded.
    pub fn failures(&self) -> &[DeltaReceiveFailure] {
        &self.failures
    }

    /// Returns whether every range was applied or had already been handled.
    pub fn is_success(&self) -> bool {
        !self.ignored_removed_source
            && self.failures.is_empty()
            && self.statuses.iter().all(|status| {
                matches!(
                    status,
                    DeltaReceiveStatus::Applied { .. } | DeltaReceiveStatus::AlreadyHandled { .. }
                )
            })
    }

    /// Returns the requested ACK or NACK, or no reply when replies were not
    /// requested or the removed source was deliberately ignored.
    pub fn reply(&self) -> Option<DeltaReceiveReply> {
        if !self.reply_requested || self.ignored_removed_source {
            return None;
        }

        if self.is_success() {
            Some(DeltaReceiveReply::Ack(ReplicatorDeltaAck))
        } else {
            Some(DeltaReceiveReply::Nack(ReplicatorDeltaNack))
        }
    }

    pub(crate) fn ignored(from: ReplicaId, reply_requested: bool) -> Self {
        Self {
            from,
            reply_requested,
            ignored_removed_source: true,
            statuses: Vec::new(),
            failures: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Failure to receive one encoded delta range.
pub enum DeltaReceiveFailure {
    /// The range payload or its pruning metadata could not be decoded.
    DecodeFailed {
        /// Stable key carried by the wire range.
        key: String,
        /// Human-readable codec failure.
        reason: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Protocol reply derived from a delta propagation receive report.
pub enum DeltaReceiveReply {
    /// All ranges were applied or had already been handled.
    Ack(ReplicatorDeltaAck),
    /// At least one range was missing, invalid, or undecodable.
    Nack(ReplicatorDeltaNack),
}

#[derive(Debug, Clone, Default)]
/// Tracks the last causally applied version per source replica and key.
pub struct DeltaReceiveTracker {
    versions: BTreeMap<(ReplicaId, ReplicatorKey), u64>,
}

impl DeltaReceiveTracker {
    /// Creates an empty receive tracker.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the last causally applied version for `from` and `key`.
    pub fn current_version(&self, from: &ReplicaId, key: &ReplicatorKey) -> u64 {
        self.versions
            .get(&(from.clone(), key.clone()))
            .copied()
            .unwrap_or_default()
    }

    /// Clears every tracked key for a source replica that left the cluster.
    pub fn clear_from(&mut self, from: &ReplicaId) {
        self.versions.retain(|(node, _), _| node != from);
    }

    /// Clears receive versions for a deleted key across every source replica.
    pub fn forget_key(&mut self, key: &ReplicatorKey) {
        self.versions
            .retain(|(_, existing_key), _| existing_key != key);
    }

    /// Decodes and causally applies every range in a propagation.
    ///
    /// Pruning metadata carried beside each range is merged before its delta
    /// is applied. Callers responsible for cluster membership must reject
    /// propagations from removed sources before invoking this method.
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
            ignored_removed_source: false,
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

    /// Applies one already-decoded causal range without wire pruning metadata.
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
