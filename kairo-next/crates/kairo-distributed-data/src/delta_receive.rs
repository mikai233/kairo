use std::collections::BTreeMap;

use crate::{DeltaReplicatedData, ReplicaId, ReplicatorKey, ReplicatorState};

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
