#![deny(missing_docs)]

//! Read and write acknowledgement requirements for typed replicator operations.
//!
//! Replica counts include the local replica. Majority is `floor(n / 2) + 1`,
//! optionally raised by a minimum cap or additional acknowledgements, and is
//! always capped at the current total replica count.

use std::time::Duration;

use crate::ConsistencyError;

const DEFAULT_MAJORITY_MIN_CAP: usize = 0;

#[derive(Debug, Clone, PartialEq, Eq)]
/// Consistency requirement for a replicated-data read.
pub enum ReadConsistency {
    /// Reads only the local replica without a timeout.
    Local,
    /// Requires results from an explicit total number of replicas.
    From {
        /// Required replicas including the local replica; must be at least two.
        replicas: usize,
        /// Maximum time allowed for remote results.
        timeout: Duration,
    },
    /// Requires a strict majority, optionally raised by `min_cap`.
    Majority {
        /// Maximum time allowed for remote results.
        timeout: Duration,
        /// Minimum total result count before capping at available replicas.
        min_cap: usize,
    },
    /// Requires a majority plus extra results, capped at all replicas.
    MajorityPlus {
        /// Maximum time allowed for remote results.
        timeout: Duration,
        /// Extra results requested beyond the strict majority.
        additional: usize,
        /// Minimum total result count before capping at available replicas.
        min_cap: usize,
    },
    /// Requires results from every current replica.
    All {
        /// Maximum time allowed for remote results.
        timeout: Duration,
    },
}

impl ReadConsistency {
    /// Creates a local-only read requirement.
    pub fn local() -> Self {
        Self::Local
    }

    /// Creates an explicit total-replica read requirement.
    ///
    /// Returns [`ConsistencyError::ReplicaCountTooSmall`] below two; use
    /// [`Self::local`] for one replica.
    pub fn from(replicas: usize, timeout: Duration) -> Result<Self, ConsistencyError> {
        ensure_multi_replica(replicas)?;
        Ok(Self::From { replicas, timeout })
    }

    /// Creates a strict-majority read requirement with no minimum cap.
    pub fn majority(timeout: Duration) -> Self {
        Self::Majority {
            timeout,
            min_cap: DEFAULT_MAJORITY_MIN_CAP,
        }
    }

    /// Creates a majority read requirement raised by `min_cap` when possible.
    pub fn majority_with_min_cap(timeout: Duration, min_cap: usize) -> Self {
        Self::Majority { timeout, min_cap }
    }

    /// Creates a majority-plus read requirement with no minimum cap.
    pub fn majority_plus(timeout: Duration, additional: usize) -> Self {
        Self::MajorityPlus {
            timeout,
            additional,
            min_cap: DEFAULT_MAJORITY_MIN_CAP,
        }
    }

    /// Creates a majority-plus read with an explicit minimum cap.
    pub fn majority_plus_with_min_cap(
        timeout: Duration,
        additional: usize,
        min_cap: usize,
    ) -> Self {
        Self::MajorityPlus {
            timeout,
            additional,
            min_cap,
        }
    }

    /// Creates a read requirement for every current replica.
    pub fn all(timeout: Duration) -> Self {
        Self::All { timeout }
    }

    /// Reports whether this requirement can complete from local state alone.
    ///
    /// Majority, majority-plus, and all collapse to local when there are no
    /// remote replicas because their required total is capped at one.
    pub fn is_local(&self, remote_replica_count: usize) -> bool {
        matches!(self, Self::Local)
            || (remote_replica_count == 0
                && matches!(
                    self,
                    Self::Majority { .. } | Self::MajorityPlus { .. } | Self::All { .. }
                ))
    }

    /// Returns the remote-operation timeout, or `None` for local consistency.
    pub fn timeout(&self) -> Option<Duration> {
        match self {
            Self::Local => None,
            Self::From { timeout, .. }
            | Self::Majority { timeout, .. }
            | Self::MajorityPlus { timeout, .. }
            | Self::All { timeout } => Some(*timeout),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Consistency requirement for a replicated-data write.
pub enum WriteConsistency {
    /// Updates only local state without waiting for remote acknowledgement.
    Local,
    /// Requires acknowledgement from an explicit total number of replicas.
    To {
        /// Required replicas including the local replica; must be at least two.
        replicas: usize,
        /// Maximum time allowed for remote acknowledgements.
        timeout: Duration,
    },
    /// Requires acknowledgement from a strict majority, raised by `min_cap`.
    Majority {
        /// Maximum time allowed for remote acknowledgements.
        timeout: Duration,
        /// Minimum total acknowledgement count before capping at all replicas.
        min_cap: usize,
    },
    /// Requires a majority plus extra acknowledgements, capped at all replicas.
    MajorityPlus {
        /// Maximum time allowed for remote acknowledgements.
        timeout: Duration,
        /// Extra acknowledgements requested beyond the strict majority.
        additional: usize,
        /// Minimum total acknowledgement count before capping at all replicas.
        min_cap: usize,
    },
    /// Requires acknowledgement from every current replica.
    All {
        /// Maximum time allowed for remote acknowledgements.
        timeout: Duration,
    },
}

impl WriteConsistency {
    /// Creates a local-only write requirement.
    pub fn local() -> Self {
        Self::Local
    }

    /// Creates an explicit total-replica write requirement.
    ///
    /// Returns [`ConsistencyError::ReplicaCountTooSmall`] below two; use
    /// [`Self::local`] for one replica.
    pub fn to(replicas: usize, timeout: Duration) -> Result<Self, ConsistencyError> {
        ensure_multi_replica(replicas)?;
        Ok(Self::To { replicas, timeout })
    }

    /// Creates a strict-majority write requirement with no minimum cap.
    pub fn majority(timeout: Duration) -> Self {
        Self::Majority {
            timeout,
            min_cap: DEFAULT_MAJORITY_MIN_CAP,
        }
    }

    /// Creates a majority write requirement raised by `min_cap` when possible.
    pub fn majority_with_min_cap(timeout: Duration, min_cap: usize) -> Self {
        Self::Majority { timeout, min_cap }
    }

    /// Creates a majority-plus write requirement with no minimum cap.
    pub fn majority_plus(timeout: Duration, additional: usize) -> Self {
        Self::MajorityPlus {
            timeout,
            additional,
            min_cap: DEFAULT_MAJORITY_MIN_CAP,
        }
    }

    /// Creates a majority-plus write with an explicit minimum cap.
    pub fn majority_plus_with_min_cap(
        timeout: Duration,
        additional: usize,
        min_cap: usize,
    ) -> Self {
        Self::MajorityPlus {
            timeout,
            additional,
            min_cap,
        }
    }

    /// Creates a write requirement for every current replica.
    pub fn all(timeout: Duration) -> Self {
        Self::All { timeout }
    }

    /// Reports whether this requirement can complete after the local update.
    ///
    /// Majority, majority-plus, and all collapse to local when there are no
    /// remote replicas because their required total is capped at one.
    pub fn is_local(&self, remote_replica_count: usize) -> bool {
        matches!(self, Self::Local)
            || (remote_replica_count == 0
                && matches!(
                    self,
                    Self::Majority { .. } | Self::MajorityPlus { .. } | Self::All { .. }
                ))
    }

    /// Returns the remote-operation timeout, or `None` for local consistency.
    pub fn timeout(&self) -> Option<Duration> {
        match self {
            Self::Local => None,
            Self::To { timeout, .. }
            | Self::Majority { timeout, .. }
            | Self::MajorityPlus { timeout, .. }
            | Self::All { timeout } => Some(*timeout),
        }
    }
}

fn ensure_multi_replica(replicas: usize) -> Result<(), ConsistencyError> {
    if replicas >= 2 {
        Ok(())
    } else {
        Err(ConsistencyError::ReplicaCountTooSmall {
            requested: replicas,
        })
    }
}
