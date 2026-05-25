use std::time::Duration;

use crate::ConsistencyError;

const DEFAULT_MAJORITY_MIN_CAP: usize = 0;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadConsistency {
    Local,
    From {
        replicas: usize,
        timeout: Duration,
    },
    Majority {
        timeout: Duration,
        min_cap: usize,
    },
    MajorityPlus {
        timeout: Duration,
        additional: usize,
        min_cap: usize,
    },
    All {
        timeout: Duration,
    },
}

impl ReadConsistency {
    pub fn local() -> Self {
        Self::Local
    }

    pub fn from(replicas: usize, timeout: Duration) -> Result<Self, ConsistencyError> {
        ensure_multi_replica(replicas)?;
        Ok(Self::From { replicas, timeout })
    }

    pub fn majority(timeout: Duration) -> Self {
        Self::Majority {
            timeout,
            min_cap: DEFAULT_MAJORITY_MIN_CAP,
        }
    }

    pub fn majority_with_min_cap(timeout: Duration, min_cap: usize) -> Self {
        Self::Majority { timeout, min_cap }
    }

    pub fn majority_plus(timeout: Duration, additional: usize) -> Self {
        Self::MajorityPlus {
            timeout,
            additional,
            min_cap: DEFAULT_MAJORITY_MIN_CAP,
        }
    }

    pub fn all(timeout: Duration) -> Self {
        Self::All { timeout }
    }

    pub fn is_local(&self, remote_replica_count: usize) -> bool {
        matches!(self, Self::Local)
            || (remote_replica_count == 0
                && matches!(self, Self::Majority { .. } | Self::All { .. }))
    }

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
pub enum WriteConsistency {
    Local,
    To {
        replicas: usize,
        timeout: Duration,
    },
    Majority {
        timeout: Duration,
        min_cap: usize,
    },
    MajorityPlus {
        timeout: Duration,
        additional: usize,
        min_cap: usize,
    },
    All {
        timeout: Duration,
    },
}

impl WriteConsistency {
    pub fn local() -> Self {
        Self::Local
    }

    pub fn to(replicas: usize, timeout: Duration) -> Result<Self, ConsistencyError> {
        ensure_multi_replica(replicas)?;
        Ok(Self::To { replicas, timeout })
    }

    pub fn majority(timeout: Duration) -> Self {
        Self::Majority {
            timeout,
            min_cap: DEFAULT_MAJORITY_MIN_CAP,
        }
    }

    pub fn majority_with_min_cap(timeout: Duration, min_cap: usize) -> Self {
        Self::Majority { timeout, min_cap }
    }

    pub fn majority_plus(timeout: Duration, additional: usize) -> Self {
        Self::MajorityPlus {
            timeout,
            additional,
            min_cap: DEFAULT_MAJORITY_MIN_CAP,
        }
    }

    pub fn all(timeout: Duration) -> Self {
        Self::All { timeout }
    }

    pub fn is_local(&self, remote_replica_count: usize) -> bool {
        matches!(self, Self::Local)
            || (remote_replica_count == 0
                && matches!(self, Self::Majority { .. } | Self::All { .. }))
    }

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
