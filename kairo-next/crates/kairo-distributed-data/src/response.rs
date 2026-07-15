#![deny(missing_docs)]

//! Typed results and subscription notifications produced by a replicator.
//!
//! Read results distinguish a successfully observed absence from a failed
//! consistency requirement. Write timeouts do not roll back the local update:
//! the value may already have reached some peers and remains eligible for later
//! dissemination.

use crate::{ReplicatedData, ReplicatorKey};

/// Result of reading one replicated-data key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GetResponse<D> {
    /// The consistency requirement completed with a replicated value.
    Success {
        /// Key that was read.
        key: ReplicatorKey,
        /// Merged value observed by the read.
        data: D,
    },
    /// The consistency requirement completed and no value was found.
    NotFound {
        /// Key that was read.
        key: ReplicatorKey,
    },
    /// The read could not satisfy its consistency requirement.
    Failure {
        /// Key that was read.
        key: ReplicatorKey,
        /// Diagnostic description of the failed read.
        reason: String,
    },
}

impl<D> GetResponse<D> {
    /// Returns the key associated with this result.
    pub fn key(&self) -> &ReplicatorKey {
        match self {
            Self::Success { key, .. } | Self::NotFound { key } | Self::Failure { key, .. } => key,
        }
    }

    /// Returns the replicated value only for [`Self::Success`].
    pub fn data(&self) -> Option<&D> {
        match self {
            Self::Success { data, .. } => Some(data),
            Self::NotFound { .. } | Self::Failure { .. } => None,
        }
    }
}

/// Local state change and optional propagation delta produced by an update.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateOutcome<Delta> {
    key: ReplicatorKey,
    changed: bool,
    delta: Option<Delta>,
}

impl<Delta> UpdateOutcome<Delta> {
    /// Creates an update outcome.
    ///
    /// `changed` records whether the merged local envelope changed. `delta` is
    /// the update's optional CRDT delta for direct propagation; the stored full
    /// state has its transient delta reset.
    pub fn new(key: ReplicatorKey, changed: bool, delta: Option<Delta>) -> Self {
        Self {
            key,
            changed,
            delta,
        }
    }

    /// Returns the updated key.
    pub fn key(&self) -> &ReplicatorKey {
        &self.key
    }

    /// Reports whether the merged local envelope changed.
    pub fn changed(&self) -> bool {
        self.changed
    }

    /// Borrows the CRDT delta emitted by the update, when one was produced.
    pub fn delta(&self) -> Option<&Delta> {
        self.delta.as_ref()
    }

    /// Consumes the outcome and returns its optional CRDT delta.
    ///
    /// The key and changed flag are discarded.
    pub fn into_delta(self) -> Option<Delta> {
        self.delta
    }
}

/// Result of applying and, when requested, directly replicating an update.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpdateResponse<Delta> {
    /// The update satisfied its write-consistency requirement.
    Success(UpdateOutcome<Delta>),
    /// Direct replication did not satisfy the requirement before its deadline.
    ///
    /// The update was applied locally and may have reached some remote replicas.
    /// Normal dissemination can still converge it later.
    Timeout {
        /// Key whose direct replication timed out.
        key: ReplicatorKey,
    },
    /// The update function rejected the value before local state was changed.
    ModifyFailure {
        /// Key whose update function failed.
        key: ReplicatorKey,
        /// Diagnostic returned by the update function.
        reason: String,
    },
    /// A non-timeout update or replication step failed.
    Failure {
        /// Key whose update failed.
        key: ReplicatorKey,
        /// Diagnostic description of the failure.
        reason: String,
    },
}

impl<Delta> UpdateResponse<Delta> {
    /// Returns the key associated with this result.
    pub fn key(&self) -> &ReplicatorKey {
        match self {
            Self::Success(outcome) => outcome.key(),
            Self::Timeout { key } | Self::ModifyFailure { key, .. } | Self::Failure { key, .. } => {
                key
            }
        }
    }
}

/// Current value delivered on subscription or after coalesced key changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorChange<D> {
    key: ReplicatorKey,
    data: D,
}

impl<D> ReplicatorChange<D>
where
    D: ReplicatedData,
{
    /// Creates a subscription notification for `key` and its current `data`.
    pub fn new(key: ReplicatorKey, data: D) -> Self {
        Self { key, data }
    }

    /// Returns the changed key.
    pub fn key(&self) -> &ReplicatorKey {
        &self.key
    }

    /// Returns the current replicated value.
    pub fn data(&self) -> &D {
        &self.data
    }

    /// Consumes the notification and returns the replicated value.
    pub fn into_data(self) -> D {
        self.data
    }
}
