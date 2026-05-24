use crate::{ReplicatedData, ReplicatorKey};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GetResponse<D> {
    Success { key: ReplicatorKey, data: D },
    NotFound { key: ReplicatorKey },
    Failure { key: ReplicatorKey, reason: String },
}

impl<D> GetResponse<D> {
    pub fn key(&self) -> &ReplicatorKey {
        match self {
            Self::Success { key, .. } | Self::NotFound { key } | Self::Failure { key, .. } => key,
        }
    }

    pub fn data(&self) -> Option<&D> {
        match self {
            Self::Success { data, .. } => Some(data),
            Self::NotFound { .. } | Self::Failure { .. } => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateOutcome<Delta> {
    key: ReplicatorKey,
    changed: bool,
    delta: Option<Delta>,
}

impl<Delta> UpdateOutcome<Delta> {
    pub fn new(key: ReplicatorKey, changed: bool, delta: Option<Delta>) -> Self {
        Self {
            key,
            changed,
            delta,
        }
    }

    pub fn key(&self) -> &ReplicatorKey {
        &self.key
    }

    pub fn changed(&self) -> bool {
        self.changed
    }

    pub fn delta(&self) -> Option<&Delta> {
        self.delta.as_ref()
    }

    pub fn into_delta(self) -> Option<Delta> {
        self.delta
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpdateResponse<Delta> {
    Success(UpdateOutcome<Delta>),
    Timeout { key: ReplicatorKey },
    ModifyFailure { key: ReplicatorKey, reason: String },
    Failure { key: ReplicatorKey, reason: String },
}

impl<Delta> UpdateResponse<Delta> {
    pub fn key(&self) -> &ReplicatorKey {
        match self {
            Self::Success(outcome) => outcome.key(),
            Self::Timeout { key } | Self::ModifyFailure { key, .. } | Self::Failure { key, .. } => {
                key
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorChange<D> {
    key: ReplicatorKey,
    data: D,
}

impl<D> ReplicatorChange<D>
where
    D: ReplicatedData,
{
    pub fn new(key: ReplicatorKey, data: D) -> Self {
        Self { key, data }
    }

    pub fn key(&self) -> &ReplicatorKey {
        &self.key
    }

    pub fn data(&self) -> &D {
        &self.data
    }

    pub fn into_data(self) -> D {
        self.data
    }
}
