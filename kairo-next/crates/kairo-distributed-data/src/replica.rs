use kairo_cluster::UniqueAddress;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReplicaId(String);

impl ReplicaId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&UniqueAddress> for ReplicaId {
    fn from(value: &UniqueAddress) -> Self {
        Self(value.ordering_key())
    }
}

impl From<UniqueAddress> for ReplicaId {
    fn from(value: UniqueAddress) -> Self {
        Self(value.ordering_key())
    }
}
