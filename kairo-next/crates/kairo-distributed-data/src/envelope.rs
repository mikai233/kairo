use crate::{DeltaReplicatedData, ReplicatedData};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataEnvelope<D> {
    data: D,
}

impl<D> DataEnvelope<D>
where
    D: ReplicatedData,
{
    pub fn new(data: D) -> Self {
        Self { data }
    }

    pub fn data(&self) -> &D {
        &self.data
    }

    pub fn into_data(self) -> D {
        self.data
    }

    pub fn merge(&self, other: &Self) -> Self {
        Self::new(self.data.merge(&other.data))
    }

    pub fn merge_data(&self, other: &D) -> Self {
        Self::new(self.data.merge(other))
    }
}

impl<D> DataEnvelope<D>
where
    D: DeltaReplicatedData,
{
    pub fn merge_delta(&self, delta: &D::Delta) -> Self {
        Self::new(self.data.merge_delta(delta))
    }
}
