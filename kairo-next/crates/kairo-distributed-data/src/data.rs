pub trait ReplicatedData: Clone + Eq {
    fn merge(&self, other: &Self) -> Self;
}

pub trait DeltaReplicatedData: ReplicatedData {
    type Delta: ReplicatedDelta<Full = Self>;

    fn delta(&self) -> Option<Self::Delta>;

    fn merge_delta(&self, delta: &Self::Delta) -> Self;

    fn reset_delta(&self) -> Self;
}

pub trait ReplicatedDelta: ReplicatedData {
    type Full: DeltaReplicatedData<Delta = Self>;

    fn zero(&self) -> Self::Full;
}
