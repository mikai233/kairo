use kairo_serialization::SerializationError;

use crate::{
    CrdtDataCodec, DeltaPropagation, ReplicaId, ReplicatedData, ReplicatorDelta,
    ReplicatorDeltaPropagation, ReplicatorKey,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedReplicatorDelta<Delta> {
    key: ReplicatorKey,
    delta: Delta,
    from_version: u64,
    to_version: u64,
}

impl<Delta> DecodedReplicatorDelta<Delta> {
    pub fn new(key: ReplicatorKey, delta: Delta, from_version: u64, to_version: u64) -> Self {
        Self {
            key,
            delta,
            from_version,
            to_version,
        }
    }

    pub fn key(&self) -> &ReplicatorKey {
        &self.key
    }

    pub fn delta(&self) -> &Delta {
        &self.delta
    }

    pub fn into_delta(self) -> Delta {
        self.delta
    }

    pub fn from_version(&self) -> u64 {
        self.from_version
    }

    pub fn to_version(&self) -> u64 {
        self.to_version
    }
}

pub fn encode_delta_propagation<Delta, Codec>(
    from: ReplicaId,
    reply: bool,
    propagation: &DeltaPropagation<Delta>,
    codec: &Codec,
) -> kairo_serialization::Result<ReplicatorDeltaPropagation>
where
    Delta: ReplicatedData,
    Codec: CrdtDataCodec<Delta>,
{
    let mut deltas = Vec::with_capacity(propagation.entries().len());
    for (key, entry) in propagation.entries() {
        let encoded = codec.serialize(entry.delta())?;
        deltas.push(ReplicatorDelta::new(
            key.as_str(),
            encoded,
            entry.from_version(),
            entry.to_version(),
        ));
    }
    Ok(ReplicatorDeltaPropagation {
        from,
        reply,
        deltas,
    })
}

pub fn decode_delta_propagation<Delta, Codec>(
    propagation: &ReplicatorDeltaPropagation,
    codec: &Codec,
) -> kairo_serialization::Result<Vec<DecodedReplicatorDelta<Delta>>>
where
    Codec: CrdtDataCodec<Delta> + ?Sized,
{
    propagation
        .deltas
        .iter()
        .map(|delta| decode_delta(delta, codec))
        .collect()
}

pub fn decode_delta<Delta, Codec>(
    delta: &ReplicatorDelta,
    codec: &Codec,
) -> kairo_serialization::Result<DecodedReplicatorDelta<Delta>>
where
    Codec: CrdtDataCodec<Delta> + ?Sized,
{
    if delta.crdt_manifest != codec.manifest() {
        return Err(SerializationError::Message(format!(
            "expected CRDT manifest {}, got {}",
            codec.manifest(),
            delta.crdt_manifest
        )));
    }

    Ok(DecodedReplicatorDelta::new(
        ReplicatorKey::new(delta.key.clone()),
        codec.decode_payload(delta.payload.clone(), delta.crdt_version)?,
        delta.from_version,
        delta.to_version,
    ))
}
