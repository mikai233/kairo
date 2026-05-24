use kairo_serialization::SerializationError;

use crate::{
    CrdtDataCodec, DataEnvelope, ReplicaId, ReplicatedData, ReplicatorDataEnvelope, ReplicatorKey,
    ReplicatorRead, ReplicatorReadResult, ReplicatorWrite,
};

pub fn encode_data_envelope<D, Codec>(
    envelope: &DataEnvelope<D>,
    codec: &Codec,
) -> kairo_serialization::Result<ReplicatorDataEnvelope>
where
    D: ReplicatedData,
    Codec: CrdtDataCodec<D> + ?Sized,
{
    Ok(ReplicatorDataEnvelope::new(
        codec.serialize(envelope.data())?,
    ))
}

pub fn decode_data_envelope<D, Codec>(
    envelope: &ReplicatorDataEnvelope,
    codec: &Codec,
) -> kairo_serialization::Result<DataEnvelope<D>>
where
    D: ReplicatedData,
    Codec: CrdtDataCodec<D> + ?Sized,
{
    if envelope.crdt_manifest != codec.manifest() {
        return Err(SerializationError::Message(format!(
            "expected CRDT manifest {}, got {}",
            codec.manifest(),
            envelope.crdt_manifest
        )));
    }

    Ok(DataEnvelope::new(codec.decode_payload(
        envelope.payload.clone(),
        envelope.crdt_version,
    )?))
}

pub fn encode_write<D, Codec>(
    key: &ReplicatorKey,
    from: Option<ReplicaId>,
    envelope: &DataEnvelope<D>,
    codec: &Codec,
) -> kairo_serialization::Result<ReplicatorWrite>
where
    D: ReplicatedData,
    Codec: CrdtDataCodec<D> + ?Sized,
{
    Ok(ReplicatorWrite {
        key: key.as_str().to_string(),
        from,
        envelope: encode_data_envelope(envelope, codec)?,
    })
}

pub fn encode_read(key: &ReplicatorKey, from: Option<ReplicaId>) -> ReplicatorRead {
    ReplicatorRead {
        key: key.as_str().to_string(),
        from,
    }
}

pub fn encode_read_result<D, Codec>(
    envelope: Option<&DataEnvelope<D>>,
    codec: &Codec,
) -> kairo_serialization::Result<ReplicatorReadResult>
where
    D: ReplicatedData,
    Codec: CrdtDataCodec<D> + ?Sized,
{
    Ok(ReplicatorReadResult {
        envelope: envelope
            .map(|envelope| encode_data_envelope(envelope, codec))
            .transpose()?,
    })
}

pub fn decode_read_result<D, Codec>(
    result: &ReplicatorReadResult,
    codec: &Codec,
) -> kairo_serialization::Result<Option<DataEnvelope<D>>>
where
    D: ReplicatedData,
    Codec: CrdtDataCodec<D> + ?Sized,
{
    result
        .envelope
        .as_ref()
        .map(|envelope| decode_data_envelope(envelope, codec))
        .transpose()
}
