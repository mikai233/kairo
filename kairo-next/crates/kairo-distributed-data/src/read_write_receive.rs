use crate::{
    CrdtDataCodec, DeltaReplicatedData, ReplicaId, ReplicatorKey, ReplicatorRead,
    ReplicatorReadResult, ReplicatorState, ReplicatorWrite, ReplicatorWriteAck,
    ReplicatorWriteNack,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DirectWriteResult {
    Ack {
        key: ReplicatorKey,
        from: Option<ReplicaId>,
        changed: bool,
        message: ReplicatorWriteAck,
    },
    Nack {
        key: ReplicatorKey,
        from: Option<ReplicaId>,
        reason: String,
        message: ReplicatorWriteNack,
    },
}

impl DirectWriteResult {
    pub fn key(&self) -> &ReplicatorKey {
        match self {
            Self::Ack { key, .. } | Self::Nack { key, .. } => key,
        }
    }

    pub fn from(&self) -> Option<&ReplicaId> {
        match self {
            Self::Ack { from, .. } | Self::Nack { from, .. } => from.as_ref(),
        }
    }

    pub fn is_ack(&self) -> bool {
        matches!(self, Self::Ack { .. })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectReadResult {
    key: ReplicatorKey,
    from: Option<ReplicaId>,
    message: ReplicatorReadResult,
}

impl DirectReadResult {
    pub fn new(key: ReplicatorKey, from: Option<ReplicaId>, message: ReplicatorReadResult) -> Self {
        Self { key, from, message }
    }

    pub fn key(&self) -> &ReplicatorKey {
        &self.key
    }

    pub fn from(&self) -> Option<&ReplicaId> {
        self.from.as_ref()
    }

    pub fn message(&self) -> &ReplicatorReadResult {
        &self.message
    }

    pub fn into_message(self) -> ReplicatorReadResult {
        self.message
    }
}

pub fn apply_write<D, Codec>(
    state: &mut ReplicatorState<D>,
    write: &ReplicatorWrite,
    codec: &Codec,
) -> DirectWriteResult
where
    D: DeltaReplicatedData,
    Codec: CrdtDataCodec<D> + ?Sized,
{
    let key = ReplicatorKey::new(write.key.clone());
    let from = write.from.clone();
    match crate::decode_data_envelope(&write.envelope, codec) {
        Ok(envelope) => {
            let changed = state.write_full(key.clone(), envelope);
            DirectWriteResult::Ack {
                key,
                from,
                changed,
                message: ReplicatorWriteAck,
            }
        }
        Err(error) => DirectWriteResult::Nack {
            key,
            from,
            reason: error.to_string(),
            message: ReplicatorWriteNack,
        },
    }
}

pub fn serve_read<D, Codec>(
    state: &ReplicatorState<D>,
    read: &ReplicatorRead,
    codec: &Codec,
) -> kairo_serialization::Result<DirectReadResult>
where
    D: DeltaReplicatedData,
    Codec: CrdtDataCodec<D> + ?Sized,
{
    let key = ReplicatorKey::new(read.key.clone());
    let envelope = state.envelope(&key);
    let message = crate::encode_read_result(envelope, codec)?;
    Ok(DirectReadResult::new(key, read.from.clone(), message))
}
