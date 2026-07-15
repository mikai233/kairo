#![deny(missing_docs)]

//! Direct peer read/write request handling for a replicator state.
//!
//! Writes decode a stable wire envelope and merge it with pruning-aware state
//! semantics before returning an acknowledgement. Reads encode the current
//! envelope, including explicit absence, while preserving request correlation.

use crate::{
    CrdtDataCodec, DeltaReplicatedData, RemovedNodePruning, ReplicaId, ReplicatorKey,
    ReplicatorRead, ReplicatorReadResult, ReplicatorState, ReplicatorWrite, ReplicatorWriteAck,
    ReplicatorWriteNack,
};

#[derive(Debug, Clone, PartialEq, Eq)]
/// Outcome of applying one direct peer write request.
pub enum DirectWriteResult {
    /// The wire envelope decoded and merged successfully.
    Ack {
        /// Logical key carried by the request.
        key: ReplicatorKey,
        /// Optional request source used to correlate the reply.
        from: Option<ReplicaId>,
        /// Whether the merge changed local replicated state or pruning metadata.
        changed: bool,
        /// Stable acknowledgement message for the remote reply boundary.
        message: ReplicatorWriteAck,
    },
    /// The wire envelope could not be decoded and local state was unchanged.
    Nack {
        /// Logical key carried by the request.
        key: ReplicatorKey,
        /// Optional request source used to correlate the reply.
        from: Option<ReplicaId>,
        /// Human-readable decoding failure.
        reason: String,
        /// Stable negative-acknowledgement message for the remote reply boundary.
        message: ReplicatorWriteNack,
    },
}

impl DirectWriteResult {
    /// Returns the logical key carried by the request.
    pub fn key(&self) -> &ReplicatorKey {
        match self {
            Self::Ack { key, .. } | Self::Nack { key, .. } => key,
        }
    }

    /// Returns the optional source replica used to correlate the reply.
    pub fn from(&self) -> Option<&ReplicaId> {
        match self {
            Self::Ack { from, .. } | Self::Nack { from, .. } => from.as_ref(),
        }
    }

    /// Reports whether the write decoded and merged successfully.
    pub fn is_ack(&self) -> bool {
        matches!(self, Self::Ack { .. })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Encoded result of serving one direct peer read request.
pub struct DirectReadResult {
    key: ReplicatorKey,
    from: Option<ReplicaId>,
    message: ReplicatorReadResult,
}

impl DirectReadResult {
    /// Creates a read result for `key` and its optional request source.
    pub fn new(key: ReplicatorKey, from: Option<ReplicaId>, message: ReplicatorReadResult) -> Self {
        Self { key, from, message }
    }

    /// Returns the requested logical key.
    pub fn key(&self) -> &ReplicatorKey {
        &self.key
    }

    /// Returns the optional source replica used to correlate the reply.
    pub fn from(&self) -> Option<&ReplicaId> {
        self.from.as_ref()
    }

    /// Borrows the stable wire read-result message.
    pub fn message(&self) -> &ReplicatorReadResult {
        &self.message
    }

    /// Consumes this result and returns its stable wire message.
    pub fn into_message(self) -> ReplicatorReadResult {
        self.message
    }
}

/// Decodes and pruning-aware merges one direct peer write.
///
/// A successful result acknowledges both state-changing and idempotent merges.
/// A decoding error returns a negative acknowledgement without mutating
/// `state`. This stateless helper cannot record the receiving replica on
/// initialized pruning markers; the composed [`crate::ReplicatorActor`] does so
/// when configured with [`crate::ReplicatorActor::with_self_replica`].
pub fn apply_write<D, Codec>(
    state: &mut ReplicatorState<D>,
    write: &ReplicatorWrite,
    codec: &Codec,
) -> DirectWriteResult
where
    D: DeltaReplicatedData + RemovedNodePruning,
    Codec: CrdtDataCodec<D> + ?Sized,
{
    apply_write_envelope(state, write, codec, None)
}

pub(crate) fn apply_write_with_seen<D, Codec>(
    state: &mut ReplicatorState<D>,
    write: &ReplicatorWrite,
    codec: &Codec,
    seen_by: &ReplicaId,
) -> DirectWriteResult
where
    D: DeltaReplicatedData + RemovedNodePruning,
    Codec: CrdtDataCodec<D> + ?Sized,
{
    apply_write_envelope(state, write, codec, Some(seen_by))
}

fn apply_write_envelope<D, Codec>(
    state: &mut ReplicatorState<D>,
    write: &ReplicatorWrite,
    codec: &Codec,
    seen_by: Option<&ReplicaId>,
) -> DirectWriteResult
where
    D: DeltaReplicatedData + RemovedNodePruning,
    Codec: CrdtDataCodec<D> + ?Sized,
{
    let key = ReplicatorKey::new(write.key.clone());
    let from = write.from.clone();
    match crate::decode_data_envelope(&write.envelope, codec) {
        Ok(envelope) => {
            let merged = state.write_full_pruned(key.clone(), envelope, wall_millis());
            let seen_changed =
                seen_by.is_some_and(|seen_by| state.mark_key_pruning_seen(&key, seen_by.clone()));
            DirectWriteResult::Ack {
                key,
                from,
                changed: merged || seen_changed,
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

fn wall_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or(std::time::Duration::ZERO)
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

/// Encodes the current envelope for one direct peer read.
///
/// Missing keys are represented by a successful wire result with no envelope;
/// codec failures are returned to the caller.
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
