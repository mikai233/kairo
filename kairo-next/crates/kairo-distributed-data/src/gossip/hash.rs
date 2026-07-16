use crate::{
    CrdtDataCodec, DataEnvelope, DeltaReplicatedData, ReplicaId, ReplicatorKey,
    ReplicatorPruningState, encode_data_envelope,
};

use super::ReplicatorGossipError;

/// Reserved digest meaning that the status sender does not hold the key.
///
/// Real envelope digests are normalized away from this value.
pub const REPLICATOR_GOSSIP_NOT_FOUND_DIGEST: u64 = 0;

/// Computes the deterministic non-zero digest for a full CRDT envelope.
///
/// The fixed FNV-1a digest covers stable CRDT manifest/version/payload bytes and
/// ordered pruning metadata. It is a difference detector, not a cryptographic
/// integrity proof.
pub fn digest_envelope<D>(
    envelope: &DataEnvelope<D>,
    codec: &dyn CrdtDataCodec<D>,
) -> Result<u64, ReplicatorGossipError>
where
    D: DeltaReplicatedData,
{
    let envelope = encode_data_envelope(envelope, codec)?;
    let mut hash = FNV_OFFSET_BASIS;
    hash = stable_hash_field(hash, envelope.crdt_manifest.as_bytes());
    hash = stable_hash_bytes(hash, &envelope.crdt_version.to_be_bytes());
    hash = stable_hash_field(hash, envelope.payload.as_ref());
    for pruning in &envelope.pruning {
        hash = stable_hash_replica_id(hash, &pruning.removed);
        hash = stable_hash_pruning_state(hash, &pruning.state);
    }
    Ok(non_zero_digest(hash))
}

pub(super) fn ensure_valid_chunk(
    chunk: u32,
    total_chunks: u32,
) -> Result<(), ReplicatorGossipError> {
    if total_chunks == 0 || chunk >= total_chunks {
        Err(ReplicatorGossipError::InvalidChunk {
            chunk,
            total_chunks,
        })
    } else {
        Ok(())
    }
}

pub(super) fn key_belongs_to_chunk(key: &ReplicatorKey, chunk: u32, total_chunks: u32) -> bool {
    total_chunks == 1 || stable_hash_key(key) % u64::from(total_chunks) == u64::from(chunk)
}

fn stable_hash_key(key: &ReplicatorKey) -> u64 {
    non_zero_digest(stable_hash_bytes(FNV_OFFSET_BASIS, key.as_str().as_bytes()))
}

fn stable_hash_pruning_state(hash: u64, state: &ReplicatorPruningState) -> u64 {
    match state {
        ReplicatorPruningState::Initialized { owner, seen } => {
            let mut hash = stable_hash_bytes(hash, &[1]);
            hash = stable_hash_replica_id(hash, owner);
            hash = stable_hash_bytes(hash, &(seen.len() as u64).to_be_bytes());
            for replica_id in seen {
                hash = stable_hash_replica_id(hash, replica_id);
            }
            hash
        }
        ReplicatorPruningState::Performed { obsolete_at_millis } => {
            let hash = stable_hash_bytes(hash, &[2]);
            stable_hash_bytes(hash, &obsolete_at_millis.to_be_bytes())
        }
    }
}

fn stable_hash_replica_id(hash: u64, replica_id: &ReplicaId) -> u64 {
    stable_hash_field(hash, replica_id.as_str().as_bytes())
}

const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;

fn stable_hash_bytes(mut hash: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

fn stable_hash_field(hash: u64, bytes: &[u8]) -> u64 {
    let hash = stable_hash_bytes(hash, &(bytes.len() as u64).to_be_bytes());
    stable_hash_bytes(hash, bytes)
}

fn non_zero_digest(hash: u64) -> u64 {
    if hash == REPLICATOR_GOSSIP_NOT_FOUND_DIGEST {
        1
    } else {
        hash
    }
}
