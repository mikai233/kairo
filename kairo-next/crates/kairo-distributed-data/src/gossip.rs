use std::collections::BTreeSet;
use std::fmt::{self, Display, Formatter};

use kairo_serialization::SerializationError;

use crate::{
    CrdtDataCodec, DataEnvelope, DeltaReplicatedData, ReplicaId, ReplicatorGossip,
    ReplicatorGossipDigest, ReplicatorGossipEntry, ReplicatorGossipStatus, ReplicatorKey,
    ReplicatorPruningState, ReplicatorState, decode_data_envelope, encode_data_envelope,
};

pub const REPLICATOR_GOSSIP_NOT_FOUND_DIGEST: u64 = 0;

#[derive(Debug)]
pub enum ReplicatorGossipError {
    Serialization(SerializationError),
    InvalidChunk { chunk: u32, total_chunks: u32 },
    ZeroMaxEntries,
}

impl Display for ReplicatorGossipError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Serialization(error) => {
                write!(f, "replicator gossip serialization failed: {error}")
            }
            Self::InvalidChunk {
                chunk,
                total_chunks,
            } => write!(
                f,
                "invalid replicator gossip chunk {chunk} for {total_chunks} total chunks"
            ),
            Self::ZeroMaxEntries => {
                write!(f, "replicator gossip max entries must be greater than zero")
            }
        }
    }
}

impl std::error::Error for ReplicatorGossipError {}

impl From<SerializationError> for ReplicatorGossipError {
    fn from(error: SerializationError) -> Self {
        Self::Serialization(error)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorGossipStatusPlan {
    gossip: Option<ReplicatorGossip>,
    missing_status: Option<ReplicatorGossipStatus>,
}

impl ReplicatorGossipStatusPlan {
    pub fn new(
        gossip: Option<ReplicatorGossip>,
        missing_status: Option<ReplicatorGossipStatus>,
    ) -> Self {
        Self {
            gossip,
            missing_status,
        }
    }

    pub fn gossip(&self) -> Option<&ReplicatorGossip> {
        self.gossip.as_ref()
    }

    pub fn missing_status(&self) -> Option<&ReplicatorGossipStatus> {
        self.missing_status.as_ref()
    }

    pub fn into_parts(self) -> (Option<ReplicatorGossip>, Option<ReplicatorGossipStatus>) {
        (self.gossip, self.missing_status)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorGossipApplyReport {
    changed_keys: BTreeSet<ReplicatorKey>,
    reply: Option<ReplicatorGossip>,
}

impl ReplicatorGossipApplyReport {
    pub fn new(changed_keys: BTreeSet<ReplicatorKey>, reply: Option<ReplicatorGossip>) -> Self {
        Self {
            changed_keys,
            reply,
        }
    }

    pub fn changed_keys(&self) -> &BTreeSet<ReplicatorKey> {
        &self.changed_keys
    }

    pub fn reply(&self) -> Option<&ReplicatorGossip> {
        self.reply.as_ref()
    }
}

pub fn build_gossip_status<D>(
    state: &ReplicatorState<D>,
    codec: &dyn CrdtDataCodec<D>,
    chunk: u32,
    total_chunks: u32,
    to_system_uid: Option<u64>,
    from_system_uid: Option<u64>,
) -> Result<ReplicatorGossipStatus, ReplicatorGossipError>
where
    D: DeltaReplicatedData,
{
    ensure_valid_chunk(chunk, total_chunks)?;
    let mut entries = Vec::new();
    for (key, envelope) in state.entries() {
        if key_belongs_to_chunk(key, chunk, total_chunks) {
            entries.push(ReplicatorGossipDigest {
                key: key.as_str().to_string(),
                digest: digest_envelope(envelope, codec)?,
                used_timestamp_millis: 0,
            });
        }
    }
    Ok(ReplicatorGossipStatus {
        entries,
        chunk,
        total_chunks,
        to_system_uid,
        from_system_uid,
    })
}

pub fn respond_to_gossip_status<D>(
    state: &ReplicatorState<D>,
    status: &ReplicatorGossipStatus,
    codec: &dyn CrdtDataCodec<D>,
    max_entries: usize,
) -> Result<ReplicatorGossipStatusPlan, ReplicatorGossipError>
where
    D: DeltaReplicatedData,
{
    ensure_valid_chunk(status.chunk, status.total_chunks)?;
    if max_entries == 0 {
        return Err(ReplicatorGossipError::ZeroMaxEntries);
    }

    let my_keys = state
        .keys()
        .filter(|key| key_belongs_to_chunk(key, status.chunk, status.total_chunks))
        .cloned()
        .collect::<BTreeSet<_>>();
    let other_keys = status
        .entries
        .iter()
        .map(|entry| ReplicatorKey::new(entry.key.clone()))
        .collect::<BTreeSet<_>>();

    let mut different = Vec::new();
    for entry in &status.entries {
        let key = ReplicatorKey::new(entry.key.clone());
        let Some(envelope) = state.envelope(&key) else {
            continue;
        };
        if entry.digest != REPLICATOR_GOSSIP_NOT_FOUND_DIGEST
            && digest_envelope(envelope, codec)? != entry.digest
        {
            different.push(key);
        }
    }

    let mut send_keys = different.clone();
    for key in my_keys.difference(&other_keys) {
        send_keys.push(key.clone());
    }
    send_keys.truncate(max_entries);

    let gossip = (!send_keys.is_empty())
        .then(|| {
            create_gossip(
                state,
                send_keys,
                !different.is_empty(),
                status.from_system_uid,
                status.to_system_uid,
                codec,
            )
        })
        .transpose()?;

    let missing_keys = other_keys.difference(&my_keys).cloned().collect::<Vec<_>>();
    let missing_status = (!missing_keys.is_empty()).then(|| ReplicatorGossipStatus {
        entries: missing_keys
            .into_iter()
            .map(|key| ReplicatorGossipDigest {
                key: key.as_str().to_string(),
                digest: REPLICATOR_GOSSIP_NOT_FOUND_DIGEST,
                used_timestamp_millis: 0,
            })
            .collect(),
        chunk: status.chunk,
        total_chunks: status.total_chunks,
        to_system_uid: status.from_system_uid,
        from_system_uid: status.to_system_uid,
    });

    Ok(ReplicatorGossipStatusPlan::new(gossip, missing_status))
}

pub fn apply_gossip<D>(
    state: &mut ReplicatorState<D>,
    gossip: &ReplicatorGossip,
    codec: &dyn CrdtDataCodec<D>,
) -> Result<ReplicatorGossipApplyReport, ReplicatorGossipError>
where
    D: DeltaReplicatedData,
{
    let mut changed_keys = BTreeSet::new();
    let mut reply_keys = Vec::new();

    for entry in &gossip.entries {
        let key = ReplicatorKey::new(entry.key.clone());
        let had_data = state.contains_key(&key);
        let envelope = decode_data_envelope(&entry.envelope, codec)?;
        let changed = state.write_full(key.clone(), envelope);
        if changed {
            changed_keys.insert(key.clone());
        }
        if gossip.send_back {
            let has_pruning = state
                .envelope(&key)
                .is_some_and(|envelope| !envelope.pruning().is_empty());
            if had_data || has_pruning {
                reply_keys.push(key);
            }
        }
    }

    let reply = (!reply_keys.is_empty())
        .then(|| {
            create_gossip(
                state,
                reply_keys,
                false,
                gossip.from_system_uid,
                gossip.to_system_uid,
                codec,
            )
        })
        .transpose()?;

    Ok(ReplicatorGossipApplyReport::new(changed_keys, reply))
}

pub fn create_gossip<D>(
    state: &ReplicatorState<D>,
    keys: impl IntoIterator<Item = ReplicatorKey>,
    send_back: bool,
    to_system_uid: Option<u64>,
    from_system_uid: Option<u64>,
    codec: &dyn CrdtDataCodec<D>,
) -> Result<ReplicatorGossip, ReplicatorGossipError>
where
    D: DeltaReplicatedData,
{
    let mut entries = Vec::new();
    for key in keys {
        if let Some(envelope) = state.envelope(&key) {
            entries.push(ReplicatorGossipEntry {
                key: key.as_str().to_string(),
                envelope: encode_data_envelope(envelope, codec)?,
                used_timestamp_millis: 0,
            });
        }
    }
    Ok(ReplicatorGossip {
        entries,
        send_back,
        to_system_uid,
        from_system_uid,
    })
}

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

fn ensure_valid_chunk(chunk: u32, total_chunks: u32) -> Result<(), ReplicatorGossipError> {
    if total_chunks == 0 || chunk >= total_chunks {
        Err(ReplicatorGossipError::InvalidChunk {
            chunk,
            total_chunks,
        })
    } else {
        Ok(())
    }
}

fn key_belongs_to_chunk(key: &ReplicatorKey, chunk: u32, total_chunks: u32) -> bool {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{GCounter, GCounterCodec, ReplicaId};

    fn replica(id: &str) -> ReplicaId {
        ReplicaId::new(id)
    }

    fn counter(replica_id: &str, value: u128) -> GCounter {
        GCounter::new()
            .increment(replica(replica_id), value)
            .unwrap()
            .reset_delta()
    }

    fn write_counter(state: &mut ReplicatorState<GCounter>, key: &str, value: u128) {
        state.write_full(
            ReplicatorKey::new(key),
            DataEnvelope::new(counter("local", value)),
        );
    }

    #[test]
    fn gossip_status_contains_stable_non_zero_digests() {
        let mut state = ReplicatorState::new();
        write_counter(&mut state, "a", 1);
        write_counter(&mut state, "b", 2);

        let status = build_gossip_status(&state, &GCounterCodec, 0, 1, Some(2), Some(1)).unwrap();

        assert_eq!(status.entries.len(), 2);
        assert_eq!(status.to_system_uid, Some(2));
        assert_eq!(status.from_system_uid, Some(1));
        assert!(
            status
                .entries
                .iter()
                .all(|entry| entry.digest != REPLICATOR_GOSSIP_NOT_FOUND_DIGEST)
        );
    }

    #[test]
    fn status_response_sends_different_and_missing_local_keys() {
        let mut local = ReplicatorState::new();
        write_counter(&mut local, "different", 10);
        write_counter(&mut local, "local-only", 20);

        let remote_envelope = DataEnvelope::new(counter("remote", 99));
        let remote_digest = digest_envelope(&remote_envelope, &GCounterCodec).unwrap();
        let status = ReplicatorGossipStatus {
            entries: vec![ReplicatorGossipDigest {
                key: "different".to_string(),
                digest: remote_digest,
                used_timestamp_millis: 0,
            }],
            chunk: 0,
            total_chunks: 1,
            to_system_uid: Some(7),
            from_system_uid: Some(8),
        };

        let plan = respond_to_gossip_status(&local, &status, &GCounterCodec, 10).unwrap();

        let gossip = plan.gossip().unwrap();
        assert!(gossip.send_back);
        assert_eq!(gossip.to_system_uid, Some(8));
        assert_eq!(gossip.from_system_uid, Some(7));
        assert_eq!(
            gossip
                .entries
                .iter()
                .map(|entry| entry.key.as_str())
                .collect::<Vec<_>>(),
            vec!["different", "local-only"]
        );
        assert!(plan.missing_status().is_none());
    }

    #[test]
    fn status_response_requests_keys_missing_locally() {
        let local = ReplicatorState::<GCounter>::new();
        let status = ReplicatorGossipStatus {
            entries: vec![ReplicatorGossipDigest {
                key: "remote-only".to_string(),
                digest: 42,
                used_timestamp_millis: 0,
            }],
            chunk: 0,
            total_chunks: 1,
            to_system_uid: Some(1),
            from_system_uid: Some(2),
        };

        let plan = respond_to_gossip_status(&local, &status, &GCounterCodec, 10).unwrap();

        assert!(plan.gossip().is_none());
        let request = plan.missing_status().unwrap();
        assert_eq!(request.to_system_uid, Some(2));
        assert_eq!(request.from_system_uid, Some(1));
        assert_eq!(request.entries[0].key, "remote-only");
        assert_eq!(
            request.entries[0].digest,
            REPLICATOR_GOSSIP_NOT_FOUND_DIGEST
        );
    }

    #[test]
    fn applying_gossip_merges_full_state_and_replies_when_requested() {
        let mut local = ReplicatorState::new();
        write_counter(&mut local, "counter", 1);
        let mut remote = ReplicatorState::new();
        remote.write_full(
            ReplicatorKey::new("counter"),
            DataEnvelope::new(counter("remote", 5)),
        );
        let gossip = create_gossip(
            &remote,
            [ReplicatorKey::new("counter")],
            true,
            Some(1),
            Some(2),
            &GCounterCodec,
        )
        .unwrap();

        let report = apply_gossip(&mut local, &gossip, &GCounterCodec).unwrap();

        assert!(
            report
                .changed_keys()
                .contains(&ReplicatorKey::new("counter"))
        );
        assert_eq!(
            local
                .envelope(&ReplicatorKey::new("counter"))
                .unwrap()
                .data()
                .value()
                .unwrap(),
            6
        );
        let reply = report.reply().unwrap();
        assert!(!reply.send_back);
        assert_eq!(reply.entries.len(), 1);
        assert_eq!(reply.to_system_uid, Some(2));
        assert_eq!(reply.from_system_uid, Some(1));
    }
}
