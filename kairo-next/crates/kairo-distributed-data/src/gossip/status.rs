use std::collections::BTreeSet;

use crate::{
    CrdtDataCodec, DeltaReplicatedData, ReplicatorGossipDigest, ReplicatorGossipStatus,
    ReplicatorKey, ReplicatorState,
};

use super::apply::create_gossip;
use super::hash::{
    REPLICATOR_GOSSIP_NOT_FOUND_DIGEST, digest_envelope, ensure_valid_chunk, key_belongs_to_chunk,
};
use super::{ReplicatorGossipError, ReplicatorGossipStatusPlan};

/// Builds one chunk of the local full-state digest summary.
///
/// Keys are assigned with Kairo's fixed stable hash. `chunk` is zero-based and
/// must be less than a non-zero `total_chunks`. Incarnation metadata is copied
/// unchanged into the resulting status.
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

/// Plans full-state and missing-key responses to a peer's gossip status.
///
/// Different digests, including the peer's not-found sentinel, request bounded
/// full-state gossip. Local keys absent from the peer status are also sent;
/// peer keys absent locally produce a status carrying the not-found sentinel.
/// `max_entries` bounds the full-state response and must be greater than zero.
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
        if digest_envelope(envelope, codec)? != entry.digest {
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
