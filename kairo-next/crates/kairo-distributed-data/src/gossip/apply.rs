use std::collections::BTreeSet;

use crate::{
    CrdtDataCodec, DeltaReplicatedData, ReplicatorGossip, ReplicatorGossipEntry, ReplicatorKey,
    ReplicatorState, decode_data_envelope, encode_data_envelope,
};

use super::{ReplicatorGossipApplyReport, ReplicatorGossipError};

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
