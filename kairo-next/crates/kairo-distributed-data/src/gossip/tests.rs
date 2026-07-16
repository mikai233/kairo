use super::*;
use crate::{
    DataEnvelope, DeltaReplicatedData, GCounter, GCounterCodec, ReplicaId, ReplicatorGossipDigest,
    ReplicatorGossipStatus, ReplicatorKey, ReplicatorState,
};

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
fn not_found_digest_requests_local_full_state() {
    let mut local = ReplicatorState::new();
    write_counter(&mut local, "requested", 10);
    let status = ReplicatorGossipStatus {
        entries: vec![ReplicatorGossipDigest {
            key: "requested".to_string(),
            digest: REPLICATOR_GOSSIP_NOT_FOUND_DIGEST,
            used_timestamp_millis: 0,
        }],
        chunk: 0,
        total_chunks: 1,
        to_system_uid: Some(7),
        from_system_uid: Some(8),
    };

    let plan = respond_to_gossip_status(&local, &status, &GCounterCodec, 10).unwrap();

    let gossip = plan.gossip().expect("not-found digest must request data");
    assert!(gossip.send_back);
    assert_eq!(gossip.to_system_uid, Some(8));
    assert_eq!(gossip.from_system_uid, Some(7));
    assert_eq!(gossip.entries.len(), 1);
    assert_eq!(gossip.entries[0].key, "requested");
    assert!(plan.missing_status().is_none());
}

#[test]
fn gossip_rejects_invalid_chunks_and_zero_response_limit() {
    let state = ReplicatorState::<GCounter>::new();
    let invalid_status = ReplicatorGossipStatus {
        entries: Vec::new(),
        chunk: 1,
        total_chunks: 1,
        to_system_uid: None,
        from_system_uid: None,
    };
    let valid_status = ReplicatorGossipStatus {
        chunk: 0,
        total_chunks: 1,
        ..invalid_status.clone()
    };

    assert!(matches!(
        build_gossip_status(&state, &GCounterCodec, 0, 0, None, None),
        Err(ReplicatorGossipError::InvalidChunk {
            chunk: 0,
            total_chunks: 0
        })
    ));
    assert!(matches!(
        respond_to_gossip_status(&state, &invalid_status, &GCounterCodec, 1),
        Err(ReplicatorGossipError::InvalidChunk {
            chunk: 1,
            total_chunks: 1
        })
    ));
    assert!(matches!(
        respond_to_gossip_status(&state, &valid_status, &GCounterCodec, 0),
        Err(ReplicatorGossipError::ZeroMaxEntries)
    ));
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
