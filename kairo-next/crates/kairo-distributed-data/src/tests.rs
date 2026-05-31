use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, mpsc};
use std::time::Duration;

use kairo_actor::{Actor, ActorResult, ActorSystem, Address, Context, ManualScheduler, Props};
use kairo_cluster::UniqueAddress;

use crate::{
    AggregationError, AggregationTarget, AggregationTransport, AggregationTransportFailure,
    AggregationTransportOperation, ConsistencyError, CrdtDataCodec, CrdtError, DataEnvelope,
    DeltaPropagationLog, DeltaPropagationLoop, DeltaPropagationTarget, DeltaPropagationTickReport,
    DeltaPropagationTransport, DeltaReceiveFailure, DeltaReceiveReply, DeltaReceiveStatus,
    DeltaReceiveTracker, DeltaReplicatedData, DeltaTransportFailure, DirectReadResult,
    DirectWriteResult, GCounter, GCounterCodec, GSet, GSetStringCodec, GetResponse, ORSet,
    PNCounter, PNCounterCodec, PruningPerformed, PruningState, ReadAggregationOutcome,
    ReadAggregationPlan, ReadAggregatorState, ReadConsistency, RemovedNodePruning,
    RemovedNodePruningTick, RemovedNodePruningTickReport, ReplicaId, ReplicatedData,
    ReplicatedDelta, ReplicatorActor, ReplicatorActorMsg, ReplicatorAggregation,
    ReplicatorClusterRouteReport, ReplicatorClusterRouteUpdate, ReplicatorDeltaPropagation,
    ReplicatorGossip, ReplicatorGossipReceiveReport, ReplicatorGossipStatus,
    ReplicatorGossipStatusReceiveReport, ReplicatorGossipTarget, ReplicatorGossipTickReport,
    ReplicatorGossipTransport, ReplicatorKey, ReplicatorState, UpdateResponse,
    WriteAggregationOutcome, WriteAggregationPlan, WriteAggregatorState, WriteConsistency,
    calculate_majority, decode_data_envelope, decode_delta_propagation, decode_read_result,
    encode_data_envelope, encode_delta_propagation, encode_read, encode_read_result, encode_write,
};

mod crdt_codecs;
mod crdt_foundation;
mod delta_log;
mod delta_transport;
mod delta_wire;

fn replica(id: &str) -> ReplicaId {
    ReplicaId::new(id)
}

fn delta_counter(id: &str, amount: u128) -> GCounter {
    GCounter::new()
        .increment(replica(id), amount)
        .unwrap()
        .delta()
        .unwrap()
}

fn full_counter(id: &str, amount: u128) -> GCounter {
    delta_counter(id, amount).reset_delta()
}

#[test]
fn delta_receive_tracker_applies_in_order_causal_deltas_once() {
    let key = ReplicatorKey::new("counter");
    let remote = replica("remote");
    let mut state = ReplicatorState::<GCounter>::new();
    let mut tracker = DeltaReceiveTracker::new();

    let first = tracker.apply_delta(
        &mut state,
        remote.clone(),
        key.clone(),
        1,
        1,
        delta_counter("a", 1),
    );
    assert!(matches!(
        first,
        DeltaReceiveStatus::Applied {
            previous_version: 0,
            to_version: 1,
            changed: true,
            ..
        }
    ));
    assert_eq!(tracker.current_version(&remote, &key), 1);
    assert_eq!(state.get_local(&key).data().unwrap().value().unwrap(), 1);

    let duplicate = tracker.apply_delta(
        &mut state,
        remote.clone(),
        key.clone(),
        1,
        1,
        delta_counter("a", 1),
    );
    assert!(matches!(
        duplicate,
        DeltaReceiveStatus::AlreadyHandled {
            current_version: 1,
            to_version: 1,
            ..
        }
    ));
    assert_eq!(state.get_local(&key).data().unwrap().value().unwrap(), 1);
}

#[test]
fn delta_receive_tracker_reports_missing_and_invalid_ranges() {
    let key = ReplicatorKey::new("counter");
    let remote = replica("remote");
    let mut state = ReplicatorState::<GCounter>::new();
    let mut tracker = DeltaReceiveTracker::new();

    let missing = tracker.apply_delta(
        &mut state,
        remote.clone(),
        key.clone(),
        3,
        3,
        delta_counter("a", 3),
    );
    assert!(matches!(
        missing,
        DeltaReceiveStatus::Missing {
            current_version: 0,
            expected_from_version: 1,
            from_version: 3,
            to_version: 3,
            ..
        }
    ));
    assert_eq!(tracker.current_version(&remote, &key), 0);
    assert_eq!(
        state.get_local(&key),
        GetResponse::NotFound { key: key.clone() }
    );

    let invalid = tracker.apply_delta(&mut state, remote, key, 2, 1, delta_counter("a", 1));
    assert!(matches!(
        invalid,
        DeltaReceiveStatus::InvalidRange {
            from_version: 2,
            to_version: 1,
            ..
        }
    ));
}

#[test]
fn delta_receive_tracker_applies_propagation_and_summarizes_ack() {
    let key = ReplicatorKey::new("counter");
    let remote = replica("remote");
    let mut log = DeltaPropagationLog::new([replica("local")]);
    log.record_delta(key.clone(), Some(delta_counter("a", 2)));
    let propagation = log.collect_propagations().into_values().next().unwrap();
    let wire =
        encode_delta_propagation(remote.clone(), true, &propagation, &GCounterCodec).unwrap();
    let mut state = ReplicatorState::<GCounter>::new();
    let mut tracker = DeltaReceiveTracker::new();

    let report = tracker.apply_propagation(&mut state, &wire, &GCounterCodec);

    assert_eq!(report.from(), &remote);
    assert!(report.reply_requested());
    assert!(report.is_success());
    assert!(matches!(report.reply(), Some(DeltaReceiveReply::Ack(_))));
    assert_eq!(report.failures(), &[]);
    assert!(matches!(
        report.statuses(),
        [DeltaReceiveStatus::Applied {
            previous_version: 0,
            to_version: 1,
            changed: true,
            ..
        }]
    ));
    assert_eq!(state.get_local(&key).data().unwrap().value().unwrap(), 2);

    let duplicate = tracker.apply_propagation(&mut state, &wire, &GCounterCodec);
    assert!(duplicate.is_success());
    assert!(matches!(
        duplicate.statuses(),
        [DeltaReceiveStatus::AlreadyHandled {
            current_version: 1,
            to_version: 1,
            ..
        }]
    ));
    assert!(matches!(duplicate.reply(), Some(DeltaReceiveReply::Ack(_))));
}

#[test]
fn delta_receive_tracker_summarizes_missing_or_decode_failure_as_nack() {
    let key = ReplicatorKey::new("counter");
    let remote = replica("remote");
    let encoded = GCounterCodec.serialize(&delta_counter("a", 3)).unwrap();
    let missing_wire = crate::ReplicatorDeltaPropagation {
        from: remote.clone(),
        reply: true,
        deltas: vec![crate::ReplicatorDelta::new(key.as_str(), encoded, 3, 3)],
    };
    let mut state = ReplicatorState::<GCounter>::new();
    let mut tracker = DeltaReceiveTracker::new();

    let missing_report = tracker.apply_propagation(&mut state, &missing_wire, &GCounterCodec);

    assert!(!missing_report.is_success());
    assert!(matches!(
        missing_report.statuses(),
        [DeltaReceiveStatus::Missing {
            expected_from_version: 1,
            from_version: 3,
            ..
        }]
    ));
    assert!(matches!(
        missing_report.reply(),
        Some(DeltaReceiveReply::Nack(_))
    ));

    let decode_failure_wire = crate::ReplicatorDeltaPropagation {
        from: remote,
        reply: true,
        deltas: vec![crate::ReplicatorDelta {
            key: "bad-counter".to_string(),
            crdt_manifest: crate::GSET_STRING_MANIFEST.to_string(),
            crdt_version: crate::CRDT_CODEC_VERSION,
            payload: bytes::Bytes::new(),
            from_version: 1,
            to_version: 1,
        }],
    };
    let decode_report = tracker.apply_propagation(&mut state, &decode_failure_wire, &GCounterCodec);

    assert!(!decode_report.is_success());
    assert!(matches!(
        decode_report.failures(),
        [DeltaReceiveFailure::DecodeFailed { key, .. }] if key == "bad-counter"
    ));
    assert!(matches!(
        decode_report.reply(),
        Some(DeltaReceiveReply::Nack(_))
    ));
}

#[test]
fn read_and_write_consistency_reject_single_remote_replica_counts() {
    assert_eq!(
        ReadConsistency::from(1, std::time::Duration::from_secs(1)),
        Err(ConsistencyError::ReplicaCountTooSmall { requested: 1 })
    );
    assert_eq!(
        WriteConsistency::to(0, std::time::Duration::from_secs(1)),
        Err(ConsistencyError::ReplicaCountTooSmall { requested: 0 })
    );
    assert!(ReadConsistency::local().is_local(3));
    assert!(WriteConsistency::majority(std::time::Duration::from_secs(1)).is_local(0));
}

#[test]
fn aggregators_calculate_majority_quorums_and_select_reachable_first() {
    assert_eq!(calculate_majority(0, 5, 0), 3);
    assert_eq!(calculate_majority(4, 5, 0), 4);
    assert_eq!(calculate_majority(0, 5, 3), 5);

    let nodes = vec![
        replica("a"),
        replica("b"),
        replica("c"),
        replica("d"),
        replica("e"),
    ];
    let aggregator = WriteAggregatorState::new(
        ReplicatorKey::new("counter"),
        &WriteConsistency::majority(Duration::from_secs(1)),
        nodes.clone(),
    )
    .unwrap();
    let unreachable = BTreeSet::from([replica("b")]);
    let selection = aggregator.select_replicas(&unreachable);

    assert_eq!(aggregator.required_remote_acks(), 3);
    assert_eq!(
        selection.primary(),
        &[replica("a"), replica("c"), replica("d")]
    );
    assert_eq!(selection.secondary(), &[replica("e"), replica("b")]);
}

#[test]
fn write_aggregator_tracks_ack_nack_timeout_and_not_enough_replicas() {
    let key = ReplicatorKey::new("counter");
    let nodes = vec![replica("a"), replica("b")];
    let mut aggregator = WriteAggregatorState::new(
        key.clone(),
        &WriteConsistency::to(3, Duration::from_secs(1)).unwrap(),
        nodes,
    )
    .unwrap();

    assert_eq!(aggregator.key(), &key);
    assert_eq!(aggregator.required_remote_acks(), 2);
    assert_eq!(
        aggregator.record_ack(&replica("a")),
        WriteAggregationOutcome::InProgress
    );
    assert_eq!(
        aggregator.record_ack(&replica("b")),
        WriteAggregationOutcome::Success
    );

    let mut failed = WriteAggregatorState::new(
        key,
        &WriteConsistency::to(3, Duration::from_secs(1)).unwrap(),
        vec![replica("a"), replica("b")],
    )
    .unwrap();
    assert_eq!(
        failed.record_nack(&replica("a")),
        WriteAggregationOutcome::Failed {
            required: 2,
            available: 1,
        }
    );

    let timed_out = WriteAggregatorState::new(
        ReplicatorKey::new("timeout"),
        &WriteConsistency::majority(Duration::from_secs(1)),
        vec![replica("a"), replica("b")],
    )
    .unwrap();
    assert_eq!(
        timed_out.timeout(),
        WriteAggregationOutcome::Timeout {
            required: 1,
            acknowledged: 0,
        }
    );

    assert_eq!(
        WriteAggregatorState::new(
            ReplicatorKey::new("not-enough"),
            &WriteConsistency::to(4, Duration::from_secs(1)).unwrap(),
            vec![replica("a"), replica("b")],
        )
        .unwrap_err(),
        AggregationError::NotEnoughReplicas {
            required: 3,
            available: 2,
        }
    );
}

#[test]
fn read_aggregator_merges_results_and_reports_not_found_or_failure() {
    let key = ReplicatorKey::new("counter");
    let mut aggregator = ReadAggregatorState::new(
        key.clone(),
        &ReadConsistency::from(3, Duration::from_secs(1)).unwrap(),
        vec![replica("a"), replica("b")],
        Some(DataEnvelope::new(
            GCounter::new()
                .increment(replica("local"), 1)
                .unwrap()
                .reset_delta(),
        )),
    )
    .unwrap();

    assert_eq!(aggregator.key(), &key);
    assert_eq!(aggregator.required_remote_reads(), 2);
    assert!(matches!(
        aggregator.record_read(Some(DataEnvelope::new(
            GCounter::new()
                .increment(replica("a"), 2)
                .unwrap()
                .reset_delta()
        ))),
        ReadAggregationOutcome::InProgress
    ));
    let outcome = aggregator.record_read(Some(DataEnvelope::new(
        GCounter::new()
            .increment(replica("b"), 3)
            .unwrap()
            .reset_delta(),
    )));
    match outcome {
        ReadAggregationOutcome::Success { envelope } => {
            assert_eq!(envelope.data().value().unwrap(), 6);
        }
        other => panic!("expected read success, got {other:?}"),
    }

    let mut not_found = ReadAggregatorState::<GCounter>::new(
        ReplicatorKey::new("missing"),
        &ReadConsistency::from(2, Duration::from_secs(1)).unwrap(),
        vec![replica("a")],
        None,
    )
    .unwrap();
    assert_eq!(
        not_found.record_read(None),
        ReadAggregationOutcome::NotFound
    );

    let timed_out = ReadAggregatorState::<GCounter>::new(
        ReplicatorKey::new("timeout"),
        &ReadConsistency::from(3, Duration::from_secs(1)).unwrap(),
        vec![replica("a"), replica("b")],
        None,
    )
    .unwrap();
    assert_eq!(
        timed_out.timeout(),
        ReadAggregationOutcome::Failure {
            required: 2,
            received: 0,
        }
    );
}

#[test]
fn aggregation_wire_round_trips_manifest_tagged_data_envelopes() {
    let removed = replica("removed");
    let owner = replica("local");
    let seen = replica("peer");
    let envelope = DataEnvelope::new(
        GCounter::new()
            .increment(owner.clone(), 7)
            .unwrap()
            .increment(removed.clone(), 3)
            .unwrap()
            .reset_delta(),
    )
    .init_removed_node_pruning(removed.clone(), owner.clone())
    .add_pruning_seen(seen.clone());
    let key = ReplicatorKey::new("counter");
    let from = owner.clone();

    let wire_envelope = encode_data_envelope(&envelope, &GCounterCodec).unwrap();
    assert_eq!(wire_envelope.crdt_manifest, crate::GCOUNTER_MANIFEST);
    assert_eq!(wire_envelope.crdt_version, crate::CRDT_CODEC_VERSION);
    assert_eq!(wire_envelope.pruning.len(), 1);
    let decoded = decode_data_envelope(&wire_envelope, &GCounterCodec).unwrap();
    assert_eq!(decoded.data().value().unwrap(), 10);
    let PruningState::Initialized(initialized) = decoded.pruning().get(&removed).unwrap() else {
        panic!("expected initialized pruning marker");
    };
    assert_eq!(initialized.owner(), &owner);
    assert!(initialized.seen().contains(&seen));

    let write = encode_write(&key, Some(from.clone()), &envelope, &GCounterCodec).unwrap();
    assert_eq!(write.key, key.as_str());
    assert_eq!(write.from, Some(from));
    assert_eq!(write.envelope.crdt_manifest, crate::GCOUNTER_MANIFEST);

    let read_result = encode_read_result(Some(&envelope), &GCounterCodec).unwrap();
    assert_eq!(
        decode_read_result(&read_result, &GCounterCodec)
            .unwrap()
            .unwrap()
            .data()
            .value()
            .unwrap(),
        10
    );
    assert_eq!(
        decode_read_result::<GCounter, _>(
            &encode_read_result::<GCounter, _>(None, &GCounterCodec).unwrap(),
            &GCounterCodec,
        )
        .unwrap(),
        None
    );

    let wrong_manifest = crate::ReplicatorDataEnvelope {
        crdt_manifest: crate::GSET_STRING_MANIFEST.to_string(),
        crdt_version: crate::CRDT_CODEC_VERSION,
        payload: wire_envelope.payload,
        pruning: Vec::new(),
    };
    assert!(
        decode_data_envelope::<GCounter, _>(&wrong_manifest, &GCounterCodec)
            .unwrap_err()
            .to_string()
            .contains("expected CRDT manifest")
    );
}

#[test]
fn aggregation_wire_round_trips_performed_pruning_markers() {
    let removed = replica("removed");
    let envelope = DataEnvelope::new(GCounter::new().reset_delta())
        .init_removed_node_pruning(removed.clone(), replica("owner"))
        .prune_removed_node(&removed, PruningPerformed::new(123))
        .unwrap();

    let decoded = decode_data_envelope(
        &encode_data_envelope(&envelope, &GCounterCodec).unwrap(),
        &GCounterCodec,
    )
    .unwrap();

    assert_eq!(
        decoded.pruning().get(&removed),
        Some(&PruningState::Performed(PruningPerformed::new(123)))
    );
}

#[test]
fn aggregation_transport_sends_primary_write_and_read_messages() {
    let system = ActorSystem::builder("ddata-aggregation-transport")
        .build()
        .unwrap();
    let (write_a, write_rx_a) = forward_ref(&system, "write-a");
    let (read_a, read_rx_a) = forward_ref(&system, "read-a");
    let (write_b, write_rx_b) = forward_ref(&system, "write-b");
    let (read_b, read_rx_b) = forward_ref(&system, "read-b");
    let (write_c, write_rx_c) = forward_ref(&system, "write-c");
    let (read_c, read_rx_c) = forward_ref(&system, "read-c");
    let key = ReplicatorKey::new("counter");
    let remote_nodes = vec![replica("a"), replica("b"), replica("c")];
    let write_state = WriteAggregatorState::new(
        key.clone(),
        &WriteConsistency::majority(Duration::from_secs(1)),
        remote_nodes.clone(),
    )
    .unwrap();
    let read_state = ReadAggregatorState::<GCounter>::new(
        key.clone(),
        &ReadConsistency::majority(Duration::from_secs(1)),
        remote_nodes,
        None,
    )
    .unwrap();
    let write_plan = WriteAggregationPlan::new(
        write_state.clone(),
        write_state.select_replicas(&BTreeSet::new()),
    );
    let read_plan = ReadAggregationPlan::new(
        read_state.clone(),
        read_state.select_replicas(&BTreeSet::new()),
    );
    let mut transport = AggregationTransport::new(replica("local"), GCounterCodec);
    transport.set_targets([
        AggregationTarget::new(replica("a"), write_a, read_a),
        AggregationTarget::new(replica("b"), write_b, read_b),
        AggregationTarget::new(replica("c"), write_c, read_c),
    ]);
    let envelope = DataEnvelope::new(
        GCounter::new()
            .increment(replica("local"), 5)
            .unwrap()
            .reset_delta(),
    );

    let write_report = transport.publish_write(&write_plan, &envelope);
    let read_report = transport.publish_read(&read_plan);

    assert!(write_report.is_success());
    assert_eq!(write_report.sent_to(), &[replica("a"), replica("b")]);
    assert!(read_report.is_success());
    assert_eq!(read_report.sent_to(), &[replica("a"), replica("b")]);

    for rx in [&write_rx_a, &write_rx_b] {
        let wire = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(wire.key, key.as_str());
        assert_eq!(wire.from, Some(replica("local")));
        assert_eq!(
            decode_data_envelope::<GCounter, _>(&wire.envelope, &GCounterCodec)
                .unwrap()
                .data()
                .value()
                .unwrap(),
            5
        );
    }
    for rx in [&read_rx_a, &read_rx_b] {
        let wire = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(wire.key, key.as_str());
        assert_eq!(wire.from, Some(replica("local")));
    }
    assert!(write_rx_c.recv_timeout(Duration::from_millis(100)).is_err());
    assert!(read_rx_c.recv_timeout(Duration::from_millis(100)).is_err());

    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn aggregation_transport_reports_missing_targets_without_stopping_other_sends() {
    let system = ActorSystem::builder("ddata-aggregation-transport-missing")
        .build()
        .unwrap();
    let (write_a, write_rx_a) = forward_ref(&system, "write-a");
    let (read_a, read_rx_a) = forward_ref(&system, "read-a");
    let nodes = vec![replica("a"), replica("b")];
    let key = ReplicatorKey::new("counter");
    let write_state = WriteAggregatorState::new(
        key.clone(),
        &WriteConsistency::to(3, Duration::from_secs(1)).unwrap(),
        nodes.clone(),
    )
    .unwrap();
    let read_state = ReadAggregatorState::<GCounter>::new(
        key.clone(),
        &ReadConsistency::from(3, Duration::from_secs(1)).unwrap(),
        nodes,
        None,
    )
    .unwrap();
    let write_plan = WriteAggregationPlan::new(
        write_state.clone(),
        write_state.select_replicas(&BTreeSet::new()),
    );
    let read_plan = ReadAggregationPlan::new(
        read_state.clone(),
        read_state.select_replicas(&BTreeSet::new()),
    );
    let mut transport = AggregationTransport::new(replica("local"), GCounterCodec);
    transport.insert_target(AggregationTarget::new(replica("a"), write_a, read_a));
    let envelope = DataEnvelope::new(
        GCounter::new()
            .increment(replica("local"), 1)
            .unwrap()
            .reset_delta(),
    );

    let write_report = transport.publish_write(&write_plan, &envelope);
    let read_report = transport.publish_read(&read_plan);

    assert_eq!(write_report.sent_to(), &[replica("a")]);
    assert!(matches!(
        write_report.failures(),
        [AggregationTransportFailure::MissingTarget { replica: failed_replica, operation }]
            if failed_replica == &replica("b") && operation == &AggregationTransportOperation::Write
    ));
    assert_eq!(read_report.sent_to(), &[replica("a")]);
    assert!(matches!(
        read_report.failures(),
        [AggregationTransportFailure::MissingTarget { replica: failed_replica, operation }]
            if failed_replica == &replica("b") && operation == &AggregationTransportOperation::Read
    ));
    let write = write_rx_a.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(write.key, key.as_str());
    let read = read_rx_a.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(read.key, key.as_str());

    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn direct_read_write_receive_applies_writes_and_serves_reads() {
    let key = ReplicatorKey::new("counter");
    let from = replica("remote");
    let envelope = DataEnvelope::new(
        GCounter::new()
            .increment(from.clone(), 6)
            .unwrap()
            .reset_delta(),
    );
    let write = encode_write(&key, Some(from.clone()), &envelope, &GCounterCodec).unwrap();
    let mut state = ReplicatorState::<GCounter>::new();

    let write_result = crate::apply_write(&mut state, &write, &GCounterCodec);

    assert!(write_result.is_ack());
    assert_eq!(write_result.key(), &key);
    assert_eq!(write_result.from(), Some(&from));
    assert!(matches!(
        write_result,
        DirectWriteResult::Ack { changed: true, .. }
    ));
    assert_eq!(state.envelope(&key).unwrap().data().value().unwrap(), 6);

    let read = encode_read(&key, Some(from.clone()));
    let read_result = crate::serve_read(&state, &read, &GCounterCodec).unwrap();

    assert_eq!(read_result.key(), &key);
    assert_eq!(read_result.from(), Some(&from));
    assert_eq!(
        decode_read_result(read_result.message(), &GCounterCodec)
            .unwrap()
            .unwrap()
            .data()
            .value()
            .unwrap(),
        6
    );

    let missing = crate::serve_read(
        &state,
        &encode_read(&ReplicatorKey::new("missing"), None),
        &GCounterCodec,
    )
    .unwrap();
    assert_eq!(missing.message().envelope, None);
}

#[test]
fn direct_write_receive_nacks_decode_failures_without_changing_state() {
    let key = ReplicatorKey::new("counter");
    let mut state = ReplicatorState::<GCounter>::new();
    let write = crate::ReplicatorWrite {
        key: key.as_str().to_string(),
        from: Some(replica("remote")),
        envelope: crate::ReplicatorDataEnvelope {
            crdt_manifest: crate::GSET_STRING_MANIFEST.to_string(),
            crdt_version: crate::CRDT_CODEC_VERSION,
            payload: bytes::Bytes::new(),
            pruning: Vec::new(),
        },
    };

    let result = crate::apply_write(&mut state, &write, &GCounterCodec);

    assert!(matches!(
        result,
        DirectWriteResult::Nack { reason, .. } if reason.contains("expected CRDT manifest")
    ));
    assert!(!state.contains_key(&key));
}

#[test]
fn replicator_state_gets_missing_and_existing_local_values() {
    let key = ReplicatorKey::new("counter-a");
    let node = replica("a");
    let mut state = ReplicatorState::<GCounter>::new();

    assert_eq!(
        state.get_local(&key),
        GetResponse::NotFound { key: key.clone() }
    );

    state
        .update_local(key.clone(), GCounter::new(), |counter| {
            counter.increment(node.clone(), 3)
        })
        .unwrap();

    assert_eq!(
        state.get_local(&key),
        GetResponse::Success {
            key,
            data: GCounter::new().increment(node, 3).unwrap().reset_delta(),
        }
    );
}

#[test]
fn replicator_state_update_stores_reset_full_state_and_returns_delta() {
    let key = ReplicatorKey::new("counter-a");
    let node = replica("a");
    let mut state = ReplicatorState::<GCounter>::new();

    let outcome = state
        .update_local(key.clone(), GCounter::new(), |counter| {
            counter.increment(node.clone(), 5)
        })
        .unwrap();

    assert!(outcome.changed());
    assert_eq!(outcome.key(), &key);
    assert_eq!(outcome.delta().unwrap().replica_value(&node), 5);
    assert_eq!(state.envelope(&key).unwrap().data().delta(), None);
}

#[test]
fn replicator_state_update_merges_with_existing_value() {
    let key = ReplicatorKey::new("counter-a");
    let node_a = replica("a");
    let node_b = replica("b");
    let mut state = ReplicatorState::<GCounter>::new();

    state.write_full(
        key.clone(),
        DataEnvelope::new(
            GCounter::new()
                .increment(node_a.clone(), 10)
                .unwrap()
                .reset_delta(),
        ),
    );
    state
        .update_local(key.clone(), GCounter::new(), |counter| {
            counter.increment(node_b.clone(), 4)
        })
        .unwrap();

    let GetResponse::Success { data, .. } = state.get_local(&key) else {
        panic!("counter should exist");
    };
    assert_eq!(data.replica_value(&node_a), 10);
    assert_eq!(data.replica_value(&node_b), 4);
}

#[test]
fn replicator_state_applies_remote_full_state_by_crdt_merge() {
    let key = ReplicatorKey::new("counter-a");
    let node_a = replica("a");
    let node_b = replica("b");
    let mut state = ReplicatorState::<GCounter>::new();

    state.write_full(
        key.clone(),
        DataEnvelope::new(
            GCounter::new()
                .increment(node_a.clone(), 2)
                .unwrap()
                .reset_delta(),
        ),
    );
    let changed = state.write_full(
        key.clone(),
        DataEnvelope::new(
            GCounter::new()
                .increment(node_a.clone(), 1)
                .unwrap()
                .increment(node_b.clone(), 7)
                .unwrap()
                .reset_delta(),
        ),
    );

    assert!(changed);
    let GetResponse::Success { data, .. } = state.get_local(&key) else {
        panic!("counter should exist");
    };
    assert_eq!(data.replica_value(&node_a), 2);
    assert_eq!(data.replica_value(&node_b), 7);
}

#[test]
fn replicator_state_applies_remote_delta_to_zero_when_missing() {
    let key = ReplicatorKey::new("set-a");
    let mut state = ReplicatorState::<GSet<&str>>::new();
    let delta = GSet::new().add("a").delta().unwrap();

    state.write_delta(key.clone(), delta);

    let GetResponse::Success { data, .. } = state.get_local(&key) else {
        panic!("set should exist");
    };
    assert!(data.contains(&"a"));
}

#[test]
fn replicator_state_flushes_changes_once_in_key_order() {
    let mut state = ReplicatorState::<GCounter>::new();
    let node = replica("a");
    let key_a = ReplicatorKey::new("a");
    let key_b = ReplicatorKey::new("b");

    state
        .update_local(key_b.clone(), GCounter::new(), |counter| {
            counter.increment(node.clone(), 1)
        })
        .unwrap();
    state
        .update_local(key_a.clone(), GCounter::new(), |counter| {
            counter.increment(node.clone(), 1)
        })
        .unwrap();

    let changes = state.flush_changes();

    assert_eq!(
        changes
            .iter()
            .map(|change| change.key().as_str())
            .collect::<Vec<_>>(),
        vec!["a", "b"]
    );
    assert!(state.flush_changes().is_empty());
}

struct Forward<M> {
    tx: mpsc::Sender<M>,
}

impl<M> Actor for Forward<M>
where
    M: Send + 'static,
{
    type Msg = M;

    fn receive(&mut self, _ctx: &mut Context<Self::Msg>, msg: Self::Msg) -> ActorResult {
        self.tx
            .send(msg)
            .map_err(|error| kairo_actor::ActorError::Message(error.to_string()))
    }
}

fn forward_ref<M>(system: &ActorSystem, name: &str) -> (kairo_actor::ActorRef<M>, mpsc::Receiver<M>)
where
    M: Send + 'static,
{
    let (tx, rx) = mpsc::channel();
    let actor = system
        .spawn(name, Props::new(move || Forward { tx }))
        .expect("forward actor should spawn");
    (actor, rx)
}

#[test]
fn replicator_actor_handles_local_get_and_update() {
    let system = ActorSystem::builder("ddata-replicator-get-update")
        .build()
        .unwrap();
    let replicator = system
        .spawn("replicator", Props::new(ReplicatorActor::<GCounter>::new))
        .unwrap();
    let (get_ref, get_rx) = forward_ref(&system, "get-replies");
    let (update_ref, update_rx) = forward_ref(&system, "update-replies");
    let key = ReplicatorKey::new("counter");
    let node = replica("a");

    replicator
        .tell(ReplicatorActorMsg::Get {
            key: key.clone(),
            consistency: ReadConsistency::local(),
            reply_to: get_ref.clone(),
        })
        .unwrap();
    assert_eq!(
        get_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        GetResponse::NotFound { key: key.clone() }
    );

    replicator
        .tell(ReplicatorActorMsg::Update {
            key: key.clone(),
            initial: GCounter::new(),
            consistency: WriteConsistency::local(),
            modify: Box::new(move |counter| {
                counter
                    .increment(node.clone(), 4)
                    .map_err(|e| e.to_string())
            }),
            reply_to: update_ref,
        })
        .unwrap();
    let update = update_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(matches!(update, UpdateResponse::Success(_)));

    replicator
        .tell(ReplicatorActorMsg::Get {
            key: key.clone(),
            consistency: ReadConsistency::local(),
            reply_to: get_ref,
        })
        .unwrap();
    let GetResponse::Success { data, .. } = get_rx.recv_timeout(Duration::from_secs(1)).unwrap()
    else {
        panic!("counter should be available");
    };
    assert_eq!(data.value().unwrap(), 4);

    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn replicator_actor_spawns_write_session_for_non_local_update() {
    let system = ActorSystem::builder("ddata-replicator-aggregate-update")
        .build()
        .unwrap();
    let (write_ref, write_rx) = forward_ref::<crate::ReplicatorWrite>(&system, "remote-writes");
    let (read_ref, _read_rx) = forward_ref::<crate::ReplicatorRead>(&system, "remote-reads");
    let (update_ref, update_rx) = forward_ref(&system, "update-replies");
    let mut transport = AggregationTransport::new(replica("local"), GCounterCodec);
    transport.insert_target(AggregationTarget::new(
        replica("remote"),
        write_ref,
        read_ref,
    ));
    let aggregation = ReplicatorAggregation::new(transport, Arc::new(GCounterCodec));
    let replicator = system
        .spawn(
            "replicator",
            Props::new(move || ReplicatorActor::<GCounter>::with_aggregation(aggregation)),
        )
        .unwrap();
    let key = ReplicatorKey::new("counter");

    replicator
        .tell(ReplicatorActorMsg::SetRemoteReplicas {
            nodes: vec![replica("remote")],
            unreachable: BTreeSet::new(),
        })
        .unwrap();
    replicator
        .tell(ReplicatorActorMsg::Update {
            key: key.clone(),
            initial: GCounter::new(),
            consistency: WriteConsistency::to(2, Duration::from_millis(20)).unwrap(),
            modify: Box::new(|counter| {
                counter
                    .increment(replica("local"), 5)
                    .map_err(|error| error.to_string())
            }),
            reply_to: update_ref,
        })
        .unwrap();

    let write = write_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(write.key, key.as_str());
    assert_eq!(write.from, Some(replica("local")));
    assert_eq!(
        update_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        UpdateResponse::Timeout { key }
    );
    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn replicator_actor_spawns_read_session_for_non_local_get() {
    let system = ActorSystem::builder("ddata-replicator-aggregate-get")
        .build()
        .unwrap();
    let (write_ref, _write_rx) = forward_ref::<crate::ReplicatorWrite>(&system, "remote-writes");
    let (read_ref, read_rx) = forward_ref::<crate::ReplicatorRead>(&system, "remote-reads");
    let (get_ref, get_rx) = forward_ref(&system, "get-replies");
    let mut transport = AggregationTransport::new(replica("local"), GCounterCodec);
    transport.insert_target(AggregationTarget::new(
        replica("remote"),
        write_ref,
        read_ref,
    ));
    let aggregation = ReplicatorAggregation::new(transport, Arc::new(GCounterCodec));
    let replicator = system
        .spawn(
            "replicator",
            Props::new(move || ReplicatorActor::<GCounter>::with_aggregation(aggregation)),
        )
        .unwrap();
    let key = ReplicatorKey::new("counter");

    replicator
        .tell(ReplicatorActorMsg::SetRemoteReplicas {
            nodes: vec![replica("remote")],
            unreachable: BTreeSet::new(),
        })
        .unwrap();
    replicator
        .tell(ReplicatorActorMsg::Get {
            key: key.clone(),
            consistency: ReadConsistency::from(2, Duration::from_millis(20)).unwrap(),
            reply_to: get_ref,
        })
        .unwrap();

    let read = read_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(read.key, key.as_str());
    assert_eq!(read.from, Some(replica("local")));
    assert!(matches!(
        get_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        GetResponse::Failure { key: failed_key, reason }
            if failed_key == key && reason.contains("required 1")
    ));
    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn replicator_actor_sends_current_value_on_subscribe_and_flushes_later_changes() {
    let system = ActorSystem::builder("ddata-replicator-subscribe")
        .build()
        .unwrap();
    let replicator = system
        .spawn(
            "replicator",
            Props::new(ReplicatorActor::<GSet<&'static str>>::new),
        )
        .unwrap();
    let (update_ref, update_rx) = forward_ref(&system, "update-replies");
    let (change_ref, change_rx) = forward_ref(&system, "changes");
    let key = ReplicatorKey::new("set");

    replicator
        .tell(ReplicatorActorMsg::Update {
            key: key.clone(),
            initial: GSet::new(),
            consistency: WriteConsistency::local(),
            modify: Box::new(|set| Ok(set.add("a"))),
            reply_to: update_ref.clone(),
        })
        .unwrap();
    update_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    replicator
        .tell(ReplicatorActorMsg::Subscribe {
            key: key.clone(),
            subscriber: change_ref.clone(),
        })
        .unwrap();
    let current = change_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(current.key(), &key);
    assert!(current.data().contains(&"a"));

    replicator
        .tell(ReplicatorActorMsg::Update {
            key: key.clone(),
            initial: GSet::new(),
            consistency: WriteConsistency::local(),
            modify: Box::new(|set| Ok(set.add("b"))),
            reply_to: update_ref,
        })
        .unwrap();
    replicator.tell(ReplicatorActorMsg::FlushChanges).unwrap();

    let changed = change_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(changed.key(), &key);
    assert!(changed.data().contains(&"a"));
    assert!(changed.data().contains(&"b"));

    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn replicator_actor_can_unsubscribe_from_later_flushes() {
    let system = ActorSystem::builder("ddata-replicator-unsubscribe")
        .build()
        .unwrap();
    let replicator = system
        .spawn(
            "replicator",
            Props::new(ReplicatorActor::<GSet<&'static str>>::new),
        )
        .unwrap();
    let (update_ref, update_rx) = forward_ref(&system, "update-replies");
    let (change_ref, change_rx) = forward_ref(&system, "changes");
    let key = ReplicatorKey::new("set");

    replicator
        .tell(ReplicatorActorMsg::Subscribe {
            key: key.clone(),
            subscriber: change_ref.clone(),
        })
        .unwrap();
    replicator
        .tell(ReplicatorActorMsg::Unsubscribe {
            key: key.clone(),
            subscriber: change_ref,
        })
        .unwrap();
    replicator
        .tell(ReplicatorActorMsg::Update {
            key,
            initial: GSet::new(),
            consistency: WriteConsistency::local(),
            modify: Box::new(|set| Ok(set.add("a"))),
            reply_to: update_ref,
        })
        .unwrap();
    update_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    replicator.tell(ReplicatorActorMsg::FlushChanges).unwrap();

    assert!(change_rx.recv_timeout(Duration::from_millis(100)).is_err());

    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn replicator_actor_collects_delta_propagations_from_local_updates() {
    let system = ActorSystem::builder("ddata-replicator-delta")
        .build()
        .unwrap();
    let replicator = system
        .spawn("replicator", Props::new(ReplicatorActor::<GCounter>::new))
        .unwrap();
    let (update_ref, update_rx) = forward_ref(&system, "update-replies");
    let (delta_ref, delta_rx) = forward_ref::<BTreeMap<ReplicaId, crate::DeltaPropagation<GCounter>>>(
        &system,
        "delta-replies",
    );
    let key = ReplicatorKey::new("counter");
    let remote = replica("remote");
    let node = replica("local");

    replicator
        .tell(ReplicatorActorMsg::SetDeltaNodes {
            nodes: vec![remote.clone()],
        })
        .unwrap();
    replicator
        .tell(ReplicatorActorMsg::Update {
            key: key.clone(),
            initial: GCounter::new(),
            consistency: WriteConsistency::local(),
            modify: Box::new(move |counter| {
                counter
                    .increment(node.clone(), 3)
                    .map_err(|e| e.to_string())
            }),
            reply_to: update_ref,
        })
        .unwrap();
    update_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    replicator
        .tell(ReplicatorActorMsg::CollectDeltaPropagations {
            reply_to: delta_ref,
        })
        .unwrap();

    let propagations = delta_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let entry = propagations
        .get(&remote)
        .unwrap()
        .entries()
        .get(&key)
        .unwrap();
    assert_eq!(entry.from_version(), 1);
    assert_eq!(entry.to_version(), 1);
    assert_eq!(entry.delta().value().unwrap(), 3);

    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn replicator_actor_delta_loop_publishes_and_cleans_on_manual_tick() {
    let system = ActorSystem::builder("ddata-replicator-delta-loop")
        .build()
        .unwrap();
    let (target_ref, target_rx) =
        forward_ref::<ReplicatorDeltaPropagation>(&system, "delta-target");
    let mut transport = DeltaPropagationTransport::new(replica("local"), GCounterCodec);
    transport.insert_target(DeltaPropagationTarget::new(replica("remote"), target_ref));
    let delta_loop = DeltaPropagationLoop::new(transport).with_cleanup_every_ticks(1);
    let replicator = system
        .spawn(
            "replicator",
            Props::new(move || {
                ReplicatorActor::<GCounter>::with_delta_propagation_loop(delta_loop)
            }),
        )
        .unwrap();
    let (update_ref, update_rx) = forward_ref(&system, "update-replies");
    let (tick_ref, tick_rx) = forward_ref::<DeltaPropagationTickReport>(&system, "tick-replies");
    let key = ReplicatorKey::new("counter");

    replicator
        .tell(ReplicatorActorMsg::SetDeltaNodes {
            nodes: vec![replica("remote")],
        })
        .unwrap();
    replicator
        .tell(ReplicatorActorMsg::Update {
            key: key.clone(),
            initial: GCounter::new(),
            consistency: WriteConsistency::local(),
            modify: Box::new(|counter| {
                counter
                    .increment(replica("local"), 5)
                    .map_err(|error| error.to_string())
            }),
            reply_to: update_ref,
        })
        .unwrap();
    update_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    replicator
        .tell(ReplicatorActorMsg::RunDeltaPropagation { reply_to: tick_ref })
        .unwrap();
    let report = tick_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(report.propagation_count(), 1);
    assert!(report.cleaned_delta_entries());
    assert_eq!(report.transport().sent_to(), &[replica("remote")]);
    let outbound = target_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(outbound.from, replica("local"));
    assert!(!outbound.reply);
    assert_eq!(outbound.deltas.len(), 1);
    assert_eq!(outbound.deltas[0].key, key.as_str());

    let (tick_ref, tick_rx) = forward_ref::<DeltaPropagationTickReport>(&system, "tick-replies-2");
    replicator
        .tell(ReplicatorActorMsg::RunDeltaPropagation { reply_to: tick_ref })
        .unwrap();
    let report = tick_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(report.propagation_count(), 2);
    assert!(report.cleaned_delta_entries());
    assert!(report.transport().sent_to().is_empty());
    assert!(target_rx.recv_timeout(Duration::from_millis(50)).is_err());

    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn replicator_actor_schedules_delta_loop_ticks_with_manual_time() {
    let manual = ManualScheduler::new();
    let system = ActorSystem::builder("ddata-replicator-delta-loop-scheduled")
        .manual_scheduler(manual.clone())
        .build()
        .unwrap();
    let (target_ref, target_rx) =
        forward_ref::<ReplicatorDeltaPropagation>(&system, "delta-target");
    let mut transport = DeltaPropagationTransport::new(replica("local"), GCounterCodec);
    transport.insert_target(DeltaPropagationTarget::new(replica("remote"), target_ref));
    let delta_loop = DeltaPropagationLoop::new(transport).with_cleanup_every_ticks(5);
    let replicator = system
        .spawn(
            "replicator",
            Props::new(move || {
                ReplicatorActor::<GCounter>::with_delta_propagation_loop_interval(
                    delta_loop,
                    Duration::from_millis(25),
                )
            }),
        )
        .unwrap();
    let (update_ref, update_rx) = forward_ref(&system, "update-replies");

    replicator
        .tell(ReplicatorActorMsg::SetDeltaNodes {
            nodes: vec![replica("remote")],
        })
        .unwrap();
    replicator
        .tell(ReplicatorActorMsg::Update {
            key: ReplicatorKey::new("counter"),
            initial: GCounter::new(),
            consistency: WriteConsistency::local(),
            modify: Box::new(|counter| {
                counter
                    .increment(replica("local"), 8)
                    .map_err(|error| error.to_string())
            }),
            reply_to: update_ref,
        })
        .unwrap();
    update_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    manual.advance(Duration::from_millis(24));
    assert!(target_rx.recv_timeout(Duration::from_millis(50)).is_err());
    manual.advance(Duration::from_millis(1));
    let outbound = target_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(outbound.from, replica("local"));
    assert_eq!(outbound.deltas.len(), 1);

    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn replicator_actor_gossip_tick_sends_status_to_reachable_target() {
    let system = ActorSystem::builder("ddata-replicator-gossip-tick")
        .build()
        .unwrap();
    let remote = replica("remote");
    let (status_ref, status_rx) = forward_ref::<ReplicatorGossipStatus>(&system, "gossip-status");
    let (gossip_ref, _gossip_rx) = forward_ref::<ReplicatorGossip>(&system, "gossip-target");
    let transport = ReplicatorGossipTransport::new();
    transport.insert_target(ReplicatorGossipTarget::new(
        remote.clone(),
        status_ref,
        gossip_ref,
    ));
    let replicator = system
        .spawn(
            "replicator",
            Props::new(move || {
                ReplicatorActor::<GCounter>::with_gossip(transport, Arc::new(GCounterCodec))
                    .with_self_system_uid(1)
            }),
        )
        .unwrap();
    let (tick_ref, tick_rx) = forward_ref::<ReplicatorGossipTickReport>(&system, "gossip-reply");

    replicator
        .tell(ReplicatorActorMsg::SetRemoteReplicas {
            nodes: vec![remote.clone()],
            unreachable: BTreeSet::new(),
        })
        .unwrap();
    replicator
        .tell(ReplicatorActorMsg::WriteFull {
            key: ReplicatorKey::new("counter"),
            envelope: DataEnvelope::new(full_counter("local", 3)),
        })
        .unwrap();
    replicator
        .tell(ReplicatorActorMsg::RunGossip {
            reply_to: Some(tick_ref),
        })
        .unwrap();

    let report = tick_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(report.target(), Some(&remote));
    assert_eq!(report.transport().sent_status_to(), &[remote]);
    let status = status_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(status.entries.len(), 1);
    assert_eq!(status.entries[0].key, "counter");
    assert_ne!(status.entries[0].digest, 0);
    assert_eq!(status.from_system_uid, Some(1));

    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn replicator_actor_schedules_gossip_ticks_with_manual_time() {
    let manual = ManualScheduler::new();
    let system = ActorSystem::builder("ddata-replicator-gossip-scheduled")
        .manual_scheduler(manual.clone())
        .build()
        .unwrap();
    let remote = replica("remote");
    let (status_ref, status_rx) =
        forward_ref::<ReplicatorGossipStatus>(&system, "scheduled-status");
    let (gossip_ref, _gossip_rx) = forward_ref::<ReplicatorGossip>(&system, "scheduled-gossip");
    let transport = ReplicatorGossipTransport::new();
    transport.insert_target(ReplicatorGossipTarget::new(
        remote.clone(),
        status_ref,
        gossip_ref,
    ));
    let replicator = system
        .spawn(
            "replicator",
            Props::new(move || {
                ReplicatorActor::<GCounter>::with_gossip_interval(
                    transport,
                    Arc::new(GCounterCodec),
                    Duration::from_millis(25),
                )
            }),
        )
        .unwrap();

    replicator
        .tell(ReplicatorActorMsg::SetRemoteReplicas {
            nodes: vec![remote],
            unreachable: BTreeSet::new(),
        })
        .unwrap();
    let (barrier_ref, barrier_rx) =
        forward_ref::<ReplicatorGossipTickReport>(&system, "scheduled-barrier");
    replicator
        .tell(ReplicatorActorMsg::RunGossip {
            reply_to: Some(barrier_ref),
        })
        .unwrap();
    barrier_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    status_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    manual.advance(Duration::from_millis(24));
    assert!(status_rx.recv_timeout(Duration::from_millis(50)).is_err());
    manual.advance(Duration::from_millis(1));
    let status = status_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(status.chunk, 0);
    assert_eq!(status.total_chunks, 1);

    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn replicator_actor_receives_gossip_status_and_sends_gossip_responses() {
    let system = ActorSystem::builder("ddata-replicator-gossip-status")
        .build()
        .unwrap();
    let remote = replica("remote");
    let (status_ref, status_rx) = forward_ref::<ReplicatorGossipStatus>(&system, "missing-status");
    let (gossip_ref, gossip_rx) = forward_ref::<ReplicatorGossip>(&system, "full-gossip");
    let transport = ReplicatorGossipTransport::new();
    transport.insert_target(ReplicatorGossipTarget::new(
        remote.clone(),
        status_ref,
        gossip_ref,
    ));
    let replicator = system
        .spawn(
            "replicator",
            Props::new(move || {
                ReplicatorActor::<GCounter>::with_gossip(transport, Arc::new(GCounterCodec))
            }),
        )
        .unwrap();
    let (reply_ref, reply_rx) =
        forward_ref::<ReplicatorGossipStatusReceiveReport>(&system, "status-report");

    replicator
        .tell(ReplicatorActorMsg::WriteFull {
            key: ReplicatorKey::new("different"),
            envelope: DataEnvelope::new(full_counter("local", 10)),
        })
        .unwrap();
    replicator
        .tell(ReplicatorActorMsg::WriteFull {
            key: ReplicatorKey::new("local-only"),
            envelope: DataEnvelope::new(full_counter("local", 20)),
        })
        .unwrap();
    let remote_envelope = DataEnvelope::new(full_counter("remote", 99));
    let remote_digest = crate::digest_envelope(&remote_envelope, &GCounterCodec).unwrap();

    replicator
        .tell(ReplicatorActorMsg::ReceiveGossipStatus {
            from: remote.clone(),
            status: ReplicatorGossipStatus {
                entries: vec![
                    crate::ReplicatorGossipDigest {
                        key: "different".to_string(),
                        digest: remote_digest,
                        used_timestamp_millis: 0,
                    },
                    crate::ReplicatorGossipDigest {
                        key: "remote-only".to_string(),
                        digest: 7,
                        used_timestamp_millis: 0,
                    },
                ],
                chunk: 0,
                total_chunks: 1,
                to_system_uid: Some(1),
                from_system_uid: Some(2),
            },
            codec: Arc::new(GCounterCodec),
            reply_to: Some(reply_ref),
        })
        .unwrap();

    let report = reply_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(report.plan().gossip().is_some());
    assert!(report.plan().missing_status().is_some());
    assert_eq!(
        report.transport().sent_gossip_to(),
        std::slice::from_ref(&remote)
    );
    assert_eq!(
        report.transport().sent_status_to(),
        std::slice::from_ref(&remote)
    );
    let gossip = gossip_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(gossip.send_back);
    assert_eq!(
        gossip
            .entries
            .iter()
            .map(|entry| entry.key.as_str())
            .collect::<Vec<_>>(),
        vec!["different", "local-only"]
    );
    let missing = status_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(missing.entries[0].key, "remote-only");
    assert_eq!(missing.entries[0].digest, 0);

    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn replicator_actor_receives_gossip_merges_state_and_sends_reply() {
    let system = ActorSystem::builder("ddata-replicator-gossip-receive")
        .build()
        .unwrap();
    let remote = replica("remote");
    let (status_ref, _status_rx) = forward_ref::<ReplicatorGossipStatus>(&system, "status-target");
    let (gossip_ref, gossip_rx) = forward_ref::<ReplicatorGossip>(&system, "gossip-replies");
    let transport = ReplicatorGossipTransport::new();
    transport.insert_target(ReplicatorGossipTarget::new(
        remote.clone(),
        status_ref,
        gossip_ref,
    ));
    let replicator = system
        .spawn(
            "replicator",
            Props::new(move || {
                ReplicatorActor::<GCounter>::with_gossip(transport, Arc::new(GCounterCodec))
            }),
        )
        .unwrap();
    let (reply_ref, reply_rx) =
        forward_ref::<ReplicatorGossipReceiveReport>(&system, "gossip-report");

    replicator
        .tell(ReplicatorActorMsg::WriteFull {
            key: ReplicatorKey::new("counter"),
            envelope: DataEnvelope::new(full_counter("local", 1)),
        })
        .unwrap();
    let remote_envelope = encode_data_envelope(
        &DataEnvelope::new(full_counter("remote", 5)),
        &GCounterCodec,
    )
    .unwrap();
    replicator
        .tell(ReplicatorActorMsg::ReceiveGossip {
            from: remote.clone(),
            gossip: ReplicatorGossip {
                entries: vec![crate::ReplicatorGossipEntry {
                    key: "counter".to_string(),
                    envelope: remote_envelope,
                    used_timestamp_millis: 0,
                }],
                send_back: true,
                to_system_uid: Some(1),
                from_system_uid: Some(2),
            },
            codec: Arc::new(GCounterCodec),
            reply_to: Some(reply_ref),
        })
        .unwrap();

    let report = reply_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(
        report
            .apply()
            .changed_keys()
            .contains(&ReplicatorKey::new("counter"))
    );
    assert_eq!(report.transport().sent_gossip_to(), &[remote]);
    let reply = gossip_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(!reply.send_back);
    assert_eq!(reply.entries.len(), 1);

    let (get_ref, get_rx) = forward_ref::<GetResponse<GCounter>>(&system, "get-after-gossip");
    replicator
        .tell(ReplicatorActorMsg::Get {
            key: ReplicatorKey::new("counter"),
            consistency: ReadConsistency::local(),
            reply_to: get_ref,
        })
        .unwrap();
    assert_eq!(
        get_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .data()
            .unwrap()
            .value()
            .unwrap(),
        6
    );

    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn replicator_actor_runs_removed_node_pruning_tick_after_seen_marker() {
    let system = ActorSystem::builder("ddata-replicator-pruning")
        .build()
        .unwrap();
    let replicator = system
        .spawn("replicator", Props::new(ReplicatorActor::<GCounter>::new))
        .unwrap();
    let key = ReplicatorKey::new("counter");
    let self_replica = replica("self");
    let peer = replica("peer");
    let removed = replica("removed");
    let envelope = DataEnvelope::new(
        GCounter::new()
            .increment(self_replica.clone(), 2)
            .unwrap()
            .increment(removed.clone(), 4)
            .unwrap()
            .reset_delta(),
    );
    let (pruning_ref, pruning_rx) =
        forward_ref::<RemovedNodePruningTickReport>(&system, "pruning-reports");
    let (seen_ref, seen_rx) = forward_ref::<BTreeSet<ReplicatorKey>>(&system, "seen-reports");
    let (read_ref, read_rx) =
        forward_ref::<Result<DirectReadResult, String>>(&system, "read-results");

    replicator
        .tell(ReplicatorActorMsg::WriteFull {
            key: key.clone(),
            envelope,
        })
        .unwrap();

    replicator
        .tell(ReplicatorActorMsg::RunRemovedNodePruning {
            tick: pruning_tick(self_replica.clone(), [peer.clone()], 0, 10, 1_000, true),
            reply_to: pruning_ref.clone(),
        })
        .unwrap();
    let first = pruning_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(first.collected_removed, BTreeSet::from([removed.clone()]));
    assert!(first.initialized.is_empty());
    assert!(first.performed.is_empty());

    replicator
        .tell(ReplicatorActorMsg::RunRemovedNodePruning {
            tick: pruning_tick(self_replica.clone(), [peer.clone()], 11, 10, 1_000, true),
            reply_to: pruning_ref.clone(),
        })
        .unwrap();
    let initialized = pruning_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(initialized.initialized, BTreeSet::from([key.clone()]));
    assert!(initialized.performed.is_empty());

    replicator
        .tell(ReplicatorActorMsg::MarkRemovedNodePruningSeen {
            seen_by: peer.clone(),
            reply_to: seen_ref,
        })
        .unwrap();
    assert_eq!(
        seen_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        BTreeSet::from([key.clone()])
    );

    replicator
        .tell(ReplicatorActorMsg::RunRemovedNodePruning {
            tick: pruning_tick(self_replica.clone(), [peer], 12, 10, 1_000, true),
            reply_to: pruning_ref,
        })
        .unwrap();
    let performed = pruning_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(performed.performed, BTreeSet::from([key.clone()]));
    assert!(performed.failures.is_empty());

    replicator
        .tell(ReplicatorActorMsg::ServeRead {
            read: encode_read(&key, None),
            codec: Arc::new(GCounterCodec),
            reply_to: read_ref,
        })
        .unwrap();
    let read_result = read_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap()
        .unwrap();
    let decoded = decode_read_result(read_result.message(), &GCounterCodec)
        .unwrap()
        .unwrap();
    assert_eq!(decoded.data().replica_value(&removed), 0);
    assert_eq!(decoded.data().replica_value(&self_replica), 6);
    assert_eq!(
        decoded.pruning().get(&removed),
        Some(&PruningState::Performed(PruningPerformed::new(1_100)))
    );

    system.terminate(Duration::from_secs(1)).unwrap();
}

fn pruning_tick(
    self_replica: ReplicaId,
    live_replicas: impl IntoIterator<Item = ReplicaId>,
    all_reachable_time_nanos: u64,
    max_pruning_dissemination_nanos: u64,
    now_millis: u64,
    is_leader: bool,
) -> RemovedNodePruningTick {
    RemovedNodePruningTick {
        self_replica,
        live_replicas: live_replicas.into_iter().collect(),
        unreachable_replicas: BTreeSet::new(),
        all_reachable_time_nanos,
        max_pruning_dissemination_nanos,
        now_millis,
        pruning_marker_ttl_millis: 100,
        is_leader,
    }
}

#[test]
fn replicator_actor_plans_remote_read_with_local_value_and_reachable_first_targets() {
    let system = ActorSystem::builder("ddata-replicator-plan-read")
        .build()
        .unwrap();
    let replicator = system
        .spawn("replicator", Props::new(ReplicatorActor::<GCounter>::new))
        .unwrap();
    let (update_ref, update_rx) = forward_ref(&system, "update-replies");
    let (plan_ref, plan_rx) = forward_ref::<Result<ReadAggregationPlan<GCounter>, AggregationError>>(
        &system,
        "read-plans",
    );
    let key = ReplicatorKey::new("counter");

    replicator
        .tell(ReplicatorActorMsg::SetRemoteReplicas {
            nodes: vec![replica("a"), replica("b"), replica("c")],
            unreachable: BTreeSet::from([replica("b")]),
        })
        .unwrap();
    replicator
        .tell(ReplicatorActorMsg::Update {
            key: key.clone(),
            initial: GCounter::new(),
            consistency: WriteConsistency::local(),
            modify: Box::new(|counter| {
                counter
                    .increment(replica("local"), 4)
                    .map_err(|e| e.to_string())
            }),
            reply_to: update_ref,
        })
        .unwrap();
    update_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    replicator
        .tell(ReplicatorActorMsg::PlanRead {
            key: key.clone(),
            consistency: ReadConsistency::majority(Duration::from_secs(1)),
            reply_to: plan_ref,
        })
        .unwrap();

    let plan = plan_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap()
        .unwrap();
    assert_eq!(plan.state().key(), &key);
    assert_eq!(plan.state().required_remote_reads(), 2);
    assert_eq!(plan.selection().primary(), &[replica("a"), replica("c")]);
    assert_eq!(plan.selection().secondary(), &[replica("b")]);

    let mut state = plan.into_state();
    assert!(matches!(
        state.record_read(Some(DataEnvelope::new(
            GCounter::new()
                .increment(replica("a"), 3)
                .unwrap()
                .reset_delta()
        ))),
        ReadAggregationOutcome::InProgress
    ));
    match state.record_read(None) {
        ReadAggregationOutcome::Success { envelope } => {
            assert_eq!(envelope.data().value().unwrap(), 7);
        }
        other => panic!("expected read success, got {other:?}"),
    }

    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn replicator_actor_plans_remote_write_and_reports_quorum_errors() {
    let system = ActorSystem::builder("ddata-replicator-plan-write")
        .build()
        .unwrap();
    let replicator = system
        .spawn("replicator", Props::new(ReplicatorActor::<GCounter>::new))
        .unwrap();
    let (plan_ref, plan_rx) =
        forward_ref::<Result<WriteAggregationPlan, AggregationError>>(&system, "write-plans");
    let (error_ref, error_rx) =
        forward_ref::<Result<WriteAggregationPlan, AggregationError>>(&system, "write-errors");
    let key = ReplicatorKey::new("counter");

    replicator
        .tell(ReplicatorActorMsg::SetRemoteReplicas {
            nodes: vec![replica("a"), replica("b")],
            unreachable: BTreeSet::new(),
        })
        .unwrap();
    replicator
        .tell(ReplicatorActorMsg::PlanWrite {
            key: key.clone(),
            consistency: WriteConsistency::to(3, Duration::from_secs(1)).unwrap(),
            reply_to: plan_ref,
        })
        .unwrap();

    let plan = plan_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap()
        .unwrap();
    assert_eq!(plan.state().key(), &key);
    assert_eq!(plan.state().required_remote_acks(), 2);
    assert_eq!(plan.selection().primary(), &[replica("a"), replica("b")]);
    assert!(plan.selection().secondary().is_empty());

    replicator
        .tell(ReplicatorActorMsg::PlanWrite {
            key,
            consistency: WriteConsistency::to(4, Duration::from_secs(1)).unwrap(),
            reply_to: error_ref,
        })
        .unwrap();
    assert_eq!(
        error_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .expect_err("insufficient remote replicas should fail"),
        AggregationError::NotEnoughReplicas {
            required: 3,
            available: 2,
        }
    );

    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn replicator_actor_applies_cluster_route_updates_to_remote_plans() {
    let system = ActorSystem::builder("ddata-replicator-cluster-routes")
        .build()
        .unwrap();
    let replicator = system
        .spawn("replicator", Props::new(ReplicatorActor::<GCounter>::new))
        .unwrap();
    let (route_ref, route_rx) =
        forward_ref::<ReplicatorClusterRouteReport>(&system, "route-replies");
    let (plan_ref, plan_rx) =
        forward_ref::<Result<WriteAggregationPlan, AggregationError>>(&system, "route-plans");

    replicator
        .tell(ReplicatorActorMsg::ApplyClusterRouteUpdate {
            update: ReplicatorClusterRouteUpdate::new(
                [replica("c"), replica("a"), replica("b")],
                [replica("b")],
                [replica("removed")],
                true,
            ),
            all_reachable_time_nanos: 42,
            reply_to: route_ref,
        })
        .unwrap();

    let report = route_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(
        report.remote_replicas,
        vec![replica("a"), replica("b"), replica("c")]
    );
    assert_eq!(report.unreachable_replicas, BTreeSet::from([replica("b")]));
    assert_eq!(
        report.recorded_removed,
        BTreeSet::from([replica("removed")])
    );

    replicator
        .tell(ReplicatorActorMsg::PlanWrite {
            key: ReplicatorKey::new("counter"),
            consistency: WriteConsistency::majority(Duration::from_secs(1)),
            reply_to: plan_ref,
        })
        .unwrap();

    let plan = plan_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap()
        .unwrap();
    assert_eq!(plan.selection().primary(), &[replica("a"), replica("c")]);
    assert_eq!(plan.selection().secondary(), &[replica("b")]);

    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn replicator_actor_applies_remote_write_and_serves_remote_read_messages() {
    let system = ActorSystem::builder("ddata-replicator-direct-read-write")
        .build()
        .unwrap();
    let replicator = system
        .spawn("replicator", Props::new(ReplicatorActor::<GCounter>::new))
        .unwrap();
    let (write_result_ref, write_result_rx) = forward_ref(&system, "write-results");
    let (read_result_ref, read_result_rx) =
        forward_ref::<Result<DirectReadResult, String>>(&system, "read-results");
    let key = ReplicatorKey::new("counter");
    let remote = replica("remote");
    let envelope = DataEnvelope::new(
        GCounter::new()
            .increment(remote.clone(), 8)
            .unwrap()
            .reset_delta(),
    );
    let write = encode_write(&key, Some(remote.clone()), &envelope, &GCounterCodec).unwrap();
    let write_codec: Arc<dyn CrdtDataCodec<GCounter> + Send + Sync> = Arc::new(GCounterCodec);
    let read_codec: Arc<dyn CrdtDataCodec<GCounter> + Send + Sync> = Arc::new(GCounterCodec);

    replicator
        .tell(ReplicatorActorMsg::ApplyWrite {
            write,
            codec: write_codec,
            reply_to: write_result_ref,
        })
        .unwrap();
    assert!(matches!(
        write_result_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap(),
        DirectWriteResult::Ack { changed: true, .. }
    ));

    replicator
        .tell(ReplicatorActorMsg::ServeRead {
            read: encode_read(&key, Some(remote.clone())),
            codec: read_codec,
            reply_to: read_result_ref,
        })
        .unwrap();

    let read_result = read_result_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap()
        .unwrap();
    assert_eq!(read_result.key(), &key);
    assert_eq!(read_result.from(), Some(&remote));
    assert_eq!(
        decode_read_result(read_result.message(), &GCounterCodec)
            .unwrap()
            .unwrap()
            .data()
            .value()
            .unwrap(),
        8
    );

    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn replicator_actor_nacks_remote_write_decode_failures() {
    let system = ActorSystem::builder("ddata-replicator-direct-write-nack")
        .build()
        .unwrap();
    let replicator = system
        .spawn("replicator", Props::new(ReplicatorActor::<GCounter>::new))
        .unwrap();
    let (write_result_ref, write_result_rx) = forward_ref(&system, "write-results");
    let (get_ref, get_rx) = forward_ref(&system, "get-replies");
    let key = ReplicatorKey::new("counter");
    let write = crate::ReplicatorWrite {
        key: key.as_str().to_string(),
        from: Some(replica("remote")),
        envelope: crate::ReplicatorDataEnvelope {
            crdt_manifest: crate::GSET_STRING_MANIFEST.to_string(),
            crdt_version: crate::CRDT_CODEC_VERSION,
            payload: bytes::Bytes::new(),
            pruning: Vec::new(),
        },
    };
    let codec: Arc<dyn CrdtDataCodec<GCounter> + Send + Sync> = Arc::new(GCounterCodec);

    replicator
        .tell(ReplicatorActorMsg::ApplyWrite {
            write,
            codec,
            reply_to: write_result_ref,
        })
        .unwrap();
    assert!(matches!(
        write_result_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        DirectWriteResult::Nack { reason, .. } if reason.contains("expected CRDT manifest")
    ));

    replicator
        .tell(ReplicatorActorMsg::Get {
            key: key.clone(),
            consistency: ReadConsistency::local(),
            reply_to: get_ref,
        })
        .unwrap();
    assert_eq!(
        get_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        GetResponse::NotFound { key }
    );

    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn replicator_actor_applies_causal_delta_and_reports_status() {
    let system = ActorSystem::builder("ddata-replicator-causal-delta")
        .build()
        .unwrap();
    let replicator = system
        .spawn("replicator", Props::new(ReplicatorActor::<GCounter>::new))
        .unwrap();
    let (status_ref, status_rx) = forward_ref(&system, "delta-status");
    let (get_ref, get_rx) = forward_ref(&system, "get-replies");
    let key = ReplicatorKey::new("counter");
    let remote = replica("remote");

    replicator
        .tell(ReplicatorActorMsg::WriteCausalDelta {
            from: remote.clone(),
            key: key.clone(),
            from_version: 1,
            to_version: 1,
            delta: delta_counter("a", 4),
            reply_to: status_ref.clone(),
        })
        .unwrap();
    let applied = status_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(matches!(
        applied,
        DeltaReceiveStatus::Applied {
            previous_version: 0,
            to_version: 1,
            changed: true,
            ..
        }
    ));

    replicator
        .tell(ReplicatorActorMsg::Get {
            key: key.clone(),
            consistency: ReadConsistency::local(),
            reply_to: get_ref,
        })
        .unwrap();
    assert_eq!(
        get_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .data()
            .unwrap()
            .value()
            .unwrap(),
        4
    );

    replicator
        .tell(ReplicatorActorMsg::WriteCausalDelta {
            from: remote,
            key,
            from_version: 3,
            to_version: 3,
            delta: delta_counter("a", 7),
            reply_to: status_ref,
        })
        .unwrap();
    let missing = status_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(matches!(
        missing,
        DeltaReceiveStatus::Missing {
            current_version: 1,
            expected_from_version: 2,
            from_version: 3,
            ..
        }
    ));

    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn replicator_actor_applies_remote_delta_propagation_with_codec() {
    let system = ActorSystem::builder("ddata-replicator-apply-propagation")
        .build()
        .unwrap();
    let replicator = system
        .spawn("replicator", Props::new(ReplicatorActor::<GCounter>::new))
        .unwrap();
    let (report_ref, report_rx) = forward_ref(&system, "delta-report");
    let (get_ref, get_rx) = forward_ref(&system, "get-replies");
    let key = ReplicatorKey::new("counter");
    let remote = replica("remote");
    let mut log = DeltaPropagationLog::new([replica("local")]);
    log.record_delta(key.clone(), Some(delta_counter("a", 6)));
    let propagation = log.collect_propagations().into_values().next().unwrap();
    let wire = encode_delta_propagation(remote.clone(), true, &propagation, &GCounterCodec)
        .expect("wire propagation should encode");

    replicator
        .tell(ReplicatorActorMsg::ApplyDeltaPropagation {
            propagation: wire,
            codec: Arc::new(GCounterCodec),
            reply_to: report_ref,
        })
        .unwrap();

    let report = report_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(report.from(), &remote);
    assert!(report.is_success());
    assert!(matches!(report.reply(), Some(DeltaReceiveReply::Ack(_))));
    assert!(matches!(
        report.statuses(),
        [DeltaReceiveStatus::Applied {
            previous_version: 0,
            to_version: 1,
            changed: true,
            ..
        }]
    ));

    replicator
        .tell(ReplicatorActorMsg::Get {
            key,
            consistency: ReadConsistency::local(),
            reply_to: get_ref,
        })
        .unwrap();
    assert_eq!(
        get_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .data()
            .unwrap()
            .value()
            .unwrap(),
        6
    );

    system.terminate(Duration::from_secs(1)).unwrap();
}
