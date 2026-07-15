use super::*;

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
fn majority_plus_collapses_to_local_when_only_self_replica_exists() {
    let timeout = Duration::from_secs(1);
    let read = ReadConsistency::majority_plus_with_min_cap(timeout, 3, 5);
    let write = WriteConsistency::majority_plus_with_min_cap(timeout, 3, 5);

    assert!(read.is_local(0));
    assert!(write.is_local(0));
    assert!(!read.is_local(1));
    assert!(!write.is_local(1));
    assert_eq!(read.timeout(), Some(timeout));
    assert_eq!(write.timeout(), Some(timeout));
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
fn aggregators_ignore_unknown_and_duplicate_identified_replies() {
    let key = ReplicatorKey::new("counter");
    let nodes = vec![replica("a"), replica("b")];
    let mut write = WriteAggregatorState::new(
        key.clone(),
        &WriteConsistency::to(3, Duration::from_secs(1)).unwrap(),
        nodes.clone(),
    )
    .unwrap();

    assert_eq!(
        write.record_ack(&replica("unknown")),
        WriteAggregationOutcome::InProgress
    );
    assert_eq!(
        write.record_nack(&replica("a")),
        WriteAggregationOutcome::Failed {
            required: 2,
            available: 1,
        }
    );
    assert_eq!(
        write.record_ack(&replica("a")),
        WriteAggregationOutcome::InProgress
    );
    assert_eq!(
        write.record_ack(&replica("a")),
        WriteAggregationOutcome::InProgress
    );
    assert_eq!(
        write.record_nack(&replica("a")),
        WriteAggregationOutcome::InProgress
    );
    assert_eq!(
        write.record_ack(&replica("b")),
        WriteAggregationOutcome::Success
    );

    let mut read = ReadAggregatorState::<GCounter>::new(
        key,
        &ReadConsistency::from(3, Duration::from_secs(1)).unwrap(),
        nodes,
        None,
    )
    .unwrap();
    assert_eq!(
        read.record_read_from(&replica("unknown"), None),
        ReadAggregationOutcome::InProgress
    );
    assert_eq!(
        read.record_read_from(&replica("a"), None),
        ReadAggregationOutcome::InProgress
    );
    assert_eq!(
        read.record_read_from(
            &replica("a"),
            Some(DataEnvelope::new(full_counter("a", 10)))
        ),
        ReadAggregationOutcome::InProgress
    );
    assert_eq!(
        read.record_read_from(&replica("b"), None),
        ReadAggregationOutcome::NotFound
    );
}

#[test]
fn replica_selection_caps_delayed_secondaries_at_ten() {
    let nodes = (0..15)
        .map(|index| replica(&format!("node-{index:02}")))
        .collect::<Vec<_>>();
    let aggregator = WriteAggregatorState::new(
        ReplicatorKey::new("counter"),
        &WriteConsistency::to(2, Duration::from_secs(1)).unwrap(),
        nodes,
    )
    .unwrap();

    let selection = aggregator.select_replicas(&BTreeSet::new());

    assert_eq!(selection.primary(), &[replica("node-00")]);
    assert_eq!(selection.secondary().len(), 10);
    assert_eq!(selection.secondary()[0], replica("node-01"));
    assert_eq!(selection.secondary()[9], replica("node-10"));
}
