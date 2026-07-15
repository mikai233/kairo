use super::*;

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
            pruning: Vec::new(),
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
fn delta_receive_applies_pruning_before_late_removed_replica_data() {
    let key = ReplicatorKey::new("counter");
    let remote = replica("remote");
    let removed = replica("removed");
    let mut pruning = PruningTable::new();
    pruning.mark_performed(removed.clone(), u64::MAX);
    let mut log = DeltaPropagationLog::new([replica("local")]);
    log.record_delta(key.clone(), Some(delta_counter("removed", 4)));
    let mut propagation = log.collect_propagations().into_values().next().unwrap();
    propagation.attach_pruning(|_| pruning.clone());
    let wire = encode_delta_propagation(remote, false, &propagation, &GCounterCodec).unwrap();
    let mut state = ReplicatorState::<GCounter>::new();
    let mut tracker = DeltaReceiveTracker::new();

    let report = tracker.apply_propagation(&mut state, &wire, &GCounterCodec);

    assert!(report.is_success());
    let envelope = state.envelope(&key).unwrap();
    assert_eq!(envelope.data().value().unwrap(), 0);
    assert!(matches!(
        envelope.pruning().get(&removed),
        Some(PruningState::Performed(_))
    ));
}

#[test]
fn delta_receive_marks_initialized_pruning_seen_by_receiver() {
    let key = ReplicatorKey::new("counter");
    let remote = replica("remote");
    let local = replica("local");
    let removed = replica("removed");
    let mut pruning = PruningTable::new();
    pruning.initialize(removed.clone(), remote.clone());
    let mut log = DeltaPropagationLog::new([local.clone()]);
    log.record_delta(key.clone(), Some(delta_counter("remote", 2)));
    let mut propagation = log.collect_propagations().into_values().next().unwrap();
    propagation.attach_pruning(|_| pruning.clone());
    let wire = encode_delta_propagation(remote, false, &propagation, &GCounterCodec).unwrap();
    let mut state = ReplicatorState::<GCounter>::new();
    let mut tracker = DeltaReceiveTracker::new();

    let report = tracker.apply_propagation_with_seen(&mut state, &wire, &GCounterCodec, &local);

    assert!(report.is_success());
    let PruningState::Initialized(initialized) = state
        .envelope(&key)
        .unwrap()
        .pruning()
        .get(&removed)
        .unwrap()
    else {
        panic!("expected initialized pruning marker");
    };
    assert!(initialized.seen().contains(&local));
}
