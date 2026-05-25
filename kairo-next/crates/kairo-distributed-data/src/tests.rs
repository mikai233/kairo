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
    PNCounter, PNCounterCodec, ReadAggregationOutcome, ReadAggregationPlan, ReadAggregatorState,
    ReadConsistency, ReplicaId, ReplicatedData, ReplicatedDelta, ReplicatorActor,
    ReplicatorActorMsg, ReplicatorAggregation, ReplicatorDeltaPropagation, ReplicatorKey,
    ReplicatorState, UpdateResponse, WriteAggregationOutcome, WriteAggregationPlan,
    WriteAggregatorState, WriteConsistency, calculate_majority, decode_data_envelope,
    decode_delta_propagation, decode_read_result, encode_data_envelope, encode_delta_propagation,
    encode_read, encode_read_result, encode_write,
};

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

#[test]
fn replica_id_can_be_derived_from_cluster_unique_address() {
    let address = Address::new("kairo", "sys", Some("127.0.0.1".to_string()), Some(25520));
    let unique = UniqueAddress::new(address, 42);

    assert_eq!(
        ReplicaId::from(&unique).as_str(),
        "kairo://sys@127.0.0.1:25520#42"
    );
}

#[test]
fn gset_adds_and_merges_by_union() {
    let left = GSet::new().add("a").add("b");
    let right = GSet::new().add("b").add("c");

    let merged = left.merge(&right);

    assert_eq!(merged.elements(), &BTreeSet::from(["a", "b", "c"]));
    assert_eq!(merged.delta(), None);
}

#[test]
fn gset_accumulates_delta_and_can_merge_delta_into_empty_state() {
    let full = GSet::new().add("a").add("b");
    let delta = full.delta().expect("delta should be collected");

    assert_eq!(delta.elements(), &BTreeSet::from(["a", "b"]));
    assert_eq!(delta.zero().merge_delta(&delta), full.reset_delta());
    assert_eq!(full.reset_delta().delta(), None);
}

#[test]
fn orset_add_remove_delta_replays_observed_operations() {
    let node_a = replica("a");
    let full = ORSet::new()
        .add(node_a.clone(), "entity-1")
        .add(node_a.clone(), "entity-2")
        .remove(node_a, &"entity-1");
    let delta = full.delta().expect("orset should collect deltas");

    assert_eq!(full.elements(), BTreeSet::from(["entity-2"]));
    assert_eq!(delta.zero().merge_delta(&delta), full.reset_delta());
    assert_eq!(full.reset_delta().delta(), None);
}

#[test]
fn orset_full_merge_removes_seen_adds_and_keeps_concurrent_adds() {
    let node_a = replica("a");
    let node_b = replica("b");
    let base = ORSet::new().add(node_a.clone(), "entity").reset_delta();

    let removed = base.remove(node_b.clone(), &"entity").reset_delta();
    assert!(!base.merge(&removed).contains(&"entity"));

    let concurrent_add = base.add(node_a, "entity").reset_delta();
    let merged = removed.merge(&concurrent_add);

    assert!(merged.contains(&"entity"));
    assert_eq!(
        merged.dots_for(&"entity").unwrap(),
        concurrent_add.dots_for(&"entity").unwrap()
    );
}

#[test]
fn orset_remove_delta_keeps_unseen_concurrent_dot() {
    let node_a = replica("a");
    let node_b = replica("b");
    let base = ORSet::new().add(node_a.clone(), "entity").reset_delta();
    let remove_delta = base
        .remove(node_b, &"entity")
        .delta()
        .expect("remove should produce a delta");
    let concurrent_add = base.add(node_a, "entity").reset_delta();

    let merged = concurrent_add.merge_delta(&remove_delta);

    assert!(merged.contains(&"entity"));
    assert_eq!(
        merged.dots_for(&"entity").unwrap(),
        concurrent_add.dots_for(&"entity").unwrap()
    );
}

#[test]
fn orset_merges_concurrent_add_dots_for_same_element() {
    let node_a = replica("a");
    let node_b = replica("b");
    let left = ORSet::new().add(node_a, "entity").reset_delta();
    let right = ORSet::new().add(node_b, "entity").reset_delta();

    let merged = left.merge(&right);

    assert_eq!(merged.elements(), BTreeSet::from(["entity"]));
    assert_eq!(merged.dots_for(&"entity").unwrap().len(), 2);
}

#[test]
fn gcounter_increments_are_per_replica_and_merge_by_maximum() {
    let node_a = replica("a");
    let node_b = replica("b");
    let left = GCounter::new()
        .increment(node_a.clone(), 3)
        .unwrap()
        .increment(node_b.clone(), 1)
        .unwrap();
    let right = GCounter::new()
        .increment(node_a.clone(), 2)
        .unwrap()
        .increment(node_b.clone(), 5)
        .unwrap();

    let merged = left.merge(&right);

    assert_eq!(merged.replica_value(&node_a), 3);
    assert_eq!(merged.replica_value(&node_b), 5);
    assert_eq!(merged.value().unwrap(), 8);
    assert_eq!(merged.delta(), None);
}

#[test]
fn gcounter_delta_tracks_absolute_replica_values() {
    let node_a = replica("a");
    let full = GCounter::new()
        .increment(node_a.clone(), 2)
        .unwrap()
        .increment(node_a.clone(), 3)
        .unwrap();
    let delta = full.delta().expect("delta should be collected");

    assert_eq!(delta.replica_value(&node_a), 5);
    assert_eq!(GCounter::new().merge_delta(&delta), full.reset_delta());
    assert_eq!(full.reset_delta().delta(), None);
}

#[test]
fn gcounter_prunes_removed_replica_into_survivor() {
    let removed = replica("removed");
    let survivor = replica("survivor");
    let counter = GCounter::new()
        .increment(removed.clone(), 4)
        .unwrap()
        .increment(survivor.clone(), 6)
        .unwrap()
        .reset_delta();

    let pruned = counter.prune(&removed, survivor.clone()).unwrap();

    assert_eq!(pruned.replica_value(&removed), 0);
    assert_eq!(pruned.replica_value(&survivor), 10);
    assert!(!pruned.need_pruning_from(&removed));
}

#[test]
fn gcounter_reports_overflow_instead_of_wrapping() {
    let error = GCounter::from_state([(replica("a"), u128::MAX)])
        .increment(replica("a"), 1)
        .expect_err("overflow should be explicit");

    assert_eq!(error, CrdtError::CounterOverflow);
}

#[test]
fn pncounter_composes_increment_and_decrement_counters() {
    let node_a = replica("a");
    let node_b = replica("b");
    let left = PNCounter::new()
        .increment(node_a.clone(), 7)
        .unwrap()
        .decrement(node_b.clone(), 2)
        .unwrap();
    let right = PNCounter::new()
        .increment(node_a.clone(), 3)
        .unwrap()
        .decrement(node_b.clone(), 5)
        .unwrap();

    let merged = left.merge(&right);

    assert_eq!(merged.increments().replica_value(&node_a), 7);
    assert_eq!(merged.decrements().replica_value(&node_b), 5);
    assert_eq!(merged.value().unwrap(), 2);
}

#[test]
fn pncounter_delta_contains_inner_counter_deltas() {
    let node = replica("a");
    let full = PNCounter::new()
        .increment(node.clone(), 10)
        .unwrap()
        .decrement(node.clone(), 4)
        .unwrap();
    let delta = full.delta().expect("pn counter keeps a delta value");

    assert_eq!(delta.value().unwrap(), 6);
    assert_eq!(PNCounter::new().merge_delta(&delta), full.reset_delta());
}

#[test]
fn crdt_codecs_round_trip_gset_strings_in_stable_order() {
    let data = GSet::new()
        .add("b".to_string())
        .add("a".to_string())
        .reset_delta();

    let serialized = GSetStringCodec.serialize(&data).unwrap();
    let serialized_again = GSetStringCodec.serialize(&data).unwrap();

    assert_eq!(serialized.manifest(), crate::GSET_STRING_MANIFEST);
    assert_eq!(serialized.payload(), serialized_again.payload());
    assert_eq!(GSetStringCodec.deserialize(serialized).unwrap(), data);
}

#[test]
fn crdt_codecs_round_trip_gcounter_by_sorted_replica_ids() {
    let data = GCounter::new()
        .increment(replica("b"), 2)
        .unwrap()
        .increment(replica("a"), 5)
        .unwrap()
        .reset_delta();

    let serialized = GCounterCodec.serialize(&data).unwrap();
    let serialized_again = GCounterCodec.serialize(&data).unwrap();

    assert_eq!(serialized.manifest(), crate::GCOUNTER_MANIFEST);
    assert_eq!(serialized.payload(), serialized_again.payload());
    assert_eq!(GCounterCodec.deserialize(serialized).unwrap(), data);
}

#[test]
fn crdt_codecs_round_trip_pncounter() {
    let data = PNCounter::new()
        .increment(replica("a"), 7)
        .unwrap()
        .decrement(replica("b"), 4)
        .unwrap()
        .reset_delta();

    let serialized = PNCounterCodec.serialize(&data).unwrap();

    assert_eq!(serialized.manifest(), crate::PNCOUNTER_MANIFEST);
    assert_eq!(PNCounterCodec.deserialize(serialized).unwrap(), data);
}

#[test]
fn crdt_codecs_reject_wrong_manifest_and_unknown_version() {
    let data = GCounter::new().increment(replica("a"), 1).unwrap();
    let serialized = GCounterCodec.serialize(&data).unwrap();
    let wrong_manifest = crate::SerializedCrdt::new(
        crate::GSET_STRING_MANIFEST,
        serialized.version(),
        serialized.payload().clone(),
    );
    let wrong_version = crate::SerializedCrdt::new(
        crate::GCOUNTER_MANIFEST,
        crate::CRDT_CODEC_VERSION + 1,
        serialized.payload().clone(),
    );

    assert!(
        GCounterCodec
            .deserialize(wrong_manifest)
            .unwrap_err()
            .to_string()
            .contains("expected CRDT manifest")
    );
    assert!(
        GCounterCodec
            .deserialize(wrong_version)
            .unwrap_err()
            .to_string()
            .contains("unsupported")
    );
}

#[test]
fn delta_propagation_log_records_versions_and_merges_unsent_deltas() {
    let key = ReplicatorKey::new("counter");
    let node_a = replica("node-a");
    let node_b = replica("node-b");
    let mut log = DeltaPropagationLog::new([node_a.clone(), node_b.clone()]);

    assert_eq!(
        log.record_delta(key.clone(), Some(delta_counter("a", 1))),
        1
    );
    assert_eq!(
        log.record_delta(key.clone(), Some(delta_counter("b", 2))),
        2
    );
    assert_eq!(log.current_version(&key), 2);

    let propagations = log.collect_propagations();

    assert_eq!(propagations.len(), 2);
    for node in [node_a, node_b] {
        let entry = propagations
            .get(&node)
            .unwrap()
            .entries()
            .get(&key)
            .unwrap();
        assert_eq!(entry.from_version(), 1);
        assert_eq!(entry.to_version(), 2);
        assert_eq!(entry.delta().value().unwrap(), 3);
    }
}

#[test]
fn delta_propagation_log_advances_versions_for_no_payload_entries() {
    let key = ReplicatorKey::new("counter");
    let node = replica("node");
    let mut log = DeltaPropagationLog::new([node]);

    log.record_delta(key.clone(), None);
    log.record_delta(key.clone(), Some(delta_counter("a", 1)));

    let propagations = log.collect_propagations();

    assert!(propagations.is_empty());
    assert_eq!(log.current_version(&key), 2);
    assert!(log.has_delta_entries(&key));
}

#[test]
fn delta_propagation_log_selects_nodes_by_round_robin_slice() {
    let key = ReplicatorKey::new("counter");
    let nodes = (0..12)
        .map(|idx| replica(&format!("node-{idx:02}")))
        .collect::<Vec<_>>();
    let mut log = DeltaPropagationLog::new(nodes.clone()).with_gossip_interval_divisor(5);
    log.record_delta(key.clone(), Some(delta_counter("a", 1)));

    let first = log.collect_propagations();
    log.record_delta(key.clone(), Some(delta_counter("a", 2)));
    let second = log.collect_propagations();

    assert_eq!(first.keys().cloned().collect::<Vec<_>>(), nodes[0..3]);
    assert_eq!(second.keys().cloned().collect::<Vec<_>>(), nodes[3..6]);
}

#[test]
fn delta_propagation_log_cleans_entries_after_all_nodes_have_seen_them() {
    let key = ReplicatorKey::new("counter");
    let mut log = DeltaPropagationLog::new([replica("a"), replica("b")]);
    log.record_delta(key.clone(), Some(delta_counter("a", 1)));

    log.collect_propagations();
    log.cleanup_delta_entries();

    assert!(!log.has_delta_entries(&key));
    assert_eq!(log.current_version(&key), 1);
}

#[test]
fn delta_propagation_log_deletes_key_and_forgets_removed_nodes() {
    let key = ReplicatorKey::new("counter");
    let node_a = replica("node-a");
    let node_b = replica("node-b");
    let mut log = DeltaPropagationLog::new([node_a.clone(), node_b.clone()]);
    log.record_delta(key.clone(), Some(delta_counter("a", 1)));
    log.collect_propagations();

    log.cleanup_removed_node(&node_b);
    log.set_nodes([node_a]);
    log.cleanup_delta_entries();
    assert!(!log.has_delta_entries(&key));

    log.record_delta(key.clone(), Some(delta_counter("a", 2)));
    log.delete_key(&key);
    assert_eq!(log.current_version(&key), 0);
    assert!(!log.has_delta_entries(&key));
}

#[test]
fn delta_wire_encodes_manifest_tagged_propagation_entries() {
    let key = ReplicatorKey::new("counter");
    let remote = replica("remote");
    let local = replica("local");
    let mut log = DeltaPropagationLog::new([remote]);
    log.record_delta(key.clone(), Some(delta_counter("a", 1)));
    log.record_delta(key.clone(), Some(delta_counter("b", 2)));
    let propagation = log
        .collect_propagations()
        .remove(&replica("remote"))
        .unwrap();

    let wire = encode_delta_propagation(local.clone(), true, &propagation, &GCounterCodec).unwrap();

    assert_eq!(wire.from, local);
    assert!(wire.reply);
    assert_eq!(wire.deltas.len(), 1);
    assert_eq!(wire.deltas[0].key, key.as_str());
    assert_eq!(wire.deltas[0].crdt_manifest, crate::GCOUNTER_MANIFEST);
    assert_eq!(wire.deltas[0].crdt_version, crate::CRDT_CODEC_VERSION);
    assert_eq!(wire.deltas[0].from_version, 1);
    assert_eq!(wire.deltas[0].to_version, 2);

    let decoded = decode_delta_propagation(&wire, &GCounterCodec).unwrap();
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].key(), &key);
    assert_eq!(decoded[0].from_version(), 1);
    assert_eq!(decoded[0].to_version(), 2);
    assert_eq!(decoded[0].delta().value().unwrap(), 3);
}

#[test]
fn delta_wire_rejects_unregistered_crdt_manifest_for_codec() {
    let wire_delta = crate::ReplicatorDelta {
        key: "counter".to_string(),
        crdt_manifest: "kairo.ddata.some-other-crdt".to_string(),
        crdt_version: crate::CRDT_CODEC_VERSION,
        payload: bytes::Bytes::new(),
        from_version: 1,
        to_version: 1,
    };
    let wire = crate::ReplicatorDeltaPropagation {
        from: replica("remote"),
        reply: false,
        deltas: vec![wire_delta],
    };

    let error = decode_delta_propagation::<GCounter, _>(&wire, &GCounterCodec)
        .expect_err("wrong CRDT manifest should fail");

    assert!(error.to_string().contains("expected CRDT manifest"));
}

#[test]
fn delta_transport_publishes_collected_propagations_to_targets() {
    let system = ActorSystem::builder("ddata-delta-transport")
        .build()
        .unwrap();
    let (target_ref, target_rx) = forward_ref(&system, "remote-replicator");
    let local = replica("local");
    let remote = replica("remote");
    let key = ReplicatorKey::new("counter");
    let mut log = DeltaPropagationLog::new([remote.clone()]);
    log.record_delta(key.clone(), Some(delta_counter("a", 5)));
    let propagations = log.collect_propagations();
    let mut transport = DeltaPropagationTransport::new(local.clone(), GCounterCodec);
    transport.insert_target(DeltaPropagationTarget::new(remote.clone(), target_ref));

    let report = transport.publish(propagations);

    assert!(report.is_success());
    assert_eq!(report.sent_to(), &[remote]);
    let wire = target_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(wire.from, local);
    assert!(!wire.reply);
    let decoded = decode_delta_propagation(&wire, &GCounterCodec).unwrap();
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].key(), &key);
    assert_eq!(decoded[0].delta().value().unwrap(), 5);

    system.terminate(Duration::from_secs(1)).unwrap();
}

#[test]
fn delta_transport_reports_missing_targets_without_dropping_other_sends() {
    let system = ActorSystem::builder("ddata-delta-transport-missing")
        .build()
        .unwrap();
    let (target_ref, target_rx) = forward_ref(&system, "remote-a");
    let remote_a = replica("remote-a");
    let remote_b = replica("remote-b");
    let mut log = DeltaPropagationLog::new([remote_a.clone(), remote_b.clone()]);
    log.record_delta(ReplicatorKey::new("counter"), Some(delta_counter("a", 1)));
    let propagations = log.collect_propagations();
    let mut transport = DeltaPropagationTransport::new(replica("local"), GCounterCodec);
    transport.insert_target(DeltaPropagationTarget::new(remote_a.clone(), target_ref));

    let report = transport.publish(propagations);

    assert_eq!(report.sent_to(), &[remote_a]);
    assert!(matches!(
        report.failures(),
        [DeltaTransportFailure::MissingTarget { replica }] if replica == &remote_b
    ));
    target_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    system.terminate(Duration::from_secs(1)).unwrap();
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
    let envelope = DataEnvelope::new(
        GCounter::new()
            .increment(replica("local"), 7)
            .unwrap()
            .reset_delta(),
    );
    let key = ReplicatorKey::new("counter");
    let from = replica("local");

    let wire_envelope = encode_data_envelope(&envelope, &GCounterCodec).unwrap();
    assert_eq!(wire_envelope.crdt_manifest, crate::GCOUNTER_MANIFEST);
    assert_eq!(wire_envelope.crdt_version, crate::CRDT_CODEC_VERSION);
    assert_eq!(
        decode_data_envelope(&wire_envelope, &GCounterCodec)
            .unwrap()
            .data()
            .value()
            .unwrap(),
        7
    );

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
        7
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
    };
    assert!(
        decode_data_envelope::<GCounter, _>(&wrong_manifest, &GCounterCodec)
            .unwrap_err()
            .to_string()
            .contains("expected CRDT manifest")
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
