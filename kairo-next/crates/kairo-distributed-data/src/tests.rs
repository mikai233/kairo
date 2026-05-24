use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, mpsc};
use std::time::Duration;

use kairo_actor::{Actor, ActorResult, ActorSystem, Address, Context, Props};
use kairo_cluster::UniqueAddress;

use crate::{
    ConsistencyError, CrdtDataCodec, CrdtError, DataEnvelope, DeltaPropagationLog,
    DeltaPropagationTarget, DeltaPropagationTransport, DeltaReceiveFailure, DeltaReceiveReply,
    DeltaReceiveStatus, DeltaReceiveTracker, DeltaReplicatedData, DeltaTransportFailure, GCounter,
    GCounterCodec, GSet, GSetStringCodec, GetResponse, PNCounter, PNCounterCodec, ReadConsistency,
    ReplicaId, ReplicatedData, ReplicatedDelta, ReplicatorActor, ReplicatorActorMsg, ReplicatorKey,
    ReplicatorState, UpdateResponse, WriteConsistency, decode_delta_propagation,
    encode_delta_propagation,
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
