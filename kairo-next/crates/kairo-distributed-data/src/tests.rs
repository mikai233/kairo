use std::collections::BTreeSet;

use kairo_actor::Address;
use kairo_cluster::UniqueAddress;

use crate::{
    CrdtError, DeltaReplicatedData, GCounter, GSet, PNCounter, ReplicaId, ReplicatedData,
    ReplicatedDelta,
};

fn replica(id: &str) -> ReplicaId {
    ReplicaId::new(id)
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
