use super::*;

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
fn orset_prunes_removed_replica_dots_into_survivor() {
    let removed = replica("removed");
    let survivor = replica("survivor");
    let set = ORSet::new().add(removed.clone(), "entity").reset_delta();

    assert!(set.need_pruning_from(&removed));
    let pruned = RemovedNodePruning::prune(&set, &removed, survivor.clone()).unwrap();

    assert!(pruned.contains(&"entity"));
    assert!(!pruned.need_pruning_from(&removed));
    assert!(pruned.modified_by_replica_ids().contains(&survivor));
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
fn lww_register_uses_latest_timestamp_and_lowest_replica_tie_breaker() {
    let node_a = replica("a");
    let node_b = replica("b");
    let older = LWWRegister::new(node_a.clone(), "older", 1);
    let newer = LWWRegister::new(node_b.clone(), "newer", 2);

    assert_eq!(older.merge(&newer), newer);

    let high_replica = LWWRegister::new(node_b, "high", 3);
    let low_replica = LWWRegister::new(node_a, "low", 3);

    assert_eq!(high_replica.merge(&low_replica), low_replica);
}

#[test]
fn lww_register_tracks_delta_for_value_update() {
    let node = replica("a");
    let register = LWWRegister::new(node.clone(), "old", 1);
    let updated = register.with_value(node.clone(), "new", 2);
    let delta = updated.delta().expect("register update should keep delta");

    assert_eq!(delta, LWWRegister::new(node, "new", 2));
    assert_eq!(register.merge_delta(&delta), updated.reset_delta());
    assert_eq!(updated.reset_delta().delta(), None);
}

#[test]
fn lww_register_default_and_reverse_clocks_are_monotonic() {
    let node = replica("a");
    let default = LWWRegister::new(node.clone(), 0, 100).with_value_by_clock(
        node.clone(),
        1,
        |timestamp, _| crate::default_lww_clock(timestamp),
    );
    assert!(default.timestamp() > 100);

    let reverse = LWWRegister::new(node.clone(), 0, -100).with_value_by_clock(
        node.clone(),
        1,
        |timestamp, _| crate::reverse_lww_clock(timestamp),
    );
    assert!(reverse.timestamp() < -100);

    let first = LWWRegister::new(node.clone(), "first", -1);
    let later = first.with_value_reverse_clock(node, "later");

    assert_eq!(first.merge(&later), first);
}

#[test]
fn lww_register_prunes_removed_writer_to_survivor() {
    let removed = replica("removed");
    let survivor = replica("survivor");
    let register = LWWRegister::new(removed.clone(), "value", 7);

    assert!(register.need_pruning_from(&removed));
    let pruned = register.prune(&removed, survivor.clone()).unwrap();

    assert_eq!(pruned.node(), &survivor);
    assert_eq!(pruned.value(), &"value");
    assert_eq!(pruned.timestamp(), 7);
    assert!(!pruned.need_pruning_from(&removed));
}

#[test]
fn ormap_adds_entries_and_replays_put_deltas() {
    let node = replica("a");
    let map = ORMap::new()
        .put(node.clone(), "a", GSet::new().add("A"))
        .put(node, "b", GSet::new().add("B"));
    let delta = map.delta().expect("put operations should collect a delta");

    let replayed = ORMap::new().merge_delta(&delta);

    assert_eq!(
        replayed.get(&"a").unwrap().elements(),
        &BTreeSet::from(["A"])
    );
    assert_eq!(
        replayed.get(&"b").unwrap().elements(),
        &BTreeSet::from(["B"])
    );
    assert_eq!(replayed.delta(), None);
}

#[test]
fn ormap_removes_entry_and_replays_remove_delta() {
    let node = replica("a");
    let map = ORMap::new()
        .put(node.clone(), "a", GSet::new().add("A"))
        .put(node.clone(), "b", GSet::new().add("B"));
    let add_delta = map.delta().expect("puts should collect a delta");
    let remove_delta = map
        .reset_delta()
        .remove(node, &"a")
        .delta()
        .expect("remove should collect a delta");

    let replayed = ORMap::new()
        .merge_delta(&add_delta)
        .merge_delta(&remove_delta);

    assert!(!replayed.contains_key(&"a"));
    assert_eq!(
        replayed.get(&"b").unwrap().elements(),
        &BTreeSet::from(["B"])
    );
}

#[test]
fn ormap_full_merge_keeps_observed_removed_keys_and_merges_concurrent_values() {
    let node_a = replica("a");
    let node_b = replica("b");
    let left = ORMap::new()
        .put(node_a.clone(), "a", GSet::new().add("A1"))
        .put(node_a.clone(), "b", GSet::new().add("B1"))
        .remove(node_a.clone(), &"a")
        .put(node_a, "d", GSet::new().add("D1"))
        .reset_delta();
    let right = ORMap::new()
        .put(node_b.clone(), "c", GSet::new().add("C2"))
        .put(node_b.clone(), "a", GSet::new().add("A2"))
        .put(node_b.clone(), "b", GSet::new().add("B2"))
        .remove(node_b.clone(), &"b")
        .put(node_b, "d", GSet::new().add("D2"))
        .reset_delta();

    let merged = left.merge(&right);

    assert_eq!(
        merged.get(&"a").unwrap().elements(),
        &BTreeSet::from(["A2"])
    );
    assert_eq!(
        merged.get(&"b").unwrap().elements(),
        &BTreeSet::from(["B1"])
    );
    assert_eq!(
        merged.get(&"c").unwrap().elements(),
        &BTreeSet::from(["C2"])
    );
    assert_eq!(
        merged.get(&"d").unwrap().elements(),
        &BTreeSet::from(["D1", "D2"])
    );
}

#[test]
fn ormap_updated_uses_value_deltas_for_existing_delta_crdts() {
    let node_a = replica("a");
    let node_b = replica("b");
    let initial = ORMap::new().put(node_a.clone(), "counter", GCounter::new());
    let first_update =
        initial
            .reset_delta()
            .updated(node_a.clone(), "counter", GCounter::new(), |counter| {
                counter.increment(node_a, 10).unwrap()
            });
    let second_update =
        first_update
            .reset_delta()
            .updated(node_b.clone(), "counter", GCounter::new(), |counter| {
                counter.increment(node_b, 7).unwrap()
            });

    let replayed = ORMap::new()
        .merge_delta(&initial.delta().unwrap())
        .merge_delta(&first_update.delta().unwrap())
        .merge_delta(&second_update.delta().unwrap());

    assert_eq!(replayed.get(&"counter").unwrap().value().unwrap(), 17);
}

#[test]
fn ormap_prunes_keys_and_values_modified_by_removed_replica() {
    let removed = replica("removed");
    let survivor = replica("survivor");
    let map = ORMap::new()
        .put(
            removed.clone(),
            "register",
            LWWRegister::new(removed.clone(), "value", 1),
        )
        .reset_delta();

    assert!(map.need_pruning_from(&removed));
    let pruned = map.prune(&removed, survivor.clone()).unwrap();

    assert!(!pruned.need_pruning_from(&removed));
    assert!(pruned.modified_by_replica_ids().contains(&survivor));
    assert_eq!(pruned.get(&"register").unwrap().node(), &survivor);
}

#[test]
fn ormap_pruning_cleanup_removes_removed_replica_from_key_causality() {
    let removed = replica("removed");
    let map = ORMap::new()
        .put(removed.clone(), "key", GSet::new().add("value"))
        .reset_delta();

    assert!(map.need_pruning_from(&removed));
    assert!(map.modified_by_replica_ids().contains(&removed));

    let cleaned = map.pruning_cleanup(&removed);

    assert_eq!(
        cleaned.get(&"key").unwrap().elements(),
        &BTreeSet::from(["value"])
    );
    assert!(!cleaned.need_pruning_from(&removed));
    assert!(!cleaned.modified_by_replica_ids().contains(&removed));
    assert_eq!(cleaned.delta(), None);
}
