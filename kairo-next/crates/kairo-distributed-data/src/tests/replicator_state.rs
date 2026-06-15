use super::*;

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
fn replicator_state_stores_lww_register_updates_and_remote_delta() {
    let key = ReplicatorKey::new("lww");
    let node_a = replica("a");
    let node_b = replica("b");
    let mut state = ReplicatorState::<LWWRegister<&'static str>>::new();

    let outcome = state
        .update_local(
            key.clone(),
            LWWRegister::new(node_a.clone(), "initial", 1),
            |register| Ok::<_, CrdtError>(register.with_value(node_a.clone(), "local", 2)),
        )
        .unwrap();
    assert!(outcome.changed());

    state.write_delta(key.clone(), LWWRegister::new(node_b.clone(), "remote", 3));

    assert_eq!(
        state.get_local(&key),
        GetResponse::Success {
            key,
            data: LWWRegister::new(node_b, "remote", 3),
        }
    );
}

#[test]
fn replicator_state_applies_remote_ormap_delta_to_zero_when_missing() {
    let key = ReplicatorKey::new("ormap");
    let node = replica("a");
    let mut state = ReplicatorState::<ORMap<&'static str, GCounter>>::new();
    let delta = ORMap::new()
        .put(node.clone(), "counter", GCounter::new())
        .updated(node.clone(), "counter", GCounter::new(), |counter| {
            counter.increment(node, 4).unwrap()
        })
        .delta()
        .unwrap();

    state.write_delta(key.clone(), delta);

    let GetResponse::Success { data, .. } = state.get_local(&key) else {
        panic!("map should exist");
    };
    assert_eq!(data.get(&"counter").unwrap().value().unwrap(), 4);
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

#[test]
fn replicator_state_removes_obsolete_performed_pruning_markers() {
    let mut state = ReplicatorState::<GCounter>::new();
    let removed = replica("removed");
    let owner = replica("owner");
    let key_a = ReplicatorKey::new("a");
    let key_b = ReplicatorKey::new("b");

    for (key, amount) in [(key_a.clone(), 3), (key_b.clone(), 5)] {
        let envelope = DataEnvelope::new(
            GCounter::new()
                .increment(removed.clone(), amount)
                .unwrap()
                .reset_delta(),
        )
        .init_removed_node_pruning(removed.clone(), owner.clone())
        .prune_removed_node(&removed, PruningPerformed::new(100))
        .unwrap();
        state.write_full(key, envelope);
    }
    state.flush_changes();

    let (changed_before_deadline, removed_before_deadline) =
        state.remove_obsolete_pruning_performed(99);
    assert!(changed_before_deadline.is_empty());
    assert!(removed_before_deadline.is_empty());

    let (changed, forgotten) = state.remove_obsolete_pruning_performed(100);

    assert_eq!(changed, BTreeSet::from([key_a.clone(), key_b.clone()]));
    assert_eq!(forgotten, BTreeSet::from([removed.clone()]));
    assert!(state.envelope(&key_a).unwrap().pruning().is_empty());
    assert!(state.envelope(&key_b).unwrap().pruning().is_empty());
    assert_eq!(
        state.envelope(&key_a).unwrap().data().replica_value(&owner),
        3
    );
    assert_eq!(
        state.envelope(&key_b).unwrap().data().replica_value(&owner),
        5
    );
    assert_eq!(
        state
            .flush_changes()
            .into_iter()
            .map(|change| change.key().clone())
            .collect::<BTreeSet<_>>(),
        changed
    );
}
