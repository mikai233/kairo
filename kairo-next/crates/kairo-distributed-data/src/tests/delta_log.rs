use super::*;

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
